//! Implementation of [`DQLLike`] for sqlparser's `Query` node.
//!
//! The `FROM` scope of a query's outer `SELECT` resolves each base-table
//! relation and derives the output columns of CTE references and derived
//! subqueries from their own definitions, since the projection that produces
//! them sits in the same statement. `projection_source_table` then resolves
//! each projected item to the base table it comes from. A derived relation
//! only feeds that row-identity answer when its body preserves row identity
//! (no grouping, deduplication, or window filtering), so a consumer
//! re-executing rows against the source table is never handed a computed or
//! collapsed row. It is deliberately strict: anything it cannot prove comes
//! from a single base table yields `Ok(None)`, and a table function, an
//! unresolvable name, or a body whose columns cannot be enumerated (an
//! unnamed computed projection item, a nested join) stays opaque.

use alloc::{
    borrow::ToOwned,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use sqlparser::ast::{
    Expr, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetQuantifier, TableAlias, TableFactor,
    WildcardAdditionalOptions, With,
};

use crate::{
    errors::LookupError,
    traits::{ColumnLike, DQLLike, DatabaseLike, TableLike},
    utils::{
        identifier_resolution::identifiers_match,
        object_name::{
            object_name_last_part, render_table_candidate, resolve_object_name,
            schema_from_object_name,
        },
    },
};

/// A `FROM` relation that resolved to a base table, paired with the identifier
/// (alias, or table name when unaliased) used to qualify it in the projection.
/// `nullable` marks a relation on the null-extended side of an outer join:
/// its rows may be absent from an output row, so it never answers the
/// row-identity question. `output_names` are the names the relation exposes:
/// the table's own columns in declaration order with the alias's column list
/// applied positionally, which replaces the originals (PostgreSQL).
pub(crate) struct FromTableRef<'a, 'db, DB: DatabaseLike> {
    pub(crate) key_value: &'a str,
    pub(crate) key_quoted: bool,
    pub(crate) table: &'db DB::Table,
    pub(crate) nullable: bool,
    pub(crate) entry_index: usize,
    pub(crate) output_names: Vec<(String, bool)>,
}

/// One output column of a derivable relation, and the base table whose column
/// it passes through. A `None` source means no single base table declares the
/// column: it is computed (`count(*) AS n`), or its set-operation arms name
/// different tables.
pub(crate) struct DerivedColumn<'db, DB: DatabaseLike> {
    pub(crate) name: String,
    pub(crate) quoted: bool,
    pub(crate) source: Option<&'db DB::Table>,
}

impl<DB: DatabaseLike> Clone for DerivedColumn<'_, DB> {
    fn clone(&self) -> Self {
        Self { name: self.name.clone(), quoted: self.quoted, source: self.source }
    }
}

/// The derivable output shape of a relation defined inside the statement.
pub(crate) struct DerivedShape<'db, DB: DatabaseLike> {
    pub(crate) columns: Vec<DerivedColumn<'db, DB>>,
    /// Whether each output row is exactly one row of a source table passed
    /// through: false when the defining body deduplicates (`DISTINCT`),
    /// groups, window-filters, reads through a null-extended outer join, or
    /// combines arms with a set operation other than `ALL`. A filter
    /// (`WHERE`) keeps rows, so it preserves row identity.
    pub(crate) row_preserving: bool,
}

impl<DB: DatabaseLike> Clone for DerivedShape<'_, DB> {
    fn clone(&self) -> Self {
        Self { columns: self.columns.clone(), row_preserving: self.row_preserving }
    }
}
/// A `FROM` relation defined inside the statement (a CTE reference or a
/// derived subquery) whose output columns were enumerated, keyed by the
/// identifier the projection uses to qualify it. A derived subquery written
/// without an alias (allowed since PostgreSQL 16) has no key, so no
/// reference can qualify with it, but its columns still answer bare ones.
pub(crate) struct DerivedRelationRef<'a, 'db, DB: DatabaseLike> {
    pub(crate) key_value: Option<&'a str>,
    pub(crate) key_quoted: bool,
    pub(crate) shape: DerivedShape<'db, DB>,
    pub(crate) nullable: bool,
    pub(crate) entry_index: usize,
}

/// A CTE name and its derived shape. A `None` shape means a reference to it
/// stays opaque: the body is not derivable, or the entry is the placeholder a
/// recursive CTE carries while its own shape is still being derived, which is
/// what stops the recursive arm's self-reference from recursing forever.
struct CteShape<'a, 'db, DB: DatabaseLike> {
    name: &'a str,
    quoted: bool,
    shape: Option<DerivedShape<'db, DB>>,
}

impl<DB: DatabaseLike> Clone for CteShape<'_, '_, DB> {
    fn clone(&self) -> Self {
        Self { name: self.name, quoted: self.quoted, shape: self.shape.clone() }
    }
}
/// A column name merged by a `USING` or `NATURAL` join. `subsumed` is the
/// number of `FROM` entries the merge consumed: relations collected before
/// that boundary pass their exposure of the name into the merged column and
/// no longer count individually, while relations joined in afterwards collide
/// with it, as PostgreSQL reports for a bare reference.
pub(crate) struct MergedName {
    pub(crate) name: String,
    pub(crate) quoted: bool,
    pub(crate) subsumed: usize,
}

/// One output position of a `FROM` item's join chain as a `*` projection
/// sees it: a base relation (index into `FromScope::bases`), a derived
/// relation (index into `FromScope::derived`), or a column merged by a
/// `USING` or `NATURAL` join, whose coalesced value has no single source.
pub(crate) enum WildcardEntry {
    Base(usize),
    Derived(usize),
    Merged { name: String, quoted: bool },
}

/// The `FROM` scope of a query's outer `SELECT`: the resolved base tables,
/// the relations whose columns were derived from their definitions, the
/// column names merged by `USING`/`NATURAL` joins, the number of `FROM`
/// entries seen (opaque ones counted), whether any entry is opaque, and,
/// one per `FROM` item, the plan of entries a `*` projects in PostgreSQL's
/// join output order (a poisoned item stores an empty plan and relies on
/// `has_opaque`).
pub(crate) struct FromScope<'a, 'db, DB: DatabaseLike> {
    pub(crate) bases: Vec<FromTableRef<'a, 'db, DB>>,
    pub(crate) derived: Vec<DerivedRelationRef<'a, 'db, DB>>,
    pub(crate) merged: Vec<MergedName>,
    pub(crate) wildcard_plans: Vec<Vec<WildcardEntry>>,
    pub(crate) from_entry_count: usize,
    pub(crate) has_opaque: bool,
}

/// Collects the `FROM` relations of the query's outer `SELECT` into the
/// resolver's shared shape. Returns `Ok(None)` when the body is not a plain
/// `SELECT`, so a caller has no outer scope to work with.
pub(crate) fn collect_from_clause<'a, 'db, DB: DatabaseLike>(
    query: &'a Query,
    database: &'db DB,
) -> Result<Option<FromScope<'a, 'db, DB>>, LookupError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let cte_scope = match &query.with {
        Some(with) => derive_cte_shapes(with, &[], database)?,
        None => Vec::new(),
    };
    Ok(Some(collect_select_from(select, &cte_scope, database)?))
}

/// Collects one `SELECT`'s `FROM` relations against a CTE scope. Join chains
/// are walked left-associatively: `USING`/`NATURAL` column names are recorded
/// as merged (the join output carries each once as a coalesced value with no
/// single source), and entries on the null-extended side of an outer join are
/// marked `nullable`. Each `FROM` item also gets the plan a `*` projects:
/// PostgreSQL emits a join's merged columns first, then the accumulated
/// side's remaining entries, then the joined relation's, so the plan is
/// rebuilt per join as `merged ++ left-remaining ++ right-remaining` and the
/// latest merge ends up first. Operators this resolver does not model reach
/// opaqueness through `collect_factor` (a nested join factor, a table
/// function). An unrecognized operator itself null-extends nothing.
fn collect_select_from<'a, 'db, DB: DatabaseLike>(
    select: &'a Select,
    cte_scope: &[CteShape<'a, 'db, DB>],
    database: &'db DB,
) -> Result<FromScope<'a, 'db, DB>, LookupError> {
    let mut scope = FromScope {
        bases: Vec::new(),
        derived: Vec::new(),
        merged: Vec::new(),
        wildcard_plans: Vec::new(),
        from_entry_count: 0,
        has_opaque: false,
    };
    for table_with_joins in &select.from {
        // Null-extension marks only this `FROM` item's own relations: a comma
        // is a cross join, so `FROM a, b RIGHT JOIN c` leaves `a` intact.
        let entry_bases = scope.bases.len();
        let entry_derived = scope.derived.len();
        let (mut accumulated, mut plan) =
            match collect_factor(&table_with_joins.relation, database, cte_scope, &mut scope)? {
                Some((names, entries)) => (Some(names), Some(entries)),
                None => (None, None),
            };
        for join in &table_with_joins.joins {
            let bases_before = scope.bases.len();
            let derived_before = scope.derived.len();
            let (right_names, right_entries) =
                match collect_factor(&join.relation, database, cte_scope, &mut scope)? {
                    Some((names, entries)) => (Some(names), Some(entries)),
                    None => (None, None),
                };
            let (left_nullable, right_nullable) = nullable_sides(&join.join_operator);
            if left_nullable {
                for base in &mut scope.bases[entry_bases..bases_before] {
                    base.nullable = true;
                }
                for relation in &mut scope.derived[entry_derived..derived_before] {
                    relation.nullable = true;
                }
            }
            if right_nullable {
                for base in &mut scope.bases[bases_before..] {
                    base.nullable = true;
                }
                for relation in &mut scope.derived[derived_before..] {
                    relation.nullable = true;
                }
            }
            let mut merged = Vec::new();
            if let Some(names) =
                merge_names(&join.join_operator, accumulated.as_deref(), right_names.as_deref())
            {
                for (name, quoted) in &names {
                    merge_name(&mut scope.merged, name.clone(), *quoted, scope.from_entry_count);
                }
                merged = names;
            } else {
                scope.has_opaque = true;
            }
            plan = merge_plans(plan, right_entries, &merged);
            accumulated = merge_output_names(accumulated, right_names, &merged);
        }
        scope.wildcard_plans.push(plan.unwrap_or_default());
    }
    Ok(scope)
}

/// Whether each side of a join can be null-extended: `(left, right)`. An
/// inner, cross, semi, or anti join extends nothing. `LEFT` null-extends the
/// joined relation, `RIGHT` the accumulated relation, `FULL` both, and
/// `OUTER APPLY` the applied relation.
fn nullable_sides(operator: &JoinOperator) -> (bool, bool) {
    match operator {
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) | JoinOperator::OuterApply => {
            (false, true)
        }
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => (true, false),
        JoinOperator::FullOuter(_) => (true, true),
        _ => (false, false),
    }
}

/// The column names a join merges: a `USING` list directly, a `NATURAL`
/// join as the intersection of the two sides' output names, each name once.
/// A `NATURAL` join whose sides' columns cannot be enumerated reports `None`
/// so the caller marks the scope opaque rather than guessing that a shared
/// name is unmerged.
fn merge_names(
    operator: &JoinOperator,
    left: Option<&[(String, bool)]>,
    right: Option<&[(String, bool)]>,
) -> Option<Vec<(String, bool)>> {
    let mut merged: Vec<(String, bool)> = Vec::new();
    match join_constraint(operator) {
        None | Some(JoinConstraint::On(_) | JoinConstraint::None) => {}
        Some(JoinConstraint::Using(names)) => {
            for name in names {
                if let Some((value, quoted)) = object_name_last_part(name) {
                    push_unseen(&mut merged, value, quoted);
                }
            }
        }
        Some(JoinConstraint::Natural) => {
            let (Some(left_names), Some(right_names)) = (left, right) else {
                return None;
            };
            for (name, quoted) in left_names {
                if right_names.iter().any(|(other, other_quoted)| {
                    identifiers_match(other, *other_quoted, name, *quoted)
                }) {
                    push_unseen(&mut merged, name, *quoted);
                }
            }
        }
    }
    Some(merged)
}

/// Appends an identifier unless the list already carries it.
fn push_unseen(names: &mut Vec<(String, bool)>, value: &str, quoted: bool) {
    if !names.iter().any(|(name, known)| identifiers_match(name, *known, value, quoted)) {
        names.push((value.to_string(), quoted));
    }
}

/// The wildcard plan after one join, in PostgreSQL's join output order: the
/// merged columns (in list order), then the accumulated plan's entries that
/// are not an earlier `Merged` entry of the same name, then the joined
/// relation's. A re-merge of a name therefore drops the older merged entry
/// and re-prepends it at the latest join's position. A poisoned side poisons
/// the item's whole plan.
fn merge_plans(
    left: Option<Vec<WildcardEntry>>,
    right: Option<Vec<WildcardEntry>>,
    merged: &[(String, bool)],
) -> Option<Vec<WildcardEntry>> {
    let (left, right) = (left?, right?);
    let mut next: Vec<WildcardEntry> = merged
        .iter()
        .map(|(name, quoted)| WildcardEntry::Merged { name: name.clone(), quoted: *quoted })
        .collect();
    let remerged = |entry: &WildcardEntry| {
        matches!(entry, WildcardEntry::Merged { name, quoted }
        if merged.iter().any(|(merged, merged_quoted)| {
            identifiers_match(merged, *merged_quoted, name, *quoted)
        }))
    };
    next.extend(left.into_iter().filter(|entry| !remerged(entry)));
    next.extend(right.into_iter().filter(|entry| !remerged(entry)));
    Some(next)
}

/// The accumulated output names after one join: the merged names first, then
/// each side's names that are not merged. Membership matches the names before
/// the join (a merged name still stands, as the join's own coalesced column),
/// so `NATURAL` intersections are unchanged. Only the order follows the
/// output. A poisoned side poisons the accumulated list.
fn merge_output_names(
    left: Option<Vec<(String, bool)>>,
    right: Option<Vec<(String, bool)>>,
    merged: &[(String, bool)],
) -> Option<Vec<(String, bool)>> {
    let (left, right) = (left?, right?);
    let unmerged = |entry: &&(String, bool)| {
        !merged.iter().any(|(merged, merged_quoted)| {
            identifiers_match(merged, *merged_quoted, &entry.0, entry.1)
        })
    };
    let mut names: Vec<(String, bool)> = merged.to_vec();
    names.extend(left.iter().filter(unmerged).cloned());
    names.extend(right.iter().filter(unmerged).cloned());
    Some(names)
}

/// The constraint carried by a join operator, if it carries one.
fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint) => Some(constraint),
        _ => None,
    }
}

/// Records a merged name, moving its subsumption boundary to the latest
/// merge of that name.
fn merge_name(merged: &mut Vec<MergedName>, name: String, quoted: bool, subsumed: usize) {
    match merged
        .iter_mut()
        .find(|merged| identifiers_match(&merged.name, merged.quoted, &name, quoted))
    {
        Some(existing) => existing.subsumed = subsumed,
        None => merged.push(MergedName { name, quoted, subsumed }),
    }
}

/// The subsumption boundary for this identifier if it is merged.
fn merged_boundary(merged: &[MergedName], name: &str, quoted: bool) -> Option<usize> {
    merged
        .iter()
        .find(|merged| identifiers_match(&merged.name, merged.quoted, name, quoted))
        .map(|merged| merged.subsumed)
}

/// Whether a relation's own copy of this column name is absorbed into a
/// merged join column: true when the relation was collected before that
/// name's merge.
fn subsumed_exposure(merged: &[MergedName], entry_index: usize, name: &str, quoted: bool) -> bool {
    merged_boundary(merged, name, quoted).is_some_and(|boundary| entry_index < boundary)
}

/// Derives the shapes of a `WITH` clause's CTEs, in order, appended to the
/// enclosing CTE scope. A recursive list binds every name before any body is
/// resolved (PostgreSQL resolves a forward reference inside `WITH RECURSIVE`
/// to its sibling, not to a same-named base table), so all placeholders go in
/// first and forward references stay opaque instead of answering a shadowed
/// base table. A non-recursive list registers names one by one, matching
/// PostgreSQL, where a forward reference can only reach a base table.
fn derive_cte_shapes<'a, 'db, DB: DatabaseLike>(
    with: &'a With,
    outer: &[CteShape<'a, 'db, DB>],
    database: &'db DB,
) -> Result<Vec<CteShape<'a, 'db, DB>>, LookupError> {
    let mut shapes: Vec<CteShape<'a, 'db, DB>> = outer.to_vec();
    let base = shapes.len();
    if with.recursive {
        for cte in &with.cte_tables {
            shapes.push(CteShape {
                name: cte.alias.name.value.as_str(),
                quoted: cte.alias.name.quote_style.is_some(),
                shape: None,
            });
        }
    }
    for (index, cte) in with.cte_tables.iter().enumerate() {
        let position = if with.recursive { base + index } else { shapes.len() };
        // The recursive arm names the CTE while its own shape is unknown, so
        // the entry starts opaque and only becomes derivable after its body.
        if !with.recursive {
            shapes.push(CteShape {
                name: cte.alias.name.value.as_str(),
                quoted: cte.alias.name.quote_style.is_some(),
                shape: None,
            });
        }
        let body = derive_query_shape(&cte.query, &shapes, database)?;
        shapes[position].shape = apply_alias_columns(body, &cte.alias);
    }
    Ok(shapes)
}

/// Derives the output shape of a query used as a relation body. Returns
/// `Ok(None)` when the columns cannot be enumerated.
fn derive_query_shape<'a, 'db, DB: DatabaseLike>(
    query: &'a Query,
    cte_scope: &[CteShape<'a, 'db, DB>],
    database: &'db DB,
) -> Result<Option<DerivedShape<'db, DB>>, LookupError> {
    let scoped;
    let scope = match &query.with {
        Some(with) => {
            scoped = derive_cte_shapes(with, cte_scope, database)?;
            &scoped
        }
        None => cte_scope,
    };
    derive_set_expr_shape(&query.body, scope, database)
}

/// Derives the output shape of a body. A set operation merges its arms by
/// ordinal position: names come from the left arm (as in PostgreSQL) and a
/// column keeps a source only while the arms agree on one.
fn derive_set_expr_shape<'a, 'db, DB: DatabaseLike>(
    body: &'a SetExpr,
    cte_scope: &[CteShape<'a, 'db, DB>],
    database: &'db DB,
) -> Result<Option<DerivedShape<'db, DB>>, LookupError> {
    match body {
        SetExpr::Select(select) => derive_select_shape(select, cte_scope, database),
        SetExpr::Query(query) => derive_query_shape(query, cte_scope, database),
        SetExpr::SetOperation { left, right, set_quantifier, .. } => {
            let Some(left_shape) = derive_set_expr_shape(left, cte_scope, database)? else {
                return Ok(None);
            };
            let Some(right_shape) = derive_set_expr_shape(right, cte_scope, database)? else {
                return Ok(None);
            };
            if left_shape.columns.len() != right_shape.columns.len() {
                return Ok(None);
            }
            let columns = left_shape
                .columns
                .iter()
                .zip(right_shape.columns.iter())
                .map(|(left, right)| {
                    DerivedColumn {
                        name: left.name.clone(),
                        quoted: left.quoted,
                        source: match (left.source, right.source) {
                            (Some(left_table), Some(right_table))
                                if database.table_id(left_table)
                                    == database.table_id(right_table) =>
                            {
                                Some(left_table)
                            }
                            _ => None,
                        },
                    }
                })
                .collect();
            Ok(Some(DerivedShape {
                columns,
                // Only an `ALL` set operation preserves row identity, and only
                // when both arms do: a deduplicating `UNION` collapses rows,
                // and `INTERSECT`/`EXCEPT` drop them.
                row_preserving: matches!(set_quantifier, SetQuantifier::All)
                    && left_shape.row_preserving
                    && right_shape.row_preserving,
            }))
        }
        // `VALUES` and `TABLE` bodies name their columns by rules this
        // resolver does not model.
        _ => Ok(None),
    }
}

/// Expands a `*` projection by materializing each `FROM` item's plan in
/// PostgreSQL's join output order. A relation contributes its own columns
/// except those a merge absorbed (relations collected before that name's
/// merge). A merged name stands once per join with no source, and a repeated
/// merge of the same name stands once at its latest position.
fn push_wildcard_columns<'db, DB: DatabaseLike>(
    scope: &FromScope<'_, 'db, DB>,
    columns: &mut Vec<DerivedColumn<'db, DB>>,
) {
    for plan in &scope.wildcard_plans {
        for entry in plan {
            match entry {
                WildcardEntry::Base(index) => {
                    let base = &scope.bases[*index];
                    columns.extend(base_columns(base).into_iter().filter(|column| {
                        !subsumed_exposure(
                            &scope.merged,
                            base.entry_index,
                            &column.name,
                            column.quoted,
                        )
                    }));
                }
                WildcardEntry::Derived(index) => {
                    let relation = &scope.derived[*index];
                    columns.extend(
                        relation
                            .shape
                            .columns
                            .iter()
                            .filter(|column| {
                                !subsumed_exposure(
                                    &scope.merged,
                                    relation.entry_index,
                                    &column.name,
                                    column.quoted,
                                )
                            })
                            .cloned(),
                    );
                }
                WildcardEntry::Merged { name, quoted } => {
                    columns.push(DerivedColumn {
                        name: name.clone(),
                        quoted: *quoted,
                        source: None,
                    });
                }
            }
        }
    }
}

/// Resource guard for pathological `SELECT *` chains: a derivation whose
/// output exceeds this width becomes opaque rather than materializing more
/// columns. Without it, sibling `WITH` items that each reference the previous
/// one twice double the width every level. No ordinary query comes close:
/// PostgreSQL itself refuses target lists beyond roughly 1600 entries.
const MAX_DERIVED_COLUMNS: usize = 4096;

/// Whether a wildcard carries options that change what it outputs: `EXCLUDE`,
/// `EXCEPT` and `ILIKE` drop columns, `RENAME` relabels them, `REPLACE`
/// substitutes a computed value for one, and Redshift's trailing alias names
/// the expansion. None of those output sets are enumerated here, so a body
/// projecting such a wildcard stays opaque rather than claiming columns the
/// relation does not output.
fn wildcard_reshapes_output(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
        || options.opt_alias.is_some()
}

/// Derives the output shape of a plain `SELECT`: each projected column's name
/// and pass-through source, enumerated from the projection (wildcards expand
/// over the `FROM` relations they stand for).
fn derive_select_shape<'a, 'db, DB: DatabaseLike>(
    select: &'a Select,
    cte_scope: &[CteShape<'a, 'db, DB>],
    database: &'db DB,
) -> Result<Option<DerivedShape<'db, DB>>, LookupError> {
    // Dialect forms whose output columns this resolver does not enumerate.
    if !select.lateral_views.is_empty()
        || select.exclude.is_some()
        || select.value_table_mode.is_some()
        || !select.connect_by.is_empty()
    {
        return Ok(None);
    }
    let scope = collect_select_from(select, cte_scope, database)?;
    let mut columns: Vec<DerivedColumn<'db, DB>> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                // A computed item written without an alias has no output name
                // this resolver models, so the whole relation stays opaque.
                // A three-part reference carries PostgreSQL's label (the
                // trailing column name) once it matches a base table.
                let named = match projected_column_name(expr) {
                    Some(name) => Some(name),
                    None => three_part_output_name(expr, &scope.bases),
                };
                let Some((name, quoted)) = named else {
                    return Ok(None);
                };
                let source = match column_source(
                    expr,
                    &scope.bases,
                    &scope.derived,
                    &scope.merged,
                    scope.has_opaque,
                ) {
                    Ok(source) => source,
                    // A body-internal ambiguity means this output column names
                    // no single source. The reference itself can still report
                    // the ambiguity when a caller asks it directly.
                    Err(LookupError::AmbiguousTableLookup { .. }) => None,
                    Err(error) => return Err(error),
                };
                columns.push(DerivedColumn { name, quoted, source });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let source = match column_source(
                    expr,
                    &scope.bases,
                    &scope.derived,
                    &scope.merged,
                    scope.has_opaque,
                ) {
                    Ok(source) => source,
                    Err(LookupError::AmbiguousTableLookup { .. }) => None,
                    Err(error) => return Err(error),
                };
                columns.push(DerivedColumn {
                    name: alias.value.clone(),
                    quoted: alias.quote_style.is_some(),
                    source,
                });
            }
            SelectItem::ExprWithAliases { .. } => return Ok(None),
            SelectItem::Wildcard(options) => {
                if scope.has_opaque || wildcard_reshapes_output(options) {
                    return Ok(None);
                }
                push_wildcard_columns(&scope, &mut columns);
            }
            SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(object_name),
                options,
            ) => {
                if wildcard_reshapes_output(options) {
                    return Ok(None);
                }
                let Some(expansion) =
                    expand_qualified_wildcard(&scope.bases, &scope.derived, object_name)
                else {
                    return Ok(None);
                };
                columns.extend(expansion);
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _) => {
                return Ok(None);
            }
        }
    }
    let grouped = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    // A body reading through a null-extended outer join has output rows with
    // no source row, so it does not preserve row identity either.
    let outer_join = select.from.iter().flat_map(|entry| &entry.joins).any(|join| {
        let (left_nullable, right_nullable) = nullable_sides(&join.join_operator);
        left_nullable || right_nullable
    });
    // A body reading a relation whose own rows are not source rows cannot
    // preserve row identity either: its rows are that relation's rows.
    let reads_non_preserving = scope.derived.iter().any(|relation| !relation.shape.row_preserving);
    let row_preserving = select.distinct.is_none()
        && select.having.is_none()
        && select.qualify.is_none()
        && !grouped
        && !outer_join
        && !reads_non_preserving;
    if columns.len() > MAX_DERIVED_COLUMNS {
        return Ok(None);
    }
    Ok(Some(DerivedShape { columns, row_preserving }))
}

/// The output name and quoting a pass-through column projection writes, or
/// `None` for an expression that is not a column reference.
fn projected_column_name(expr: &Expr) -> Option<(String, bool)> {
    match expr {
        Expr::Identifier(ident) => Some((ident.value.clone(), ident.quote_style.is_some())),
        // Only the two-part `qualifier.column` shape names a pass-through
        // column outright. A three-part `schema.table.column` reference is
        // named separately, once its schema is checked against a base
        // table, and anything longer has no name derived here.
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let last = &parts[1];
            Some((last.value.clone(), last.quote_style.is_some()))
        }
        _ => None,
    }
}

/// The output name a `schema.table.column` projection writes: PostgreSQL
/// labels such a column with the trailing name, but only when the reference
/// names a base table in that schema. An unresolvable reference belongs to a
/// statement PostgreSQL would reject, so it yields no name and the body
/// stays opaque rather than inventing an output column.
fn three_part_output_name<DB: DatabaseLike>(
    expr: &Expr,
    bases: &[FromTableRef<'_, '_, DB>],
) -> Option<(String, bool)> {
    let Expr::CompoundIdentifier(parts) = expr else {
        return None;
    };
    if parts.len() != 3 {
        return None;
    }
    let base = base_for_qualified_name(
        bases,
        parts[0].value.as_str(),
        parts[0].quote_style.is_some(),
        parts[1].value.as_str(),
        parts[1].quote_style.is_some(),
        false,
    )?;
    base_exposes_column(base, &parts[2])
        .then(|| (parts[2].value.clone(), parts[2].quote_style.is_some()))
}

/// Every column a base relation outputs as a derived column sourced by that
/// table: the relation's `output_names` in declaration order, so an alias
/// column list renames the wildcard expansion too.
fn base_columns<'db, DB: DatabaseLike>(
    base: &FromTableRef<'_, 'db, DB>,
) -> Vec<DerivedColumn<'db, DB>> {
    base.output_names
        .iter()
        .map(|(name, quoted)| {
            DerivedColumn { name: name.clone(), quoted: *quoted, source: Some(base.table) }
        })
        .collect()
}

/// The relation a qualified wildcard prefix names.
enum WildcardTarget<'s, 'a, 'db, DB: DatabaseLike> {
    Base(&'s FromTableRef<'a, 'db, DB>),
    Derived(&'s DerivedRelationRef<'a, 'db, DB>),
}

/// Resolves a qualified wildcard's prefix to the relation it names, sharing
/// the qualified column reference rules: a one-part prefix matches a base
/// relation's alias (or table name) first, then a derived relation's key. A
/// two-part prefix must name a base relation whose own schema equals the
/// leading part, because a CTE or derived relation has no schema (PostgreSQL
/// rejects `schema.cte.*`). A prefix of three or more parts matches nothing:
/// PostgreSQL accepts an exact `database.schema.table.*`, but database names
/// are not modeled, so that leading part can never be verified. With
/// `require_row_identity`, null-extended base relations and non-row-preserving
/// derived relations are excluded.
fn resolve_wildcard_target<'s, 'a, 'db, DB: DatabaseLike>(
    bases: &'s [FromTableRef<'a, 'db, DB>],
    derived: &'s [DerivedRelationRef<'a, 'db, DB>],
    qualifier: &ObjectName,
    require_row_identity: bool,
) -> Option<WildcardTarget<'s, 'a, 'db, DB>> {
    let (value, quoted) = object_name_last_part(qualifier)?;
    match qualifier.0.len() {
        1 => {
            base_for_qualifier(bases, value, quoted, require_row_identity)
                .map(WildcardTarget::Base)
                .or_else(|| {
                    derived_for_qualifier(derived, value, quoted, require_row_identity)
                        .map(WildcardTarget::Derived)
                })
        }
        2 => {
            let (schema, schema_quoted) = schema_from_object_name(qualifier)?;
            base_for_qualified_name(
                bases,
                schema,
                schema_quoted,
                value,
                quoted,
                require_row_identity,
            )
            .map(WildcardTarget::Base)
        }
        _ => None,
    }
}

/// The columns a qualified wildcard (`alias.*`) stands for: a base relation
/// expands to its `output_names` (alias-list renamed when the `FROM` clause
/// wrote one), a derived relation to its shape's columns. `None` when the
/// prefix matches nothing.
fn expand_qualified_wildcard<'s, 'a, 'db, DB: DatabaseLike>(
    bases: &'s [FromTableRef<'a, 'db, DB>],
    derived: &'s [DerivedRelationRef<'a, 'db, DB>],
    qualifier: &ObjectName,
) -> Option<Vec<DerivedColumn<'db, DB>>> {
    match resolve_wildcard_target(bases, derived, qualifier, false)? {
        WildcardTarget::Base(base) => Some(base_columns(base)),
        WildcardTarget::Derived(relation) => Some(relation.shape.columns.clone()),
    }
}

/// Renames a derived shape's columns positionally by a table alias's column
/// list (`v(x)` or `(SELECT ...) s(x)`). More aliases than output columns is
/// a mismatch PostgreSQL rejects, so the shape becomes non-derivable.
fn apply_alias_columns<'db, DB: DatabaseLike>(
    shape: Option<DerivedShape<'db, DB>>,
    alias: &TableAlias,
) -> Option<DerivedShape<'db, DB>> {
    let mut shape = shape?;
    if alias.columns.is_empty() {
        return Some(shape);
    }
    if alias.columns.len() > shape.columns.len() {
        return None;
    }
    for (column, alias_column) in shape.columns.iter_mut().zip(&alias.columns) {
        alias_column.name.value.clone_into(&mut column.name);
        column.quoted = alias_column.name.quote_style.is_some();
    }
    Some(shape)
}

/// Finds the scope entry for a bare, single-part relation name, innermost
/// scope first so a nested `WITH` shadows an outer one.
fn find_cte<'a, 'db, 's, DB: DatabaseLike>(
    name: &ObjectName,
    cte_scope: &'s [CteShape<'a, 'db, DB>],
) -> Option<&'s CteShape<'a, 'db, DB>> {
    if name.0.len() != 1 {
        return None;
    }
    let (value, quoted) = object_name_last_part(name)?;
    cte_scope.iter().rev().find(|cte| identifiers_match(cte.name, cte.quoted, value, quoted))
}

/// Returns whether a base relation exposes a column matching `column` under
/// the names it outputs (an alias column list replaces the originals),
/// applying PostgreSQL identifier semantics (including the reference
/// identifier's own quoting).
fn base_exposes_column(base: &FromTableRef<'_, '_, impl DatabaseLike>, column: &Ident) -> bool {
    base.output_names.iter().any(|(name, quoted)| {
        identifiers_match(name, *quoted, column.value.as_str(), column.quote_style.is_some())
    })
}

/// Finds the base relation whose qualifying key matches `value`/`quoted`.
/// With `require_row_identity`, entries on the null-extended side of an outer
/// join are excluded.
fn base_for_qualifier<'s, 'a, 'db, DB: DatabaseLike>(
    bases: &'s [FromTableRef<'a, 'db, DB>],
    value: &str,
    quoted: bool,
    require_row_identity: bool,
) -> Option<&'s FromTableRef<'a, 'db, DB>> {
    bases
        .iter()
        .filter(|base| !require_row_identity || !base.nullable)
        .find(|base| identifiers_match(base.key_value, base.key_quoted, value, quoted))
}

/// Finds the base relation a `schema.table` prefix names, shared by
/// `schema.table.column` references and two-part qualified wildcards.
/// PostgreSQL matches the relation by its alias (or name when unaliased) and
/// requires the leading part to equal the table's own schema. A table stored
/// without a schema lives in `public`, and PostgreSQL accepts either spelling
/// of that schema name. A CTE or derived relation has no schema, so
/// PostgreSQL rejects the reference outright and there is nothing to find.
/// With `require_row_identity`, entries on the null-extended side of an outer
/// join are excluded.
fn base_for_qualified_name<'s, 'a, 'db, DB: DatabaseLike>(
    bases: &'s [FromTableRef<'a, 'db, DB>],
    schema: &str,
    schema_quoted: bool,
    table_part: &str,
    table_quoted: bool,
    require_row_identity: bool,
) -> Option<&'s FromTableRef<'a, 'db, DB>> {
    bases.iter().filter(|base| !require_row_identity || !base.nullable).find(|base| {
        let schema_matches = match base.table.table_schema() {
            Some(stored) => {
                identifiers_match(
                    stored,
                    base.table.table_schema_is_quoted(),
                    schema,
                    schema_quoted,
                )
            }
            None => identifiers_match("public", false, schema, schema_quoted),
        };
        identifiers_match(base.key_value, base.key_quoted, table_part, table_quoted)
            && schema_matches
    })
}

/// Finds the derived relation whose qualifying key matches `value`/`quoted`.
/// An anonymous derived subquery has no key and never matches. With
/// `require_row_identity`, only relations whose bodies preserve row
/// identity qualify.
fn derived_for_qualifier<'a, 'db, 's, DB: DatabaseLike>(
    derived: &'s [DerivedRelationRef<'a, 'db, DB>],
    value: &str,
    quoted: bool,
    require_row_identity: bool,
) -> Option<&'s DerivedRelationRef<'a, 'db, DB>> {
    derived
        .iter()
        .find(|relation| {
            relation
                .key_value
                .is_some_and(|key| identifiers_match(key, relation.key_quoted, value, quoted))
        })
        .filter(|relation| {
            !require_row_identity || (relation.shape.row_preserving && !relation.nullable)
        })
}

/// Where a derivable relation's output column passes through from.
/// `Computed` covers output columns not passed through from a base table:
/// aggregates, literals, or columns sourced from a relation that itself
/// answers nothing. `Ambiguous` means the relation exposes this name in two
/// or more output columns, which PostgreSQL refuses to resolve.
enum PassThrough<'db, DB: DatabaseLike> {
    Table(&'db DB::Table),
    Computed,
    Ambiguous,
}

/// The source of a derived relation's output column named `column`, or `None`
/// when the relation has no column of that name.
fn find_derived_column<'db, DB: DatabaseLike>(
    columns: &[DerivedColumn<'db, DB>],
    column: &Ident,
) -> Option<PassThrough<'db, DB>> {
    let mut matches = columns.iter().filter(|candidate| {
        identifiers_match(
            &candidate.name,
            candidate.quoted,
            column.value.as_str(),
            column.quote_style.is_some(),
        )
    });
    let first = matches.next()?;
    if matches.next().is_some() {
        return Some(PassThrough::Ambiguous);
    }
    Some(first.source.map_or(PassThrough::Computed, PassThrough::Table))
}

/// The single base table every output column of a derived relation comes
/// from, when the relation preserves row identity and every column is a
/// pass-through of that same table.
fn single_source_relation<'db, DB: DatabaseLike>(
    relation: &DerivedRelationRef<'_, 'db, DB>,
    database: &DB,
) -> Option<&'db DB::Table> {
    if !relation.shape.row_preserving {
        return None;
    }
    let first = relation.shape.columns.first()?.source?;
    relation
        .shape
        .columns
        .iter()
        .all(|column| {
            column.source.is_some_and(|table| database.table_id(table) == database.table_id(first))
        })
        .then_some(first)
}

/// Renders a derived relation's qualifying key for an ambiguity candidate
/// list, quoting it when the SQL wrote it quoted, or naming an anonymous
/// derived subquery for a candidate list.
fn relation_key_display(relation: &DerivedRelationRef<'_, '_, impl DatabaseLike>) -> String {
    match relation.key_value {
        Some(key) if relation.key_quoted => format!("\"{key}\""),
        Some(key) => key.to_string(),
        None => "(subquery)".to_string(),
    }
}

/// Resolves an unqualified column to the single `FROM` relation that exposes
/// it, answering that relation's pass-through source. A `USING`/`NATURAL`
/// merged name counts as one exposure with no source, because the coalesced
/// column belongs to no single table. With `require_row_identity`, entries on
/// the null-extended side of an outer join answer nothing.
fn unqualified_column_source<'db, DB: DatabaseLike>(
    bases: &[FromTableRef<'_, 'db, DB>],
    derived: &[DerivedRelationRef<'_, 'db, DB>],
    merged: &[MergedName],
    has_opaque: bool,
    require_row_identity: bool,
    column: &Ident,
) -> Result<Option<&'db DB::Table>, LookupError> {
    // An opaque relation might also expose the column, and its columns cannot
    // be enumerated, so a single answer cannot be claimed.
    if has_opaque {
        return Ok(None);
    }

    let mut sources: Vec<Option<&'db DB::Table>> = Vec::new();
    let mut candidates: Vec<String> = Vec::new();
    // The merge's output carries the name once and its coalesced value
    // belongs to no table. Relations collected before the merge boundary
    // passed their exposure into the merged column and no longer count on
    // their own. Relations joined in afterwards still collide with it.
    let boundary = merged_boundary(merged, column.value.as_str(), column.quote_style.is_some());
    if boundary.is_some() {
        sources.push(None);
    }
    let start = boundary.unwrap_or(0);
    for base in bases.iter().filter(|base| base.entry_index >= start) {
        if base_exposes_column(base, column) {
            // A null-extended side contributes no row identity, but its
            // name exposure still makes a bare reference ambiguous, as in
            // PostgreSQL.
            if require_row_identity && base.nullable {
                sources.push(None);
            } else {
                sources.push(Some(base.table));
            }
            candidates.push(render_table_candidate(base.table));
        }
    }
    for relation in derived.iter().filter(|relation| relation.entry_index >= start) {
        match find_derived_column(&relation.shape.columns, column) {
            Some(PassThrough::Table(table)) => {
                if require_row_identity && relation.nullable {
                    sources.push(None);
                } else {
                    sources.push(Some(table));
                }
                candidates.push(render_table_candidate(table));
            }
            Some(PassThrough::Computed) => {
                sources.push(None);
                candidates.push(relation_key_display(relation));
            }
            // The relation itself exposes the name twice, which is
            // ambiguous even as the only exposure.
            Some(PassThrough::Ambiguous) => {
                sources.push(None);
                sources.push(None);
                candidates.push(relation_key_display(relation));
            }
            None => {}
        }
    }

    match sources.as_slice() {
        [] => Ok(None),
        [source] => Ok(*source),
        _ => {
            // Two relations expose the name, the PostgreSQL `column reference
            // is ambiguous` case, whether or not either side is computed.
            candidates.sort_unstable();
            candidates.dedup();
            Err(LookupError::AmbiguousTableLookup { object_name: column.value.clone(), candidates })
        }
    }
}

/// Resolves a single column reference to the base table it passes through.
/// A pass-through reaches into derived relations: a column of a CTE reference
/// or a derived subquery answers the base table its projection passes the
/// column through from, or nothing when the output column is computed or the
/// set-operation arms disagree. With `require_row_identity`, a derived
/// relation only answers through a body that preserves row identity, and a
/// null-extended outer-join side never answers. A `schema.table.column`
/// reference resolves like the two-part form after checking the leading part
/// against the base table's own schema, as PostgreSQL does, and answers
/// nothing for a CTE, a derived relation, or a schema mismatch. A reference
/// of four or more parts answers nothing: the database part is not modeled.
fn resolve_source<'db, DB: DatabaseLike>(
    expr: &Expr,
    bases: &[FromTableRef<'_, 'db, DB>],
    derived: &[DerivedRelationRef<'_, 'db, DB>],
    merged: &[MergedName],
    has_opaque: bool,
    require_row_identity: bool,
) -> Result<Option<&'db DB::Table>, LookupError> {
    match expr {
        Expr::Identifier(column) => {
            unqualified_column_source(
                bases,
                derived,
                merged,
                has_opaque,
                require_row_identity,
                column,
            )
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let column = &parts[1];
            let qualifier = &parts[0];
            let value = qualifier.value.as_str();
            let quoted = qualifier.quote_style.is_some();
            if let Some(base) = base_for_qualifier(bases, value, quoted, require_row_identity) {
                if base_exposes_column(base, column) {
                    return Ok(Some(base.table));
                }
                return Ok(None);
            }
            let Some(relation) =
                derived_for_qualifier(derived, value, quoted, require_row_identity)
            else {
                return Ok(None);
            };
            match find_derived_column(&relation.shape.columns, column) {
                Some(PassThrough::Table(table)) => Ok(Some(table)),
                Some(PassThrough::Ambiguous) => {
                    Err(LookupError::AmbiguousTableLookup {
                        object_name: column.value.clone(),
                        candidates: vec![relation_key_display(relation)],
                    })
                }
                Some(PassThrough::Computed) | None => Ok(None),
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 3 => {
            let Some(base) = base_for_qualified_name(
                bases,
                parts[0].value.as_str(),
                parts[0].quote_style.is_some(),
                parts[1].value.as_str(),
                parts[1].quote_style.is_some(),
                require_row_identity,
            ) else {
                return Ok(None);
            };
            if base_exposes_column(base, &parts[2]) { Ok(Some(base.table)) } else { Ok(None) }
        }
        _ => Ok(None),
    }
}

/// Resolves a single projected expression to the base table it passes
/// through, or `Ok(None)` when it is not a pass-through column.
pub(crate) fn column_source<'db, DB: DatabaseLike>(
    expr: &Expr,
    bases: &[FromTableRef<'_, 'db, DB>],
    derived: &[DerivedRelationRef<'_, 'db, DB>],
    merged: &[MergedName],
    has_opaque: bool,
) -> Result<Option<&'db DB::Table>, LookupError> {
    resolve_source(expr, bases, derived, merged, has_opaque, false)
}

/// Resolves a projected expression for the row-identity question: a derived
/// relation carries an answer only through a row-preserving body, a
/// null-extended outer-join side never carries one, and any non-preserving
/// relation poisons unqualified references.
pub(crate) fn row_identity_source<'db, DB: DatabaseLike>(
    expr: &Expr,
    bases: &[FromTableRef<'_, 'db, DB>],
    derived: &[DerivedRelationRef<'_, 'db, DB>],
    merged: &[MergedName],
    has_opaque: bool,
) -> Result<Option<&'db DB::Table>, LookupError> {
    resolve_source(expr, bases, derived, merged, has_opaque, true)
}

/// What one `FROM` factor contributes: the output names it exposes and the
/// wildcard plan entry naming what it pushed.
type FactorContribution = (Vec<(String, bool)>, Vec<WildcardEntry>);

/// A base table's output names with the alias column list applied
/// positionally. PostgreSQL replaces the originals with the aliases (a
/// partial list keeps the tail's own names). More aliases than columns is a
/// mismatch PostgreSQL rejects, reported here as `None` so the relation
/// stays opaque.
fn aliased_output_names<'db, DB: DatabaseLike>(
    table: &'db DB::Table,
    alias: Option<&TableAlias>,
    database: &'db DB,
) -> Result<Option<Vec<(String, bool)>>, LookupError> {
    let mut output_names: Vec<(String, bool)> = table
        .columns(database)?
        .map(|column| (column.column_name().to_string(), column.column_name_is_quoted()))
        .collect();
    if let Some(table_alias) = alias
        && !table_alias.columns.is_empty()
    {
        if table_alias.columns.len() > output_names.len() {
            return Ok(None);
        }
        for (column, alias_column) in output_names.iter_mut().zip(&table_alias.columns) {
            alias_column.name.value.clone_into(&mut column.0);
            column.1 = alias_column.name.quote_style.is_some();
        }
    }
    Ok(Some(output_names))
}

/// Records a single `FROM` table factor into the scope, returning the output
/// column names it contributes and the wildcard plan entry naming what it
/// pushed, or `None` when the factor is opaque.
fn collect_factor<'a, 'db, DB: DatabaseLike>(
    factor: &'a TableFactor,
    database: &'db DB,
    cte_scope: &[CteShape<'a, 'db, DB>],
    scope: &mut FromScope<'a, 'db, DB>,
) -> Result<Option<FactorContribution>, LookupError> {
    let entry_index = scope.from_entry_count;
    scope.from_entry_count += 1;
    let names_of = |shape: &DerivedShape<'db, DB>| {
        shape
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.quoted))
            .collect::<Vec<(String, bool)>>()
    };
    match factor {
        TableFactor::Table { name, alias, args, .. } => {
            let (key_value, key_quoted) = match alias {
                Some(table_alias) => {
                    (table_alias.name.value.as_str(), table_alias.name.quote_style.is_some())
                }
                None => object_name_last_part(name).unwrap_or(("", false)),
            };
            if args.is_some() {
                // A table-valued function call: its columns are not knowable
                // from the SQL text.
                scope.has_opaque = true;
                return Ok(None);
            }
            if let Some(cte) = find_cte(name, cte_scope) {
                // A CTE reference is not a base table, even if a base table
                // shares the CTE's name.
                let shape = match (cte.shape.clone(), alias) {
                    (Some(shape), Some(table_alias)) => {
                        apply_alias_columns(Some(shape), table_alias)
                    }
                    (shape, _) => shape,
                };
                let Some(shape) = shape else {
                    scope.has_opaque = true;
                    return Ok(None);
                };
                let names = names_of(&shape);
                let entry = WildcardEntry::Derived(scope.derived.len());
                scope.derived.push(DerivedRelationRef {
                    key_value: Some(key_value),
                    key_quoted,
                    nullable: false,
                    entry_index,
                    shape,
                });
                return Ok(Some((names, vec![entry])));
            }
            let Some(table) = resolve_object_name(name, database)? else {
                scope.has_opaque = true;
                return Ok(None);
            };
            let Some(output_names) = aliased_output_names(table, alias.as_ref(), database)? else {
                scope.has_opaque = true;
                return Ok(None);
            };
            let entry = WildcardEntry::Base(scope.bases.len());
            scope.bases.push(FromTableRef {
                key_value,
                key_quoted,
                table,
                nullable: false,
                entry_index,
                output_names: output_names.clone(),
            });
            Ok(Some((output_names, vec![entry])))
        }
        TableFactor::Derived { subquery, alias, .. } => {
            let body = derive_query_shape(subquery, cte_scope, database)?;
            // A derived table written without an alias (accepted since
            // PostgreSQL 16) has no key a reference could qualify with, but
            // its columns still answer bare references.
            let shape = match alias {
                Some(table_alias) => apply_alias_columns(body, table_alias),
                None => body,
            };
            let Some(shape) = shape else {
                scope.has_opaque = true;
                return Ok(None);
            };
            let names = names_of(&shape);
            let entry = WildcardEntry::Derived(scope.derived.len());
            scope.derived.push(DerivedRelationRef {
                key_value: alias.as_ref().map(|table_alias| table_alias.name.value.as_str()),
                key_quoted: alias
                    .as_ref()
                    .is_some_and(|table_alias| table_alias.name.quote_style.is_some()),
                nullable: false,
                entry_index,
                shape,
            });
            Ok(Some((names, vec![entry])))
        }
        // Nested joins, standalone function factors, and the rest stay
        // opaque: their columns are not enumerated here.
        _ => {
            scope.has_opaque = true;
            Ok(None)
        }
    }
}

impl<DB: DatabaseLike> DQLLike<DB> for Query {
    fn projection_source_table<'db>(
        &self,
        database: &'db DB,
    ) -> Result<Option<&'db DB::Table>, LookupError> {
        // Only a plain SELECT body has a single projection to analyze.
        let SetExpr::Select(select) = self.body.as_ref() else {
            return Ok(None);
        };

        // DISTINCT and GROUP BY collapse rows, so the output is not keyed by a
        // base table's primary key.
        if select.distinct.is_some() {
            return Ok(None);
        }
        match &select.group_by {
            GroupByExpr::All(_) => return Ok(None),
            GroupByExpr::Expressions(expressions, _) if !expressions.is_empty() => {
                return Ok(None);
            }
            GroupByExpr::Expressions(_, _) => {}
        }

        let Some(FromScope { bases, derived, merged, from_entry_count, has_opaque, .. }) =
            collect_from_clause(self, database)?
        else {
            return Ok(None);
        };

        // Rows may only pass through a derived relation whose body preserves
        // row identity. Any other one poisons unqualified references here.
        let opaque = has_opaque || derived.iter().any(|relation| !relation.shape.row_preserving);

        let mut source: Option<&'db DB::Table> = None;
        for item in &select.projection {
            let item_source = match item {
                // `*` is a single base-table row only when the FROM is exactly
                // that one base table, or exactly one row-preserving derived
                // relation whose every column passes through the same table.
                SelectItem::Wildcard(_) => {
                    if from_entry_count == 1 && bases.len() == 1 {
                        Some(bases[0].table)
                    } else if from_entry_count == 1 && derived.len() == 1 {
                        single_source_relation(&derived[0], database)
                    } else {
                        None
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                            match resolve_wildcard_target(&bases, &derived, object_name, true) {
                                Some(WildcardTarget::Base(base)) => Some(base.table),
                                Some(WildcardTarget::Derived(relation)) => {
                                    single_source_relation(relation, database)
                                }
                                None => None,
                            }
                        }
                        SelectItemQualifiedWildcardKind::Expr(_) => None,
                    }
                }
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    row_identity_source(expr, &bases, &derived, &merged, opaque)?
                }
                // Spark's `expr AS (a, b)` names several outputs for one
                // expression, which is not a single pass-through column.
                SelectItem::ExprWithAliases { .. } => None,
            };

            match item_source {
                None => return Ok(None),
                Some(table) => {
                    match source {
                        None => source = Some(table),
                        Some(existing) => {
                            if database.table_id(existing) != database.table_id(table) {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }

        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

    use crate::{
        errors::LookupError,
        prelude::ParserDB,
        traits::{DQLLike, TableLike},
    };

    const SCHEMA: &str = "
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
    ";

    fn schema_db() -> ParserDB {
        ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema parses")
    }

    fn query_of(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("query parses");
        match statements.pop().expect("one statement") {
            Statement::Query(query) => *query,
            other => panic!("expected a query, got {other:?}"),
        }
    }

    fn source_name(sql: &str, db: &ParserDB) -> Option<String> {
        query_of(sql)
            .projection_source_table(db)
            .expect("projection_source_table succeeds")
            .map(|table| table.table_name().to_string())
    }

    #[test]
    fn single_table_columns() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id, name FROM users", &db), Some("users".to_string()));
    }

    #[test]
    fn single_table_wildcard() {
        let db = schema_db();
        assert_eq!(source_name("SELECT * FROM users", &db), Some("users".to_string()));
    }

    #[test]
    fn join_qualified_single_table_columns() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id, o.total FROM orders o JOIN users u ON o.user_id = u.id", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn join_qualified_wildcard() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn join_multi_table_projection_is_none() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id", &db),
            None
        );
    }

    #[test]
    fn wildcard_over_join_is_none() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT * FROM orders o JOIN users u ON o.user_id = u.id", &db),
            None
        );
    }

    #[test]
    fn computed_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT o.id + 1 FROM orders o", &db), None);
    }

    #[test]
    fn aggregate_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT COUNT(*) FROM users", &db), None);
    }

    #[test]
    fn group_by_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT user_id FROM orders GROUP BY user_id", &db), None);
    }

    #[test]
    fn distinct_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT DISTINCT user_id FROM orders", &db), None);
    }

    #[test]
    fn subquery_in_where_keeps_single_table() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders)", &db),
            Some("users".to_string())
        );
    }

    #[test]
    fn set_operation_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users UNION SELECT id FROM orders", &db), None);
    }

    #[test]
    fn cte_reference_resolves_through_a_row_preserving_body() {
        let db = schema_db();
        // `users` here is the CTE, not the base table. The CTE renames one
        // table's rows, so the projection is still rows of `orders`.
        assert_eq!(
            source_name("WITH users AS (SELECT id FROM orders) SELECT id FROM users", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn derived_table_projection_resolves_through_the_body() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT total FROM (SELECT total FROM orders) d", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn grouped_cte_body_refuses_the_row_identity_answer() {
        let db = schema_db();
        // The CTE column passes through `orders`, but grouped rows are not
        // orders rows, so the row-identity answer must stay nothing.
        assert_eq!(
            source_name(
                "WITH s AS (SELECT user_id, count(*) AS n FROM orders GROUP BY user_id) \
                 SELECT user_id FROM s",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH s AS (SELECT user_id, count(*) AS n FROM orders GROUP BY user_id) \
                 SELECT s.user_id FROM s",
                &db
            ),
            None
        );
    }

    #[test]
    fn distinct_cte_body_refuses_the_row_identity_answer() {
        let db = schema_db();
        assert_eq!(
            source_name("WITH v AS (SELECT DISTINCT name FROM users) SELECT name FROM v", &db),
            None
        );
    }

    #[test]
    fn wildcard_over_row_preserving_cte_resolves() {
        let db = schema_db();
        assert_eq!(
            source_name("WITH v AS (SELECT id, total FROM orders) SELECT * FROM v", &db),
            Some("orders".to_string())
        );
        assert_eq!(
            source_name("WITH v AS (SELECT id, total FROM orders) SELECT v.* FROM v", &db),
            Some("orders".to_string())
        );
    }

    // A wildcard over a relation whose body collapses rows is not rows of the
    // base table, even though every output column passes through it, so the
    // row-identity question answers nothing. The qualified path refuses such
    // a relation earlier, so only a wildcard reaches this rule.
    #[test]
    fn wildcard_over_collapsing_body_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT * FROM (SELECT DISTINCT total FROM orders) s", &db), None);
        assert_eq!(
            source_name(
                "WITH v AS (SELECT total FROM orders UNION SELECT total FROM orders) \
                 SELECT * FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name("SELECT * FROM (SELECT total FROM orders) s", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn wildcard_over_cte_with_a_computed_column_is_none() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH s AS (SELECT user_id, count(*) AS n FROM orders GROUP BY user_id) \
                 SELECT * FROM s",
                &db
            ),
            None
        );
    }

    #[test]
    fn ambiguous_unqualified_column_errors() {
        let db = schema_db();
        let result = query_of("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
            .projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
    }

    #[test]
    fn self_join_unqualified_column_is_ambiguous() {
        let db = schema_db();
        let result = query_of("SELECT name FROM users a JOIN users b ON a.id = b.id")
            .projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
    }

    #[test]
    fn self_join_qualified_wildcard_picks_one_alias() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT a.* FROM users a JOIN users b ON a.id = b.id", &db),
            Some("users".to_string())
        );
    }

    #[test]
    fn aliased_projection_columns_keep_single_table() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id AS oid, o.total AS amount FROM orders o", &db),
            Some("orders".to_string()),
        );
    }

    #[test]
    fn qualified_column_with_unknown_qualifier_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT x.id FROM orders o", &db), None);
    }

    #[test]
    fn qualified_column_absent_from_table_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT o.nope FROM orders o", &db), None);
    }

    #[test]
    fn unknown_from_table_is_opaque_and_not_eligible() {
        let db = schema_db();
        assert_eq!(source_name("SELECT anything FROM does_not_exist", &db), None);
    }

    #[test]
    fn two_part_public_name_matches_schemaless_table() {
        let db = schema_db();
        // A schema-less table resides in `public`, so both spellings reach it.
        assert_eq!(source_name("SELECT id FROM public.users", &db), Some("users".to_string()));
        // Any other schema stays a miss, and the relation is treated as opaque.
        assert_eq!(source_name("SELECT id FROM app.users", &db), None);
    }

    #[test]
    fn group_by_all_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users GROUP BY ALL", &db), None);
    }

    #[test]
    fn overqualified_from_name_errors() {
        let db = schema_db();
        let result = query_of("SELECT * FROM a.b.c").projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::InvalidObjectName { .. })), "got {result:?}");
    }

    // Measured on PostgreSQL 18.4: `UNION ALL` keeps every input row, while a
    // deduplicating `UNION` does not, so only the `ALL` form carries rows
    // through for the row-identity question.
    #[test]
    fn union_without_all_is_not_row_preserving() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users UNION SELECT id FROM users) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users UNION ALL SELECT id FROM users) \
                 SELECT v.id FROM v",
                &db
            ),
            Some("users".to_string())
        );
    }

    #[test]
    fn outer_join_body_is_not_row_preserving() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT u.id FROM users u LEFT JOIN orders o ON o.user_id = u.id) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
    }

    #[test]
    fn null_extended_side_refuses_row_identity() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT u.id FROM users u LEFT JOIN orders o ON o.user_id = u.id", &db),
            Some("users".to_string())
        );
        assert_eq!(
            source_name("SELECT o.total FROM users u LEFT JOIN orders o ON o.user_id = u.id", &db),
            None
        );
        // The null-extended side's name exposure still collides with the
        // other side's under a bare reference, matching PostgreSQL.
        assert!(matches!(
            query_of("SELECT id FROM users u LEFT JOIN orders o ON o.user_id = u.id")
                .projection_source_table(&db),
            Err(LookupError::AmbiguousTableLookup { .. })
        ));
    }

    #[test]
    fn multi_name_alias_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id AS (x, y) FROM users", &db), None);
    }

    // Measured on PostgreSQL 18.4: the merged `USING` column stands once in
    // the join output and its coalesced value belongs to neither table.
    #[test]
    fn using_merged_column_refuses_row_identity() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users JOIN orders USING (id)", &db), None);
        assert_eq!(
            source_name("SELECT total FROM users JOIN orders USING (id)", &db),
            Some("orders".to_string())
        );
    }

    // Measured on PostgreSQL 18.4: a three-part reference resolves when the
    // leading part is the base table's own schema, and never for a CTE or a
    // schema mismatch.
    #[test]
    fn three_part_reference_resolves_for_matching_schema() {
        let db = schema_db();
        // A bare table lives in `public`, so `public.` names it.
        assert_eq!(
            source_name("SELECT public.users.id FROM users", &db),
            Some("users".to_string())
        );
        // An alias shadows the qualified name, matching PostgreSQL's
        // `invalid reference to FROM-clause entry`.
        assert_eq!(source_name("SELECT public.users.id FROM users AS u", &db), None);
        // Quoted schema identifiers stay case-sensitive.
        assert_eq!(source_name("SELECT \"Public\".users.id FROM users", &db), None);
        // The leading part must be the table's own schema.
        assert_eq!(source_name("SELECT other.users.id FROM users", &db), None);
        assert_eq!(source_name("SELECT public.orders.id FROM users", &db), None);
        // A column the table does not have answers nothing.
        assert_eq!(source_name("SELECT public.users.nope FROM users", &db), None);
        // An alias on the projection keeps the source.
        assert_eq!(
            source_name("SELECT public.users.id AS x FROM users", &db),
            Some("users".to_string())
        );
        // The database part is not modeled.
        assert_eq!(source_name("SELECT postgres.public.users.id FROM users", &db), None);
        // A null-extended side never answers the row-identity question.
        assert_eq!(
            source_name("SELECT public.orders.id FROM users LEFT JOIN orders ON true", &db),
            None
        );
    }

    #[test]
    fn unqualified_reference_under_outer_join_still_errors_when_ambiguous() {
        let db = schema_db();
        // The nullable side still counts toward ambiguity: PostgreSQL
        // rejects the bare name, the scope must not silently pick a side.
        let error = query_of("SELECT id FROM users LEFT JOIN orders ON true")
            .projection_source_table(&db)
            .expect_err("ambiguous");
        assert!(matches!(error, LookupError::AmbiguousTableLookup { .. }));
    }

    // A body that reads a relation whose rows are not source rows (a grouped
    // or deduplicated body) is itself not row preserving, one nesting level
    // deep or many.
    #[test]
    fn row_preserving_is_transitive_through_relations() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH g AS (SELECT user_id, count(*) AS n FROM orders \
                 GROUP BY user_id), v AS (SELECT g.user_id FROM g) \
                 SELECT v.user_id FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH d AS (SELECT DISTINCT id FROM users), \
                 v AS (SELECT d.id FROM d) SELECT v.id FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH f AS (SELECT id FROM users), v AS (SELECT f.id FROM f) \
                 SELECT v.id FROM v",
                &db
            ),
            Some("users".to_string())
        );
    }

    #[test]
    fn right_and_full_outer_join_nullability() {
        let db = schema_db();
        assert_eq!(source_name("SELECT users.id FROM users RIGHT JOIN orders ON true", &db), None);
        assert_eq!(
            source_name("SELECT orders.id FROM users RIGHT JOIN orders ON true", &db),
            Some("orders".to_string())
        );
        assert_eq!(source_name("SELECT users.id FROM users FULL JOIN orders ON true", &db), None);
        assert_eq!(source_name("SELECT orders.id FROM users FULL JOIN orders ON true", &db), None);
    }

    // A comma is a cross join: an outer join inside one `FROM` item
    // null-extends only that item's own relations.
    #[test]
    fn comma_from_item_keeps_identity_across_outer_join() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE a(id INT); CREATE TABLE b(id INT); CREATE TABLE c(id INT);",
        )
        .expect("schema parses");
        assert_eq!(
            source_name("SELECT a.id FROM a, b RIGHT JOIN c ON true", &db),
            Some("a".to_string())
        );
        assert_eq!(source_name("SELECT b.id FROM a, b RIGHT JOIN c ON true", &db), None);
        assert_eq!(
            source_name("SELECT c.id FROM a, b RIGHT JOIN c ON true", &db),
            Some("c".to_string())
        );
    }

    #[test]
    fn having_and_qualify_poison_the_body() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users HAVING count(*) > 1) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users QUALIFY count(*) OVER () > 1) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
    }

    #[test]
    fn intersect_and_except_poison_row_identity_without_all() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users INTERSECT SELECT id FROM users) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users INTERSECT ALL SELECT id FROM users) \
                 SELECT v.id FROM v",
                &db
            ),
            Some("users".to_string())
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users EXCEPT SELECT id FROM orders) \
                 SELECT v.id FROM v",
                &db
            ),
            None
        );
    }

    // Sibling `WITH` items that each reference the previous one twice double
    // the width every level. The width cap turns the over-wide relation
    // opaque rather than letting materialization explode.
    // The lint's suggestion (`push_fmt`) is nightly-only, and CI gates on
    // stable.
    #[allow(clippy::format_push_string)]
    #[test]
    fn doubling_wildcard_chain_becomes_opaque_at_the_cap() {
        let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t(x INT, y INT);")
            .expect("schema parses");
        let mut sql = String::from("WITH a1 AS (SELECT * FROM t t1, t t2)");
        for level in 2..=30 {
            sql.push_str(&format!(
                ", a{level} AS (SELECT * FROM a{} x, a{} y)",
                level - 1,
                level - 1
            ));
        }
        sql.push_str(" SELECT z.x FROM a30 z");
        assert_eq!(source_name(&sql, &db), None);
    }

    // The second `USING` on the same name moves the subsumption boundary, so
    // the bare name is still one merged column (source-less), not ambiguous
    // against the third relation.
    #[test]
    fn second_using_on_the_same_name_stays_sourceless() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE TABLE a(id INT); CREATE TABLE b(id INT); CREATE TABLE c(id INT);",
        )
        .expect("schema parses");
        assert_eq!(source_name("SELECT id FROM a JOIN b USING (id) JOIN c USING (id)", &db), None);
    }

    // A body-internal ambiguity degrades to a source-less output column: the
    // name is still exposed, and asking about it answers nothing rather than
    // failing the whole derivation.
    #[test]
    fn body_internal_ambiguity_degrades_to_sourceless_column() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT id FROM users JOIN orders \
                 ON users.id = orders.user_id) SELECT v.id FROM v",
                &db
            ),
            None
        );
    }

    #[test]
    fn qualified_wildcard_in_body_passes_through() {
        let db = schema_db();
        assert_eq!(
            source_name("WITH v AS (SELECT u.* FROM users u) SELECT v.name FROM v", &db),
            Some("users".to_string())
        );
    }

    // Set-operation arms pair by ordinal, naming the output from the left
    // arm: a right arm written in a different column order pairs `name`
    // against a foreign source and answers nothing.
    #[test]
    fn set_operation_projection_pairs_arms_by_ordinal() {
        let db = schema_db();
        assert_eq!(
            source_name(
                "WITH v AS (SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id \
                 UNION ALL SELECT o.total FROM users u JOIN orders o ON u.id = o.user_id) \
                 SELECT v.name FROM v",
                &db
            ),
            None
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id \
                 UNION ALL SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id) \
                 SELECT v.name FROM v",
                &db
            ),
            Some("users".to_string())
        );
    }

    // A base-table alias column list renames the exposed columns: the new
    // name answers, the replaced one stops answering, and the tail keeps
    // its own name. More aliases than columns is a mismatch PostgreSQL
    // rejects, so the relation stays opaque.
    #[test]
    fn base_alias_list_renames_under_projection_identity() {
        let db = schema_db();
        assert_eq!(source_name("SELECT u.n FROM users u(n)", &db), Some("users".to_string()));
        assert_eq!(source_name("SELECT u.id FROM users u(n)", &db), None);
        assert_eq!(source_name("SELECT u.name FROM users u(n)", &db), Some("users".to_string()));
        assert_eq!(source_name("SELECT id FROM users u(a,b,c)", &db), None);
    }

    // A qualified wildcard's prefix resolves by the column reference rules
    // (see the type docs): a one-part prefix matches aliases and keys, a
    // two-part prefix must match a base relation's own schema, and three
    // parts match nothing because the database name is not modeled.
    #[test]
    fn qualified_wildcard_prefix_follows_column_rules_under_identity() {
        let db = schema_db();
        assert_eq!(source_name("SELECT public.users.* FROM users", &db), Some("users".to_string()));
        assert_eq!(source_name("SELECT nosch.users.* FROM users", &db), None);
        assert_eq!(
            source_name("WITH cv AS (SELECT name FROM users) SELECT sch.cv.* FROM cv", &db),
            None
        );
        assert_eq!(source_name("SELECT probe.public.users.* FROM users", &db), None);
    }

    // A wildcard carrying `EXCLUDE`, `EXCEPT`, `ILIKE`, `RENAME`, `REPLACE`
    // or a trailing alias does not output the columns a bare `*` would, so a
    // body projecting one is not enumerated and a reference through it
    // answers nothing rather than claiming a column the relation dropped,
    // renamed or replaced.
    #[test]
    fn reshaped_wildcard_leaves_the_body_opaque() {
        let db = schema_db();
        for body in [
            "SELECT * EXCLUDE (name) FROM users",
            "SELECT * EXCEPT (name) FROM users",
            "SELECT * ILIKE 'id%' FROM users",
            "SELECT * RENAME (name AS handle) FROM users",
            "SELECT * REPLACE ('x' AS name) FROM users",
            "SELECT users.* EXCLUDE (name) FROM users",
        ] {
            let sql = format!("WITH v AS ({body}) SELECT v.name FROM v");
            assert_eq!(source_name(&sql, &db), None, "{body}");
        }
        // A plain wildcard body still resolves.
        assert_eq!(
            source_name("WITH v AS (SELECT * FROM users) SELECT v.name FROM v", &db),
            Some("users".to_string())
        );
    }

    // Every join spelling the resolver models routes through the same
    // nullability rules: the accumulated side keeps row identity unless the
    // operator null-extends it, and `APPLY` carries no join constraint at all.
    #[test]
    fn join_spellings_share_the_nullability_rules() {
        let db = schema_db();
        for (sql, expected) in [
            ("SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id", Some("users")),
            (
                "SELECT u.id FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id",
                Some("users"),
            ),
            ("SELECT u.id FROM users u RIGHT OUTER JOIN orders o ON u.id = o.user_id", None),
            ("SELECT u.id FROM users u CROSS JOIN orders o", Some("users")),
            ("SELECT u.id FROM users u CROSS APPLY orders o", Some("users")),
            ("SELECT u.id FROM users u OUTER APPLY orders o", Some("users")),
            ("SELECT u.id FROM users u SEMI JOIN orders o ON u.id = o.user_id", Some("users")),
            ("SELECT u.id FROM users u LEFT ANTI JOIN orders o ON u.id = o.user_id", Some("users")),
            ("SELECT u.id FROM users u LEFT SEMI JOIN orders o ON u.id = o.user_id", Some("users")),
            (
                "SELECT u.id FROM users u RIGHT SEMI JOIN orders o ON u.id = o.user_id",
                Some("users"),
            ),
            ("SELECT u.id FROM users u ANTI JOIN orders o ON u.id = o.user_id", Some("users")),
        ] {
            assert_eq!(source_name(sql, &db), expected.map(str::to_string), "{sql}");
        }
    }

    // A `NATURAL` join needs both sides' columns to know what it merges, so an
    // unresolvable side leaves the scope opaque instead of guessing that a
    // shared name is unmerged.
    #[test]
    fn natural_join_with_an_unenumerable_side_is_opaque() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users NATURAL JOIN absent_table", &db), None);
    }

    // Bodies whose output columns follow rules this resolver does not model
    // stay opaque: a `VALUES` list names columns positionally, a nested join
    // factor is not a relation with a key, and `GROUP BY ALL` collapses rows.
    #[test]
    fn unmodeled_from_and_group_forms_stay_opaque() {
        let db = schema_db();
        assert_eq!(source_name("WITH v AS (VALUES (1)) SELECT * FROM v", &db), None);
        assert_eq!(
            source_name("SELECT * FROM (users JOIN orders ON users.id = orders.user_id)", &db),
            None
        );
        assert_eq!(
            source_name(
                "WITH v AS (SELECT name FROM users GROUP BY ALL) SELECT v.name FROM v",
                &db
            ),
            None
        );
        // A hierarchical query names output columns by rules this resolver
        // does not model.
        assert_eq!(
            source_name(
                "WITH v AS (SELECT name FROM users CONNECT BY id = 1) SELECT v.name FROM v",
                &db
            ),
            None
        );
    }
}
