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
use core::ops::ControlFlow;

use sqlparser::ast::{
    AccessExpr, CaseWhen, Cte, DictionaryField, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator,
    JsonPathElem, MapEntry, ObjectName, OrderByExpr, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, Subscript, TableAlias,
    TableAliasColumnDef, TableFactor, Visit, Visitor, WildcardAdditionalOptions, WindowFrameBound,
    WindowType, With,
};

use crate::{
    errors::LookupError,
    traits::{ColumnLike, DQLLike, DatabaseLike, TableLike, ViewLike},
    utils::{
        identifier_resolution::identifiers_match,
        object_name::{
            object_name_last_part, render_table_candidate, resolve_object_name,
            schema_from_object_name, target_name_from_object_name,
        },
    },
};

pub(crate) mod definition_graph;

use definition_graph::{AstRef, DefinitionDerivation, ScopeCursor};

/// A view whose definition is being derived right now.
///
/// A `FROM` reference reaching one of these is a cycle, which PostgreSQL
/// accepts at creation and only refuses when the view is read, so the model
/// has to terminate on it rather than assume it cannot happen. The reference
/// stays opaque, exactly as a recursive CTE's self-reference does.
#[derive(Clone)]
enum DerivingView<'db, DB: DatabaseLike> {
    /// A plain view.
    Plain(&'db DB::View),
    /// A materialized view.
    Materialized(&'db DB::MaterializedView),
}

impl<DB: DatabaseLike> Copy for DerivingView<'_, DB> {}

impl<DB: DatabaseLike> DerivingView<'_, DB> {
    /// Whether both name the same recorded view.
    ///
    /// Compared by identity rather than by name, since a view's address in the
    /// database it was read from is what makes it that view.
    fn is(self, other: Self) -> bool {
        match (self, other) {
            (Self::Plain(left), Self::Plain(right)) => core::ptr::eq(left, right),
            (Self::Materialized(left), Self::Materialized(right)) => core::ptr::eq(left, right),
            _ => false,
        }
    }
}

/// One view on the chain a derivation is currently inside, linked to the one
/// that reached it.
///
/// A frame lives in the stack of the call that pushed it, which outlives every
/// call it makes, so extending the chain costs nothing. Holding the chain as a
/// slice instead meant copying it to a fresh allocation for every view
/// reference a query resolves.
struct DerivingFrame<'s, 'db, DB: DatabaseLike> {
    /// The view this frame stands for.
    view: DerivingView<'db, DB>,
    /// The frame that reached it, if any.
    parent: Option<&'s DerivingFrame<'s, 'db, DB>>,
}

/// The database a derivation reads, and the views whose definitions it is
/// already inside.
#[derive(Clone)]
struct Deriving<'s, 'db, DB: DatabaseLike> {
    /// The database every relation name resolves against.
    database: &'db DB,
    /// The innermost view being derived right now, if any.
    views: Option<&'s DerivingFrame<'s, 'db, DB>>,
}

impl<DB: DatabaseLike> Copy for Deriving<'_, '_, DB> {}

impl<'db, DB: DatabaseLike> Deriving<'_, 'db, DB> {
    /// A derivation of `database` that is inside no view.
    fn of(database: &'db DB) -> Self {
        Self { database, views: None }
    }

    /// Whether a reference to `view` would close a cycle.
    ///
    /// Walks the chain, whose length is the view nesting depth: a view appears
    /// on it at most once, since the second appearance is what this reports.
    fn is_deriving(&self, view: DerivingView<'db, DB>) -> bool {
        let mut frame = self.views;
        while let Some(entry) = frame {
            if entry.view.is(view) {
                return true;
            }
            frame = entry.parent;
        }
        false
    }
}

/// One output column of a base relation.
struct BaseColumnRef<'db, DB: DatabaseLike, D: Copy> {
    name: String,
    quoted: bool,
    source: &'db DB::Table,
    definition: D,
}

type BaseColumns<'db, DB, D> = Vec<BaseColumnRef<'db, DB, D>>;

impl<DB: DatabaseLike, D: Copy> Clone for BaseColumnRef<'_, DB, D> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            quoted: self.quoted,
            source: self.source,
            definition: self.definition,
        }
    }
}

#[derive(Clone, Copy)]
struct RelationKey<'query, 'db> {
    value: AstRef<'query, 'db, str>,
    quoted: bool,
}

struct FromTableRef<'query, 'db, DB: DatabaseLike, D: Copy> {
    key: RelationKey<'query, 'db>,
    schema_key: Option<RelationKey<'query, 'db>>,
    table: &'db DB::Table,
    nullable: bool,
    entry_index: usize,
    output_columns: Vec<BaseColumnRef<'db, DB, D>>,
}

/// One output column of a derivable relation.
struct DerivedColumn<'query, 'db, DB: DatabaseLike, D: Copy> {
    name: String,
    quoted: bool,
    source: Option<&'db DB::Table>,
    definition: D,
    marker: core::marker::PhantomData<&'query ()>,
}

impl<DB: DatabaseLike, D: Copy> Clone for DerivedColumn<'_, '_, DB, D> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            quoted: self.quoted,
            source: self.source,
            definition: self.definition,
            marker: core::marker::PhantomData,
        }
    }
}

enum OutputNameSource<'names, 'query, 'db, DB: DatabaseLike, D: Copy> {
    Columns(&'names [DerivedColumn<'query, 'db, DB, D>]),
    AliasColumns(AstRef<'query, 'db, [TableAliasColumnDef]>),
    Declared(&'names [(String, bool)]),
}

impl<'names, DB: DatabaseLike, D: Copy> OutputNameSource<'names, '_, '_, DB, D> {
    fn from_alias<'query, 'db>(
        alias: AstRef<'query, 'db, TableAlias>,
    ) -> Option<OutputNameSource<'names, 'query, 'db, DB, D>>
    where
        'query: 'names,
        'db: 'names,
    {
        (!alias.get().columns.is_empty())
            .then_some(OutputNameSource::AliasColumns(alias.map(|alias| alias.columns.as_slice())))
    }

    fn get(&self, ordinal: usize) -> Option<(String, bool)> {
        match self {
            Self::Columns(columns) => {
                columns.get(ordinal).map(|column| (column.name.clone(), column.quoted))
            }
            Self::AliasColumns(columns) => {
                columns
                    .get()
                    .get(ordinal)
                    .map(|column| (column.name.value.clone(), column.name.quote_style.is_some()))
            }
            Self::Declared(names) => names.get(ordinal).cloned(),
        }
    }
}

/// The derivable output shape of a relation.
struct DerivedShape<'query, 'db, DB: DatabaseLike, D: Copy> {
    columns: Vec<DerivedColumn<'query, 'db, DB, D>>,
    row_preserving: bool,
}

impl<DB: DatabaseLike, D: Copy> Clone for DerivedShape<'_, '_, DB, D> {
    fn clone(&self) -> Self {
        Self { columns: self.columns.clone(), row_preserving: self.row_preserving }
    }
}

struct DerivedRelationRef<'query, 'db, DB: DatabaseLike, D: Copy> {
    key: Option<RelationKey<'query, 'db>>,
    shape: DerivedShape<'query, 'db, DB, D>,
    nullable: bool,
    entry_index: usize,
}

struct CteShape<'query, 'db, DB: DatabaseLike, D: Copy> {
    name: RelationKey<'query, 'db>,
    shape: Option<DerivedShape<'query, 'db, DB, D>>,
}

impl<DB: DatabaseLike, D: Copy> Clone for CteShape<'_, '_, DB, D> {
    fn clone(&self) -> Self {
        Self { name: self.name, shape: self.shape.clone() }
    }
}

#[derive(Clone, Copy)]
enum OpaqueIdentity<'query, 'db> {
    Known { key: RelationKey<'query, 'db>, schema: Option<RelationKey<'query, 'db>> },
    Anonymous,
    AnyQualifier,
}

#[derive(Clone, Copy)]
struct OpaqueRelation<'query, 'db> {
    identity: OpaqueIdentity<'query, 'db>,
    entry_index: usize,
}

struct CteDependencyVisitor<'a> {
    candidates: &'a [Cte],
    shadowed: Vec<Vec<Ident>>,
    dependencies: Vec<bool>,
}

impl Visitor for CteDependencyVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.shadowed.push(query.with.as_ref().map_or_else(Vec::new, |with| {
            with.cte_tables.iter().map(|cte| cte.alias.name.clone()).collect()
        }));
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.shadowed.truncate(self.shadowed.len().saturating_sub(1));
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if relation.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let Some((value, quoted)) = object_name_last_part(relation) else {
            return ControlFlow::Continue(());
        };
        if self.shadowed.iter().rev().flatten().any(|ident| {
            identifiers_match(&ident.value, ident.quote_style.is_some(), value, quoted)
        }) {
            return ControlFlow::Continue(());
        }
        if let Some(position) = self.candidates.iter().position(|cte| {
            identifiers_match(
                &cte.alias.name.value,
                cte.alias.name.quote_style.is_some(),
                value,
                quoted,
            )
        }) {
            self.dependencies[position] = true;
        }
        ControlFlow::Continue(())
    }
}

fn cte_dependencies(cte: &Cte, candidates: &[Cte]) -> Vec<bool> {
    let mut visitor = CteDependencyVisitor {
        candidates,
        shadowed: Vec::new(),
        dependencies: vec![false; candidates.len()],
    };
    let _: ControlFlow<()> = cte.query.visit(&mut visitor);
    visitor.dependencies
}

fn cte_reaches(adjacency: &[Vec<bool>], from: usize, target: usize) -> bool {
    let mut visited = vec![false; adjacency.len()];
    let mut pending = vec![from];
    visited[from] = true;
    while let Some(current) = pending.pop() {
        for (next, follows) in adjacency[current].iter().copied().enumerate() {
            if !follows || visited[next] {
                continue;
            }
            if next == target {
                return true;
            }
            visited[next] = true;
            pending.push(next);
        }
    }
    false
}

fn mutually_recursive_ctes(with: &With) -> Vec<bool> {
    let adjacency: Vec<Vec<bool>> =
        with.cte_tables.iter().map(|cte| cte_dependencies(cte, &with.cte_tables)).collect();
    adjacency
        .iter()
        .enumerate()
        .map(|(left, _)| {
            adjacency.iter().enumerate().any(|(right, _)| {
                left != right
                    && cte_reaches(&adjacency, left, right)
                    && cte_reaches(&adjacency, right, left)
            })
        })
        .collect()
}
/// A column name merged by a `USING` or `NATURAL` join. `subsumed` is the
/// number of `FROM` entries the merge consumed: relations collected before
/// that boundary pass their exposure of the name into the merged column and
/// no longer count individually, while relations joined in afterwards collide
/// with it, as PostgreSQL reports for a bare reference.
struct MergedName {
    name: String,
    quoted: bool,
    subsumed: usize,
}

/// One output position of a `FROM` item's join chain as a `*` projection
/// sees it: a base relation (index into `FromScope::bases`), a derived
/// relation (index into `FromScope::derived`), or a column merged by a
/// `USING` or `NATURAL` join, whose coalesced value has no single source.
enum WildcardEntry {
    Base(usize),
    Derived(usize),
    Merged { name: String, quoted: bool },
}

struct FromScope<'query, 'db, DB: DatabaseLike, D: Copy> {
    bases: Vec<FromTableRef<'query, 'db, DB, D>>,
    derived: Vec<DerivedRelationRef<'query, 'db, DB, D>>,
    merged: Vec<MergedName>,
    wildcard_plans: Vec<Vec<WildcardEntry>>,
    opaque: Vec<OpaqueRelation<'query, 'db>>,
    from_entry_count: usize,
    unqualified_poison: bool,
}

impl<DB: DatabaseLike, D: Copy> FromScope<'_, '_, DB, D> {
    fn new() -> Self {
        Self {
            bases: Vec::new(),
            derived: Vec::new(),
            merged: Vec::new(),
            wildcard_plans: Vec::new(),
            opaque: Vec::new(),
            from_entry_count: 0,
            unqualified_poison: false,
        }
    }

    fn has_opaque(&self) -> bool {
        self.unqualified_poison || !self.opaque.is_empty()
    }
}

trait DerivationProfile<'query, 'db, DB: DatabaseLike> {
    type Definition: Copy;
    type Scope;
    type Cursor: Copy;
    type Checkpoint: Copy;

    const INDEX_NESTED_QUERIES: bool;

    fn no_parent(&self) -> Self::Cursor;
    fn begin_scope(
        &mut self,
        select: AstRef<'query, 'db, Select>,
        parent: Self::Cursor,
    ) -> Self::Scope;
    fn scope<'scope>(
        &'scope self,
        scope: &'scope Self::Scope,
    ) -> &'scope FromScope<'query, 'db, DB, Self::Definition>;
    fn scope_mut<'scope>(
        &'scope mut self,
        scope: &'scope mut Self::Scope,
    ) -> &'scope mut FromScope<'query, 'db, DB, Self::Definition>;
    fn cursor(&self, scope: &Self::Scope) -> Self::Cursor;
    fn opaque_definition(&self) -> Self::Definition;
    fn base_definition(
        &mut self,
        table: &'db DB::Table,
        column: &'db DB::Column,
    ) -> Self::Definition;
    fn expression_definition(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
        scope: Self::Cursor,
    ) -> Result<Self::Definition, LookupError>;
    fn set_definition(
        &mut self,
        operator: SetOperator,
        left: Self::Definition,
        right: Self::Definition,
    ) -> Self::Definition;
    fn recursive_definition(
        &mut self,
        anchor: Self::Definition,
        recursive: Self::Definition,
    ) -> Self::Definition;
    fn checkpoint(&self) -> Self::Checkpoint;
    fn rollback(&mut self, checkpoint: Self::Checkpoint);
}

struct SourceDerivation;

impl<'query, 'db, DB> DerivationProfile<'query, 'db, DB> for SourceDerivation
where
    DB: DatabaseLike,
    DB::Table: 'db,
{
    type Definition = ();
    type Scope = FromScope<'query, 'db, DB, ()>;
    type Cursor = ();
    type Checkpoint = ();

    const INDEX_NESTED_QUERIES: bool = false;

    fn no_parent(&self) {}

    fn begin_scope(
        &mut self,
        _select: AstRef<'query, 'db, Select>,
        _parent: Self::Cursor,
    ) -> Self::Scope {
        FromScope::new()
    }

    fn scope<'scope>(
        &'scope self,
        scope: &'scope Self::Scope,
    ) -> &'scope FromScope<'query, 'db, DB, Self::Definition> {
        scope
    }

    fn scope_mut<'scope>(
        &'scope mut self,
        scope: &'scope mut Self::Scope,
    ) -> &'scope mut FromScope<'query, 'db, DB, Self::Definition> {
        scope
    }

    fn cursor(&self, _scope: &Self::Scope) {}

    fn opaque_definition(&self) {}

    fn base_definition(&mut self, _table: &'db DB::Table, _column: &'db DB::Column) {}

    fn expression_definition(
        &mut self,
        _expression: AstRef<'query, 'db, Expr>,
        _scope: Self::Cursor,
    ) -> Result<Self::Definition, LookupError> {
        Ok(())
    }

    fn set_definition(
        &mut self,
        _operator: SetOperator,
        _left: Self::Definition,
        _right: Self::Definition,
    ) {
    }

    fn recursive_definition(&mut self, _anchor: Self::Definition, _recursive: Self::Definition) {}

    fn checkpoint(&self) {}

    fn rollback(&mut self, _checkpoint: Self::Checkpoint) {}
}

enum RelationContribution<'query, 'db, DB: DatabaseLike, D: Copy> {
    Base(FromTableRef<'query, 'db, DB, D>),
    Derived(DerivedRelationRef<'query, 'db, DB, D>),
    Opaque(OpaqueIdentity<'query, 'db>),
}

/// What one `FROM` factor contributes: the output names it exposes and the
/// wildcard plan entry naming what it pushed.
struct FactorContribution<'query, 'db, DB: DatabaseLike, D: Copy> {
    relation: RelationContribution<'query, 'db, DB, D>,
    names: Option<Vec<(String, bool)>>,
}

type FactorOutput = (Vec<(String, bool)>, Vec<WildcardEntry>);

fn append_factor<'query, 'db, DB, P>(
    profile: &mut P,
    scope: &mut P::Scope,
    contribution: FactorContribution<'query, 'db, DB, P::Definition>,
) -> Option<FactorOutput>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let data = profile.scope_mut(scope);
    let entry_index = data.from_entry_count;
    data.from_entry_count += 1;
    let entry = match contribution.relation {
        RelationContribution::Base(mut relation) => {
            relation.entry_index = entry_index;
            let entry = WildcardEntry::Base(data.bases.len());
            data.bases.push(relation);
            Some(entry)
        }
        RelationContribution::Derived(mut relation) => {
            relation.entry_index = entry_index;
            let entry = WildcardEntry::Derived(data.derived.len());
            data.derived.push(relation);
            Some(entry)
        }
        RelationContribution::Opaque(identity) => {
            data.opaque.push(OpaqueRelation { identity, entry_index });
            None
        }
    };
    contribution.names.map(|names| (names, entry.into_iter().collect()))
}

fn collect_source_from_clause<'query, 'db, DB: DatabaseLike>(
    query: &'query Query,
    database: &'db DB,
) -> Result<Option<FromScope<'query, 'db, DB, ()>>, LookupError> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let deriving = Deriving::of(database);
    let mut profile = SourceDerivation;
    let parent = ();
    let cte_scope = match &query.with {
        Some(with) => derive_cte_shapes(AstRef::Query(with), &[], deriving, parent, &mut profile)?,
        None => Vec::new(),
    };
    collect_select_from(AstRef::Query(select), &cte_scope, deriving, parent, &mut profile).map(Some)
}

fn collect_select_from<'query, 'db, DB, P>(
    select: AstRef<'query, 'db, Select>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<P::Scope, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let mut scope = profile.begin_scope(select, parent);
    for table_with_joins in select.map(|select| select.from.as_slice()).iter() {
        let (entry_bases, entry_derived) = {
            let data = profile.scope(&scope);
            (data.bases.len(), data.derived.len())
        };
        let local_parent = profile.cursor(&scope);
        let contribution = collect_factor(
            table_with_joins.map(|entry| &entry.relation),
            deriving,
            cte_scope,
            parent,
            local_parent,
            profile,
        )?;
        let (mut accumulated, mut plan) = match append_factor(profile, &mut scope, contribution) {
            Some((names, entries)) => (Some(names), Some(entries)),
            None => (None, None),
        };
        for join in table_with_joins.map(|entry| entry.joins.as_slice()).iter() {
            let (bases_before, derived_before) = {
                let data = profile.scope(&scope);
                (data.bases.len(), data.derived.len())
            };
            let local_parent = profile.cursor(&scope);
            let contribution = collect_factor(
                join.map(|join| &join.relation),
                deriving,
                cte_scope,
                parent,
                local_parent,
                profile,
            )?;
            let (right_names, right_entries) =
                match append_factor(profile, &mut scope, contribution) {
                    Some((names, entries)) => (Some(names), Some(entries)),
                    None => (None, None),
                };
            let (left_nullable, right_nullable) = nullable_sides(&join.get().join_operator);
            let data = profile.scope_mut(&mut scope);
            if left_nullable {
                for base in &mut data.bases[entry_bases..bases_before] {
                    base.nullable = true;
                }
                for relation in &mut data.derived[entry_derived..derived_before] {
                    relation.nullable = true;
                }
            }
            if right_nullable {
                for base in &mut data.bases[bases_before..] {
                    base.nullable = true;
                }
                for relation in &mut data.derived[derived_before..] {
                    relation.nullable = true;
                }
            }
            let mut merged = Vec::new();
            if let Some(names) = merge_names(
                &join.get().join_operator,
                accumulated.as_deref(),
                right_names.as_deref(),
            ) {
                let boundary = data.from_entry_count;
                for (name, quoted) in &names {
                    merge_name(&mut data.merged, name.clone(), *quoted, boundary);
                }
                merged = names;
            } else {
                data.unqualified_poison = true;
            }
            plan = merge_plans(plan, right_entries, &merged);
            accumulated = merge_output_names(accumulated, right_names, &merged);
        }
        profile.scope_mut(&mut scope).wildcard_plans.push(plan.unwrap_or_default());
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

/// Derives the shapes of a `WITH` clause's CTEs in order. A recursive list
/// binds every name before any body is resolved. Multi-CTE cycles and forward
/// references stay opaque, while a self-recursive CTE is seeded from its
/// nonrecursive term. A non-recursive list registers names one by one.
fn derive_cte_shapes<'query, 'db, DB, P>(
    with: AstRef<'query, 'db, With>,
    outer: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Vec<CteShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let mut shapes = outer.to_vec();
    #[cfg(test)]
    tests::record_cte_shape_derivation();
    let base = shapes.len();
    let mutually_recursive = if with.get().recursive {
        mutually_recursive_ctes(with.get())
    } else {
        vec![false; with.get().cte_tables.len()]
    };
    if with.get().recursive {
        for cte in with.map(|with| with.cte_tables.as_slice()).iter() {
            let alias = cte.map(|cte| &cte.alias);
            shapes.push(CteShape {
                name: RelationKey {
                    value: alias.map(|alias| alias.name.value.as_str()),
                    quoted: alias.get().name.quote_style.is_some(),
                },
                shape: None,
            });
        }
    }
    for (index, cte) in with.map(|with| with.cte_tables.as_slice()).iter().enumerate() {
        let position = if with.get().recursive { base + index } else { shapes.len() };
        if with.get().recursive {
            if mutually_recursive[index] {
                continue;
            }
            let body = derive_recursive_cte_query_shape(
                cte.map(|cte| cte.query.as_ref()),
                &mut shapes,
                position,
                cte.map(|cte| &cte.alias),
                deriving,
                parent,
                profile,
            )?;
            shapes[position].shape = apply_alias_columns(body, &cte.get().alias);
        } else {
            let alias = cte.map(|cte| &cte.alias);
            shapes.push(CteShape {
                name: RelationKey {
                    value: alias.map(|alias| alias.name.value.as_str()),
                    quoted: alias.get().name.quote_style.is_some(),
                },
                shape: None,
            });
            let body = derive_query_shape(
                cte.map(|cte| cte.query.as_ref()),
                &shapes,
                OutputNameSource::from_alias(alias),
                deriving,
                parent,
                profile,
            )?;
            shapes[position].shape = apply_alias_columns(body, alias.get());
        }
    }
    Ok(shapes)
}

fn select_body_ref<'query, 'db>(
    body: AstRef<'query, 'db, SetExpr>,
) -> Option<AstRef<'query, 'db, Select>> {
    body.try_map(|body| {
        match body {
            SetExpr::Select(select) => Some(select.as_ref()),
            _ => None,
        }
    })
}

fn query_body_ref<'query, 'db>(
    body: AstRef<'query, 'db, SetExpr>,
) -> Option<AstRef<'query, 'db, Query>> {
    body.try_map(|body| {
        match body {
            SetExpr::Query(query) => Some(query.as_ref()),
            _ => None,
        }
    })
}

fn set_operation_arms<'query, 'db>(
    body: AstRef<'query, 'db, SetExpr>,
) -> Option<(AstRef<'query, 'db, SetExpr>, AstRef<'query, 'db, SetExpr>)> {
    let left = body.try_map(|body| {
        match body {
            SetExpr::SetOperation { left, .. } => Some(left.as_ref()),
            _ => None,
        }
    })?;
    let right = body.try_map(|body| {
        match body {
            SetExpr::SetOperation { right, .. } => Some(right.as_ref()),
            _ => None,
        }
    })?;
    Some((left, right))
}

fn derive_recursive_cte_query_shape<'query, 'db, DB, P>(
    query: AstRef<'query, 'db, Query>,
    cte_scope: &mut Vec<CteShape<'query, 'db, DB, P::Definition>>,
    position: usize,
    alias: AstRef<'query, 'db, TableAlias>,
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let checkpoint = profile.checkpoint();
    let mut scoped;
    let scope = match &query.get().with {
        Some(_) => {
            let Some(with) = query.try_map(|query| query.with.as_ref()) else {
                return Ok(None);
            };
            scoped = derive_cte_shapes(with, cte_scope, deriving, parent, profile)?;
            &mut scoped
        }
        None => cte_scope,
    };
    let result = derive_recursive_cte_set_expr_shape(
        query.map(|query| query.body.as_ref()),
        scope,
        position,
        alias,
        deriving,
        parent,
        profile,
    );
    if !matches!(result, Ok(Some(_))) {
        profile.rollback(checkpoint);
    }
    result
}

fn derive_recursive_cte_set_expr_shape<'query, 'db, DB, P>(
    body: AstRef<'query, 'db, SetExpr>,
    cte_scope: &mut Vec<CteShape<'query, 'db, DB, P::Definition>>,
    position: usize,
    alias: AstRef<'query, 'db, TableAlias>,
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let output_names = OutputNameSource::from_alias(alias);
    match body.get() {
        SetExpr::Query(_) => {
            let Some(query) = query_body_ref(body) else {
                return Ok(None);
            };
            derive_recursive_cte_query_shape(
                query, cte_scope, position, alias, deriving, parent, profile,
            )
        }
        SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, .. } => {
            let Some((left, right)) = set_operation_arms(body) else {
                return Ok(None);
            };
            let Some(left_shape) =
                derive_set_expr_shape(left, cte_scope, output_names, deriving, parent, profile)?
            else {
                return Ok(None);
            };
            cte_scope[position].shape = apply_alias_columns(Some(left_shape), alias.get());
            let Some(right_shape) = ({
                let Some(left_shape) = cte_scope[position].shape.as_ref() else {
                    return Ok(None);
                };
                derive_set_expr_shape(
                    right,
                    cte_scope,
                    Some(OutputNameSource::Columns(&left_shape.columns)),
                    deriving,
                    parent,
                    profile,
                )?
            }) else {
                return Ok(None);
            };
            let Some(left_shape) = cte_scope[position].shape.take() else {
                return Ok(None);
            };
            Ok(merge_set_operation_shapes(
                left_shape,
                right_shape,
                SetOperator::Union,
                *set_quantifier,
                true,
                deriving,
                profile,
            ))
        }
        _ => derive_set_expr_shape(body, cte_scope, output_names, deriving, parent, profile),
    }
}

/// Derives the output shape of a query used as a relation body. Returns
/// `Ok(None)` when the columns cannot be enumerated.
fn derive_query_shape<'query, 'db, DB, P>(
    query: AstRef<'query, 'db, Query>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    output_names: Option<OutputNameSource<'_, 'query, 'db, DB, P::Definition>>,
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let checkpoint = profile.checkpoint();
    let scoped;
    let scope = match &query.get().with {
        Some(_) => {
            let Some(with) = query.try_map(|query| query.with.as_ref()) else {
                return Ok(None);
            };
            scoped = derive_cte_shapes(with, cte_scope, deriving, parent, profile)?;
            &scoped
        }
        None => cte_scope,
    };
    let result = derive_set_expr_shape(
        query.map(|query| query.body.as_ref()),
        scope,
        output_names,
        deriving,
        parent,
        profile,
    );
    if !matches!(result, Ok(Some(_))) {
        profile.rollback(checkpoint);
    }
    result
}

/// Derives the output shape of a body. A set operation merges its arms by
/// ordinal position: names come from the left arm (as in PostgreSQL) and a
/// column keeps a source only while the arms agree on one.
fn derive_set_expr_shape<'query, 'db, DB, P>(
    body: AstRef<'query, 'db, SetExpr>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    output_names: Option<OutputNameSource<'_, 'query, 'db, DB, P::Definition>>,
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    match body.get() {
        SetExpr::Select(_) => {
            let Some(select) = select_body_ref(body) else {
                return Ok(None);
            };
            derive_select_shape(select, cte_scope, output_names.as_ref(), deriving, parent, profile)
        }
        SetExpr::Query(_) => {
            let Some(query) = query_body_ref(body) else {
                return Ok(None);
            };
            derive_query_shape(query, cte_scope, output_names, deriving, parent, profile)
        }
        SetExpr::SetOperation { op, set_quantifier, .. } => {
            let Some((left, right)) = set_operation_arms(body) else {
                return Ok(None);
            };
            let Some(left_shape) =
                derive_set_expr_shape(left, cte_scope, output_names, deriving, parent, profile)?
            else {
                return Ok(None);
            };
            let Some(right_shape) = derive_set_expr_shape(
                right,
                cte_scope,
                Some(OutputNameSource::Columns(&left_shape.columns)),
                deriving,
                parent,
                profile,
            )?
            else {
                return Ok(None);
            };
            Ok(merge_set_operation_shapes(
                left_shape,
                right_shape,
                *op,
                *set_quantifier,
                false,
                deriving,
                profile,
            ))
        }
        _ => Ok(None),
    }
}

fn merge_set_operation_shapes<'query, 'db, DB, P>(
    left: DerivedShape<'query, 'db, DB, P::Definition>,
    right: DerivedShape<'query, 'db, DB, P::Definition>,
    operator: SetOperator,
    set_quantifier: SetQuantifier,
    recursive: bool,
    deriving: Deriving<'_, 'db, DB>,
    profile: &mut P,
) -> Option<DerivedShape<'query, 'db, DB, P::Definition>>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    if left.columns.len() != right.columns.len() {
        return None;
    }
    let row_preserving =
        matches!(set_quantifier, SetQuantifier::All) && left.row_preserving && right.row_preserving;
    let columns = left
        .columns
        .into_iter()
        .zip(right.columns)
        .map(|(left, right)| {
            let definition = if recursive {
                profile.recursive_definition(left.definition, right.definition)
            } else {
                profile.set_definition(operator, left.definition, right.definition)
            };
            DerivedColumn {
                name: left.name,
                quoted: left.quoted,
                source: match (left.source, right.source) {
                    (Some(left_table), Some(right_table))
                        if deriving.database.table_id(left_table)
                            == deriving.database.table_id(right_table) =>
                    {
                        Some(left_table)
                    }
                    _ => None,
                },
                definition,
                marker: core::marker::PhantomData,
            }
        })
        .collect();
    Some(DerivedShape { columns, row_preserving })
}

/// Expands a `*` projection by materializing each `FROM` item's plan in
/// PostgreSQL's join output order. A relation contributes its own columns
/// except those a merge absorbed (relations collected before that name's
/// merge). A merged name stands once per join with no source, and a repeated
/// merge of the same name stands once at its latest position.
fn push_wildcard_columns<'query, 'db, DB: DatabaseLike, D: Copy>(
    scope: &FromScope<'query, 'db, DB, D>,
    columns: &mut Vec<DerivedColumn<'query, 'db, DB, D>>,
    opaque: D,
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
                        definition: opaque,
                        marker: core::marker::PhantomData,
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

/// Whether a wildcard substitutes a computed value for one of the columns it
/// expands (`REPLACE`). Such an output row is no longer a row of the source
/// table, so it carries no row identity, whereas dropping or relabelling
/// columns leaves the rows themselves intact.
fn wildcard_replaces_values(options: &WildcardAdditionalOptions) -> bool {
    options.opt_replace.is_some()
}

fn projection_output_name<DB: DatabaseLike, D: Copy>(
    expr: &Expr,
    bases: &[FromTableRef<'_, '_, DB, D>],
    output_names: Option<&OutputNameSource<'_, '_, '_, DB, D>>,
    ordinal: usize,
) -> Option<(String, bool)> {
    projected_column_name(expr)
        .or_else(|| three_part_output_name(expr, bases))
        .or_else(|| output_names.and_then(|names| names.get(ordinal)))
}

fn projection_definition<'query, 'db, DB, P>(
    expression: AstRef<'query, 'db, Expr>,
    scope: P::Cursor,
    profile: &mut P,
) -> Result<P::Definition, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    profile.expression_definition(expression, scope)
}

fn index_nested_query_scopes<'query, 'db, DB, P>(
    query: AstRef<'query, 'db, Query>,
    outer_ctes: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<(), LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let scoped_ctes;
    let cte_scope = match &query.get().with {
        Some(_) => {
            let Some(with) = query.try_map(|query| query.with.as_ref()) else {
                return Ok(());
            };
            scoped_ctes = derive_cte_shapes(with, outer_ctes, deriving, parent, profile)?;
            &scoped_ctes
        }
        None => outer_ctes,
    };
    index_nested_set_scopes(
        query.map(|query| query.body.as_ref()),
        cte_scope,
        deriving,
        parent,
        profile,
    )
}

fn index_nested_set_scopes<'query, 'db, DB, P>(
    body: AstRef<'query, 'db, SetExpr>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<(), LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    match body.get() {
        SetExpr::Select(_) => {
            let Some(select) = select_body_ref(body) else {
                return Ok(());
            };
            let scope = collect_select_from(select, cte_scope, deriving, parent, profile)?;
            let cursor = profile.cursor(&scope);
            index_select_expression_queries(select, cte_scope, deriving, cursor, profile)
        }
        SetExpr::Query(_) => {
            let Some(query) = query_body_ref(body) else {
                return Ok(());
            };
            index_nested_query_scopes(query, cte_scope, deriving, parent, profile)
        }
        SetExpr::SetOperation { .. } => {
            let Some((left, right)) = set_operation_arms(body) else {
                return Ok(());
            };
            index_nested_set_scopes(left, cte_scope, deriving, parent, profile)?;
            index_nested_set_scopes(right, cte_scope, deriving, parent, profile)
        }
        _ => Ok(()),
    }
}

fn select_item_expression<'query, 'db>(
    item: AstRef<'query, 'db, SelectItem>,
) -> Option<AstRef<'query, 'db, Expr>> {
    item.try_map(|item| match item {
        SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias { expr: expression, .. }
        | SelectItem::ExprWithAliases { expr: expression, .. }
        | SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::Expr(expression),
            _,
        ) => Some(expression),
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(_), _)
        | SelectItem::Wildcard(_) => None,
    })
}

fn index_select_expression_queries<'query, 'db, DB, P>(
    select: AstRef<'query, 'db, Select>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<(), LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    for item in select.map(|select| select.projection.as_slice()).iter() {
        if let Some(expression) = select_item_expression(item) {
            index_expression_queries(expression, cte_scope, deriving, parent, profile)?;
        }
    }
    let optional_expressions = [
        select.try_map(|select| select.prewhere.as_ref()),
        select.try_map(|select| select.selection.as_ref()),
        select.try_map(|select| select.having.as_ref()),
        select.try_map(|select| select.qualify.as_ref()),
    ];
    for expression in optional_expressions.into_iter().flatten() {
        index_expression_queries(expression, cte_scope, deriving, parent, profile)?;
    }
    if let Some(expressions) = select.try_map(|select| {
        match &select.group_by {
            GroupByExpr::Expressions(expressions, _) => Some(expressions.as_slice()),
            GroupByExpr::All(_) => None,
        }
    }) {
        for expression in expressions.iter() {
            index_expression_queries(expression, cte_scope, deriving, parent, profile)?;
        }
    }
    for expressions in [
        select.map(|select| select.cluster_by.as_slice()),
        select.map(|select| select.distribute_by.as_slice()),
    ] {
        for expression in expressions.iter() {
            index_expression_queries(expression, cte_scope, deriving, parent, profile)?;
        }
    }
    for order in select.map(|select| select.sort_by.as_slice()).iter() {
        index_expression_queries(
            order.map(|order| &order.expr),
            cte_scope,
            deriving,
            parent,
            profile,
        )?;
    }
    Ok(())
}

struct NestedQueryIndexer<'walk, 'derive, 'query, 'db, DB, P>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    cte_scope: &'walk [CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'derive, 'db, DB>,
    parent: P::Cursor,
    profile: &'walk mut P,
}

impl<'query, 'db, DB, P> NestedQueryIndexer<'_, '_, 'query, 'db, DB, P>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    fn query(&mut self, query: AstRef<'query, 'db, Query>) -> Result<(), LookupError> {
        index_nested_query_scopes(query, self.cte_scope, self.deriving, self.parent, self.profile)
    }

    fn child<T: ?Sized>(
        &mut self,
        node: AstRef<'query, 'db, T>,
        child: impl for<'source> FnOnce(&'source T) -> Option<&'source Expr>,
    ) -> Result<(), LookupError> {
        let Some(expression) = node.try_map(child) else {
            return Ok(());
        };
        self.expression(expression)
    }

    fn children<T: ?Sized>(
        &mut self,
        node: AstRef<'query, 'db, T>,
        children: impl for<'source> FnOnce(&'source T) -> Option<&'source [Expr]>,
    ) -> Result<(), LookupError> {
        let Some(expressions) = node.try_map(children) else {
            return Ok(());
        };
        for expression in expressions.iter() {
            self.expression(expression)?;
        }
        Ok(())
    }

    fn query_child<T: ?Sized>(
        &mut self,
        node: AstRef<'query, 'db, T>,
        child: impl for<'source> FnOnce(&'source T) -> Option<&'source Query>,
    ) -> Result<(), LookupError> {
        let Some(query) = node.try_map(child) else {
            return Ok(());
        };
        self.query(query)
    }

    fn access(&mut self, access: AstRef<'query, 'db, AccessExpr>) -> Result<(), LookupError> {
        match access.get() {
            AccessExpr::Dot(_) => {
                self.child(access, |access| {
                    let AccessExpr::Dot(expression) = access else {
                        return None;
                    };
                    Some(expression)
                })
            }
            AccessExpr::Subscript(_) => {
                let Some(subscript) = access.try_map(|access| {
                    let AccessExpr::Subscript(subscript) = access else {
                        return None;
                    };
                    Some(subscript)
                }) else {
                    return Ok(());
                };
                self.subscript(subscript)
            }
        }
    }

    fn subscript(&mut self, subscript: AstRef<'query, 'db, Subscript>) -> Result<(), LookupError> {
        match subscript.get() {
            Subscript::Index { .. } => {
                self.child(subscript, |subscript| {
                    let Subscript::Index { index } = subscript else {
                        return None;
                    };
                    Some(index)
                })
            }
            Subscript::Slice { .. } => {
                self.child(subscript, |subscript| {
                    let Subscript::Slice { lower_bound, .. } = subscript else {
                        return None;
                    };
                    lower_bound.as_ref()
                })?;
                self.child(subscript, |subscript| {
                    let Subscript::Slice { upper_bound, .. } = subscript else {
                        return None;
                    };
                    upper_bound.as_ref()
                })?;
                self.child(subscript, |subscript| {
                    let Subscript::Slice { stride, .. } = subscript else {
                        return None;
                    };
                    stride.as_ref()
                })
            }
        }
    }

    fn json_path_element(
        &mut self,
        element: AstRef<'query, 'db, JsonPathElem>,
    ) -> Result<(), LookupError> {
        self.child(element, |element| {
            match element {
                JsonPathElem::Bracket { key } | JsonPathElem::ColonBracket { key } => Some(key),
                JsonPathElem::Dot { .. } => None,
            }
        })
    }

    fn function_argument_expression(
        &mut self,
        argument: AstRef<'query, 'db, FunctionArgExpr>,
    ) -> Result<(), LookupError> {
        match argument.get() {
            FunctionArgExpr::Expr(_) => {
                self.child(argument, |argument| {
                    let FunctionArgExpr::Expr(expression) = argument else {
                        return None;
                    };
                    Some(expression)
                })
            }
            FunctionArgExpr::WildcardWithOptions(_) => {
                let Some(options) = argument.try_map(|argument| {
                    let FunctionArgExpr::WildcardWithOptions(options) = argument else {
                        return None;
                    };
                    Some(options)
                }) else {
                    return Ok(());
                };
                self.wildcard_options(options)
            }
            FunctionArgExpr::QualifiedWildcard(_) | FunctionArgExpr::Wildcard => Ok(()),
        }
    }

    fn function_argument(
        &mut self,
        argument: AstRef<'query, 'db, FunctionArg>,
    ) -> Result<(), LookupError> {
        if matches!(argument.get(), FunctionArg::ExprNamed { .. }) {
            self.child(argument, |argument| {
                let FunctionArg::ExprNamed { name, .. } = argument else {
                    return None;
                };
                Some(name)
            })?;
        }
        let Some(value) = argument.try_map(|argument| {
            match argument {
                FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. }
                | FunctionArg::Unnamed(arg) => Some(arg),
            }
        }) else {
            return Ok(());
        };
        self.function_argument_expression(value)
    }

    fn order_by(&mut self, order: AstRef<'query, 'db, OrderByExpr>) -> Result<(), LookupError> {
        self.child(order, |order| Some(&order.expr))?;
        let Some(fill) = order.try_map(|order| order.with_fill.as_ref()) else {
            return Ok(());
        };
        self.child(fill, |fill| fill.from.as_ref())?;
        self.child(fill, |fill| fill.to.as_ref())?;
        self.child(fill, |fill| fill.step.as_ref())
    }

    fn window_bound(
        &mut self,
        bound: AstRef<'query, 'db, WindowFrameBound>,
    ) -> Result<(), LookupError> {
        self.child(bound, |bound| {
            match bound {
                WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => {
                    value.as_deref()
                }
                WindowFrameBound::CurrentRow => None,
            }
        })
    }

    fn window(&mut self, window: AstRef<'query, 'db, WindowType>) -> Result<(), LookupError> {
        let Some(specification) = window.try_map(|window| {
            let WindowType::WindowSpec(specification) = window else {
                return None;
            };
            Some(specification)
        }) else {
            return Ok(());
        };
        self.children(specification, |specification| Some(specification.partition_by.as_slice()))?;
        for order in specification.map(|specification| specification.order_by.as_slice()).iter() {
            self.order_by(order)?;
        }
        let start = specification.map(|specification| &specification.window_frame);
        if let Some(bound) = start.try_map(|frame| frame.as_ref().map(|frame| &frame.start_bound)) {
            self.window_bound(bound)?;
        }
        if let Some(bound) =
            start.try_map(|frame| frame.as_ref().and_then(|frame| frame.end_bound.as_ref()))
        {
            self.window_bound(bound)?;
        }
        Ok(())
    }

    fn function_clause(
        &mut self,
        clause: AstRef<'query, 'db, FunctionArgumentClause>,
    ) -> Result<(), LookupError> {
        match clause.get() {
            FunctionArgumentClause::Where(_) | FunctionArgumentClause::Limit(_) => {
                self.child(clause, |clause| {
                    match clause {
                        FunctionArgumentClause::Where(expression)
                        | FunctionArgumentClause::Limit(expression) => Some(expression),
                        _ => None,
                    }
                })
            }
            FunctionArgumentClause::OrderBy(_) => {
                let Some(orders) = clause.try_map(|clause| {
                    let FunctionArgumentClause::OrderBy(orders) = clause else {
                        return None;
                    };
                    Some(orders.as_slice())
                }) else {
                    return Ok(());
                };
                for order in orders.iter() {
                    self.order_by(order)?;
                }
                Ok(())
            }
            FunctionArgumentClause::OnOverflow(_) => {
                self.child(clause, |clause| {
                    let FunctionArgumentClause::OnOverflow(
                        sqlparser::ast::ListAggOnOverflow::Truncate { filler, .. },
                    ) = clause
                    else {
                        return None;
                    };
                    filler.as_deref()
                })
            }
            FunctionArgumentClause::Having(_) => {
                self.child(clause, |clause| {
                    let FunctionArgumentClause::Having(bound) = clause else {
                        return None;
                    };
                    Some(&bound.1)
                })
            }
            FunctionArgumentClause::IgnoreOrRespectNulls(_)
            | FunctionArgumentClause::Separator(_)
            | FunctionArgumentClause::JsonNullClause(_)
            | FunctionArgumentClause::JsonReturningClause(_) => Ok(()),
        }
    }

    fn function_arguments(
        &mut self,
        arguments: AstRef<'query, 'db, FunctionArguments>,
    ) -> Result<(), LookupError> {
        match arguments.get() {
            FunctionArguments::None => Ok(()),
            FunctionArguments::Subquery(_) => {
                self.query_child(arguments, |arguments| {
                    let FunctionArguments::Subquery(query) = arguments else {
                        return None;
                    };
                    Some(query.as_ref())
                })
            }
            FunctionArguments::List(_) => {
                let Some(values) = arguments.try_map(|arguments| {
                    let FunctionArguments::List(list) = arguments else {
                        return None;
                    };
                    Some(list.args.as_slice())
                }) else {
                    return Ok(());
                };
                for value in values.iter() {
                    self.function_argument(value)?;
                }
                let Some(clauses) = arguments.try_map(|arguments| {
                    let FunctionArguments::List(list) = arguments else {
                        return None;
                    };
                    Some(list.clauses.as_slice())
                }) else {
                    return Ok(());
                };
                for clause in clauses.iter() {
                    self.function_clause(clause)?;
                }
                Ok(())
            }
        }
    }

    fn function(&mut self, function: AstRef<'query, 'db, Function>) -> Result<(), LookupError> {
        self.function_arguments(function.map(|function| &function.parameters))?;
        self.function_arguments(function.map(|function| &function.args))?;
        self.child(function, |function| function.filter.as_deref())?;
        if let Some(window) = function.try_map(|function| function.over.as_ref()) {
            self.window(window)?;
        }
        for order in function.map(|function| function.within_group.as_slice()).iter() {
            self.order_by(order)?;
        }
        Ok(())
    }

    fn wildcard_options(
        &mut self,
        options: AstRef<'query, 'db, WildcardAdditionalOptions>,
    ) -> Result<(), LookupError> {
        let Some(items) = options.try_map(|options| {
            options.opt_replace.as_ref().map(|replace| replace.items.as_slice())
        }) else {
            return Ok(());
        };
        for item in items.iter() {
            self.child(item, |item| Some(&item.expr))?;
        }
        Ok(())
    }

    fn case_when(&mut self, case: AstRef<'query, 'db, CaseWhen>) -> Result<(), LookupError> {
        self.child(case, |case| Some(&case.condition))?;
        self.child(case, |case| Some(&case.result))
    }

    fn dictionary_field(
        &mut self,
        field: AstRef<'query, 'db, DictionaryField>,
    ) -> Result<(), LookupError> {
        self.child(field, |field| Some(field.value.as_ref()))
    }

    fn map_entry(&mut self, entry: AstRef<'query, 'db, MapEntry>) -> Result<(), LookupError> {
        self.child(entry, |entry| Some(entry.key.as_ref()))?;
        self.child(entry, |entry| Some(entry.value.as_ref()))
    }

    fn query_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::InSubquery { .. } => {
                self.child(expression, |expression| {
                    let Expr::InSubquery { expr, .. } = expression else {
                        return None;
                    };
                    Some(expr.as_ref())
                })?;
                self.query_child(expression, |expression| {
                    let Expr::InSubquery { subquery, .. } = expression else {
                        return None;
                    };
                    Some(subquery.as_ref())
                })
            }
            Expr::Exists { .. } | Expr::Subquery(_) => {
                self.query_child(expression, |expression| {
                    match expression {
                        Expr::Exists { subquery, .. } | Expr::Subquery(subquery) => {
                            Some(subquery.as_ref())
                        }
                        _ => None,
                    }
                })
            }
            _ => Ok(()),
        }
    }

    fn access_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::CompoundFieldAccess { .. } => {
                self.child(expression, |expression| {
                    let Expr::CompoundFieldAccess { root, .. } = expression else {
                        return None;
                    };
                    Some(root.as_ref())
                })?;
                let Some(chain) = expression.try_map(|expression| {
                    let Expr::CompoundFieldAccess { access_chain, .. } = expression else {
                        return None;
                    };
                    Some(access_chain.as_slice())
                }) else {
                    return Ok(());
                };
                for access in chain.iter() {
                    self.access(access)?;
                }
                Ok(())
            }
            Expr::JsonAccess { .. } => {
                self.child(expression, |expression| {
                    let Expr::JsonAccess { value, .. } = expression else {
                        return None;
                    };
                    Some(value.as_ref())
                })?;
                let Some(path) = expression.try_map(|expression| {
                    let Expr::JsonAccess { path, .. } = expression else {
                        return None;
                    };
                    Some(path.path.as_slice())
                }) else {
                    return Ok(());
                };
                for element in path.iter() {
                    self.json_path_element(element)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn operator_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsJson { .. }
            | Expr::IsNormalized { .. }
            | Expr::UnaryOp { .. }
            | Expr::Cast { .. }
            | Expr::Extract { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Collate { .. }
            | Expr::Nested(_)
            | Expr::Prefixed { .. }
            | Expr::Named { .. }
            | Expr::Interval(_)
            | Expr::OuterJoin(_)
            | Expr::Prior(_)
            | Expr::Lambda(_) => self.child(expression, unary_expression_child),
            Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::InUnnest { .. }
            | Expr::BinaryOp { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::RLike { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::AtTimeZone { .. }
            | Expr::Position { .. }
            | Expr::MemberOf(_) => {
                self.child(expression, left_expression_child)?;
                self.child(expression, right_expression_child)
            }
            _ => Ok(()),
        }
    }

    fn list_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::InList { .. } => {
                self.child(expression, |expression| {
                    let Expr::InList { expr, .. } = expression else {
                        return None;
                    };
                    Some(expr.as_ref())
                })?;
                self.children(expression, |expression| {
                    let Expr::InList { list, .. } = expression else {
                        return None;
                    };
                    Some(list.as_slice())
                })
            }
            Expr::Between { .. } => {
                for child in
                    [between_expression as fn(&Expr) -> Option<&Expr>, between_low, between_high]
                {
                    self.child(expression, child)?;
                }
                Ok(())
            }
            Expr::Convert { .. } => {
                self.child(expression, unary_expression_child)?;
                self.children(expression, |expression| {
                    let Expr::Convert { styles, .. } = expression else {
                        return None;
                    };
                    Some(styles.as_slice())
                })
            }
            Expr::Substring { .. } => {
                self.child(expression, substring_expression)?;
                self.child(expression, substring_start)?;
                self.child(expression, substring_length)
            }
            Expr::Trim { .. } => {
                self.child(expression, trim_expression)?;
                self.child(expression, trim_what)?;
                self.children(expression, |expression| {
                    let Expr::Trim { trim_characters, .. } = expression else {
                        return None;
                    };
                    trim_characters.as_deref()
                })
            }
            Expr::Overlay { .. } => {
                for child in [
                    overlay_expression as fn(&Expr) -> Option<&Expr>,
                    overlay_value,
                    overlay_start,
                    overlay_length,
                ] {
                    self.child(expression, child)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn call_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::Function(_) => {
                let Some(function) = expression.try_map(|expression| {
                    let Expr::Function(function) = expression else {
                        return None;
                    };
                    Some(function)
                }) else {
                    return Ok(());
                };
                self.function(function)
            }
            Expr::Case { .. } => {
                self.child(expression, |expression| {
                    let Expr::Case { operand, .. } = expression else {
                        return None;
                    };
                    operand.as_deref()
                })?;
                let Some(cases) = expression.try_map(|expression| {
                    let Expr::Case { conditions, .. } = expression else {
                        return None;
                    };
                    Some(conditions.as_slice())
                }) else {
                    return Ok(());
                };
                for case in cases.iter() {
                    self.case_when(case)?;
                }
                self.child(expression, |expression| {
                    let Expr::Case { else_result, .. } = expression else {
                        return None;
                    };
                    else_result.as_deref()
                })
            }
            _ => Ok(()),
        }
    }

    fn collection_expression(
        &mut self,
        expression: AstRef<'query, 'db, Expr>,
    ) -> Result<(), LookupError> {
        match expression.get() {
            Expr::GroupingSets(_) | Expr::Cube(_) | Expr::Rollup(_) => {
                let Some(groups) = expression.try_map(|expression| {
                    match expression {
                        Expr::GroupingSets(groups) | Expr::Cube(groups) | Expr::Rollup(groups) => {
                            Some(groups.as_slice())
                        }
                        _ => None,
                    }
                }) else {
                    return Ok(());
                };
                for group in groups.iter() {
                    self.children(group, |group| Some(group.as_slice()))?;
                }
                Ok(())
            }
            Expr::Tuple(_) | Expr::Struct { .. } | Expr::Array(_) => {
                self.children(expression, |expression| {
                    match expression {
                        Expr::Tuple(expressions) => Some(expressions.as_slice()),
                        Expr::Struct { values, .. } => Some(values.as_slice()),
                        Expr::Array(array) => Some(array.elem.as_slice()),
                        _ => None,
                    }
                })
            }
            Expr::Dictionary(_) => {
                let Some(fields) = expression.try_map(|expression| {
                    let Expr::Dictionary(fields) = expression else {
                        return None;
                    };
                    Some(fields.as_slice())
                }) else {
                    return Ok(());
                };
                for field in fields.iter() {
                    self.dictionary_field(field)?;
                }
                Ok(())
            }
            Expr::Map(_) => {
                let Some(entries) = expression.try_map(|expression| {
                    let Expr::Map(map) = expression else {
                        return None;
                    };
                    Some(map.entries.as_slice())
                }) else {
                    return Ok(());
                };
                for entry in entries.iter() {
                    self.map_entry(entry)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn expression(&mut self, expression: AstRef<'query, 'db, Expr>) -> Result<(), LookupError> {
        match expression.get() {
            Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => {
                self.query_expression(expression)
            }
            Expr::CompoundFieldAccess { .. } | Expr::JsonAccess { .. } => {
                self.access_expression(expression)
            }
            Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(_, _)
            | Expr::IsNotDistinctFrom(_, _)
            | Expr::IsJson { .. }
            | Expr::IsNormalized { .. }
            | Expr::InUnnest { .. }
            | Expr::BinaryOp { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::RLike { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::UnaryOp { .. }
            | Expr::Cast { .. }
            | Expr::AtTimeZone { .. }
            | Expr::Extract { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Collate { .. }
            | Expr::Nested(_)
            | Expr::Prefixed { .. }
            | Expr::Named { .. }
            | Expr::Interval(_)
            | Expr::OuterJoin(_)
            | Expr::Prior(_)
            | Expr::Lambda(_)
            | Expr::MemberOf(_) => self.operator_expression(expression),
            Expr::InList { .. }
            | Expr::Between { .. }
            | Expr::Convert { .. }
            | Expr::Substring { .. }
            | Expr::Trim { .. }
            | Expr::Overlay { .. } => self.list_expression(expression),
            Expr::Function(_) | Expr::Case { .. } => self.call_expression(expression),
            Expr::GroupingSets(_)
            | Expr::Cube(_)
            | Expr::Rollup(_)
            | Expr::Tuple(_)
            | Expr::Struct { .. }
            | Expr::Dictionary(_)
            | Expr::Map(_)
            | Expr::Array(_) => self.collection_expression(expression),
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Value(_)
            | Expr::TypedString(_)
            | Expr::MatchAgainst { .. }
            | Expr::Wildcard(_)
            | Expr::QualifiedWildcard(_, _) => Ok(()),
        }
    }
}

fn unary_expression_child(expression: &Expr) -> Option<&Expr> {
    match expression {
        Expr::IsFalse(expression)
        | Expr::IsNotFalse(expression)
        | Expr::IsTrue(expression)
        | Expr::IsNotTrue(expression)
        | Expr::IsNull(expression)
        | Expr::IsNotNull(expression)
        | Expr::IsUnknown(expression)
        | Expr::IsNotUnknown(expression)
        | Expr::Nested(expression)
        | Expr::OuterJoin(expression)
        | Expr::Prior(expression) => Some(expression.as_ref()),
        Expr::IsJson { expr, .. }
        | Expr::IsNormalized { expr, .. }
        | Expr::UnaryOp { expr, .. }
        | Expr::Convert { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::Named { expr, .. } => Some(expr.as_ref()),
        Expr::Prefixed { value, .. } => Some(value.as_ref()),
        Expr::Interval(interval) => Some(interval.value.as_ref()),
        Expr::Lambda(lambda) => Some(lambda.body.as_ref()),
        _ => None,
    }
}

fn left_expression_child(expression: &Expr) -> Option<&Expr> {
    match expression {
        Expr::IsDistinctFrom(left, _)
        | Expr::IsNotDistinctFrom(left, _)
        | Expr::BinaryOp { left, .. }
        | Expr::AnyOp { left, .. }
        | Expr::AllOp { left, .. } => Some(left.as_ref()),
        Expr::InUnnest { expr, .. }
        | Expr::Like { expr, .. }
        | Expr::ILike { expr, .. }
        | Expr::SimilarTo { expr, .. }
        | Expr::RLike { expr, .. }
        | Expr::Position { expr, .. } => Some(expr.as_ref()),
        Expr::AtTimeZone { timestamp, .. } => Some(timestamp.as_ref()),
        Expr::MemberOf(member) => Some(member.value.as_ref()),
        _ => None,
    }
}

fn right_expression_child(expression: &Expr) -> Option<&Expr> {
    match expression {
        Expr::IsDistinctFrom(_, right)
        | Expr::IsNotDistinctFrom(_, right)
        | Expr::BinaryOp { right, .. }
        | Expr::AnyOp { right, .. }
        | Expr::AllOp { right, .. } => Some(right.as_ref()),
        Expr::InUnnest { array_expr, .. } => Some(array_expr.as_ref()),
        Expr::Like { pattern, .. }
        | Expr::ILike { pattern, .. }
        | Expr::SimilarTo { pattern, .. }
        | Expr::RLike { pattern, .. } => Some(pattern.as_ref()),
        Expr::AtTimeZone { time_zone, .. } => Some(time_zone.as_ref()),
        Expr::Position { r#in, .. } => Some(r#in.as_ref()),
        Expr::MemberOf(member) => Some(member.array.as_ref()),
        _ => None,
    }
}

fn between_expression(expression: &Expr) -> Option<&Expr> {
    let Expr::Between { expr, .. } = expression else {
        return None;
    };
    Some(expr.as_ref())
}

fn between_low(expression: &Expr) -> Option<&Expr> {
    let Expr::Between { low, .. } = expression else {
        return None;
    };
    Some(low.as_ref())
}

fn between_high(expression: &Expr) -> Option<&Expr> {
    let Expr::Between { high, .. } = expression else {
        return None;
    };
    Some(high.as_ref())
}

fn substring_expression(expression: &Expr) -> Option<&Expr> {
    let Expr::Substring { expr, .. } = expression else {
        return None;
    };
    Some(expr.as_ref())
}

fn substring_start(expression: &Expr) -> Option<&Expr> {
    let Expr::Substring { substring_from, .. } = expression else {
        return None;
    };
    substring_from.as_deref()
}

fn substring_length(expression: &Expr) -> Option<&Expr> {
    let Expr::Substring { substring_for, .. } = expression else {
        return None;
    };
    substring_for.as_deref()
}

fn trim_expression(expression: &Expr) -> Option<&Expr> {
    let Expr::Trim { expr, .. } = expression else {
        return None;
    };
    Some(expr.as_ref())
}

fn trim_what(expression: &Expr) -> Option<&Expr> {
    let Expr::Trim { trim_what, .. } = expression else {
        return None;
    };
    trim_what.as_deref()
}

fn overlay_expression(expression: &Expr) -> Option<&Expr> {
    let Expr::Overlay { expr, .. } = expression else {
        return None;
    };
    Some(expr.as_ref())
}

fn overlay_value(expression: &Expr) -> Option<&Expr> {
    let Expr::Overlay { overlay_what, .. } = expression else {
        return None;
    };
    Some(overlay_what.as_ref())
}

fn overlay_start(expression: &Expr) -> Option<&Expr> {
    let Expr::Overlay { overlay_from, .. } = expression else {
        return None;
    };
    Some(overlay_from.as_ref())
}

fn overlay_length(expression: &Expr) -> Option<&Expr> {
    let Expr::Overlay { overlay_for, .. } = expression else {
        return None;
    };
    overlay_for.as_deref()
}

fn index_expression_queries<'query, 'db, DB, P>(
    expression: AstRef<'query, 'db, Expr>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<(), LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    NestedQueryIndexer { cte_scope, deriving, parent, profile }.expression(expression)
}

fn derived_projection_source<'db, DB: DatabaseLike, D: Copy>(
    expression: &Expr,
    scope: &FromScope<'_, 'db, DB, D>,
    opaque: D,
) -> Result<Option<&'db DB::Table>, LookupError> {
    match column_source(expression, scope, scope.from_entry_count, opaque, false) {
        Ok(source) => Ok(source),
        Err(LookupError::AmbiguousTableLookup { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn append_select_item<'query, 'db, DB, P>(
    item: AstRef<'query, 'db, SelectItem>,
    scope: &P::Scope,
    scope_cursor: P::Cursor,
    output_names: Option<&OutputNameSource<'_, 'query, 'db, DB, P::Definition>>,
    profile: &mut P,
    columns: &mut Vec<DerivedColumn<'query, 'db, DB, P::Definition>>,
) -> Result<bool, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    match item.get() {
        SelectItem::UnnamedExpr(_) => {
            let Some(expression) = select_item_expression(item) else {
                return Ok(false);
            };
            let opaque = profile.opaque_definition();
            let (named, source) = {
                let data = profile.scope(scope);
                (
                    projection_output_name(
                        expression.get(),
                        &data.bases,
                        output_names,
                        columns.len(),
                    ),
                    derived_projection_source(expression.get(), data, opaque)?,
                )
            };
            let Some((name, quoted)) = named else {
                return Ok(false);
            };
            let definition = projection_definition(expression, scope_cursor, profile)?;
            columns.push(DerivedColumn {
                name,
                quoted,
                source,
                definition,
                marker: core::marker::PhantomData,
            });
            Ok(true)
        }
        SelectItem::ExprWithAlias { alias, .. } => {
            let Some(expression) = select_item_expression(item) else {
                return Ok(false);
            };
            let opaque = profile.opaque_definition();
            let source = derived_projection_source(expression.get(), profile.scope(scope), opaque)?;
            let definition = projection_definition(expression, scope_cursor, profile)?;
            columns.push(DerivedColumn {
                name: alias.value.clone(),
                quoted: alias.quote_style.is_some(),
                source,
                definition,
                marker: core::marker::PhantomData,
            });
            Ok(true)
        }
        SelectItem::ExprWithAliases { .. }
        | SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _) => Ok(false),
        SelectItem::Wildcard(options) => {
            let data = profile.scope(scope);
            if data.has_opaque() || wildcard_reshapes_output(options) {
                return Ok(false);
            }
            let opaque = profile.opaque_definition();
            push_wildcard_columns(data, columns, opaque);
            Ok(true)
        }
        SelectItem::QualifiedWildcard(
            SelectItemQualifiedWildcardKind::ObjectName(object_name),
            options,
        ) => {
            if wildcard_reshapes_output(options) {
                return Ok(false);
            }
            let Some(expansion) = ({
                let data = profile.scope(scope);
                expand_qualified_wildcard(&data.bases, &data.derived, object_name)
            }) else {
                return Ok(false);
            };
            columns.extend(expansion);
            Ok(true)
        }
    }
}

/// Derives the output shape of a plain `SELECT`: each projected column's name
/// and pass-through source, enumerated from the projection (wildcards expand
/// over the `FROM` relations they stand for).
fn derive_select_shape<'query, 'db, DB, P>(
    select: AstRef<'query, 'db, Select>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    output_names: Option<&OutputNameSource<'_, 'query, 'db, DB, P::Definition>>,
    deriving: Deriving<'_, 'db, DB>,
    parent: P::Cursor,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    if !select.get().lateral_views.is_empty()
        || select.get().exclude.is_some()
        || select.get().value_table_mode.is_some()
        || !select.get().connect_by.is_empty()
    {
        return Ok(None);
    }
    let scope = collect_select_from(select, cte_scope, deriving, parent, profile)?;
    let scope_cursor = profile.cursor(&scope);
    if P::INDEX_NESTED_QUERIES {
        index_select_expression_queries(select, cte_scope, deriving, scope_cursor, profile)?;
    }
    let mut columns = Vec::new();
    for item in select.map(|select| select.projection.as_slice()).iter() {
        if !append_select_item(item, &scope, scope_cursor, output_names, profile, &mut columns)? {
            return Ok(None);
        }
    }
    let grouped = match &select.get().group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(expressions, _) => !expressions.is_empty(),
    };
    let outer_join = select.get().from.iter().flat_map(|entry| &entry.joins).any(|join| {
        let (left_nullable, right_nullable) = nullable_sides(&join.join_operator);
        left_nullable || right_nullable
    });
    let reads_non_preserving =
        profile.scope(&scope).derived.iter().any(|relation| !relation.shape.row_preserving);
    let row_preserving = select.get().distinct.is_none()
        && select.get().having.is_none()
        && select.get().qualify.is_none()
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
fn three_part_output_name<DB: DatabaseLike, D: Copy>(
    expr: &Expr,
    bases: &[FromTableRef<'_, '_, DB, D>],
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
        usize::MAX,
    )?;
    base_exposes_column(base, &parts[2])
        .then(|| (parts[2].value.clone(), parts[2].quote_style.is_some()))
}

/// Every column a base relation outputs as a derived column sourced by that
/// table: the relation's `output_names` in declaration order, so an alias
/// column list renames the wildcard expansion too.
fn base_columns<'query, 'db, DB: DatabaseLike, D: Copy>(
    base: &FromTableRef<'query, 'db, DB, D>,
) -> Vec<DerivedColumn<'query, 'db, DB, D>> {
    base.output_columns
        .iter()
        .map(|column| {
            DerivedColumn {
                name: column.name.clone(),
                quoted: column.quoted,
                source: Some(base.table),
                definition: column.definition,
                marker: core::marker::PhantomData,
            }
        })
        .collect()
}

/// The relation a qualified wildcard prefix names.
enum WildcardTarget<'scope, 'query, 'db, DB: DatabaseLike, D: Copy> {
    Base(&'scope FromTableRef<'query, 'db, DB, D>),
    Derived(&'scope DerivedRelationRef<'query, 'db, DB, D>),
}

fn resolve_wildcard_target<'scope, 'query, 'db, DB: DatabaseLike, D: Copy>(
    bases: &'scope [FromTableRef<'query, 'db, DB, D>],
    derived: &'scope [DerivedRelationRef<'query, 'db, DB, D>],
    qualifier: &ObjectName,
    require_row_identity: bool,
) -> Option<WildcardTarget<'scope, 'query, 'db, DB, D>> {
    let (value, quoted) = object_name_last_part(qualifier)?;
    match qualifier.0.len() {
        1 => {
            base_for_qualifier(bases, value, quoted, require_row_identity, usize::MAX)
                .map(WildcardTarget::Base)
                .or_else(|| {
                    derived_for_qualifier(derived, value, quoted, require_row_identity, usize::MAX)
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
                usize::MAX,
            )
            .map(WildcardTarget::Base)
        }
        _ => None,
    }
}

fn expand_qualified_wildcard<'query, 'db, DB: DatabaseLike, D: Copy>(
    bases: &[FromTableRef<'query, 'db, DB, D>],
    derived: &[DerivedRelationRef<'query, 'db, DB, D>],
    qualifier: &ObjectName,
) -> Option<Vec<DerivedColumn<'query, 'db, DB, D>>> {
    match resolve_wildcard_target(bases, derived, qualifier, false)? {
        WildcardTarget::Base(base) => Some(base_columns(base)),
        WildcardTarget::Derived(relation) => Some(relation.shape.columns.clone()),
    }
}

fn apply_alias_columns<'query, 'db, DB: DatabaseLike, D: Copy>(
    shape: Option<DerivedShape<'query, 'db, DB, D>>,
    alias: &TableAlias,
) -> Option<DerivedShape<'query, 'db, DB, D>> {
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

fn find_cte<'query, 'db, 'scope, DB: DatabaseLike, D: Copy>(
    name: &ObjectName,
    cte_scope: &'scope [CteShape<'query, 'db, DB, D>],
) -> Option<&'scope CteShape<'query, 'db, DB, D>> {
    if name.0.len() != 1 {
        return None;
    }
    let (value, quoted) = object_name_last_part(name)?;
    cte_scope
        .iter()
        .rev()
        .find(|cte| identifiers_match(cte.name.value.get(), cte.name.quoted, value, quoted))
}

fn base_exposes_column<DB: DatabaseLike, D: Copy>(
    base: &FromTableRef<'_, '_, DB, D>,
    column: &Ident,
) -> bool {
    base.output_columns.iter().any(|candidate| {
        identifiers_match(
            &candidate.name,
            candidate.quoted,
            column.value.as_str(),
            column.quote_style.is_some(),
        )
    })
}

fn find_base_column<'scope, 'db, DB: DatabaseLike, D: Copy>(
    base: &'scope FromTableRef<'_, 'db, DB, D>,
    column: &Ident,
) -> Option<&'scope BaseColumnRef<'db, DB, D>> {
    base.output_columns.iter().find(|candidate| {
        identifiers_match(
            &candidate.name,
            candidate.quoted,
            column.value.as_str(),
            column.quote_style.is_some(),
        )
    })
}

fn key_matches(key: RelationKey<'_, '_>, value: &str, quoted: bool) -> bool {
    identifiers_match(key.value.get(), key.quoted, value, quoted)
}

fn base_for_qualifier<'scope, 'query, 'db, DB: DatabaseLike, D: Copy>(
    bases: &'scope [FromTableRef<'query, 'db, DB, D>],
    value: &str,
    quoted: bool,
    require_row_identity: bool,
    visible_entries: usize,
) -> Option<&'scope FromTableRef<'query, 'db, DB, D>> {
    bases
        .iter()
        .filter(|base| base.entry_index < visible_entries)
        .filter(|base| !require_row_identity || !base.nullable)
        .find(|base| key_matches(base.key, value, quoted))
}

fn base_for_qualified_name<'scope, 'query, 'db, DB: DatabaseLike, D: Copy>(
    bases: &'scope [FromTableRef<'query, 'db, DB, D>],
    schema: &str,
    schema_quoted: bool,
    table_part: &str,
    table_quoted: bool,
    require_row_identity: bool,
    visible_entries: usize,
) -> Option<&'scope FromTableRef<'query, 'db, DB, D>> {
    bases
        .iter()
        .filter(|base| base.entry_index < visible_entries)
        .filter(|base| !require_row_identity || !base.nullable)
        .find(|base| {
            base.schema_key.is_some_and(|stored| {
                key_matches(stored, schema, schema_quoted)
                    && key_matches(base.key, table_part, table_quoted)
            })
        })
}

fn derived_for_qualifier<'scope, 'query, 'db, DB: DatabaseLike, D: Copy>(
    derived: &'scope [DerivedRelationRef<'query, 'db, DB, D>],
    value: &str,
    quoted: bool,
    require_row_identity: bool,
    visible_entries: usize,
) -> Option<&'scope DerivedRelationRef<'query, 'db, DB, D>> {
    derived
        .iter()
        .filter(|relation| relation.entry_index < visible_entries)
        .find(|relation| relation.key.is_some_and(|key| key_matches(key, value, quoted)))
        .filter(|relation| {
            !require_row_identity || (relation.shape.row_preserving && !relation.nullable)
        })
}

enum DerivedMatch<'scope, 'query, 'db, DB: DatabaseLike, D: Copy> {
    Column(&'scope DerivedColumn<'query, 'db, DB, D>),
    Ambiguous,
}

fn find_derived_column<'scope, 'query, 'db, DB: DatabaseLike, D: Copy>(
    columns: &'scope [DerivedColumn<'query, 'db, DB, D>],
    column: &Ident,
) -> Option<DerivedMatch<'scope, 'query, 'db, DB, D>> {
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
        return Some(DerivedMatch::Ambiguous);
    }
    Some(DerivedMatch::Column(first))
}

fn single_source_relation<'db, DB: DatabaseLike, D: Copy>(
    relation: &DerivedRelationRef<'_, 'db, DB, D>,
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

fn relation_key_display<DB: DatabaseLike, D: Copy>(
    relation: &DerivedRelationRef<'_, '_, DB, D>,
) -> String {
    match relation.key {
        Some(key) if key.quoted => format!("\"{}\"", key.value.get()),
        Some(key) => key.value.get().to_string(),
        None => "(subquery)".to_string(),
    }
}

struct ResolvedColumn<'db, DB: DatabaseLike, D: Copy> {
    source: Option<&'db DB::Table>,
    definition: D,
}

impl<DB: DatabaseLike, D: Copy> Copy for ResolvedColumn<'_, DB, D> {}

#[expect(clippy::expl_impl_clone_on_copy, reason = "derive would require DB: Clone")]
impl<DB: DatabaseLike, D: Copy> Clone for ResolvedColumn<'_, DB, D> {
    fn clone(&self) -> Self {
        *self
    }
}

enum LookupOutcome<T> {
    Found(T),
    SearchParent,
    Stop,
}

fn visible_merged_boundary(
    merged: &[MergedName],
    column: &Ident,
    visible_entries: usize,
) -> Option<usize> {
    merged
        .iter()
        .filter(|merged| merged.subsumed <= visible_entries)
        .rev()
        .find(|merged| {
            identifiers_match(
                &merged.name,
                merged.quoted,
                column.value.as_str(),
                column.quote_style.is_some(),
            )
        })
        .map(|merged| merged.subsumed)
}

fn opaque_qualifier_matches(identity: OpaqueIdentity<'_, '_>, qualifier: &[Ident]) -> bool {
    match identity {
        OpaqueIdentity::Known { key, schema } => {
            match qualifier {
                [table] => key_matches(key, table.value.as_str(), table.quote_style.is_some()),
                [schema_part, table] => {
                    schema.is_some_and(|schema| {
                        key_matches(
                            schema,
                            schema_part.value.as_str(),
                            schema_part.quote_style.is_some(),
                        )
                    }) && key_matches(key, table.value.as_str(), table.quote_style.is_some())
                }
                _ => false,
            }
        }
        OpaqueIdentity::Anonymous => false,
        OpaqueIdentity::AnyQualifier => true,
    }
}

fn unqualified_definition_local<'db, DB: DatabaseLike, D: Copy>(
    scope: &FromScope<'_, 'db, DB, D>,
    visible_entries: usize,
    opaque: D,
    column: &Ident,
    require_row_identity: bool,
) -> Result<LookupOutcome<ResolvedColumn<'db, DB, D>>, LookupError> {
    if scope.unqualified_poison
        || scope.opaque.iter().any(|entry| entry.entry_index < visible_entries)
    {
        return Ok(LookupOutcome::Found(ResolvedColumn { source: None, definition: opaque }));
    }
    let mut definitions = Vec::new();
    let mut candidates = Vec::new();
    let boundary = visible_merged_boundary(&scope.merged, column, visible_entries);
    if boundary.is_some() {
        definitions.push(ResolvedColumn { source: None, definition: opaque });
    }
    let start = boundary.unwrap_or(0);
    for base in scope
        .bases
        .iter()
        .filter(|base| base.entry_index >= start && base.entry_index < visible_entries)
    {
        if let Some(base_column) = find_base_column(base, column) {
            definitions.push(ResolvedColumn {
                source: (!require_row_identity || !base.nullable).then_some(base.table),
                definition: base_column.definition,
            });
            candidates.push(render_table_candidate(base.table));
        }
    }
    for relation in scope
        .derived
        .iter()
        .filter(|relation| relation.entry_index >= start && relation.entry_index < visible_entries)
    {
        match find_derived_column(&relation.shape.columns, column) {
            Some(DerivedMatch::Column(found)) => {
                definitions.push(ResolvedColumn {
                    source: (!require_row_identity || !relation.nullable)
                        .then_some(found.source)
                        .flatten(),
                    definition: found.definition,
                });
                candidates.push(relation_key_display(relation));
            }
            Some(DerivedMatch::Ambiguous) => {
                definitions.push(ResolvedColumn { source: None, definition: opaque });
                definitions.push(ResolvedColumn { source: None, definition: opaque });
                candidates.push(relation_key_display(relation));
            }
            None => {}
        }
    }
    match definitions.as_slice() {
        [] => Ok(LookupOutcome::SearchParent),
        [definition] => Ok(LookupOutcome::Found(*definition)),
        _ => {
            candidates.sort_unstable();
            candidates.dedup();
            Err(LookupError::AmbiguousTableLookup { object_name: column.value.clone(), candidates })
        }
    }
}

fn resolve_definition_local<'db, DB: DatabaseLike, D: Copy>(
    scope: &FromScope<'_, 'db, DB, D>,
    visible_entries: usize,
    opaque: D,
    expression: &Expr,
    require_row_identity: bool,
) -> Result<LookupOutcome<ResolvedColumn<'db, DB, D>>, LookupError> {
    match expression {
        Expr::Identifier(column) => {
            unqualified_definition_local(
                scope,
                visible_entries,
                opaque,
                column,
                require_row_identity,
            )
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let column = &parts[1];
            let qualifier = &parts[0];
            let value = qualifier.value.as_str();
            let quoted = qualifier.quote_style.is_some();
            if let Some(base) = base_for_qualifier(
                &scope.bases,
                value,
                quoted,
                require_row_identity,
                visible_entries,
            ) {
                return Ok(match find_base_column(base, column) {
                    Some(column) => {
                        LookupOutcome::Found(ResolvedColumn {
                            source: Some(base.table),
                            definition: column.definition,
                        })
                    }
                    None => LookupOutcome::Stop,
                });
            }
            if let Some(relation) = derived_for_qualifier(
                &scope.derived,
                value,
                quoted,
                require_row_identity,
                visible_entries,
            ) {
                return match find_derived_column(&relation.shape.columns, column) {
                    Some(DerivedMatch::Column(column)) => {
                        Ok(LookupOutcome::Found(ResolvedColumn {
                            source: column.source,
                            definition: column.definition,
                        }))
                    }
                    Some(DerivedMatch::Ambiguous) => {
                        Err(LookupError::AmbiguousTableLookup {
                            object_name: column.value.clone(),
                            candidates: vec![relation_key_display(relation)],
                        })
                    }
                    None => Ok(LookupOutcome::Stop),
                };
            }
            if scope.opaque.iter().any(|entry| {
                entry.entry_index < visible_entries
                    && opaque_qualifier_matches(entry.identity, &parts[..1])
            }) {
                Ok(LookupOutcome::Found(ResolvedColumn { source: None, definition: opaque }))
            } else {
                Ok(LookupOutcome::SearchParent)
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() == 3 => {
            if let Some(base) = base_for_qualified_name(
                &scope.bases,
                parts[0].value.as_str(),
                parts[0].quote_style.is_some(),
                parts[1].value.as_str(),
                parts[1].quote_style.is_some(),
                require_row_identity,
                visible_entries,
            ) {
                return Ok(match find_base_column(base, &parts[2]) {
                    Some(column) => {
                        LookupOutcome::Found(ResolvedColumn {
                            source: Some(base.table),
                            definition: column.definition,
                        })
                    }
                    None => LookupOutcome::Stop,
                });
            }
            if scope.opaque.iter().any(|entry| {
                entry.entry_index < visible_entries
                    && opaque_qualifier_matches(entry.identity, &parts[..2])
            }) {
                Ok(LookupOutcome::Found(ResolvedColumn { source: None, definition: opaque }))
            } else {
                Ok(LookupOutcome::SearchParent)
            }
        }
        _ => Ok(LookupOutcome::Stop),
    }
}

fn column_source<'db, DB: DatabaseLike, D: Copy>(
    expression: &Expr,
    scope: &FromScope<'_, 'db, DB, D>,
    visible_entries: usize,
    opaque: D,
    require_row_identity: bool,
) -> Result<Option<&'db DB::Table>, LookupError> {
    if require_row_identity
        && scope
            .derived
            .iter()
            .filter(|relation| relation.entry_index < visible_entries)
            .any(|relation| !relation.shape.row_preserving)
    {
        return Ok(None);
    }
    match resolve_definition_local(
        scope,
        visible_entries,
        opaque,
        expression,
        require_row_identity,
    )? {
        LookupOutcome::Found(column) => Ok(column.source),
        LookupOutcome::SearchParent | LookupOutcome::Stop => Ok(None),
    }
}

/// A base table's output names with the alias column list applied
/// positionally. PostgreSQL replaces the originals with the aliases (a
/// partial list keeps the tail's own names). More aliases than columns is a
/// mismatch PostgreSQL rejects, reported here as `None` so the relation
/// stays opaque.
fn aliased_output_columns<'query, 'db, DB, P>(
    table: &'db DB::Table,
    alias: Option<AstRef<'query, 'db, TableAlias>>,
    database: &'db DB,
    profile: &mut P,
) -> Result<Option<BaseColumns<'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let mut output_columns = table
        .columns(database)?
        .map(|column| {
            BaseColumnRef {
                name: column.column_name().to_string(),
                quoted: column.column_name_is_quoted(),
                source: table,
                definition: profile.base_definition(table, column),
            }
        })
        .collect::<Vec<_>>();
    if let Some(table_alias) = alias
        && !table_alias.get().columns.is_empty()
    {
        if table_alias.get().columns.len() > output_columns.len() {
            return Ok(None);
        }
        for (column, alias_column) in output_columns.iter_mut().zip(&table_alias.get().columns) {
            alias_column.name.value.clone_into(&mut column.name);
            column.quoted = alias_column.name.quote_style.is_some();
        }
    }
    Ok(Some(output_columns))
}

/// Records a `FROM` relation that names a view, deriving its output columns
/// from the definition the database holds.
///
/// A view's definition is resolved in its own context, so the enclosing
/// statement's `WITH` items are not in scope inside it. Its own column list
/// renames what the definition produces, positionally, and the `FROM` alias's
/// column list then renames again on top, both as PostgreSQL applies them.
///
/// A materialized view answers column references the same way, because a
/// column's declared type is inherited from what it reads and cannot go stale.
/// Its rows, though, are a snapshot taken when it was last populated rather
/// than the current rows of anything, so its shape never preserves row
/// identity and the row-identity question stops there.
///
/// A reference reaching a view already being derived closes a cycle, which
/// PostgreSQL accepts at creation and refuses only on read, so the reference
/// stays opaque rather than recursing.
fn collect_view_factor<'query, 'db, DB, P>(
    name: AstRef<'query, 'db, ObjectName>,
    alias: Option<AstRef<'query, 'db, TableAlias>>,
    key: RelationKey<'query, 'db>,
    schema: Option<RelationKey<'query, 'db>>,
    deriving: Deriving<'_, 'db, DB>,
    profile: &mut P,
) -> Result<FactorContribution<'query, 'db, DB, P::Definition>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let Some(target) = target_name_from_object_name(name.get()) else {
        return Ok(opaque_factor(OpaqueIdentity::Known { key, schema }));
    };
    let (view, row_preserving) =
        if let Some(view) = deriving.database.resolve_target_view(target.clone())? {
            (DerivingView::Plain(view), true)
        } else if let Some(view) = deriving.database.resolve_target_materialized_view(target)? {
            (DerivingView::Materialized(view), false)
        } else {
            return Ok(opaque_factor(OpaqueIdentity::Known { key, schema }));
        };
    let shape = derive_view_shape(view, row_preserving, deriving, profile)?;
    let shape = match alias {
        Some(table_alias) => apply_alias_columns(shape, table_alias.get()),
        None => shape,
    };
    let Some(shape) = shape else {
        return Ok(opaque_factor(OpaqueIdentity::Known { key, schema }));
    };
    let names = derived_column_names(&shape.columns);
    Ok(FactorContribution {
        relation: RelationContribution::Derived(DerivedRelationRef {
            key: Some(key),
            shape,
            nullable: false,
            entry_index: 0,
        }),
        names: Some(names),
    })
}

/// The output shape of a view's definition, with the view's own column list
/// applied, or [`None`] when the definition's columns cannot be enumerated or
/// the reference closes a cycle.
fn derive_view_shape<'query, 'db, DB, P>(
    view: DerivingView<'db, DB>,
    row_preserving: bool,
    deriving: Deriving<'_, 'db, DB>,
    profile: &mut P,
) -> Result<Option<DerivedShape<'query, 'db, DB, P::Definition>>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    if deriving.is_deriving(view) {
        return Ok(None);
    }
    let (definition, declared) = match view {
        DerivingView::Plain(view) => (view.definition(), view.declared_column_names()),
        DerivingView::Materialized(view) => (view.definition(), view.declared_column_names()),
    };
    let frame = DerivingFrame { view, parent: deriving.views };
    let inner = Deriving { database: deriving.database, views: Some(&frame) };
    let output_names = (!declared.is_empty()).then_some(OutputNameSource::Declared(declared));
    let parent = profile.no_parent();
    let Some(mut shape) = derive_query_shape(
        AstRef::Database(definition),
        &[],
        output_names,
        inner,
        parent,
        profile,
    )?
    else {
        return Ok(None);
    };
    if declared.len() > shape.columns.len() {
        return Ok(None);
    }
    for (column, (declared_name, declared_quoted)) in shape.columns.iter_mut().zip(declared.iter())
    {
        declared_name.clone_into(&mut column.name);
        column.quoted = *declared_quoted;
    }
    shape.row_preserving &= row_preserving;
    Ok(Some(shape))
}
fn derived_column_names<DB: DatabaseLike, D: Copy>(
    columns: &[DerivedColumn<'_, '_, DB, D>],
) -> Vec<(String, bool)> {
    columns.iter().map(|column| (column.name.clone(), column.quoted)).collect()
}

fn alias_key<'query, 'db>(alias: AstRef<'query, 'db, TableAlias>) -> RelationKey<'query, 'db> {
    RelationKey {
        value: alias.map(|alias| alias.name.value.as_str()),
        quoted: alias.get().name.quote_style.is_some(),
    }
}

fn object_name_key<'query, 'db>(
    name: AstRef<'query, 'db, ObjectName>,
) -> Option<RelationKey<'query, 'db>> {
    match name {
        AstRef::Query(name) => {
            let part = name.0.last()?.as_ident()?;
            Some(RelationKey {
                value: AstRef::Query(part.value.as_str()),
                quoted: part.quote_style.is_some(),
            })
        }
        AstRef::Database(name) => {
            let part = name.0.last()?.as_ident()?;
            Some(RelationKey {
                value: AstRef::Database(part.value.as_str()),
                quoted: part.quote_style.is_some(),
            })
        }
    }
}

fn object_name_schema<'query, 'db>(
    name: AstRef<'query, 'db, ObjectName>,
) -> Option<RelationKey<'query, 'db>> {
    match name {
        AstRef::Query(name) if name.0.len() == 2 => {
            let part = name.0.first()?.as_ident()?;
            Some(RelationKey {
                value: AstRef::Query(part.value.as_str()),
                quoted: part.quote_style.is_some(),
            })
        }
        AstRef::Database(name) if name.0.len() == 2 => {
            let part = name.0.first()?.as_ident()?;
            Some(RelationKey {
                value: AstRef::Database(part.value.as_str()),
                quoted: part.quote_style.is_some(),
            })
        }
        AstRef::Query(_) | AstRef::Database(_) => None,
    }
}

fn stored_schema_key<'query, DB: DatabaseLike>(table: &DB::Table) -> RelationKey<'query, '_> {
    RelationKey {
        value: AstRef::Database(table.table_schema().unwrap_or("public")),
        quoted: table.table_schema().is_some() && table.table_schema_is_quoted(),
    }
}

fn opaque_factor<'query, 'db, DB: DatabaseLike, D: Copy>(
    identity: OpaqueIdentity<'query, 'db>,
) -> FactorContribution<'query, 'db, DB, D> {
    FactorContribution { relation: RelationContribution::Opaque(identity), names: None }
}

fn factor_alias<'query, 'db>(
    factor: AstRef<'query, 'db, TableFactor>,
) -> Option<AstRef<'query, 'db, TableAlias>> {
    factor.try_map(|factor| {
        match factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        }
    })
}

fn factor_name<'query, 'db>(
    factor: AstRef<'query, 'db, TableFactor>,
) -> Option<AstRef<'query, 'db, ObjectName>> {
    factor.try_map(|factor| {
        match factor {
            TableFactor::Table { name, .. } | TableFactor::Function { name, .. } => Some(name),
            _ => None,
        }
    })
}

fn derived_factor_query<'query, 'db>(
    factor: AstRef<'query, 'db, TableFactor>,
) -> Option<AstRef<'query, 'db, Query>> {
    factor.try_map(|factor| {
        match factor {
            TableFactor::Derived { subquery, .. } => Some(subquery.as_ref()),
            _ => None,
        }
    })
}

fn table_function_expression<'query, 'db>(
    factor: AstRef<'query, 'db, TableFactor>,
) -> Option<AstRef<'query, 'db, Expr>> {
    factor.try_map(|factor| {
        match factor {
            TableFactor::TableFunction { expr, .. } => Some(expr),
            _ => None,
        }
    })
}

fn collect_table_factor<'query, 'db, DB, P>(
    factor: AstRef<'query, 'db, TableFactor>,
    has_arguments: bool,
    deriving: Deriving<'_, 'db, DB>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    profile: &mut P,
) -> Result<FactorContribution<'query, 'db, DB, P::Definition>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    let Some(name) = factor_name(factor) else {
        return Ok(opaque_factor(OpaqueIdentity::Anonymous));
    };
    let alias = factor_alias(factor);
    let Some(key) = alias.map(alias_key).or_else(|| object_name_key(name)) else {
        return Ok(opaque_factor(OpaqueIdentity::Anonymous));
    };
    let schema = alias.is_none().then(|| object_name_schema(name)).flatten();
    if has_arguments {
        return Ok(opaque_factor(OpaqueIdentity::Known { key, schema: None }));
    }
    if let Some(cte) = find_cte(name.get(), cte_scope) {
        let shape = match (cte.shape.clone(), alias) {
            (Some(shape), Some(table_alias)) => apply_alias_columns(Some(shape), table_alias.get()),
            (shape, _) => shape,
        };
        let Some(shape) = shape else {
            return Ok(opaque_factor(OpaqueIdentity::Known { key, schema: None }));
        };
        let names = derived_column_names(&shape.columns);
        return Ok(FactorContribution {
            relation: RelationContribution::Derived(DerivedRelationRef {
                key: Some(key),
                shape,
                nullable: false,
                entry_index: 0,
            }),
            names: Some(names),
        });
    }
    let database = deriving.database;
    let Some(table) = resolve_object_name(name.get(), database)? else {
        return collect_view_factor(name, alias, key, schema, deriving, profile);
    };
    let Some(output_columns) = aliased_output_columns(table, alias, database, profile)? else {
        return Ok(opaque_factor(OpaqueIdentity::Known { key, schema }));
    };
    let names = output_columns.iter().map(|column| (column.name.clone(), column.quoted)).collect();
    Ok(FactorContribution {
        relation: RelationContribution::Base(FromTableRef {
            key,
            schema_key: alias.is_none().then(|| stored_schema_key::<DB>(table)),
            table,
            nullable: false,
            entry_index: 0,
            output_columns,
        }),
        names: Some(names),
    })
}

/// Records a single `FROM` table factor into the scope, returning the output
/// column names it contributes and the wildcard plan entry naming what it
/// pushed, or `None` when the factor is opaque.
fn collect_factor<'query, 'db, DB, P>(
    factor: AstRef<'query, 'db, TableFactor>,
    deriving: Deriving<'_, 'db, DB>,
    cte_scope: &[CteShape<'query, 'db, DB, P::Definition>],
    inherited_parent: P::Cursor,
    local_parent: P::Cursor,
    profile: &mut P,
) -> Result<FactorContribution<'query, 'db, DB, P::Definition>, LookupError>
where
    DB: DatabaseLike,
    P: DerivationProfile<'query, 'db, DB>,
{
    match factor.get() {
        TableFactor::Table { args, .. } => {
            collect_table_factor(factor, args.is_some(), deriving, cte_scope, profile)
        }
        TableFactor::Derived { lateral, .. } => {
            let Some(subquery) = derived_factor_query(factor) else {
                return Ok(opaque_factor(OpaqueIdentity::Anonymous));
            };
            let alias = factor_alias(factor);
            let output_names = alias.and_then(OutputNameSource::from_alias);
            let parent = if *lateral { local_parent } else { inherited_parent };
            let body =
                derive_query_shape(subquery, cte_scope, output_names, deriving, parent, profile)?;
            let shape = match alias {
                Some(table_alias) => apply_alias_columns(body, table_alias.get()),
                None => body,
            };
            let Some(shape) = shape else {
                return Ok(opaque_factor(match alias {
                    Some(alias) => OpaqueIdentity::Known { key: alias_key(alias), schema: None },
                    None => OpaqueIdentity::Anonymous,
                }));
            };
            let names = derived_column_names(&shape.columns);
            Ok(FactorContribution {
                relation: RelationContribution::Derived(DerivedRelationRef {
                    key: alias.map(alias_key),
                    shape,
                    nullable: false,
                    entry_index: 0,
                }),
                names: Some(names),
            })
        }
        TableFactor::TableFunction { .. } => {
            let alias = factor_alias(factor);
            let Some(expression) = table_function_expression(factor) else {
                return Ok(opaque_factor(OpaqueIdentity::Anonymous));
            };
            let function_name = match expression {
                AstRef::Query(Expr::Function(function)) => Some(AstRef::Query(&function.name)),
                AstRef::Database(Expr::Function(function)) => {
                    Some(AstRef::Database(&function.name))
                }
                AstRef::Query(_) | AstRef::Database(_) => None,
            };
            let key = alias.map(alias_key).or_else(|| function_name.and_then(object_name_key));
            Ok(opaque_factor(key.map_or(OpaqueIdentity::Anonymous, |key| {
                OpaqueIdentity::Known { key, schema: None }
            })))
        }
        TableFactor::Function { .. } => {
            let Some(name) = factor_name(factor) else {
                return Ok(opaque_factor(OpaqueIdentity::Anonymous));
            };
            let alias = factor_alias(factor);
            let key = alias.map(alias_key).or_else(|| object_name_key(name));
            Ok(opaque_factor(key.map_or(OpaqueIdentity::Anonymous, |key| {
                OpaqueIdentity::Known { key, schema: None }
            })))
        }
        TableFactor::NestedJoin { .. } => {
            let identity = factor_alias(factor).map_or(OpaqueIdentity::AnyQualifier, |alias| {
                OpaqueIdentity::Known { key: alias_key(alias), schema: None }
            });
            Ok(opaque_factor(identity))
        }
        _ => Ok(opaque_factor(OpaqueIdentity::Anonymous)),
    }
}

pub(crate) fn build_definition_graph<'query, 'db, DB: DatabaseLike>(
    query: &'query Query,
    database: &'db DB,
) -> Result<(definition_graph::DefinitionGraph<'query, 'db, DB>, ScopeCursor), LookupError> {
    let deriving = Deriving::of(database);
    let mut profile = DefinitionDerivation::new();
    let parent = profile.no_parent();
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(profile.empty_scope());
    };
    let cte_scope = match &query.with {
        Some(with) => derive_cte_shapes(AstRef::Query(with), &[], deriving, parent, &mut profile)?,
        None => Vec::new(),
    };
    let root =
        collect_select_from(AstRef::Query(select), &cte_scope, deriving, parent, &mut profile)?;
    Ok(profile.finish(root))
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

        let Some(scope) = collect_source_from_clause(self, database)? else {
            return Ok(None);
        };

        let mut source: Option<&'db DB::Table> = None;
        for item in &select.projection {
            let item_source = match item {
                // `*` is a single base-table row only when the FROM is exactly
                // that one base table, or exactly one row-preserving derived
                // relation whose every column passes through the same table.
                // `REPLACE` substitutes a computed value for one of the
                // columns, so the output is no longer a source row, while
                // `EXCLUDE`, `EXCEPT`, `ILIKE` and `RENAME` only drop or
                // relabel columns and leave the rows themselves intact.
                SelectItem::Wildcard(options) => {
                    if wildcard_replaces_values(options) {
                        None
                    } else if scope.from_entry_count == 1 && scope.bases.len() == 1 {
                        Some(scope.bases[0].table)
                    } else if scope.from_entry_count == 1 && scope.derived.len() == 1 {
                        single_source_relation(&scope.derived[0], database)
                    } else {
                        None
                    }
                }
                SelectItem::QualifiedWildcard(_, options) if wildcard_replaces_values(options) => {
                    None
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                            match resolve_wildcard_target(
                                &scope.bases,
                                &scope.derived,
                                object_name,
                                true,
                            ) {
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
                    column_source(expr, &scope, scope.from_entry_count, (), true)?
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

    std::thread_local! {
        static CTE_SHAPE_DERIVATIONS: core::cell::Cell<usize> =
            const { core::cell::Cell::new(0) };
    }

    pub(super) fn record_cte_shape_derivation() {
        CTE_SHAPE_DERIVATIONS.with(|count| count.set(count.get() + 1));
    }

    fn reset_cte_shape_derivations() {
        CTE_SHAPE_DERIVATIONS.with(|count| count.set(0));
    }

    fn cte_shape_derivations() -> usize {
        CTE_SHAPE_DERIVATIONS.with(core::cell::Cell::get)
    }

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

    fn nested_recursive_cte_body(depth: usize) -> String {
        if depth == 0 {
            return "SELECT id FROM users UNION ALL SELECT t0.id FROM t0 WHERE false".to_string();
        }
        let inner = depth - 1;
        format!(
            "WITH RECURSIVE t{inner} AS ({}) SELECT id FROM t{inner} \
             UNION ALL SELECT t{depth}.id FROM t{depth} WHERE false",
            nested_recursive_cte_body(inner)
        )
    }

    #[test]
    fn nested_recursive_ctes_derive_each_with_once() {
        let depth = 6;
        let db = schema_db();
        let sql = format!(
            "WITH RECURSIVE t{depth} AS ({}) SELECT t{depth}.id FROM t{depth}",
            nested_recursive_cte_body(depth)
        );
        reset_cte_shape_derivations();
        assert_eq!(source_name(&sql, &db), Some("users".to_string()));
        assert_eq!(cte_shape_derivations(), depth + 1);
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

    // `REPLACE` substitutes a computed value for one of the expanded columns,
    // so the output row is no longer a row of the source table and carries no
    // row identity. Dropping columns (`EXCLUDE`, `EXCEPT`, `ILIKE`) or
    // relabelling them (`RENAME`) leaves the rows intact, so those still
    // answer.
    #[test]
    fn replacing_wildcard_carries_no_row_identity() {
        let db = schema_db();
        assert_eq!(source_name("SELECT * REPLACE ('x' AS name) FROM users", &db), None);
        assert_eq!(source_name("SELECT users.* REPLACE ('x' AS name) FROM users", &db), None);
        for sql in [
            "SELECT * EXCLUDE (name) FROM users",
            "SELECT * EXCEPT (name) FROM users",
            "SELECT * ILIKE 'id%' FROM users",
            "SELECT * RENAME (name AS handle) FROM users",
            "SELECT users.* EXCLUDE (name) FROM users",
        ] {
            assert_eq!(source_name(sql, &db), Some("users".to_string()), "{sql}");
        }
    }
}
