//! Per-query resolution of column references to the tables that expose them.
//!
//! [`ColumnScope`] answers which table a column reference in an expression
//! belongs to, the question a type-dependent rewrite must ask before it can
//! read a declared type. It is built once from a query plus a database (or
//! from the single table a definition belongs to) and then queried per
//! reference.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use sqlparser::ast::{Expr, Query};

use crate::{
    errors::LookupError,
    impls::dql::{
        DerivedRelationRef, FromScope, FromTableRef, MergedName, collect_from_clause, column_source,
    },
    traits::{ColumnLike, DatabaseLike, TableLike},
};

/// The relations a query exposes for resolving column references.
///
/// A scope is built once from a [`Query`] plus the [`DatabaseLike`] it runs
/// against, resolving each `FROM` relation through the same search-path
/// lookup a direct table lookup uses, and then answers any number of column
/// references:
///
/// - a bare reference (`col`) resolves to the single exposed table that
///   declares it,
/// - a qualified reference (`alias.col`, `table.col`, quoted spellings
///   included) resolves through the `FROM` alias when present, else the table
///   name, with PostgreSQL identifier folding,
/// - a reference qualified by a CTE or derived-subquery alias (`cg.id` where
///   `cg` aliases `WITH child_groups AS (SELECT id FROM groups ...)`) resolves
///   to the base table the relation's projection passes that column through
///   from. A computed output column (`count(*) AS n`), and a set-operation
///   column whose arms name different tables, answer nothing, as does a base
///   table's own computed projection.
/// - a `FROM` alias column list (`users u(n)`) renames the base relation's
///   exposed columns positionally and hides the originals, as PostgreSQL does:
///   `u.n` resolves and `u.id` answers nothing. A partial list keeps the tail's
///   own names. More aliases than columns is a mismatch PostgreSQL rejects, so
///   the relation becomes opaque.
/// - the same reference rules govern a qualified wildcard's prefix (`alias.*`,
///   `schema.table.*`): a one-part prefix matches a relation alias or key, a
///   two-part prefix must name a base relation's own schema (a CTE or derived
///   relation has none), and a prefix of three or more parts matches nothing,
///   since the database part is not modeled.
/// - `Ok(None)` means no answer: the name is exposed by no relation in scope,
///   the qualifier matches nothing, or an opaque relation (a table function, an
///   unresolvable name, a nested join, or a CTE or subquery whose columns
///   cannot be enumerated, such as one computing an unnamed item) makes a bare
///   reference unknowable. PostgreSQL resolves some of those and errors on
///   others (a qualifier matching nothing). This scope never guesses, so a
///   consumer treats `Ok(None)` as "refuse or fall back knowingly".
/// - an error is returned when a bare reference is exposed by more than one
///   relation (or by a derived relation twice), the PostgreSQL `column
///   reference is ambiguous` case. A `schema.table.column` reference answers
///   only when the leading part names the exact schema of a base table, as
///   PostgreSQL requires. CTE and derived references have no schema and the
///   database part is not modeled. A column merged by a `USING` or `NATURAL`
///   join answers nothing when spelled bare: the coalesced value belongs to no
///   single table, and naming a third relation makes the bare reference
///   ambiguous, as PostgreSQL reports. A qualified reference to one side's own
///   column still resolves to that side, also as PostgreSQL does.
///
/// [`ColumnScope::for_table`] builds the definition-context scope: the
/// columns in scope inside a constraint check, computed column, index
/// expression, policy condition, or trigger body are exactly the defined
/// table's own.
///
/// ```
/// use sql_traits::prelude::*;
/// use sqlparser::{
///     ast::{SelectItem, SetExpr, Statement},
///     dialect::GenericDialect,
///     parser::Parser,
/// };
///
/// let db = ParserDB::parse::<GenericDialect>(
///     "CREATE TABLE a(payload TEXT); CREATE TABLE b(payload JSON);",
/// )
/// .unwrap();
/// let mut statements =
///     Parser::parse_sql(&GenericDialect {}, "SELECT a.payload FROM a JOIN b ON a.id = b.id")
///         .unwrap();
/// let Statement::Query(query) = statements.pop().unwrap() else {
///     panic!("expected a query");
/// };
/// let scope = ColumnScope::from_query(&query, &db).unwrap();
/// let SetExpr::Select(select) = query.body.as_ref() else {
///     panic!("expected a SELECT");
/// };
/// let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
///     panic!("expected an expression projection");
/// };
/// let table = scope.resolve_column(reference).unwrap().expect("resolves");
/// assert_eq!(table.table_name(), "a");
/// ```
pub struct ColumnScope<'a, 'db, DB: DatabaseLike> {
    bases: Vec<FromTableRef<'a, 'db, DB>>,
    derived: Vec<DerivedRelationRef<'a, 'db, DB>>,
    merged: Vec<MergedName>,
    has_opaque: bool,
}

impl<'a, 'db, DB: DatabaseLike> ColumnScope<'a, 'db, DB> {
    /// Builds the column scope of a query from its outer `SELECT` body.
    ///
    /// Each `FROM` relation is resolved against `database` now, so relation
    /// name errors surface at construction, and each CTE introduced by the
    /// query's `WITH` clause has its output columns derived from its own body
    /// (a recursive CTE's self-reference stops at the in-progress marker
    /// rather than recursing). A body that is not a plain `SELECT` (a set
    /// operation, `VALUES`, `TABLE`) has no single outer `FROM`, and its
    /// scope answers nothing. `DISTINCT` and `GROUP BY` do not change which
    /// relations are in scope.
    ///
    /// Subqueries in `WHERE`, `ON`, or elsewhere introduce their own scopes
    /// inside their own queries; this scope covers only the relations the
    /// outer projection sees.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::InvalidObjectName`] when a `FROM` relation
    /// name is malformed for table lookup and
    /// [`LookupError::AmbiguousTableLookup`] when a `FROM` relation name
    /// resolves ambiguously.
    pub fn from_query(query: &'a Query, database: &'db DB) -> Result<Self, LookupError> {
        let (bases, derived, merged, has_opaque) = match collect_from_clause(query, database)? {
            Some(FromScope { bases, derived, merged, has_opaque, .. }) => {
                (bases, derived, merged, has_opaque)
            }
            None => (Vec::new(), Vec::new(), Vec::new(), true),
        };
        Ok(Self { bases, derived, merged, has_opaque })
    }

    /// Resolves one column reference to the table in this scope that exposes
    /// it.
    ///
    /// The bare identifier shape (`col`) and the compound shapes of a
    /// qualifier plus column (`alias.col`) and a schema, table, and column
    /// (`schema.table.col`) carry a reference. Four or more parts answer
    /// `Ok(None)` (see the type docs above), and any other expression is not a
    /// column reference and answers `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the reference is
    /// unqualified and more than one relation in this scope exposes the
    /// name, or when it is qualified and the matching derived relation
    /// exposes that name in two of its output columns.
    pub fn resolve_column(&self, reference: &Expr) -> Result<Option<&'db DB::Table>, LookupError> {
        column_source(reference, &self.bases, &self.derived, &self.merged, self.has_opaque)
    }
}

impl<'db, DB: DatabaseLike> ColumnScope<'db, 'db, DB> {
    /// Builds the scope of a definition: the expression belongs to `table`
    /// and sees exactly that table's columns.
    ///
    /// A bare reference resolves to `table` when it exposes the column, a
    /// reference qualified by the table's own stored name resolves (quoted
    /// spellings must match exactly, as PostgreSQL requires), and anything
    /// else resolves to nothing.
    ///
    /// ```
    /// use sql_traits::prelude::*;
    /// use sqlparser::{
    ///     ast::{SelectItem, SetExpr, Statement},
    ///     dialect::GenericDialect,
    ///     parser::Parser,
    /// };
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE docs(body TEXT); CREATE TABLE other(body INT);",
    /// )
    /// .unwrap();
    /// let table = db.table(None, "docs").expect("docs exists");
    /// let scope = ColumnScope::for_table(table, &db);
    /// let mut statements =
    ///     Parser::parse_sql(&GenericDialect {}, "SELECT docs.body FROM placeholder").unwrap();
    /// let Statement::Query(query) = statements.pop().unwrap() else {
    ///     panic!("expected a query");
    /// };
    /// let SetExpr::Select(select) = query.body.as_ref() else {
    ///     panic!("expected a SELECT");
    /// };
    /// let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
    ///     panic!("expected an expression projection");
    /// };
    /// let resolved = scope.resolve_column(reference).unwrap().expect("resolves");
    /// assert_eq!(resolved.table_name(), "docs");
    /// ```
    #[must_use]
    pub fn for_table(table: &'db DB::Table, database: &'db DB) -> Self {
        // The definition context never aliases, so the output names are the
        // table's own. Column enumeration cannot fail for a table borrowed
        // from `database`. An implementation that reports an error leaves
        // the scope empty rather than answering from unknowns.
        let output_names: Vec<(String, bool)> = table
            .columns(database)
            .map(|columns| {
                columns
                    .map(|column| {
                        (column.column_name().to_string(), column.column_name_is_quoted())
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            bases: vec![FromTableRef {
                key_value: table.table_name(),
                key_quoted: table.table_name_is_quoted(),
                table,
                nullable: false,
                entry_index: 0,
                output_names,
            }],
            merged: Vec::new(),
            derived: Vec::new(),
            has_opaque: false,
        }
    }
}
