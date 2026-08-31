//! Per-query resolution of column references to the tables that expose them.
//!
//! [`ColumnScope`] answers which table a column reference in an expression
//! belongs to, the question a type-dependent rewrite must ask before it can
//! read a declared type. It is built once from a query plus a database (or
//! from the single table a definition belongs to) and then queried per
//! reference.

use alloc::{vec, vec::Vec};

use sqlparser::ast::{Expr, Query};

use crate::{
    errors::LookupError,
    impls::dql::{FromScope, FromTableRef, collect_from_clause, column_source},
    traits::{DatabaseLike, TableLike},
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
/// - `Ok(None)` means no answer: the name is exposed by no relation in scope,
///   the qualifier matches nothing, or an opaque relation (a derived subquery,
///   a table function, a CTE reference, an unresolvable name) makes a bare
///   reference unknowable. PostgreSQL resolves some of those (it introspects
///   derived output columns) and errors on others (a qualifier matching
///   nothing); this scope never guesses, so a consumer treats `Ok(None)` as
///   "refuse or fall back knowingly".
/// - an error is returned when a bare reference is exposed by more than one
///   relation, the PostgreSQL `column reference is ambiguous` case.
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
    has_opaque: bool,
    database: &'db DB,
}

impl<'a, 'db, DB: DatabaseLike> ColumnScope<'a, 'db, DB> {
    /// Builds the column scope of a query from its outer `SELECT` body.
    ///
    /// Each `FROM` relation is resolved against `database` now, so relation
    /// name errors surface at construction. A body that is not a plain
    /// `SELECT` (a set operation, `VALUES`, `TABLE`) has no single outer
    /// `FROM`, and its scope answers nothing. `DISTINCT` and `GROUP BY` do
    /// not change which relations are in scope.
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
        let (bases, has_opaque) = match collect_from_clause(query, database)? {
            Some(FromScope { bases, has_opaque, .. }) => (bases, has_opaque),
            None => (Vec::new(), true),
        };
        Ok(Self { bases, has_opaque, database })
    }

    /// Resolves one column reference to the table in this scope that exposes
    /// it.
    ///
    /// Only the two identifier shapes carry a reference: a bare identifier
    /// (`col`) and a compound identifier whose last part is the column and
    /// whose second-to-last part is the qualifier (`alias.col`). Any other
    /// expression is not a column reference and answers `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the reference is
    /// unqualified and more than one relation in this scope exposes the
    /// name.
    pub fn resolve_column(&self, reference: &Expr) -> Result<Option<&'db DB::Table>, LookupError> {
        column_source(reference, &self.bases, self.has_opaque, self.database)
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
        Self {
            bases: vec![FromTableRef {
                key_value: table.table_name(),
                key_quoted: table.table_name_is_quoted(),
                table,
            }],
            has_opaque: false,
            database,
        }
    }
}
