//! Submodule providing the [`DQLLike`] trait for SQL data-query statements
//! (`SELECT`), analyzed against a [`DatabaseLike`].

use crate::{
    errors::LookupError,
    traits::{DataStatementLike, DatabaseLike},
};

/// A parsed SQL query (`SELECT`) analyzed against a [`DatabaseLike`].
///
/// Extends [`DataStatementLike`] with projection analysis. It is implemented on
/// sqlparser's `Query` node.
pub trait DQLLike<DB: DatabaseLike>: DataStatementLike<DB> {
    /// Returns the single base table that every projected column comes from,
    /// when exactly one such table exists.
    ///
    /// This is the eligibility rule for "single-table row re-execution": the
    /// query's output rows are rows of one base table, so they can be delivered
    /// as a primary-key-keyed patchset. The cases that qualify are:
    ///
    /// - qualified columns (`t.c`) or a qualified wildcard (`t.*`) all bound to
    ///   the same base table, or
    /// - unqualified columns over a single-table `FROM` (no joins).
    ///
    /// Returns `Ok(None)` (not eligible) when the output rows are not rows of a
    /// single base table, specifically when:
    ///
    /// - the projection draws from more than one base table,
    /// - any projected item is a computed expression (arithmetic, a function
    ///   call, an aggregate, a scalar subquery, `CASE`, ...) rather than a
    ///   pass-through column or wildcard,
    /// - the statement uses `GROUP BY` or `DISTINCT`,
    /// - the `FROM` includes a relation that is not a base table (a derived
    ///   subquery, a table function, or a CTE reference), and the projection
    ///   could draw from it,
    /// - `*` is projected over anything other than exactly one base table, or
    /// - the query body is a set operation (`UNION`/`EXCEPT`/`INTERSECT`),
    ///   `VALUES`, or `TABLE`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when an unqualified column
    /// is exposed by more than one table in the `FROM` clause, and
    /// [`LookupError::InvalidObjectName`] /
    /// [`LookupError::AmbiguousTableLookup`] when a `FROM` relation name is
    /// malformed or resolves ambiguously.
    fn projection_source_table<'db>(
        &self,
        database: &'db DB,
    ) -> Result<Option<&'db DB::Table>, LookupError>;
}
