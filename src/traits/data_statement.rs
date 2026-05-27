//! Submodule providing the [`DataStatementLike`] trait, the umbrella for SQL
//! data statements (ISO "SQL-data statements": `SELECT`, `INSERT`, `UPDATE`,
//! `DELETE`) analyzed against a [`DatabaseLike`].
//!
//! Unlike the DDL object traits, which carry an associated `DB` because each
//! object belongs to one schema, a data statement is analyzed *against* a
//! database passed in, and the same statement may be analyzed against different
//! databases. The database is therefore a trait type parameter.

use alloc::vec::Vec;

use crate::{errors::LookupError, traits::DatabaseLike};

/// A parsed SQL statement that references table data, analyzed against a
/// [`DatabaseLike`].
///
/// This is the umbrella trait for the data-statement hierarchy. It is
/// implemented on the sqlparser AST nodes that model data statements
/// (`Query` for `SELECT`, and `Insert` / `Update` / `Delete` for DML),
/// mirroring how the DDL traits are implemented on `CreateTable`, `ColumnDef`,
/// and so on.
///
/// The public surface speaks only resolved schema types
/// ([`DatabaseLike::Table`]), never sqlparser's `ObjectName` or `TableFactor`.
pub trait DataStatementLike<DB: DatabaseLike> {
    /// Returns every base table referenced anywhere in the statement, resolved
    /// against `database` and deduplicated.
    ///
    /// This includes `FROM` and `JOIN` relations, relations inside subqueries
    /// (for example in `WHERE` or `HAVING`), CTE bodies, set operations, and,
    /// for DML, the mutation target together with any subqueries it contains.
    /// Relations that do not resolve to a base table (CTE names, table
    /// functions, otherwise unknown names) are skipped rather than reported as
    /// errors.
    ///
    /// Tables are returned in first-seen order and deduplicated by their
    /// position in [`DatabaseLike`] (so the same base table reached through
    /// several aliases or self-joins appears once).
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when a referenced name
    /// matches more than one base table (for example an unqualified name
    /// present in several schemas), and [`LookupError::InvalidObjectName`]
    /// when a referenced name is malformed for table lookup.
    fn referenced_tables<'db>(&self, database: &'db DB)
    -> Result<Vec<&'db DB::Table>, LookupError>;
}
