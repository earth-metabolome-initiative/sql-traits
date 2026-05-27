//! Submodule providing the [`DMLLike`] trait for SQL data-manipulation
//! statements (`INSERT`, `UPDATE`, `DELETE`), analyzed against a
//! [`DatabaseLike`].

use crate::{
    errors::LookupError,
    traits::{DataStatementLike, DatabaseLike},
};

/// Which data-manipulation statement a [`DMLLike`] value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DmlKind {
    /// An `INSERT` statement.
    Insert,
    /// An `UPDATE` statement.
    Update,
    /// A `DELETE` statement.
    Delete,
}

/// A data-manipulation statement whose [`DmlKind`] is known independently of
/// any database.
///
/// This is a non-generic supertrait of [`DMLLike`] so that `kind()` can be
/// called on a concrete `Insert`/`Update`/`Delete` without naming a database
/// type. It is implemented on sqlparser's `Insert`, `Update`, and `Delete`
/// nodes.
pub trait DmlStatement {
    /// Returns which mutation this statement is.
    fn kind(&self) -> DmlKind;
}

/// A parsed SQL data-manipulation statement (`INSERT`, `UPDATE`, `DELETE`)
/// analyzed against a [`DatabaseLike`].
///
/// Extends [`DataStatementLike`] with the mutation target (and, via
/// [`DmlStatement`], the kind of mutation). It is implemented on sqlparser's
/// `Insert`, `Update`, and `Delete` nodes.
///
/// Per-row interest (which rows a write actually touches) is intentionally not
/// modeled here: those identities come from execution results (RETURNING,
/// affected rows, generated keys), not from parsing.
pub trait DMLLike<DB: DatabaseLike>: DataStatementLike<DB> + DmlStatement {
    /// Returns the base table this statement mutates: the target of
    /// `INSERT INTO t`, `UPDATE t`, or `DELETE FROM t`, resolved against
    /// `database`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::TableNotFound`] when the target name does not
    /// resolve to a table in `database`, [`LookupError::InvalidObjectName`]
    /// when the statement has no single base-table target (an `INSERT` into
    /// a subquery or table function, an `UPDATE` of a non-table relation,
    /// or a multi-table `DELETE`), and
    /// [`LookupError::AmbiguousTableLookup`] when the target name resolves
    /// ambiguously.
    fn target_table<'db>(&self, database: &'db DB) -> Result<&'db DB::Table, LookupError>;
}
