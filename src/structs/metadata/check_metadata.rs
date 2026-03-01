//! Submodule defining a generic `IndexMetadata` struct.

use std::sync::Arc;

use sqlparser::ast::Expr;

use crate::traits::{CheckConstraintLike, DatabaseLike};

#[derive(Debug, Clone)]
/// Struct collecting metadata about a check constraint.
pub struct CheckMetadata<U: CheckConstraintLike> {
    /// The expression defining the constraint.
    expression: Expr,
    /// The table on which the constraint is defined.
    table: Arc<<U::DB as DatabaseLike>::Table>,
    /// The columns involved in the constraint.
    columns: Vec<Arc<<U::DB as DatabaseLike>::Column>>,
    /// The functions involved in the constraint.
    functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
}

impl<U: CheckConstraintLike> CheckMetadata<U> {
    /// Creates a new `CheckMetadata` instance.
    #[inline]
    pub fn new(
        expression: Expr,
        table: Arc<<U::DB as DatabaseLike>::Table>,
        columns: Vec<Arc<<U::DB as DatabaseLike>::Column>>,
        functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
    ) -> Self {
        Self { expression, table, columns, functions }
    }

    /// Returns a reference to the expression defining the constraint.
    #[must_use]
    #[inline]
    pub fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns a reference to the table on which the constraint is defined.
    #[must_use]
    #[inline]
    pub fn table(&self) -> &<U::DB as DatabaseLike>::Table {
        &self.table
    }

    /// Returns an iterator over the columns involved in the constraint.
    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Column> {
        self.columns.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the functions involved in the constraint.
    #[inline]
    pub fn functions(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Function> {
        self.functions.iter().map(std::convert::AsRef::as_ref)
    }
}
