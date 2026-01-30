//! Submodule defining a generic `IndexMetadata` struct.

use std::rc::Rc;

use sqlparser::ast::Expr;

use crate::traits::{DatabaseLike, IndexLike};

#[derive(Debug, Clone)]
/// Struct collecting metadata about an index.
pub struct IndexMetadata<I: IndexLike> {
    /// The expression defining the index.
    expression: Expr,
    /// The table on which the index is defined.
    table: Rc<<I::DB as DatabaseLike>::Table>,
}

impl<I: IndexLike> IndexMetadata<I> {
    /// Creates a new `IndexMetadata` instance.
    #[inline]
    pub fn new(expression: Expr, table: Rc<<I::DB as DatabaseLike>::Table>) -> Self {
        Self { expression, table }
    }

    /// Returns a reference to the expression defining the index.
    #[must_use]
    #[inline]
    pub fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns a reference to the table on which the index is defined.
    #[must_use]
    #[inline]
    pub fn table(&self) -> &<I::DB as DatabaseLike>::Table {
        &self.table
    }
}

/// Type alias for `IndexMetadata` to be used with unique indices.
pub type UniqueIndexMetadata<U> = IndexMetadata<U>;
