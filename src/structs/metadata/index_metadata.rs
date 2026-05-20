//! Submodule defining a generic `IndexMetadata` struct.

use alloc::sync::Arc;

use sqlparser::ast::Expr;

use crate::traits::{DatabaseLike, IndexLike};

#[derive(Debug, Clone)]
/// Struct collecting metadata about an index.
pub struct IndexMetadata<I: IndexLike> {
    /// The expression defining the index.
    expression: Expr,
    /// The table on which the index is defined.
    table: Arc<<I::DB as DatabaseLike>::Table>,
}

impl<I: IndexLike> IndexMetadata<I> {
    /// Creates a new `IndexMetadata` instance.
    #[inline]
    pub fn new(expression: Expr, table: Arc<<I::DB as DatabaseLike>::Table>) -> Self {
        Self { expression, table }
    }

    /// Returns a reference to the expression defining the index.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db =
    ///     ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT); CREATE INDEX idx ON t(id);")?;
    /// let table = db.table(None, "t").unwrap();
    /// let index = table.indices(&db).next().unwrap();
    /// let meta = db.index_metadata(index).unwrap();
    /// // The expression is the parenthesized column list as parsed.
    /// assert_eq!(meta.expression().to_string(), "(id)");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    #[inline]
    pub fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Returns a reference to the table on which the index is defined.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE my_t (id INT); CREATE INDEX idx ON my_t(id);",
    /// )?;
    /// let index = db.table(None, "my_t").unwrap().indices(&db).next().unwrap();
    /// let meta = db.index_metadata(index).unwrap();
    /// assert_eq!(meta.table().table_name(), "my_t");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    #[inline]
    pub fn table(&self) -> &<I::DB as DatabaseLike>::Table {
        &self.table
    }
}

/// Type alias for `IndexMetadata` to be used with unique indices.
pub type UniqueIndexMetadata<U> = IndexMetadata<U>;
