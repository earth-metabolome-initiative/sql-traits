//! Submodule defining the `IndexLike` trait for SQL indices.

use std::fmt::Debug;

use sqlparser::ast::Expr;

use crate::{
    traits::{DatabaseLike, Metadata, TableLike},
    utils::columns_in_expression::columns_in_expression,
};

/// An index is a rule that specifies that the values in a column
/// (or a group of columns) must used to speed up queries on a table.
/// This trait represents such an index in a database-agnostic way.
pub trait IndexLike: Metadata + Ord + Eq + Debug + Clone {
    /// The database type the index belongs to.
    type DB: DatabaseLike;

    /// Returns the expression of the index as an SQL AST node.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::ast::Expr;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE users (id int, name text); CREATE INDEX idx_name ON users (name);")?;
    /// let table = db.table(None, "users").unwrap();
    /// let index = table.indices(&db).next().unwrap();
    /// let expr = index.expression(&db);
    /// let inner = match expr {
    ///     Expr::Nested(inner) => inner,
    ///     _ => expr,
    /// };
    /// assert!(matches!(inner, Expr::Identifier(ident) if ident.value == "name"));
    /// # Ok(())
    /// # }
    /// ```
    fn expression<'db>(&'db self, database: &'db Self::DB) -> &'db Expr
    where
        Self: 'db;

    /// Returns a reference to the table this index belongs to.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE users (id int, name text); CREATE INDEX idx_name ON users (name);",
    /// )?;
    /// let table = db.table(None, "users").unwrap();
    /// let index = table.indices(&db).next().unwrap();
    /// assert_eq!(IndexLike::table(index, &db).table_name(), "users");
    /// # Ok(())
    /// # }
    /// ```
    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db;

    /// Returns whether the index is defined using simply columns
    /// and no other expressions.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE users (id int, name text); CREATE INDEX idx_name ON users (name);",
    /// )?;
    /// let table = db.table(None, "users").unwrap();
    /// let index = table.indices(&db).next().unwrap();
    /// assert!(index.is_simple(&db));
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn is_simple(&self, database: &Self::DB) -> bool {
        let expr = self.expression(database);
        let inner_expr = match expr {
            Expr::Nested(inner) => inner,
            _ => expr,
        };
        matches!(inner_expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Tuple(_))
    }

    /// Returns the columns which appear in the index.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE users (id int, name text); CREATE INDEX idx_name ON users (name);",
    /// )?;
    /// let table = db.table(None, "users").unwrap();
    /// let index = table.indices(&db).next().unwrap();
    /// let columns: Vec<_> = index.columns(&db).collect();
    /// assert_eq!(columns.len(), 1);
    /// assert_eq!(columns[0].column_name(), "name");
    /// # Ok(())
    /// # }
    /// ```
    fn columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>
    where
        Self: 'db,
    {
        let table = <Self as IndexLike>::table(self, database);
        let expr = self.expression(database);

        let all_columns: Vec<&<Self::DB as DatabaseLike>::Column> =
            table.columns(database).collect();

        let table_name = table.table_name();

        let found_cols: Vec<&<Self::DB as DatabaseLike>::Column> =
            columns_in_expression(expr, table_name, &all_columns).unwrap_or_default();

        found_cols.into_iter()
    }
}
