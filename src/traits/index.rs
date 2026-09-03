//! Submodule defining the `IndexLike` trait for SQL indices.

use alloc::vec::Vec;
use core::fmt::Debug;

use sqlparser::ast::Expr;

use crate::{
    errors::LookupError,
    structs::TargetName,
    traits::{DatabaseLike, Metadata, TableLike},
    utils::columns_in_expression::columns_in_expression,
};

/// An index is a rule that specifies that the values in a column
/// (or a group of columns) must used to speed up queries on a table.
/// This trait represents such an index in a database-agnostic way.
pub trait IndexLike: Metadata + Ord + Eq + Debug + Clone + Send + Sync {
    /// The database type the index belongs to.
    type DB: DatabaseLike;

    /// Returns the declared index name, or `None` for an anonymous index.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), sql_traits::errors::Error> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE users (id int, name text); CREATE INDEX idx_name ON users (name);",
    /// )?;
    /// let index = db.indexes().next().unwrap();
    /// assert_eq!(index.name(), Some("idx_name"));
    /// assert!(!index.name_is_quoted());
    /// # Ok(())
    /// # }
    /// ```
    fn name(&self) -> Option<&str>;

    /// Returns whether the index identifier was quoted in SQL.
    ///
    /// This only matters when [`Self::name`] returns `Some`, and answers
    /// `false` for an anonymous index, which has no identifier to quote.
    ///
    /// The default `false` folds every identifier to lowercase, so an
    /// implementation over a source that preserves quoting must override it.
    #[inline]
    fn name_is_quoted(&self) -> bool {
        false
    }

    /// Returns the schema qualifier written on the index name, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), sql_traits::errors::Error> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE SCHEMA app;
    ///      CREATE TABLE app.users (id int, name text);
    ///      CREATE INDEX app.\"IdxName\" ON app.users (name);",
    /// )?;
    /// let index = db.indexes().next().unwrap();
    /// assert_eq!(index.name(), Some("IdxName"));
    /// assert!(index.name_is_quoted());
    /// assert_eq!(index.schema(), Some("app"));
    /// assert!(!index.schema_is_quoted());
    /// # Ok(())
    /// # }
    /// ```
    fn schema(&self) -> Option<&str>;

    /// Returns whether that schema qualifier was quoted in SQL.
    ///
    /// This only matters when [`Self::schema`] returns `Some`.
    ///
    /// The default `false` folds every identifier to lowercase, so an
    /// implementation over a source that preserves quoting must override it.
    #[inline]
    fn schema_is_quoted(&self) -> bool {
        false
    }

    /// Returns the expression of the index as an SQL AST node.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::ObjectNotInDatabase`] when `database` does not
    /// hold this index.
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
    /// let index = table.indices(&db)?.next().unwrap();
    /// let expr = index.expression(&db)?;
    /// let inner = match expr {
    ///     Expr::Nested(inner) => inner,
    ///     _ => expr,
    /// };
    /// assert!(matches!(inner, Expr::Identifier(ident) if ident.value == "name"));
    /// # Ok(())
    /// # }
    /// ```
    fn expression<'db>(&'db self, database: &'db Self::DB) -> Result<&'db Expr, LookupError>
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
    /// let index = table.indices(&db)?.next().unwrap();
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
    /// # Errors
    ///
    /// Returns [`LookupError::ObjectNotInDatabase`] when `database` does not
    /// hold this index.
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
    /// let index = table.indices(&db)?.next().unwrap();
    /// assert!(index.is_simple(&db)?);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn is_simple(&self, database: &Self::DB) -> Result<bool, LookupError> {
        let expr = self.expression(database)?;
        let inner_expr = match expr {
            Expr::Nested(inner) => inner,
            _ => expr,
        };
        Ok(matches!(inner_expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Tuple(_)))
    }

    /// Returns the columns which appear in the index.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::ObjectNotInDatabase`] when `database` does not
    /// hold this index.
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
    /// let index = table.indices(&db)?.next().unwrap();
    /// let columns: Vec<_> = index.columns(&db)?.collect();
    /// assert_eq!(columns.len(), 1);
    /// assert_eq!(columns[0].column_name(), "name");
    /// # Ok(())
    /// # }
    /// ```
    fn columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>, LookupError>
    where
        Self: 'db,
    {
        let table = <Self as IndexLike>::table(self, database);
        let expr = self.expression(database)?;

        let all_columns: Vec<&<Self::DB as DatabaseLike>::Column> =
            table.columns(database)?.collect();

        let mut target = TargetName::new(table.table_name(), table.table_name_is_quoted());
        if let Some(schema) = table.table_schema() {
            target = target.with_schema(schema, table.table_schema_is_quoted());
        }

        // A reference this table does not own, or a column it does not
        // declare, leaves the index reporting no columns rather than a wrong
        // one. A creation refuses both before they can be recorded.
        let found_cols: Vec<&<Self::DB as DatabaseLike>::Column> =
            columns_in_expression(expr, database.catalog_name(), &target, &all_columns)
                .unwrap_or_default();

        Ok(found_cols.into_iter())
    }
}
