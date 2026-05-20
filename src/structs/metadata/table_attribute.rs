//! Submodule for objects whose main metadata is just that they are part of a
//! table.

use alloc::sync::Arc;
use core::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// A struct associating a table with one of its attributes, such as a column or
/// constraint.
pub struct TableAttribute<T, A> {
    /// The attribute associated with the table.
    attribute: A,
    /// The table the attribute belongs to.
    table: Arc<T>,
}

impl<T, A> Display for TableAttribute<T, A>
where
    A: Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.attribute)
    }
}

impl<T, A> TableAttribute<T, A> {
    /// Creates a new `TableAttribute` associating the given table with the
    /// given attribute.
    ///
    /// # Example
    ///
    /// ```rust
    /// use alloc::sync::Arc;
    /// extern crate alloc;
    /// use sql_traits::structs::TableAttribute;
    ///
    /// let table = Arc::new("users");
    /// let attr = TableAttribute::new(table, "id");
    /// assert_eq!(*attr.table(), "users");
    /// assert_eq!(*attr.attribute(), "id");
    /// ```
    #[inline]
    pub fn new(table: Arc<T>, attribute: A) -> Self {
        Self { attribute, table }
    }

    /// Returns a reference to the table.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// let table = db.table(None, "t").unwrap();
    /// let column = table.column("id", &db).unwrap();
    /// // `column` is a `TableAttribute<CreateTable, ColumnDef>` — its
    /// // `.table()` accessor returns the host table.
    /// assert_eq!(column.table().table_name(), "t");
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn table(&self) -> &T {
        &self.table
    }

    /// Returns a reference to the attribute.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// let table = db.table(None, "t").unwrap();
    /// let column = table.column("id", &db).unwrap();
    /// // `column.attribute()` is the underlying `sqlparser::ast::ColumnDef`.
    /// assert_eq!(column.attribute().name.value, "id");
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    pub fn attribute(&self) -> &A {
        &self.attribute
    }
}
