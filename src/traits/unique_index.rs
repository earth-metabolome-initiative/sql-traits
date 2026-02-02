//! Submodule definining the `UniqueIndexLike` trait for SQL unique
//! indexes.

use crate::traits::{IndexLike, TableLike};

/// A unique index is a rule that specifies that the values in a column
/// (or a group of columns) must be unique across all rows in a table.
/// This trait represents such a unique index in a database-agnostic way.
pub trait UniqueIndexLike: IndexLike {
    /// Returns whether this unique index is also the primary key of the table.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse(
    ///     r#"CREATE TABLE my_table (id INT PRIMARY KEY, name TEXT, UNIQUE (name));"#,
    ///     &GenericDialect,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let unique_indices: Vec<_> = table.unique_indices(&db).collect();
    /// let primary_key_flags: Vec<bool> =
    ///     unique_indices.iter().map(|ui| ui.is_primary_key(&db)).collect();
    /// assert_eq!(primary_key_flags, vec![true, false]);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn is_primary_key(&self, database: &<Self as IndexLike>::DB) -> bool {
        self.table(database).primary_key_columns(database).eq(self.columns(database))
    }
}

impl<T: IndexLike> UniqueIndexLike for T {}
