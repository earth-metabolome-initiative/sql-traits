//! Submodule defining a `FunctionMetadata` struct.

use alloc::string::String;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Metadata about a database function.
///
/// Holds what the input says about a function that the function's own
/// declaration cannot carry, which today is the role a later statement hands it
/// to.
pub struct FunctionMetadata {
    /// The role the input names as the function's owner, if it names one.
    owner: Option<String>,
}

impl FunctionMetadata {
    /// Returns the role the input names as the function's owner.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), sql_traits::errors::Error> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "CREATE ROLE app_reader;
    ///      CREATE FUNCTION f() RETURNS INT AS 'SELECT 1';
    ///      ALTER FUNCTION f() OWNER TO app_reader;",
    /// )?;
    /// let function = db.function("f").expect("Function should exist");
    /// let metadata = db.function_metadata(function).expect("Metadata should exist");
    /// assert_eq!(metadata.owner(), Some("app_reader"));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    #[inline]
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    /// Sets the role the input names as the function's owner.
    ///
    /// # Arguments
    ///
    /// * `owner` - The owning role, or [`None`] when the input names no role.
    #[inline]
    pub fn set_owner(&mut self, owner: Option<String>) {
        self.owner = owner;
    }
}
