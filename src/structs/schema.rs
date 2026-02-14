//! Schema struct for storing parsed schema information.

/// A database schema parsed from a CREATE SCHEMA statement.
///
/// This struct stores the schema name and optional authorization owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Schema {
    /// The name of the schema.
    name: String,
    /// The authorization owner of the schema, if specified.
    authorization: Option<String>,
}

impl Schema {
    /// Creates a new `Schema` with the given name.
    #[must_use]
    pub fn new(name: String) -> Self {
        Self { name, authorization: None }
    }

    /// Creates a new `Schema` with the given name and authorization owner.
    #[must_use]
    pub fn with_authorization(name: String, authorization: String) -> Self {
        Self { name, authorization: Some(authorization) }
    }

    /// Returns the name of the schema.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the authorization owner of the schema, if specified.
    #[must_use]
    pub fn authorization(&self) -> Option<&str> {
        self.authorization.as_deref()
    }
}
