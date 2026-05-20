//! Schema struct for storing parsed schema information.

use alloc::string::String;

/// A database schema parsed from a CREATE SCHEMA statement.
///
/// This struct stores the schema name and optional authorization owner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Schema {
    /// The name of the schema.
    name: String,
    /// Whether the schema identifier was quoted in SQL.
    quoted: bool,
    /// The authorization owner of the schema, if specified.
    authorization: Option<String>,
}

impl Schema {
    /// Creates a new `Schema` with the given name.
    #[must_use]
    pub fn new(name: String) -> Self {
        Self { name, quoted: false, authorization: None }
    }

    /// Creates a new `Schema` with the given name and authorization owner.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::structs::Schema;
    ///
    /// let s = Schema::with_authorization("public".to_string(), "admin".to_string());
    /// assert_eq!(s.name(), "public");
    /// assert_eq!(s.authorization(), Some("admin"));
    /// assert!(!s.is_quoted());
    /// ```
    #[must_use]
    pub fn with_authorization(name: String, authorization: String) -> Self {
        Self { name, quoted: false, authorization: Some(authorization) }
    }

    /// Creates a new `Schema` with quoted-name metadata.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::structs::Schema;
    ///
    /// let s = Schema::with_quoted("MySchema".to_string(), true);
    /// assert_eq!(s.name(), "MySchema");
    /// assert!(s.is_quoted());
    /// assert_eq!(s.authorization(), None);
    /// ```
    #[must_use]
    pub fn with_quoted(name: String, quoted: bool) -> Self {
        Self { name, quoted, authorization: None }
    }

    /// Creates a new `Schema` with authorization and quoted-name metadata.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::structs::Schema;
    ///
    /// let s =
    ///     Schema::with_authorization_and_quoted("MySchema".to_string(), "admin".to_string(), true);
    /// assert_eq!(s.name(), "MySchema");
    /// assert_eq!(s.authorization(), Some("admin"));
    /// assert!(s.is_quoted());
    /// ```
    #[must_use]
    pub fn with_authorization_and_quoted(
        name: String,
        authorization: String,
        quoted: bool,
    ) -> Self {
        Self { name, quoted, authorization: Some(authorization) }
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

    /// Returns whether this schema name was quoted in SQL.
    #[must_use]
    pub fn is_quoted(&self) -> bool {
        self.quoted
    }
}
