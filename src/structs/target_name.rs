//! A name an object writes for one of its targets, before any resolution.

/// A name exactly as a schema object wrote it, with no resolution applied.
///
/// This is the item type of the target readers on
/// [`GrantLike`](crate::traits::GrantLike), which yield lists rather than a
/// single target. The four questions it answers are the ones
/// [`TableLike`](crate::traits::TableLike) already asks of a stored table
/// name, so a caller applying its own resolution rules has everything the SQL
/// carried: the identifier, whether it was quoted, the qualifier if one was
/// written, and whether that was quoted.
///
/// # Example
///
/// ```rust
/// use sql_traits::prelude::*;
///
/// let bare = TargetName::new("docs", false);
/// assert_eq!(bare.name(), "docs");
/// assert_eq!(bare.schema(), None);
///
/// let qualified = TargetName::new("Docs", true).with_schema("app", false);
/// assert_eq!(qualified.name(), "Docs");
/// assert!(qualified.name_is_quoted());
/// assert_eq!(qualified.schema(), Some("app"));
/// assert!(!qualified.schema_is_quoted());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TargetName<'a> {
    name: &'a str,
    name_is_quoted: bool,
    schema: Option<&'a str>,
    schema_is_quoted: bool,
}

impl<'a> TargetName<'a> {
    /// Creates an unqualified target name.
    #[must_use]
    pub fn new(name: &'a str, name_is_quoted: bool) -> Self {
        Self { name, name_is_quoted, schema: None, schema_is_quoted: false }
    }

    /// Adds the qualifier the SQL wrote in front of the name.
    #[must_use]
    pub fn with_schema(self, schema: &'a str, schema_is_quoted: bool) -> Self {
        Self { schema: Some(schema), schema_is_quoted, ..self }
    }

    /// Returns the identifier as written, without its surrounding quotes.
    #[must_use]
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// Returns whether the identifier was quoted in SQL.
    #[must_use]
    pub fn name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    /// Returns the qualifier written in front of the name, if any.
    #[must_use]
    pub fn schema(&self) -> Option<&'a str> {
        self.schema
    }

    /// Returns whether that qualifier was quoted in SQL.
    ///
    /// This only matters when [`Self::schema`] returns `Some`.
    #[must_use]
    pub fn schema_is_quoted(&self) -> bool {
        self.schema_is_quoted
    }
}
