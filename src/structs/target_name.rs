//! A name an object writes for one of its targets, before any resolution.

use core::fmt::{Display, Formatter, Result as FmtResult};

/// A name exactly as a schema object wrote it, with no resolution applied.
///
/// This is what the target readers hand back: a single one on
/// [`PolicyLike`](crate::traits::PolicyLike),
/// [`TriggerLike`](crate::traits::TriggerLike) and
/// [`ForeignKeyLike`](crate::traits::ForeignKeyLike), and a list on
/// [`GrantLike`](crate::traits::GrantLike). The four questions it answers are
/// the ones
/// [`TableLike`](crate::traits::TableLike) already asks of a stored table
/// name, so a caller applying its own resolution rules has everything the SQL
/// carried: the identifier, whether it was quoted, the qualifier if one was
/// written, and whether that was quoted. [`Display`] renders it back to SQL
/// text.
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
/// assert_eq!(qualified.to_string(), "app.\"Docs\"");
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

/// Writes one identifier, quoting it and doubling any embedded quote when it
/// was quoted in SQL.
fn write_identifier(f: &mut Formatter<'_>, value: &str, quoted: bool) -> FmtResult {
    if !quoted {
        return f.write_str(value);
    }
    f.write_str("\"")?;
    for (index, part) in value.split('"').enumerate() {
        if index > 0 {
            f.write_str("\"\"")?;
        }
        f.write_str(part)?;
    }
    f.write_str("\"")
}

impl Display for TargetName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        if let Some(schema) = self.schema {
            write_identifier(f, schema, self.schema_is_quoted)?;
            f.write_str(".")?;
        }
        write_identifier(f, self.name, self.name_is_quoted)
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::TargetName;
    use crate::utils::identifier_resolution::parse_lookup_identifier;

    /// A quoted identifier carrying a quote of its own is rendered with that
    /// quote doubled, which is the only escape SQL has, and reading the result
    /// back recovers the value the target held.
    #[test]
    fn an_embedded_quote_is_doubled_and_survives_a_round_trip() {
        let target = TargetName::new("A\"B", true);
        let rendered = target.to_string();
        assert_eq!(rendered, "\"A\"\"B\"");

        let reparsed = parse_lookup_identifier(&rendered);
        assert_eq!(reparsed.value(), "A\"B");
        assert!(reparsed.is_quoted());
    }

    /// Both parts escape independently of each other.
    #[test]
    fn each_part_escapes_on_its_own() {
        let both = TargetName::new("T\"1", true).with_schema("S\"2", true);
        assert_eq!(both.to_string(), "\"S\"\"2\".\"T\"\"1\"");

        let unquoted = TargetName::new("plain", false).with_schema("app", false);
        assert_eq!(unquoted.to_string(), "app.plain");

        // A lone quote is the shortest input that exercises the escape twice.
        assert_eq!(TargetName::new("\"", true).to_string(), "\"\"\"\"");
    }
}
