//! Identifier resolution helpers with PostgreSQL matching semantics.
//!
//! PostgreSQL folds unquoted identifiers to lowercase and treats quoted
//! identifiers as exact/case-sensitive.

use std::borrow::Cow;

/// Parsed lookup identifier from a textual query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupIdentifier<'a> {
    value: Cow<'a, str>,
    quoted: bool,
}

impl<'a> LookupIdentifier<'a> {
    /// Returns the lookup identifier value (without surrounding quotes).
    #[must_use]
    pub fn value(&self) -> &str {
        self.value.as_ref()
    }

    /// Returns whether the lookup identifier was quoted.
    #[must_use]
    pub fn is_quoted(&self) -> bool {
        self.quoted
    }
}

/// Parses an identifier used in lookup APIs.
///
/// If `name` is wrapped with double quotes (`"..."`) it is treated as quoted.
/// Escaped quotes (`""`) are unescaped.
#[must_use]
pub fn parse_lookup_identifier(name: &str) -> LookupIdentifier<'_> {
    if name.len() >= 2 && name.starts_with('\"') && name.ends_with('\"') {
        let inner = &name[1..name.len() - 1];
        let value = if inner.contains("\"\"") {
            Cow::Owned(inner.replace("\"\"", "\""))
        } else {
            Cow::Borrowed(inner)
        };
        LookupIdentifier { value, quoted: true }
    } else {
        LookupIdentifier { value: Cow::Borrowed(name), quoted: false }
    }
}

/// Returns whether two identifiers refer to the same object following
/// PostgreSQL rules:
/// - quoted identifiers: exact/case-sensitive
/// - unquoted identifiers: case-insensitive via lowercase folding
#[must_use]
pub fn identifiers_match(
    left_value: &str,
    left_quoted: bool,
    right_value: &str,
    right_quoted: bool,
) -> bool {
    let left = normalize_identifier(left_value, left_quoted);
    let right = normalize_identifier(right_value, right_quoted);
    left == right
}

/// Returns whether a stored identifier matches a textual lookup identifier.
#[must_use]
pub fn stored_identifier_matches_lookup(
    stored_value: &str,
    stored_quoted: bool,
    lookup: &str,
) -> bool {
    let lookup_ident = parse_lookup_identifier(lookup);
    identifiers_match(stored_value, stored_quoted, lookup_ident.value(), lookup_ident.is_quoted())
}

fn normalize_identifier<'a>(value: &'a str, quoted: bool) -> Cow<'a, str> {
    if quoted { Cow::Borrowed(value) } else { Cow::Owned(value.to_ascii_lowercase()) }
}

#[cfg(test)]
mod tests {
    use super::{identifiers_match, parse_lookup_identifier, stored_identifier_matches_lookup};

    #[test]
    fn test_parse_lookup_identifier_unquoted() {
        let ident = parse_lookup_identifier("foo");
        assert_eq!(ident.value(), "foo");
        assert!(!ident.is_quoted());
    }

    #[test]
    fn test_parse_lookup_identifier_quoted() {
        let ident = parse_lookup_identifier("\"Foo\"");
        assert_eq!(ident.value(), "Foo");
        assert!(ident.is_quoted());
    }

    #[test]
    fn test_parse_lookup_identifier_quoted_unescapes_double_quotes() {
        let ident = parse_lookup_identifier("\"a\"\"b\"");
        assert_eq!(ident.value(), "a\"b");
        assert!(ident.is_quoted());
    }

    #[test]
    fn test_identifiers_match_postgres_rules() {
        // unquoted on both sides => case-insensitive
        assert!(identifiers_match("Foo", false, "foo", false));
        // quoted side preserves case
        assert!(identifiers_match("foo", false, "foo", true));
        assert!(!identifiers_match("Foo", false, "Foo", true));
        assert!(!identifiers_match("Foo", true, "foo", true));
    }

    #[test]
    fn test_stored_identifier_matches_lookup() {
        assert!(stored_identifier_matches_lookup("Foo", false, "foo"));
        assert!(stored_identifier_matches_lookup("Foo", false, "\"foo\""));
        assert!(!stored_identifier_matches_lookup("Foo", false, "\"Foo\""));

        assert!(stored_identifier_matches_lookup("Foo", true, "\"Foo\""));
        assert!(!stored_identifier_matches_lookup("Foo", true, "\"foo\""));
        assert!(!stored_identifier_matches_lookup("Foo", true, "foo"));
    }
}
