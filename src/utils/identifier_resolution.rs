//! Identifier resolution helpers with PostgreSQL matching semantics.
//!
//! PostgreSQL folds unquoted identifiers to lowercase and treats quoted
//! identifiers as exact/case-sensitive. Identifiers are additionally
//! Unicode-NFC-normalized and whitespace-trimmed per
//! FINGERPRINT_SPEC §7.1.

use alloc::borrow::Cow;

use unicode_normalization::UnicodeNormalization;

/// Parsed lookup identifier from a textual query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupIdentifier<'a> {
    value: Cow<'a, str>,
    quoted: bool,
}

impl LookupIdentifier<'_> {
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

/// Normalizes an identifier for comparison and fingerprint encoding.
///
/// Applies the FINGERPRINT_SPEC §7.1 / audit §5 rules:
/// 1. Trim surrounding ASCII whitespace.
/// 2. Apply Unicode NFC normalization so that byte-distinct but
///    canonically-equal identifiers produce the same normalized form (e.g.
///    precomposed `é` vs `e` + combining acute).
/// 3. ASCII-lowercase unquoted identifiers (matching PostgreSQL folding);
///    quoted identifiers retain their case post-NFC.
#[must_use]
pub fn normalize_identifier(value: &str, quoted: bool) -> Cow<'_, str> {
    let trimmed = value.trim();
    let needs_nfc = !trimmed.is_ascii();
    let after_nfc: Cow<'_, str> =
        if needs_nfc { Cow::Owned(trimmed.nfc().collect()) } else { Cow::Borrowed(trimmed) };

    if quoted {
        // Preserve case; only the trim+NFC pass applies.
        if needs_nfc || trimmed.len() != value.len() {
            Cow::Owned(after_nfc.into_owned())
        } else {
            Cow::Borrowed(trimmed)
        }
    } else {
        // PostgreSQL-style ASCII lowercase folding.
        Cow::Owned(after_nfc.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        identifiers_match, normalize_identifier, parse_lookup_identifier,
        stored_identifier_matches_lookup,
    };

    // ---------------------------------------------------------------
    // Spec §7.1 normalization tests (audit §5, P-02).
    //
    // `normalize_identifier` must trim surrounding whitespace, apply
    // Unicode NFC normalization to all identifiers, and lowercase only
    // unquoted identifiers (quoted preserve case after NFC).
    // ---------------------------------------------------------------

    /// INV-001: surrounding whitespace must be stripped before any
    /// downstream comparison or fingerprint encoding.
    #[test]
    fn test_inv_001_normalize_trims_whitespace() {
        assert_eq!(normalize_identifier("  users  ", false), normalize_identifier("users", false));
        assert_eq!(normalize_identifier("  Users  ", true), normalize_identifier("Users", true));
    }

    /// NFC equivalence: byte-distinct but canonically-equal Unicode
    /// identifiers must normalize to the same string. The fixture
    /// pairs precomposed `é` (U+00E9) with `e` + combining acute
    /// (U+0065 U+0301).
    #[test]
    fn test_nfc_normalization_unquoted() {
        let precomposed = "caf\u{00e9}";
        let decomposed = "cafe\u{0301}";
        assert_eq!(
            normalize_identifier(precomposed, false),
            normalize_identifier(decomposed, false)
        );
    }

    /// Same NFC equivalence applies to quoted identifiers (post-NFC
    /// case is preserved, but the underlying code points are
    /// normalized).
    #[test]
    fn test_nfc_normalization_quoted() {
        let precomposed = "caf\u{00e9}";
        let decomposed = "cafe\u{0301}";
        assert_eq!(normalize_identifier(precomposed, true), normalize_identifier(decomposed, true));
    }

    /// Quoted identifiers retain case after NFC.
    #[test]
    fn test_quoted_preserves_case_after_nfc() {
        let s = "caf\u{00e9}";
        let normalized = normalize_identifier(s, true);
        assert!(normalized.contains('c'));
        // Quoted should keep the lowercase 'c' from the input;
        // and should NOT lowercase any letter (it's a no-op on already-lowercase
        // here, but the contract is "preserve case").
        assert_eq!(normalize_identifier("Foo", true), "Foo");
    }

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
