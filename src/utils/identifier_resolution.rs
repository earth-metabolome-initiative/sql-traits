//! Identifier resolution helpers with PostgreSQL matching semantics.
//!
//! PostgreSQL folds unquoted identifiers to lowercase and treats quoted
//! identifiers as exact/case-sensitive. Identifiers are additionally
//! Unicode-NFC-normalized and whitespace-trimmed per
//! FINGERPRINT_SPEC §7.1.

use alloc::{borrow::Cow, string::String};

use sqlparser::ast::{Function, Ident, ObjectNamePart};
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

/// Returns whether an identifier names the `PUBLIC` pseudo-role, meaning every
/// role, rather than a role somebody created.
///
/// SQL spells "everyone" as an unquoted `PUBLIC`, which the grammar hands back
/// as an ordinary identifier. A quoted `"PUBLIC"` is a role of that exact name
/// and is not the pseudo-role.
#[must_use]
pub fn is_public_pseudo_role(value: &str, quoted: bool) -> bool {
    !quoted && value.eq_ignore_ascii_case("PUBLIC")
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
    if trimmed.is_ascii() {
        if quoted || !trimmed.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Borrowed(trimmed)
        } else {
            Cow::Owned(trimmed.to_ascii_lowercase())
        }
    } else {
        let mut normalized: String = trimmed.nfc().collect();
        if !quoted {
            normalized.make_ascii_lowercase();
        }
        Cow::Owned(normalized)
    }
}

/// Returns the name PostgreSQL stores for a written identifier.
///
/// This is [`normalize_identifier`] applied to the parser's own node, which
/// carries the quoting the writer used.
///
/// # Example
///
/// ```rust
/// use sql_traits::utils::identifier_resolution::stored_ident_name;
/// use sqlparser::ast::Ident;
///
/// assert_eq!(stored_ident_name(&Ident::new("Owner_Id")), "owner_id");
/// assert_eq!(stored_ident_name(&Ident::with_quote('"', "Owner_Id")), "Owner_Id");
/// ```
#[must_use]
pub fn stored_ident_name(ident: &Ident) -> Cow<'_, str> {
    normalize_identifier(&ident.value, ident.quote_style.is_some())
}

/// Returns the terminal identifier of a written function name.
fn terminal_ident(part: &ObjectNamePart) -> &Ident {
    match part {
        ObjectNamePart::Identifier(ident) => ident,
        ObjectNamePart::Function(function) => &function.name,
    }
}

/// Returns the folded terminal name of a call, and [`None`] when that name was
/// quoted.
///
/// This is the form a keyword or a display name is compared against, so a
/// quoted spelling is refused rather than folded: `"now"` is a function
/// somebody declared under that exact name, not the keyword `now`.
///
/// The qualifier is ignored, so this says what a call is called and not which
/// function it reaches. Use [`builtin_function_name`] to decide whether a call
/// can only be a catalog builtin.
///
/// # Example
///
/// ```rust
/// # fn main() -> Result<(), sqlparser::parser::ParserError> {
/// use sql_traits::utils::identifier_resolution::folded_function_name;
/// use sqlparser::{ast::Expr, dialect::PostgreSqlDialect, parser::Parser};
///
/// let written = |sql: &str| -> Result<Option<String>, sqlparser::parser::ParserError> {
///     let expr = Parser::new(&PostgreSqlDialect {}).try_with_sql(sql)?.parse_expr()?;
///     let Expr::Function(call) = expr else { return Ok(None) };
///     Ok(folded_function_name(&call))
/// };
///
/// assert_eq!(written("NOW()")?.as_deref(), Some("now"));
/// assert_eq!(written("app.NOW()")?.as_deref(), Some("now"));
/// assert_eq!(written("\"now\"()")?, None);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn folded_function_name(function: &Function) -> Option<String> {
    let terminal = terminal_ident(function.name.0.last()?);
    terminal.quote_style.is_none().then(|| terminal.value.to_ascii_lowercase())
}

/// Returns the catalog builtin a call can only be, and [`None`] when the
/// spelling could name a declared function instead.
///
/// A call qualifies as a builtin only when its terminal identifier is unquoted
/// and the name is either unqualified or qualified by exactly `pg_catalog`,
/// which the quoted spelling `"pg_catalog"` also names. Every other schema
/// names a declared function whatever the terminal says, which is how
/// `app.now()` shadows the clock, and a name of three or more parts is refused
/// outright.
///
/// A caller that reads a builtin as something it can trust, a clock or a
/// current-user probe, has to refuse those spellings: taking `app.now()` for
/// the real clock turns a policy that grants nothing into one that grants
/// every row whose expiry has not passed.
///
/// One residue is knowingly out of scope: an unqualified call that a declared
/// function shadows earlier on the search path is still read as the builtin.
/// Resolving it needs the database, and no dump produces that shape, because a
/// dump qualifies the calls it emits.
///
/// # Example
///
/// ```rust
/// # fn main() -> Result<(), sqlparser::parser::ParserError> {
/// use sql_traits::utils::identifier_resolution::builtin_function_name;
/// use sqlparser::{ast::Expr, dialect::PostgreSqlDialect, parser::Parser};
///
/// let written = |sql: &str| -> Result<Option<String>, sqlparser::parser::ParserError> {
///     let expr = Parser::new(&PostgreSqlDialect {}).try_with_sql(sql)?.parse_expr()?;
///     let Expr::Function(call) = expr else { return Ok(None) };
///     Ok(builtin_function_name(&call))
/// };
///
/// assert_eq!(written("NOW()")?.as_deref(), Some("now"));
/// assert_eq!(written("pg_catalog.now()")?.as_deref(), Some("now"));
/// assert_eq!(written("\"pg_catalog\".now()")?.as_deref(), Some("now"));
/// assert_eq!(written("app.now()")?, None);
/// assert_eq!(written("\"now\"()")?, None);
/// assert_eq!(written("a.b.now()")?, None);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn builtin_function_name(function: &Function) -> Option<String> {
    let (schema, terminal) = match function.name.0.as_slice() {
        [terminal] => (None, terminal_ident(terminal)),
        [schema, terminal] => (Some(terminal_ident(schema)), terminal_ident(terminal)),
        _ => return None,
    };
    if schema.is_some_and(|schema| stored_ident_name(schema) != "pg_catalog") {
        return None;
    }
    terminal.quote_style.is_none().then(|| terminal.value.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{Expr, Function, Ident},
        dialect::PostgreSqlDialect,
        parser::Parser,
    };

    use super::{
        builtin_function_name, folded_function_name, identifiers_match, normalize_identifier,
        parse_lookup_identifier, stored_ident_name, stored_identifier_matches_lookup,
    };

    /// Parses a written call into the AST node the helpers read.
    fn call(sql: &str) -> Function {
        let dialect = PostgreSqlDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(sql).expect("the call tokenizes");
        match parser.parse_expr().expect("the call parses") {
            Expr::Function(function) => function,
            other => panic!("`{sql}` is not a function call: {other:?}"),
        }
    }

    #[test]
    fn stored_ident_name_follows_the_parsed_quote_style() {
        assert_eq!(stored_ident_name(&Ident::new("Owner_Id")), "owner_id");
        assert_eq!(stored_ident_name(&Ident::with_quote('"', "Owner_Id")), "Owner_Id");
    }

    #[test]
    fn folded_function_name_reads_the_terminal_identifier() {
        assert_eq!(folded_function_name(&call("NOW()")).as_deref(), Some("now"));
        assert_eq!(folded_function_name(&call("app.NOW()")).as_deref(), Some("now"));
        assert_eq!(folded_function_name(&call("pg_catalog.now()")).as_deref(), Some("now"));
        assert_eq!(folded_function_name(&call("\"now\"()")), None);
    }

    #[test]
    fn builtin_function_name_accepts_only_the_catalog() {
        assert_eq!(builtin_function_name(&call("now()")).as_deref(), Some("now"));
        assert_eq!(builtin_function_name(&call("NOW()")).as_deref(), Some("now"));
        assert_eq!(builtin_function_name(&call("pg_catalog.now()")).as_deref(), Some("now"));
        assert_eq!(builtin_function_name(&call("\"pg_catalog\".now()")).as_deref(), Some("now"));
        assert_eq!(builtin_function_name(&call("PG_CATALOG.now()")).as_deref(), Some("now"));
        assert_eq!(builtin_function_name(&call("app.now()")), None);
        assert_eq!(builtin_function_name(&call("\"now\"()")), None);
        assert_eq!(builtin_function_name(&call("a.b.now()")), None);
        assert_eq!(builtin_function_name(&call("\"PG_CATALOG\".now()")), None);
    }

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
        // and should NOT lowercase any letter (it's a no-op on
        // already-lowercase here, but the contract is "preserve case").
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
