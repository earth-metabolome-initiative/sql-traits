//! Maps SQL type strings to canonical type tokens for schema fingerprinting.
//!
//! The canonical tokens ensure that equivalent types across different SQL
//! dialects produce the same fingerprint.

use alloc::{borrow::ToOwned, string::String};

/// Returns a canonical type token for the given SQL data type string.
///
/// Known families map to fixed uppercase tokens (`INT`, `STRING`, …).
/// Unknown types are emitted as `OTHER:<lowercase-normalized-db-type>`
/// with internal whitespace collapsed to a single ASCII space, per
/// FINGERPRINT_SPEC §7.2.1.
///
/// # Examples
///
/// ```rust
/// use sql_traits::utils::fingerprint_type_token::canonical_type_token;
///
/// assert_eq!(canonical_type_token("INT"), "INT");
/// assert_eq!(canonical_type_token("integer"), "INT");
/// assert_eq!(canonical_type_token("VARCHAR"), "STRING");
/// assert_eq!(canonical_type_token("geometry"), "OTHER:geometry");
/// assert_eq!(canonical_type_token("INTERVAL  YEAR  TO  MONTH"), "OTHER:interval year to month");
/// assert_eq!(canonical_type_token("TIMESTAMP WITH TIME ZONE"), "TIMESTAMPTZ");
/// assert_eq!(canonical_type_token("JSONB"), "JSONB");
/// ```
#[must_use]
pub fn canonical_type_token(sql_type: &str) -> String {
    // Trim and match case-insensitively against known families.
    let trimmed = sql_type.trim();

    // Fast path: try to match against known canonical families.
    if let Some(token) = match_known_type(trimmed) {
        return token.to_owned();
    }

    // Unknown type: emit `OTHER:<lowercase-normalized-db-type>` with
    // internal whitespace runs collapsed to a single ASCII space.
    let mut result = String::with_capacity(6 + trimmed.len());
    result.push_str("OTHER:");
    let mut prev_was_space = false;
    for ch in trimmed.chars() {
        if ch.is_whitespace() {
            if !prev_was_space {
                result.push(' ');
                prev_was_space = true;
            }
        } else {
            for lower in ch.to_lowercase() {
                result.push(lower);
            }
            prev_was_space = false;
        }
    }
    result
}

/// Attempts to match a trimmed SQL type string (case-insensitive) against
/// known canonical families. Returns `None` for unknown types.
fn match_known_type(s: &str) -> Option<&'static str> {
    // Integer family
    if s.eq_ignore_ascii_case("INT")
        || s.eq_ignore_ascii_case("INTEGER")
        || s.eq_ignore_ascii_case("SMALLINT")
        || s.eq_ignore_ascii_case("BIGINT")
        || s.eq_ignore_ascii_case("TINYINT")
        || s.eq_ignore_ascii_case("MEDIUMINT")
        || s.eq_ignore_ascii_case("INT2")
        || s.eq_ignore_ascii_case("INT4")
        || s.eq_ignore_ascii_case("INT8")
        || s.eq_ignore_ascii_case("SERIAL")
        || s.eq_ignore_ascii_case("SMALLSERIAL")
        || s.eq_ignore_ascii_case("BIGSERIAL")
    {
        return Some("INT");
    }

    // Float family
    if s.eq_ignore_ascii_case("FLOAT")
        || s.eq_ignore_ascii_case("REAL")
        || s.eq_ignore_ascii_case("DOUBLE")
        || s.eq_ignore_ascii_case("DOUBLE PRECISION")
        || s.eq_ignore_ascii_case("FLOAT4")
        || s.eq_ignore_ascii_case("FLOAT8")
    {
        return Some("FLOAT");
    }

    // Decimal family
    if s.eq_ignore_ascii_case("DECIMAL")
        || s.eq_ignore_ascii_case("NUMERIC")
        || s.eq_ignore_ascii_case("NUMBER")
        || s.eq_ignore_ascii_case("MONEY")
    {
        return Some("DECIMAL");
    }

    // Boolean family
    if s.eq_ignore_ascii_case("BOOL") || s.eq_ignore_ascii_case("BOOLEAN") {
        return Some("BOOL");
    }

    // String family
    if s.eq_ignore_ascii_case("TEXT")
        || s.eq_ignore_ascii_case("VARCHAR")
        || s.eq_ignore_ascii_case("CHAR")
        || s.eq_ignore_ascii_case("CHARACTER")
        || s.eq_ignore_ascii_case("CHARACTER VARYING")
        || s.eq_ignore_ascii_case("NVARCHAR")
        || s.eq_ignore_ascii_case("NCHAR")
        || s.eq_ignore_ascii_case("CLOB")
        || s.eq_ignore_ascii_case("STRING")
    {
        return Some("STRING");
    }

    // Bytes family
    if s.eq_ignore_ascii_case("BYTEA")
        || s.eq_ignore_ascii_case("BLOB")
        || s.eq_ignore_ascii_case("BINARY")
        || s.eq_ignore_ascii_case("VARBINARY")
        || s.eq_ignore_ascii_case("BYTES")
    {
        return Some("BYTES");
    }

    // Date family
    if s.eq_ignore_ascii_case("DATE") {
        return Some("DATE");
    }

    // Time family
    if s.eq_ignore_ascii_case("TIME")
        || s.eq_ignore_ascii_case("TIME WITHOUT TIME ZONE")
        || s.eq_ignore_ascii_case("TIME WITH TIME ZONE")
        || s.eq_ignore_ascii_case("TIMETZ")
    {
        return Some("TIME");
    }

    // Timestamp-with-timezone family
    if s.eq_ignore_ascii_case("TIMESTAMPTZ")
        || s.eq_ignore_ascii_case("TIMESTAMP WITH TIME ZONE")
        || s.eq_ignore_ascii_case("TIMESTAMP WITH TIMEZONE")
    {
        return Some("TIMESTAMPTZ");
    }

    // Timestamp family
    if s.eq_ignore_ascii_case("TIMESTAMP")
        || s.eq_ignore_ascii_case("TIMESTAMP WITHOUT TIME ZONE")
        || s.eq_ignore_ascii_case("DATETIME")
    {
        return Some("TIMESTAMP");
    }

    // UUID family
    if s.eq_ignore_ascii_case("UUID") {
        return Some("UUID");
    }

    // JSONB family (Postgres binary JSON)
    if s.eq_ignore_ascii_case("JSONB") {
        return Some("JSONB");
    }

    // JSON family
    if s.eq_ignore_ascii_case("JSON") {
        return Some("JSON");
    }

    None
}

#[cfg(test)]
mod tests {
    use super::canonical_type_token;

    #[test]
    fn test_integer_family() {
        for ty in &[
            "INT",
            "integer",
            "SMALLINT",
            "bigint",
            "TINYINT",
            "MEDIUMINT",
            "int2",
            "INT4",
            "INT8",
            "SERIAL",
            "smallserial",
            "BIGSERIAL",
        ] {
            assert_eq!(canonical_type_token(ty), "INT", "failed for {ty}");
        }
    }

    #[test]
    fn test_float_family() {
        for ty in &["FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION", "float4", "float8"] {
            assert_eq!(canonical_type_token(ty), "FLOAT", "failed for {ty}");
        }
    }

    #[test]
    fn test_decimal_family() {
        for ty in &["DECIMAL", "NUMERIC", "number", "MONEY"] {
            assert_eq!(canonical_type_token(ty), "DECIMAL", "failed for {ty}");
        }
    }

    #[test]
    fn test_bool_family() {
        for ty in &["BOOL", "boolean"] {
            assert_eq!(canonical_type_token(ty), "BOOL", "failed for {ty}");
        }
    }

    #[test]
    fn test_string_family() {
        for ty in &[
            "TEXT",
            "VARCHAR",
            "CHAR",
            "CHARACTER",
            "CHARACTER VARYING",
            "nvarchar",
            "NCHAR",
            "CLOB",
            "STRING",
        ] {
            assert_eq!(canonical_type_token(ty), "STRING", "failed for {ty}");
        }
    }

    #[test]
    fn test_bytes_family() {
        for ty in &["BYTEA", "BLOB", "binary", "VARBINARY", "BYTES"] {
            assert_eq!(canonical_type_token(ty), "BYTES", "failed for {ty}");
        }
    }

    #[test]
    fn test_date() {
        assert_eq!(canonical_type_token("DATE"), "DATE");
        assert_eq!(canonical_type_token("date"), "DATE");
    }

    #[test]
    fn test_time_family() {
        for ty in &["TIME", "TIME WITHOUT TIME ZONE", "TIME WITH TIME ZONE", "timetz"] {
            assert_eq!(canonical_type_token(ty), "TIME", "failed for {ty}");
        }
    }

    #[test]
    fn test_timestamp_family() {
        for ty in &["TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "DATETIME"] {
            assert_eq!(canonical_type_token(ty), "TIMESTAMP", "failed for {ty}");
        }
    }

    #[test]
    fn test_timestamptz_family() {
        for ty in &[
            "TIMESTAMPTZ",
            "timestamptz",
            "TIMESTAMP WITH TIME ZONE",
            "timestamp with time zone",
            "TIMESTAMP WITH TIMEZONE",
            "timestamp with timezone",
        ] {
            assert_eq!(canonical_type_token(ty), "TIMESTAMPTZ", "failed for {ty}");
        }
    }

    #[test]
    fn test_uuid() {
        assert_eq!(canonical_type_token("UUID"), "UUID");
        assert_eq!(canonical_type_token("uuid"), "UUID");
    }

    #[test]
    fn test_json_family() {
        assert_eq!(canonical_type_token("JSON"), "JSON");
        assert_eq!(canonical_type_token("json"), "JSON");
    }

    #[test]
    fn test_jsonb_family() {
        assert_eq!(canonical_type_token("JSONB"), "JSONB");
        assert_eq!(canonical_type_token("jsonb"), "JSONB");
    }

    #[test]
    fn test_unknown_type() {
        // Spec §7.2.1: OTHER tokens are lowercase.
        assert_eq!(canonical_type_token("geometry"), "OTHER:geometry");
        assert_eq!(canonical_type_token("HSTORE"), "OTHER:hstore");
        assert_eq!(canonical_type_token("citext"), "OTHER:citext");
        // Mixed case folds to lowercase too.
        assert_eq!(canonical_type_token("Geometry"), "OTHER:geometry");
    }

    #[test]
    fn test_unknown_type_collapses_whitespace() {
        // Spec §7.2.1: internal whitespace runs collapse to a single ASCII space.
        assert_eq!(
            canonical_type_token("INTERVAL  YEAR  TO  MONTH"),
            "OTHER:interval year to month"
        );
        // Tabs and other whitespace also collapse.
        assert_eq!(canonical_type_token("foo\tbar  \t baz"), "OTHER:foo bar baz");
    }

    #[test]
    fn test_whitespace_trimming() {
        assert_eq!(canonical_type_token("  INT  "), "INT");
        assert_eq!(canonical_type_token(" text "), "STRING");
    }
}
