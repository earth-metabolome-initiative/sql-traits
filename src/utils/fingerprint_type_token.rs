//! Maps SQL type strings to canonical type tokens for schema fingerprinting.
//!
//! The canonical tokens ensure that equivalent types across different SQL
//! dialects produce the same fingerprint.

/// Returns a canonical type token for the given SQL data type string.
///
/// Known types are mapped without allocation. Unknown types are returned as
/// `OTHER:<uppercase>`.
///
/// # Examples
///
/// ```rust
/// use sql_traits::utils::fingerprint_type_token::canonical_type_token;
///
/// assert_eq!(canonical_type_token("INT"), "INT");
/// assert_eq!(canonical_type_token("integer"), "INT");
/// assert_eq!(canonical_type_token("VARCHAR"), "STRING");
/// assert_eq!(canonical_type_token("geometry"), "OTHER:GEOMETRY");
/// ```
#[must_use]
pub fn canonical_type_token(sql_type: &str) -> String {
    // Trim and match case-insensitively against known families.
    let trimmed = sql_type.trim();

    // Fast path: try to match against known canonical families.
    if let Some(token) = match_known_type(trimmed) {
        return token.to_owned();
    }

    // Unknown type: emit OTHER:<UPPERCASE>.
    let mut result = String::with_capacity(6 + trimmed.len());
    result.push_str("OTHER:");
    for ch in trimmed.chars() {
        for upper in ch.to_uppercase() {
            result.push(upper);
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

    // Timestamp family
    if s.eq_ignore_ascii_case("TIMESTAMP")
        || s.eq_ignore_ascii_case("TIMESTAMP WITHOUT TIME ZONE")
        || s.eq_ignore_ascii_case("TIMESTAMP WITH TIME ZONE")
        || s.eq_ignore_ascii_case("TIMESTAMPTZ")
        || s.eq_ignore_ascii_case("DATETIME")
    {
        return Some("TIMESTAMP");
    }

    // UUID family
    if s.eq_ignore_ascii_case("UUID") {
        return Some("UUID");
    }

    // JSON family
    if s.eq_ignore_ascii_case("JSON") || s.eq_ignore_ascii_case("JSONB") {
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
        for ty in &[
            "TIMESTAMP",
            "TIMESTAMP WITHOUT TIME ZONE",
            "TIMESTAMP WITH TIME ZONE",
            "timestamptz",
            "DATETIME",
        ] {
            assert_eq!(canonical_type_token(ty), "TIMESTAMP", "failed for {ty}");
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
        assert_eq!(canonical_type_token("JSONB"), "JSON");
        assert_eq!(canonical_type_token("jsonb"), "JSON");
    }

    #[test]
    fn test_unknown_type() {
        assert_eq!(canonical_type_token("geometry"), "OTHER:GEOMETRY");
        assert_eq!(canonical_type_token("HSTORE"), "OTHER:HSTORE");
        assert_eq!(canonical_type_token("citext"), "OTHER:CITEXT");
    }

    #[test]
    fn test_whitespace_trimming() {
        assert_eq!(canonical_type_token("  INT  "), "INT");
        assert_eq!(canonical_type_token(" text "), "STRING");
    }
}
