//! Submodule providing a function for normalizing `PostgreSQL` data types.

/// Normalizes `PostgreSQL` data types to a standard representation.
///
/// # Arguments
///
/// * `pg_type`: The `PostgreSQL` data type as a string slice.
///
/// # Returns
///
/// The normalized `PostgreSQL` data type as a string slice.
///
/// # Examples
///
/// ```rust
/// use sql_traits::utils::normalize_postgres_type;
///
/// let normalized = normalize_postgres_type("INT4");
/// assert_eq!(normalized, "INT");
/// ```
#[must_use]
#[inline]
pub fn normalize_postgres_type(pg_type: &str) -> &str {
    match pg_type.to_lowercase().trim_matches('\"') {
        "int2" | "smallint" | "smallserial" => "SMALLINT",
        "int4" | "integer" | "serial" => "INT",
        "int8" | "bigint" | "bigserial" => "BIGINT",
        "float4" | "real" => "real",
        "float8" | "double precision" => "double precision",
        "numeric" | "decimal" => "numeric",
        "bool" | "boolean" => "boolean",
        "varchar" | "character varying" => "VARCHAR",
        "char" | "character" => "CHAR",
        "text" => "TEXT",
        "date" => "date",
        "uuid" => "UUID",
        "timestamp" | "timestamp without time zone" => "timestamp without time zone",
        "timestamptz" | "timestamp with time zone" => "timestamp with time zone",
        "time" | "time without time zone" => "time without time zone",
        "timetz" | "time with time zone" => "time with time zone",
        "bytea" => "bytea",
        _ => pg_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_postgres_type_integers() {
        assert_eq!(normalize_postgres_type("int2"), "SMALLINT");
        assert_eq!(normalize_postgres_type("smallint"), "SMALLINT");
        assert_eq!(normalize_postgres_type("smallserial"), "SMALLINT");

        assert_eq!(normalize_postgres_type("int4"), "INT");
        assert_eq!(normalize_postgres_type("integer"), "INT");
        assert_eq!(normalize_postgres_type("serial"), "INT");

        assert_eq!(normalize_postgres_type("int8"), "BIGINT");
        assert_eq!(normalize_postgres_type("bigint"), "BIGINT");
        assert_eq!(normalize_postgres_type("bigserial"), "BIGINT");
    }

    #[test]
    fn test_normalize_postgres_type_floats() {
        assert_eq!(normalize_postgres_type("float4"), "real");
        assert_eq!(normalize_postgres_type("real"), "real");

        assert_eq!(normalize_postgres_type("float8"), "double precision");
        assert_eq!(normalize_postgres_type("double precision"), "double precision");

        assert_eq!(normalize_postgres_type("numeric"), "numeric");
        assert_eq!(normalize_postgres_type("decimal"), "numeric");
    }

    #[test]
    fn test_normalize_postgres_type_boolean() {
        assert_eq!(normalize_postgres_type("bool"), "boolean");
        assert_eq!(normalize_postgres_type("boolean"), "boolean");
    }

    #[test]
    fn test_normalize_postgres_type_strings() {
        assert_eq!(normalize_postgres_type("varchar"), "VARCHAR");
        assert_eq!(normalize_postgres_type("character varying"), "VARCHAR");

        assert_eq!(normalize_postgres_type("char"), "CHAR");
        assert_eq!(normalize_postgres_type("character"), "CHAR");

        assert_eq!(normalize_postgres_type("text"), "TEXT");
    }

    #[test]
    fn test_normalize_postgres_type_dates() {
        assert_eq!(normalize_postgres_type("date"), "date");
    }

    #[test]
    fn test_normalize_postgres_type_uuid() {
        assert_eq!(normalize_postgres_type("uuid"), "UUID");
    }

    #[test]
    fn test_normalize_postgres_type_timestamps() {
        assert_eq!(normalize_postgres_type("timestamp"), "timestamp without time zone");
        assert_eq!(
            normalize_postgres_type("timestamp without time zone"),
            "timestamp without time zone"
        );

        assert_eq!(normalize_postgres_type("timestamptz"), "timestamp with time zone");
        assert_eq!(normalize_postgres_type("timestamp with time zone"), "timestamp with time zone");

        assert_eq!(normalize_postgres_type("time"), "time without time zone");
        assert_eq!(normalize_postgres_type("time without time zone"), "time without time zone");

        assert_eq!(normalize_postgres_type("timetz"), "time with time zone");
        assert_eq!(normalize_postgres_type("time with time zone"), "time with time zone");
    }

    #[test]
    fn test_normalize_postgres_type_binary() {
        assert_eq!(normalize_postgres_type("bytea"), "bytea");
    }

    #[test]
    fn test_normalize_postgres_type_case_sensitivity() {
        assert_eq!(normalize_postgres_type("INT2"), "SMALLINT");
        assert_eq!(normalize_postgres_type("InT4"), "INT");
        assert_eq!(normalize_postgres_type("BiGiNt"), "BIGINT");
        assert_eq!(normalize_postgres_type("VARCHAR"), "VARCHAR");
    }

    #[test]
    fn test_normalize_postgres_type_quoted() {
        assert_eq!(normalize_postgres_type("\"int2\""), "SMALLINT");
        assert_eq!(normalize_postgres_type("\"varchar\""), "VARCHAR");
    }

    #[test]
    fn test_normalize_postgres_type_fallback() {
        assert_eq!(normalize_postgres_type("custom_type"), "custom_type");
        assert_eq!(normalize_postgres_type("unknown"), "unknown");
    }
}
