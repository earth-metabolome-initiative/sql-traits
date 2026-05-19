//! Submodule providing a function for normalizing `SQLParser` data types.

use alloc::string::ToString;

use sqlparser::ast::{DataType, ObjectName, ObjectNamePart, TimezoneInfo};

/// Normalizes `SQLParser` data types to a standard representation.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::normalize_sqlparser_type;
/// use sqlparser::ast::{DataType, ExactNumberInfo, ObjectName, ObjectNamePart};
///
/// assert_eq!(normalize_sqlparser_type(&DataType::Text), "TEXT");
/// assert_eq!(normalize_sqlparser_type(&DataType::Int(None)), "INT");
/// assert_eq!(normalize_sqlparser_type(&DataType::BigInt(None)), "BIGINT");
/// assert_eq!(normalize_sqlparser_type(&DataType::Uuid), "UUID");
/// assert_eq!(normalize_sqlparser_type(&DataType::Date), "DATE");
/// assert_eq!(normalize_sqlparser_type(&DataType::JSON), "JSON");
/// assert_eq!(normalize_sqlparser_type(&DataType::Bytea), "BYTEA");
/// assert_eq!(normalize_sqlparser_type(&DataType::Decimal(ExactNumberInfo::None)), "DECIMAL",);
///
/// // Custom types
/// let custom = DataType::Custom(
///     ObjectName(vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new("GEOGRAPHY"))]),
///     vec!["Point".to_string(), "4326".to_string()],
/// );
/// assert_eq!(normalize_sqlparser_type(&custom), "GEOGRAPHY(Point, 4326)");
///
/// let custom_geom = DataType::Custom(
///     ObjectName(vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new("GEOMETRY"))]),
///     vec!["Point".to_string(), "4326".to_string()],
/// );
/// assert_eq!(normalize_sqlparser_type(&custom_geom), "GEOMETRY(Point, 4326)");
///
/// let custom_other = DataType::Custom(
///     ObjectName(vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new("OTHER"))]),
///     vec![],
/// );
/// assert_eq!(normalize_sqlparser_type(&custom_other), "OTHER");
/// ```
///
/// # Panics
///
/// Panics on data types not yet supported by the normalizer:
///
/// ```should_panic
/// use sql_traits::utils::normalize_sqlparser_type;
/// use sqlparser::ast::DataType;
///
/// // `HugeInt` has no canonical mapping yet — calling normalize panics.
/// normalize_sqlparser_type(&DataType::HugeInt);
/// ```
#[must_use]
#[inline]
pub fn normalize_sqlparser_type(sqlparser_type: &DataType) -> &str {
    match sqlparser_type {
        // INT family
        DataType::Int(_) | DataType::Integer(_) => "INT",
        DataType::SmallInt(_) => "SMALLINT",
        DataType::BigInt(_) => "BIGINT",
        DataType::TinyInt(_) => "TINYINT",
        DataType::MediumInt(_) => "MEDIUMINT",
        DataType::Int2(_) => "INT2",
        DataType::Int4(_) => "INT4",
        DataType::Int8(_) => "INT8",
        // FLOAT family
        DataType::Real => "REAL",
        DataType::Float(_) => "FLOAT",
        DataType::Double(_) => "DOUBLE",
        DataType::DoublePrecision => "DOUBLE PRECISION",
        // DECIMAL family
        DataType::Decimal(_) | DataType::Dec(_) | DataType::BigDecimal(_) => "DECIMAL",
        DataType::Numeric(_) | DataType::BigNumeric(_) => "NUMERIC",
        // BOOL family
        DataType::Bool | DataType::Boolean => "BOOLEAN",
        // STRING family
        DataType::Text => "TEXT",
        DataType::Varchar(_) => "VARCHAR",
        DataType::Char(_) | DataType::Character(_) => "CHAR",
        DataType::Clob(_) => "CLOB",
        DataType::Nvarchar(_) => "NVARCHAR",
        // BYTES family
        DataType::Bytea => "BYTEA",
        DataType::Bytes(_) => "BYTES",
        DataType::Blob(_) => "BLOB",
        DataType::Binary(_) => "BINARY",
        DataType::Varbinary(_) => "VARBINARY",
        // DATE
        DataType::Date => "DATE",
        // TIME family
        DataType::Time(_, TimezoneInfo::None) => "TIME",
        DataType::Time(_, TimezoneInfo::WithoutTimeZone) => "TIME WITHOUT TIME ZONE",
        DataType::Time(_, TimezoneInfo::WithTimeZone) => "TIME WITH TIME ZONE",
        DataType::Time(_, TimezoneInfo::Tz) => "TIMETZ",
        // TIMESTAMP family
        DataType::Timestamp(_, TimezoneInfo::None) => "TIMESTAMP",
        DataType::Timestamp(_, TimezoneInfo::WithoutTimeZone) => "TIMESTAMP WITHOUT TIME ZONE",
        DataType::Timestamp(_, TimezoneInfo::WithTimeZone) => "TIMESTAMP WITH TIME ZONE",
        DataType::Timestamp(_, TimezoneInfo::Tz) => "TIMESTAMPTZ",
        // UUID
        DataType::Uuid => "UUID",
        // JSON family
        DataType::JSON => "JSON",
        DataType::JSONB => "JSONB",
        // Custom: single-ident pass-through; pinned GEOGRAPHY/GEOMETRY recognition.
        DataType::Custom(ObjectName(object_names), segments) => {
            if let [ObjectNamePart::Identifier(ident)] = object_names.as_slice() {
                if ident.value.as_str() == "GEOGRAPHY" && segments == &["Point", "4326"] {
                    return "GEOGRAPHY(Point, 4326)";
                }
                if ident.value.as_str() == "GEOMETRY" && segments == &["Point", "4326"] {
                    return "GEOMETRY(Point, 4326)";
                }
                ident.value.as_str()
            } else {
                unimplemented!(
                    "Normalization for custom SQLParser data type `{sqlparser_type:?}` is not yet implemented for object names `{object_names:?}` and segments `{segments:?}`"
                )
            }
        }
        _ => {
            unimplemented!(
                "Normalization for SQLParser data type `{sqlparser_type:?}` is not yet implemented `{}`",
                sqlparser_type.to_string()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{DataType, Ident, ObjectName, ObjectNamePart, TimezoneInfo};

    use super::*;

    #[test]
    fn test_normalize_sqlparser_type_simple() {
        assert_eq!(normalize_sqlparser_type(&DataType::Uuid), "UUID");
        assert_eq!(normalize_sqlparser_type(&DataType::Text), "TEXT");
        assert_eq!(normalize_sqlparser_type(&DataType::Varchar(None)), "VARCHAR");
        assert_eq!(normalize_sqlparser_type(&DataType::Int(None)), "INT");
        assert_eq!(normalize_sqlparser_type(&DataType::Integer(None)), "INT");
        assert_eq!(normalize_sqlparser_type(&DataType::Real), "REAL");
        assert_eq!(normalize_sqlparser_type(&DataType::SmallInt(None)), "SMALLINT");
        assert_eq!(normalize_sqlparser_type(&DataType::Bool), "BOOLEAN");
        assert_eq!(normalize_sqlparser_type(&DataType::Boolean), "BOOLEAN");
    }

    #[test]
    fn test_normalize_sqlparser_type_timestamp() {
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::None)),
            "TIMESTAMP"
        );
        // `WithTimeZone` corresponds to the verbose SQL form and renders as
        // `TIMESTAMP WITH TIME ZONE`; the compact `TIMESTAMPTZ` form comes
        // from `TimezoneInfo::Tz`. Both fold to the canonical `TIMESTAMP`
        // type token in `fingerprint_type_token`.
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)),
            "TIMESTAMP WITH TIME ZONE"
        );
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::Tz)),
            "TIMESTAMPTZ"
        );
    }

    #[test]
    fn test_normalize_sqlparser_type_custom() {
        let geography = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("GEOGRAPHY"))]),
            vec!["Point".to_string(), "4326".to_string()],
        );
        assert_eq!(normalize_sqlparser_type(&geography), "GEOGRAPHY(Point, 4326)");

        let geometry = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("GEOMETRY"))]),
            vec!["Point".to_string(), "4326".to_string()],
        );
        assert_eq!(normalize_sqlparser_type(&geometry), "GEOMETRY(Point, 4326)");

        let other = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::new("OTHER"))]),
            vec![],
        );
        assert_eq!(normalize_sqlparser_type(&other), "OTHER");
    }

    #[test]
    #[should_panic(expected = "Normalization for SQLParser data type")]
    fn test_normalize_sqlparser_type_unimplemented_huge_int() {
        // `HugeInt` is genuinely unsupported by the current normalizer;
        // it keeps the unimplemented-fallthrough path under test after
        // BigInt and Date were promoted to supported variants.
        let _ = normalize_sqlparser_type(&DataType::HugeInt);
    }

    #[test]
    #[should_panic(expected = "Normalization for custom SQLParser data type")]
    fn test_normalize_sqlparser_type_custom_unimplemented() {
        let custom = DataType::Custom(
            ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("Many")),
                ObjectNamePart::Identifier(Ident::new("Parts")),
            ]),
            vec![],
        );
        let _ = normalize_sqlparser_type(&custom);
    }

    // -----------------------------------------------------------------
    // Plan-driven coverage (audit gap-A): every DataType variant that
    // maps cleanly to a canonical token family must normalize to a
    // string accepted by `fingerprint_type_token::match_known_type`.
    // -----------------------------------------------------------------

    #[test]
    fn test_normalize_sqlparser_type_int_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::Int(None)), "INT");
        assert_eq!(normalize_sqlparser_type(&DataType::Int(Some(10))), "INT");
        assert_eq!(normalize_sqlparser_type(&DataType::Integer(None)), "INT");
        assert_eq!(normalize_sqlparser_type(&DataType::SmallInt(None)), "SMALLINT");
        assert_eq!(normalize_sqlparser_type(&DataType::SmallInt(Some(5))), "SMALLINT");
        assert_eq!(normalize_sqlparser_type(&DataType::BigInt(None)), "BIGINT");
        assert_eq!(normalize_sqlparser_type(&DataType::BigInt(Some(20))), "BIGINT");
        assert_eq!(normalize_sqlparser_type(&DataType::TinyInt(None)), "TINYINT");
        assert_eq!(normalize_sqlparser_type(&DataType::MediumInt(None)), "MEDIUMINT");
        assert_eq!(normalize_sqlparser_type(&DataType::Int2(None)), "INT2");
        assert_eq!(normalize_sqlparser_type(&DataType::Int4(None)), "INT4");
        assert_eq!(normalize_sqlparser_type(&DataType::Int8(None)), "INT8");
    }

    #[test]
    fn test_normalize_sqlparser_type_float_family() {
        use sqlparser::ast::ExactNumberInfo;
        assert_eq!(normalize_sqlparser_type(&DataType::Real), "REAL");
        assert_eq!(normalize_sqlparser_type(&DataType::Float(ExactNumberInfo::None)), "FLOAT");
        assert_eq!(
            normalize_sqlparser_type(&DataType::Float(ExactNumberInfo::Precision(24))),
            "FLOAT"
        );
        assert_eq!(normalize_sqlparser_type(&DataType::Double(ExactNumberInfo::None)), "DOUBLE");
        assert_eq!(normalize_sqlparser_type(&DataType::DoublePrecision), "DOUBLE PRECISION");
    }

    #[test]
    fn test_normalize_sqlparser_type_decimal_family() {
        use sqlparser::ast::ExactNumberInfo;
        assert_eq!(normalize_sqlparser_type(&DataType::Decimal(ExactNumberInfo::None)), "DECIMAL");
        assert_eq!(
            normalize_sqlparser_type(&DataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 2))),
            "DECIMAL"
        );
        assert_eq!(normalize_sqlparser_type(&DataType::Numeric(ExactNumberInfo::None)), "NUMERIC");
        assert_eq!(normalize_sqlparser_type(&DataType::Dec(ExactNumberInfo::None)), "DECIMAL");
        assert_eq!(
            normalize_sqlparser_type(&DataType::BigNumeric(ExactNumberInfo::None)),
            "NUMERIC"
        );
        assert_eq!(
            normalize_sqlparser_type(&DataType::BigDecimal(ExactNumberInfo::None)),
            "DECIMAL"
        );
    }

    #[test]
    fn test_normalize_sqlparser_type_string_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::Text), "TEXT");
        assert_eq!(normalize_sqlparser_type(&DataType::Varchar(None)), "VARCHAR");
        assert_eq!(normalize_sqlparser_type(&DataType::Char(None)), "CHAR");
        assert_eq!(normalize_sqlparser_type(&DataType::Character(None)), "CHAR");
        assert_eq!(normalize_sqlparser_type(&DataType::Clob(None)), "CLOB");
        assert_eq!(normalize_sqlparser_type(&DataType::Nvarchar(None)), "NVARCHAR");
    }

    #[test]
    fn test_normalize_sqlparser_type_bytes_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::Bytea), "BYTEA");
        assert_eq!(normalize_sqlparser_type(&DataType::Bytes(None)), "BYTES");
        assert_eq!(normalize_sqlparser_type(&DataType::Blob(None)), "BLOB");
        assert_eq!(normalize_sqlparser_type(&DataType::Binary(None)), "BINARY");
        assert_eq!(normalize_sqlparser_type(&DataType::Varbinary(None)), "VARBINARY");
    }

    #[test]
    fn test_normalize_sqlparser_type_date_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::Date), "DATE");
    }

    #[test]
    fn test_normalize_sqlparser_type_time_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::Time(None, TimezoneInfo::None)), "TIME");
        assert_eq!(
            normalize_sqlparser_type(&DataType::Time(None, TimezoneInfo::WithoutTimeZone)),
            "TIME WITHOUT TIME ZONE"
        );
        assert_eq!(
            normalize_sqlparser_type(&DataType::Time(None, TimezoneInfo::WithTimeZone)),
            "TIME WITH TIME ZONE"
        );
        assert_eq!(normalize_sqlparser_type(&DataType::Time(None, TimezoneInfo::Tz)), "TIMETZ");
        assert_eq!(normalize_sqlparser_type(&DataType::Time(Some(6), TimezoneInfo::None)), "TIME");
    }

    #[test]
    fn test_normalize_sqlparser_type_json_family() {
        assert_eq!(normalize_sqlparser_type(&DataType::JSON), "JSON");
        assert_eq!(normalize_sqlparser_type(&DataType::JSONB), "JSONB");
    }

    #[test]
    fn test_normalize_sqlparser_type_timestamp_extended() {
        // Existing arms (TimezoneInfo::None, WithTimeZone) get widened
        // to accept Some(precision); add WithoutTimeZone and Tz.
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(Some(6), TimezoneInfo::None)),
            "TIMESTAMP"
        );
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::WithoutTimeZone)),
            "TIMESTAMP WITHOUT TIME ZONE"
        );
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::Tz)),
            "TIMESTAMPTZ"
        );
    }
}
