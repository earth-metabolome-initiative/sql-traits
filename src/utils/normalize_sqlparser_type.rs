//! Submodule providing a function for normalizing `SQLParser` data types.

use sqlparser::ast::{DataType, ObjectName, ObjectNamePart, TimezoneInfo};

/// Normalizes `SQLParser` data types to a standard representation.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::normalize_sqlparser_type;
/// use sqlparser::ast::{DataType, ObjectName, ObjectNamePart};
///
/// assert_eq!(normalize_sqlparser_type(&DataType::Text), "TEXT");
/// assert_eq!(normalize_sqlparser_type(&DataType::Int(None)), "INT");
/// assert_eq!(normalize_sqlparser_type(&DataType::Uuid), "UUID");
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
/// Panics on unsupported data types:
///
/// ```should_panic
/// use sql_traits::utils::normalize_sqlparser_type;
/// use sqlparser::ast::DataType;
///
/// // This will panic as BIGINT is not supported
/// normalize_sqlparser_type(&DataType::BigInt(None));
/// ```
#[must_use]
#[inline]
pub fn normalize_sqlparser_type(sqlparser_type: &DataType) -> &str {
    match sqlparser_type {
        DataType::Uuid => "UUID",
        DataType::Text => "TEXT",
        DataType::Varchar(_) => "VARCHAR",
        DataType::Int(None) | DataType::Integer(None) => "INT",
        DataType::Real => "REAL",
        DataType::SmallInt(None) => "SMALLINT",
        DataType::Bool | DataType::Boolean => "BOOLEAN",
        DataType::Timestamp(None, TimezoneInfo::None) => "TIMESTAMP",
        DataType::Timestamp(None, TimezoneInfo::WithTimeZone) => "TIMESTAMPTZ",
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
        assert_eq!(
            normalize_sqlparser_type(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)),
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
    fn test_normalize_sqlparser_type_unimplemented() {
        let _ = normalize_sqlparser_type(&DataType::BigInt(None));
    }

    #[test]
    #[should_panic(expected = "Normalization for SQLParser data type")]
    fn test_normalize_sqlparser_type_unimplemented_date() {
        let _ = normalize_sqlparser_type(&DataType::Date);
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
}
