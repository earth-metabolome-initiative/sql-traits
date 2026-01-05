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
///     vec!["Point".to_string(), "4326".to_string()]
/// );
/// assert_eq!(normalize_sqlparser_type(&custom), "GEOGRAPHY(Point, 4326)");
///
/// let custom_geom = DataType::Custom(
///     ObjectName(vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new("GEOMETRY"))]),
///     vec!["Point".to_string(), "4326".to_string()]
/// );
/// assert_eq!(normalize_sqlparser_type(&custom_geom), "GEOMETRY(Point, 4326)");
///
/// let custom_other = DataType::Custom(
///     ObjectName(vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new("OTHER"))]),
///     vec![]
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
