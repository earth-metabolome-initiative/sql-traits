//! Returns a reference to the value at the last value in the provided
//! `ObjectName`.

use sqlparser::ast::{ObjectName, ObjectNamePart, ObjectNamePartFunction};

/// Returns a reference to the value at the last value in the provided
/// `ObjectName`.
///
/// The result is a display name, not a lookup key: it drops any schema
/// qualifier and the quoting of the identifier. Resolving a name against a
/// database MUST go through `resolve_object_name` instead, which honours both.
///
/// # Arguments
///
/// * `object_name` - The `ObjectName` to extract the last part from.
///
/// # Panics
///
/// * Panics if the `ObjectName` has no parts.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::last_str;
/// use sqlparser::ast::{Ident, ObjectName, ObjectNamePart, ObjectNamePartFunction};
///
/// let object_name =
///     ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(Ident::new("table"))]);
/// assert_eq!(last_str(&object_name), "table");
///
/// // Test with function part
/// let func_part = ObjectNamePartFunction { name: Ident::new("func"), args: vec![] };
/// let object_name_func = ObjectName(vec![ObjectNamePart::Function(func_part)]);
/// assert_eq!(last_str(&object_name_func), "func");
/// ```
#[allow(
    clippy::expect_used,
    reason = "sqlparser guarantees every ObjectName has at least one part"
)]
#[must_use]
pub fn last_str(object_name: &ObjectName) -> &str {
    match &object_name.0.last().expect("ObjectName has no parts") {
        ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        ObjectNamePart::Function(ObjectNamePartFunction { name, .. }) => name.value.as_str(),
    }
}
