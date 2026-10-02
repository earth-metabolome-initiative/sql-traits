//! Returns a reference to the value at the last value in the provided
//! `ObjectName`.

use sqlparser::ast::ObjectName;

use crate::utils::object_name::object_name_last_part;

/// Returns a reference to the value at the last part of the provided
/// `ObjectName`.
///
/// The result is a display name, not a lookup key: it drops any schema
/// qualifier and the quoting of the identifier. Resolving a name against a
/// database MUST go through `resolve_object_name` instead, which honours both.
///
/// An empty name returns an empty string, and so does a name whose last part
/// is a call producing it when the statement runs, since neither carries an
/// identifier to display. The parser never produces an empty name, so a caller
/// receiving `""` built the name by hand.
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
/// // A name built when the statement runs displays as nothing, since the
/// // identifier it will carry is not known yet.
/// let func_part = ObjectNamePartFunction { name: Ident::new("IDENTIFIER"), args: vec![] };
/// let object_name_func = ObjectName(vec![ObjectNamePart::Function(func_part)]);
/// assert_eq!(last_str(&object_name_func), "");
///
/// // An empty name returns an empty string.
/// assert_eq!(last_str(&ObjectName(vec![])), "");
/// ```
#[must_use]
pub fn last_str(object_name: &ObjectName) -> &str {
    object_name_last_part(object_name).map_or("", |(value, _)| value)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    use super::last_str;

    #[test]
    fn empty_name_returns_empty_str() {
        assert_eq!(last_str(&ObjectName(vec![])), "");
    }

    #[test]
    fn identifier_part_returns_value() {
        let name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("foo"))]);
        assert_eq!(last_str(&name), "foo");
    }
}
