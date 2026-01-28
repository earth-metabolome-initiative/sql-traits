//! Utilities for detecting shared snake_case prefixes and suffixes.

/// Returns the shared snake_case prefix across all strings.
///
/// The returned prefix ends at the last `_` boundary within the common prefix.
/// If no `_` boundary exists in the common prefix (or the iterator is empty),
/// this returns `None`.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::common_snake_prefix;
///
/// let strings = vec!["alpha_one", "alpha_two", "alpha_three"];
/// assert_eq!(common_snake_prefix(strings.iter().copied()), Some("alpha_"));
///
/// let strings = ["cat", "caterpillar"];
/// assert_eq!(common_snake_prefix(strings), None);
///
/// let empty: Vec<&str> = Vec::new();
/// assert_eq!(common_snake_prefix(empty), None);
/// ```
#[must_use]
pub fn common_snake_prefix<'a, I>(strings: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = strings.into_iter();
    let first = iter.next()?;
    let first_bytes = first.as_bytes();
    let mut len = first_bytes.len();

    for s in iter {
        let bytes = s.as_bytes();
        len = len.min(bytes.len());
        let mut i = 0;
        while i < len && first_bytes[i] == bytes[i] {
            i += 1;
        }
        len = i;
        if len == 0 {
            break;
        }
    }

    let prefix = &first[..len];
    let underscore = prefix.rfind('_')?;
    Some(&first[..=underscore])
}

/// Returns the shared snake_case suffix across all strings.
///
/// The returned suffix starts at the first `_` boundary within the common
/// suffix. If no `_` boundary exists in the common suffix (or the iterator is
/// empty), this returns `None`.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::common_snake_suffix;
///
/// let strings = vec!["user_id", "group_id", "team_id"];
/// assert_eq!(common_snake_suffix(strings.iter().copied()), Some("_id"));
///
/// let strings = ["cat", "dog"];
/// assert_eq!(common_snake_suffix(strings), None);
///
/// let empty: Vec<&str> = Vec::new();
/// assert_eq!(common_snake_suffix(empty), None);
/// ```
#[must_use]
pub fn common_snake_suffix<'a, I>(strings: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = strings.into_iter();
    let first = iter.next()?;
    let first_bytes = first.as_bytes();
    let mut len = first_bytes.len();

    for s in iter {
        let bytes = s.as_bytes();
        len = len.min(bytes.len());
        let mut i = 0;
        while i < len && first_bytes[first_bytes.len() - 1 - i] == bytes[bytes.len() - 1 - i] {
            i += 1;
        }
        len = i;
        if len == 0 {
            break;
        }
    }

    let suffix = &first[first.len() - len..];
    let underscore = suffix.find('_')?;
    Some(&suffix[underscore..])
}
