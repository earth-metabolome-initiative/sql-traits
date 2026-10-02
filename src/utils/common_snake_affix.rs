//! Utilities for detecting shared snake_case prefixes and suffixes.

/// Returns the shared snake_case prefix across all strings.
///
/// The returned prefix is the first segment (up to the first `_`) plus the `_`
/// itself. If any string lacks a `_` or the first segments differ (or the
/// iterator is empty), this returns `None`.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::common_column_name_snake_prefix;
///
/// let strings = vec!["alpha_one", "alpha_two", "alpha_three"];
/// assert_eq!(common_column_name_snake_prefix(strings.iter().copied()), Some("alpha_"));
///
/// let strings = ["user_id", "username"];
/// assert_eq!(common_column_name_snake_prefix(strings), None);
///
/// let empty: Vec<&str> = Vec::new();
/// assert_eq!(common_column_name_snake_prefix(empty), None);
/// ```
pub fn common_column_name_snake_prefix<'a, I>(strings: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = strings.into_iter();
    let first = iter.next()?;
    let (head, _) = first.split_once('_')?;
    for s in iter {
        if s.split_once('_').map(|(h, _)| h) != Some(head) {
            return None;
        }
    }
    Some(&first[..=head.len()])
}

/// Returns the shared snake_case suffix across all strings.
///
/// The returned suffix is the last `_` plus the final segment. If any string
/// lacks a `_` or the final segments differ (or the iterator is empty), this
/// returns `None`.
///
/// # Examples
///
/// ```
/// use sql_traits::utils::common_column_name_snake_suffix;
///
/// let strings = vec!["user_id", "group_id", "team_id"];
/// assert_eq!(common_column_name_snake_suffix(strings.iter().copied()), Some("_id"));
///
/// let strings = ["cat", "dog"];
/// assert_eq!(common_column_name_snake_suffix(strings), None);
///
/// let empty: Vec<&str> = Vec::new();
/// assert_eq!(common_column_name_snake_suffix(empty), None);
/// ```
pub fn common_column_name_snake_suffix<'a, I>(strings: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = strings.into_iter();
    let first = iter.next()?;
    let (before, tail) = first.rsplit_once('_')?;
    for s in iter {
        if s.rsplit_once('_').map(|(_, t)| t) != Some(tail) {
            return None;
        }
    }
    Some(&first[before.len()..])
}
