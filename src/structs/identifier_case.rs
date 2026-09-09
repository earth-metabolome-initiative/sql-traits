//! The comparison a lookup applies to the identifiers it matches.

use alloc::borrow::Cow;

use crate::utils::identifier_resolution::normalize_identifier;

/// How a lookup compares a written identifier against a stored one.
///
/// The three engines this crate serves do not share one rule, and no catalog
/// can carry the rule for them. PostgreSQL folds an unquoted identifier and
/// reads a quoted one literally, with no setting that changes it. SQLite
/// compares a relation name case-insensitively for ASCII whether or not it was
/// quoted. MySQL follows `lower_case_table_names`, which is fixed when the
/// server is initialised and appears in no DDL, so one catalog parsed from a
/// dump can be read by two servers that disagree about it. The comparison is
/// therefore an argument to every lookup rather than a property of the
/// database.
///
/// All three cases trim the identifier and apply Unicode NFC normalization
/// first, as [`normalize_identifier`] does, so a precomposed name and a
/// decomposed one are one identifier under each of them.
///
/// # Example
///
/// ```rust
/// use sql_traits::prelude::*;
///
/// // A table the DDL wrote quoted, so the catalog stores its case.
/// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE \"Docs\" (id INT);")?;
/// let written = || TargetName::new("docs", false);
///
/// // PostgreSQL reads the stored name literally and this misses.
/// assert!(db.resolve_target_table(written(), IdentifierCase::AsWritten)?.is_none());
/// // SQLite, and MySQL with `lower_case_table_names` at 1 or 2, resolve it.
/// assert!(db.resolve_target_table(written(), IdentifierCase::Folded)?.is_some());
/// # Ok::<(), sql_traits::errors::Error>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IdentifierCase {
    /// Quoting decides, which is PostgreSQL's rule: an unquoted identifier
    /// folds and a quoted one is compared as it stands.
    AsWritten,
    /// Both sides fold, whatever quoting either carried.
    ///
    /// Folding is ASCII-only, which is SQLite's rule rather than an
    /// approximation of it: every schema comparison goes through
    /// `sqlite3StrICmp`, whose table maps bytes 65 to 90 and leaves every byte
    /// above 127 as itself. A MySQL utf8 collation does fold beyond ASCII, so
    /// a name outside ASCII compares case-sensitively here even for a server
    /// that would fold it.
    Folded,
    /// Neither side folds, whatever quoting either carried, so the stored name
    /// is matched as the catalog holds it.
    ///
    /// This is MySQL with `lower_case_table_names = 0`, the Unix default,
    /// where a table created `Docs` is not reached by `docs`.
    Exact,
}

impl IdentifierCase {
    /// Whether an identifier carrying this quoting folds under the rule.
    #[must_use]
    #[inline]
    pub const fn folds(self, quoted: bool) -> bool {
        match self {
            Self::AsWritten => !quoted,
            Self::Folded => true,
            Self::Exact => false,
        }
    }

    /// The form an identifier is compared under, trimmed and NFC-normalized,
    /// folded when the rule folds it.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::prelude::*;
    ///
    /// assert_eq!(IdentifierCase::AsWritten.compared_form("Docs", true), "Docs");
    /// assert_eq!(IdentifierCase::AsWritten.compared_form("Docs", false), "docs");
    /// assert_eq!(IdentifierCase::Folded.compared_form("Docs", true), "docs");
    /// assert_eq!(IdentifierCase::Exact.compared_form("Docs", false), "Docs");
    /// ```
    #[must_use]
    #[inline]
    pub fn compared_form(self, value: &str, quoted: bool) -> Cow<'_, str> {
        normalize_identifier(value, !self.folds(quoted))
    }
}
