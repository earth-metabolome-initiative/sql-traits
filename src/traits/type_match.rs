//! Three-valued match results for dialect type predicates.
//!
//! Dialect predicates (`is_bool`, `is_uuid`, …) return an associated
//! [`DialectLike::Match`](crate::traits::DialectLike::Match) value rather than
//! a bare `bool`. This forces callers to distinguish a certain match (`Yes`)
//! from a merely plausible one (`Maybe`), which matters when a dialect stores
//! booleans as integer-affinity columns (SQLite) or UUIDs as `CHAR(36)` /
//! `BINARY(16)` (MySQL, SQLite, most non-native backends).
//!
//! Backends that want richer match semantics implement [`TypeMatchLike`] on
//! their own type; the canonical [`TypeMatch`] enum ships with `sql-traits`
//! and is what every stock backend uses.
//!
//! [`DialectLike`]: crate::traits::DialectLike

use core::fmt::Debug;

/// A dialect-side match result exposing certainty and possibility separately.
///
/// Implementers must be able to answer whether the result is a strict
/// positive ([`Self::is_yes`]) or a weaker "plausible under this dialect's
/// conventions" ([`Self::is_maybe`]). A negative match is anything that is
/// neither.
pub trait TypeMatchLike: Debug + Clone + Send + Sync + 'static {
    /// The DDL declaration unambiguously matches the queried type family.
    fn is_yes(&self) -> bool;

    /// The DDL declaration is compatible with the queried type family under
    /// this dialect's conventions but does not guarantee it (e.g. SQLite
    /// `INTEGER` may hold `0`/`1` booleans, MySQL `CHAR(36)` may hold a
    /// UUID string).
    fn is_maybe(&self) -> bool;

    /// Neither certain nor plausible.
    #[inline]
    fn is_no(&self) -> bool {
        !self.is_yes() && !self.is_maybe()
    }
}

/// Canonical three-valued match result used by every stock backend.
///
/// # Semantics
///
/// * [`TypeMatch::Yes`]: the DDL declares this exact type family. For example,
///   a Postgres `BOOLEAN` column against `is_bool`, or a Postgres `UUID` column
///   against `is_uuid`.
/// * [`TypeMatch::Maybe`]: the DDL is compatible with the queried type under
///   this dialect's storage conventions but is not specific enough to guarantee
///   it. For example, a SQLite `INTEGER` column against `is_bool` (SQLite
///   stores booleans as integer affinity), or a MySQL `CHAR(36)` column against
///   `is_uuid`.
/// * [`TypeMatch::No`]: the DDL is incompatible with the queried type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeMatch {
    /// The DDL unambiguously declares this type family.
    Yes,
    /// The DDL is compatible but not specific.
    Maybe,
    /// The DDL is incompatible with this type family.
    No,
}

impl TypeMatchLike for TypeMatch {
    #[inline]
    fn is_yes(&self) -> bool {
        matches!(self, Self::Yes)
    }

    #[inline]
    fn is_maybe(&self) -> bool {
        matches!(self, Self::Maybe)
    }

    #[inline]
    fn is_no(&self) -> bool {
        matches!(self, Self::No)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_is_partition() {
        for m in [TypeMatch::Yes, TypeMatch::Maybe, TypeMatch::No] {
            let yes = m.is_yes();
            let maybe = m.is_maybe();
            let no = m.is_no();
            assert_eq!(
                u8::from(yes) + u8::from(maybe) + u8::from(no),
                1,
                "{m:?} must map to exactly one bin"
            );
        }
    }

    /// Minimal [`TypeMatchLike`] impl that overrides only [`is_yes`] and
    /// [`is_maybe`], exercising the default body of [`is_no`].
    #[derive(Debug, Clone)]
    struct Stub {
        yes: bool,
        maybe: bool,
    }
    impl TypeMatchLike for Stub {
        fn is_yes(&self) -> bool {
            self.yes
        }
        fn is_maybe(&self) -> bool {
            self.maybe
        }
    }

    #[test]
    fn default_is_no_computes_neither() {
        assert!(Stub { yes: false, maybe: false }.is_no(), "neither yes nor maybe is a no");
        assert!(!Stub { yes: true, maybe: false }.is_no(), "yes shadows no");
        assert!(!Stub { yes: false, maybe: true }.is_no(), "maybe shadows no");
    }
}
