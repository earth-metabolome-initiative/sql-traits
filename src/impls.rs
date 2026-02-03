//! Submodule providing implementations of the traits defined in the `traits`
//! module.

mod sqlparser;

pub use sqlparser::grant_matches_revoke;
