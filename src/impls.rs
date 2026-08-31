//! Submodule providing implementations of the traits defined in the `traits`
//! module.

mod sqlparser;

pub use sqlparser::{SqlparserDialect, apply_revoke_to_grant};
pub(crate) use sqlparser::{dql, partition_grantees_for_revoke, validate_granted_columns};
