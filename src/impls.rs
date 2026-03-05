//! Submodule providing implementations of the traits defined in the `traits`
//! module.

mod sqlparser;

pub use sqlparser::apply_revoke_to_grant;
pub(crate) use sqlparser::{has_unsupported_column_scoped_revoke, partition_grantees_for_revoke};
