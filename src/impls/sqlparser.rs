//! Submodule providing implementations of the traits defined in the `traits`
//! module for the `sqlparser` crate.

mod check_constraint;
mod column_def;
mod create_function;
mod create_index;
mod create_policy;
mod create_role;
mod create_table;
mod create_trigger;
mod foreign_key_constraint;
mod grant;
mod schema;
mod unique_constraint;

pub use grant::grant_matches_revoke;
