//! Submodule providing general structs for representing database schemas.

pub(crate) mod fingerprint;
pub mod generic_db;
pub use generic_db::{GenericDB, ParserDB, ParserDBBuilder};
pub mod metadata;
mod schema;

pub use fingerprint::SchemaFingerprint;
pub use metadata::{TableAttribute, TableMetadata};
pub use schema::Schema;
