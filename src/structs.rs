//! Submodule providing general structs for representing database schemas.

pub(crate) mod fingerprint;
pub mod generic_db;
pub use generic_db::{
    AccessResolution, GenericDB, ParseOptions, ParserDB, ParserDBBuilder, UnresolvedAccessReference,
};
pub mod metadata;
mod schema;
mod target_name;

pub use fingerprint::{AlgorithmId, FingerprintError, SchemaFingerprint, canonical_bytes_v1};
pub use metadata::{TableAttribute, TableMetadata};
pub use schema::Schema;
pub use target_name::TargetName;
