//! SHA-256 based schema fingerprint for deterministic, persistable table
//! identity.

use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    traits::{ColumnLike, TableLike},
    utils::{
        fingerprint_type_token::canonical_type_token, identifier_resolution::normalize_identifier,
    },
};

/// Current fingerprint encoding version.
const FINGERPRINT_VERSION: u8 = 1;

/// Magic bytes identifying a schema fingerprint payload.
const MAGIC: &[u8; 4] = b"SFP1";

/// A deterministic SHA-256 fingerprint of a table's schema.
///
/// The fingerprint encodes the table's schema name, table name, columns
/// (ordinal, name, canonical type token, nullability), and primary-key
/// ordinals using a versioned binary format. The resulting digest is stable
/// across Rust toolchain versions and can be safely persisted.
///
/// # Examples
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sql_traits::prelude::*;
///
/// let db =
///     ParserDB::parse::<GenericDialect>("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")?;
/// let table = db.table(None, "users").unwrap();
/// let fp = table.schema_fingerprint(&db);
///
/// assert_eq!(fp.to_hex().len(), 64);
/// assert_eq!(fp.version(), 1);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy)]
pub struct SchemaFingerprint {
    version: u8,
    digest: [u8; 32],
}

impl SchemaFingerprint {
    /// Creates a new fingerprint from a version and digest.
    #[must_use]
    pub(crate) fn new(version: u8, digest: [u8; 32]) -> Self {
        Self { version, digest }
    }

    /// Returns the full 256-bit digest.
    #[must_use]
    pub fn fingerprint256(&self) -> [u8; 32] {
        self.digest
    }

    /// Returns the first 128 bits of the digest.
    #[must_use]
    pub fn fingerprint128(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.digest[..16]);
        buf
    }

    /// Returns the first 64 bits of the digest as a big-endian `u64`.
    #[must_use]
    pub fn fingerprint64(&self) -> u64 {
        u64::from_be_bytes(self.digest[..8].try_into().expect("slice is 8 bytes"))
    }

    /// Returns the encoding version of this fingerprint.
    #[must_use]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Returns the digest as a lowercase hex string (64 characters).
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in &self.digest {
            use fmt::Write;
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }

    /// Returns whether two fingerprints are comparable (same version).
    ///
    /// Fingerprints with different versions were produced by different
    /// encoding schemes and should not be compared for equality.
    #[must_use]
    pub fn is_comparable_to(&self, other: &Self) -> bool {
        self.version == other.version
    }
}

impl std::hash::Hash for SchemaFingerprint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.version.hash(state);
        self.digest.hash(state);
    }
}

impl PartialEq for SchemaFingerprint {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.digest == other.digest
    }
}

impl Eq for SchemaFingerprint {}

impl fmt::Debug for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaFingerprint")
            .field("version", &self.version)
            .field("digest", &self.to_hex())
            .finish()
    }
}

impl fmt::Display for SchemaFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}:{}", self.version, self.to_hex())
    }
}

/// Writes a length-prefixed UTF-8 string into the buffer.
fn write_str(buf: &mut Vec<u8>, s: &str) {
    let len = u32::try_from(s.len()).expect("string length fits in u32");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

/// Computes a v1 schema fingerprint for the given table.
pub(crate) fn compute_persistence_v1<T: TableLike>(
    table: &T,
    database: &T::DB,
) -> SchemaFingerprint {
    let mut buf = Vec::with_capacity(256);

    // Magic + version
    buf.extend_from_slice(MAGIC);
    buf.push(FINGERPRINT_VERSION);

    // Schema name (empty string if None)
    let schema_name = match table.table_schema() {
        Some(s) => normalize_identifier(s, table.table_schema_is_quoted()).into_owned(),
        None => String::new(),
    };
    write_str(&mut buf, &schema_name);

    // Table name
    let table_name = normalize_identifier(table.table_name(), table.table_name_is_quoted());
    write_str(&mut buf, &table_name);

    // Columns
    let columns: Vec<_> = table.columns(database).collect();
    let col_count = u32::try_from(columns.len()).expect("column count fits in u32");
    buf.extend_from_slice(&col_count.to_be_bytes());

    for (ordinal, column) in columns.iter().enumerate() {
        let ordinal_u32 = u32::try_from(ordinal).expect("ordinal fits in u32");
        buf.extend_from_slice(&ordinal_u32.to_be_bytes());

        let col_name = normalize_identifier(column.column_name(), column.column_name_is_quoted());
        write_str(&mut buf, &col_name);

        let type_token = canonical_type_token(column.data_type(database));
        write_str(&mut buf, &type_token);

        buf.push(u8::from(column.is_nullable(database)));
    }

    // Primary key ordinals
    let pk_ordinals: Vec<u32> = table
        .primary_key_columns(database)
        .filter_map(|col| col.column_id(database))
        .map(|id| u32::try_from(id).expect("pk ordinal fits in u32"))
        .collect();

    let pk_count = u32::try_from(pk_ordinals.len()).expect("pk count fits in u32");
    buf.extend_from_slice(&pk_count.to_be_bytes());
    for ord in &pk_ordinals {
        buf.extend_from_slice(&ord.to_be_bytes());
    }

    // SHA-256
    let hash = Sha256::digest(&buf);
    let digest: [u8; 32] = hash.into();

    SchemaFingerprint::new(FINGERPRINT_VERSION, digest)
}
