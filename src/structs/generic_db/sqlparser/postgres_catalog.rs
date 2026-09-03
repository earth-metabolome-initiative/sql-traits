use alloc::{string::String, vec::Vec};

use crate::utils::identifier_resolution::identifiers_match;

/// PostgreSQL catalog facts used while validating DDL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalog {
    collations: Vec<PostgresCatalogCollation>,
    collatable_types: Vec<PostgresCatalogType>,
}

/// A PostgreSQL collation identity and its deterministic flag.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalogCollation {
    schema: Option<String>,
    schema_is_quoted: bool,
    name: String,
    name_is_quoted: bool,
    deterministic: bool,
}

/// A PostgreSQL type whose values can carry a collation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalogType {
    schema: Option<String>,
    schema_is_quoted: bool,
    name: String,
    name_is_quoted: bool,
}

impl Default for PostgresCatalog {
    fn default() -> Self {
        Self::postgres_18()
    }
}

impl PostgresCatalog {
    /// Creates a catalog with no configured facts.
    #[must_use]
    pub const fn empty() -> Self {
        Self { collations: Vec::new(), collatable_types: Vec::new() }
    }

    /// Creates the built-in PostgreSQL 18 catalog facts.
    #[must_use]
    pub fn postgres_18() -> Self {
        let mut catalog = Self::empty();
        for collation in [
            PostgresCatalogCollation::new("default", false),
            PostgresCatalogCollation::new("C", true),
            PostgresCatalogCollation::new("C.utf8", true),
            PostgresCatalogCollation::new("POSIX", true),
            PostgresCatalogCollation::new("ucs_basic", false),
            PostgresCatalogCollation::new("unicode", false),
            PostgresCatalogCollation::new("pg_c_utf8", false),
            PostgresCatalogCollation::new("pg_unicode_fast", false),
        ] {
            catalog = catalog.with_collation(collation);
        }
        for name in super::postgres_icu_collations::iter() {
            catalog = catalog.with_collation(PostgresCatalogCollation::new(name, true));
        }
        for ty in ["bpchar", "name", "text", "varchar", "_bpchar", "_name", "_text", "_varchar"] {
            catalog = catalog.with_collatable_type(PostgresCatalogType::new(ty, false));
        }
        catalog
    }

    /// Adds or replaces a collation fact.
    #[must_use]
    pub fn with_collation(mut self, collation: PostgresCatalogCollation) -> Self {
        self.collations.retain(|held| !held.same_identity(&collation));
        self.collations.push(collation);
        self
    }

    /// Adds or replaces a collatable type fact.
    #[must_use]
    pub fn with_collatable_type(mut self, ty: PostgresCatalogType) -> Self {
        self.collatable_types.retain(|held| !held.same_identity(&ty));
        self.collatable_types.push(ty);
        self
    }

    /// Returns the collation facts in insertion order.
    // Clippy versions disagree on whether an opaque iterator return needs an
    // explicit `must_use`, so carry the attribute and silence the redundancy.
    #[allow(clippy::double_must_use)]
    #[must_use]
    pub fn collations(&self) -> impl DoubleEndedIterator<Item = &PostgresCatalogCollation> {
        self.collations.iter()
    }

    /// Returns the collatable type facts in insertion order.
    pub fn collatable_types(&self) -> impl Iterator<Item = &PostgresCatalogType> {
        self.collatable_types.iter()
    }

    pub(crate) fn rename_schema(
        &mut self,
        from: &str,
        from_quoted: bool,
        to: &str,
        to_quoted: bool,
    ) {
        for collation in &mut self.collations {
            if collation.schema.as_ref().is_some_and(|schema| {
                identifiers_match(schema, collation.schema_is_quoted, from, from_quoted)
            }) {
                collation.schema = Some(String::from(to));
                collation.schema_is_quoted = to_quoted;
            }
        }
    }
}

impl PostgresCatalogCollation {
    /// Creates a collation in `pg_catalog`.
    #[must_use]
    pub fn new(name: impl Into<String>, name_is_quoted: bool) -> Self {
        Self {
            schema: Some(String::from("pg_catalog")),
            schema_is_quoted: false,
            name: name.into(),
            name_is_quoted,
            deterministic: true,
        }
    }

    /// Stores the schema that owns this collation.
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>, schema_is_quoted: bool) -> Self {
        self.schema = Some(schema.into());
        self.schema_is_quoted = schema_is_quoted;
        self
    }

    /// Stores whether this collation is deterministic.
    #[must_use]
    pub const fn with_deterministic(mut self, deterministic: bool) -> Self {
        self.deterministic = deterministic;
        self
    }

    /// Returns the owning schema.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns whether the schema is quoted.
    #[must_use]
    pub const fn schema_is_quoted(&self) -> bool {
        self.schema_is_quoted
    }

    /// Returns the collation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the collation name is quoted.
    #[must_use]
    pub const fn name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    /// Returns whether the collation is deterministic.
    #[must_use]
    pub const fn deterministic(&self) -> bool {
        self.deterministic
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.schema_is_quoted == other.schema_is_quoted
            && self.name == other.name
            && self.name_is_quoted == other.name_is_quoted
    }
}

impl PostgresCatalogType {
    /// Creates a collatable type in `pg_catalog`.
    #[must_use]
    pub fn new(name: impl Into<String>, name_is_quoted: bool) -> Self {
        Self {
            schema: Some(String::from("pg_catalog")),
            schema_is_quoted: false,
            name: name.into(),
            name_is_quoted,
        }
    }

    /// Stores the schema that owns this type.
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>, schema_is_quoted: bool) -> Self {
        self.schema = Some(schema.into());
        self.schema_is_quoted = schema_is_quoted;
        self
    }

    /// Returns the owning schema.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns whether the schema is quoted.
    #[must_use]
    pub const fn schema_is_quoted(&self) -> bool {
        self.schema_is_quoted
    }

    /// Returns the type name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns whether the type name is quoted.
    #[must_use]
    pub const fn name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.schema_is_quoted == other.schema_is_quoted
            && self.name == other.name
            && self.name_is_quoted == other.name_is_quoted
    }
}
