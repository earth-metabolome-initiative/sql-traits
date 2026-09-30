use alloc::{borrow::Cow, string::String};

use super::postgres_18_collations::POSTGRES_18_COLLATIONS;
use crate::utils::identifier_resolution::identifiers_match;

/// The schema holding every built-in PostgreSQL catalog fact.
const PG_CATALOG: &str = "pg_catalog";

/// The types PostgreSQL 18 marks collatable in `pg_catalog`.
static POSTGRES_18_COLLATABLE_TYPES: &[PostgresCatalogType] = &[
    PostgresCatalogType::built_in("bpchar"),
    PostgresCatalogType::built_in("name"),
    PostgresCatalogType::built_in("text"),
    PostgresCatalogType::built_in("varchar"),
    PostgresCatalogType::built_in("_bpchar"),
    PostgresCatalogType::built_in("_name"),
    PostgresCatalogType::built_in("_text"),
    PostgresCatalogType::built_in("_varchar"),
];

/// PostgreSQL catalog facts used while validating DDL.
///
/// The built-in facts are borrowed from a static table, so building the
/// PostgreSQL 18 catalog allocates nothing. Adding a fact copies the table into
/// an owned list once.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalog {
    collations: Cow<'static, [PostgresCatalogCollation]>,
    collatable_types: Cow<'static, [PostgresCatalogType]>,
}

/// A PostgreSQL collation identity and its deterministic flag.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalogCollation {
    schema: Option<Cow<'static, str>>,
    schema_is_quoted: bool,
    name: Cow<'static, str>,
    name_is_quoted: bool,
    deterministic: bool,
}

/// A PostgreSQL type whose values can carry a collation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PostgresCatalogType {
    schema: Option<Cow<'static, str>>,
    schema_is_quoted: bool,
    name: Cow<'static, str>,
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
        Self { collations: Cow::Borrowed(&[]), collatable_types: Cow::Borrowed(&[]) }
    }

    /// Creates the built-in PostgreSQL 18 catalog facts.
    #[must_use]
    pub const fn postgres_18() -> Self {
        Self {
            collations: Cow::Borrowed(POSTGRES_18_COLLATIONS),
            collatable_types: Cow::Borrowed(POSTGRES_18_COLLATABLE_TYPES),
        }
    }

    /// Adds or replaces a collation fact.
    #[must_use]
    pub fn with_collation(mut self, collation: PostgresCatalogCollation) -> Self {
        let collations = self.collations.to_mut();
        collations.retain(|held| !held.same_identity(&collation));
        collations.push(collation);
        self
    }

    /// Adds or replaces a collatable type fact.
    #[must_use]
    pub fn with_collatable_type(mut self, ty: PostgresCatalogType) -> Self {
        let collatable_types = self.collatable_types.to_mut();
        collatable_types.retain(|held| !held.same_identity(&ty));
        collatable_types.push(ty);
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
        let in_renamed_schema = |collation: &PostgresCatalogCollation| {
            collation.schema.as_ref().is_some_and(|schema| {
                identifiers_match(schema, collation.schema_is_quoted, from, from_quoted)
            })
        };
        if !self.collations.iter().any(in_renamed_schema) {
            return;
        }
        for collation in self.collations.to_mut() {
            if in_renamed_schema(collation) {
                collation.schema = Some(Cow::Owned(String::from(to)));
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
            schema: Some(Cow::Borrowed(PG_CATALOG)),
            schema_is_quoted: false,
            name: Cow::Owned(name.into()),
            name_is_quoted,
            deterministic: true,
        }
    }

    /// Creates a deterministic built-in collation in `pg_catalog`.
    pub(super) const fn built_in(name: &'static str, name_is_quoted: bool) -> Self {
        Self {
            schema: Some(Cow::Borrowed(PG_CATALOG)),
            schema_is_quoted: false,
            name: Cow::Borrowed(name),
            name_is_quoted,
            deterministic: true,
        }
    }

    /// Stores the schema that owns this collation.
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>, schema_is_quoted: bool) -> Self {
        self.schema = Some(Cow::Owned(schema.into()));
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
            schema: Some(Cow::Borrowed(PG_CATALOG)),
            schema_is_quoted: false,
            name: Cow::Owned(name.into()),
            name_is_quoted,
        }
    }

    /// Creates a built-in collatable type in `pg_catalog`, whose name is
    /// unquoted.
    const fn built_in(name: &'static str) -> Self {
        Self {
            schema: Some(Cow::Borrowed(PG_CATALOG)),
            schema_is_quoted: false,
            name: Cow::Borrowed(name),
            name_is_quoted: false,
        }
    }

    /// Stores the schema that owns this type.
    #[must_use]
    pub fn with_schema(mut self, schema: impl Into<String>, schema_is_quoted: bool) -> Self {
        self.schema = Some(Cow::Owned(schema.into()));
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

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;

    use super::PostgresCatalog;

    /// `with_collation` replaces by identity, so the built-in table it starts
    /// from must already hold each identity once.
    #[test]
    fn built_in_facts_hold_each_identity_once() {
        let catalog = PostgresCatalog::postgres_18();
        let mut collations: Vec<_> = catalog
            .collations()
            .map(|c| (c.schema(), c.schema_is_quoted(), c.name(), c.name_is_quoted()))
            .collect();
        let held = collations.len();
        collations.sort_unstable();
        collations.dedup();
        assert_eq!(collations.len(), held);

        let mut types: Vec<_> = catalog
            .collatable_types()
            .map(|t| (t.schema(), t.schema_is_quoted(), t.name(), t.name_is_quoted()))
            .collect();
        let held = types.len();
        types.sort_unstable();
        types.dedup();
        assert_eq!(types.len(), held);
    }
}
