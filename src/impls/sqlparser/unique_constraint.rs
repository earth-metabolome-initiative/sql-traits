//! Implement the [`UniqueConstraint`] trait for the `sqlparser` crate's
//! [`TableAttribute`] wrapper.

use sqlparser::ast::{CreateTable, Expr, UniqueConstraint};

use crate::{
    errors::{LookupError, ObjectKind},
    structs::{ParserDB, TableAttribute, metadata::UniqueIndexMetadata},
    traits::{DatabaseLike, IndexLike, Metadata, TableLike},
};

impl Metadata for TableAttribute<CreateTable, UniqueConstraint> {
    type Meta = UniqueIndexMetadata<Self>;
}

/// Returns the name a unique constraint was declared with.
///
/// PostgreSQL spells it as a constraint name (`CONSTRAINT uq UNIQUE (..)`)
/// while MySQL spells it as an index name (`UNIQUE KEY uq (..)`), and sqlparser
/// keeps the two in separate fields.
fn unique_constraint_name(constraint: &UniqueConstraint) -> Option<&sqlparser::ast::Ident> {
    constraint.name.as_ref().or(constraint.index_name.as_ref())
}

impl IndexLike for TableAttribute<CreateTable, UniqueConstraint> {
    type DB = ParserDB;

    #[inline]
    fn table<'db>(&'db self, _database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        self.table()
    }

    /// A unique constraint stores its optional index name as an
    /// [`Ident`](sqlparser::ast::Ident) (`UniqueConstraint::index_name`), not
    /// an [`ObjectName`](sqlparser::ast::ObjectName), so it is not exposed
    /// through this accessor. Unique indexes are enumerated via
    /// [`TableLike::unique_indices`],
    /// while [`DatabaseLike::indexes`] only yields `CREATE INDEX` indexes.
    #[inline]
    fn name(&self) -> Option<&sqlparser::ast::ObjectName> {
        None
    }

    #[inline]
    fn expression<'db>(&'db self, database: &'db Self::DB) -> Result<&'db Expr, LookupError>
    where
        Self: 'db,
    {
        Ok(database
            .unique_index_metadata(self)
            .ok_or_else(|| {
                match unique_constraint_name(self.attribute()) {
                    Some(name) => ObjectKind::UniqueIndex.not_in_database(&name.value),
                    None => {
                        ObjectKind::UniqueIndex.anonymous_not_in_database(self.table().table_name())
                    }
                }
            })?
            .expression())
    }
}
