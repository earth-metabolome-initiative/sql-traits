//! Implement the `IndexLike` trait for `sqlparser`'s `CreateIndex`.

use alloc::string::ToString;

use sqlparser::ast::{CreateIndex, CreateTable, Expr};

use crate::{
    errors::{LookupError, ObjectKind},
    structs::{ParserDB, TableAttribute, metadata::IndexMetadata},
    traits::{DatabaseLike, IndexLike, Metadata, TableLike},
    utils::object_name::{Qualifier, object_name_last_part, qualifier_of},
};

impl Metadata for TableAttribute<CreateTable, CreateIndex> {
    type Meta = IndexMetadata<Self>;
}

impl TableAttribute<CreateTable, CreateIndex> {
    /// What the index name says about its schema, if the index is named at
    /// all.
    fn index_qualifier(&self) -> Qualifier<'_> {
        self.attribute().name.as_ref().map_or(Qualifier::Absent, qualifier_of)
    }
}

impl IndexLike for TableAttribute<CreateTable, CreateIndex> {
    type DB = ParserDB;

    #[inline]
    fn table<'db>(&'db self, _database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        self.table()
    }

    #[inline]
    fn name(&self) -> Option<&str> {
        self.attribute().name.as_ref().and_then(object_name_last_part).map(|(name, _)| name)
    }

    #[inline]
    fn name_is_quoted(&self) -> bool {
        self.attribute()
            .name
            .as_ref()
            .and_then(object_name_last_part)
            .is_some_and(|(_, quoted)| quoted)
    }

    #[inline]
    fn schema(&self) -> Option<&str> {
        self.index_qualifier().named().map(|(schema, _)| schema)
    }

    #[inline]
    fn schema_is_quoted(&self) -> bool {
        self.index_qualifier().named().is_some_and(|(_, quoted)| quoted)
    }

    #[inline]
    fn expression<'db>(&'db self, database: &'db Self::DB) -> Result<&'db Expr, LookupError>
    where
        Self: 'db,
    {
        Ok(database
            .index_metadata(self)
            .ok_or_else(|| {
                match self.attribute().name.as_ref() {
                    Some(name) => ObjectKind::Index.not_in_database(&name.to_string()),
                    None => ObjectKind::Index.anonymous_not_in_database(self.table().table_name()),
                }
            })?
            .expression())
    }
}
