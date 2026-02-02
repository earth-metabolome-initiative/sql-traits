//! Implement the [`UniqueConstraint`] trait for the `sqlparser` crate's

use sqlparser::ast::{CreateTable, Expr, UniqueConstraint};

use crate::{
    structs::{TableAttribute, generic_db::ParserDBInner, metadata::UniqueIndexMetadata},
    traits::{DatabaseLike, IndexLike, Metadata},
};

impl Metadata for TableAttribute<CreateTable, UniqueConstraint> {
    type Meta = UniqueIndexMetadata<Self>;
}

impl IndexLike for TableAttribute<CreateTable, UniqueConstraint> {
    type DB = ParserDBInner;

    #[inline]
    fn table<'db>(&'db self, _database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        self.table()
    }

    #[inline]
    fn expression<'db>(&'db self, database: &'db Self::DB) -> &'db Expr
    where
        Self: 'db,
    {
        database
            .unique_index_metadata(self)
            .expect("Unique index must exist in database")
            .expression()
    }
}
