//! Implement the `IndexLike` trait for `sqlparser`'s `CreateIndex`.

use sqlparser::ast::{CreateIndex, CreateTable, Expr};

use crate::{
    structs::{TableAttribute, generic_db::ParserDB, metadata::IndexMetadata},
    traits::{DatabaseLike, IndexLike, Metadata},
};

impl Metadata for TableAttribute<CreateTable, CreateIndex> {
    type Meta = IndexMetadata<Self>;
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
    fn expression<'db>(&'db self, database: &'db Self::DB) -> &'db Expr
    where
        Self: 'db,
    {
        database.index_metadata(self).expect("Index must exist in database").expression()
    }
}
