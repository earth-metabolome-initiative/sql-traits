//! Implement the [`UniqueConstraint`] trait for the `sqlparser` crate's

use sqlparser::ast::{CreateTable, Expr, UniqueConstraint};

use crate::{
    structs::{ParserDB, TableAttribute, metadata::UniqueIndexMetadata},
    traits::{DatabaseLike, IndexLike, Metadata},
};

impl Metadata for TableAttribute<CreateTable, UniqueConstraint> {
    type Meta = UniqueIndexMetadata<Self>;
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
    /// [`TableLike::unique_indices`](crate::traits::TableLike::unique_indices),
    /// while [`DatabaseLike::indexes`](crate::traits::DatabaseLike::indexes)
    /// only yields `CREATE INDEX` indexes.
    #[inline]
    fn name(&self) -> Option<&sqlparser::ast::ObjectName> {
        None
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
