//! Submodule implementing the [`TableLike`] trait for `sqlparser`'s
//! [`CreateTable`] struct.

use ::sqlparser::ast::{CreateTable, Ident};
use sql_docs::docs::TableDoc;

use crate::{
    structs::{TableMetadata, generic_db::ParserDBInner},
    traits::{DatabaseLike, DocumentationMetadata, Metadata, TableLike},
    utils::last_str,
};

impl Metadata for CreateTable {
    type Meta = TableMetadata<CreateTable>;
}

impl DocumentationMetadata for CreateTable {
    type Documentation = TableDoc;
}

impl TableLike for CreateTable {
    type DB = ParserDBInner;

    #[inline]
    fn table_name(&self) -> &str {
        last_str(&self.name)
    }

    #[inline]
    fn table_doc<'db>(&'db self, database: &'db Self::DB) -> Option<&'db str>
    where
        Self: 'db,
    {
        database
            .table_metadata(self)
            .expect("Table must exist in database")
            .table_doc()
            .and_then(|d| d.doc())
    }

    #[inline]
    fn table_schema(&self) -> Option<&str> {
        let object_name_parts = &self.name.0;
        if object_name_parts.len() > 1 {
            let schema_part = &object_name_parts[0];
            match schema_part {
                sqlparser::ast::ObjectNamePart::Identifier(Ident { value, .. }) => {
                    Some(value.as_str())
                }
                sqlparser::ast::ObjectNamePart::Function(_) => {
                    panic!("Unexpected object name part in CreateTable: {schema_part:?}")
                }
            }
        } else {
            None
        }
    }

    fn columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").columns()
    }

    fn primary_key_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").primary_key_columns()
    }

    fn unique_indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::UniqueIndex>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").unique_indices()
    }

    fn indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Index>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").indices()
    }

    fn check_constraints<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::CheckConstraint>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").check_constraints()
    }

    fn foreign_keys<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::ForeignKey>
    where
        Self: 'db,
    {
        database.table_metadata(self).expect("Table must exist in database").foreign_keys()
    }

    #[inline]
    fn has_row_level_security(&self, database: &Self::DB) -> bool {
        database.table_metadata(self).expect("Table must exist in database").rls_enabled()
    }
}
