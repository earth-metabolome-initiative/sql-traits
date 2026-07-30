//! Submodule implementing the [`TableLike`] trait for `sqlparser`'s
//! [`CreateTable`] struct.

use alloc::string::ToString;

use ::sqlparser::ast::{CreateTable, Ident, ObjectNamePart};
use sql_docs::docs::TableDoc;

use crate::{
    errors::{LookupError, ObjectKind},
    structs::{ParserDB, TableMetadata},
    traits::{DatabaseLike, DocumentationMetadata, Metadata, TableLike},
    utils::last_str,
};

impl Metadata for CreateTable {
    type Meta = TableMetadata<CreateTable>;
}

impl DocumentationMetadata for CreateTable {
    type Documentation = TableDoc;
}

/// Resolves the metadata `database` holds for `table`.
///
/// A [`CreateTable`] node and a [`ParserDB`] are independent values, so a node
/// the database does not hold (renamed away, dropped, or parsed from different
/// input) has no metadata to report.
fn table_metadata<'db>(
    table: &CreateTable,
    database: &'db ParserDB,
) -> Result<&'db TableMetadata<CreateTable>, LookupError> {
    database
        .table_metadata(table)
        .ok_or_else(|| ObjectKind::Table.not_in_database(&table.name.to_string()))
}

impl TableLike for CreateTable {
    type DB = ParserDB;

    #[inline]
    fn table_name(&self) -> &str {
        last_str(&self.name)
    }

    #[inline]
    fn table_name_is_quoted(&self) -> bool {
        self.name.0.last().is_some_and(
            |part| matches!(part, ObjectNamePart::Identifier(ident) if ident.quote_style.is_some()),
        )
    }

    #[inline]
    fn table_doc<'db>(&'db self, database: &'db Self::DB) -> Result<Option<&'db str>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.table_doc().and_then(|d| d.doc()))
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
                sqlparser::ast::ObjectNamePart::Function(function) => {
                    Some(function.name.value.as_str())
                }
            }
        } else {
            None
        }
    }

    #[inline]
    fn table_schema_is_quoted(&self) -> bool {
        if self.name.0.len() <= 1 {
            return false;
        }
        self.name.0.first().is_some_and(
            |part| matches!(part, ObjectNamePart::Identifier(ident) if ident.quote_style.is_some()),
        )
    }

    fn columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.columns())
    }

    fn primary_key_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.primary_key_columns())
    }

    fn unique_indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::UniqueIndex>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.unique_indices())
    }

    fn indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Index>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.indices())
    }

    fn check_constraints<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::CheckConstraint>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.check_constraints())
    }

    fn foreign_keys<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::ForeignKey>, LookupError>
    where
        Self: 'db,
    {
        Ok(table_metadata(self, database)?.foreign_keys())
    }

    #[inline]
    fn has_row_level_security(&self, database: &Self::DB) -> Result<bool, LookupError> {
        Ok(table_metadata(self, database)?.rls_enabled())
    }

    #[inline]
    fn has_forced_row_level_security(&self, database: &Self::DB) -> Result<bool, LookupError> {
        Ok(table_metadata(self, database)?.rls_forced())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{
        Ident, ObjectName, ObjectNamePart, ObjectNamePartFunction,
        helpers::stmt_create_table::CreateTableBuilder,
    };

    use super::*;

    #[test]
    fn table_schema_reads_a_function_name_part() {
        // sqlparser never emits a `Function` part in a table name from a
        // `CREATE TABLE` parse, but the accessor must stay total rather than
        // panic if one is ever constructed. The schema part falls back to the
        // function's name, mirroring `last_str`.
        let name = ObjectName(vec![
            ObjectNamePart::Function(ObjectNamePartFunction {
                name: Ident::new("schema_fn"),
                args: alloc::vec::Vec::new(),
            }),
            ObjectNamePart::Identifier(Ident::new("t")),
        ]);
        let create_table = CreateTableBuilder::new(name).build();
        assert_eq!(create_table.table_schema(), Some("schema_fn"));
        assert_eq!(create_table.table_name(), "t");
    }
}
