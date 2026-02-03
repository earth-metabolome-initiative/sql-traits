//! Implementation of the `GrantLike` trait for sqlparser's `Grant` struct.

use sqlparser::ast::{
    Action, Grant, GrantObjects, Grantee, Ident, ObjectName, ObjectNamePart, Privileges,
};

use crate::{
    structs::ParserDB,
    traits::{ColumnLike, DatabaseLike, GrantLike, Metadata, TableLike},
    utils::last_str,
};

/// Extracts the schema name from an ObjectName if it has at least 2 parts.
///
/// For a name like `schema.table`, returns `Some("schema")`.
/// For a name like `table`, returns `None`.
fn schema_from_object_name(obj: &ObjectName) -> Option<&str> {
    if obj.0.len() > 1 {
        match &obj.0[obj.0.len() - 2] {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            ObjectNamePart::Function(f) => Some(f.name.value.as_str()),
        }
    } else {
        None
    }
}

impl Metadata for Grant {
    type Meta = ();
}

impl GrantLike for Grant {
    type DB = ParserDB;

    fn privileges(&self) -> impl Iterator<Item = &Action> {
        match &self.privileges {
            Privileges::All { .. } => {
                // Return an empty iterator for ALL privileges
                // Users should check is_all_privileges() separately
                [].iter()
            }
            Privileges::Actions(actions) => actions.iter(),
        }
    }

    fn is_all_privileges(&self) -> bool {
        matches!(&self.privileges, Privileges::All { .. })
    }

    fn tables<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Table> {
        let direct_tables: Box<dyn Iterator<Item = &<Self::DB as DatabaseLike>::Table> + 'a> =
            match &self.objects {
                Some(GrantObjects::Tables(tables)) => {
                    Box::new(tables.iter().filter_map(|t| {
                        let table_name = last_str(t);
                        let schema = schema_from_object_name(t);
                        database.table(schema, table_name)
                    }))
                }
                Some(GrantObjects::AllTablesInSchema { schemas }) => {
                    // For ALL TABLES IN SCHEMA, return all tables matching the schema
                    Box::new(database.tables().filter(move |table| {
                        if let Some(table_schema) = table.table_schema() {
                            schemas.iter().any(|s| last_str(s) == table_schema)
                        } else {
                            false
                        }
                    }))
                }
                _ => Box::new(std::iter::empty()),
            };
        direct_tables
    }

    fn schemas(&self) -> Option<impl Iterator<Item = &ObjectName>> {
        match &self.objects {
            Some(GrantObjects::Schemas(schemas)) => Some(schemas.iter()),
            _ => None,
        }
    }

    fn functions<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Function> {
        // Note: sqlparser represents functions differently, there's no direct
        // GrantObjects::Functions For ALL FUNCTIONS IN SCHEMA, we return
        // functions that match the schema
        let funcs: Box<dyn Iterator<Item = &<Self::DB as DatabaseLike>::Function> + 'a> =
            match &self.objects {
                Some(GrantObjects::AllFunctionsInSchema { schemas }) => {
                    // Return all functions - functions don't have schema info in our current model
                    // so we return all of them when ALL FUNCTIONS IN SCHEMA is used
                    let schema_names: Vec<&str> = schemas.iter().map(last_str).collect();
                    Box::new(database.functions().filter(move |_func| {
                        // Since functions don't have schema info, we can't filter by schema
                        // Return all functions when schema matches "public" or any common schema
                        // This is a limitation - a future version could add schema to FunctionLike
                        !schema_names.is_empty()
                    }))
                }
                _ => Box::new(std::iter::empty()),
            };
        funcs
    }

    fn grantees(&self) -> impl Iterator<Item = &Grantee> {
        self.grantees.iter()
    }

    fn with_grant_option(&self) -> bool {
        self.with_grant_option
    }

    fn granted_by<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> Option<&'a <Self::DB as DatabaseLike>::Role> {
        self.granted_by.as_ref().and_then(|ident| database.role(&ident.value))
    }

    fn privilege_columns<'a>(
        &'a self,
        table: &'a <Self::DB as DatabaseLike>::Table,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Column> {
        let column_idents: Vec<&Ident> = match &self.privileges {
            Privileges::All { .. } => Vec::new(),
            Privileges::Actions(actions) => {
                actions
                    .iter()
                    .flat_map(|action| {
                        match action {
                            Action::Select { columns } => {
                                columns.as_ref().map(|c| c.iter()).into_iter().flatten().collect()
                            }
                            Action::Insert { columns } => {
                                columns.as_ref().map(|c| c.iter()).into_iter().flatten().collect()
                            }
                            Action::Update { columns } => {
                                columns.as_ref().map(|c| c.iter()).into_iter().flatten().collect()
                            }
                            Action::References { columns } => {
                                columns.as_ref().map(|c| c.iter()).into_iter().flatten().collect()
                            }
                            _ => Vec::new(),
                        }
                    })
                    .collect()
            }
        };

        table
            .columns(database)
            .filter(move |col| column_idents.iter().any(|ident| ident.value == col.column_name()))
    }

    fn applies_to_table(
        &self,
        table: &<Self::DB as DatabaseLike>::Table,
        _database: &Self::DB,
    ) -> bool {
        match &self.objects {
            Some(GrantObjects::Tables(tables)) => {
                tables.iter().any(|t| {
                    let grant_table_name = last_str(t);
                    grant_table_name == table.table_name()
                })
            }
            Some(GrantObjects::AllTablesInSchema { schemas }) => {
                // Check if the table's schema matches any of the schemas
                if let Some(table_schema) = table.table_schema() {
                    schemas.iter().any(|s| last_str(s) == table_schema)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn applies_to_grantee(&self, grantee_name: &str) -> bool {
        self.grantees.iter().any(|g| {
            match &g.name {
                Some(sqlparser::ast::GranteeName::ObjectName(name)) => {
                    last_str(name) == grantee_name
                }
                _ => {
                    // Handle PUBLIC and other special cases
                    format!("{g}").to_uppercase() == grantee_name.to_uppercase()
                }
            }
        })
    }
}

/// Checks if this grant matches a revoke statement.
///
/// A grant matches a revoke if:
/// - The privileges overlap (or revoke is ALL)
/// - The objects match
/// - The grantees overlap
#[must_use]
pub fn grant_matches_revoke(grant: &Grant, revoke: &sqlparser::ast::Revoke) -> bool {
    // Check if objects match
    if grant.objects != revoke.objects {
        return false;
    }

    // Check if any grantees overlap
    let grantees_match = grant
        .grantees
        .iter()
        .any(|g| revoke.grantees.iter().any(|rg| format!("{g}") == format!("{rg}")));

    if !grantees_match {
        return false;
    }

    // Check if privileges overlap
    match (&revoke.privileges, &grant.privileges) {
        (Privileges::All { .. }, _) | (_, Privileges::All { .. }) => true,
        (Privileges::Actions(revoke_actions), Privileges::Actions(grant_actions)) => {
            revoke_actions
                .iter()
                .any(|ra| grant_actions.iter().any(|ga| format!("{ra}") == format!("{ga}")))
        }
    }
}
