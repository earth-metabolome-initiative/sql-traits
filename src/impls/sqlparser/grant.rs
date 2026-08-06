//! Implementation of grant traits for sqlparser's `Grant` struct.
//!
//! In sqlparser, both table-level and column-level grants are represented
//! by the same `Grant` struct. This module implements all grant traits
//! on `Grant` to support both use cases.

use alloc::{string::ToString, vec::Vec};
use core::mem;

use sqlparser::ast::{
    Action, CreateRole, CreateTable, Grant, GrantObjects, Grantee, GranteeName, GranteesType,
    Ident, ObjectName, ObjectNamePart, Privileges, Revoke,
};

use crate::{
    errors::LookupError,
    structs::{ParserDB, TargetName},
    traits::{
        ColumnGrantLike, ColumnLike, DatabaseLike, GrantLike, Metadata, RoleLike, TableGrantLike,
        TableLike,
    },
    utils::{
        identifier_resolution::{identifiers_match, is_public_pseudo_role},
        object_name::{
            object_name_identifiers, object_name_last_part, resolve_object_name,
            resolve_table_object_name_on_search_path_in_iter, table_matches_object_name,
            target_name_from_object_name,
        },
    },
};

fn object_names_match(left: &ObjectName, right: &ObjectName) -> bool {
    if left.0.len() != right.0.len() {
        return false;
    }

    left.0.iter().zip(right.0.iter()).all(|(left_part, right_part)| {
        match (left_part, right_part) {
            (ObjectNamePart::Identifier(left_ident), ObjectNamePart::Identifier(right_ident)) => {
                identifiers_match(
                    left_ident.value.as_str(),
                    left_ident.quote_style.is_some(),
                    right_ident.value.as_str(),
                    right_ident.quote_style.is_some(),
                )
            }
            (ObjectNamePart::Function(left_fn), ObjectNamePart::Function(right_fn)) => {
                identifiers_match(
                    left_fn.name.value.as_str(),
                    left_fn.name.quote_style.is_some(),
                    right_fn.name.value.as_str(),
                    right_fn.name.quote_style.is_some(),
                )
            }
            _ => false,
        }
    })
}

fn role_matches_ident(role: &CreateRole, lookup_name: &str, lookup_quoted: bool) -> bool {
    role.names.iter().any(|role_name| {
        object_name_last_part(role_name).is_some_and(|(role_name, role_quoted)| {
            identifiers_match(role_name, role_quoted, lookup_name, lookup_quoted)
        })
    })
}

/// Returns whether a grantee names the `PUBLIC` pseudo-role, in either
/// spelling the parser produces: the dedicated grantee type for the keyword,
/// and a bare unquoted `PUBLIC` identifier for dialects that reserve it.
fn grantee_is_public(grantee: &Grantee) -> bool {
    if grantee.grantee_type == GranteesType::Public {
        return true;
    }

    matches!(&grantee.name, Some(GranteeName::ObjectName(name))
        if object_name_last_part(name)
            .is_some_and(|(value, quoted)| is_public_pseudo_role(value, quoted)))
}

fn grantee_matches_role(grantee: &Grantee, role: &CreateRole) -> bool {
    if grantee_is_public(grantee) {
        return true;
    }

    if let Some(GranteeName::ObjectName(name)) = &grantee.name {
        role.names.iter().any(|role_name| object_names_match(name, role_name))
    } else {
        let role_name = role.name();
        format!("{grantee}").eq_ignore_ascii_case(role_name)
    }
}

fn schema_matches_table<T: TableLike>(schema_name: &ObjectName, table: &T) -> bool {
    let Some(table_schema) = table.table_schema() else {
        return false;
    };
    let Some((lookup_schema, lookup_schema_quoted)) = object_name_last_part(schema_name) else {
        return false;
    };

    identifiers_match(
        table_schema,
        table.table_schema_is_quoted(),
        lookup_schema,
        lookup_schema_quoted,
    )
}

fn grantees_match(left_grantee: &Grantee, right_grantee: &Grantee) -> bool {
    if left_grantee.grantee_type == GranteesType::Public
        || right_grantee.grantee_type == GranteesType::Public
    {
        return left_grantee.grantee_type == right_grantee.grantee_type;
    }

    match (&left_grantee.name, &right_grantee.name) {
        (Some(GranteeName::ObjectName(left_name)), Some(GranteeName::ObjectName(right_name))) => {
            object_names_match(left_name, right_name)
        }
        _ => format!("{left_grantee}").eq_ignore_ascii_case(&format!("{right_grantee}")),
    }
}

fn grantee_matches_any(grantee: &Grantee, candidates: &[Grantee]) -> bool {
    candidates.iter().any(|candidate| grantees_match(grantee, candidate))
}

fn grantees_overlap(left: &[Grantee], right: &[Grantee]) -> bool {
    left.iter().any(|left_grantee| grantee_matches_any(left_grantee, right))
}

pub(crate) fn partition_grantees_for_revoke(
    grant_grantees: &[Grantee],
    revoke_grantees: &[Grantee],
) -> (Vec<Grantee>, Vec<Grantee>) {
    grant_grantees
        .iter()
        .cloned()
        .partition(|grant_grantee| grantee_matches_any(grant_grantee, revoke_grantees))
}

fn object_name_lists_match(left: &[ObjectName], right: &[ObjectName]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left_name, right_name)| object_names_match(left_name, right_name))
}

fn grant_objects_inner_match(left: &GrantObjects, right: &GrantObjects) -> bool {
    match (left, right) {
        (
            GrantObjects::AllSequencesInSchema { schemas: left_schemas },
            GrantObjects::AllSequencesInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::AllTablesInSchema { schemas: left_schemas },
            GrantObjects::AllTablesInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::AllViewsInSchema { schemas: left_schemas },
            GrantObjects::AllViewsInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::AllMaterializedViewsInSchema { schemas: left_schemas },
            GrantObjects::AllMaterializedViewsInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::AllExternalTablesInSchema { schemas: left_schemas },
            GrantObjects::AllExternalTablesInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::AllFunctionsInSchema { schemas: left_schemas },
            GrantObjects::AllFunctionsInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::FutureTablesInSchema { schemas: left_schemas },
            GrantObjects::FutureTablesInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::FutureViewsInSchema { schemas: left_schemas },
            GrantObjects::FutureViewsInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::FutureExternalTablesInSchema { schemas: left_schemas },
            GrantObjects::FutureExternalTablesInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::FutureMaterializedViewsInSchema { schemas: left_schemas },
            GrantObjects::FutureMaterializedViewsInSchema { schemas: right_schemas },
        )
        | (
            GrantObjects::FutureSequencesInSchema { schemas: left_schemas },
            GrantObjects::FutureSequencesInSchema { schemas: right_schemas },
        ) => object_name_lists_match(left_schemas, right_schemas),
        (
            GrantObjects::FutureSchemasInDatabase { databases: left_databases },
            GrantObjects::FutureSchemasInDatabase { databases: right_databases },
        ) => object_name_lists_match(left_databases, right_databases),
        (GrantObjects::Databases(left_objects), GrantObjects::Databases(right_objects))
        | (GrantObjects::Schemas(left_objects), GrantObjects::Schemas(right_objects))
        | (GrantObjects::Sequences(left_objects), GrantObjects::Sequences(right_objects))
        | (GrantObjects::Tables(left_objects), GrantObjects::Tables(right_objects))
        | (GrantObjects::Views(left_objects), GrantObjects::Views(right_objects))
        | (GrantObjects::Warehouses(left_objects), GrantObjects::Warehouses(right_objects))
        | (GrantObjects::Integrations(left_objects), GrantObjects::Integrations(right_objects))
        | (
            GrantObjects::ResourceMonitors(left_objects),
            GrantObjects::ResourceMonitors(right_objects),
        )
        | (GrantObjects::Users(left_objects), GrantObjects::Users(right_objects))
        | (GrantObjects::ComputePools(left_objects), GrantObjects::ComputePools(right_objects))
        | (GrantObjects::Connections(left_objects), GrantObjects::Connections(right_objects))
        | (GrantObjects::FailoverGroup(left_objects), GrantObjects::FailoverGroup(right_objects))
        | (
            GrantObjects::ReplicationGroup(left_objects),
            GrantObjects::ReplicationGroup(right_objects),
        )
        | (
            GrantObjects::ExternalVolumes(left_objects),
            GrantObjects::ExternalVolumes(right_objects),
        ) => object_name_lists_match(left_objects, right_objects),
        (
            GrantObjects::Procedure { name: left_name, arg_types: left_arg_types },
            GrantObjects::Procedure { name: right_name, arg_types: right_arg_types },
        )
        | (
            GrantObjects::Function { name: left_name, arg_types: left_arg_types },
            GrantObjects::Function { name: right_name, arg_types: right_arg_types },
        ) => object_names_match(left_name, right_name) && left_arg_types == right_arg_types,
        (left_objects, right_objects) => left_objects == right_objects,
    }
}

fn grant_objects_match(left: Option<&GrantObjects>, right: Option<&GrantObjects>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left_objects), Some(right_objects)) => {
            grant_objects_inner_match(left_objects, right_objects)
        }
        _ => false,
    }
}

fn action_columns(action: &Action) -> Option<&[Ident]> {
    match action {
        Action::Select { columns }
        | Action::Insert { columns }
        | Action::Update { columns }
        | Action::References { columns } => columns.as_deref(),
        _ => None,
    }
}

/// Enforces that every column a grant or a revoke names exists on each table it
/// applies to.
///
/// The database requires the column on every table in the list, and names the
/// one that lacks it, so this walks them in order and reports the same way.
/// `ALL TABLES IN SCHEMA` carries no per-table column list to check and the
/// database accepts a column list beside it, so that form is left alone. A
/// one-part or two-part name resolves through `search_path`, and a shape the
/// strict splitter rejects keeps the lenient last-two-parts matching.
///
/// # Errors
///
/// Returns [`crate::errors::Error::ColumnNotFoundForGrant`] for the first
/// column a listed table does not have, and
/// [`crate::errors::Error::IdentifierLookupError`] when a resolvable name
/// matches more than one table.
pub(crate) fn validate_granted_columns(
    privileges: &Privileges,
    objects: Option<&GrantObjects>,
    database_tables: &[&CreateTable],
    search_path: &[(&str, bool)],
) -> Result<(), crate::errors::Error> {
    let Privileges::Actions(actions) = privileges else {
        return Ok(());
    };
    let Some(GrantObjects::Tables(names)) = objects else {
        return Ok(());
    };

    for name in names {
        let resolved = if object_name_identifiers(name).is_ok() {
            resolve_table_object_name_on_search_path_in_iter(
                database_tables.iter().copied(),
                name,
                search_path.iter().copied(),
            )
            .map_err(crate::errors::Error::IdentifierLookupError)?
        } else {
            database_tables.iter().copied().find(|table| table_matches_object_name(*table, name))
        };
        let Some(table) = resolved else {
            // The table did not resolve, which the open world excuses and the
            // closed world has already refused, so there is nothing to check
            // the columns against.
            continue;
        };

        for column in actions.iter().filter_map(action_columns).flatten() {
            let known = table.columns.iter().any(|declared| {
                identifiers_match(
                    declared.name.value.as_str(),
                    declared.name.quote_style.is_some(),
                    column.value.as_str(),
                    column.quote_style.is_some(),
                )
            });
            if !known {
                return Err(crate::errors::Error::ColumnNotFoundForGrant {
                    column_name: column.value.clone(),
                    table_name: table.name.to_string(),
                });
            }
        }
    }

    Ok(())
}

fn is_column_scoped_action(action: &Action) -> bool {
    matches!(
        action,
        Action::Select { .. }
            | Action::Insert { .. }
            | Action::Update { .. }
            | Action::References { .. }
    )
}

fn action_with_columns(action: &Action, columns: Option<Vec<Ident>>) -> Action {
    match action {
        Action::Select { .. } => Action::Select { columns },
        Action::Insert { .. } => Action::Insert { columns },
        Action::Update { .. } => Action::Update { columns },
        Action::References { .. } => Action::References { columns },
        _ => action.clone(),
    }
}

fn is_unsupported_column_scoped_revoke_against_table_wide_action(
    grant_action: &Action,
    revoke_action: &Action,
) -> bool {
    mem::discriminant(grant_action) == mem::discriminant(revoke_action)
        && is_column_scoped_action(grant_action)
        && matches!((action_columns(grant_action), action_columns(revoke_action)), (None, Some(_)))
}

/// Returns whether a `REVOKE` targets an unrepresentable case for this grant:
/// revoking specific columns from a table-wide action grant.
///
/// Current grant model limits:
/// - It can represent table-wide actions (for example `SELECT ON t`) or
///   explicit column lists (for example `SELECT (a, b) ON t`).
/// - It cannot represent "table-wide minus subset" (for example `SELECT ON t`
///   minus column `a`).
pub(crate) fn has_unsupported_column_scoped_revoke(grant: &Grant, revoke: &Revoke) -> bool {
    if !grant_objects_match(grant.objects.as_ref(), revoke.objects.as_ref()) {
        return false;
    }

    if !grantees_overlap(&grant.grantees, &revoke.grantees) {
        return false;
    }

    match (&grant.privileges, &revoke.privileges) {
        (Privileges::Actions(grant_actions), Privileges::Actions(revoke_actions)) => {
            grant_actions.iter().any(|grant_action| {
                revoke_actions.iter().any(|revoke_action| {
                    is_unsupported_column_scoped_revoke_against_table_wide_action(
                        grant_action,
                        revoke_action,
                    )
                })
            })
        }
        _ => false,
    }
}

fn apply_revoke_action_to_grant_action(
    grant_action: &Action,
    revoke_action: &Action,
) -> (bool, Option<Action>) {
    if mem::discriminant(grant_action) != mem::discriminant(revoke_action) {
        return (false, Some(grant_action.clone()));
    }

    if !is_column_scoped_action(grant_action) {
        return (true, None);
    }

    match (action_columns(grant_action), action_columns(revoke_action)) {
        // Unrepresentable in the current model ("table-wide action minus some
        // columns"), so this action is explicitly treated as unsupported.
        (None, Some(_)) => (false, Some(grant_action.clone())),
        (None | Some(_), None) => (true, None),
        (Some(grant_columns), Some(revoke_columns)) => {
            let remaining_columns: Vec<Ident> = grant_columns
                .iter()
                .filter(|grant_ident| {
                    !revoke_columns.iter().any(|revoke_ident| {
                        identifiers_match(
                            grant_ident.value.as_str(),
                            grant_ident.quote_style.is_some(),
                            revoke_ident.value.as_str(),
                            revoke_ident.quote_style.is_some(),
                        )
                    })
                })
                .cloned()
                .collect();

            if remaining_columns.is_empty() {
                (true, None)
            } else {
                (true, Some(action_with_columns(grant_action, Some(remaining_columns))))
            }
        }
    }
}

/// Result of applying a REVOKE statement to a single grant.
#[derive(Debug, Clone)]
pub struct RevokeApplication {
    /// Whether the revoke matched this grant (objects, grantees and
    /// privileges).
    pub matched: bool,
    /// Updated grant. `None` means the grant is fully removed.
    pub updated_grant: Option<Grant>,
}

/// Applies a REVOKE statement to a grant and returns the resulting grant (if
/// any).
///
/// Representation notes:
/// - `GRANT ALL` minus a subset of actions is not representable; the grant is
///   preserved unchanged and treated as matched.
/// - Column-scoped revoke from a table-wide action grant is also
///   unrepresentable and is surfaced via higher-level
///   `Error::UnsupportedRevoke`.
#[must_use]
pub fn apply_revoke_to_grant(grant: &Grant, revoke: &Revoke) -> RevokeApplication {
    // Objects must match for a revoke to apply to this grant.
    if !grant_objects_match(grant.objects.as_ref(), revoke.objects.as_ref()) {
        return RevokeApplication { matched: false, updated_grant: Some(grant.clone()) };
    }

    // At least one grantee must overlap.
    if !grantees_overlap(&grant.grantees, &revoke.grantees) {
        return RevokeApplication { matched: false, updated_grant: Some(grant.clone()) };
    }

    // TODO: a role membership variant on `Privileges`, once upstream parses
    // `GRANT role TO role`, breaks this match on purpose. Give it an arm rather
    // than a wildcard: membership is neither a table nor a column grant.
    match (&grant.privileges, &revoke.privileges) {
        (_, Privileges::All { .. }) => RevokeApplication { matched: true, updated_grant: None },
        (Privileges::All { .. }, Privileges::Actions(_)) => {
            // We cannot represent "ALL minus X" in this model.
            RevokeApplication { matched: true, updated_grant: Some(grant.clone()) }
        }
        (Privileges::Actions(grant_actions), Privileges::Actions(revoke_actions)) => {
            let mut matched = false;
            let mut updated_actions = Vec::new();

            for grant_action in grant_actions {
                let mut current = Some(grant_action.clone());

                for revoke_action in revoke_actions {
                    let Some(current_action) = current.take() else {
                        break;
                    };
                    let (action_matched, next_action) =
                        apply_revoke_action_to_grant_action(&current_action, revoke_action);
                    if action_matched {
                        matched = true;
                    }
                    current = next_action;
                }

                if let Some(action) = current {
                    updated_actions.push(action);
                }
            }

            if !matched {
                return RevokeApplication { matched: false, updated_grant: Some(grant.clone()) };
            }

            if updated_actions.is_empty() {
                RevokeApplication { matched: true, updated_grant: None }
            } else {
                let mut updated_grant = grant.clone();
                updated_grant.privileges = Privileges::Actions(updated_actions);
                RevokeApplication { matched: true, updated_grant: Some(updated_grant) }
            }
        }
    }
}

impl Metadata for Grant {
    type Meta = ();
}

impl GrantLike for Grant {
    type DB = ParserDB;

    fn privileges<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Action>
    where
        Self: 'db,
    {
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

    fn grantees<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Grantee>
    where
        Self: 'db,
    {
        self.grantees.iter()
    }

    fn applies_to_public(&self) -> bool {
        self.grantees.iter().any(grantee_is_public)
    }

    fn target_table_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        let names: &[ObjectName] = match &self.objects {
            Some(GrantObjects::Tables(tables)) => tables,
            _ => &[],
        };
        names.iter().filter_map(target_name_from_object_name)
    }

    fn target_schema_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        let names: &[ObjectName] = match &self.objects {
            Some(GrantObjects::AllTablesInSchema { schemas }) => schemas,
            _ => &[],
        };
        names.iter().filter_map(target_name_from_object_name)
    }

    fn with_grant_option(&self) -> bool {
        self.with_grant_option
    }

    fn granted_by<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> Option<&'a <Self::DB as DatabaseLike>::Role> {
        self.granted_by.as_ref().and_then(|ident| {
            database.roles().find(|role| {
                let role: &CreateRole = role;
                role_matches_ident(role, ident.value.as_str(), ident.quote_style.is_some())
            })
        })
    }

    fn applies_to_role(&self, role: &<Self::DB as DatabaseLike>::Role) -> bool {
        let role: &CreateRole = role;
        self.grantees.iter().any(|grantee| grantee_matches_role(grantee, role))
    }
}

/// Resolves each table a grant names through the database's search path, the
/// same way the read did, so a bare name reaches the same table here as there.
fn granted_tables<'a>(
    grant: &'a Grant,
    database: &'a ParserDB,
) -> Vec<&'a <ParserDB as DatabaseLike>::Table> {
    match &grant.objects {
        Some(GrantObjects::Tables(names)) => {
            names
                .iter()
                .filter_map(|name| resolve_object_name(name, database).ok().flatten())
                .collect()
        }
        Some(GrantObjects::AllTablesInSchema { schemas }) => {
            database
                .tables()
                .filter(|table| {
                    schemas.iter().any(|schema_name| schema_matches_table(schema_name, *table))
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

impl TableGrantLike for Grant {
    fn tables<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Table> {
        granted_tables(self, database).into_iter()
    }

    fn applies_to_table(
        &self,
        table: &<Self::DB as DatabaseLike>::Table,
        database: &Self::DB,
    ) -> bool {
        match &self.objects {
            Some(GrantObjects::AllTablesInSchema { schemas }) => {
                schemas.iter().any(|schema_name| schema_matches_table(schema_name, table))
            }
            _ => {
                granted_tables(self, database).iter().any(|granted| core::ptr::eq(*granted, table))
            }
        }
    }
}

impl ColumnGrantLike for Grant {
    fn columns<'a>(
        &'a self,
        table: &'a <Self::DB as DatabaseLike>::Table,
        database: &'a Self::DB,
    ) -> Result<impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Column>, LookupError> {
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

        Ok(table
            .columns(database)?
            .filter(move |col| column_idents.iter().any(|ident| ident.value == col.column_name())))
    }

    fn table<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> Option<&'a <Self::DB as DatabaseLike>::Table> {
        granted_tables(self, database).into_iter().next()
    }
}
