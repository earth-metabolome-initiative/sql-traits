//! Implementations for [`ParserDB`] - a database schema parsed from SQL text.

use alloc::{
    borrow::ToOwned,
    boxed::Box,
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
#[cfg(feature = "std")]
use std::path::{Path, PathBuf};

#[cfg(feature = "git")]
use git2::Repository;
use sql_docs::SqlDoc;
#[cfg(feature = "std")]
use sqlparser::parser::ParserError;
use sqlparser::{
    ast::{
        AlterPolicy, AlterPolicyOperation, AlterSchema, AlterSchemaOperation, AlterTableOperation,
        CheckConstraint, ColumnDef, ColumnOption, CreateFunction, CreateFunctionBody, CreateIndex,
        CreatePolicy, CreateRole, CreateTable, CreateTrigger, DataType, ExactNumberInfo, Expr,
        ForeignKeyConstraint, FunctionReturnType, Grant, GrantObjects, Grantee, GranteeName,
        GranteesType, Ident, IndexColumn, ObjectName, ObjectNamePart, OperateFunctionArg,
        OrderByExpr, OrderByOptions, RenameTableNameKind, SchemaName, Statement, TableConstraint,
        TimezoneInfo, UniqueConstraint, Value, ValueWithSpan,
    },
    dialect::Dialect,
    parser::Parser,
    tokenizer::Span,
};

use crate::{
    errors::{LookupError, ObjectKind},
    impls::SqlparserDialect,
    structs::{
        GenericDB, Schema, TableAttribute, TableMetadata,
        metadata::{CheckMetadata, IndexMetadata, PolicyMetadata, UniqueIndexMetadata},
    },
    traits::{ColumnLike, FunctionLike, TableLike},
    utils::{
        columns_in_expression,
        identifier_resolution::identifiers_match,
        last_str,
        object_name::{
            object_name_identifiers, object_name_last_part, resolve_table_object_name_in_iter,
            resolve_table_object_name_with_implicit_public_in_iter,
        },
    },
};

mod functions_in_expression;
mod parse_options;

pub use parse_options::{GrantResolution, ParseOptions};

/// A type alias for a `GenericDBBuilder` specialized for `sqlparser`'s
/// `CreateTable`.
pub type ParserDBBuilder = super::GenericDBBuilder<
    CreateTable,
    TableAttribute<CreateTable, ColumnDef>,
    TableAttribute<CreateTable, CreateIndex>,
    TableAttribute<CreateTable, UniqueConstraint>,
    TableAttribute<CreateTable, ForeignKeyConstraint>,
    CreateFunction,
    TableAttribute<CreateTable, CheckConstraint>,
    CreateTrigger,
    CreatePolicy,
    CreateRole,
    Schema,
    Grant,
    Grant,
    SqlparserDialect,
>;

impl ParserDBBuilder {
    /// Checks if a function with the given name is referenced by any schema
    /// object.
    ///
    /// Returns `true` if the function is used by:
    /// - Check constraints (via their metadata)
    /// - Policies (via USING or WITH CHECK expressions)
    /// - Triggers (via EXECUTE FUNCTION)
    fn is_function_used(&self, function_name: &str, function_name_quoted: bool) -> bool {
        use crate::traits::{FunctionLike, TriggerLike};

        // Check if any check constraint references the function
        for (_, metadata) in self.check_constraints() {
            if metadata.functions().any(|f| {
                identifiers_match(f.name(), f.name_is_quoted(), function_name, function_name_quoted)
            }) {
                return true;
            }
        }

        // Check if any policy references the function
        for (_, metadata) in self.policies() {
            if metadata.using_functions().any(|f| {
                identifiers_match(f.name(), f.name_is_quoted(), function_name, function_name_quoted)
            }) {
                return true;
            }
            if metadata.check_functions().any(|f| {
                identifiers_match(f.name(), f.name_is_quoted(), function_name, function_name_quoted)
            }) {
                return true;
            }
        }

        // Check if any trigger executes the function
        for (trigger, ()) in self.triggers() {
            if trigger.function_name_ident().is_some_and(|(name, quoted)| {
                identifiers_match(name, quoted, function_name, function_name_quoted)
            }) {
                return true;
            }
        }

        false
    }

    /// Checks if a table with the given name is referenced by foreign keys from
    /// other tables.
    ///
    /// Returns `true` if any other table has a foreign key pointing to this
    /// table.
    fn is_table_referenced(
        &self,
        table_name: &str,
        table_name_quoted: bool,
        schema_name: Option<&str>,
        schema_quoted: bool,
    ) -> bool {
        for (fk, ()) in self.foreign_keys() {
            // Check if this FK references the table being dropped
            // and is NOT from the same table (self-referential FKs are OK to drop)
            let Some(referenced_table) = resolve_table_object_name_in_iter(
                self.tables().iter().map(|(table, _)| table.as_ref()),
                &fk.attribute().foreign_table,
            )
            .ok()
            .flatten() else {
                continue;
            };
            let Some(host_table) = resolve_table_object_name_in_iter(
                self.tables().iter().map(|(table, _)| table.as_ref()),
                &fk.table().name,
            )
            .ok()
            .flatten() else {
                continue;
            };

            let referenced_matches = table_matches_resolved_identity(
                referenced_table,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            );
            let host_matches = table_matches_resolved_identity(
                host_table,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            );

            if referenced_matches && !host_matches {
                return true;
            }
        }

        false
    }

    /// Removes a table and all its associated schema objects.
    ///
    /// This removes:
    /// - The table itself
    /// - All columns belonging to the table
    /// - All indices on the table
    /// - All unique indices on the table
    /// - All foreign keys from the table
    /// - All check constraints on the table
    /// - All triggers on the table
    /// - All policies on the table
    /// - All grants on the table
    fn remove_table(
        &mut self,
        table_name: &str,
        table_name_quoted: bool,
        schema_name: Option<&str>,
        schema_quoted: bool,
    ) {
        // Remove the table
        self.tables_mut().retain(|(t, _)| {
            !table_matches_resolved_identity(
                t,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove columns belonging to this table
        self.columns_mut().retain(|(c, ())| {
            !table_matches_resolved_identity(
                TableAttribute::table(c),
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove indices on this table
        self.indices_mut().retain(|(i, _)| {
            !table_matches_resolved_identity(
                TableAttribute::table(i),
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove unique indices on this table
        self.unique_indices_mut().retain(|(u, _)| {
            !table_matches_resolved_identity(
                TableAttribute::table(u),
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove foreign keys from this table
        self.foreign_keys_mut().retain(|(fk, ())| {
            !table_matches_resolved_identity(
                TableAttribute::table(fk),
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove check constraints on this table
        self.check_constraints_mut().retain(|(c, _)| {
            !table_matches_resolved_identity(
                TableAttribute::table(c),
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove triggers on this table
        self.triggers_mut().retain(|(t, ())| {
            !object_name_matches_resolved_identity(
                &t.table_name,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove policies on this table
        self.policies_mut().retain(|(p, _)| {
            !object_name_matches_resolved_identity(
                &p.table_name,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        });

        // Remove table grants for this table
        self.table_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| {
                object_name_matches_resolved_identity(
                    t,
                    table_name,
                    table_name_quoted,
                    schema_name,
                    schema_quoted,
                )
            }))
        });

        // Remove column grants for this table
        self.column_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| {
                object_name_matches_resolved_identity(
                    t,
                    table_name,
                    table_name_quoted,
                    schema_name,
                    schema_quoted,
                )
            }))
        });
    }

    /// Detaches every model object derived from the stored node of a table,
    /// leaving the table entry itself in place.
    ///
    /// Derived objects wrap the node they were built from, so a statement that
    /// edits the node has to rebuild them. Columns and constraint-implied
    /// objects follow from the node and are recomputed, while `CREATE INDEX`
    /// indexes do not and are returned with their expressions so the caller can
    /// re-attach them.
    fn take_table_derived_objects(
        &mut self,
        table_name: &str,
        table_name_quoted: bool,
        schema_name: Option<&str>,
        schema_quoted: bool,
    ) -> Vec<(CreateIndex, Expr)> {
        let belongs_to = |table: &CreateTable| {
            table_matches_resolved_identity(
                table,
                table_name,
                table_name_quoted,
                schema_name,
                schema_quoted,
            )
        };

        let mut detached_indices = Vec::new();
        self.indices_mut().retain(|(index, metadata)| {
            if belongs_to(TableAttribute::table(index)) {
                detached_indices.push((index.attribute().clone(), metadata.expression().clone()));
                return false;
            }
            true
        });

        self.columns_mut().retain(|(column, ())| !belongs_to(TableAttribute::table(column)));
        self.unique_indices_mut().retain(|(index, _)| !belongs_to(TableAttribute::table(index)));
        self.foreign_keys_mut().retain(|(fk, ())| !belongs_to(TableAttribute::table(fk)));
        self.check_constraints_mut().retain(|(check, _)| !belongs_to(TableAttribute::table(check)));

        detached_indices
    }

    /// Checks if a role with the given name is referenced by any grants.
    ///
    /// Returns `true` if the role is a grantee in any table or column grant.
    fn is_role_referenced(&self, role_name: &str, role_quoted: bool) -> bool {
        let check_grantees = |grantees: &[sqlparser::ast::Grantee]| -> bool {
            grantees.iter().any(|g| {
                matches!(
                    &g.name,
                    Some(GranteeName::ObjectName(name))
                        if object_name_last_identifier(name).is_some_and(|grantee_ident| {
                            identifiers_match(
                                grantee_ident.value.as_str(),
                                grantee_ident.quote_style.is_some(),
                                role_name,
                                role_quoted,
                            )
                        })
                )
            })
        };

        // Check table grants
        for (grant, ()) in self.table_grants() {
            if check_grantees(&grant.grantees) {
                return true;
            }
        }

        // Check column grants
        for (grant, ()) in self.column_grants() {
            if check_grantees(&grant.grantees) {
                return true;
            }
        }

        false
    }

    /// Checks if a schema contains any objects (tables).
    ///
    /// Returns `true` if any table belongs to this schema.
    fn is_schema_non_empty(&self, schema_name: &str, schema_quoted: bool) -> bool {
        use crate::traits::TableLike;

        // Check if any table is in this schema
        self.tables().iter().any(|(t, _)| {
            t.table_schema().is_some_and(|table_schema| {
                identifiers_match(
                    table_schema,
                    t.table_schema_is_quoted(),
                    schema_name,
                    schema_quoted,
                )
            })
        })
    }

    fn resolve_schema_ident(&self, ident: &Ident) -> Option<&Schema> {
        resolve_schema_ident_in_iter(
            self.schemas().iter().map(|(schema, ())| schema.as_ref()),
            ident,
        )
    }

    fn resolve_table_object_name(
        &self,
        object_name: &ObjectName,
    ) -> Result<Option<&CreateTable>, LookupError> {
        resolve_table_object_name_in_iter(
            self.tables().iter().map(|(table, _)| table.as_ref()),
            object_name,
        )
    }
}

/// A type alias for the result of processing check constraints.
type CheckConstraintResult =
    (Vec<Arc<TableAttribute<CreateTable, ColumnDef>>>, Vec<Arc<CreateFunction>>);

/// A type alias for the result of processing unique constraints.
type UniqueConstraintResult = (
    Arc<TableAttribute<CreateTable, UniqueConstraint>>,
    UniqueIndexMetadata<TableAttribute<CreateTable, UniqueConstraint>>,
);

fn object_name_last_identifier(object_name: &ObjectName) -> Option<&Ident> {
    match object_name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => Some(ident),
        _ => None,
    }
}

fn resolve_schema_ident_in_iter<'a>(
    mut schemas: impl Iterator<Item = &'a Schema>,
    ident: &Ident,
) -> Option<&'a Schema> {
    schemas.find(|schema| {
        identifiers_match(
            schema.name(),
            schema.is_quoted(),
            ident.value.as_str(),
            ident.quote_style.is_some(),
        )
    })
}

fn table_matches_resolved_identity(
    table: &CreateTable,
    table_name: &str,
    table_name_quoted: bool,
    schema_name: Option<&str>,
    schema_quoted: bool,
) -> bool {
    if !identifiers_match(
        table.table_name(),
        table.table_name_is_quoted(),
        table_name,
        table_name_quoted,
    ) {
        return false;
    }

    match (table.table_schema(), schema_name) {
        (None, None) => true,
        (Some(table_schema), Some(schema_name)) => {
            identifiers_match(
                table_schema,
                table.table_schema_is_quoted(),
                schema_name,
                schema_quoted,
            )
        }
        _ => false,
    }
}

fn object_name_matches_resolved_identity(
    object_name: &ObjectName,
    table_name: &str,
    table_name_quoted: bool,
    schema_name: Option<&str>,
    schema_quoted: bool,
) -> bool {
    let Ok((schema_ident, table_ident)) = object_name_identifiers(object_name) else {
        return false;
    };

    if !identifiers_match(
        table_ident.value.as_str(),
        table_ident.quote_style.is_some(),
        table_name,
        table_name_quoted,
    ) {
        return false;
    }

    match (schema_ident, schema_name) {
        (None, None) => true,
        (Some(schema_ident), Some(schema_name)) => {
            identifiers_match(
                schema_ident.value.as_str(),
                schema_ident.quote_style.is_some(),
                schema_name,
                schema_quoted,
            )
        }
        _ => false,
    }
}

fn role_matches_lookup_ident(role: &CreateRole, lookup_ident: &Ident) -> bool {
    role.names.iter().any(|role_name| {
        object_name_last_identifier(role_name).is_some_and(|role_ident| {
            identifiers_match(
                role_ident.value.as_str(),
                role_ident.quote_style.is_some(),
                lookup_ident.value.as_str(),
                lookup_ident.quote_style.is_some(),
            )
        })
    })
}

/// Returns the identifier a grantee resolves a role by, or `None` when the
/// grantee names no role of its own: the `PUBLIC` pseudo-role, however
/// spelled, or a grantee whose name is not a plain identifier.
fn grantee_role_ident(grantee: &Grantee) -> Option<&Ident> {
    if grantee.grantee_type == GranteesType::Public {
        return None;
    }

    let Some(GranteeName::ObjectName(grantee_name)) = &grantee.name else {
        return None;
    };
    let grantee_ident = object_name_last_identifier(grantee_name)?;

    if grantee_ident.quote_style.is_none() && grantee_ident.value.eq_ignore_ascii_case("PUBLIC") {
        return None;
    }

    Some(grantee_ident)
}

/// Enforces [`GrantResolution::ClosedWorld`] on one `GRANT`: every grantee
/// names a role, and every table target names a table, that the input has
/// created up to this statement.
fn validate_grant_against_builder(
    builder: &ParserDBBuilder,
    grant: &Grant,
) -> Result<(), crate::errors::Error> {
    for grantee in &grant.grantees {
        let Some(grantee_ident) = grantee_role_ident(grantee) else {
            continue;
        };

        let role_exists =
            builder.roles().iter().any(|(role, ())| role_matches_lookup_ident(role, grantee_ident));
        if !role_exists {
            return Err(crate::errors::Error::RoleNotFoundForGrant {
                role_name: grantee_ident.value.clone(),
            });
        }
    }

    if let Some(GrantObjects::Tables(tables)) = &grant.objects {
        for table_obj in tables {
            if builder.resolve_table_object_name(table_obj)?.is_none() {
                return Err(crate::errors::Error::TableNotFoundForGrant {
                    table_name: last_str(table_obj).to_string(),
                });
            }
        }
    }

    Ok(())
}

/// Returns whether a table constraint was declared under `name`.
///
/// PostgreSQL spells the name as a constraint name while MySQL spells some of
/// them as an index name, and sqlparser keeps the two in separate fields. A
/// constraint declared without a name has no name to be found by.
fn table_constraint_has_name(constraint: &TableConstraint, name: &Ident) -> bool {
    let declared = match constraint {
        TableConstraint::Unique(unique) => unique.name.as_ref().or(unique.index_name.as_ref()),
        TableConstraint::PrimaryKey(pk) => pk.name.as_ref().or(pk.index_name.as_ref()),
        TableConstraint::ForeignKey(fk) => fk.name.as_ref(),
        TableConstraint::Check(check) => check.name.as_ref(),
        TableConstraint::Index(index) => index.name.as_ref(),
        TableConstraint::FulltextOrSpatial(index) => index.opt_index_name.as_ref(),
        TableConstraint::PrimaryKeyUsingIndex(using_index)
        | TableConstraint::UniqueUsingIndex(using_index) => using_index.name.as_ref(),
        TableConstraint::Exclude(exclude) => exclude.name.as_ref(),
    };

    declared.is_some_and(|declared| {
        identifiers_match(
            declared.value.as_str(),
            declared.quote_style.is_some(),
            name.value.as_str(),
            name.quote_style.is_some(),
        )
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct RevokeStoreApplication {
    matched_any: bool,
    has_unsupported_column_scoped_revoke: bool,
}

fn apply_revoke_to_grant_store(
    grants: &mut Vec<(Arc<Grant>, ())>,
    revoke: &sqlparser::ast::Revoke,
) -> RevokeStoreApplication {
    let mut matched_any = false;
    let mut has_unsupported_column_scoped_revoke = false;
    let mut updated_grants = Vec::with_capacity(grants.len());
    let original_grants = core::mem::take(grants);

    for (grant, ()) in original_grants {
        let (targeted_grantees, untouched_grantees) =
            crate::impls::partition_grantees_for_revoke(&grant.grantees, &revoke.grantees);

        if targeted_grantees.is_empty() {
            updated_grants.push((grant, ()));
            continue;
        }

        let mut targeted_grant = grant.as_ref().clone();
        targeted_grant.grantees = targeted_grantees;

        if crate::impls::has_unsupported_column_scoped_revoke(&targeted_grant, revoke) {
            has_unsupported_column_scoped_revoke = true;
            updated_grants.push((grant, ()));
            continue;
        }

        let application = crate::impls::apply_revoke_to_grant(&targeted_grant, revoke);

        if !application.matched {
            updated_grants.push((grant, ()));
            continue;
        }
        matched_any = true;

        // Preserve the original storage entry when revoke matched but did not
        // change the targeted grantee's privileges (e.g. ALL minus action).
        if application.updated_grant.as_ref().is_some_and(|g| g == &targeted_grant) {
            updated_grants.push((grant, ()));
            continue;
        }

        if !untouched_grantees.is_empty() {
            let mut untouched_grant = grant.as_ref().clone();
            untouched_grant.grantees = untouched_grantees;
            updated_grants.push((Arc::new(untouched_grant), ()));
        }

        if let Some(updated_grant) = application.updated_grant {
            updated_grants.push((Arc::new(updated_grant), ()));
        }
    }

    *grants = updated_grants;
    RevokeStoreApplication { matched_any, has_unsupported_column_scoped_revoke }
}

/// A database schema parsed from SQL text.
///
/// This is the main type for working with SQL schemas parsed from SQL text.
/// It provides methods for parsing SQL from strings, files, or git
/// repositories.
///
/// # Example
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sql_traits::prelude::*;
/// use sqlparser::dialect::GenericDialect;
///
/// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE users (id INT PRIMARY KEY);")?;
/// let table = db.table(None, "users").unwrap();
/// assert_eq!(table.table_name(), "users");
/// # Ok(())
/// # }
/// ```
///
/// # Using PostgreSQL dialect
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sql_traits::prelude::*;
/// use sqlparser::dialect::PostgreSqlDialect;
///
/// let sql = "CREATE ROLE admin SUPERUSER LOGIN;";
/// let db = ParserDB::parse::<PostgreSqlDialect>(sql)?;
/// let role = db.role("admin").unwrap();
/// assert!(role.is_superuser());
/// # Ok(())
/// # }
/// ```
///
/// # Constraints declared after the table
///
/// A key declared by `ALTER TABLE ... ADD CONSTRAINT`, which is how `pg_dump`
/// always spells one, answers exactly as the inline declaration does, and
/// `DROP CONSTRAINT` removes it again.
///
/// ```rust
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use sql_traits::prelude::*;
/// use sqlparser::dialect::PostgreSqlDialect;
///
/// let db = ParserDB::parse::<PostgreSqlDialect>(
///     "
///     CREATE TABLE t (id uuid NOT NULL);
///     ALTER TABLE ONLY t ADD CONSTRAINT t_pkey PRIMARY KEY (id);
///     ",
/// )?;
/// let table = db.table(None, "t").unwrap();
/// assert_eq!(table.primary_key_column(&db)?.unwrap().column_name(), "id");
/// # Ok(())
/// # }
/// ```
pub type ParserDB = GenericDB<
    CreateTable,
    TableAttribute<CreateTable, ColumnDef>,
    TableAttribute<CreateTable, CreateIndex>,
    TableAttribute<CreateTable, UniqueConstraint>,
    TableAttribute<CreateTable, ForeignKeyConstraint>,
    CreateFunction,
    TableAttribute<CreateTable, CheckConstraint>,
    CreateTrigger,
    CreatePolicy,
    CreateRole,
    Schema,
    Grant,
    Grant,
    SqlparserDialect,
>;

/// A grant reference that the database it was read from does not hold.
///
/// Yielded by [`ParserDB::unresolved_grant_references`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnresolvedGrantReference<'a> {
    /// A grantee naming a role no `CREATE ROLE` in the input creates.
    Role(&'a Ident),
    /// A grant target naming a table no `CREATE TABLE` in the input creates.
    Table(&'a ObjectName),
}

impl ParserDB {
    /// Resolves a schema using a parsed SQL identifier.
    ///
    /// Resolution follows PostgreSQL identifier rules:
    /// - quoted identifiers are exact/case-sensitive;
    /// - unquoted identifiers are folded to lowercase.
    #[must_use]
    pub fn resolve_schema_ident(&self, ident: &Ident) -> Option<&Schema> {
        resolve_schema_ident_in_iter(self.schemas.iter().map(|(schema, ())| schema.as_ref()), ident)
    }

    /// Resolves a table from a one-part or two-part SQL object name.
    ///
    /// For one-part names, only schema-less tables are considered.
    /// For two-part names, the first part is treated as schema and the second
    /// part as table.
    ///
    /// # Errors
    ///
    /// Returns an error when the object name is malformed for table lookup, or
    /// when lookup is ambiguous.
    pub fn resolve_table_object_name(
        &self,
        object_name: &ObjectName,
    ) -> Result<Option<&CreateTable>, LookupError> {
        resolve_table_object_name_in_iter(
            self.tables.iter().map(|(table, _)| table.as_ref()),
            object_name,
        )
    }

    /// Resolves a table from an SQL object name with implicit `public`
    /// fallback.
    ///
    /// For unqualified names, this method first resolves against schema-less
    /// tables, then against tables in schema `public`.
    ///
    /// # Errors
    ///
    /// Returns an error when the object name is malformed for table lookup, or
    /// when lookup is ambiguous.
    pub fn resolve_table_object_name_with_implicit_public(
        &self,
        object_name: &ObjectName,
    ) -> Result<Option<&CreateTable>, LookupError> {
        resolve_table_object_name_with_implicit_public_in_iter(
            self.tables.iter().map(|(table, _)| table.as_ref()),
            object_name,
        )
    }

    /// Checks that every foreign key resolves: the referenced table exists in
    /// this database and each referenced column exists on it.
    ///
    /// Order-insensitive: it runs against the fully ingested database, so
    /// forward and self-references pass. Targets resolve under the same
    /// implicit-`public` policy as other object-name lookups. Opt-in, so
    /// partial schemas still parse; call it after parse to enforce closure.
    ///
    /// # Errors
    ///
    /// Returns the first unresolved constraint as
    /// [`ReferencedTableNotFoundForForeignKey`](crate::errors::Error::ReferencedTableNotFoundForForeignKey)
    /// or
    /// [`ReferencedColumnNotFoundForForeignKey`](crate::errors::Error::ReferencedColumnNotFoundForForeignKey).
    /// A malformed target name surfaces as
    /// [`IdentifierLookupError`](crate::errors::Error::IdentifierLookupError).
    ///
    /// # Examples
    ///
    /// ```
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::GenericDialect;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE TABLE parent (id INT PRIMARY KEY);
    ///     CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
    ///     ",
    /// )?;
    /// assert!(db.validate_foreign_key_targets().is_ok());
    ///
    /// let dangling = ParserDB::parse::<GenericDialect>(
    ///     "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES orders(id));",
    /// )?;
    /// assert!(dangling.validate_foreign_key_targets().is_err());
    /// # Ok::<(), sql_traits::errors::Error>(())
    /// ```
    pub fn validate_foreign_key_targets(&self) -> Result<(), crate::errors::Error> {
        for (fk, ()) in &self.foreign_keys {
            let constraint = fk.attribute();
            let host_table = fk.table();
            let Some(referenced_table) =
                self.resolve_table_object_name_with_implicit_public(&constraint.foreign_table)?
            else {
                return Err(crate::errors::Error::ReferencedTableNotFoundForForeignKey {
                    referenced_table: constraint.foreign_table.to_string(),
                    host_table: host_table.name.to_string(),
                });
            };

            for referred in &constraint.referred_columns {
                let column_exists = referenced_table.columns.iter().any(|column| {
                    identifiers_match(
                        column.name.value.as_str(),
                        column.name.quote_style.is_some(),
                        referred.value.as_str(),
                        referred.quote_style.is_some(),
                    )
                });
                if !column_exists {
                    return Err(crate::errors::Error::ReferencedColumnNotFoundForForeignKey {
                        referenced_column: referred.value.clone(),
                        referenced_table: referenced_table.name.to_string(),
                        host_table: host_table.name.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Reports the grantees and the table targets of this database's grants
    /// that the database does not itself hold. The grant object shapes the
    /// closed world never resolved either, `ALL TABLES IN SCHEMA` and the
    /// sequence and schema forms, are left alone here too.
    ///
    /// A [`GrantResolution::ClosedWorld`] parse rejects such a reference on
    /// the spot, so one surfaces here either because the database was parsed
    /// under [`GrantResolution::OpenWorld`], or because a later statement
    /// moved an object out from under a grant that names it: a table rename
    /// leaves the grant naming the old name. The walk is order-insensitive,
    /// running against the fully ingested database, so a grant preceding the
    /// `CREATE ROLE` it names resolves. Each distinct reference is reported
    /// once, in a deterministic order.
    ///
    /// # Errors
    ///
    /// Returns a [`LookupError`] when a grant target is malformed as a table
    /// name or matches more than one table.
    ///
    /// # Examples
    ///
    /// ```
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParseOptions::default()
    ///     .with_grant_resolution(GrantResolution::OpenWorld)
    ///     .parse::<PostgreSqlDialect>(
    ///         "CREATE TABLE docs (id uuid PRIMARY KEY);
    ///          GRANT SELECT ON docs TO app;",
    ///     )?;
    ///
    /// let unresolved: Vec<_> = db.unresolved_grant_references()?.collect();
    /// assert!(matches!(
    ///     unresolved[..],
    ///     [UnresolvedGrantReference::Role(role)] if role.value == "app"
    /// ));
    /// # Ok::<(), sql_traits::errors::Error>(())
    /// ```
    pub fn unresolved_grant_references(
        &self,
    ) -> Result<impl Iterator<Item = UnresolvedGrantReference<'_>>, LookupError> {
        // The parse path records each `GRANT` in both stores, so the set
        // collapses the two views back into one reference per name.
        let grants = self
            .table_grants
            .iter()
            .chain(self.column_grants.iter())
            .map(|(grant, ())| grant.as_ref());

        let mut unresolved = BTreeSet::new();
        for grant in grants {
            for grantee in &grant.grantees {
                let Some(grantee_ident) = grantee_role_ident(grantee) else {
                    continue;
                };
                if !self
                    .roles
                    .iter()
                    .any(|(role, ())| role_matches_lookup_ident(role.as_ref(), grantee_ident))
                {
                    unresolved.insert(UnresolvedGrantReference::Role(grantee_ident));
                }
            }

            if let Some(GrantObjects::Tables(tables)) = &grant.objects {
                for table_obj in tables {
                    if self.resolve_table_object_name(table_obj)?.is_none() {
                        unresolved.insert(UnresolvedGrantReference::Table(table_obj));
                    }
                }
            }
        }

        Ok(unresolved.into_iter())
    }

    /// Checks that every recorded grant resolves: each grantee names a role
    /// and each table target names a table this database holds.
    ///
    /// This is the [`GrantResolution::ClosedWorld`] verdict on a database
    /// parsed under [`GrantResolution::OpenWorld`], deferred until the whole
    /// input is in and therefore insensitive to statement order.
    ///
    /// # Errors
    ///
    /// Returns the first unresolved reference as
    /// [`RoleNotFoundForGrant`](crate::errors::Error::RoleNotFoundForGrant) or
    /// [`TableNotFoundForGrant`](crate::errors::Error::TableNotFoundForGrant).
    /// A malformed or ambiguous target name surfaces as
    /// [`IdentifierLookupError`](crate::errors::Error::IdentifierLookupError).
    ///
    /// # Examples
    ///
    /// ```
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let options = ParseOptions::default().with_grant_resolution(GrantResolution::OpenWorld);
    ///
    /// let db = options.parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE docs (id uuid PRIMARY KEY);
    ///      GRANT SELECT ON docs TO app;
    ///      CREATE ROLE app;",
    /// )?;
    /// assert!(db.validate_grant_targets().is_ok());
    ///
    /// let dangling = options.parse::<PostgreSqlDialect>(
    ///     "CREATE TABLE docs (id uuid PRIMARY KEY); GRANT SELECT ON docs TO app;",
    /// )?;
    /// assert!(dangling.validate_grant_targets().is_err());
    /// # Ok::<(), sql_traits::errors::Error>(())
    /// ```
    pub fn validate_grant_targets(&self) -> Result<(), crate::errors::Error> {
        match self.unresolved_grant_references()?.next() {
            Some(UnresolvedGrantReference::Role(grantee_ident)) => {
                Err(crate::errors::Error::RoleNotFoundForGrant {
                    role_name: grantee_ident.value.clone(),
                })
            }
            Some(UnresolvedGrantReference::Table(table_obj)) => {
                Err(crate::errors::Error::TableNotFoundForGrant {
                    table_name: last_str(table_obj).to_string(),
                })
            }
            None => Ok(()),
        }
    }

    /// Helper function to process check constraints.
    fn process_check_constraint(
        check_expr: &Expr,
        create_table: &Arc<CreateTable>,
        table_metadata: &TableMetadata<CreateTable>,
        builder: &ParserDBBuilder,
    ) -> Result<CheckConstraintResult, crate::errors::Error> {
        let columns_in_expression =
            columns_in_expression::<Arc<TableAttribute<CreateTable, ColumnDef>>>(
                check_expr,
                &create_table.name.to_string(),
                table_metadata.column_arc_slice(),
            )?;
        let functions_in_expression = functions_in_expression::functions_in_expression::<Self>(
            check_expr,
            builder.function_arc_vec().as_slice(),
        );
        Ok((columns_in_expression, functions_in_expression))
    }

    /// Helper function to create an index expression from columns.
    ///
    /// The shape mirrors a parsed parenthesized list, which is what
    /// [`crate::traits::IndexLike::is_simple`] reads: one key nests, several
    /// keys form a tuple. An ordering qualifier or an operator class qualifies
    /// a key rather than forming part of the expression, so neither appears
    /// here.
    fn create_index_expression(columns: &[IndexColumn]) -> Option<Expr> {
        match columns {
            [] => None,
            [single] => Some(Expr::Nested(Box::new(single.column.expr.clone()))),
            _ => {
                Some(Expr::Tuple(columns.iter().map(|column| column.column.expr.clone()).collect()))
            }
        }
    }

    /// Helper function to process unique constraints.
    fn process_unique_constraint(
        unique_constraint: UniqueConstraint,
        create_table: &Arc<CreateTable>,
    ) -> Option<UniqueConstraintResult> {
        let unique_index = Arc::new(TableAttribute::new(create_table.clone(), unique_constraint));
        let expression = Self::create_index_expression(&unique_index.attribute().columns)?;
        let unique_index_metadata = UniqueIndexMetadata::new(expression, create_table.clone());
        Some((unique_index, unique_index_metadata))
    }

    #[allow(clippy::type_complexity)]
    /// Helper function to process create index statements.
    fn process_create_index(
        create_index: CreateIndex,
        builder: &ParserDBBuilder,
    ) -> Result<
        (
            Arc<TableAttribute<CreateTable, CreateIndex>>,
            IndexMetadata<TableAttribute<CreateTable, CreateIndex>>,
        ),
        crate::errors::Error,
    > {
        let table_name = last_str(&create_index.table_name);

        let Some(table) = builder.resolve_table_object_name(&create_index.table_name)? else {
            return Err(crate::errors::Error::TableNotFoundForIndex {
                table_name: table_name.to_string(),
                index_name: create_index.name.as_ref().map_or("<unnamed>", last_str).to_string(),
            });
        };

        let index_arc = Arc::new(TableAttribute::new(Arc::new(table.clone()), create_index));
        let Some(expression) = Self::create_index_expression(&index_arc.attribute().columns) else {
            return Err(crate::errors::Error::InvalidIndex {
                index_name: index_arc
                    .attribute()
                    .name
                    .as_ref()
                    .map_or("<unnamed>", last_str)
                    .to_string(),
                reason: "index has no columns".to_string(),
            });
        };
        let metadata = IndexMetadata::new(expression, Arc::new(table.clone()));
        Ok((index_arc, metadata))
    }

    /// Helper function to rename a table while preserving lookup invariants.
    fn rename_table_checked(
        mut builder: ParserDBBuilder,
        old_name: &ObjectName,
        new_name: ObjectName,
        if_exists: bool,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        use crate::traits::TableLike;

        let Some(resolved_table) = builder.resolve_table_object_name(old_name)? else {
            if if_exists {
                return Ok(builder);
            }
            return Err(crate::errors::Error::RenameTableNotFound {
                table_name: last_str(old_name).to_string(),
            });
        };
        let resolved_table_name = resolved_table.table_name().to_string();
        let resolved_table_quoted = resolved_table.table_name_is_quoted();
        let resolved_schema_name = resolved_table.table_schema().map(str::to_string);
        let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

        let Some(table_position) = builder.tables().iter().position(|(table, _)| {
            table_matches_resolved_identity(
                table.as_ref(),
                &resolved_table_name,
                resolved_table_quoted,
                resolved_schema_name.as_deref(),
                resolved_schema_quoted,
            )
        }) else {
            if if_exists {
                return Ok(builder);
            }
            return Err(crate::errors::Error::RenameTableNotFound {
                table_name: last_str(old_name).to_string(),
            });
        };

        let (old_table, meta) = builder.tables_mut().remove(table_position);
        let mut renamed_table = (*old_table).clone();
        renamed_table.name = new_name;

        builder = builder.add_table(Arc::new(renamed_table), meta)?;
        builder.tables_mut().sort_by(|(a, _), (b, _)| {
            (a.table_schema(), a.table_name()).cmp(&(b.table_schema(), b.table_name()))
        });

        Ok(builder)
    }

    /// Helper function to process column options.
    fn process_column_options(
        column: &Arc<TableAttribute<CreateTable, ColumnDef>>,
        create_table: &Arc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        mut builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for option in &column.attribute().options {
            match option.option.clone() {
                ColumnOption::Check(check_constraint) => {
                    let check_arc = Arc::new(TableAttribute::new(
                        create_table.clone(),
                        check_constraint.clone(),
                    ));
                    table_metadata.add_check_constraint(check_arc.clone());
                    let (columns_in_expression, functions_in_expression) =
                        Self::process_check_constraint(
                            &check_constraint.expr,
                            create_table,
                            table_metadata,
                            &builder,
                        )?;
                    builder = builder.add_check_constraint(
                        check_arc,
                        CheckMetadata::new(
                            *check_constraint.expr.clone(),
                            create_table.clone(),
                            columns_in_expression,
                            functions_in_expression,
                        ),
                    );
                }
                ColumnOption::ForeignKey(mut foreign_key) => {
                    foreign_key.columns.push(column.attribute().name.clone());
                    let fk = Arc::new(TableAttribute::new(create_table.clone(), foreign_key));
                    table_metadata.add_foreign_key(fk.clone());
                    builder = builder.add_foreign_key(fk, ());
                }
                ColumnOption::Unique(mut unique_constraint) => {
                    unique_constraint.columns.push(IndexColumn {
                        column: OrderByExpr {
                            expr: Expr::Identifier(column.attribute().name.clone()),
                            options: OrderByOptions::default(),
                            with_fill: None,
                        },
                        operator_class: None,
                    });
                    if let Some((unique_index, unique_index_metadata)) =
                        Self::process_unique_constraint(unique_constraint, create_table)
                    {
                        table_metadata.add_unique_index(unique_index.clone());
                        builder = builder.add_unique_index(unique_index, unique_index_metadata);
                    }
                }
                ColumnOption::PrimaryKey(_) => {
                    let primary_key_unique_constraint = UniqueConstraint {
                        name: None,
                        index_name: None,
                        index_type_display: sqlparser::ast::KeyOrIndexDisplay::None,
                        index_type: None,
                        columns: vec![IndexColumn {
                            column: OrderByExpr {
                                expr: Expr::Identifier(column.attribute().name.clone()),
                                options: OrderByOptions::default(),
                                with_fill: None,
                            },
                            operator_class: None,
                        }],
                        include: vec![],
                        index_options: vec![],
                        characteristics: None,
                        nulls_distinct: sqlparser::ast::NullsDistinctOption::None,
                    };

                    if let Some((unique_index, unique_index_metadata)) =
                        Self::process_unique_constraint(primary_key_unique_constraint, create_table)
                    {
                        table_metadata.add_unique_index(unique_index.clone());
                        builder = builder.add_unique_index(unique_index, unique_index_metadata);
                    }

                    table_metadata.set_primary_key(vec![column.clone()]);
                }
                _ => {}
            }
        }
        Ok(builder)
    }

    /// Helper function to process a foreign key table constraint.
    fn process_foreign_key_table_constraint(
        fk: &ForeignKeyConstraint,
        create_table: &Arc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for col_ident in &fk.columns {
            let column_exists = table_metadata.column_arcs().any(|col| {
                identifiers_match(
                    col.column_name(),
                    col.column_name_is_quoted(),
                    col_ident.value.as_str(),
                    col_ident.quote_style.is_some(),
                )
            });

            if !column_exists {
                return Err(crate::errors::Error::HostColumnNotFoundForForeignKey {
                    host_column: col_ident.value.clone(),
                    host_table: create_table.name.to_string(),
                });
            }
        }

        let referenced_table_name = fk.foreign_table.to_string();

        let referenced_table = resolve_table_object_name_in_iter(
            builder
                .tables()
                .iter()
                .map(|(t, _)| t.as_ref())
                .chain(core::iter::once(create_table.as_ref())),
            &fk.foreign_table,
        )?;
        let Some(referenced_table) = referenced_table else {
            return Err(crate::errors::Error::ReferencedTableNotFoundForForeignKey {
                referenced_table: referenced_table_name.clone(),
                host_table: create_table.name.to_string(),
            });
        };

        for ref_col_ident in &fk.referred_columns {
            let column_exists = referenced_table.columns.iter().any(|col| {
                identifiers_match(
                    col.name.value.as_str(),
                    col.name.quote_style.is_some(),
                    ref_col_ident.value.as_str(),
                    ref_col_ident.quote_style.is_some(),
                )
            });

            if !column_exists {
                return Err(crate::errors::Error::ReferencedColumnNotFoundForForeignKey {
                    referenced_column: ref_col_ident.value.clone(),
                    referenced_table: referenced_table_name.clone(),
                    host_table: create_table.name.to_string(),
                });
            }
        }

        let fk_arc = Arc::new(TableAttribute::new(create_table.clone(), fk.clone()));
        table_metadata.add_foreign_key(fk_arc.clone());
        let builder = builder.add_foreign_key(fk_arc, ());
        Ok(builder)
    }

    /// Helper function to process table constraints.
    fn process_table_constraints(
        constraints: &[TableConstraint],
        create_table: &Arc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        mut builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique(uc) => {
                    Self::validate_constraint_columns(&uc.columns, create_table, table_metadata)?;
                    if let Some((unique_index, unique_index_metadata)) =
                        Self::process_unique_constraint(uc.clone(), create_table)
                    {
                        table_metadata.add_unique_index(unique_index.clone());
                        builder = builder.add_unique_index(unique_index, unique_index_metadata);
                    }
                }
                TableConstraint::ForeignKey(fk) => {
                    builder = Self::process_foreign_key_table_constraint(
                        fk,
                        create_table,
                        table_metadata,
                        builder,
                    )?;
                }
                TableConstraint::Check(check) => {
                    let check_arc =
                        Arc::new(TableAttribute::new(create_table.clone(), check.clone()));
                    table_metadata.add_check_constraint(check_arc.clone());
                    let (columns_in_expression, functions_in_expression) =
                        Self::process_check_constraint(
                            &check.expr,
                            create_table,
                            table_metadata,
                            &builder,
                        )?;
                    builder = builder.add_check_constraint(
                        check_arc,
                        CheckMetadata::new(
                            *check.expr.clone(),
                            create_table.clone(),
                            columns_in_expression,
                            functions_in_expression,
                        ),
                    );
                }
                TableConstraint::PrimaryKey(pk) => {
                    Self::validate_constraint_columns(&pk.columns, create_table, table_metadata)?;
                    let mut primary_key_columns = Vec::new();
                    for col_name in &pk.columns {
                        let Expr::Identifier(column_name) = &col_name.column.expr else {
                            return Err(crate::errors::Error::InvalidPrimaryKey {
                                table_name: create_table.name.to_string(),
                                reason: format!(
                                    "primary key entries must be plain columns, found expression `{}`",
                                    col_name.column.expr
                                ),
                            });
                        };
                        primary_key_columns.extend(
                            table_metadata
                                .column_arcs()
                                .filter(|col: &&Arc<TableAttribute<CreateTable, ColumnDef>>| {
                                    identifiers_match(
                                        col.column_name(),
                                        col.column_name_is_quoted(),
                                        column_name.value.as_str(),
                                        column_name.quote_style.is_some(),
                                    )
                                })
                                .cloned(),
                        );
                    }

                    let primary_key_unique_constraint = UniqueConstraint {
                        name: pk.name.clone(),
                        index_name: None,
                        index_type_display: sqlparser::ast::KeyOrIndexDisplay::None,
                        index_type: None,
                        columns: pk.columns.clone(),
                        include: pk.include.clone(),
                        index_options: vec![],
                        characteristics: pk.characteristics,
                        nulls_distinct: sqlparser::ast::NullsDistinctOption::None,
                    };

                    if let Some((unique_index, unique_index_metadata)) =
                        Self::process_unique_constraint(primary_key_unique_constraint, create_table)
                    {
                        table_metadata.add_unique_index(unique_index.clone());
                        builder = builder.add_unique_index(unique_index, unique_index_metadata);
                    }

                    table_metadata.set_primary_key(primary_key_columns);
                }
                _ => {}
            }
        }
        Ok(builder)
    }

    /// Checks that every plain column an index-shaped constraint names is
    /// declared by the table the constraint is attached to.
    ///
    /// Entries that are expressions rather than plain columns name no single
    /// column, so they are left alone.
    fn validate_constraint_columns(
        columns: &[IndexColumn],
        create_table: &CreateTable,
        table_metadata: &TableMetadata<CreateTable>,
    ) -> Result<(), LookupError> {
        for column in columns {
            let Expr::Identifier(column_name) = &column.column.expr else {
                continue;
            };
            let declared = table_metadata.column_arcs().any(|declared| {
                identifiers_match(
                    declared.column_name(),
                    declared.column_name_is_quoted(),
                    column_name.value.as_str(),
                    column_name.quote_style.is_some(),
                )
            });
            if !declared {
                return Err(LookupError::ColumnNotFound {
                    table_name: create_table.name.to_string(),
                    column_name: column_name.value.clone(),
                });
            }
        }
        Ok(())
    }

    /// Ingests a table node, deriving its columns and everything its column
    /// options and table constraints imply.
    ///
    /// `table_metadata` carries the state the node does not express: row level
    /// security flags and `CREATE INDEX` indexes.
    fn ingest_table_node(
        mut builder: ParserDBBuilder,
        create_table: Arc<CreateTable>,
        mut table_metadata: TableMetadata<CreateTable>,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for column in create_table.columns.clone() {
            table_metadata.add_column(Arc::new(TableAttribute::new(create_table.clone(), column)));
        }

        for column in table_metadata.clone().column_arcs() {
            builder =
                Self::process_column_options(column, &create_table, &mut table_metadata, builder)?;
            builder = builder.add_column(column.clone(), ());
        }

        builder = Self::process_table_constraints(
            &create_table.constraints,
            &create_table,
            &mut table_metadata,
            builder,
        )?;

        Ok(builder.add_table(create_table, table_metadata)?)
    }

    /// Applies `edit` to the constraint list of the table an `ALTER TABLE`
    /// statement targets, then rebuilds the model objects derived from it.
    ///
    /// A constraint reaches a table either inline in `CREATE TABLE` or later
    /// through `ALTER TABLE`, and both spellings have to answer alike, so the
    /// table node stays the single source of truth and everything derived from
    /// it is recomputed rather than patched.
    fn alter_table_constraints(
        mut builder: ParserDBBuilder,
        table_name: &ObjectName,
        if_exists: bool,
        edit: impl FnOnce(&CreateTable, &mut Vec<TableConstraint>) -> Result<(), crate::errors::Error>,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        let Some(resolved_table) = builder.resolve_table_object_name(table_name)? else {
            if if_exists {
                return Ok(builder);
            }
            return Err(crate::errors::Error::AlterTableNotFound {
                table_name: last_str(table_name).to_string(),
            });
        };
        let resolved_table_name = resolved_table.table_name().to_string();
        let resolved_table_quoted = resolved_table.table_name_is_quoted();
        let resolved_schema_name = resolved_table.table_schema().map(str::to_string);
        let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

        let Some(table_position) = builder.tables().iter().position(|(table, _)| {
            table_matches_resolved_identity(
                table.as_ref(),
                &resolved_table_name,
                resolved_table_quoted,
                resolved_schema_name.as_deref(),
                resolved_schema_quoted,
            )
        }) else {
            if if_exists {
                return Ok(builder);
            }
            return Err(crate::errors::Error::AlterTableNotFound {
                table_name: last_str(table_name).to_string(),
            });
        };

        let (previous_table, previous_metadata) = builder.tables_mut().remove(table_position);
        let mut altered_table = (*previous_table).clone();
        edit(&previous_table, &mut altered_table.constraints)?;

        let mut table_metadata: TableMetadata<CreateTable> = TableMetadata::default();
        table_metadata.set_rls_enabled(previous_metadata.rls_enabled());
        table_metadata.set_rls_forced(previous_metadata.rls_forced());

        let detached_indices = builder.take_table_derived_objects(
            &resolved_table_name,
            resolved_table_quoted,
            resolved_schema_name.as_deref(),
            resolved_schema_quoted,
        );

        let altered_table = Arc::new(altered_table);
        for (index, expression) in detached_indices {
            let index = Arc::new(TableAttribute::new(altered_table.clone(), index));
            table_metadata.add_index(index.clone());
            builder =
                builder.add_index(index, IndexMetadata::new(expression, altered_table.clone()));
        }

        builder = Self::ingest_table_node(builder, altered_table, table_metadata)?;
        builder.tables_mut().sort_by(|(a, _), (b, _)| {
            (a.table_schema(), a.table_name()).cmp(&(b.table_schema(), b.table_name()))
        });

        Ok(builder)
    }

    /// Applies `edit` to the metadata of the table an `ALTER TABLE` statement
    /// targets.
    ///
    /// Metadata carries settings the table node does not spell, so unlike
    /// [`Self::alter_table_constraints`] nothing derived from the node needs
    /// rebuilding.
    ///
    /// # Errors
    ///
    /// Returns [`crate::errors::Error::AlterTableNotFound`] when the statement
    /// names a table the input never created and does not say `IF EXISTS`.
    fn alter_table_metadata(
        mut builder: ParserDBBuilder,
        table_name: &ObjectName,
        if_exists: bool,
        edit: impl FnOnce(&mut TableMetadata<CreateTable>),
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        let Some(resolved_table) = builder.resolve_table_object_name(table_name)? else {
            if if_exists {
                return Ok(builder);
            }
            return Err(crate::errors::Error::AlterTableNotFound {
                table_name: last_str(table_name).to_string(),
            });
        };
        let resolved_table_name = resolved_table.table_name().to_string();
        let resolved_table_quoted = resolved_table.table_name_is_quoted();
        let resolved_schema_name = resolved_table.table_schema().map(str::to_string);
        let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

        let Some(entry) = builder.tables_mut().iter_mut().find(|(table, _)| {
            table_matches_resolved_identity(
                table.as_ref(),
                &resolved_table_name,
                resolved_table_quoted,
                resolved_schema_name.as_deref(),
                resolved_schema_quoted,
            )
        }) else {
            // The identity came from a table this builder resolved, so this is
            // unreachable. `IF EXISTS` does not apply: the statement's table was
            // found, and it is the stored entry that went missing.
            return Err(ObjectKind::Table.not_in_database(&resolved_table_name).into());
        };

        edit(&mut entry.1);

        Ok(builder)
    }

    /// Creates a new `ParserDB` from a vector of SQL statements and a catalog
    /// name.
    ///
    /// # Arguments
    ///
    /// * `statements` - A vector of SQL statements to parse.
    /// * `catalog_name` - The name of the database catalog.
    ///
    /// # Panics
    ///
    /// Panics if an unsupported statement is encountered.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails (e.g., foreign key references
    /// non-existent tables or columns).
    ///
    /// # Example
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = "
    /// CREATE TABLE users (
    ///     id INTEGER PRIMARY KEY,
    ///     name VARCHAR(100)
    /// );
    /// ";
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let db = ParserDB::from_statements(statements, "test".to_string()).unwrap();
    /// assert_eq!(db.catalog_name(), "test");
    /// ```
    #[allow(clippy::too_many_lines)]
    pub fn from_statements(
        statements: Vec<Statement>,
        catalog_name: String,
    ) -> Result<Self, crate::errors::Error> {
        Self::from_statements_with_dialect(statements, catalog_name, SqlparserDialect::default())
    }

    /// Same as [`Self::from_statements`] but explicitly attaches the SQL
    /// dialect the statements were parsed under. Used by [`Self::parse`] to
    /// route dialect-conditional predicates (see
    /// [`crate::traits::DialectLike::is_bool`]).
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails (e.g. a foreign key references a
    /// non-existent table or column).
    pub fn from_statements_with_dialect(
        statements: Vec<Statement>,
        catalog_name: String,
        dialect: SqlparserDialect,
    ) -> Result<Self, crate::errors::Error> {
        Self::from_statements_with_options(
            statements,
            catalog_name,
            dialect,
            ParseOptions::default(),
        )
    }

    /// Same as [`Self::from_statements_with_dialect`] but under caller-chosen
    /// [`ParseOptions`].
    #[allow(clippy::too_many_lines)]
    pub(crate) fn from_statements_with_options(
        statements: Vec<Statement>,
        catalog_name: String,
        dialect: SqlparserDialect,
        options: ParseOptions,
    ) -> Result<Self, crate::errors::Error> {
        let mut builder: ParserDBBuilder = super::GenericDBBuilder::new(catalog_name, dialect);

        let any_type = DataType::Custom(
            ObjectName(vec![ObjectNamePart::Identifier(Ident::with_quote('"', "any"))]),
            vec![],
        );

        let arg = |data_type: DataType| {
            OperateFunctionArg { mode: None, name: None, data_type, default_expr: None }
        };

        let variadic_arg = |data_type: DataType| {
            OperateFunctionArg {
                mode: None,
                name: Some(Ident::new("VARIADIC")),
                data_type,
                default_expr: None,
            }
        };

        let builtins = vec![
            ("length", vec![arg(DataType::Text)], DataType::Int(None)),
            ("len", vec![arg(DataType::Text)], DataType::Int(None)),
            ("char_length", vec![arg(DataType::Text)], DataType::Int(None)),
            ("character_length", vec![arg(DataType::Text)], DataType::Int(None)),
            ("octet_length", vec![arg(DataType::Text)], DataType::Int(None)),
            ("coalesce", vec![variadic_arg(any_type.clone())], any_type.clone()),
            ("nullif", vec![arg(any_type.clone()), arg(any_type.clone())], any_type.clone()),
            ("now", vec![], DataType::Timestamp(None, TimezoneInfo::WithTimeZone)),
            ("current_timestamp", vec![], DataType::Timestamp(None, TimezoneInfo::WithTimeZone)),
            ("current_date", vec![], DataType::Date),
            ("current_time", vec![], DataType::Time(None, TimezoneInfo::WithTimeZone)),
            ("localtimestamp", vec![], DataType::Timestamp(None, TimezoneInfo::None)),
            ("localtime", vec![], DataType::Time(None, TimezoneInfo::None)),
            ("gen_random_uuid", vec![], DataType::Uuid),
            ("uuidv4", vec![], DataType::Uuid),
            ("uuidv7", vec![], DataType::Uuid),
            (
                "uuidv7",
                vec![arg(DataType::Interval { fields: None, precision: None })],
                DataType::Uuid,
            ),
            ("count", vec![arg(any_type.clone())], DataType::BigInt(None)),
            ("sum", vec![arg(any_type.clone())], DataType::Numeric(ExactNumberInfo::None)),
            ("avg", vec![arg(any_type.clone())], DataType::Numeric(ExactNumberInfo::None)),
            ("min", vec![arg(any_type.clone())], any_type.clone()),
            ("max", vec![arg(any_type.clone())], any_type.clone()),
            ("current_user", vec![], DataType::Text),
            ("session_user", vec![], DataType::Text),
            ("user", vec![], DataType::Text),
        ];

        for (name, args, return_type) in builtins {
            let create_function = CreateFunction {
                or_alter: false,
                or_replace: false,
                temporary: false,
                if_not_exists: false,
                name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
                args: Some(args),
                return_type: Some(FunctionReturnType::DataType(return_type)),
                function_body: Some(CreateFunctionBody::AsBeforeOptions {
                    body: Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(String::new()),
                        span: Span::empty(),
                    }),
                    link_symbol: None,
                }),
                behavior: None,
                called_on_null: None,
                parallel: None,
                using: None,
                language: Some(Ident::new("internal")),
                determinism_specifier: None,
                options: None,
                remote_connection: None,
                security: None,
                set_params: vec![],
            };
            builder = builder.add_function(Arc::new(create_function), ());
        }

        for statement in statements {
            match statement {
                Statement::CreateFunction(create_function) => {
                    builder = builder.add_function(Arc::new(create_function), ());
                }
                Statement::DropFunction(drop_function) => {
                    for func_desc in &drop_function.func_desc {
                        let Some((function_name, function_quoted)) =
                            object_name_last_part(&func_desc.name)
                        else {
                            return Err(crate::errors::Error::DropFunctionNotFound {
                                function_name: last_str(&func_desc.name).to_string(),
                            });
                        };

                        // Check if function exists
                        let function_exists = builder.function_arc_vec().iter().any(|f| {
                            identifiers_match(
                                f.name(),
                                f.name_is_quoted(),
                                function_name,
                                function_quoted,
                            )
                        });

                        if !function_exists {
                            if drop_function.if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropFunctionNotFound {
                                function_name: function_name.to_string(),
                            });
                        }

                        // Check for references in check constraints, policies, or triggers
                        if builder.is_function_used(function_name, function_quoted) {
                            return Err(crate::errors::Error::FunctionReferenced {
                                function_name: function_name.to_string(),
                            });
                        }

                        // Remove the function
                        let functions = builder.functions_mut();
                        functions.retain(|(f, ())| {
                            !identifiers_match(
                                f.name(),
                                f.name_is_quoted(),
                                function_name,
                                function_quoted,
                            )
                        });
                    }
                }
                Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    if_exists,
                    names,
                    cascade,
                    ..
                } => {
                    for name in names {
                        let table_name = last_str(&name);

                        // Check if table exists and resolve the canonical stored table.
                        let maybe_table = builder.resolve_table_object_name(&name)?;

                        let Some(table) = maybe_table else {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropTableNotFound {
                                table_name: table_name.to_string(),
                            });
                        };
                        let resolved_table_name = table.table_name().to_string();
                        let resolved_table_quoted = table.table_name_is_quoted();
                        let resolved_schema_name = table.table_schema().map(str::to_string);
                        let resolved_schema_quoted = table.table_schema_is_quoted();

                        // Check for references from other tables (unless CASCADE)
                        if !cascade
                            && builder.is_table_referenced(
                                &resolved_table_name,
                                resolved_table_quoted,
                                resolved_schema_name.as_deref(),
                                resolved_schema_quoted,
                            )
                        {
                            return Err(crate::errors::Error::TableReferenced {
                                table_name: resolved_table_name.clone(),
                            });
                        }

                        // Remove the table and all associated objects
                        builder.remove_table(
                            &resolved_table_name,
                            resolved_table_quoted,
                            resolved_schema_name.as_deref(),
                            resolved_schema_quoted,
                        );
                    }
                }
                Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Index,
                    if_exists,
                    names,
                    ..
                } => {
                    for name in names {
                        let index_name = last_str(&name);

                        // Find the index
                        let index_exists = builder.indices_mut().iter().any(|(idx, _)| {
                            idx.attribute().name.as_ref().is_some_and(|n| last_str(n) == index_name)
                        });

                        if !index_exists {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropIndexNotFound {
                                index_name: index_name.to_string(),
                            });
                        }

                        // Remove from builder's indices list
                        builder.indices_mut().retain(|(idx, _)| {
                            idx.attribute().name.as_ref().is_none_or(|n| last_str(n) != index_name)
                        });

                        // Remove from table metadata
                        for (_, table_meta) in builder.tables_mut() {
                            table_meta.retain_indices(|idx| {
                                idx.attribute()
                                    .name
                                    .as_ref()
                                    .is_none_or(|n| last_str(n) != index_name)
                            });
                        }
                    }
                }
                Statement::CreateTrigger(create_trigger) => {
                    let table_name = last_str(&create_trigger.table_name);
                    let table_exists =
                        builder.resolve_table_object_name(&create_trigger.table_name)?.is_some();

                    if !table_exists {
                        return Err(crate::errors::Error::TableNotFoundForTrigger {
                            table_name: table_name.to_string(),
                            trigger_name: last_str(&create_trigger.name).to_string(),
                        });
                    }

                    if let Some(exec_body) = &create_trigger.exec_body {
                        let Some((function_name, function_quoted)) =
                            object_name_last_part(&exec_body.func_desc.name)
                        else {
                            return Err(crate::errors::Error::FunctionNotFoundForTrigger {
                                function_name: last_str(&exec_body.func_desc.name).to_string(),
                                trigger_name: last_str(&create_trigger.name).to_string(),
                            });
                        };
                        let function_exists = builder.function_arc_vec().iter().any(|f| {
                            identifiers_match(
                                f.name(),
                                f.name_is_quoted(),
                                function_name,
                                function_quoted,
                            )
                        });

                        if !function_exists {
                            return Err(crate::errors::Error::FunctionNotFoundForTrigger {
                                function_name: function_name.to_string(),
                                trigger_name: last_str(&create_trigger.name).to_string(),
                            });
                        }
                    }

                    builder = builder.add_trigger(Arc::new(create_trigger), ());
                }
                Statement::DropTrigger(drop_trigger) => {
                    let trigger_name = last_str(&drop_trigger.trigger_name);

                    // Find the trigger
                    let trigger_exists =
                        builder.triggers().iter().any(|(t, ())| last_str(&t.name) == trigger_name);

                    if !trigger_exists {
                        if drop_trigger.if_exists {
                            continue;
                        }
                        return Err(crate::errors::Error::DropTriggerNotFound {
                            trigger_name: trigger_name.to_string(),
                        });
                    }

                    // Remove the trigger
                    builder.triggers_mut().retain(|(t, ())| last_str(&t.name) != trigger_name);
                }
                Statement::DropPolicy(drop_policy) => {
                    let policy_name = drop_policy.name.value.as_str();

                    // Find the policy
                    let policy_exists =
                        builder.policies().iter().any(|(p, _)| p.name.value == policy_name);

                    if !policy_exists {
                        if drop_policy.if_exists {
                            continue;
                        }
                        return Err(crate::errors::Error::DropPolicyNotFound {
                            policy_name: policy_name.to_string(),
                        });
                    }

                    // Remove the policy
                    builder.policies_mut().retain(|(p, _)| p.name.value != policy_name);
                }
                Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Role,
                    if_exists,
                    names,
                    ..
                } => {
                    // Note: DROP ROLE doesn't support CASCADE/RESTRICT in PostgreSQL syntax.
                    // We always use RESTRICT semantics (fail if role is referenced).
                    for name in names {
                        let Some(role_ident) = object_name_last_identifier(&name) else {
                            continue;
                        };
                        let role_name = role_ident.value.as_str();
                        let role_quoted = role_ident.quote_style.is_some();

                        // Check if role exists
                        let role_exists = builder
                            .roles()
                            .iter()
                            .any(|(role, ())| role_matches_lookup_ident(role, role_ident));

                        if !role_exists {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropRoleNotFound {
                                role_name: role_name.to_string(),
                            });
                        }

                        // Check for references from grants
                        if builder.is_role_referenced(role_name, role_quoted) {
                            return Err(crate::errors::Error::RoleReferenced {
                                role_name: role_name.to_string(),
                            });
                        }

                        // Remove the role
                        builder
                            .roles_mut()
                            .retain(|(r, ())| !role_matches_lookup_ident(r, role_ident));
                    }
                }
                Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Schema,
                    if_exists,
                    names,
                    cascade,
                    ..
                } => {
                    for name in names {
                        let schema_name = last_str(&name);
                        let maybe_schema = object_name_last_identifier(&name)
                            .and_then(|ident| builder.resolve_schema_ident(ident));

                        let Some(schema) = maybe_schema else {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropSchemaNotFound {
                                schema_name: schema_name.to_string(),
                            });
                        };
                        let resolved_schema_name = schema.name().to_string();
                        let resolved_schema_quoted = schema.is_quoted();

                        // Check for contained objects unless CASCADE is specified
                        if !cascade
                            && builder
                                .is_schema_non_empty(&resolved_schema_name, resolved_schema_quoted)
                        {
                            return Err(crate::errors::Error::SchemaNotEmpty {
                                schema_name: resolved_schema_name.clone(),
                            });
                        }

                        // If CASCADE, remove all tables in the schema first
                        if cascade {
                            use crate::traits::TableLike;
                            let tables_to_remove: Vec<_> = builder
                                .tables()
                                .iter()
                                .filter(|(t, _)| {
                                    t.table_schema().is_some_and(|table_schema| {
                                        identifiers_match(
                                            table_schema,
                                            t.table_schema_is_quoted(),
                                            &resolved_schema_name,
                                            resolved_schema_quoted,
                                        )
                                    })
                                })
                                .map(|(t, _)| {
                                    (
                                        t.table_name().to_string(),
                                        t.table_name_is_quoted(),
                                        t.table_schema().map(str::to_string),
                                        t.table_schema_is_quoted(),
                                    )
                                })
                                .collect();

                            for (
                                table_name,
                                table_name_quoted,
                                table_schema_name,
                                table_schema_quoted,
                            ) in tables_to_remove
                            {
                                builder.remove_table(
                                    &table_name,
                                    table_name_quoted,
                                    table_schema_name.as_deref(),
                                    table_schema_quoted,
                                );
                            }
                        }

                        // Remove the schema
                        builder.schemas_mut().retain(|(s, ())| {
                            !identifiers_match(
                                s.name(),
                                s.is_quoted(),
                                &resolved_schema_name,
                                resolved_schema_quoted,
                            )
                        });
                    }
                }
                Statement::CreateIndex(create_index) => {
                    let (index, metadata) = Self::process_create_index(create_index, &builder)?;
                    let resolved_table = index.table();
                    let resolved_table_name = resolved_table.table_name().to_string();
                    let resolved_table_quoted = resolved_table.table_name_is_quoted();
                    let resolved_schema_name = resolved_table.table_schema().map(str::to_string);
                    let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

                    if let Some(entry) = builder.tables_mut().iter_mut().find(|(table, _)| {
                        table_matches_resolved_identity(
                            table.as_ref(),
                            &resolved_table_name,
                            resolved_table_quoted,
                            resolved_schema_name.as_deref(),
                            resolved_schema_quoted,
                        )
                    }) {
                        entry.1.add_index(index.clone());
                    }
                    builder = builder.add_index(index, metadata);
                }
                Statement::AlterTable(alter_table) => {
                    for operation in alter_table.operations {
                        match operation {
                            AlterTableOperation::EnableRowLevelSecurity => {
                                builder = Self::alter_table_metadata(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |metadata| metadata.set_rls_enabled(true),
                                )?;
                            }
                            AlterTableOperation::DisableRowLevelSecurity => {
                                builder = Self::alter_table_metadata(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |metadata| metadata.set_rls_enabled(false),
                                )?;
                            }
                            AlterTableOperation::ForceRowLevelSecurity => {
                                builder = Self::alter_table_metadata(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |metadata| metadata.set_rls_forced(true),
                                )?;
                            }
                            AlterTableOperation::NoForceRowLevelSecurity => {
                                builder = Self::alter_table_metadata(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |metadata| metadata.set_rls_forced(false),
                                )?;
                            }
                            AlterTableOperation::RenameTable { table_name } => {
                                let new_name = match table_name {
                                    RenameTableNameKind::As(name)
                                    | RenameTableNameKind::To(name) => name,
                                };
                                builder = Self::rename_table_checked(
                                    builder,
                                    &alter_table.name,
                                    new_name,
                                    alter_table.if_exists,
                                )?;
                            }
                            AlterTableOperation::AddConstraint { constraint, .. } => {
                                builder = Self::alter_table_constraints(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |_, constraints| {
                                        constraints.push(constraint);
                                        Ok(())
                                    },
                                )?;
                            }
                            AlterTableOperation::DropConstraint { if_exists, name, .. } => {
                                builder = Self::alter_table_constraints(
                                    builder,
                                    &alter_table.name,
                                    alter_table.if_exists,
                                    |table, constraints| {
                                        let declared = constraints.len();
                                        constraints.retain(|constraint| {
                                            !table_constraint_has_name(constraint, &name)
                                        });
                                        if constraints.len() == declared && !if_exists {
                                            return Err(
                                                crate::errors::Error::DropConstraintNotFound {
                                                    table_name: table.name.to_string(),
                                                    constraint_name: name.value.clone(),
                                                },
                                            );
                                        }
                                        Ok(())
                                    },
                                )?;
                            }
                            _ => {}
                        }
                    }
                }
                Statement::CreateTable(create_table) => {
                    builder = Self::ingest_table_node(
                        builder,
                        Arc::new(create_table),
                        TableMetadata::default(),
                    )?;
                }
                Statement::CreatePolicy(policy) => {
                    let using_functions = if let Some(using_expr) = &policy.using {
                        functions_in_expression::functions_in_expression::<Self>(
                            using_expr,
                            builder.function_arc_vec().as_slice(),
                        )
                    } else {
                        Vec::new()
                    };

                    let check_functions = if let Some(check_expr) = &policy.with_check {
                        functions_in_expression::functions_in_expression::<Self>(
                            check_expr,
                            builder.function_arc_vec().as_slice(),
                        )
                    } else {
                        Vec::new()
                    };

                    let metadata = PolicyMetadata::new(using_functions, check_functions);
                    builder = builder.add_policy(Arc::new(policy), metadata);
                }
                Statement::CreateRole(create_role) => {
                    builder = builder.add_role(Arc::new(create_role), ());
                }
                Statement::CreateSchema { schema_name, if_not_exists, .. } => {
                    let (name, quoted, authorization) = match &schema_name {
                        SchemaName::Simple(name) => {
                            let schema_ident = object_name_last_identifier(name);
                            (
                                schema_ident.map_or_else(
                                    || last_str(name).to_string(),
                                    |ident| ident.value.clone(),
                                ),
                                schema_ident.is_some_and(|ident| ident.quote_style.is_some()),
                                None,
                            )
                        }
                        SchemaName::UnnamedAuthorization(auth) => {
                            // CREATE SCHEMA AUTHORIZATION admin creates schema named "admin"
                            (
                                auth.value.clone(),
                                auth.quote_style.is_some(),
                                Some(auth.value.clone()),
                            )
                        }
                        SchemaName::NamedAuthorization(name, auth) => {
                            let schema_ident = object_name_last_identifier(name);
                            (
                                schema_ident.map_or_else(
                                    || last_str(name).to_string(),
                                    |ident| ident.value.clone(),
                                ),
                                schema_ident.is_some_and(|ident| ident.quote_style.is_some()),
                                Some(auth.value.clone()),
                            )
                        }
                    };

                    // Check if schema already exists
                    let schema_exists = builder
                        .schemas()
                        .iter()
                        .any(|(s, ())| identifiers_match(s.name(), s.is_quoted(), &name, quoted));

                    if schema_exists {
                        if !if_not_exists {
                            return Err(crate::errors::Error::SchemaAlreadyExists {
                                schema_name: name.clone(),
                            });
                        }
                        // IF NOT EXISTS - skip adding duplicate
                    } else {
                        let schema = match authorization {
                            Some(auth) => Schema::with_authorization_and_quoted(name, auth, quoted),
                            None => Schema::with_quoted(name, quoted),
                        };
                        builder = builder.add_schema(Arc::new(schema), ());
                    }
                }
                Statement::Grant(grant) => {
                    if options.grant_resolution() == GrantResolution::ClosedWorld {
                        validate_grant_against_builder(&builder, &grant)?;
                    }

                    builder = builder.add_table_grant(Arc::new(grant.clone()), ());
                    builder = builder.add_column_grant(Arc::new(grant), ());
                }
                Statement::Revoke(revoke) => {
                    // Apply revoke semantics to both canonical grant stores.
                    let table_application =
                        apply_revoke_to_grant_store(builder.table_grants_mut(), &revoke);
                    let column_application =
                        apply_revoke_to_grant_store(builder.column_grants_mut(), &revoke);

                    // We fail fast on revoke shapes that this model cannot
                    // represent (for example column-subset revoke from a
                    // table-wide action grant).
                    if table_application.has_unsupported_column_scoped_revoke
                        || column_application.has_unsupported_column_scoped_revoke
                    {
                        return Err(crate::errors::Error::UnsupportedRevoke {
                            statement: revoke.to_string(),
                            reason: "column-scoped REVOKE against a table-wide action grant is \
                                     not representable in this model"
                                .to_string(),
                        });
                    }

                    // An open world cannot tell a revoke of a privilege the
                    // input never granted (`pg_dump` emits one per function
                    // whose default execute privilege was revoked) from a
                    // revoke of a grant it failed to record.
                    if options.grant_resolution() == GrantResolution::ClosedWorld
                        && !table_application.matched_any
                        && !column_application.matched_any
                    {
                        return Err(crate::errors::Error::RevokeNotFound(format!(
                            "No matching grant found for REVOKE: {revoke}"
                        )));
                    }
                }
                Statement::Set(sqlparser::ast::Set::SetTimeZone { local, value }) => {
                    if local {
                        builder = builder.timezone("LOCAL".to_string());
                    } else if let Expr::Value(ValueWithSpan {
                        value: Value::SingleQuotedString(lit),
                        ..
                    }) = value
                    {
                        builder = builder.timezone(lit);
                    }
                    // Ignore unsupported SET TIME ZONE expressions (e.g.,
                    // binary ops)
                }
                Statement::RenameTable(renames) => {
                    for rename in renames {
                        builder = Self::rename_table_checked(
                            builder,
                            &rename.old_name,
                            rename.new_name,
                            false,
                        )?;
                    }
                }
                Statement::AlterPolicy(AlterPolicy { name, table_name, operation }) => {
                    use crate::traits::PolicyLike;

                    let policy_name = &name.value;
                    let _table_name = last_str(&table_name);

                    // Check if policy exists
                    let policy_exists =
                        builder.policies().iter().any(|(p, _)| p.name() == policy_name);

                    if !policy_exists {
                        return Err(crate::errors::Error::AlterPolicyNotFound {
                            policy_name: policy_name.clone(),
                        });
                    }

                    match operation {
                        AlterPolicyOperation::Rename { new_name } => {
                            // Update the policy name
                            let policies = builder.policies_mut();
                            if let Some(idx) =
                                policies.iter().position(|(p, _)| p.name() == policy_name)
                            {
                                let (old_policy, meta) = policies.remove(idx);
                                let mut new_policy = (*old_policy).clone();
                                new_policy.name = new_name.clone();
                                policies.push((Arc::new(new_policy), meta));
                            }
                        }
                        AlterPolicyOperation::Apply { .. } => {
                            // For Apply operations (changing USING/WITH CHECK
                            // expressions),
                            // we would need to update the policy metadata with
                            // new function refs.
                            // This is complex and would require re-parsing
                            // expressions. For now,
                            // we skip detailed tracking of expression changes.
                        }
                    }
                }
                Statement::AlterSchema(AlterSchema { name, if_exists, operations }) => {
                    let schema_name = last_str(&name);

                    // Check if schema exists
                    let resolved_schema = object_name_last_identifier(&name)
                        .and_then(|ident| builder.resolve_schema_ident(ident));

                    let Some(resolved_schema) = resolved_schema else {
                        if if_exists {
                            continue;
                        }
                        return Err(crate::errors::Error::AlterSchemaNotFound {
                            schema_name: schema_name.to_string(),
                        });
                    };

                    let mut current_schema_name = resolved_schema.name().to_string();
                    let mut current_schema_quoted = resolved_schema.is_quoted();

                    for operation in &operations {
                        match operation {
                            AlterSchemaOperation::Rename { name: new_name } => {
                                let new_schema_ident = object_name_last_identifier(new_name);
                                let new_schema_name = new_schema_ident.map_or_else(
                                    || last_str(new_name).to_string(),
                                    |ident| ident.value.clone(),
                                );
                                let new_schema_quoted = new_schema_ident
                                    .is_some_and(|ident| ident.quote_style.is_some());
                                let schemas = builder.schemas_mut();
                                let Some(idx) = schemas.iter().position(|(schema, ())| {
                                    identifiers_match(
                                        schema.name(),
                                        schema.is_quoted(),
                                        &current_schema_name,
                                        current_schema_quoted,
                                    )
                                }) else {
                                    continue;
                                };

                                let duplicate_exists = schemas.iter().enumerate().any(
                                    |(existing_idx, (schema, ()))| {
                                        existing_idx != idx
                                            && identifiers_match(
                                                schema.name(),
                                                schema.is_quoted(),
                                                &new_schema_name,
                                                new_schema_quoted,
                                            )
                                    },
                                );
                                if duplicate_exists {
                                    return Err(crate::errors::Error::SchemaAlreadyExists {
                                        schema_name: new_schema_name.clone(),
                                    });
                                }

                                let (old_schema, ()) = schemas.remove(idx);
                                let new_schema = if let Some(auth) = old_schema.authorization() {
                                    Schema::with_authorization_and_quoted(
                                        new_schema_name.clone(),
                                        auth.to_string(),
                                        new_schema_quoted,
                                    )
                                } else {
                                    Schema::with_quoted(new_schema_name.clone(), new_schema_quoted)
                                };
                                schemas.push((Arc::new(new_schema), ()));
                                schemas.sort_by(|(a, ()), (b, ())| a.name().cmp(b.name()));
                                current_schema_name = new_schema_name;
                                current_schema_quoted = new_schema_quoted;
                            }
                            AlterSchemaOperation::OwnerTo { owner } => {
                                // Update the authorization
                                let owner_name = match owner {
                                    sqlparser::ast::Owner::Ident(ident) => ident.value.clone(),
                                    sqlparser::ast::Owner::CurrentRole
                                    | sqlparser::ast::Owner::CurrentUser
                                    | sqlparser::ast::Owner::SessionUser => continue,
                                };
                                let schemas = builder.schemas_mut();
                                let Some(idx) = schemas.iter().position(|(schema, ())| {
                                    identifiers_match(
                                        schema.name(),
                                        schema.is_quoted(),
                                        &current_schema_name,
                                        current_schema_quoted,
                                    )
                                }) else {
                                    continue;
                                };
                                let (old_schema, ()) = schemas.remove(idx);
                                let new_schema = Schema::with_authorization_and_quoted(
                                    old_schema.name().to_string(),
                                    owner_name,
                                    old_schema.is_quoted(),
                                );
                                schemas.push((Arc::new(new_schema), ()));
                            }
                            // Other operations don't affect our schema tracking
                            AlterSchemaOperation::SetDefaultCollate { .. }
                            | AlterSchemaOperation::AddReplica { .. }
                            | AlterSchemaOperation::DropReplica { .. }
                            | AlterSchemaOperation::SetOptionsParens { .. } => {}
                        }
                    }
                }
                _ => {
                    // Ignored statements - no schema tracking needed
                }
            }
        }

        Ok(builder.into())
    }

    /// Parses SQL using the specified dialect.
    ///
    /// The dialect type parameter `D` must implement both `Dialect` and
    /// `Default`. This allows calling the method with turbofish syntax to
    /// specify the dialect.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL string to parse.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be parsed or if there are
    /// validation errors.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
    ///
    /// // Using GenericDialect
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE users (id INT PRIMARY KEY);")?;
    /// assert_eq!(db.table(None, "users").unwrap().table_name(), "users");
    ///
    /// // Using PostgreSqlDialect
    /// let db = ParserDB::parse::<PostgreSqlDialect>("CREATE ROLE admin SUPERUSER;")?;
    /// assert!(db.role("admin").unwrap().is_superuser());
    /// # Ok(())
    /// # }
    /// ```
    pub fn parse<D: Dialect + Default + 'static>(sql: &str) -> Result<Self, crate::errors::Error> {
        Self::parse_with_options::<D>(sql, ParseOptions::default())
    }

    /// Same as [`Self::parse`] but under caller-chosen [`ParseOptions`].
    pub(crate) fn parse_with_options<D: Dialect + Default + 'static>(
        sql: &str,
        options: ParseOptions,
    ) -> Result<Self, crate::errors::Error> {
        let dialect = D::default();
        let mut parser = Parser::new(&dialect).try_with_sql(sql)?;
        let statements = parser.parse_statements()?;
        let mut db = Self::from_statements_with_options(
            statements,
            "unknown_catalog".to_string(),
            SqlparserDialect::of::<D>(),
            options,
        )?;

        if let Ok(documentation) = SqlDoc::builder_from_str(sql).build::<D>() {
            for (table, metadata) in db.tables_metadata_mut() {
                if let Ok(table_doc) = documentation.table(table.table_name(), table.table_schema())
                {
                    metadata.set_doc(table_doc.to_owned());
                }
            }
        }
        Ok(db)
    }

    /// Constructs a `ParserDB` from a git URL.
    ///
    /// # Example
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let url = "https://github.com/earth-metabolome-initiative/asset-procedure-schema.git";
    /// let db = ParserDB::from_git_url::<PostgreSqlDialect>(url).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the repository cannot be cloned or if the SQL files
    /// cannot be parsed.
    #[cfg(feature = "git")]
    pub fn from_git_url<D: Dialect + Default>(url: &str) -> Result<Self, crate::errors::Error> {
        let dir = tempfile::tempdir()?;
        Repository::clone(url, dir.path())?;
        Self::from_path::<D>(dir.path())
    }

    /// Constructs a `ParserDB` from a git URL using a specific dialect.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository cannot be cloned or if the SQL files
    /// cannot be parsed.
    #[cfg(feature = "git")]
    pub fn from_git_url_with_dialect<D: Dialect + Default>(
        url: &str,
    ) -> Result<Self, crate::errors::Error> {
        let dir = tempfile::tempdir()?;
        Repository::clone(url, dir.path())?;
        Self::from_path::<D>(dir.path())
    }

    /// Parses SQL from a file or directory path.
    ///
    /// If the path is a directory, all `.sql` files (except `down.sql`) will be
    /// parsed recursively.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to a SQL file or directory containing SQL files.
    /// * `dialect` - The SQL dialect to use for parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if the path doesn't exist, files can't be read, or
    /// parsing fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::from_path::<PostgreSqlDialect>(Path::new("migrations/")).unwrap();
    /// ```
    #[cfg(feature = "std")]
    pub fn from_path<D: Dialect + Default>(path: &Path) -> Result<Self, crate::errors::Error> {
        Self::from_paths::<D>(&[path])
    }

    /// Parses SQL from multiple file or directory paths.
    ///
    /// # Arguments
    ///
    /// * `paths` - A slice of paths to SQL files or directories.
    /// * `dialect` - The SQL dialect to use for parsing.
    ///
    /// # Errors
    ///
    /// Returns an error if any path doesn't exist, files can't be read, or
    /// parsing fails.
    #[cfg(feature = "std")]
    pub fn from_paths<D: Dialect + Default>(paths: &[&Path]) -> Result<Self, crate::errors::Error> {
        Self::from_paths_with_options::<D>(paths, ParseOptions::default())
    }

    /// Same as [`Self::from_paths`] but under caller-chosen [`ParseOptions`].
    #[cfg(feature = "std")]
    pub(crate) fn from_paths_with_options<D: Dialect + Default>(
        paths: &[&Path],
        options: ParseOptions,
    ) -> Result<Self, crate::errors::Error> {
        let mut statements = Vec::new();
        let mut sql_str: Vec<(String, PathBuf)> = Vec::new();

        for path in paths {
            if !path.exists() {
                return Err(ParserError::TokenizerError(format!(
                    "Path does not exist: {}",
                    path.display()
                ))
                .into());
            }

            let mut sql_paths = search_sql_documents(path);
            sql_paths.sort_unstable();

            for sql_path in sql_paths {
                let sql_content = std::fs::read_to_string(&sql_path)
                    .map_err(|e| ParserError::TokenizerError(e.to_string()))
                    .map_err(|e| {
                        crate::errors::Error::SqlParserError {
                            error: e,
                            file: Some(sql_path.clone()),
                        }
                    })?;

                let dialect = D::default();
                let mut parser = Parser::new(&dialect).try_with_sql(&sql_content).map_err(|e| {
                    crate::errors::Error::SqlParserError { error: e, file: Some(sql_path.clone()) }
                })?;
                statements.extend(parser.parse_statements().map_err(|e| {
                    crate::errors::Error::SqlParserError { error: e, file: Some(sql_path.clone()) }
                })?);
                sql_str.push((sql_content, sql_path));
            }
        }

        let mut db = Self::from_statements_with_options(
            statements,
            "unknown_catalog".to_string(),
            SqlparserDialect::default(),
            options,
        )?;

        if let Ok(documentation) = SqlDoc::builder_from_strs_with_paths(&sql_str).build::<D>() {
            for (table, metadata) in db.tables_metadata_mut() {
                if let Ok(table_doc) = documentation.table(table.table_name(), table.table_schema())
                {
                    metadata.set_doc(table_doc.to_owned());
                }
            }
        }
        Ok(db)
    }
}

#[cfg(feature = "std")]
fn search_sql_documents(path: &Path) -> Vec<PathBuf> {
    let mut sql_files = Vec::new();
    if path.is_dir() {
        let Ok(entries) = std::fs::read_dir(path) else {
            return sql_files;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                sql_files.extend(search_sql_documents(&path));
            } else if let Some(extension) = path.extension()
                && extension == "sql"
                && path.file_name().is_some_and(|name| name != "down.sql")
            {
                sql_files.push(path);
            }
        }
    } else if let Some(extension) = path.extension()
        && extension == "sql"
    {
        sql_files.push(path.to_path_buf());
    }
    sql_files
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::{
        errors::{Error, LookupError},
        traits::{DatabaseLike, TableLike},
    };

    mod identifier_aware_lookup {
        use sqlparser::{
            ast::{Ident, ObjectName, ObjectNamePart},
            dialect::PostgreSqlDialect,
        };

        use super::*;

        fn ident(value: &str, quoted: bool) -> Ident {
            if quoted { Ident::with_quote('"', value) } else { Ident::new(value) }
        }

        fn object_name(parts: &[(&str, bool)]) -> ObjectName {
            ObjectName(
                parts
                    .iter()
                    .map(|(value, quoted)| ObjectNamePart::Identifier(ident(value, *quoted)))
                    .collect(),
            )
        }

        fn parse_postgres(sql: &str) -> ParserDB {
            ParserDB::parse::<PostgreSqlDialect>(sql).expect("Failed to parse PostgreSQL SQL")
        }

        #[test]
        fn quoted_table_lookup_requires_exact_case() {
            let db = parse_postgres("CREATE TABLE \"Camel\" (id INT);");

            assert!(
                db.resolve_table_object_name(&object_name(&[("Camel", true)]))
                    .expect("Lookup should succeed")
                    .is_some()
            );
            assert!(
                db.resolve_table_object_name(&object_name(&[("camel", true)]))
                    .expect("Lookup should succeed")
                    .is_none()
            );
            assert!(
                db.resolve_table_object_name(&object_name(&[("camel", false)]))
                    .expect("Lookup should succeed")
                    .is_none()
            );
        }

        #[test]
        fn unquoted_table_lookup_resolves_via_folding() {
            let db = parse_postgres("CREATE TABLE Foo (id INT);");

            assert!(
                db.resolve_table_object_name(&object_name(&[("foo", false)]))
                    .expect("Lookup should succeed")
                    .is_some()
            );
            assert!(
                db.resolve_table_object_name(&object_name(&[("FOO", false)]))
                    .expect("Lookup should succeed")
                    .is_some()
            );
            assert!(
                db.resolve_table_object_name(&object_name(&[("foo", true)]))
                    .expect("Lookup should succeed")
                    .is_some()
            );
            assert!(
                db.resolve_table_object_name(&object_name(&[("Foo", true)]))
                    .expect("Lookup should succeed")
                    .is_none()
            );
        }

        #[test]
        fn schema_ident_resolution_handles_quoted_and_unquoted() {
            let db = parse_postgres(
                r#"
                CREATE SCHEMA Foo;
                CREATE SCHEMA "Bar";
                "#,
            );

            assert!(db.resolve_schema_ident(&ident("foo", false)).is_some());
            assert!(db.resolve_schema_ident(&ident("FOO", false)).is_some());
            assert!(db.resolve_schema_ident(&ident("foo", true)).is_some());
            assert!(db.resolve_schema_ident(&ident("Foo", true)).is_none());

            assert!(db.resolve_schema_ident(&ident("Bar", true)).is_some());
            assert!(db.resolve_schema_ident(&ident("bar", false)).is_none());
        }

        #[test]
        fn alter_table_rls_lookup_uses_resolver_rules() {
            let db = parse_postgres(
                r"
                CREATE TABLE Foo (id INT);
                ALTER TABLE FOO ENABLE ROW LEVEL SECURITY;
                ",
            );
            let foo =
                db.table(None, "foo").expect("Expected `foo` table to exist after ALTER TABLE");
            assert!(
                foo.has_row_level_security(&db).expect("rls check"),
                "Unquoted ALTER TABLE lookup should resolve via identifier folding"
            );

            let result = ParserDB::parse::<PostgreSqlDialect>(
                r#"
                CREATE TABLE Foo (id INT);
                ALTER TABLE "Foo" ENABLE ROW LEVEL SECURITY;
                "#,
            );
            assert!(
                matches!(
                    result,
                    Err(Error::AlterTableNotFound { table_name }) if table_name == "Foo"
                ),
                "Quoted ALTER TABLE name should not match unquoted table with different case"
            );
        }

        #[test]
        fn grant_table_lookup_uses_resolver_rules() {
            let sql = r"
                CREATE TABLE Foo (id INT);
                CREATE ROLE app_role;
                GRANT SELECT ON FOO TO app_role;
            ";
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(result.is_ok());

            let sql = r#"
                CREATE TABLE Foo (id INT);
                CREATE ROLE app_role;
                GRANT SELECT ON "Foo" TO app_role;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::TableNotFoundForGrant { table_name }) if table_name == "Foo"
            ));
        }

        #[test]
        fn create_index_attaches_to_correct_schema_table() {
            let db = parse_postgres(
                r"
                CREATE SCHEMA s1;
                CREATE SCHEMA s2;
                CREATE TABLE s1.t (id INT);
                CREATE TABLE s2.t (id INT);
                CREATE INDEX idx_s2_t_id ON s2.t (id);
                ",
            );

            let s1_t = db
                .resolve_table_object_name(&object_name(&[("s1", false), ("t", false)]))
                .expect("Lookup should succeed")
                .expect("Expected table s1.t to exist");
            let s2_t = db
                .resolve_table_object_name(&object_name(&[("s2", false), ("t", false)]))
                .expect("Lookup should succeed")
                .expect("Expected table s2.t to exist");

            assert_eq!(s1_t.indices(&db).expect("indices").count(), 0);
            assert_eq!(s2_t.indices(&db).expect("indices").count(), 1);
        }

        #[test]
        fn create_index_attachment_respects_quoted_schema_and_table_identity() {
            let db = parse_postgres(
                r#"
                CREATE SCHEMA s;
                CREATE SCHEMA "S";
                CREATE TABLE s.t (id INT);
                CREATE TABLE "S"."T" (id INT);
                CREATE INDEX idx_quoted_t ON "S"."T" (id);
                "#,
            );

            let unquoted = db
                .resolve_table_object_name(&object_name(&[("s", false), ("t", false)]))
                .expect("Lookup should succeed")
                .expect("Expected table s.t to exist");
            let quoted = db
                .resolve_table_object_name(&object_name(&[("S", true), ("T", true)]))
                .expect("Lookup should succeed")
                .expect("Expected table \"S\".\"T\" to exist");

            assert_eq!(unquoted.indices(&db).expect("indices").count(), 0);
            assert_eq!(quoted.indices(&db).expect("indices").count(), 1);
        }

        #[test]
        fn rename_table_lookup_uses_resolver_rules() {
            let sql = r"
                CREATE TABLE Foo (id INT);
                ALTER TABLE FOO RENAME TO bar;
            ";
            let db = parse_postgres(sql);
            assert!(db.table(None, "foo").is_none());
            assert!(db.table(None, "bar").is_some());

            let sql = r#"
                CREATE TABLE Foo (id INT);
                ALTER TABLE "Foo" RENAME TO bar;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::RenameTableNotFound { table_name }) if table_name == "Foo"
            ));
        }

        #[test]
        fn rename_table_statement_lookup_uses_resolver_rules() {
            let sql = r"
                CREATE TABLE Foo (id INT);
                RENAME TABLE FOO TO bar;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql)
                .expect("Expected unquoted RENAME TABLE lookup to resolve");
            assert!(db.table(None, "foo").is_none());
            assert!(db.table(None, "bar").is_some());

            let sql = r#"
                CREATE TABLE Foo (id INT);
                RENAME TABLE "Foo" TO bar;
            "#;
            let result = ParserDB::parse::<GenericDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::RenameTableNotFound { table_name }) if table_name == "Foo"
            ));
        }

        #[test]
        fn alter_schema_rename_rejects_semantic_duplicate() {
            let sql = r"
                CREATE SCHEMA foo;
                CREATE SCHEMA bar;
                ALTER SCHEMA bar RENAME TO FOO;
            ";

            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::SchemaAlreadyExists { schema_name })
                    if schema_name.eq_ignore_ascii_case("foo")
            ));
        }

        #[test]
        fn alter_schema_rename_rejects_quoted_unquoted_equivalent_duplicate() {
            let sql = r#"
                CREATE SCHEMA foo;
                CREATE SCHEMA bar;
                ALTER SCHEMA bar RENAME TO "foo";
            "#;

            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::SchemaAlreadyExists { schema_name }) if schema_name == "foo"
            ));
        }

        #[test]
        fn implicit_public_helper_handles_mixed_public_cases() {
            let db = parse_postgres(
                r#"
                CREATE TABLE public.foo (id INT);
                CREATE SCHEMA "Public";
                CREATE TABLE "Public".bar (id INT);
                "#,
            );

            assert!(
                db.resolve_table_object_name(&object_name(&[("foo", false)]))
                    .expect("Lookup should succeed")
                    .is_none()
            );

            let resolved = db
                .resolve_table_object_name_with_implicit_public(&object_name(&[("foo", false)]))
                .expect("Lookup should succeed");
            let resolved = resolved.expect("Expected implicit public fallback to resolve");
            assert_eq!(
                resolved.table_schema(),
                Some("public"),
                "Unqualified lookup should fallback to schema public"
            );

            assert!(
                db.resolve_table_object_name_with_implicit_public(&object_name(&[("bar", false)]))
                    .expect("Lookup should succeed")
                    .is_none()
            );

            assert!(
                db.resolve_table_object_name(&object_name(&[("Public", true), ("bar", false)]))
                    .expect("Lookup should succeed")
                    .is_some()
            );
        }

        #[test]
        fn invalid_object_name_is_reported() {
            let db = parse_postgres("CREATE TABLE t (id INT);");
            let invalid = object_name(&[("a", false), ("b", false), ("c", false)]);

            let result = db.resolve_table_object_name(&invalid);
            assert!(matches!(
                result,
                Err(LookupError::InvalidObjectName { object_name, .. }) if object_name == "a.b.c"
            ));
        }

        #[test]
        fn ambiguous_unqualified_and_public_tables_fail_at_build_time() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE TABLE public.t (id INT);
            ";

            let result = ParserDB::parse::<PostgreSqlDialect>(sql);
            assert!(matches!(
                result,
                Err(Error::IdentifierLookupError(LookupError::TableLookupConflict {
                    table,
                    conflicting_table
                })) if table == "public.t" && conflicting_table == "t"
            ));
        }
    }

    mod parser_variant_compatibility {
        use sqlparser::dialect::{MsSqlDialect, PostgreSqlDialect};

        use super::*;

        #[test]
        fn waitfor_statement_is_ignored_without_breaking_parse() {
            let sql = "
                WAITFOR DELAY '00:00:00';
                CREATE TABLE t (id INT);
            ";
            let db = ParserDB::parse::<MsSqlDialect>(sql).expect("WAITFOR should be ignored");
            assert!(db.table(None, "t").is_some());
        }

        #[test]
        fn comment_on_role_statement_is_ignored_without_breaking_parse() {
            let sql = "
                CREATE ROLE app_role;
                COMMENT ON ROLE app_role IS 'Application role';
            ";
            let db =
                ParserDB::parse::<PostgreSqlDialect>(sql).expect("COMMENT ON ROLE should parse");
            assert!(db.role("app_role").is_some());
        }
    }

    mod drop_function_errors {
        use super::*;

        #[test]
        fn test_drop_function_not_found_error_type() {
            let sql = "DROP FUNCTION nonexistent_func;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropFunctionNotFound { function_name }) if function_name == "nonexistent_func"
            ));
        }

        #[test]
        fn test_drop_function_referenced_error_type() {
            let sql = r"
                CREATE FUNCTION is_valid(x INT) RETURNS BOOLEAN AS 'SELECT x > 0;';
                CREATE TABLE t (id INT CHECK (is_valid(id)));
                DROP FUNCTION is_valid;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::FunctionReferenced { function_name }) if function_name == "is_valid"
            ));
        }

        #[test]
        fn test_drop_function_if_exists_not_found_succeeds() {
            let sql = "DROP FUNCTION IF EXISTS nonexistent_func;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(result.is_ok());
        }

        #[test]
        fn test_drop_function_if_exists_referenced_still_fails() {
            let sql = r"
                CREATE FUNCTION is_valid(x INT) RETURNS BOOLEAN AS 'SELECT x > 0;';
                CREATE TABLE t (id INT CHECK (is_valid(id)));
                DROP FUNCTION IF EXISTS is_valid;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            // IF EXISTS doesn't bypass the reference check
            assert!(matches!(
                result,
                Err(Error::FunctionReferenced { function_name }) if function_name == "is_valid"
            ));
        }

        #[test]
        fn test_drop_quoted_function_succeeds() {
            let sql = r#"
                CREATE FUNCTION "FooBar"() RETURNS INT AS 'SELECT 1;';
                DROP FUNCTION "FooBar";
            "#;
            let db = ParserDB::parse::<GenericDialect>(sql)
                .expect("Quoted DROP FUNCTION should match quoted CREATE FUNCTION");

            assert!(db.function("\"FooBar\"").is_none());
        }

        #[test]
        fn test_drop_unquoted_does_not_match_quoted_function() {
            let sql = r#"
                CREATE FUNCTION "FooBar"() RETURNS INT AS 'SELECT 1;';
                DROP FUNCTION foobar;
            "#;
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropFunctionNotFound { function_name }) if function_name == "foobar"
            ));
        }
    }

    mod drop_table_errors {
        use super::*;

        #[test]
        fn test_drop_table_not_found_error_type() {
            let sql = "DROP TABLE nonexistent_table;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropTableNotFound { table_name }) if table_name == "nonexistent_table"
            ));
        }

        #[test]
        fn test_drop_table_referenced_error_type() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
                DROP TABLE parent;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::TableReferenced { table_name }) if table_name == "parent"
            ));
        }

        #[test]
        fn test_drop_table_if_exists_not_found_succeeds() {
            let sql = "DROP TABLE IF EXISTS nonexistent_table;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(result.is_ok());
        }

        #[test]
        fn test_drop_table_if_exists_referenced_still_fails() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
                DROP TABLE IF EXISTS parent;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            // IF EXISTS doesn't bypass the reference check
            assert!(matches!(
                result,
                Err(Error::TableReferenced { table_name }) if table_name == "parent"
            ));
        }

        #[test]
        fn test_drop_table_cascade_bypasses_reference_check() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
                DROP TABLE parent CASCADE;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(result.is_ok());
            let db = result.unwrap();
            assert!(db.table(None, "parent").is_none());
            assert!(db.table(None, "child").is_some());
        }

        #[test]
        fn test_drop_multiple_tables() {
            let sql = r"
                CREATE TABLE t1 (id INT PRIMARY KEY);
                CREATE TABLE t2 (id INT PRIMARY KEY);
                CREATE TABLE t3 (id INT PRIMARY KEY);
                DROP TABLE t1, t2;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(result.is_ok());
            let db = result.unwrap();
            assert!(db.table(None, "t1").is_none());
            assert!(db.table(None, "t2").is_none());
            assert!(db.table(None, "t3").is_some());
        }

        #[test]
        fn test_drop_multiple_tables_one_referenced_fails() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
                CREATE TABLE other (id INT PRIMARY KEY);
                DROP TABLE parent, other;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            // Should fail because parent is referenced
            assert!(matches!(
                result,
                Err(Error::TableReferenced { table_name }) if table_name == "parent"
            ));
        }

        #[test]
        fn test_drop_table_with_same_name_in_other_schema_only_removes_target() {
            let sql = r"
                CREATE SCHEMA s1;
                CREATE SCHEMA s2;
                CREATE TABLE s1.t (id INT PRIMARY KEY);
                CREATE TABLE s2.t (id INT PRIMARY KEY);
                CREATE INDEX idx_s2_t_id ON s2.t (id);
                DROP TABLE s1.t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            assert!(db.table(Some("s1"), "t").is_none());
            let s2_t = db.table(Some("s2"), "t").expect("s2.t should still exist");
            assert_eq!(
                s2_t.indices(&db).expect("indices").count(),
                1,
                "Dropping s1.t must not remove indices attached to s2.t",
            );
        }

        #[test]
        fn test_drop_table_reference_check_is_schema_aware() {
            let sql = r"
                CREATE SCHEMA s1;
                CREATE SCHEMA s2;
                CREATE TABLE s1.parent (id INT PRIMARY KEY);
                CREATE TABLE s1.child (id INT, parent_id INT REFERENCES s1.parent(id));
                CREATE TABLE s2.parent (id INT PRIMARY KEY);
                DROP TABLE s2.parent;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect(
                "References to s1.parent must not block dropping same-name table s2.parent",
            );

            assert!(db.table(Some("s2"), "parent").is_none());
            assert!(db.table(Some("s1"), "parent").is_some());
            assert!(db.table(Some("s1"), "child").is_some());
        }
    }

    mod is_function_used_tests {
        use super::*;

        #[test]
        fn test_is_function_used_returns_false_when_no_references() {
            // Parse SQL with a function but no references
            let sql = r"
                CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1;';
                CREATE TABLE t (id INT);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // The function exists but isn't used by any schema object
            assert!(db.function("my_func").is_some());
        }

        #[test]
        fn test_function_used_by_check_constraint() {
            let sql = r"
                CREATE FUNCTION is_positive(x INT) RETURNS BOOLEAN AS 'SELECT x > 0;';
                CREATE TABLE t (id INT CHECK (is_positive(id)));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Function should exist and be used
            assert!(db.function("is_positive").is_some());

            // Verify dropping it would fail
            let drop_sql = format!("{sql}\nDROP FUNCTION is_positive;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(matches!(result, Err(Error::FunctionReferenced { .. })));
        }

        #[test]
        fn test_function_used_by_policy_using_clause() {
            let sql = r"
                CREATE FUNCTION check_access() RETURNS BOOLEAN AS 'SELECT true;';
                CREATE TABLE t (id INT);
                CREATE POLICY my_policy ON t USING (check_access());
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Function should exist
            assert!(db.function("check_access").is_some());

            // Verify dropping it would fail
            let drop_sql = format!("{sql}\nDROP FUNCTION check_access;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(matches!(result, Err(Error::FunctionReferenced { .. })));
        }

        #[test]
        fn test_function_used_by_policy_with_check_clause() {
            let sql = r"
                CREATE FUNCTION validate() RETURNS BOOLEAN AS 'SELECT true;';
                CREATE TABLE t (id INT);
                CREATE POLICY my_policy ON t WITH CHECK (validate());
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Function should exist
            assert!(db.function("validate").is_some());

            // Verify dropping it would fail
            let drop_sql = format!("{sql}\nDROP FUNCTION validate;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(matches!(result, Err(Error::FunctionReferenced { .. })));
        }

        #[test]
        fn test_function_used_by_trigger() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION trigger_fn() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Function should exist
            assert!(db.function("trigger_fn").is_some());

            // Verify dropping it would fail
            let drop_sql = format!("{sql}\nDROP FUNCTION trigger_fn;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(matches!(result, Err(Error::FunctionReferenced { .. })));
        }

        #[test]
        fn test_function_used_by_schema_qualified_call() {
            let sql = r"
                CREATE SCHEMA s;
                CREATE FUNCTION check_access() RETURNS BOOLEAN AS 'SELECT true;';
                CREATE TABLE t (id INT CHECK (s.check_access()));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(db.function("check_access").is_some());

            let drop_sql = format!("{sql}\nDROP FUNCTION check_access;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(matches!(result, Err(Error::FunctionReferenced { .. })));
        }

        #[test]
        fn test_quoted_function_used_by_check_constraint() {
            let sql = r#"
                CREATE FUNCTION "FooBar"(x INT) RETURNS BOOLEAN AS 'SELECT x > 0;';
                CREATE TABLE t (id INT CHECK ("FooBar"(id)));
                DROP FUNCTION "FooBar";
            "#;
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::FunctionReferenced { function_name }) if function_name == "FooBar"
            ));
        }

        #[test]
        fn test_quoted_function_used_by_policy() {
            let sql = r#"
                CREATE FUNCTION "FooBar"() RETURNS BOOLEAN AS 'SELECT true;';
                CREATE TABLE t (id INT);
                CREATE POLICY p ON t USING ("FooBar"());
                DROP FUNCTION "FooBar";
            "#;
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::FunctionReferenced { function_name }) if function_name == "FooBar"
            ));
        }

        #[test]
        fn test_quoted_function_used_by_trigger() {
            let sql = r#"
                CREATE TABLE t (id INT);
                CREATE FUNCTION "FooBar"() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION "FooBar"();
                DROP FUNCTION "FooBar";
            "#;
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::FunctionReferenced { function_name }) if function_name == "FooBar"
            ));
        }
    }

    mod function_lookup_identifier_semantics {
        use super::*;
        use crate::traits::DatabaseLike;

        #[test]
        fn quoted_function_lookup_requires_exact_quoted_name() {
            let sql = r#"
                CREATE FUNCTION "FooBar"() RETURNS INT AS 'SELECT 1;';
            "#;
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(db.function("\"FooBar\"").is_some());
            assert!(db.function("foobar").is_none());
            assert!(db.function("\"foobar\"").is_none());
        }

        #[test]
        fn unquoted_function_lookup_uses_identifier_folding() {
            let sql = r"
                CREATE FUNCTION foobar() RETURNS INT AS 'SELECT 1;';
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(db.function("foobar").is_some());
            assert!(db.function("FOOBAR").is_some());
            assert!(db.function("\"FOOBAR\"").is_none());
        }

        #[test]
        fn database_like_function_lookup_is_identifier_aware() {
            let sql = r#"
                CREATE FUNCTION "FooBar"() RETURNS INT AS 'SELECT 1;';
            "#;
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(<ParserDB as DatabaseLike>::function(&db, "\"FooBar\"").is_some());
            assert!(<ParserDB as DatabaseLike>::function(&db, "foobar").is_none());
            assert!(<ParserDB as DatabaseLike>::function(&db, "\"foobar\"").is_none());
        }
    }

    mod is_table_referenced_tests {
        use super::*;

        #[test]
        fn test_table_not_referenced() {
            let sql = r"
                CREATE TABLE standalone (id INT PRIMARY KEY);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Table exists
            assert!(db.table(None, "standalone").is_some());

            // Can be dropped
            let drop_sql = format!("{sql}\nDROP TABLE standalone;");
            let result = ParserDB::parse::<GenericDialect>(&drop_sql);
            assert!(result.is_ok());
        }

        #[test]
        fn test_table_referenced_by_single_fk() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Both tables exist
            assert!(db.table(None, "parent").is_some());
            assert!(db.table(None, "child").is_some());

            // Parent cannot be dropped (referenced)
            let drop_parent = format!("{sql}\nDROP TABLE parent;");
            assert!(matches!(
                ParserDB::parse::<GenericDialect>(&drop_parent),
                Err(Error::TableReferenced { table_name }) if table_name == "parent"
            ));

            // Child can be dropped (not referenced)
            let drop_child = format!("{sql}\nDROP TABLE child;");
            assert!(ParserDB::parse::<GenericDialect>(&drop_child).is_ok());
        }

        #[test]
        fn test_table_referenced_by_multiple_fks() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child1 (id INT, parent_id INT REFERENCES parent(id));
                CREATE TABLE child2 (id INT, parent_id INT REFERENCES parent(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(db.table(None, "parent").is_some());

            // Parent cannot be dropped
            let drop_sql = format!("{sql}\nDROP TABLE parent;");
            assert!(matches!(
                ParserDB::parse::<GenericDialect>(&drop_sql),
                Err(Error::TableReferenced { .. })
            ));
        }

        #[test]
        fn test_self_referential_table_not_blocked() {
            let sql = r"
                CREATE TABLE tree (id INT PRIMARY KEY, parent_id INT REFERENCES tree(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            assert!(db.table(None, "tree").is_some());

            // Self-referential table CAN be dropped
            let drop_sql = format!("{sql}\nDROP TABLE tree;");
            assert!(ParserDB::parse::<GenericDialect>(&drop_sql).is_ok());
        }

        #[test]
        fn test_chain_of_references() {
            let sql = r"
                CREATE TABLE grandparent (id INT PRIMARY KEY);
                CREATE TABLE parent (id INT PRIMARY KEY, gp_id INT REFERENCES grandparent(id));
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
            ";

            // Cannot drop grandparent (referenced by parent)
            let drop_gp = format!("{sql}\nDROP TABLE grandparent;");
            assert!(matches!(
                ParserDB::parse::<GenericDialect>(&drop_gp),
                Err(Error::TableReferenced { table_name }) if table_name == "grandparent"
            ));

            // Cannot drop parent (referenced by child)
            let drop_parent = format!("{sql}\nDROP TABLE parent;");
            assert!(matches!(
                ParserDB::parse::<GenericDialect>(&drop_parent),
                Err(Error::TableReferenced { table_name }) if table_name == "parent"
            ));

            // Can drop child (not referenced)
            let drop_child = format!("{sql}\nDROP TABLE child;");
            assert!(ParserDB::parse::<GenericDialect>(&drop_child).is_ok());
        }
    }

    mod remove_table_tests {
        use super::*;
        use crate::traits::{DatabaseLike, TableLike};

        #[test]
        fn test_remove_table_removes_columns() {
            let sql = r"
                CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, age INT);
                CREATE TABLE t2 (id INT PRIMARY KEY);
                DROP TABLE t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // t1 should be gone
            assert!(db.table(None, "t1").is_none());

            // t2 should still have its column
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.columns(&db).expect("columns").count(), 1);
        }

        #[test]
        fn test_remove_table_removes_indices() {
            let sql = r"
                CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX idx_name ON t1(name);
                CREATE TABLE t2 (id INT PRIMARY KEY, value TEXT);
                CREATE INDEX idx_value ON t2(value);
                DROP TABLE t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // t1's index should be gone, t2's should remain
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.indices(&db).expect("indices").count(), 1);

            // Total indices across all tables should be 1 (only t2's index)
            let total_indices: usize =
                db.tables().map(|t| t.indices(&db).expect("indices").count()).sum();
            assert_eq!(total_indices, 1);
        }

        #[test]
        fn test_remove_table_removes_foreign_keys() {
            let sql = r"
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id));
                DROP TABLE child;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Parent should exist with no foreign keys (parent doesn't have any FKs
            // pointing out)
            let parent = db.table(None, "parent").expect("parent should exist");
            assert_eq!(parent.foreign_keys(&db).expect("foreign keys").count(), 0);

            // No foreign keys in the database (child's FK was removed with child)
            let total_fks: usize =
                db.tables().map(|t| t.foreign_keys(&db).expect("foreign keys").count()).sum();
            assert_eq!(total_fks, 0);
        }

        #[test]
        fn test_remove_table_removes_check_constraints() {
            let sql = r"
                CREATE TABLE t1 (id INT CHECK (id > 0));
                CREATE TABLE t2 (value INT CHECK (value < 100));
                DROP TABLE t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Only t2's check constraint should remain
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.check_constraints(&db).expect("check constraints").count(), 1);
        }

        #[test]
        fn test_remove_table_removes_triggers() {
            let sql = r"
                CREATE TABLE t1 (id INT);
                CREATE TABLE t2 (id INT);
                CREATE FUNCTION fn1() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE FUNCTION fn2() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER trg1 BEFORE INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION fn1();
                CREATE TRIGGER trg2 BEFORE INSERT ON t2 FOR EACH ROW EXECUTE FUNCTION fn2();
                DROP TABLE t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Only t2's trigger should remain
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.triggers(&db).expect("triggers").count(), 1);
        }

        #[test]
        fn test_remove_table_removes_policies() {
            let sql = r"
                CREATE TABLE t1 (id INT);
                CREATE TABLE t2 (id INT);
                CREATE POLICY p1 ON t1 USING (true);
                CREATE POLICY p2 ON t2 USING (true);
                DROP TABLE t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse");

            // Only t2's policy should remain
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.policies(&db).expect("policies").count(), 1);
        }
    }

    mod drop_index_tests {
        use super::*;

        #[test]
        fn test_drop_index_basic() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX idx_name ON t(name);
                DROP INDEX idx_name;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Index should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.indices(&db).expect("indices").count(), 0);
        }

        #[test]
        fn test_drop_index_if_exists_when_exists() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX idx_name ON t(name);
                DROP INDEX IF EXISTS idx_name;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Index should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.indices(&db).expect("indices").count(), 0);
        }

        #[test]
        fn test_drop_index_if_exists_when_not_exists() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY);
                DROP INDEX IF EXISTS nonexistent_idx;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Should succeed without error
            assert!(db.table(None, "t").is_some());
        }

        #[test]
        fn test_drop_index_not_found_error_type() {
            let sql = "DROP INDEX nonexistent_idx;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropIndexNotFound { index_name }) if index_name == "nonexistent_idx"
            ));
        }

        #[test]
        fn test_drop_multiple_indices() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT);
                CREATE INDEX idx_name ON t(name);
                CREATE INDEX idx_age ON t(age);
                DROP INDEX idx_name;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Only idx_age should remain
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.indices(&db).expect("indices").count(), 1);
        }

        #[test]
        fn test_drop_index_then_recreate() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX idx_name ON t(name);
                DROP INDEX idx_name;
                CREATE INDEX idx_name ON t(name);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Index should exist again
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.indices(&db).expect("indices").count(), 1);
        }

        #[test]
        fn test_drop_index_keeps_other_table_indices() {
            let sql = r"
                CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);
                CREATE TABLE t2 (id INT PRIMARY KEY, value TEXT);
                CREATE INDEX idx_t1_name ON t1(name);
                CREATE INDEX idx_t2_value ON t2(value);
                DROP INDEX idx_t1_name;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // t1 should have no indices
            let t1 = db.table(None, "t1").expect("t1 should exist");
            assert_eq!(t1.indices(&db).expect("indices").count(), 0);

            // t2 should still have its index
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.indices(&db).expect("indices").count(), 1);
        }

        #[test]
        fn test_drop_index_table_still_exists() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT, age INT);
                CREATE INDEX idx_name ON t(name);
                DROP INDEX idx_name;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Table should still exist with its columns
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.columns(&db).expect("columns").count(), 3);
        }
    }

    mod index_enumeration_tests {
        use super::*;
        use crate::traits::IndexLike;

        #[test]
        fn test_indexes_enumerates_named_indexes() {
            let sql = r"
                CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX idx_name ON t1(name);
                CREATE TABLE t2 (id INT PRIMARY KEY, value TEXT);
                CREATE INDEX idx_value ON t2(value);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let names: Vec<String> =
                db.indexes().filter_map(|i| i.name().map(ToString::to_string)).collect();
            assert_eq!(names, vec!["idx_name", "idx_value"]);
        }

        #[test]
        fn test_anonymous_index_has_no_name() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
                CREATE INDEX ON t(name);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let index = db.indexes().next().expect("index should exist");
            assert!(index.name().is_none());
        }

        #[test]
        fn test_indexes_excludes_unique_constraints() {
            let sql = r"
                CREATE TABLE t (id INT PRIMARY KEY, name TEXT, UNIQUE (name));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            assert_eq!(db.indexes().count(), 0);

            // A unique constraint viewed as an `IndexLike` exposes no
            // `ObjectName` name accessor; its index name is an `Ident`.
            let table = db.table(None, "t").expect("table should exist");
            for ui in table.unique_indices(&db).expect("unique indices") {
                assert!(IndexLike::name(ui).is_none());
            }
        }
    }

    mod drop_trigger_tests {
        use super::*;

        #[test]
        fn test_drop_trigger_basic() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION trigger_fn() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
                DROP TRIGGER my_trigger ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Trigger should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.triggers(&db).expect("triggers").count(), 0);
        }

        #[test]
        fn test_drop_trigger_if_exists_when_exists() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION trigger_fn() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
                DROP TRIGGER IF EXISTS my_trigger ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Trigger should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.triggers(&db).expect("triggers").count(), 0);
        }

        #[test]
        fn test_drop_trigger_if_exists_when_not_exists() {
            let sql = r"
                CREATE TABLE t (id INT);
                DROP TRIGGER IF EXISTS nonexistent_trigger ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Should succeed without error
            assert!(db.table(None, "t").is_some());
        }

        #[test]
        fn test_drop_trigger_not_found_error_type() {
            let sql = r"
                CREATE TABLE t (id INT);
                DROP TRIGGER nonexistent_trigger ON t;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropTriggerNotFound { trigger_name }) if trigger_name == "nonexistent_trigger"
            ));
        }

        #[test]
        fn test_drop_one_of_multiple_triggers() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION fn1() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE FUNCTION fn2() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER trigger1 BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn1();
                CREATE TRIGGER trigger2 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION fn2();
                DROP TRIGGER trigger1 ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Only trigger2 should remain
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.triggers(&db).expect("triggers").count(), 1);
        }

        #[test]
        fn test_drop_trigger_then_recreate() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION trigger_fn() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
                DROP TRIGGER my_trigger ON t;
                CREATE TRIGGER my_trigger AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Trigger should exist again
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.triggers(&db).expect("triggers").count(), 1);
        }

        #[test]
        fn test_drop_trigger_keeps_other_table_triggers() {
            let sql = r"
                CREATE TABLE t1 (id INT);
                CREATE TABLE t2 (id INT);
                CREATE FUNCTION fn1() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE FUNCTION fn2() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER trigger1 BEFORE INSERT ON t1 FOR EACH ROW EXECUTE FUNCTION fn1();
                CREATE TRIGGER trigger2 BEFORE INSERT ON t2 FOR EACH ROW EXECUTE FUNCTION fn2();
                DROP TRIGGER trigger1 ON t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // t1 should have no triggers
            let t1 = db.table(None, "t1").expect("t1 should exist");
            assert_eq!(t1.triggers(&db).expect("triggers").count(), 0);

            // t2 should still have its trigger
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.triggers(&db).expect("triggers").count(), 1);
        }

        #[test]
        fn test_drop_trigger_function_still_exists() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE FUNCTION trigger_fn() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
                CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION trigger_fn();
                DROP TRIGGER my_trigger ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Function should still exist after dropping trigger
            assert!(db.function("trigger_fn").is_some());
        }
    }

    mod drop_policy_tests {
        use super::*;

        #[test]
        fn test_drop_policy_basic() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE POLICY my_policy ON t USING (true);
                DROP POLICY my_policy ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Policy should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.policies(&db).expect("policies").count(), 0);
        }

        #[test]
        fn test_drop_policy_if_exists_when_exists() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE POLICY my_policy ON t USING (true);
                DROP POLICY IF EXISTS my_policy ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Policy should be removed
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.policies(&db).expect("policies").count(), 0);
        }

        #[test]
        fn test_drop_policy_if_exists_when_not_exists() {
            let sql = r"
                CREATE TABLE t (id INT);
                DROP POLICY IF EXISTS nonexistent_policy ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Should succeed without error
            assert!(db.table(None, "t").is_some());
        }

        #[test]
        fn test_drop_policy_not_found_error_type() {
            let sql = r"
                CREATE TABLE t (id INT);
                DROP POLICY nonexistent_policy ON t;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropPolicyNotFound { policy_name }) if policy_name == "nonexistent_policy"
            ));
        }

        #[test]
        fn test_drop_one_of_multiple_policies() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE POLICY policy1 ON t USING (true);
                CREATE POLICY policy2 ON t USING (false);
                DROP POLICY policy1 ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Only policy2 should remain
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.policies(&db).expect("policies").count(), 1);
        }

        #[test]
        fn test_drop_policy_then_recreate() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE POLICY my_policy ON t USING (true);
                DROP POLICY my_policy ON t;
                CREATE POLICY my_policy ON t USING (false);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Policy should exist again
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.policies(&db).expect("policies").count(), 1);
        }

        #[test]
        fn test_drop_policy_keeps_other_table_policies() {
            let sql = r"
                CREATE TABLE t1 (id INT);
                CREATE TABLE t2 (id INT);
                CREATE POLICY policy1 ON t1 USING (true);
                CREATE POLICY policy2 ON t2 USING (true);
                DROP POLICY policy1 ON t1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // t1 should have no policies
            let t1 = db.table(None, "t1").expect("t1 should exist");
            assert_eq!(t1.policies(&db).expect("policies").count(), 0);

            // t2 should still have its policy
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.policies(&db).expect("policies").count(), 1);
        }

        #[test]
        fn test_drop_policy_table_still_exists() {
            let sql = r"
                CREATE TABLE t (id INT, name TEXT);
                CREATE POLICY my_policy ON t USING (true);
                DROP POLICY my_policy ON t;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Table should still exist with its columns
            let table = db.table(None, "t").expect("Table should exist");
            assert_eq!(table.columns(&db).expect("columns").count(), 2);
        }
    }

    mod drop_role_tests {
        use super::*;

        #[test]
        fn test_drop_role_basic() {
            let sql = r"
                CREATE ROLE my_role;
                DROP ROLE my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Role should be removed
            assert!(db.role("my_role").is_none());
        }

        #[test]
        fn test_drop_role_if_exists_when_exists() {
            let sql = r"
                CREATE ROLE my_role;
                DROP ROLE IF EXISTS my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Role should be removed
            assert!(db.role("my_role").is_none());
        }

        #[test]
        fn test_drop_role_if_exists_when_not_exists() {
            let sql = r"
                DROP ROLE IF EXISTS nonexistent_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Should succeed without error
            assert_eq!(db.roles().count(), 0);
        }

        #[test]
        fn test_drop_role_not_found_error_type() {
            let sql = "DROP ROLE nonexistent_role;";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::DropRoleNotFound { role_name }) if role_name == "nonexistent_role"
            ));
        }

        #[test]
        fn test_drop_role_referenced_by_grant_fails() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE my_role;
                GRANT SELECT ON t TO my_role;
                DROP ROLE my_role;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::RoleReferenced { role_name }) if role_name == "my_role"
            ));
        }

        #[test]
        fn test_drop_role_after_revoking_grant_succeeds() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE my_role;
                GRANT SELECT ON t TO my_role;
                REVOKE SELECT ON t FROM my_role;
                DROP ROLE my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Should succeed after revoking");

            // Role should be removed
            assert!(db.role("my_role").is_none());
        }

        #[test]
        fn test_drop_one_of_multiple_roles() {
            let sql = r"
                CREATE ROLE role1;
                CREATE ROLE role2;
                DROP ROLE role1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // role1 should be removed
            assert!(db.role("role1").is_none());

            // role2 should still exist
            assert!(db.role("role2").is_some());
        }

        #[test]
        fn test_drop_role_then_recreate() {
            let sql = r"
                CREATE ROLE my_role;
                DROP ROLE my_role;
                CREATE ROLE my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            // Role should exist again
            assert!(db.role("my_role").is_some());
        }
    }

    mod grant_revoke_semantics {
        use sqlparser::{ast::Action, dialect::PostgreSqlDialect};

        use super::*;
        use crate::traits::{GrantLike, TableLike};

        #[test]
        fn test_revoke_partial_privilege_preserves_other_actions() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE my_role;
                GRANT SELECT, INSERT ON t TO my_role;
                REVOKE SELECT ON t FROM my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let grants: Vec<_> = db.table_grants().collect();
            assert_eq!(grants.len(), 1);
            let grant = grants[0];
            assert!(!grant.is_all_privileges());

            let remaining_privileges: Vec<_> = grant.privileges(&db).collect();
            assert_eq!(remaining_privileges.len(), 1);
            assert!(matches!(remaining_privileges[0], Action::Insert { .. }));

            let table = db.table(None, "t").expect("Table should exist");
            let role = db.role("my_role").expect("Role should exist");
            assert!(
                !table.can_select(role, &db).expect("can_select"),
                "SELECT should be revoked while INSERT remains"
            );
            assert!(table.can_insert(role, &db).expect("can_insert"));
        }

        #[test]
        fn test_revoke_column_scoped_against_table_wide_action_is_unsupported() {
            let sql = r"
                CREATE TABLE t (id INT, name TEXT);
                CREATE ROLE my_role;
                GRANT SELECT ON t TO my_role;
                REVOKE SELECT (id) ON t FROM my_role;
            ";
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);

            assert!(matches!(
                result,
                Err(Error::UnsupportedRevoke { reason, .. })
                    if reason.contains("column-scoped REVOKE against a table-wide action grant")
            ));
        }

        #[test]
        fn test_revoke_column_scoped_from_column_scoped_grant_keeps_remaining_columns() {
            let sql = r"
                CREATE TABLE t (id INT, name TEXT);
                CREATE ROLE my_role;
                GRANT SELECT (id, name) ON t TO my_role;
                REVOKE SELECT (id) ON t FROM my_role;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("Failed to parse SQL");

            let grant = db.table_grants().next().expect("Expected a remaining grant");
            let remaining_privileges: Vec<_> = grant.privileges(&db).collect();

            assert_eq!(remaining_privileges.len(), 1);
            match remaining_privileges[0] {
                Action::Select { columns: Some(columns) } => {
                    assert_eq!(columns.len(), 1);
                    assert_eq!(columns[0].value, "name");
                }
                other => panic!("Expected SELECT with one remaining column, got {other:?}"),
            }
        }

        #[test]
        fn test_revoke_from_first_grantee_keeps_second_grantee_privileges() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE a;
                CREATE ROLE b;
                GRANT SELECT ON t TO a, b;
                REVOKE SELECT ON t FROM a;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let role_a = db.role("a").expect("Role a should exist");
            let role_b = db.role("b").expect("Role b should exist");

            assert!(
                !table.can_select(role_a, &db).expect("can_select"),
                "SELECT should be revoked for a"
            );
            assert!(
                table.can_select(role_b, &db).expect("can_select"),
                "SELECT should remain for b"
            );
        }

        #[test]
        fn test_revoke_from_second_grantee_keeps_first_grantee_privileges() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE a;
                CREATE ROLE b;
                GRANT SELECT ON t TO a, b;
                REVOKE SELECT ON t FROM b;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let role_a = db.role("a").expect("Role a should exist");
            let role_b = db.role("b").expect("Role b should exist");

            assert!(
                table.can_select(role_a, &db).expect("can_select"),
                "SELECT should remain for a"
            );
            assert!(
                !table.can_select(role_b, &db).expect("can_select"),
                "SELECT should be revoked for b"
            );
        }

        #[test]
        fn test_revoke_partial_privilege_from_one_grantee_preserves_other_grantee_actions() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE a;
                CREATE ROLE b;
                GRANT SELECT, INSERT ON t TO a, b;
                REVOKE SELECT ON t FROM a;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let role_a = db.role("a").expect("Role a should exist");
            let role_b = db.role("b").expect("Role b should exist");

            assert_eq!(db.table_grants().count(), 2);
            assert!(
                !table.can_select(role_a, &db).expect("can_select"),
                "SELECT should be revoked for a"
            );
            assert!(
                table.can_insert(role_a, &db).expect("can_insert"),
                "INSERT should remain for a"
            );
            assert!(
                table.can_select(role_b, &db).expect("can_select"),
                "SELECT should remain for b"
            );
            assert!(
                table.can_insert(role_b, &db).expect("can_insert"),
                "INSERT should remain for b"
            );
        }

        #[test]
        fn test_revoke_all_from_one_grantee_preserves_other_grantee_actions() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE a;
                CREATE ROLE b;
                GRANT SELECT, INSERT ON t TO a, b;
                REVOKE ALL ON t FROM a;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let role_a = db.role("a").expect("Role a should exist");
            let role_b = db.role("b").expect("Role b should exist");

            assert!(
                !table.can_select(role_a, &db).expect("can_select"),
                "SELECT should be revoked for a"
            );
            assert!(
                !table.can_insert(role_a, &db).expect("can_insert"),
                "INSERT should be revoked for a"
            );
            assert!(
                table.can_select(role_b, &db).expect("can_select"),
                "SELECT should remain for b"
            );
            assert!(
                table.can_insert(role_b, &db).expect("can_insert"),
                "INSERT should remain for b"
            );
        }

        #[test]
        fn test_revoke_all_removes_matching_grants() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE my_role;
                GRANT SELECT, INSERT ON t TO my_role;
                REVOKE ALL ON t FROM my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            assert_eq!(db.table_grants().count(), 0);
            assert_eq!(db.column_grants().count(), 0);
        }

        #[test]
        fn test_public_grant_applies_to_non_public_role() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE app_user;
                GRANT SELECT ON t TO PUBLIC;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let app_user = db.role("app_user").expect("Role should exist");
            assert!(table.can_select(app_user, &db).expect("can_select"));
        }

        #[test]
        fn test_schema_qualified_grant_applies_only_to_target_table() {
            let sql = r"
                CREATE SCHEMA s1;
                CREATE SCHEMA s2;
                CREATE TABLE s1.t (id INT);
                CREATE TABLE s2.t (id INT);
                CREATE ROLE app_role;
                GRANT SELECT ON s1.t TO app_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

            let role = db.role("app_role").expect("Role should exist");
            let s1_t = db.table(Some("s1"), "t").expect("s1.t should exist");
            let s2_t = db.table(Some("s2"), "t").expect("s2.t should exist");

            assert!(s1_t.can_select(role, &db).expect("can_select"));
            assert!(!s2_t.can_select(role, &db).expect("can_select"));
        }

        #[test]
        fn test_revoke_object_matching_is_case_insensitive_for_unquoted_identifiers() {
            let sql = r"
                CREATE TABLE T (id INT);
                CREATE ROLE my_role;
                GRANT SELECT ON T TO my_role;
                REVOKE SELECT ON t FROM my_role;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("Failed to parse SQL");

            let table = db.table(None, "t").expect("Table should exist");
            let role = db.role("my_role").expect("Role should exist");
            assert!(!table.can_select(role, &db).expect("can_select"));
            assert_eq!(db.table_grants().count(), 0);
        }

        #[test]
        fn test_revoke_object_matching_preserves_quoted_identifier_semantics() {
            let sql = r#"
                CREATE TABLE T (id INT);
                CREATE ROLE my_role;
                GRANT SELECT ON T TO my_role;
                REVOKE SELECT ON "T" FROM my_role;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);

            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        #[test]
        fn test_revoke_object_matching_does_not_match_quoted_grant_with_unquoted_lookup() {
            let sql = r#"
                CREATE TABLE "T" (id INT);
                CREATE ROLE my_role;
                GRANT SELECT ON "T" TO my_role;
                REVOKE SELECT ON t FROM my_role;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);

            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        #[test]
        fn test_revoke_function_object_matching_is_case_insensitive_for_unquoted_identifiers() {
            let sql = r"
                CREATE ROLE my_role;
                GRANT EXECUTE ON FUNCTION F() TO my_role;
                REVOKE EXECUTE ON FUNCTION f() FROM my_role;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("Failed to parse SQL");

            assert_eq!(db.table_grants().count(), 0);
            assert_eq!(db.column_grants().count(), 0);
        }

        #[test]
        fn test_revoke_function_object_matching_preserves_quoted_identifier_semantics() {
            let sql = r#"
                CREATE ROLE my_role;
                GRANT EXECUTE ON FUNCTION F() TO my_role;
                REVOKE EXECUTE ON FUNCTION "F"() FROM my_role;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);

            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        #[test]
        fn test_revoke_function_object_matching_does_not_match_quoted_grant_with_unquoted_lookup() {
            let sql = r#"
                CREATE ROLE my_role;
                GRANT EXECUTE ON FUNCTION "F"() TO my_role;
                REVOKE EXECUTE ON FUNCTION f() FROM my_role;
            "#;
            let result = ParserDB::parse::<PostgreSqlDialect>(sql);

            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        // ----------------------------------------------------------------
        // Coverage-targeted tests for `impls/sqlparser/grant.rs`:
        // `partition_grantees_for_revoke`, `apply_revoke_to_grant`,
        // `grant_objects_inner_match`, the per-column accessor branches,
        // and the GrantObjects variants the existing tests don't reach.
        // ----------------------------------------------------------------

        /// `apply_revoke_to_grant` `(Privileges::All, Privileges::Actions)`
        /// arm: the implementation cannot represent "ALL minus X" in
        /// its grant model, so a partial revoke against a `GRANT ALL
        /// PRIVILEGES` is treated as matched-but-no-op — the grant
        /// stays intact at ALL PRIVILEGES. This is a documented
        /// limitation in `impls/sqlparser/grant.
        /// rs::apply_revoke_to_grant`.
        #[test]
        fn test_revoke_select_against_all_privileges_is_a_documented_noop() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE my_role;
                GRANT ALL PRIVILEGES ON t TO my_role;
                REVOKE SELECT ON t FROM my_role;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");

            let grant = db.table_grants().next().expect("grant must remain");
            assert!(
                grant.is_all_privileges(),
                "grant stays ALL PRIVILEGES; the model cannot represent ALL-minus-Actions",
            );
        }

        /// `tables()` `GrantObjects::AllTablesInSchema` branch: a grant on
        /// `ALL TABLES IN SCHEMA` should parse and produce one entry in the
        /// `table_grants` iterator without panicking.
        #[test]
        fn test_grant_all_tables_in_schema_parses_and_is_indexed() {
            let sql = r"
                CREATE SCHEMA s;
                CREATE TABLE s.t (id INT);
                CREATE ROLE r;
                GRANT SELECT ON ALL TABLES IN SCHEMA s TO r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 1);
        }

        /// `grant_objects_inner_match` Function arm: an EXECUTE grant on a
        /// concrete function object lands in `table_grants`.
        #[test]
        fn test_grant_execute_on_function() {
            let sql = r"
                CREATE FUNCTION f(x INT) RETURNS INT AS 'SELECT $1';
                CREATE ROLE r;
                GRANT EXECUTE ON FUNCTION f(INT) TO r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 1);
        }

        /// `apply_revoke_to_grant`'s object-mismatch fast-return: revoking
        /// against a different table than the grant covers must leave the
        /// original grant untouched and itself be a no-op (no error).
        #[test]
        fn test_revoke_on_different_table_leaves_original_grant_untouched() {
            let sql = r"
                CREATE TABLE t1 (id INT);
                CREATE TABLE t2 (id INT);
                CREATE ROLE r;
                GRANT SELECT ON t1 TO r;
                REVOKE SELECT ON t2 FROM r;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);
            // A REVOKE that matches no grant returns `RevokeNotFound`.
            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        /// `partition_grantees_for_revoke` grantee-mismatch path: revoking
        /// from a role that doesn't appear as a grantee on the matching
        /// grant must surface as `RevokeNotFound`.
        #[test]
        fn test_revoke_from_different_grantee_returns_not_found() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE r1;
                CREATE ROLE r2;
                GRANT SELECT ON t TO r1;
                REVOKE SELECT ON t FROM r2;
            ";
            let result = ParserDB::parse::<GenericDialect>(sql);
            assert!(matches!(result, Err(Error::RevokeNotFound(_))));
        }

        /// `columns()` per-column INSERT/UPDATE/REFERENCES arms: a multi-
        /// action column-scoped grant must surface column grants for each
        /// privileged action.
        #[test]
        fn test_grant_per_column_insert_update_references_creates_column_grants() {
            let sql = r"
                CREATE TABLE t (a INT, b INT);
                CREATE ROLE r;
                GRANT INSERT (a), UPDATE (b), REFERENCES (a) ON t TO r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            // Each per-column action creates its own column grant.
            assert!(db.column_grants().count() >= 1, "at least one column grant expected");
        }

        /// `apply_revoke_to_grant`'s "drop the whole grant when no actions
        /// remain" path: REVOKE ALL from a single-grantee grant removes
        /// the grant entirely.
        #[test]
        fn test_revoke_all_privileges_from_only_grantee_drops_grant() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE r;
                GRANT SELECT, INSERT ON t TO r;
                REVOKE ALL PRIVILEGES ON t FROM r;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `apply_revoke_action_to_grant_action` partial-column path: a
        /// column-scoped revoke that drops some columns but keeps others
        /// must preserve the remaining columns under the same action.
        #[test]
        fn test_revoke_subset_of_column_scoped_grant_keeps_unrevoked_columns() {
            let sql = r"
                CREATE TABLE t (a INT, b INT, c INT);
                CREATE ROLE r;
                GRANT SELECT (a, b, c) ON t TO r;
                REVOKE SELECT (b) ON t FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");

            let grant = db.table_grants().next().expect("grant must remain");
            let privileges: Vec<_> = grant.privileges(&db).collect();
            assert_eq!(privileges.len(), 1);
            match privileges[0] {
                Action::Select { columns: Some(columns) } => {
                    let names: Vec<_> = columns.iter().map(|c| c.value.as_str()).collect();
                    assert_eq!(names, vec!["a", "c"], "only `b` should be revoked");
                }
                other => panic!("expected SELECT with columns, got {other:?}"),
            }
        }

        /// `partition_grantees_for_revoke` multi-grantee path: revoking
        /// from one of two grantees must keep the grant alive for the
        /// other grantee (covers the `unmatched` partition + split).
        #[test]
        fn test_revoke_select_from_one_of_two_grantees_keeps_grant_for_other() {
            let sql = r"
                CREATE TABLE t (id INT);
                CREATE ROLE r1;
                CREATE ROLE r2;
                GRANT SELECT ON t TO r1, r2;
                REVOKE SELECT ON t FROM r1;
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");

            let table = db.table(None, "t").unwrap();
            let r1 = db.role("r1").unwrap();
            let r2 = db.role("r2").unwrap();
            assert!(
                !table.can_select(r1, &db).expect("can_select"),
                "r1 should have had SELECT revoked"
            );
            assert!(table.can_select(r2, &db).expect("can_select"), "r2 should still have SELECT");
        }

        /// `grant_objects_inner_match` schema-list arm: REVOKE on
        /// `ALL TABLES IN SCHEMA` matching a same-shape GRANT goes
        /// through the giant merged arm that covers AllSequences/
        /// AllTables/AllViews/AllMaterializedViews/... InSchema.
        #[test]
        fn test_revoke_all_tables_in_schema_matches_corresponding_grant() {
            let sql = r"
                CREATE SCHEMA s;
                CREATE TABLE s.t (id INT);
                CREATE ROLE r;
                GRANT SELECT ON ALL TABLES IN SCHEMA s TO r;
                REVOKE SELECT ON ALL TABLES IN SCHEMA s FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0, "matching revoke removes the grant");
        }

        /// `grant_objects_inner_match` Schemas object-list arm:
        /// GRANT USAGE ON SCHEMA + matching REVOKE traverses the
        /// merged ObjectName-list arm.
        #[test]
        fn test_revoke_usage_on_schema_matches_grant() {
            let sql = r"
                CREATE SCHEMA s;
                CREATE ROLE r;
                GRANT USAGE ON SCHEMA s TO r;
                REVOKE USAGE ON SCHEMA s FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `grant_objects_inner_match` Function arm: REVOKE on a
        /// specific function-by-signature matches the corresponding
        /// GRANT EXECUTE on that function.
        #[test]
        fn test_revoke_execute_on_function_matches_grant() {
            let sql = r"
                CREATE FUNCTION f(x INT) RETURNS INT AS 'SELECT $1';
                CREATE ROLE r;
                GRANT EXECUTE ON FUNCTION f(INT) TO r;
                REVOKE EXECUTE ON FUNCTION f(INT) FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `applies_to_table` AllTablesInSchema branch: querying whether
        /// an `ALL TABLES IN SCHEMA s` grant applies to a specific table
        /// returns true iff the table's schema matches.
        #[test]
        fn test_all_tables_in_schema_grant_applies_to_matching_table() {
            use crate::traits::TableGrantLike;

            let sql = r"
                CREATE SCHEMA s;
                CREATE TABLE s.in_scope (id INT);
                CREATE TABLE out_of_scope (id INT);
                CREATE ROLE r;
                GRANT SELECT ON ALL TABLES IN SCHEMA s TO r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            let grant = db.table_grants().next().expect("grant");
            let in_scope = db.table(Some("s"), "in_scope").expect("in_scope table");
            let out_of_scope = db.table(None, "out_of_scope").expect("out_of_scope table");

            assert!(grant.applies_to_table(in_scope, &db));
            assert!(!grant.applies_to_table(out_of_scope, &db));
        }

        /// `grant_objects_inner_match` Sequences object-list arm:
        /// GRANT USAGE ON SEQUENCE + matching REVOKE traverses the
        /// merged ObjectName-list arm (covers another row of the merged
        /// pattern at lines ~239–261).
        #[test]
        fn test_revoke_usage_on_sequence_matches_grant() {
            let sql = r"
                CREATE ROLE r;
                GRANT USAGE ON SEQUENCE my_seq TO r;
                REVOKE USAGE ON SEQUENCE my_seq FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `grant_objects_inner_match` All-future-tables-in-schema arm.
        #[test]
        fn test_revoke_all_sequences_in_schema_matches_grant() {
            let sql = r"
                CREATE SCHEMA s;
                CREATE ROLE r;
                GRANT USAGE ON ALL SEQUENCES IN SCHEMA s TO r;
                REVOKE USAGE ON ALL SEQUENCES IN SCHEMA s FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `grant_objects_inner_match` fallback arm (`(left, right) =>
        /// left == right`): a GRANT and REVOKE on the same simple
        /// pseudo-object that doesn't match a specific variant goes
        /// through the structural-equality arm.
        #[test]
        fn test_revoke_on_database_object_matches_grant() {
            let sql = r"
                CREATE ROLE r;
                GRANT CREATE ON DATABASE my_db TO r;
                REVOKE CREATE ON DATABASE my_db FROM r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            assert_eq!(db.table_grants().count(), 0);
        }

        /// `ColumnGrantLike::columns` per-action branches: a multi-action
        /// column grant (`INSERT (a), UPDATE (b), REFERENCES (c)`) surfaces
        /// the union of columns across all three arms in `columns()`.
        #[test]
        fn test_column_grant_columns_iterator_covers_all_action_arms() {
            use crate::traits::ColumnGrantLike;

            let sql = r"
                CREATE TABLE t (a INT, b INT, c INT);
                CREATE ROLE r;
                GRANT INSERT (a), UPDATE (b), REFERENCES (c) ON t TO r;
            ";
            let db = ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse");
            let table = db.table(None, "t").expect("table");

            // Iterate every column grant and call `.columns(table, &db)`.
            // This routes through the Insert/Update/References match
            // arms in `impls/sqlparser/grant.rs::columns`.
            let mut all_cols: Vec<&str> = Vec::new();
            for cg in db.column_grants() {
                for col in cg.columns(table, &db).expect("column grant columns") {
                    all_cols.push(col.column_name());
                }
            }
            all_cols.sort_unstable();
            all_cols.dedup();
            // The three actions reference three distinct columns.
            assert!(all_cols.contains(&"a"));
            assert!(all_cols.contains(&"b"));
            assert!(all_cols.contains(&"c"));
        }
    }

    mod foreign_key_target_validation {
        use sqlparser::dialect::PostgreSqlDialect;

        use super::*;

        #[test]
        fn column_option_reference_to_existing_target_validates() {
            let sql = "
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            assert!(db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn table_constraint_reference_to_existing_target_validates() {
            let sql = "
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (
                    id INT PRIMARY KEY,
                    parent_id INT,
                    FOREIGN KEY (parent_id) REFERENCES parent(id)
                );
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            assert!(db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn reference_to_missing_table_errors_naming_target_and_host() {
            let sql = "
                CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES orders(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            match db.validate_foreign_key_targets() {
                Err(Error::ReferencedTableNotFoundForForeignKey {
                    referenced_table,
                    host_table,
                }) => {
                    assert_eq!(referenced_table, "orders");
                    assert_eq!(host_table, "child");
                }
                other => panic!("expected dangling-table error, got {other:?}"),
            }
        }

        #[test]
        fn reference_to_missing_column_errors_naming_column() {
            let sql = "
                CREATE TABLE parent (id INT PRIMARY KEY);
                CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(missing));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            match db.validate_foreign_key_targets() {
                Err(Error::ReferencedColumnNotFoundForForeignKey {
                    referenced_column,
                    referenced_table,
                    host_table,
                }) => {
                    assert_eq!(referenced_column, "missing");
                    assert_eq!(referenced_table, "parent");
                    assert_eq!(host_table, "child");
                }
                other => panic!("expected dangling-column error, got {other:?}"),
            }
        }

        #[test]
        fn bare_and_public_qualified_targets_resolve_identically() {
            let bare = "
                CREATE TABLE public.parent (id INT PRIMARY KEY);
                CREATE TABLE public.child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
            ";
            let qualified = "
                CREATE TABLE public.parent (id INT PRIMARY KEY);
                CREATE TABLE public.child (
                    id INT PRIMARY KEY,
                    parent_id INT REFERENCES public.parent(id)
                );
            ";
            let bare_db = ParserDB::parse::<PostgreSqlDialect>(bare).expect("parse");
            let qualified_db = ParserDB::parse::<PostgreSqlDialect>(qualified).expect("parse");
            assert!(bare_db.validate_foreign_key_targets().is_ok());
            assert!(qualified_db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn forward_reference_validates() {
            let sql = "
                CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
                CREATE TABLE parent (id INT PRIMARY KEY);
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            assert!(db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn self_referential_reference_validates() {
            let sql = "
                CREATE TABLE tree (
                    id INT PRIMARY KEY,
                    parent_id INT REFERENCES tree(id)
                );
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            assert!(db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn multiple_dangling_constraints_report_deterministic_first_error() {
            let sql = "
                CREATE TABLE a (id INT PRIMARY KEY, x INT REFERENCES missing_a(id));
                CREATE TABLE b (id INT PRIMARY KEY, y INT REFERENCES missing_b(id));
            ";
            let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
            let first = db.validate_foreign_key_targets().expect_err("dangling FKs must error");
            let second = db.validate_foreign_key_targets().expect_err("dangling FKs must error");
            assert_eq!(format!("{first}"), format!("{second}"));
            match first {
                Error::ReferencedTableNotFoundForForeignKey { referenced_table, .. } => {
                    assert_eq!(referenced_table, "missing_a");
                }
                other => panic!("expected dangling-table error, got {other:?}"),
            }
        }
    }

    mod primary_key_validation {
        use super::*;
        use crate::traits::ColumnLike;

        #[test]
        fn expression_primary_key_errors_instead_of_panicking() {
            match ParserDB::parse::<GenericDialect>(
                "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a - b));",
            ) {
                Err(Error::InvalidPrimaryKey { table_name, reason }) => {
                    assert_eq!(table_name, "t");
                    assert!(
                        reason.contains("a - b"),
                        "reason should name the offending expression"
                    );
                }
                other => panic!("expected InvalidPrimaryKey, got {other:?}"),
            }
        }

        #[test]
        fn function_call_primary_key_errors() {
            assert!(matches!(
                ParserDB::parse::<GenericDialect>(
                    "CREATE TABLE t (a INT, PRIMARY KEY (LOWER(a)));"
                ),
                Err(Error::InvalidPrimaryKey { .. })
            ));
        }

        #[test]
        fn plain_column_primary_key_still_parses() {
            let db = ParserDB::parse::<GenericDialect>(
                "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b));",
            )
            .expect("parse");
            let table = db.table(None, "t").expect("table t");
            let pk: Vec<&str> = table
                .primary_key_columns(&db)
                .expect("pk columns")
                .map(ColumnLike::column_name)
                .collect();
            assert_eq!(pk, vec!["a", "b"]);
        }
    }

    mod alter_table_constraints {
        use sqlparser::dialect::PostgreSqlDialect;

        use super::*;
        use crate::traits::{ColumnLike, IndexLike, PolicyLike};

        fn parse(sql: &str) -> ParserDB {
            ParserDB::parse::<PostgreSqlDialect>(sql).expect("parse")
        }

        fn primary_key(db: &ParserDB, table_name: &str) -> Vec<String> {
            db.table(None, table_name)
                .expect("table")
                .primary_key_columns(db)
                .expect("pk columns")
                .map(|column| column.column_name().to_string())
                .collect()
        }

        fn unique_index_count(db: &ParserDB, table_name: &str) -> usize {
            db.table(None, table_name)
                .expect("table")
                .unique_indices(db)
                .expect("unique indices")
                .count()
        }

        fn foreign_key_count(db: &ParserDB, table_name: &str) -> usize {
            db.table(None, table_name)
                .expect("table")
                .foreign_keys(db)
                .expect("foreign keys")
                .count()
        }

        #[test]
        fn added_primary_key_answers_as_the_inline_one() {
            let inline = parse("CREATE TABLE t (id uuid PRIMARY KEY, o uuid);");
            let altered = parse(
                "CREATE TABLE t (id uuid NOT NULL, o uuid);
                 ALTER TABLE ONLY t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
            );

            assert_eq!(primary_key(&inline, "t"), primary_key(&altered, "t"));
            assert_eq!(primary_key(&altered, "t"), vec!["id".to_string()]);
            assert_eq!(unique_index_count(&inline, "t"), unique_index_count(&altered, "t"));
            assert_eq!(unique_index_count(&altered, "t"), 1);
        }

        #[test]
        fn unnamed_added_primary_key_is_applied() {
            let db = parse(
                "CREATE TABLE t (id uuid NOT NULL);
                 ALTER TABLE t ADD PRIMARY KEY (id);",
            );
            assert_eq!(primary_key(&db, "t"), vec!["id".to_string()]);
        }

        #[test]
        fn added_unique_constraint_registers_a_unique_index() {
            let db = parse(
                "CREATE TABLE t (id uuid NOT NULL);
                 ALTER TABLE ONLY t ADD CONSTRAINT t_id_key UNIQUE (id);",
            );
            assert_eq!(unique_index_count(&db, "t"), 1);
            assert!(primary_key(&db, "t").is_empty(), "UNIQUE alone is not a primary key");
        }

        #[test]
        fn schema_qualified_alter_table_applies_the_constraint() {
            let db = parse(
                "CREATE TABLE public.t (id uuid NOT NULL);
                 ALTER TABLE ONLY public.t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
            );
            let table = db.table(Some("public"), "t").expect("table");
            let pk: Vec<&str> = table
                .primary_key_columns(&db)
                .expect("pk columns")
                .map(ColumnLike::column_name)
                .collect();
            assert_eq!(pk, vec!["id"]);
        }

        #[test]
        fn added_foreign_key_resolves_a_later_declared_target() {
            let db = parse(
                "CREATE TABLE t (id uuid NOT NULL, o uuid);
                 CREATE TABLE u (id uuid NOT NULL);
                 ALTER TABLE ONLY t ADD CONSTRAINT t_o_fkey FOREIGN KEY (o) REFERENCES u(id);",
            );
            assert_eq!(foreign_key_count(&db, "t"), 1);
            assert!(db.validate_foreign_key_targets().is_ok());
        }

        #[test]
        fn added_check_constraint_is_registered() {
            let db = parse(
                "CREATE TABLE t (id INT NOT NULL);
                 ALTER TABLE t ADD CONSTRAINT t_id_positive CHECK (id > 0);",
            );
            let table = db.table(None, "t").expect("table");
            assert_eq!(table.check_constraints(&db).expect("check constraints").count(), 1);
        }

        #[test]
        fn altering_constraints_preserves_the_rest_of_the_table() {
            let db = parse(
                "CREATE TABLE t (id uuid NOT NULL, o uuid);
                 CREATE INDEX t_o_idx ON t (o);
                 ALTER TABLE t ENABLE ROW LEVEL SECURITY;
                 ALTER TABLE t FORCE ROW LEVEL SECURITY;
                 CREATE POLICY t_all ON t USING (true);
                 ALTER TABLE ONLY t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
            );
            let table = db.table(None, "t").expect("table");

            assert!(table.has_row_level_security(&db).expect("rls"));
            assert!(table.has_forced_row_level_security(&db).expect("forced rls"));

            let columns: Vec<&str> =
                table.columns(&db).expect("columns").map(ColumnLike::column_name).collect();
            assert_eq!(columns, vec!["id", "o"]);
            for column in table.columns(&db).expect("columns") {
                assert!(
                    db.column_metadata(column).is_some(),
                    "re-seated columns must stay findable in the database"
                );
            }

            let indices: Vec<_> = table.indices(&db).expect("indices").collect();
            assert_eq!(indices.len(), 1);
            for index in indices {
                assert_eq!(index.name().map(last_str), Some("t_o_idx"));
                assert!(
                    db.index_metadata(index).is_some(),
                    "re-seated indexes must stay findable in the database"
                );
            }
            for unique_index in table.unique_indices(&db).expect("unique indices") {
                assert!(
                    db.unique_index_metadata(unique_index).is_some(),
                    "unique indexes must stay findable in the database"
                );
            }

            let policy = db.policies().next().expect("policy");
            assert_eq!(
                PolicyLike::table(policy, &db).expect("policy table").table_name(),
                "t",
                "a policy must still resolve the table it guards"
            );
        }

        #[test]
        fn dropping_a_constraint_undoes_it() {
            let db = parse(
                "CREATE TABLE u (id uuid NOT NULL, PRIMARY KEY (id));
                 CREATE TABLE t (id uuid NOT NULL, o uuid);
                 ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY (id);
                 ALTER TABLE t ADD CONSTRAINT t_o_key UNIQUE (o);
                 ALTER TABLE t ADD CONSTRAINT t_o_fkey FOREIGN KEY (o) REFERENCES u(id);
                 ALTER TABLE t DROP CONSTRAINT t_o_fkey;
                 ALTER TABLE t DROP CONSTRAINT t_o_key;",
            );

            assert_eq!(foreign_key_count(&db, "t"), 0);
            assert_eq!(unique_index_count(&db, "t"), 1, "the primary key index survives");
            assert_eq!(primary_key(&db, "t"), vec!["id".to_string()]);

            let dropped_pk = parse(
                "CREATE TABLE t (id uuid NOT NULL);
                 ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY (id);
                 ALTER TABLE t DROP CONSTRAINT t_pkey;",
            );
            assert!(primary_key(&dropped_pk, "t").is_empty());
            assert_eq!(unique_index_count(&dropped_pk, "t"), 0);
        }

        #[test]
        fn dropping_an_undeclared_constraint_is_reported() {
            match ParserDB::parse::<PostgreSqlDialect>(
                "CREATE TABLE t (id uuid NOT NULL);
                 ALTER TABLE t DROP CONSTRAINT t_pkey;",
            ) {
                Err(Error::DropConstraintNotFound { table_name, constraint_name }) => {
                    assert_eq!(table_name, "t");
                    assert_eq!(constraint_name, "t_pkey");
                }
                other => panic!("expected DropConstraintNotFound, got {other:?}"),
            }

            let tolerated = parse(
                "CREATE TABLE t (id uuid NOT NULL);
                 ALTER TABLE t DROP CONSTRAINT IF EXISTS t_pkey;",
            );
            assert!(primary_key(&tolerated, "t").is_empty());
        }

        #[test]
        fn altering_an_absent_table_is_reported() {
            match ParserDB::parse::<PostgreSqlDialect>(
                "ALTER TABLE ONLY t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
            ) {
                Err(Error::AlterTableNotFound { table_name }) => assert_eq!(table_name, "t"),
                other => panic!("expected AlterTableNotFound, got {other:?}"),
            }

            let tolerated = ParserDB::parse::<PostgreSqlDialect>(
                "ALTER TABLE IF EXISTS t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
            )
            .expect("IF EXISTS tolerates an absent table");
            assert!(tolerated.table(None, "t").is_none());
        }

        #[test]
        fn constraints_naming_an_undeclared_column_are_reported() {
            for sql in [
                "ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY (missing);",
                "ALTER TABLE t ADD CONSTRAINT t_key UNIQUE (missing);",
            ] {
                let sql = format!("CREATE TABLE t (id uuid NOT NULL);\n{sql}");
                assert!(
                    matches!(
                        ParserDB::parse::<PostgreSqlDialect>(&sql),
                        Err(Error::IdentifierLookupError(LookupError::ColumnNotFound {
                            ref table_name,
                            ref column_name,
                        })) if table_name == "t" && column_name == "missing"
                    ),
                    "expected ColumnNotFound for `{sql}`"
                );
            }

            assert!(matches!(
                ParserDB::parse::<PostgreSqlDialect>(
                    "CREATE TABLE u (id uuid NOT NULL);
                     CREATE TABLE t (id uuid NOT NULL);
                     ALTER TABLE t ADD CONSTRAINT t_fkey FOREIGN KEY (missing) REFERENCES u(id);",
                ),
                Err(Error::HostColumnNotFoundForForeignKey { .. })
            ));

            assert!(matches!(
                ParserDB::parse::<PostgreSqlDialect>(
                    "CREATE TABLE t (id uuid NOT NULL, o uuid);
                     ALTER TABLE t ADD CONSTRAINT t_fkey FOREIGN KEY (o) REFERENCES u(id);",
                ),
                Err(Error::ReferencedTableNotFoundForForeignKey { .. })
            ));
        }
    }
}
