//! Implementations for [`ParserDB`] - a database schema parsed from SQL text.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use git2::Repository;
use sql_docs::SqlDoc;
use sqlparser::{
    ast::{
        AlterPolicy, AlterPolicyOperation, AlterSchema, AlterSchemaOperation, AlterTableOperation,
        CheckConstraint, ColumnDef, ColumnOption, CreateFunction, CreateFunctionBody, CreateIndex,
        CreatePolicy, CreateRole, CreateTable, CreateTrigger, DataType, ExactNumberInfo, Expr,
        ForeignKeyConstraint, Grant, Ident, IndexColumn, ObjectName, ObjectNamePart,
        OperateFunctionArg, OrderByExpr, OrderByOptions, RenameTableNameKind, SchemaName,
        Statement, TableConstraint, TimezoneInfo, UniqueConstraint, Value, ValueWithSpan,
    },
    dialect::{Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::Span,
};

use crate::{
    errors::LookupError,
    structs::{
        GenericDB, Schema, TableAttribute, TableMetadata,
        metadata::{CheckMetadata, IndexMetadata, PolicyMetadata, UniqueIndexMetadata},
    },
    traits::{ColumnLike, FunctionLike, TableLike},
    utils::{columns_in_expression, identifier_resolution::identifiers_match, last_str},
};

mod functions_in_expression;

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
>;

impl ParserDBBuilder {
    /// Checks if a function with the given name is referenced by any schema
    /// object.
    ///
    /// Returns `true` if the function is used by:
    /// - Check constraints (via their metadata)
    /// - Policies (via USING or WITH CHECK expressions)
    /// - Triggers (via EXECUTE FUNCTION)
    fn is_function_used(&self, function_name: &str) -> bool {
        use crate::traits::{FunctionLike, TriggerLike};

        // Check if any check constraint references the function
        for (_, metadata) in self.check_constraints() {
            if metadata.functions().any(|f| f.name() == function_name) {
                return true;
            }
        }

        // Check if any policy references the function
        for (_, metadata) in self.policies() {
            if metadata.using_functions().any(|f| f.name() == function_name) {
                return true;
            }
            if metadata.check_functions().any(|f| f.name() == function_name) {
                return true;
            }
        }

        // Check if any trigger executes the function
        for (trigger, ()) in self.triggers() {
            if trigger.function_name().is_some_and(|name| name == function_name) {
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
    fn is_table_referenced(&self, table_name: &str, table_name_quoted: bool) -> bool {
        for (fk, ()) in self.foreign_keys() {
            // Check if this FK references the table being dropped
            // and is NOT from the same table (self-referential FKs are OK to drop)
            let Some(referenced_table) = object_name_last_identifier(&fk.attribute().foreign_table)
            else {
                continue;
            };
            let Some(host_table) = object_name_last_identifier(&fk.table().name) else {
                continue;
            };

            let referenced_matches = identifiers_match(
                referenced_table.value.as_str(),
                referenced_table.quote_style.is_some(),
                table_name,
                table_name_quoted,
            );
            let host_matches = identifiers_match(
                host_table.value.as_str(),
                host_table.quote_style.is_some(),
                table_name,
                table_name_quoted,
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
    fn remove_table(&mut self, table_name: &str, table_name_quoted: bool) {
        use crate::traits::TableLike;

        // Remove the table
        self.tables_mut().retain(|(t, _)| {
            !identifiers_match(
                t.table_name(),
                t.table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove columns belonging to this table
        self.columns_mut().retain(|(c, ())| {
            !identifiers_match(
                TableAttribute::table(c).table_name(),
                TableAttribute::table(c).table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove indices on this table
        self.indices_mut().retain(|(i, _)| {
            !identifiers_match(
                TableAttribute::table(i).table_name(),
                TableAttribute::table(i).table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove unique indices on this table
        self.unique_indices_mut().retain(|(u, _)| {
            !identifiers_match(
                TableAttribute::table(u).table_name(),
                TableAttribute::table(u).table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove foreign keys from this table
        self.foreign_keys_mut().retain(|(fk, ())| {
            !identifiers_match(
                TableAttribute::table(fk).table_name(),
                TableAttribute::table(fk).table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove check constraints on this table
        self.check_constraints_mut().retain(|(c, _)| {
            !identifiers_match(
                TableAttribute::table(c).table_name(),
                TableAttribute::table(c).table_name_is_quoted(),
                table_name,
                table_name_quoted,
            )
        });

        // Remove triggers on this table
        self.triggers_mut().retain(|(t, ())| {
            object_name_last_identifier(&t.table_name).is_none_or(|ident| {
                !identifiers_match(
                    ident.value.as_str(),
                    ident.quote_style.is_some(),
                    table_name,
                    table_name_quoted,
                )
            })
        });

        // Remove policies on this table
        self.policies_mut().retain(|(p, _)| {
            object_name_last_identifier(&p.table_name).is_none_or(|ident| {
                !identifiers_match(
                    ident.value.as_str(),
                    ident.quote_style.is_some(),
                    table_name,
                    table_name_quoted,
                )
            })
        });

        // Remove table grants for this table
        self.table_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| {
                object_name_last_identifier(t).is_some_and(|ident| {
                    identifiers_match(
                        ident.value.as_str(),
                        ident.quote_style.is_some(),
                        table_name,
                        table_name_quoted,
                    )
                })
            }))
        });

        // Remove column grants for this table
        self.column_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| {
                object_name_last_identifier(t).is_some_and(|ident| {
                    identifiers_match(
                        ident.value.as_str(),
                        ident.quote_style.is_some(),
                        table_name,
                        table_name_quoted,
                    )
                })
            }))
        });
    }

    /// Checks if a role with the given name is referenced by any grants.
    ///
    /// Returns `true` if the role is a grantee in any table or column grant.
    fn is_role_referenced(&self, role_name: &str) -> bool {
        use sqlparser::ast::GranteeName;

        let check_grantees = |grantees: &[sqlparser::ast::Grantee]| -> bool {
            grantees.iter().any(|g| {
                matches!(
                    &g.name,
                    Some(GranteeName::ObjectName(name)) if last_str(name) == role_name
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

fn quoted_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('\"', "\"\""))
}

fn render_table_candidate(table: &CreateTable) -> String {
    let table_name = if table.table_name_is_quoted() {
        quoted_identifier(table.table_name())
    } else {
        table.table_name().to_string()
    };

    match table.table_schema() {
        Some(schema_name) => {
            let schema_name = if table.table_schema_is_quoted() {
                quoted_identifier(schema_name)
            } else {
                schema_name.to_string()
            };
            format!("{schema_name}.{table_name}")
        }
        None => table_name,
    }
}

fn object_name_identifiers<'a>(
    object_name: &'a ObjectName,
) -> Result<(Option<&'a Ident>, &'a Ident), LookupError> {
    if object_name.0.is_empty() {
        return Err(LookupError::InvalidObjectName {
            object_name: object_name.to_string(),
            reason: "name has no identifier parts".to_string(),
        });
    }
    if object_name.0.len() > 2 {
        return Err(LookupError::InvalidObjectName {
            object_name: object_name.to_string(),
            reason: "only one-part or two-part object names are supported".to_string(),
        });
    }

    let mut idents: Vec<&Ident> = Vec::with_capacity(object_name.0.len());
    for part in &object_name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => idents.push(ident),
            _ => {
                return Err(LookupError::InvalidObjectName {
                    object_name: object_name.to_string(),
                    reason: "all object name parts must be identifiers".to_string(),
                });
            }
        }
    }

    if idents.len() == 1 { Ok((None, idents[0])) } else { Ok((Some(idents[0]), idents[1])) }
}

fn table_matches_lookup_idents(
    table: &CreateTable,
    schema_ident: Option<&Ident>,
    table_ident: &Ident,
) -> bool {
    if !identifiers_match(
        table.table_name(),
        table.table_name_is_quoted(),
        table_ident.value.as_str(),
        table_ident.quote_style.is_some(),
    ) {
        return false;
    }

    match (schema_ident, table.table_schema()) {
        (None, None) => true,
        (Some(schema_ident), Some(table_schema)) => {
            identifiers_match(
                table_schema,
                table.table_schema_is_quoted(),
                schema_ident.value.as_str(),
                schema_ident.quote_style.is_some(),
            )
        }
        _ => false,
    }
}

fn resolve_table_from_candidates<'a>(
    object_name: &ObjectName,
    candidates: Vec<&'a CreateTable>,
) -> Result<Option<&'a CreateTable>, LookupError> {
    match candidates.as_slice() {
        [] => Ok(None),
        [table] => Ok(Some(*table)),
        _ => {
            let mut rendered: Vec<String> =
                candidates.iter().copied().map(render_table_candidate).collect();
            rendered.sort_unstable();
            rendered.dedup();
            Err(LookupError::AmbiguousTableLookup {
                object_name: object_name.to_string(),
                candidates: rendered,
            })
        }
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

fn resolve_table_object_name_in_iter<'a>(
    tables: impl Iterator<Item = &'a CreateTable>,
    object_name: &ObjectName,
) -> Result<Option<&'a CreateTable>, LookupError> {
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    let candidates: Vec<&CreateTable> = tables
        .filter(|table| table_matches_lookup_idents(table, schema_ident, table_ident))
        .collect();
    resolve_table_from_candidates(object_name, candidates)
}

fn resolve_table_object_name_with_implicit_public_in_iter<'a>(
    tables: impl Iterator<Item = &'a CreateTable>,
    object_name: &ObjectName,
) -> Result<Option<&'a CreateTable>, LookupError> {
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    let table_refs: Vec<&CreateTable> = tables.collect();

    if schema_ident.is_some() {
        return resolve_table_object_name_in_iter(table_refs.into_iter(), object_name);
    }

    let unqualified_candidates: Vec<&CreateTable> = table_refs
        .iter()
        .copied()
        .filter(|table| table_matches_lookup_idents(table, None, table_ident))
        .collect();
    let unqualified = resolve_table_from_candidates(object_name, unqualified_candidates)?;

    let public_candidates: Vec<&CreateTable> = table_refs
        .iter()
        .copied()
        .filter(|table| {
            table.table_schema().is_some_and(|schema_name| {
                identifiers_match(schema_name, table.table_schema_is_quoted(), "public", false)
            }) && identifiers_match(
                table.table_name(),
                table.table_name_is_quoted(),
                table_ident.value.as_str(),
                table_ident.quote_style.is_some(),
            )
        })
        .collect();
    let public_lookup_name = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("public")),
        ObjectNamePart::Identifier(table_ident.clone()),
    ]);
    let public = resolve_table_from_candidates(&public_lookup_name, public_candidates)?;

    match (unqualified, public) {
        (Some(unqualified), Some(public)) => {
            if std::ptr::eq(unqualified, public) {
                Ok(Some(unqualified))
            } else {
                let mut candidates =
                    vec![render_table_candidate(unqualified), render_table_candidate(public)];
                candidates.sort_unstable();
                candidates.dedup();
                Err(LookupError::AmbiguousTableLookup {
                    object_name: object_name.to_string(),
                    candidates,
                })
            }
        }
        (Some(table), None) | (None, Some(table)) => Ok(Some(table)),
        (None, None) => Ok(None),
    }
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
        (Some(table_schema), Some(schema_name)) => identifiers_match(
            table_schema,
            table.table_schema_is_quoted(),
            schema_name,
            schema_quoted,
        ),
        _ => false,
    }
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
>;

impl ParserDB {
    /// Resolves a schema using a parsed SQL identifier.
    ///
    /// Resolution follows PostgreSQL identifier rules:
    /// - quoted identifiers are exact/case-sensitive;
    /// - unquoted identifiers are folded to lowercase.
    #[must_use]
    pub fn resolve_schema_ident(&self, ident: &Ident) -> Option<&Schema> {
        resolve_schema_ident_in_iter(self.schemas.iter().map(|(schema, _)| schema.as_ref()), ident)
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
    fn create_index_expression(columns: &[IndexColumn]) -> Option<Expr> {
        if columns.is_empty() {
            return None;
        }
        let expression_string = format!(
            "({})",
            columns.iter().map(|ident| ident.column.to_string()).collect::<Vec<_>>().join(", ")
        );
        Parser::new(&GenericDialect)
            .try_with_sql(expression_string.as_str())
            .ok()?
            .parse_expr()
            .ok()
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
        builder
            .tables_mut()
            .sort_by(|(a, _), (b, _)| (a.table_schema(), a.table_name()).cmp(&(b.table_schema(), b.table_name())));

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
                .chain(std::iter::once(create_table.as_ref())),
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
                    let mut primary_key_columns = Vec::new();
                    for col_name in &pk.columns {
                        let Expr::Identifier(column_name) = &col_name.column.expr else {
                            unreachable!(
                                "Unexpected expression in primary key column: {:?}",
                                col_name
                            )
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
        let mut builder: ParserDBBuilder = super::GenericDBBuilder::new(catalog_name);

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
                return_type: Some(return_type),
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
                        let function_name = last_str(&func_desc.name);

                        // Check if function exists
                        let function_exists =
                            builder.function_arc_vec().iter().any(|f| f.name() == function_name);

                        if !function_exists {
                            if drop_function.if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropFunctionNotFound {
                                function_name: function_name.to_string(),
                            });
                        }

                        // Check for references in check constraints, policies, or triggers
                        if builder.is_function_used(function_name) {
                            return Err(crate::errors::Error::FunctionReferenced {
                                function_name: function_name.to_string(),
                            });
                        }

                        // Remove the function
                        let functions = builder.functions_mut();
                        functions.retain(|(f, ())| f.name() != function_name);
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

                        // Check for references from other tables (unless CASCADE)
                        if !cascade
                            && builder
                                .is_table_referenced(&resolved_table_name, resolved_table_quoted)
                        {
                            return Err(crate::errors::Error::TableReferenced {
                                table_name: resolved_table_name.clone(),
                            });
                        }

                        // Remove the table and all associated objects
                        builder.remove_table(&resolved_table_name, resolved_table_quoted);
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
                        let function_name = last_str(&exec_body.func_desc.name);
                        let function_exists =
                            builder.function_arc_vec().iter().any(|f| f.name() == function_name);

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
                        let role_name = last_str(&name);

                        // Check if role exists
                        let role_exists = builder.roles().iter().any(|(r, ())| {
                            r.names.first().is_some_and(|n| last_str(n) == role_name)
                        });

                        if !role_exists {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropRoleNotFound {
                                role_name: role_name.to_string(),
                            });
                        }

                        // Check for references from grants
                        if builder.is_role_referenced(role_name) {
                            return Err(crate::errors::Error::RoleReferenced {
                                role_name: role_name.to_string(),
                            });
                        }

                        // Remove the role
                        builder.roles_mut().retain(|(r, ())| {
                            r.names.first().is_none_or(|n| last_str(n) != role_name)
                        });
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
                                    (t.table_name().to_string(), t.table_name_is_quoted())
                                })
                                .collect();

                            for (table_name, table_name_quoted) in tables_to_remove {
                                builder.remove_table(&table_name, table_name_quoted);
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
                                let Some(resolved_table) =
                                    builder.resolve_table_object_name(&alter_table.name)?
                                else {
                                    continue;
                                };
                                let resolved_table_name = resolved_table.table_name().to_string();
                                let resolved_table_quoted = resolved_table.table_name_is_quoted();
                                let resolved_schema_name =
                                    resolved_table.table_schema().map(str::to_string);
                                let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

                                if let Some(entry) = builder.tables_mut().iter_mut().find(
                                    |(table, _)| {
                                        table_matches_resolved_identity(
                                            table.as_ref(),
                                            &resolved_table_name,
                                            resolved_table_quoted,
                                            resolved_schema_name.as_deref(),
                                            resolved_schema_quoted,
                                        )
                                    },
                                ) {
                                    entry.1.set_rls_enabled(true);
                                }
                            }
                            AlterTableOperation::DisableRowLevelSecurity => {
                                let Some(resolved_table) =
                                    builder.resolve_table_object_name(&alter_table.name)?
                                else {
                                    continue;
                                };
                                let resolved_table_name = resolved_table.table_name().to_string();
                                let resolved_table_quoted = resolved_table.table_name_is_quoted();
                                let resolved_schema_name =
                                    resolved_table.table_schema().map(str::to_string);
                                let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

                                if let Some(entry) = builder.tables_mut().iter_mut().find(
                                    |(table, _)| {
                                        table_matches_resolved_identity(
                                            table.as_ref(),
                                            &resolved_table_name,
                                            resolved_table_quoted,
                                            resolved_schema_name.as_deref(),
                                            resolved_schema_quoted,
                                        )
                                    },
                                ) {
                                    entry.1.set_rls_enabled(false);
                                }
                            }
                            AlterTableOperation::ForceRowLevelSecurity => {
                                let Some(resolved_table) =
                                    builder.resolve_table_object_name(&alter_table.name)?
                                else {
                                    continue;
                                };
                                let resolved_table_name = resolved_table.table_name().to_string();
                                let resolved_table_quoted = resolved_table.table_name_is_quoted();
                                let resolved_schema_name =
                                    resolved_table.table_schema().map(str::to_string);
                                let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

                                if let Some(entry) = builder.tables_mut().iter_mut().find(
                                    |(table, _)| {
                                        table_matches_resolved_identity(
                                            table.as_ref(),
                                            &resolved_table_name,
                                            resolved_table_quoted,
                                            resolved_schema_name.as_deref(),
                                            resolved_schema_quoted,
                                        )
                                    },
                                ) {
                                    entry.1.set_rls_forced(true);
                                }
                            }
                            AlterTableOperation::NoForceRowLevelSecurity => {
                                let Some(resolved_table) =
                                    builder.resolve_table_object_name(&alter_table.name)?
                                else {
                                    continue;
                                };
                                let resolved_table_name = resolved_table.table_name().to_string();
                                let resolved_table_quoted = resolved_table.table_name_is_quoted();
                                let resolved_schema_name =
                                    resolved_table.table_schema().map(str::to_string);
                                let resolved_schema_quoted = resolved_table.table_schema_is_quoted();

                                if let Some(entry) = builder.tables_mut().iter_mut().find(
                                    |(table, _)| {
                                        table_matches_resolved_identity(
                                            table.as_ref(),
                                            &resolved_table_name,
                                            resolved_table_quoted,
                                            resolved_schema_name.as_deref(),
                                            resolved_schema_quoted,
                                        )
                                    },
                                ) {
                                    entry.1.set_rls_forced(false);
                                }
                            }
                            AlterTableOperation::RenameTable { table_name } => {
                                let new_name = match table_name {
                                    RenameTableNameKind::As(name) | RenameTableNameKind::To(name) => {
                                        name
                                    }
                                };
                                builder = Self::rename_table_checked(
                                    builder,
                                    &alter_table.name,
                                    new_name,
                                    alter_table.if_exists,
                                )?;
                            }
                            _ => {}
                        }
                    }
                }
                Statement::CreateTable(create_table) => {
                    let create_table = Arc::new(create_table);
                    let mut table_metadata: TableMetadata<CreateTable> = TableMetadata::default();

                    for column in create_table.columns.clone() {
                        let column_arc =
                            Arc::new(TableAttribute::new(create_table.clone(), column));
                        table_metadata.add_column(column_arc.clone());
                    }

                    for column in table_metadata.clone().column_arcs() {
                        builder = Self::process_column_options(
                            column,
                            &create_table,
                            &mut table_metadata,
                            builder,
                        )?;
                        builder = builder.add_column(column.clone(), ());
                    }

                    builder = Self::process_table_constraints(
                        &create_table.constraints,
                        &create_table,
                        &mut table_metadata,
                        builder,
                    )?;

                    builder = builder.add_table(create_table, table_metadata)?;
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
                    // Validate grantees exist (closed world assumption)
                    for grantee in &grant.grantees {
                        let grantee_name = match &grantee.name {
                            Some(sqlparser::ast::GranteeName::ObjectName(name)) => {
                                Some(last_str(name))
                            }
                            _ => None,
                        };

                        if let Some(name) = grantee_name {
                            // Skip PUBLIC pseudo-role
                            if name.to_uppercase() != "PUBLIC" {
                                let role_exists = builder.roles().iter().any(|(r, ())| {
                                    r.names.first().is_some_and(|n| last_str(n) == name)
                                });
                                if !role_exists {
                                    return Err(crate::errors::Error::RoleNotFoundForGrant {
                                        role_name: name.to_string(),
                                    });
                                }
                            }
                        }
                    }

                    // Validate tables exist (for table grants)
                    if let Some(sqlparser::ast::GrantObjects::Tables(tables)) = &grant.objects {
                        for table_obj in tables {
                            let table_name = last_str(table_obj);
                            let table_exists = builder.resolve_table_object_name(table_obj)?.is_some();
                            if !table_exists {
                                return Err(crate::errors::Error::TableNotFoundForGrant {
                                    table_name: table_name.to_string(),
                                });
                            }
                        }
                    }

                    builder = builder.add_table_grant(Arc::new(grant.clone()), ());
                    builder = builder.add_column_grant(Arc::new(grant), ());
                }
                Statement::Revoke(revoke) => {
                    // Find and remove matching grants from both table and column grants
                    let table_grants = builder.table_grants_mut();
                    let original_len = table_grants.len();
                    table_grants.retain(|(grant, ())| {
                        !crate::impls::grant_matches_revoke(grant.as_ref(), &revoke)
                    });
                    let table_grants_removed = table_grants.len() < original_len;

                    let column_grants = builder.column_grants_mut();
                    column_grants.retain(|(grant, ())| {
                        !crate::impls::grant_matches_revoke(grant.as_ref(), &revoke)
                    });

                    if !table_grants_removed {
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
                    let resolved_schema =
                        object_name_last_identifier(&name).and_then(|ident| builder.resolve_schema_ident(ident));

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
                Statement::Comment { object_type: _, object_name: _, comment: _, if_exists: _ } => {
                    // COMMENT ON statements set comments on database objects.
                    // Currently, table documentation is extracted from SQL
                    // comments (lines starting with --) via
                    // the sql_docs crate. COMMENT ON
                    // TABLE/COLUMN would be a different mechanism.
                    // For now, we acknowledge but don't store these comments.
                    // TODO: Store COMMENT ON statements when comment metadata
                    // field is added to the appropriate metadata structs.
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
    pub fn parse<D: Dialect + Default>(sql: &str) -> Result<Self, crate::errors::Error> {
        let dialect = D::default();
        let mut parser = Parser::new(&dialect).try_with_sql(sql)?;
        let statements = parser.parse_statements()?;
        let mut db = Self::from_statements(statements, "unknown_catalog".to_string())?;

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
    pub fn from_paths<D: Dialect + Default>(paths: &[&Path]) -> Result<Self, crate::errors::Error> {
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

        let mut db = Self::from_statements(statements, "unknown_catalog".to_string())?;

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

fn search_sql_documents(path: &Path) -> Vec<PathBuf> {
    let mut sql_files = Vec::new();
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("Failed to read directory") {
            let entry = entry.expect("Failed to read directory entry");
            let path = entry.path();
            if path.is_dir() {
                sql_files.extend(search_sql_documents(&path));
            } else if let Some(extension) = path.extension()
                && extension == "sql"
                && path.file_name().unwrap() != "down.sql"
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
                r#"
                CREATE TABLE Foo (id INT);
                ALTER TABLE FOO ENABLE ROW LEVEL SECURITY;
                "#,
            );
            let foo = db
                .table(None, "foo")
                .expect("Expected `foo` table to exist after ALTER TABLE");
            assert!(
                foo.has_row_level_security(&db),
                "Unquoted ALTER TABLE lookup should resolve via identifier folding"
            );

            let db = parse_postgres(
                r#"
                CREATE TABLE Foo (id INT);
                ALTER TABLE "Foo" ENABLE ROW LEVEL SECURITY;
                "#,
            );
            let foo = db
                .table(None, "foo")
                .expect("Expected `foo` table to exist after ALTER TABLE");
            assert!(
                !foo.has_row_level_security(&db),
                "Quoted ALTER TABLE name should not match unquoted table with different case"
            );
        }

        #[test]
        fn grant_table_lookup_uses_resolver_rules() {
            let sql = r#"
                CREATE TABLE Foo (id INT);
                CREATE ROLE app_role;
                GRANT SELECT ON FOO TO app_role;
            "#;
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
                r#"
                CREATE SCHEMA s1;
                CREATE SCHEMA s2;
                CREATE TABLE s1.t (id INT);
                CREATE TABLE s2.t (id INT);
                CREATE INDEX idx_s2_t_id ON s2.t (id);
                "#,
            );

            let s1_t = db
                .resolve_table_object_name(&object_name(&[("s1", false), ("t", false)]))
                .expect("Lookup should succeed")
                .expect("Expected table s1.t to exist");
            let s2_t = db
                .resolve_table_object_name(&object_name(&[("s2", false), ("t", false)]))
                .expect("Lookup should succeed")
                .expect("Expected table s2.t to exist");

            assert_eq!(s1_t.indices(&db).count(), 0);
            assert_eq!(s2_t.indices(&db).count(), 1);
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

            assert_eq!(unquoted.indices(&db).count(), 0);
            assert_eq!(quoted.indices(&db).count(), 1);
        }

        #[test]
        fn rename_table_lookup_uses_resolver_rules() {
            let sql = r#"
                CREATE TABLE Foo (id INT);
                ALTER TABLE FOO RENAME TO bar;
            "#;
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
            let sql = r#"
                CREATE TABLE Foo (id INT);
                RENAME TABLE FOO TO bar;
            "#;
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
            let sql = r#"
                CREATE SCHEMA foo;
                CREATE SCHEMA bar;
                ALTER SCHEMA bar RENAME TO FOO;
            "#;

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
            let sql = r#"
                CREATE TABLE t (id INT);
                CREATE TABLE public.t (id INT);
            "#;

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
            assert_eq!(t2.columns(&db).count(), 1);
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
            assert_eq!(t2.indices(&db).count(), 1);

            // Total indices across all tables should be 1 (only t2's index)
            let total_indices: usize = db.tables().map(|t| t.indices(&db).count()).sum();
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
            assert_eq!(parent.foreign_keys(&db).count(), 0);

            // No foreign keys in the database (child's FK was removed with child)
            let total_fks: usize = db.tables().map(|t| t.foreign_keys(&db).count()).sum();
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
            assert_eq!(t2.check_constraints(&db).count(), 1);
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
            assert_eq!(t2.triggers(&db).count(), 1);
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
            assert_eq!(t2.policies(&db).count(), 1);
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
            assert_eq!(table.indices(&db).count(), 0);
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
            assert_eq!(table.indices(&db).count(), 0);
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
            assert_eq!(table.indices(&db).count(), 1);
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
            assert_eq!(table.indices(&db).count(), 1);
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
            assert_eq!(t1.indices(&db).count(), 0);

            // t2 should still have its index
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.indices(&db).count(), 1);
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
            assert_eq!(table.columns(&db).count(), 3);
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
            assert_eq!(table.triggers(&db).count(), 0);
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
            assert_eq!(table.triggers(&db).count(), 0);
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
            assert_eq!(table.triggers(&db).count(), 1);
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
            assert_eq!(table.triggers(&db).count(), 1);
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
            assert_eq!(t1.triggers(&db).count(), 0);

            // t2 should still have its trigger
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.triggers(&db).count(), 1);
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
            assert_eq!(table.policies(&db).count(), 0);
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
            assert_eq!(table.policies(&db).count(), 0);
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
            assert_eq!(table.policies(&db).count(), 1);
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
            assert_eq!(table.policies(&db).count(), 1);
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
            assert_eq!(t1.policies(&db).count(), 0);

            // t2 should still have its policy
            let t2 = db.table(None, "t2").expect("t2 should exist");
            assert_eq!(t2.policies(&db).count(), 1);
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
            assert_eq!(table.columns(&db).count(), 2);
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
}
