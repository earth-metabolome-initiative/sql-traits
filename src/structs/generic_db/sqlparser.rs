//! Implementations for [`ParserDB`] - a database schema parsed from SQL text.

use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

use git2::Repository;
use sql_docs::SqlDoc;
use sqlparser::{
    ast::{
        AlterTableOperation, CheckConstraint, ColumnDef, ColumnOption, CreateFunction,
        CreateFunctionBody, CreateIndex, CreatePolicy, CreateRole, CreateTable, CreateTrigger,
        DataType, ExactNumberInfo, Expr, ForeignKeyConstraint, Grant, Ident, IndexColumn,
        ObjectName, ObjectNamePart, OperateFunctionArg, OrderByExpr, OrderByOptions, Statement,
        TableConstraint, TimezoneInfo, UniqueConstraint, Value, ValueWithSpan,
    },
    dialect::{Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::Span,
};

use crate::{
    structs::{
        GenericDB, TableAttribute, TableMetadata,
        metadata::{CheckMetadata, IndexMetadata, PolicyMetadata, UniqueIndexMetadata},
    },
    traits::{ColumnLike, FunctionLike, TableLike},
    utils::{columns_in_expression, last_str},
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
    Grant,
    Grant,
>;

/// A type alias for the result of processing check constraints.
type CheckConstraintResult =
    (Vec<Rc<TableAttribute<CreateTable, ColumnDef>>>, Vec<Rc<CreateFunction>>);

/// A type alias for the result of processing unique constraints.
type UniqueConstraintResult = (
    Rc<TableAttribute<CreateTable, UniqueConstraint>>,
    UniqueIndexMetadata<TableAttribute<CreateTable, UniqueConstraint>>,
);

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
    Grant,
    Grant,
>;

impl ParserDB {
    /// Helper function to process check constraints.
    fn process_check_constraint(
        check_expr: &Expr,
        create_table: &Rc<CreateTable>,
        table_metadata: &TableMetadata<CreateTable>,
        builder: &ParserDBBuilder,
    ) -> Result<CheckConstraintResult, crate::errors::Error> {
        let columns_in_expression =
            columns_in_expression::<Rc<TableAttribute<CreateTable, ColumnDef>>>(
                check_expr,
                &create_table.name.to_string(),
                table_metadata.column_rc_slice(),
            )?;
        let functions_in_expression = functions_in_expression::functions_in_expression::<Self>(
            check_expr,
            builder.function_rc_vec().as_slice(),
        );
        Ok((columns_in_expression, functions_in_expression))
    }

    /// Helper function to create an index expression from columns.
    fn create_index_expression(columns: &[IndexColumn]) -> Expr {
        let expression_string = format!(
            "({})",
            columns.iter().map(|ident| ident.column.to_string()).collect::<Vec<_>>().join(", ")
        );
        Parser::new(&GenericDialect)
            .try_with_sql(expression_string.as_str())
            .expect("Failed to parse index constraint expression")
            .parse_expr()
            .expect("No expression found in parsed index constraint")
    }

    /// Helper function to process unique constraints.
    fn process_unique_constraint(
        unique_constraint: UniqueConstraint,
        create_table: &Rc<CreateTable>,
    ) -> UniqueConstraintResult {
        let unique_index = Rc::new(TableAttribute::new(create_table.clone(), unique_constraint));
        let expression = Self::create_index_expression(&unique_index.attribute().columns);
        let unique_index_metadata = UniqueIndexMetadata::new(expression, create_table.clone());
        (unique_index, unique_index_metadata)
    }

    #[allow(clippy::type_complexity)]
    /// Helper function to process create index statements.
    fn process_create_index(
        create_index: CreateIndex,
        builder: &ParserDBBuilder,
    ) -> Result<
        (
            Rc<TableAttribute<CreateTable, CreateIndex>>,
            IndexMetadata<TableAttribute<CreateTable, CreateIndex>>,
        ),
        crate::errors::Error,
    > {
        let table_name = last_str(&create_index.table_name);

        let Some((table, _)) =
            builder.tables().iter().find(|(t, _)| t.name.to_string() == table_name)
        else {
            return Err(crate::errors::Error::TableNotFoundForIndex {
                table_name: table_name.to_string(),
                index_name: create_index.name.as_ref().map_or("<unnamed>", last_str).to_string(),
            });
        };

        let index_rc = Rc::new(TableAttribute::new(table.clone(), create_index));
        let expression = Self::create_index_expression(&index_rc.attribute().columns);
        let metadata = IndexMetadata::new(expression, table.clone());
        Ok((index_rc, metadata))
    }

    /// Helper function to process column options.
    fn process_column_options(
        column: &Rc<TableAttribute<CreateTable, ColumnDef>>,
        create_table: &Rc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        mut builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for option in &column.attribute().options {
            match option.option.clone() {
                ColumnOption::Check(check_constraint) => {
                    let check_rc = Rc::new(TableAttribute::new(
                        create_table.clone(),
                        check_constraint.clone(),
                    ));
                    table_metadata.add_check_constraint(check_rc.clone());
                    let (columns_in_expression, functions_in_expression) =
                        Self::process_check_constraint(
                            &check_constraint.expr,
                            create_table,
                            table_metadata,
                            &builder,
                        )?;
                    builder = builder.add_check_constraint(
                        check_rc,
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
                    let fk = Rc::new(TableAttribute::new(create_table.clone(), foreign_key));
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
                    let (unique_index, unique_index_metadata) =
                        Self::process_unique_constraint(unique_constraint, create_table);
                    table_metadata.add_unique_index(unique_index.clone());
                    builder = builder.add_unique_index(unique_index, unique_index_metadata);
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

                    let (unique_index, unique_index_metadata) = Self::process_unique_constraint(
                        primary_key_unique_constraint,
                        create_table,
                    );
                    table_metadata.add_unique_index(unique_index.clone());
                    builder = builder.add_unique_index(unique_index, unique_index_metadata);

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
        create_table: &Rc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for col_ident in &fk.columns {
            let column_exists = table_metadata
                .column_rcs()
                .any(|col| col.column_name() == col_ident.value.as_str());

            if !column_exists {
                return Err(crate::errors::Error::HostColumnNotFoundForForeignKey {
                    host_column: col_ident.value.clone(),
                    host_table: create_table.name.to_string(),
                });
            }
        }

        let referenced_table_name = fk.foreign_table.to_string();

        let Some(referenced_table) = builder
            .tables()
            .iter()
            .map(|(t, _)| t.as_ref())
            .chain(std::iter::once(create_table.as_ref()))
            .find(|t| t.name.to_string() == referenced_table_name)
        else {
            return Err(crate::errors::Error::ReferencedTableNotFoundForForeignKey {
                referenced_table: referenced_table_name.clone(),
                host_table: create_table.name.to_string(),
            });
        };

        for ref_col_ident in &fk.referred_columns {
            let column_exists = referenced_table
                .columns
                .iter()
                .any(|col| col.name.value.as_str() == ref_col_ident.value.as_str());

            if !column_exists {
                return Err(crate::errors::Error::ReferencedColumnNotFoundForForeignKey {
                    referenced_column: ref_col_ident.value.clone(),
                    referenced_table: referenced_table_name.clone(),
                    host_table: create_table.name.to_string(),
                });
            }
        }

        let fk_rc = Rc::new(TableAttribute::new(create_table.clone(), fk.clone()));
        table_metadata.add_foreign_key(fk_rc.clone());
        let builder = builder.add_foreign_key(fk_rc, ());
        Ok(builder)
    }

    /// Helper function to process table constraints.
    fn process_table_constraints(
        constraints: &[TableConstraint],
        create_table: &Rc<CreateTable>,
        table_metadata: &mut TableMetadata<CreateTable>,
        mut builder: ParserDBBuilder,
    ) -> Result<ParserDBBuilder, crate::errors::Error> {
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique(uc) => {
                    let (unique_index, unique_index_metadata) =
                        Self::process_unique_constraint(uc.clone(), create_table);
                    table_metadata.add_unique_index(unique_index.clone());
                    builder = builder.add_unique_index(unique_index, unique_index_metadata);
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
                    let check_rc =
                        Rc::new(TableAttribute::new(create_table.clone(), check.clone()));
                    table_metadata.add_check_constraint(check_rc.clone());
                    let (columns_in_expression, functions_in_expression) =
                        Self::process_check_constraint(
                            &check.expr,
                            create_table,
                            table_metadata,
                            &builder,
                        )?;
                    builder = builder.add_check_constraint(
                        check_rc,
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
                                .column_rcs()
                                .filter(|col: &&Rc<TableAttribute<CreateTable, ColumnDef>>| {
                                    col.column_name() == column_name.value.as_str()
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

                    let (unique_index, unique_index_metadata) = Self::process_unique_constraint(
                        primary_key_unique_constraint,
                        create_table,
                    );
                    table_metadata.add_unique_index(unique_index.clone());
                    builder = builder.add_unique_index(unique_index, unique_index_metadata);

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
            builder = builder.add_function(Rc::new(create_function), ());
        }

        for statement in statements {
            match statement {
                Statement::CreateFunction(create_function) => {
                    builder = builder.add_function(Rc::new(create_function), ());
                }
                Statement::CreateTrigger(create_trigger) => {
                    let table_name = last_str(&create_trigger.table_name);
                    let table_exists =
                        builder.tables().iter().any(|(t, _)| t.name.to_string() == table_name);

                    if !table_exists {
                        return Err(crate::errors::Error::TableNotFoundForTrigger {
                            table_name: table_name.to_string(),
                            trigger_name: last_str(&create_trigger.name).to_string(),
                        });
                    }

                    if let Some(exec_body) = &create_trigger.exec_body {
                        let function_name = last_str(&exec_body.func_desc.name);
                        let function_exists =
                            builder.function_rc_vec().iter().any(|f| f.name() == function_name);

                        if !function_exists {
                            return Err(crate::errors::Error::FunctionNotFoundForTrigger {
                                function_name: function_name.to_string(),
                                trigger_name: last_str(&create_trigger.name).to_string(),
                            });
                        }
                    }

                    builder = builder.add_trigger(Rc::new(create_trigger), ());
                }
                Statement::CreateIndex(create_index) => {
                    let (index, metadata) = Self::process_create_index(create_index, &builder)?;
                    let table_name = index.table().table_name();
                    if let Some(entry) = builder
                        .tables_mut()
                        .iter_mut()
                        .find(|entry| entry.0.table_name() == table_name)
                    {
                        entry.1.add_index(index.clone());
                    }
                    builder = builder.add_index(index, metadata);
                }
                Statement::AlterTable(alter_table) => {
                    let table_name = last_str(&alter_table.name);

                    for operation in alter_table.operations {
                        match operation {
                            AlterTableOperation::EnableRowLevelSecurity => {
                                if let Some(entry) = builder
                                    .tables_mut()
                                    .iter_mut()
                                    .find(|entry| entry.0.table_name() == table_name)
                                {
                                    entry.1.set_rls_enabled(true);
                                }
                            }
                            AlterTableOperation::DisableRowLevelSecurity => {
                                if let Some(entry) = builder
                                    .tables_mut()
                                    .iter_mut()
                                    .find(|entry| entry.0.table_name() == table_name)
                                {
                                    entry.1.set_rls_enabled(false);
                                }
                            }
                            AlterTableOperation::ForceRowLevelSecurity => {
                                if let Some(entry) = builder
                                    .tables_mut()
                                    .iter_mut()
                                    .find(|entry| entry.0.table_name() == table_name)
                                {
                                    entry.1.set_rls_forced(true);
                                }
                            }
                            AlterTableOperation::NoForceRowLevelSecurity => {
                                if let Some(entry) = builder
                                    .tables_mut()
                                    .iter_mut()
                                    .find(|entry| entry.0.table_name() == table_name)
                                {
                                    entry.1.set_rls_forced(false);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Statement::CreateTable(create_table) => {
                    let create_table = Rc::new(create_table);
                    let mut table_metadata: TableMetadata<CreateTable> = TableMetadata::default();

                    for column in create_table.columns.clone() {
                        let column_rc = Rc::new(TableAttribute::new(create_table.clone(), column));
                        table_metadata.add_column(column_rc.clone());
                    }

                    for column in table_metadata.clone().column_rcs() {
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

                    builder = builder.add_table(create_table, table_metadata);
                }
                Statement::CreatePolicy(policy) => {
                    let using_functions = if let Some(using_expr) = &policy.using {
                        functions_in_expression::functions_in_expression::<Self>(
                            using_expr,
                            builder.function_rc_vec().as_slice(),
                        )
                    } else {
                        Vec::new()
                    };

                    let check_functions = if let Some(check_expr) = &policy.with_check {
                        functions_in_expression::functions_in_expression::<Self>(
                            check_expr,
                            builder.function_rc_vec().as_slice(),
                        )
                    } else {
                        Vec::new()
                    };

                    let metadata = PolicyMetadata::new(using_functions, check_functions);
                    builder = builder.add_policy(Rc::new(policy), metadata);
                }
                Statement::CreateRole(create_role) => {
                    builder = builder.add_role(Rc::new(create_role), ());
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
                            let table_exists =
                                builder.tables().iter().any(|(t, _)| t.table_name() == table_name);
                            if !table_exists {
                                return Err(crate::errors::Error::TableNotFoundForGrant {
                                    table_name: table_name.to_string(),
                                });
                            }
                        }
                    }

                    builder = builder.add_table_grant(Rc::new(grant.clone()), ());
                    builder = builder.add_column_grant(Rc::new(grant), ());
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
                    } else {
                        unimplemented!(
                            "Only string literals are supported for SET TIME ZONE, found: {value:?}"
                        );
                    }
                }
                Statement::CreateOperator(_)
                | Statement::CreateOperatorClass(_)
                | Statement::CreateOperatorFamily(_)
                | Statement::CreateType { .. }
                | Statement::CreateExtension(_)
                | Statement::CreateView(_)
                | Statement::Query(_)
                | Statement::Rollback { .. }
                | Statement::Commit { .. }
                | Statement::StartTransaction { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
                | Statement::ShowVariable { .. }
                | Statement::Raise { .. }
                | Statement::Vacuum { .. }
                | Statement::Print { .. }
                | Statement::Open { .. }
                | Statement::Close { .. }
                | Statement::Fetch { .. }
                | Statement::Declare { .. }
                | Statement::Use { .. }
                | Statement::Throw { .. }
                | Statement::Load { .. }
                | Statement::Return { .. }
                | Statement::Assert { .. }
                | Statement::While { .. }
                | Statement::ExplainTable { .. }
                | Statement::Explain { .. }
                | Statement::Kill { .. }
                | Statement::LISTEN { .. }
                | Statement::UNLISTEN { .. }
                | Statement::NOTIFY { .. }
                | Statement::ShowTables { .. }
                | Statement::Analyze { .. }
                | Statement::Deallocate { .. }
                | Statement::Prepare { .. }
                | Statement::Execute { .. }
                | Statement::Set(_)
                | Statement::Pragma { .. }
                | Statement::Call(_)
                | Statement::Reset(_)
                | Statement::Truncate(_)
                | Statement::Directory { .. }
                | Statement::Discard { .. }
                | Statement::ShowViews { .. }
                | Statement::ShowFunctions { .. }
                | Statement::ShowCollation { .. }
                | Statement::ShowCreate { .. }
                | Statement::ShowSchemas { .. }
                | Statement::Update(_)
                | Statement::ShowColumns { .. }
                | Statement::Delete(_)
                | Statement::Insert(_) => {
                    // Ignored statements
                }
                _ => {
                    unimplemented!("Unsupported statement found: {statement:?}");
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
