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
    fn is_table_referenced(&self, table_name: &str) -> bool {
        for (fk, ()) in self.foreign_keys() {
            // Check if this FK references the table being dropped
            // and is NOT from the same table (self-referential FKs are OK to drop)
            let referenced_table = last_str(&fk.attribute().foreign_table);
            let host_table = last_str(&fk.table().name);
            if referenced_table == table_name && host_table != table_name {
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
    fn remove_table(&mut self, table_name: &str) {
        use crate::traits::TableLike;

        // Remove the table
        self.tables_mut().retain(|(t, _)| t.table_name() != table_name);

        // Remove columns belonging to this table
        self.columns_mut().retain(|(c, ())| last_str(&TableAttribute::table(c).name) != table_name);

        // Remove indices on this table
        self.indices_mut().retain(|(i, _)| last_str(&TableAttribute::table(i).name) != table_name);

        // Remove unique indices on this table
        self.unique_indices_mut()
            .retain(|(u, _)| last_str(&TableAttribute::table(u).name) != table_name);

        // Remove foreign keys from this table
        self.foreign_keys_mut()
            .retain(|(fk, ())| last_str(&TableAttribute::table(fk).name) != table_name);

        // Remove check constraints on this table
        self.check_constraints_mut()
            .retain(|(c, _)| last_str(&TableAttribute::table(c).name) != table_name);

        // Remove triggers on this table
        self.triggers_mut().retain(|(t, ())| last_str(&t.table_name) != table_name);

        // Remove policies on this table
        self.policies_mut().retain(|(p, _)| last_str(&p.table_name) != table_name);

        // Remove table grants for this table
        self.table_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| last_str(t) == table_name))
        });

        // Remove column grants for this table
        self.column_grants_mut().retain(|(g, ())| {
            use sqlparser::ast::GrantObjects;
            !matches!(&g.objects, Some(GrantObjects::Tables(tables)) if tables.iter().any(|t| last_str(t) == table_name))
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
}

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
                Statement::DropFunction(drop_function) => {
                    for func_desc in &drop_function.func_desc {
                        let function_name = last_str(&func_desc.name);

                        // Check if function exists
                        let function_exists =
                            builder.function_rc_vec().iter().any(|f| f.name() == function_name);

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

                        // Check if table exists
                        let table_exists = builder
                            .tables()
                            .iter()
                            .any(|(t, _)| t.table_name() == table_name);

                        if !table_exists {
                            if if_exists {
                                continue;
                            }
                            return Err(crate::errors::Error::DropTableNotFound {
                                table_name: table_name.to_string(),
                            });
                        }

                        // Check for references from other tables (unless CASCADE)
                        if !cascade && builder.is_table_referenced(table_name) {
                            return Err(crate::errors::Error::TableReferenced {
                                table_name: table_name.to_string(),
                            });
                        }

                        // Remove the table and all associated objects
                        builder.remove_table(table_name);
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
                        let index_exists = builder
                            .indices_mut()
                            .iter()
                            .any(|(idx, _)| {
                                idx.attribute()
                                    .name
                                    .as_ref()
                                    .is_some_and(|n| last_str(n) == index_name)
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
                            idx.attribute()
                                .name
                                .as_ref()
                                .is_none_or(|n| last_str(n) != index_name)
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
                Statement::DropTrigger(drop_trigger) => {
                    let trigger_name = last_str(&drop_trigger.trigger_name);

                    // Find the trigger
                    let trigger_exists = builder
                        .triggers()
                        .iter()
                        .any(|(t, ())| last_str(&t.name) == trigger_name);

                    if !trigger_exists {
                        if drop_trigger.if_exists {
                            continue;
                        }
                        return Err(crate::errors::Error::DropTriggerNotFound {
                            trigger_name: trigger_name.to_string(),
                        });
                    }

                    // Remove the trigger
                    builder
                        .triggers_mut()
                        .retain(|(t, ())| last_str(&t.name) != trigger_name);
                }
                Statement::DropPolicy(drop_policy) => {
                    let policy_name = drop_policy.name.value.as_str();

                    // Find the policy
                    let policy_exists = builder
                        .policies()
                        .iter()
                        .any(|(p, _)| p.name.value == policy_name);

                    if !policy_exists {
                        if drop_policy.if_exists {
                            continue;
                        }
                        return Err(crate::errors::Error::DropPolicyNotFound {
                            policy_name: policy_name.to_string(),
                        });
                    }

                    // Remove the policy
                    builder
                        .policies_mut()
                        .retain(|(p, _)| p.name.value != policy_name);
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
                            r.names
                                .first()
                                .is_none_or(|n| last_str(n) != role_name)
                        });
                    }
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
                // =================================================================
                // IGNORED STATEMENTS
                // These statements don't affect schema structure tracking.
                // =================================================================

                // DML statements (data manipulation, not schema)
                | Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::Merge { .. }
                | Statement::Truncate(_)

                // Transaction control
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::StartTransaction { .. }
                | Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }

                // Cursor operations
                | Statement::Declare { .. }
                | Statement::Fetch { .. }
                | Statement::Open { .. }
                | Statement::Close { .. }

                // Session/connection settings (Set variants handled above)
                | Statement::Set(_)
                | Statement::Reset(_)
                | Statement::Use { .. }

                // SHOW/EXPLAIN commands (read-only introspection)
                | Statement::ShowVariable { .. }
                | Statement::ShowVariables { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowCreate { .. }
                | Statement::ShowFunctions { .. }
                | Statement::ShowCollation { .. }
                | Statement::ShowViews { .. }
                | Statement::ShowSchemas { .. }
                | Statement::ShowCharset { .. }
                | Statement::Explain { .. }
                | Statement::ExplainTable { .. }

                // Utility/maintenance commands
                | Statement::Analyze { .. }
                | Statement::Vacuum { .. }
                | Statement::Copy { .. }
                | Statement::CopyIntoSnowflake { .. }
                | Statement::Kill { .. }
                | Statement::Flush { .. }
                | Statement::Discard { .. }
                | Statement::OptimizeTable { .. }

                // Prepared statements
                | Statement::Prepare { .. }
                | Statement::Execute { .. }
                | Statement::Deallocate { .. }

                // Procedural/control flow (PL/pgSQL, T-SQL, etc.)
                | Statement::Call(_)
                | Statement::Return { .. }
                | Statement::Raise { .. }
                | Statement::Assert { .. }
                | Statement::While { .. }
                | Statement::Throw { .. }
                | Statement::Print { .. }
                | Statement::Load { .. }

                // Locks
                | Statement::LockTables { .. }
                | Statement::UnlockTables

                // Pub/sub notifications
                | Statement::LISTEN { .. }
                | Statement::UNLISTEN { .. }
                | Statement::NOTIFY { .. }

                // Database-specific statements we don't track
                | Statement::Pragma { .. }
                | Statement::Directory { .. }
                | Statement::AttachDatabase { .. }
                | Statement::DetachDuckDBDatabase { .. }
                | Statement::Install { .. }
                | Statement::Msck { .. }
                | Statement::Cache { .. }
                | Statement::UNCache { .. }

                // Objects we're not currently tracking
                // (views, procedures, types, extensions, operators, etc.)
                | Statement::CreateView(_)
                | Statement::AlterView { .. }
                | Statement::CreateVirtualTable { .. }
                | Statement::CreateDatabase { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateSequence { .. }
                | Statement::CreateProcedure { .. }
                | Statement::CreateMacro { .. }
                | Statement::CreateStage { .. }
                | Statement::CreateType { .. }
                | Statement::CreateExtension(_)
                | Statement::CreateDomain(_)
                | Statement::DropDomain(_)
                | Statement::CreateOperator(_)
                | Statement::CreateOperatorClass(_)
                | Statement::CreateOperatorFamily(_)
                | Statement::Comment { .. }

                // Generic DROP for non-Table objects (INDEX, VIEW, etc. - not yet implemented)
                // Note: DROP TABLE is handled explicitly above
                | Statement::Drop { .. }

                // ALTER statements not yet implemented
                | Statement::AlterIndex { .. }
                | Statement::AlterRole { .. } => {
                    // Ignored statements - no schema tracking needed
                }

                // =================================================================
                // TODO: Future support candidates
                // These statements affect schema and should be implemented:
                //
                // DROP statements (via Statement::Drop):
                //   - DROP SCHEMA: Remove schema and contained objects
                //
                // ALTER statements:
                //   - AlterIndex: Rename, set options
                //   - AlterRole: Modify role properties
                //
                // Other DDL:
                //   - CreateSequence/AlterSequence: For auto-increment tracking
                //   - CreateSchema: Namespace management
                //   - Comment: Documentation metadata extraction
                // =================================================================
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

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::{errors::Error, traits::DatabaseLike};

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
