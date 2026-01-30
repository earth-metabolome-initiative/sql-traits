//! Implementations for [`GenericDB`] relative to sqlparser structures.

use std::{
    path::{Path, PathBuf},
    rc::Rc,
};

use git2::Repository;
use sql_docs::SqlDoc;
use sqlparser::{
    ast::{
        CheckConstraint, ColumnDef, ColumnOption, CreateFunction, CreateIndex, CreateTable,
        CreateTrigger, Expr, ForeignKeyConstraint, IndexColumn, OrderByExpr, OrderByOptions,
        Statement, TableConstraint, UniqueConstraint, Value, ValueWithSpan,
    },
    dialect::PostgreSqlDialect,
    parser::{Parser, ParserError},
};

use crate::{
    structs::{
        GenericDB, TableAttribute, TableMetadata,
        generic_db::GenericDBBuilder,
        metadata::{CheckMetadata, IndexMetadata, UniqueIndexMetadata},
    },
    traits::{DatabaseLike, FunctionLike, TableLike, column::ColumnLike},
    utils::{columns_in_expression, last_str},
};

mod functions_in_expression;

/// A type alias for a `GenericDB` specialized for `sqlparser`'s `CreateTable`.
pub type ParserDB = GenericDB<
    CreateTable,
    TableAttribute<CreateTable, ColumnDef>,
    TableAttribute<CreateTable, CreateIndex>,
    TableAttribute<CreateTable, UniqueConstraint>,
    TableAttribute<CreateTable, ForeignKeyConstraint>,
    CreateFunction,
    TableAttribute<CreateTable, CheckConstraint>,
    CreateTrigger,
>;

/// A type alias for a `GenericDBBuilder` specialized for `sqlparser`'s
/// `CreateTable`.
pub type ParserDBBuilder = GenericDBBuilder<
    CreateTable,
    TableAttribute<CreateTable, ColumnDef>,
    TableAttribute<CreateTable, CreateIndex>,
    TableAttribute<CreateTable, UniqueConstraint>,
    TableAttribute<CreateTable, ForeignKeyConstraint>,
    CreateFunction,
    TableAttribute<CreateTable, CheckConstraint>,
    CreateTrigger,
>;

/// A type alias for the result of processing check constraints.
pub type CheckConstraintResult =
    (Vec<Rc<<ParserDB as DatabaseLike>::Column>>, Vec<Rc<<ParserDB as DatabaseLike>::Function>>);

/// A type alias for the result of processing unique constraints.
pub type UniqueConstraintResult = (
    Rc<TableAttribute<CreateTable, UniqueConstraint>>,
    UniqueIndexMetadata<TableAttribute<CreateTable, UniqueConstraint>>,
);

impl ParserDB {
    /// Helper function to process check constraints.
    fn process_check_constraint(
        check_expr: &Expr,
        create_table: &Rc<CreateTable>,
        table_metadata: &TableMetadata<CreateTable>,
        builder: &ParserDBBuilder,
    ) -> Result<CheckConstraintResult, crate::errors::Error> {
        let columns_in_expression = columns_in_expression::<Rc<<Self as DatabaseLike>::Column>>(
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
        Parser::new(&sqlparser::dialect::GenericDialect {})
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
                    // From the primary key constraint we also create an associated unique index,
                    // since primary keys also have an associated unique index.
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
        // Validate host columns exist
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

        // Validate referenced table exists or is current table (self-referential)
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

        // Validate referenced columns exist
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

                    // From the primary key constraint we also create an associated unique index,
                    // since primary keys also have an associated unique index.
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
    /// Panics if a statement other than `CREATE TABLE` or `CREATE FUNCTION` is
    /// encountered, or if the builder fails to build the database.
    ///
    /// # Errors
    ///
    /// Returns an error if a check constraint references an unknown column.
    ///
    /// # Examples
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE users (
    ///     id INTEGER PRIMARY KEY,
    ///     name VARCHAR(100)
    /// );
    /// CREATE TABLE posts (
    ///     id INTEGER PRIMARY KEY,
    ///     user_id INTEGER REFERENCES users(id),
    ///     title VARCHAR(200)
    /// );
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let db = ParserDB::from_statements(statements, "test".to_string()).unwrap();
    /// assert_eq!(db.catalog_name(), "test");
    /// ```
    ///
    /// # Error Examples
    ///
    /// This will fail if a foreign key references a non-existent column:
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE users (
    ///     id INTEGER PRIMARY KEY,
    ///     name VARCHAR(100)
    /// );
    /// CREATE TABLE posts (
    ///     id INTEGER PRIMARY KEY,
    ///     user_id INTEGER,
    ///     title VARCHAR(200),
    ///     FOREIGN KEY (user_id) REFERENCES users(nonexistent_column)
    /// );
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// // This should fail with HostColumnNotFoundForForeignKey
    /// assert!(ParserDB::from_statements(statements, "test".to_string()).is_err());
    /// ```
    ///
    /// This will fail if a foreign key references a non-existent table:
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE posts (
    ///     id INTEGER PRIMARY KEY,
    ///     user_id INTEGER,
    ///     title VARCHAR(200),
    ///     FOREIGN KEY (user_id) REFERENCES nonexistent_table(id)
    /// );
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// // This should fail with ReferencedTableNotFoundForForeignKey
    /// assert!(ParserDB::from_statements(statements, "test".to_string()).is_err());
    /// ```
    ///
    /// This will fail if a trigger references a non-existent table:
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON nonexistent_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// // This should fail with TableNotFoundForTrigger
    /// assert!(ParserDB::from_statements(statements, "test".to_string()).is_err());
    /// ```
    ///
    /// This will fail if a trigger references a non-existent function:
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION nonexistent_function();
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// // This should fail with FunctionNotFoundForTrigger
    /// assert!(ParserDB::from_statements(statements, "test".to_string()).is_err());
    /// ```
    ///
    /// Supports SET TIME ZONE statements:
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// SET TIME ZONE 'UTC';
    /// CREATE TABLE users (
    ///     id INTEGER PRIMARY KEY,
    ///     name VARCHAR(100)
    /// );
    /// "#;
    ///
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// let db = ParserDB::from_statements(statements, "test".to_string()).unwrap();
    /// assert_eq!(db.catalog_name(), "test");
    /// ```
    ///
    /// Panics on unsupported statements:
    ///
    /// ```should_panic
    /// use sql_traits::prelude::ParserDB;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let sql = "SELECT 1;";
    /// let dialect = PostgreSqlDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql).unwrap();
    /// // This should panic
    /// let _ = ParserDB::from_statements(statements, "test".to_string());
    /// ```
    #[must_use = "The result should be checked for errors"]
    #[allow(clippy::too_many_lines)]
    pub fn from_statements(
        statements: Vec<Statement>,
        catalog_name: String,
    ) -> Result<Self, crate::errors::Error> {
        let mut builder: ParserDBBuilder = GenericDBBuilder::new(catalog_name);

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
                Statement::CreateTable(create_table) => {
                    let create_table = Rc::new(create_table);
                    let mut table_metadata: TableMetadata<CreateTable> = TableMetadata::default();

                    // Add all columns to metadata
                    for column in create_table.columns.clone() {
                        let column_rc = Rc::new(TableAttribute::new(create_table.clone(), column));
                        table_metadata.add_column(column_rc.clone());
                    }

                    // Process column options and add columns to builder
                    for column in table_metadata.clone().column_rcs() {
                        builder = Self::process_column_options(
                            column,
                            &create_table,
                            &mut table_metadata,
                            builder,
                        )?;
                        builder = builder.add_column(column.clone(), ());
                    }

                    // Process table constraints
                    builder = Self::process_table_constraints(
                        &create_table.constraints,
                        &create_table,
                        &mut table_metadata,
                        builder,
                    )?;

                    builder = builder.add_table(create_table, table_metadata);
                }
                Statement::Set(sqlparser::ast::Set::SetTimeZone { local, value }) => {
                    // We currently ignore SET TIME ZONE statements.
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
                | Statement::Insert(_) => {
                    // At the moment, we ignore these CREATE statements.
                }
                _ => {
                    unimplemented!("Unsupported statement found: {statement:?}");
                }
            }
        }

        Ok(builder.into())
    }

    /// Constructs a `ParserDB` from a git URL.
    ///
    /// # Example
    ///
    /// ```
    /// use sql_traits::prelude::ParserDB;
    ///
    /// let url = "https://github.com/earth-metabolome-initiative/asset-procedure-schema.git";
    /// let db = ParserDB::from_git_url(url).unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// This function will return an error if the repository cannot be cloned or
    /// if the SQL files cannot be parsed.
    pub fn from_git_url(url: &str) -> Result<Self, crate::errors::Error> {
        let dir = tempfile::tempdir()?;
        Repository::clone(url, dir.path())?;
        Self::try_from(dir.path())
    }
}

impl TryFrom<&str> for ParserDB {
    type Error = crate::errors::Error;

    fn try_from(sql: &str) -> Result<Self, Self::Error> {
        let dialect = sqlparser::dialect::GenericDialect {};
        let mut parser = sqlparser::parser::Parser::new(&dialect).try_with_sql(sql)?;
        let statements = parser.parse_statements()?;
        let mut db = Self::from_statements(statements, "unknown_catalog".to_string())?;
        let documentation = SqlDoc::builder_from_str(sql).build()?;
        for (table, metadata) in db.tables_metadata_mut() {
            let table_doc = documentation.table(table.table_name(), table.table_schema())?;
            metadata.set_doc(table_doc.to_owned());
        }
        Ok(db)
    }
}

fn search_sql_documents(path: &Path) -> Vec<std::path::PathBuf> {
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

impl TryFrom<&Path> for ParserDB {
    type Error = crate::errors::Error;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        Self::try_from(&[path] as &[&Path])
    }
}

impl TryFrom<&[&Path]> for ParserDB {
    type Error = crate::errors::Error;

    fn try_from(paths: &[&Path]) -> Result<Self, Self::Error> {
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

                let mut parser = sqlparser::parser::Parser::new(&PostgreSqlDialect {})
                    .try_with_sql(&sql_content)
                    .map_err(|e| {
                        crate::errors::Error::SqlParserError {
                            error: e,
                            file: Some(sql_path.clone()),
                        }
                    })?;
                statements.extend(parser.parse_statements().map_err(|e| {
                    crate::errors::Error::SqlParserError { error: e, file: Some(sql_path.clone()) }
                })?);
                sql_str.push((sql_content, sql_path));
            }
        }
        let documentation = SqlDoc::builder_from_strs_with_paths(&sql_str).build()?;
        let mut db = Self::from_statements(statements, "unknown_catalog".to_string())?;
        assert_eq!(
            db.number_of_tables(),
            documentation.number_of_tables(),
            "The number of tables in the DB does not match with the number of tables in documentation"
        );
        for ((table, metadata), table_doc) in db.tables_metadata_mut().zip(documentation.tables()) {
            debug_assert_eq!(
                table.table_name(),
                table_doc.name(),
                "Db table {} is not aligned with documentation table {}",
                table.table_name(),
                table_doc.name()
            );
            metadata.set_doc(table_doc.to_owned());
        }
        Ok(db)
    }
}
