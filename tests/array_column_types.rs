//! Tests that the declared type of an array column is answered rather than
//! aborting the process.
#![allow(clippy::expect_used)]

use sql_traits::prelude::*;
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

/// Builds a one-table database from `ddl` and returns the declared type of
/// every one of its columns, in declaration order.
fn column_types(ddl: &str) -> Vec<String> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, ddl).expect("SQL parses");
    let database =
        ParserDB::from_statements(statements, "test".to_string()).expect("schema builds");
    let table = database.tables().next().expect("input declares a table");

    table
        .columns(&database)
        .expect("table belongs to the database")
        .map(|column| column.data_type(&database).into_owned())
        .collect()
}

#[test]
fn test_array_column_data_type() {
    assert_eq!(
        column_types("CREATE TABLE t (id INTEGER PRIMARY KEY, tags TEXT[]);"),
        vec!["INT", "TEXT[]"]
    );
}

/// Every array spelling PostgreSQL accepts answers a canonical token: the
/// declared length is dropped exactly as a `VARCHAR(255)` length is, and
/// nesting is preserved.
#[test]
fn test_array_column_spellings() {
    for (declared, expected) in [
        ("TEXT[]", "TEXT[]"),
        ("INTEGER[]", "INT[]"),
        ("TEXT[3]", "TEXT[]"),
        ("INT[][]", "INT[][]"),
        ("TIMESTAMPTZ[]", "TIMESTAMPTZ[]"),
    ] {
        assert_eq!(
            column_types(&format!("CREATE TABLE t (value {declared});")),
            vec![expected],
            "failed for {declared}"
        );
    }
}

/// The normalized type of an array column is the array token itself, since no
/// PostgreSQL alias folds into it.
#[test]
fn test_array_column_normalized_data_type() {
    let statements =
        Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE t (tags TEXT[]);").expect("parses");
    let database =
        ParserDB::from_statements(statements, "test".to_string()).expect("schema builds");
    let table = database.tables().next().expect("input declares a table");
    let column = table.column("tags", &database).expect("lookup runs").expect("column exists");

    assert_eq!(column.normalized_data_type(&database), "TEXT[]");
    assert!(!column.is_textual(&database));
}

/// An array column takes part in a schema fingerprint like any other column,
/// and a differing element type is a differing fingerprint.
#[test]
fn test_array_column_fingerprint() {
    let fingerprint = |ddl: &str| {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, ddl).expect("parses");
        let database =
            ParserDB::from_statements(statements, "test".to_string()).expect("schema builds");
        let table = database.tables().next().expect("input declares a table");
        table.schema_fingerprint(&database).expect("fingerprint computes")
    };

    assert_eq!(
        fingerprint("CREATE TABLE t (tags TEXT[]);"),
        fingerprint("CREATE TABLE t (tags TEXT[]);")
    );
    assert_ne!(
        fingerprint("CREATE TABLE t (tags TEXT[]);"),
        fingerprint("CREATE TABLE t (tags INT[]);")
    );
}

/// A function taking and returning arrays reports both without aborting.
#[test]
fn test_array_function_types() {
    let ddl = "CREATE FUNCTION first_tag(tags TEXT[]) RETURNS TEXT[] AS $$ SELECT tags $$ \
               LANGUAGE SQL;";
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, ddl).expect("parses");
    let database =
        ParserDB::from_statements(statements, "test".to_string()).expect("schema builds");
    let function = database.function("first_tag").expect("function exists");

    assert_eq!(function.argument_type_names(&database).collect::<Vec<_>>(), vec!["TEXT[]"]);
    assert_eq!(function.return_type_name(&database).as_deref(), Some("TEXT[]"));
}
