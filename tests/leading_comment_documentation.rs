//! Tests that a table and a column are documented by the comment block written
//! directly above them, read from the same parse that builds the schema.
#![allow(clippy::expect_used)]

use sql_traits::prelude::*;
use sqlparser::{
    ast::CreateTable,
    dialect::{MySqlDialect, PostgreSqlDialect},
};

fn table<'db>(db: &'db ParserDB, schema: Option<&str>, name: &str) -> &'db CreateTable {
    let target = match schema {
        Some(schema) => TargetName::new(name, false).with_schema(schema, false),
        None => TargetName::new(name, false),
    };
    db.table_by_target(target, IdentifierCase::AsWritten)
        .expect("unambiguous lookup")
        .expect("the table exists")
}

fn table_doc<'db>(db: &'db ParserDB, schema: Option<&str>, name: &str) -> Option<&'db str> {
    table(db, schema, name).table_doc(db).expect("the table is in this database")
}

fn column_doc<'db>(
    db: &'db ParserDB,
    schema: Option<&str>,
    table_name: &str,
    column: &str,
) -> Option<&'db str> {
    table(db, schema, table_name)
        .column(column, db, IdentifierCase::AsWritten)
        .expect("the table is in this database")
        .expect("the column exists")
        .column_doc(db)
        .expect("the column is in this database")
}

fn postgres(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(sql).expect("schema builds")
}

#[test]
fn comment_markers_inside_string_literals_leave_documentation_intact() {
    let db = postgres(
        "-- users
CREATE TABLE users (
  -- the key
  id INT,
  closer TEXT DEFAULT '*/',
  opener TEXT DEFAULT '/* x',
  dashes TEXT DEFAULT '-- no'
);",
    );
    assert_eq!(table_doc(&db, None, "users"), Some("users"));
    assert_eq!(column_doc(&db, None, "users", "id"), Some("the key"));
    assert_eq!(column_doc(&db, None, "users", "dashes"), None);
}

#[test]
fn mysql_hash_comments_document() {
    let db = ParserDB::parse::<MySqlDialect>(
        "# events
CREATE TABLE events (
  # when it happened
  at INT
);",
    )
    .expect("schema builds");
    assert_eq!(table_doc(&db, None, "events"), Some("events"));
    assert_eq!(column_doc(&db, None, "events", "at"), Some("when it happened"));
}

#[test]
fn a_contiguous_comment_block_documents_whole() {
    let db = postgres(
        "-- Registered users.
-- One row per account.
CREATE TABLE users (
  /* The key,
     never reused. */
  id INT
);",
    );
    assert_eq!(table_doc(&db, None, "users"), Some("Registered users.\nOne row per account."));
    assert_eq!(column_doc(&db, None, "users", "id"), Some("The key,\nnever reused."));
}

#[test]
fn a_blank_line_detaches_a_comment() {
    let db = postgres(
        "-- file header
-- still the header

-- users
CREATE TABLE users (
  -- detached

  id INT
);",
    );
    assert_eq!(table_doc(&db, None, "users"), Some("users"));
    assert_eq!(column_doc(&db, None, "users", "id"), None);
}

#[test]
fn a_trailing_comment_documents_nothing_below_it() {
    let db = postgres(
        "CREATE TABLE a (x INT); -- about a
CREATE TABLE b (
  id INT, -- about id
  name TEXT
);",
    );
    assert_eq!(table_doc(&db, None, "b"), None);
    assert_eq!(column_doc(&db, None, "b", "id"), None);
    assert_eq!(column_doc(&db, None, "b", "name"), None);
}

#[test]
fn a_table_name_on_its_own_line_is_documented() {
    let db = postgres(
        "-- users
CREATE TABLE IF NOT EXISTS
  users (id INT);",
    );
    assert_eq!(table_doc(&db, None, "users"), Some("users"));
}

#[test]
fn documentation_follows_a_renamed_table_and_column() {
    let db = postgres(
        "-- people
CREATE TABLE users (
  -- the key
  id INT
);
ALTER TABLE users RENAME TO people;
ALTER TABLE people RENAME COLUMN id TO person_id;",
    );
    assert_eq!(table_doc(&db, None, "people"), Some("people"));
    assert_eq!(column_doc(&db, None, "people", "person_id"), Some("the key"));
}

#[test]
fn quoted_names_are_documented() {
    let db = postgres(
        "-- quoted table
CREATE TABLE \"Users\" (
  -- quoted column
  \"Id\" INT
);",
    );
    let users = db
        .table_by_target(TargetName::new("Users", true), IdentifierCase::AsWritten)
        .expect("unambiguous lookup")
        .expect("the table exists");
    assert_eq!(users.table_doc(&db).expect("in this database"), Some("quoted table"));
    let id = users
        .column("\"Id\"", &db, IdentifierCase::AsWritten)
        .expect("in this database")
        .expect("the column exists");
    assert_eq!(id.column_doc(&db).expect("in this database"), Some("quoted column"));
}

#[test]
fn a_column_dropped_and_added_again_is_undocumented() {
    let db = postgres(
        "CREATE TABLE users (
  id INT,
  -- the old note
  note TEXT
);
ALTER TABLE users DROP COLUMN note;
ALTER TABLE users ADD COLUMN note TEXT;",
    );
    assert_eq!(column_doc(&db, None, "users", "note"), None);
}

#[test]
fn an_inherited_column_does_not_take_the_parent_documentation() {
    let db = postgres(
        "CREATE TABLE parent (
  -- the parent key
  id INT
);
CREATE TABLE child (
  -- the child extra
  extra INT
) INHERITS (parent);",
    );
    assert_eq!(column_doc(&db, None, "parent", "id"), Some("the parent key"));
    assert_eq!(column_doc(&db, None, "child", "id"), None);
    assert_eq!(column_doc(&db, None, "child", "extra"), Some("the child extra"));
}

#[test]
fn same_named_tables_created_through_the_search_path_keep_their_own_documentation() {
    let db = postgres(
        "CREATE SCHEMA a;
CREATE SCHEMA b;
SET search_path TO a;
-- in a
CREATE TABLE t (id INT);
SET search_path TO b;
-- in b
CREATE TABLE t (id INT);",
    );
    assert_eq!(table_doc(&db, Some("a"), "t"), Some("in a"));
    assert_eq!(table_doc(&db, Some("b"), "t"), Some("in b"));
}

#[test]
fn each_file_documents_its_own_tables() {
    let directory = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("leading_comment_documentation_each_file");
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("scratch directory");
    std::fs::write(directory.join("1_users.sql"), "-- users\nCREATE TABLE users (id INT);\n")
        .expect("first file");
    std::fs::write(
        directory.join("2_posts.sql"),
        "CREATE TABLE posts (\n  -- the author\n  author INT\n);\n",
    )
    .expect("second file");

    let db = ParserDB::from_path::<PostgreSqlDialect>(&directory).expect("schema builds");
    assert_eq!(table_doc(&db, None, "users"), Some("users"));
    assert_eq!(table_doc(&db, None, "posts"), None);
    assert_eq!(column_doc(&db, None, "posts", "author"), Some("the author"));
}
