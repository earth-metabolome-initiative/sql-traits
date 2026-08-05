//! Tests that an object's target reads back exactly as the SQL wrote it.
//!
//! A policy, a trigger, a foreign key and a grant all name a table, and until
//! now the only way to ask which one was to resolve it. Resolution can refuse,
//! and it applies this crate's rules rather than the caller's, so a caller that
//! honours a search path of its own had no route to the name at all and was
//! forced onto the concrete parser node. These tests pin the unresolved
//! reading, including the case where resolution fails.
#![allow(clippy::expect_used)]

use sql_traits::{errors::Error, prelude::*};
use sqlparser::dialect::PostgreSqlDialect;

fn parse(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(sql).expect("schema builds")
}

/// The reproduction from the finding that opened this work: the only `docs`
/// table lives in schema `app`, and the policy names `docs` unqualified.
#[test]
fn policy_target_reads_back_when_it_does_not_resolve() {
    let db = parse(
        "CREATE SCHEMA app;
         CREATE TABLE app.docs (id INT);
         CREATE POLICY docs_sel ON docs USING (true);",
    );
    let policy = db.policies().next().expect("the policy exists");

    assert_eq!(policy.target_table_name(), "docs");
    assert!(!policy.target_table_name_is_quoted());
    assert_eq!(policy.target_table_schema(), None);
    assert!(!policy.target_table_schema_is_quoted());

    assert!(
        matches!(policy.table(&db), Err(sql_traits::errors::LookupError::TableNotFound { .. })),
        "the unqualified target must not resolve, or the test proves nothing"
    );
}

#[test]
fn policy_target_preserves_quoting_and_qualification() {
    let db = parse(
        "CREATE SCHEMA \"App\";
         CREATE TABLE \"App\".\"Docs\" (id INT);
         CREATE POLICY docs_sel ON \"App\".\"Docs\" USING (true);",
    );
    let policy = db.policies().next().expect("the policy exists");

    assert_eq!(policy.target_table_name(), "Docs");
    assert!(policy.target_table_name_is_quoted());
    assert_eq!(policy.target_table_schema(), Some("App"));
    assert!(policy.target_table_schema_is_quoted());

    // Reading through the blanket implementation for references answers the
    // same, since a caller holding `&&Policy` is the common case behind an
    // iterator.
    let by_reference = &policy;
    assert_eq!(by_reference.target_table_name(), "Docs");
    assert!(by_reference.target_table_name_is_quoted());
    assert_eq!(by_reference.target_table_schema(), Some("App"));
    assert!(by_reference.target_table_schema_is_quoted());
}

/// A trigger target that does not resolve cannot reach a built schema, since
/// the parse refuses it with `TableNotFoundForTrigger`. What the reader still
/// buys here is the name without a parser node, and without the failure mode
/// that [`TriggerLike::table`] carries for the caller who only wants a name.
#[test]
fn trigger_target_reads_back_unqualified() {
    let db = parse(
        "CREATE TABLE docs (id INT);
         CREATE FUNCTION touch() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
         CREATE TRIGGER docs_touch AFTER INSERT ON docs
         FOR EACH ROW EXECUTE FUNCTION touch();",
    );
    let trigger = db.triggers().next().expect("the trigger exists");

    assert_eq!(trigger.target_table_name(), "docs");
    assert!(!trigger.target_table_name_is_quoted());
    assert_eq!(trigger.target_table_schema(), None);

    let by_reference = &trigger;
    assert_eq!(by_reference.target_table_name(), "docs");
    assert_eq!(by_reference.target_table_schema(), None);
}

/// An unresolvable trigger target is refused outright, unlike the policy above
/// which is recorded and only refuses on resolution.
#[test]
fn trigger_target_that_does_not_resolve_is_refused() {
    let refused = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE SCHEMA app;
         CREATE TABLE app.docs (id INT);
         CREATE FUNCTION touch() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
         CREATE TRIGGER docs_touch AFTER INSERT ON docs
         FOR EACH ROW EXECUTE FUNCTION touch();",
    );

    assert!(
        matches!(refused, Err(Error::TableNotFoundForTrigger { ref table_name, .. }) if table_name == "docs"),
        "got {refused:?}"
    );
}

#[test]
fn trigger_target_preserves_quoting_and_qualification() {
    let db = parse(
        "CREATE SCHEMA app;
         CREATE TABLE app.\"Docs\" (id INT);
         CREATE FUNCTION touch() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
         CREATE TRIGGER docs_touch AFTER INSERT ON app.\"Docs\"
         FOR EACH ROW EXECUTE FUNCTION touch();",
    );
    let trigger = db.triggers().next().expect("the trigger exists");

    assert_eq!(trigger.target_table_name(), "Docs");
    assert!(trigger.target_table_name_is_quoted());
    assert_eq!(trigger.target_table_schema(), Some("app"));
    assert!(!trigger.target_table_schema_is_quoted());
}

#[test]
fn foreign_key_reference_preserves_quoting_and_qualification() {
    let db = parse(
        "CREATE SCHEMA app;
         CREATE TABLE app.\"Docs\" (id INT PRIMARY KEY);
         CREATE TABLE notes (doc_id INT, FOREIGN KEY (doc_id) REFERENCES app.\"Docs\"(id));",
    );
    let notes = db.table(None, "notes").expect("the host table exists");
    let foreign_key =
        notes.foreign_keys(&db).expect("the host table is known").next().expect("one key");

    assert_eq!(foreign_key.referenced_table_name(), "Docs");
    assert!(foreign_key.referenced_table_name_is_quoted());
    assert_eq!(foreign_key.referenced_table_schema(), Some("app"));
    assert!(!foreign_key.referenced_table_schema_is_quoted());
}

#[test]
fn foreign_key_reference_reads_back_unqualified() {
    let db = parse(
        "CREATE TABLE docs (id INT PRIMARY KEY);
         CREATE TABLE notes (doc_id INT, FOREIGN KEY (doc_id) REFERENCES docs(id));",
    );
    let notes = db.table(None, "notes").expect("the host table exists");
    let foreign_key =
        notes.foreign_keys(&db).expect("the host table is known").next().expect("one key");

    assert_eq!(foreign_key.referenced_table_name(), "docs");
    assert!(!foreign_key.referenced_table_name_is_quoted());
    assert_eq!(foreign_key.referenced_table_schema(), None);
}

#[test]
fn grant_lists_its_table_targets_as_written() {
    let db = parse(
        "CREATE SCHEMA app;
         CREATE TABLE users (id INT);
         CREATE TABLE app.\"Posts\" (id INT);
         CREATE ROLE reader;
         GRANT SELECT ON users, app.\"Posts\" TO reader;",
    );
    let grant = db.table_grants().next().expect("the grant exists");
    let targets: Vec<_> = grant.target_table_names().collect();

    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0].name(), "users");
    assert!(!targets[0].name_is_quoted());
    assert_eq!(targets[0].schema(), None);
    assert_eq!(targets[1].name(), "Posts");
    assert!(targets[1].name_is_quoted());
    assert_eq!(targets[1].schema(), Some("app"));
    assert!(!targets[1].schema_is_quoted());

    assert!(grant.target_schema_names().next().is_none());
}

/// A column grant is the same statement seen through another trait, and it
/// must not lose the tables past the first the way the resolving reader does.
#[test]
fn column_grant_lists_every_table_target() {
    let db = parse(
        "CREATE TABLE users (id INT, name TEXT);
         CREATE TABLE posts (id INT, name TEXT);
         CREATE ROLE reader;
         GRANT SELECT (id, name) ON users, posts TO reader;",
    );
    let grant = db.column_grants().next().expect("the grant exists");
    let names: Vec<_> = grant.target_table_names().map(|target| target.name()).collect();

    assert_eq!(names, ["users", "posts"]);
}

#[test]
fn schema_wide_grant_names_the_schema_not_the_tables() {
    let db = parse(
        "CREATE TABLE public.users (id INT);
         CREATE ROLE reader;
         GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;",
    );
    let grant = db.table_grants().next().expect("the grant exists");
    let schemas: Vec<_> = grant.target_schema_names().collect();

    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name(), "public");
    assert_eq!(schemas[0].schema(), None);
    assert!(grant.target_table_names().next().is_none());
}

#[test]
fn grant_target_reads_back_when_it_does_not_resolve() {
    let sql = "CREATE ROLE reader;
               GRANT SELECT ON absent_table TO reader;";

    assert!(
        matches!(
            ParserDB::parse::<PostgreSqlDialect>(sql),
            Err(Error::TableNotFoundForGrant { ref table_name }) if table_name == "absent_table"
        ),
        "a closed world refuses a grant on a table nothing creates"
    );

    let db = ParseOptions::default()
        .with_access_resolution(AccessResolution::OpenWorld)
        .parse::<PostgreSqlDialect>(sql)
        .expect("an open world records it");
    let grant = db.table_grants().next().expect("the grant exists");

    assert_eq!(grant.target_table_names().next().expect("one target").name(), "absent_table");
    assert!(grant.tables(&db).next().is_none(), "the target resolves to nothing");
}

#[test]
fn grant_on_a_non_table_object_names_neither() {
    let db = parse(
        "CREATE ROLE reader;
         CREATE SCHEMA app;
         GRANT USAGE ON SCHEMA app TO reader;",
    );
    let grant = db.table_grants().next().expect("the grant exists");

    assert!(grant.target_table_names().next().is_none());
    assert!(grant.target_schema_names().next().is_none());
}
