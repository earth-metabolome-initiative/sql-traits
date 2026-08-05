//! Tests that `SET search_path` decides where an unqualified name looks.
//!
//! PostgreSQL lets a schema put a schema on the path once and then drop the
//! prefix for the rest of the file. Every statement naming a table has to agree
//! about that, since a schema is read top to bottom and a trigger, a grant, an
//! index, a foreign key and a policy all resolve the same bare name. Before
//! this, the statement was discarded and four of those five refused a schema a
//! real server accepts.
#![allow(clippy::expect_used)]

use sql_traits::{errors::Error, prelude::*};
use sqlparser::dialect::PostgreSqlDialect;

const ON_THE_PATH: &str = "CREATE SCHEMA app;
     SET search_path TO app;
     CREATE TABLE app.docs (id INT PRIMARY KEY);
     CREATE FUNCTION app.touch() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
     CREATE POLICY p ON docs USING (true);
     CREATE TRIGGER t AFTER INSERT ON docs FOR EACH ROW EXECUTE FUNCTION app.touch();
     CREATE ROLE r;
     GRANT SELECT ON docs TO r;
     CREATE INDEX i ON docs (id);
     CREATE TABLE app.child (d INT REFERENCES docs(id));
";

fn parse(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(sql).expect("schema builds")
}

/// The whole shape, accepted by a real PostgreSQL 16 and now by this crate.
#[test]
fn every_statement_kind_resolves_through_the_path() {
    let db = parse(ON_THE_PATH);

    let policy = db.policies().next().expect("the policy exists");
    assert_eq!(policy.table(&db).expect("resolves").table_schema(), Some("app"));

    let trigger = db.triggers().next().expect("the trigger exists");
    assert_eq!(trigger.table(&db).expect("resolves").table_schema(), Some("app"));

    let grant = db.table_grants().next().expect("the grant exists");
    assert_eq!(grant.tables(&db).count(), 1);

    let index = db.indexes().next().expect("the index exists");
    assert_eq!(IndexLike::table(index, &db).table_schema(), Some("app"));

    let child = db.table(Some("app"), "child").expect("the child exists");
    let foreign_key =
        child.foreign_keys(&db).expect("child is in this database").next().expect("one key");
    assert_eq!(foreign_key.referenced_table(&db).expect("resolves").table_schema(), Some("app"));
}

/// Without the statement, the same names reach nothing and the read refuses at
/// the first of them.
#[test]
fn the_same_schema_without_the_statement_is_refused() {
    let without = ON_THE_PATH.replace("SET search_path TO app;", "");
    assert!(ParserDB::parse::<PostgreSqlDialect>(&without).is_err());
}

#[test]
fn the_default_path_is_public_alone() {
    let db = parse("CREATE TABLE docs (id INT);");
    let path: Vec<_> = db.search_path().collect();
    assert_eq!(path, [("public", false)]);
}

/// `SET` replaces the path rather than extending it, so `public` stops being
/// reachable unless it is listed. Verified against a real server.
#[test]
fn setting_the_path_replaces_it() {
    let replaced = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE SCHEMA app;
         CREATE TABLE public.pub (id INT PRIMARY KEY);
         SET search_path TO app;
         CREATE TABLE app.c (p INT REFERENCES pub(id));",
    );
    assert!(
        matches!(replaced, Err(Error::ReferencedTableNotFoundForForeignKey { .. })),
        "public left the path, got {replaced:?}"
    );

    let listed = parse(
        "CREATE SCHEMA app;
         CREATE TABLE public.pub (id INT PRIMARY KEY);
         SET search_path TO app, public;
         CREATE TABLE app.c (p INT REFERENCES pub(id));",
    );
    let path: Vec<_> = listed.search_path().collect();
    assert_eq!(path, [("app", false), ("public", false)]);
}

#[test]
fn the_path_is_walked_in_order() {
    let db = parse(
        "CREATE SCHEMA a;
         CREATE SCHEMA b;
         CREATE TABLE a.docs (id INT PRIMARY KEY);
         CREATE TABLE b.docs (id INT PRIMARY KEY);
         SET search_path TO b, a;
         CREATE TABLE b.child (d INT REFERENCES docs(id));",
    );
    let child = db.table(Some("b"), "child").expect("the child exists");
    let foreign_key =
        child.foreign_keys(&db).expect("child is in this database").next().expect("one key");

    assert_eq!(
        foreign_key.referenced_table(&db).expect("resolves").table_schema(),
        Some("b"),
        "the first entry on the path wins"
    );
}

#[test]
fn resetting_returns_to_the_default() {
    for tail in ["RESET search_path;", "SET search_path TO DEFAULT;"] {
        let db = parse(&format!("CREATE SCHEMA app; SET search_path TO app; {tail}"));
        let path: Vec<_> = db.search_path().collect();
        assert_eq!(path, [("public", false)], "after {tail}");
    }
}

/// Both spellings of an entry name a schema, and neither is a quoted
/// identifier in the case-sensitivity sense.
#[test]
fn an_entry_may_be_written_bare_or_quoted_as_a_string() {
    for statement in
        ["SET search_path TO app;", "SET search_path = app;", "SET LOCAL search_path TO app;"]
    {
        let db = parse(&format!("CREATE SCHEMA app; {statement}"));
        let path: Vec<_> = db.search_path().collect();
        assert_eq!(path, [("app", false)], "after {statement}");
    }

    let db = parse("CREATE SCHEMA app; SET search_path TO 'app';");
    let path: Vec<_> = db.search_path().collect();
    assert_eq!(path, [("app", false)]);
}
