//! Tests that `ALTER TABLE ... OWNER TO` is recorded and reachable.
//!
//! `PostgreSQL` exempts a table's owner from every policy on it unless the
//! table also has forced Row Level Security, so a caller reporting that
//! exemption has to be able to name the exempt role rather than call it "the
//! table's owner". The operation used to parse and be discarded, which left the
//! question unanswerable.
#![allow(clippy::expect_used)]

use sql_traits::{errors::Error, prelude::*};
use sqlparser::{ast::CreateTable, dialect::PostgreSqlDialect};

fn parse(sql: &str) -> Result<ParserDB, Error> {
    ParserDB::parse::<PostgreSqlDialect>(sql)
}

fn db(sql: &str) -> ParserDB {
    parse(sql).expect("schema builds")
}

/// A table with row level security and an owner, reduced to the parts these
/// tests turn on. [`a_pg_dump_of_a_guarded_table_is_read_whole`] carries the
/// full shape a dump emits.
const DUMP: &str = "CREATE TABLE docs (id uuid PRIMARY KEY);
    ALTER TABLE docs ENABLE ROW LEVEL SECURITY;
    ALTER TABLE docs OWNER TO app_owner;";

#[test]
fn an_owner_to_statement_names_the_owning_role() {
    let database = db(DUMP);
    let docs = database.table(None, "docs").expect("docs exists");

    assert_eq!(docs.owner(&database), Ok(Some("app_owner")));
    assert_eq!(docs.has_row_level_security(&database), Ok(true));
    assert_eq!(
        docs.has_forced_row_level_security(&database),
        Ok(false),
        "the owner is exempt from every policy, which is why naming it matters"
    );
}

/// Ownership is a cluster-level fact a schema need not state, so its absence is
/// an answer rather than a failure.
#[test]
fn a_table_nobody_altered_names_no_owner() {
    let database = db("CREATE TABLE docs (id uuid PRIMARY KEY);");
    let docs = database.table(None, "docs").expect("docs exists");

    assert_eq!(docs.owner(&database), Ok(None));
}

#[test]
fn ownership_moves_to_the_role_named_last() {
    let database = db("CREATE TABLE docs (id uuid PRIMARY KEY);
         ALTER TABLE docs OWNER TO first;
         ALTER TABLE docs OWNER TO second;");
    let docs = database.table(None, "docs").expect("docs exists");

    assert_eq!(docs.owner(&database), Ok(Some("second")));
}

/// These three name whoever runs the statement, so the owner did change but to
/// a role the input never spells. Reporting the previous one would state
/// something the schema does not say.
#[test]
fn a_session_dependent_owner_leaves_no_role_named() {
    for spelling in ["CURRENT_ROLE", "CURRENT_USER", "SESSION_USER"] {
        let database = db(&format!(
            "CREATE TABLE docs (id uuid PRIMARY KEY);
             ALTER TABLE docs OWNER TO app_owner;
             ALTER TABLE docs OWNER TO {spelling};"
        ));
        let docs = database.table(None, "docs").expect("docs exists");

        assert_eq!(docs.owner(&database), Ok(None), "{spelling} left a stale owner behind");
    }
}

/// The role is reported as written, quoted or not, which is what `role` does
/// with the names it stores too, so a caller comparing the two agrees with
/// itself.
#[test]
fn the_role_is_reported_as_the_statement_spelled_it() {
    for spelling in ["App_Owner", "\"App_Owner\""] {
        let database = db(&format!(
            "CREATE TABLE docs (id uuid PRIMARY KEY);
             ALTER TABLE docs OWNER TO {spelling};"
        ));
        let docs = database.table(None, "docs").expect("docs exists");

        assert_eq!(docs.owner(&database), Ok(Some("App_Owner")), "{spelling} was folded");
    }

    let database = db("CREATE ROLE App_Owner;
         CREATE TABLE docs (id uuid PRIMARY KEY);
         ALTER TABLE docs OWNER TO App_Owner;");
    let owner = database.table(None, "docs").expect("docs exists").owner(&database);
    assert_eq!(owner, Ok(Some("App_Owner")));
    assert!(database.role("App_Owner").is_some(), "the owner names a role `role` also finds");
}

/// A rename rebuilds the stored node, so anything hung off the old one has to
/// be carried across explicitly or it is silently lost.
#[test]
fn the_owner_follows_a_renamed_table() {
    let database = db("CREATE TABLE docs (id uuid PRIMARY KEY);
         ALTER TABLE docs OWNER TO app_owner;
         ALTER TABLE docs RENAME TO papers;");
    let papers = database.table(None, "papers").expect("the rename is applied");

    assert_eq!(papers.owner(&database), Ok(Some("app_owner")));
}

/// A column change rebuilds the stored node the same way a rename does.
#[test]
fn the_owner_survives_a_column_change() {
    let database = db("CREATE TABLE docs (id uuid PRIMARY KEY, size INT);
         ALTER TABLE docs OWNER TO app_owner;
         ALTER TABLE docs ALTER COLUMN size TYPE BIGINT;
         ALTER TABLE docs ADD COLUMN title TEXT;");
    let docs = database.table(None, "docs").expect("docs exists");

    assert_eq!(docs.columns(&database).expect("docs is in this database").count(), 3);
    assert_eq!(docs.owner(&database), Ok(Some("app_owner")));
}

/// Ownership is recorded per table, so one table's owner never answers for
/// another's.
#[test]
fn each_table_answers_for_itself() {
    let database = db("CREATE TABLE docs (id uuid PRIMARY KEY);
         CREATE TABLE notes (id uuid PRIMARY KEY);
         ALTER TABLE docs OWNER TO app_owner;");

    assert_eq!(
        database.table(None, "docs").expect("docs exists").owner(&database),
        Ok(Some("app_owner"))
    );
    assert_eq!(database.table(None, "notes").expect("notes exists").owner(&database), Ok(None));
}

/// An owner is recorded against a table that exists, so naming an absent one is
/// the same mistake as any other `ALTER TABLE` against an absent table.
#[test]
fn an_owner_change_to_an_absent_table_is_reported() {
    let error = parse(
        "CREATE TABLE docs (id uuid PRIMARY KEY);
         ALTER TABLE absent OWNER TO app_owner;",
    )
    .expect_err("absent is never created");

    assert!(
        matches!(&error, Error::AlterTableNotFound { table_name } if table_name == "absent"),
        "got {error:?}"
    );

    parse(
        "CREATE TABLE docs (id uuid PRIMARY KEY);
         ALTER TABLE IF EXISTS absent OWNER TO app_owner;",
    )
    .expect("IF EXISTS excuses the absent table");
}

/// Ownership is not an access grant, so it is not resolved against the roles
/// the input creates. A dump names an owner and never creates it, and the
/// closed world that refuses a `GRANT` to an uncreated role would refuse every
/// such dump.
#[test]
fn an_owner_is_not_resolved_against_the_roles_the_input_creates() {
    let database = db(DUMP);

    assert_eq!(ParseOptions::default().access_resolution(), AccessResolution::ClosedWorld);
    assert!(database.role("app_owner").is_none(), "nothing created the role");
    assert_eq!(
        database.unresolved_access_references().expect("targets are well formed").count(),
        0,
        "an owner is not an access reference"
    );
    assert_eq!(
        database.table(None, "docs").expect("docs exists").owner(&database),
        Ok(Some("app_owner"))
    );
}

/// The name lives in the database rather than in the table node, so a caller
/// may hand back a borrow of it that outlives the node it asked through. This
/// compiles only while the signature keeps the two lifetimes apart.
fn owner_of<'db>(database: &'db ParserDB, table: &CreateTable) -> Option<&'db str> {
    table.owner(database).ok().flatten()
}

#[test]
fn the_name_outlives_the_node_it_was_asked_through() {
    let database = db(DUMP);
    let node = database.table(None, "docs").expect("docs exists").clone();

    assert_eq!(owner_of(&database, &node), Some("app_owner"));
}

/// The motivating input, written the way `pg_dump` writes it: the table is
/// schema-qualified, the primary key arrives as a later `ALTER TABLE ONLY`, and
/// no `CREATE ROLE` ever appears because roles live outside the schema.
#[test]
fn a_pg_dump_of_a_guarded_table_is_read_whole() {
    let database = db("CREATE SCHEMA public;
         CREATE TABLE public.docs (
             id uuid NOT NULL,
             body text
         );
         ALTER TABLE public.docs OWNER TO app_owner;
         ALTER TABLE ONLY public.docs
             ADD CONSTRAINT docs_pkey PRIMARY KEY (id);
         ALTER TABLE public.docs ENABLE ROW LEVEL SECURITY;");
    let docs = database.table(Some("public"), "docs").expect("public.docs exists");

    assert_eq!(docs.owner(&database), Ok(Some("app_owner")));
    assert_eq!(docs.has_row_level_security(&database), Ok(true));
    assert_eq!(docs.has_forced_row_level_security(&database), Ok(false));
    assert_eq!(
        docs.primary_key_columns(&database)
            .expect("docs is in this database")
            .map(ColumnLike::column_name)
            .collect::<Vec<_>>(),
        vec!["id"],
        "the later ALTER TABLE ONLY did not displace the owner recorded before it"
    );
}

/// Generic code may hold a reference to a reference, which reaches the owner
/// through the blanket implementation for references rather than the table's
/// own. That forward is free to answer without ever consulting the table, so
/// it is pinned against the table's own answer.
#[test]
fn a_reference_to_a_table_answers_the_same() {
    let database = db(DUMP);
    let docs = database.table(None, "docs").expect("docs exists");

    assert_eq!(
        <&CreateTable as TableLike>::owner(&docs, &database),
        docs.owner(&database),
        "the blanket implementation for references disagrees with the table itself"
    );
    assert_eq!(<&CreateTable as TableLike>::owner(&docs, &database), Ok(Some("app_owner")));
}
