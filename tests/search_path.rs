//! Tests that `SET search_path` decides where an unqualified name looks, and
//! where one a statement creates lands.
//!
//! PostgreSQL lets a schema put a schema on the path once and then drop the
//! prefix for the rest of the file. Every statement naming a table has to agree
//! about that, since a schema is read top to bottom and a trigger, a grant, an
//! index, a foreign key and a policy all resolve the same bare name. Before
//! this, the statement was discarded and four of those five refused a schema a
//! real server accepts.
//!
//! Reading a bare name and writing one have to agree too. A bundle that sets
//! the path and then creates unqualified is ordinary, and recording those
//! tables in no schema puts every one of them where nothing can find it, while
//! answering for a table the server never created.
#![allow(clippy::expect_used)]

use sql_traits::{
    errors::{Error, LookupError},
    prelude::*,
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};

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

/// The whole shape a hand-written schema bundle uses: the path is set once and
/// every statement after it drops the prefix, creation included.
const CREATED_ON_THE_PATH: &str = "CREATE SCHEMA app;
     SET search_path TO app;
     CREATE TABLE docs (id INT PRIMARY KEY, owner_id TEXT);
     CREATE POLICY p ON docs FOR SELECT USING (owner_id = current_user);
     CREATE INDEX i ON docs (owner_id);
     CREATE ROLE r;
     GRANT SELECT ON docs TO r;
     CREATE TABLE child (d INT REFERENCES docs(id));
";

/// A bare `CREATE` lands where the server would put it, and every later
/// statement naming it bare reaches the same table.
#[test]
fn a_bare_create_lands_in_the_schema_on_the_path() {
    let db = parse(CREATED_ON_THE_PATH);

    let docs = db.table(Some("app"), "docs").expect("created in the schema on the path");
    assert_eq!(docs.table_name(), "docs");
    assert!(db.table(None, "docs").is_none(), "no schema-less table was created");

    let child = db.table(Some("app"), "child").expect("created in the schema on the path");
    let foreign_key =
        child.foreign_keys(&db).expect("child is in this database").next().expect("one key");
    assert_eq!(foreign_key.referenced_table(&db).expect("resolves").table_schema(), Some("app"));

    let policy = db.policies().next().expect("one policy");
    assert!(
        core::ptr::eq(policy.table(&db).expect("resolves"), docs),
        "the policy names the table that was created"
    );
}

/// Two schemas may hold the same name, and the path decides which one a bundle
/// writing that name bare creates. This is the case a lone bare name cannot
/// stand in for.
#[test]
fn two_schemas_may_hold_the_same_name() {
    let db = parse(
        "CREATE SCHEMA a;
         CREATE SCHEMA b;
         SET search_path TO a;
         CREATE TABLE docs (id INT PRIMARY KEY);
         SET search_path TO b;
         CREATE TABLE docs (id INT PRIMARY KEY);
        ",
    );

    assert!(db.table(Some("a"), "docs").is_some());
    assert!(db.table(Some("b"), "docs").is_some());
    assert_eq!(db.tables().count(), 2, "the two names stayed apart");
}

/// The default path names the default schema, which this model already spells
/// without the prefix, so nothing about a bundle that never sets a path moves.
#[test]
fn the_default_path_leaves_a_bare_create_bare() {
    for head in ["", "SET search_path TO public;", "SET search_path TO nope, public;"] {
        let db = parse(&format!("{head} CREATE TABLE docs (id INT);"));
        assert_eq!(db.table(None, "docs").map(TableLike::table_schema), Some(None), "after {head}");
    }
}

/// A bare name and one written out in full are one table, before this change
/// and after it.
#[test]
fn a_bare_create_and_a_written_one_collide() {
    let clash = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE SCHEMA app;
         SET search_path TO app;
         CREATE TABLE docs (id INT);
         CREATE TABLE app.docs (id INT);",
    );
    assert!(
        matches!(clash, Err(Error::IdentifierLookupError(LookupError::TableLookupConflict { .. }))),
        "got {clash:?}"
    );
}

/// A temporary table lives in a schema private to the session, so the path does
/// not place it and it cannot collide with the permanent table of that name.
#[test]
fn a_temporary_table_is_left_off_the_path() {
    for temporary in ["TEMP", "TEMPORARY", "GLOBAL TEMPORARY", "LOCAL TEMPORARY"] {
        let db = parse(&format!(
            "CREATE SCHEMA app;
             SET search_path TO app;
             CREATE {temporary} TABLE docs (id INT);
             CREATE TABLE app.docs (id INT, extra INT);"
        ));

        assert!(db.table(None, "docs").is_some(), "{temporary} was placed on the path");
        let permanent = db.table(Some("app"), "docs").expect("the permanent table stands apart");
        assert_eq!(permanent.columns(&db).expect("in this database").count(), 2);
    }
}

/// A server creates in the first schema on the path that exists, so an entry
/// naming nothing is passed over rather than taken.
#[test]
fn the_walk_passes_a_schema_the_input_never_creates() {
    let db = parse(
        "CREATE SCHEMA app;
         SET search_path TO nope, app;
         CREATE TABLE docs (id INT);",
    );
    assert!(db.table(Some("app"), "docs").is_some());
}

/// When no entry names a schema the input creates, the refusal is the one a
/// schema written out in full already gets, rather than a table in no schema.
#[test]
fn a_path_naming_only_absent_schemas_is_refused() {
    let refused = ParserDB::parse::<PostgreSqlDialect>(
        "SET search_path TO nope; CREATE TABLE docs (id INT);",
    );
    assert!(
        matches!(&refused, Err(Error::SchemaNotFoundForTable { schema_name, table_name })
            if schema_name == "nope" && table_name == "docs"),
        "got {refused:?}"
    );
}

/// An emptied path names no schema to create in, which a real server refuses
/// exactly as it refuses a path naming only schemas that are absent.
#[test]
fn an_emptied_path_is_refused() {
    let refused =
        ParserDB::parse::<PostgreSqlDialect>("SET search_path TO ''; CREATE TABLE docs (id INT);");
    assert!(
        matches!(&refused, Err(Error::NoSchemaSelectedForTable { table_name })
            if table_name == "docs"),
        "got {refused:?}"
    );

    // Naming the schema in full still says where it goes.
    let written =
        parse("CREATE SCHEMA app; SET search_path TO ''; CREATE TABLE app.docs (id INT);");
    assert!(written.table(Some("app"), "docs").is_some());
}

/// The refusal reads the name of the table it refuses, and a caller assembling
/// statements by hand rather than parsing them may hand over one with no name.
#[test]
fn a_node_with_no_name_is_left_where_it_is() {
    let mut statements = Parser::parse_sql(
        &PostgreSqlDialect {},
        "SET search_path TO nope; CREATE TABLE placeholder (id INT);",
    )
    .expect("the text parses");
    let create_table = statements
        .last_mut()
        .and_then(|statement| {
            match statement {
                Statement::CreateTable(create_table) => Some(create_table),
                _ => None,
            }
        })
        .expect("the second statement creates a table");
    create_table.name.0.clear();

    let db = ParserDB::from_statements(statements, "db".to_string());
    assert!(db.is_ok(), "a nameless node was placed rather than passed over: {db:?}");
}

/// Where a table lands follows the path at the moment it is created, so
/// restoring the path restores where the next one lands.
#[test]
fn resetting_the_path_returns_a_bare_create_to_bare() {
    let db = parse(
        "CREATE SCHEMA app;
         SET search_path TO app;
         CREATE TABLE on_path (id INT);
         RESET search_path;
         CREATE TABLE off_path (id INT);",
    );
    assert!(db.table(Some("app"), "on_path").is_some());
    assert!(db.table(None, "off_path").is_some());
}

/// A name written out in full says where it goes, and the path does not argue.
#[test]
fn a_written_schema_outranks_the_path() {
    let db = parse(
        "CREATE SCHEMA app;
         CREATE SCHEMA other;
         SET search_path TO app;
         CREATE TABLE other.docs (id INT);",
    );
    assert!(db.table(Some("other"), "docs").is_some());
    assert!(db.table(Some("app"), "docs").is_none());
}

/// The schema is settled before the name is read, so `IF NOT EXISTS` weighs the
/// table the statement would really create.
#[test]
fn if_not_exists_weighs_the_schema_the_path_selects() {
    let db = parse(
        "CREATE SCHEMA app;
         SET search_path TO app;
         CREATE TABLE app.docs (id INT);
         CREATE TABLE IF NOT EXISTS docs (other INT);",
    );
    assert_eq!(db.tables().count(), 1, "the second statement named the first table");
    let docs = db.table(Some("app"), "docs").expect("the first table");
    assert_eq!(docs.columns(&db).expect("in this database").count(), 1);
    assert!(docs.column("id", &db).is_ok(), "the first table was kept");
}

/// `IF NOT EXISTS` asks whether a table is already there, not whether there is
/// anywhere to put one, so it does not swallow the refusal. A real server
/// refuses this even though the bare name it skips over does exist.
#[test]
fn if_not_exists_does_not_swallow_the_refusal() {
    let refused = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE TABLE b (id INT);
         SET search_path TO nope;
         CREATE TABLE IF NOT EXISTS b (id INT);",
    );
    assert!(
        matches!(&refused, Err(Error::SchemaNotFoundForTable { schema_name, .. })
            if schema_name == "nope"),
        "got {refused:?}"
    );
}

/// A created table carries the catalog spelling of its schema, so a quoted one
/// keeps its case and reads back exactly as writing the name out in full does.
#[test]
fn a_quoted_schema_keeps_its_case() {
    let db = parse(
        "CREATE SCHEMA \"App\";
         SET search_path TO \"App\";
         CREATE TABLE docs (id INT);",
    );
    let docs = db.table(Some("\"App\""), "docs").expect("created in the quoted schema");
    assert_eq!(docs.table_schema(), Some("App"));
    assert!(docs.table_schema_is_quoted());
    assert!(db.table(Some("app"), "docs").is_none(), "the case was not folded away");

    let written = parse("CREATE SCHEMA \"App\"; CREATE TABLE \"App\".docs (id INT);");
    let written_docs = written.table(Some("\"App\""), "docs").expect("written out in full");
    assert_eq!(docs.table_schema(), written_docs.table_schema());
    assert_eq!(docs.table_schema_is_quoted(), written_docs.table_schema_is_quoted());
}

/// The downstream finding: a table created bare under a path carrying `public`
/// resides there, so a later path prefers an earlier schema deterministically
/// rather than reporting an ambiguity.
#[test]
fn a_bare_table_yields_to_an_earlier_schema_on_the_path() {
    let db = parse(
        "CREATE SCHEMA aaa;
         CREATE TABLE aaa.docs (id INT);
         SET search_path TO public;
         CREATE TABLE docs (id INT);
         SET search_path TO aaa, public;
         CREATE POLICY docs_sel ON docs FOR SELECT USING (true);",
    );
    let policy = db.policies().next().expect("the policy exists");
    assert_eq!(policy.table(&db).expect("resolves").table_schema(), Some("aaa"));
}

/// With `public` ahead on the path, the bare-created table wins instead.
#[test]
fn public_first_on_the_path_wins_instead() {
    let db = parse(
        "CREATE SCHEMA aaa;
         CREATE TABLE aaa.docs (id INT);
         SET search_path TO public;
         CREATE TABLE docs (id INT);
         SET search_path TO public, aaa;
         CREATE POLICY docs_sel ON docs FOR SELECT USING (true);",
    );
    let policy = db.policies().next().expect("the policy exists");
    assert_eq!(policy.table(&db).expect("resolves").table_schema(), None);
}

/// A bare name and a `public` one are two spellings of one place, so a lookup
/// reaches the table through either.
#[test]
fn the_default_schema_answers_both_spellings() {
    let bare = parse("SET search_path TO public; CREATE TABLE docs (id INT);");
    assert!(bare.table(Some("public"), "docs").is_some());
    assert!(bare.table(None, "docs").is_some());

    let written = parse("CREATE TABLE public.docs (id INT);");
    assert!(written.table(Some("public"), "docs").is_some());
    assert!(written.table(None, "docs").is_some());
}

/// A bare table resides in `public`, so a path omitting `public` cannot reach
/// it, which is the refusal a real server gives.
#[test]
fn a_bare_table_is_off_a_path_omitting_public() {
    let refused = ParserDB::parse::<PostgreSqlDialect>(
        "CREATE SCHEMA app;
         CREATE TABLE docs (id INT);
         SET search_path TO app;
         CREATE POLICY docs_sel ON docs USING (true);",
    );
    assert!(
        matches!(refused, Err(Error::TableNotFoundForPolicy { ref table_name, .. }) if table_name == "docs"),
        "got {refused:?}"
    );
}

/// The server's own default path shape: `"$user"` names no created schema and
/// is walked past, for creation and for resolution alike.
#[test]
fn the_user_entry_is_walked_past() {
    let db = parse(
        "SET search_path TO \"$user\", public;
         CREATE TABLE docs (id INT);
         CREATE POLICY docs_sel ON docs USING (true);",
    );
    let docs = db.table(Some("public"), "docs").expect("created in the default schema");
    assert_eq!(docs.table_schema(), None, "stored in the bare spelling");
    let policy = db.policies().next().expect("the policy exists");
    assert!(core::ptr::eq(policy.table(&db).expect("resolves"), docs));
}

/// A statement may spell out the default schema a bare create left implicit.
#[test]
fn statements_may_qualify_the_default_schema() {
    let db = parse(
        "CREATE TABLE docs (id INT);
         CREATE ROLE r;
         GRANT SELECT ON public.docs TO r;
         CREATE INDEX i ON public.docs (id);
         CREATE POLICY p ON public.docs USING (true);",
    );

    let docs = db.table(None, "docs").expect("the table exists");
    let policy = db.policies().next().expect("the policy exists");
    assert!(core::ptr::eq(policy.table(&db).expect("resolves"), docs));

    let index = db.indexes().next().expect("the index exists");
    assert_eq!(IndexLike::table(index, &db).table_name(), "docs");

    let grant = db.table_grants().next().expect("the grant exists");
    assert_eq!(grant.tables(&db).count(), 1);
}
