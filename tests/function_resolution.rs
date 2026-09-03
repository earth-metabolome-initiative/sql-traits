//! Tests that a function is found the way the database finds one: by a written
//! reference, by the search path, or by the identity it is stored under.
//!
//! A function used to be found by its last name part alone, so a declaration in
//! a schema nothing reaches answered a bare reference and a qualified reference
//! answered nothing at all. Resolution here is by name only: two declarations
//! differing in their arguments share one name, and this surface does not
//! choose between them.
#![allow(clippy::expect_used)]

use sql_traits::{errors::LookupError, prelude::*, structs::TargetName};
use sqlparser::dialect::PostgreSqlDialect;

const TWO_SCHEMAS: &str = "CREATE SCHEMA app;
CREATE SCHEMA audit;
CREATE FUNCTION app.helper() RETURNS INT LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION audit.helper() RETURNS INT LANGUAGE sql AS 'SELECT 2';
";

fn db(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(sql).expect("schema parses")
}

fn target(text: &str) -> TargetName<'_> {
    TargetName::parse(text).expect("the target name reads")
}

fn schema_of(function: &<ParserDB as DatabaseLike>::Function) -> Option<String> {
    function.target_name().schema().map(ToString::to_string)
}

/// A qualified reference resolves in its own schema, and an unqualified one
/// only through the search path, which carries neither schema here.
#[test]
fn a_qualified_reference_resolves_and_an_unreachable_one_does_not() -> Result<(), LookupError> {
    let db = db(TWO_SCHEMAS);

    let app = db
        .resolve_target_function(target("app.helper"))?
        .expect("the qualified reference resolves");
    assert_eq!(schema_of(app).as_deref(), Some("app"));

    let audit = db
        .resolve_target_function(target("audit.helper"))?
        .expect("the qualified reference resolves");
    assert_eq!(schema_of(audit).as_deref(), Some("audit"));

    // Neither schema is on the default path, so the bare name reaches nothing
    // rather than answering whichever was declared first.
    assert!(db.resolve_target_function(target("helper"))?.is_none());
    assert!(db.resolve_target_function(target("absent"))?.is_none());

    Ok(())
}

/// The search path decides which same-named declaration a bare reference
/// reaches, and its order decides which one wins.
#[test]
fn the_search_path_order_decides_a_bare_reference() -> Result<(), LookupError> {
    let audit_first = db(&format!("{TWO_SCHEMAS} SET search_path TO audit, app;"));
    let app_first = db(&format!("{TWO_SCHEMAS} SET search_path TO app, audit;"));

    let audit = audit_first
        .resolve_target_function(target("helper"))?
        .expect("the first path entry holds it");
    assert_eq!(schema_of(audit).as_deref(), Some("audit"));

    let app = app_first
        .resolve_target_function(target("helper"))?
        .expect("the first path entry holds it");
    assert_eq!(schema_of(app).as_deref(), Some("app"));

    Ok(())
}

/// A written reference folds an unquoted name and reads a quoted one exactly,
/// as the table lookup beside it does.
#[test]
fn a_written_lookup_applies_the_identifier_rules() {
    let db = db(TWO_SCHEMAS);

    assert!(db.function(Some("app"), "helper").is_some());
    assert!(db.function(Some("APP"), "HELPER").is_some());
    assert!(db.function(Some("\"App\""), "helper").is_none());
    assert!(db.function(Some("audit"), "helper").is_some());

    // Nothing declares `helper` in the default schema.
    assert!(db.function(None, "helper").is_none());
    assert!(db.function(Some("app"), "absent").is_none());
}

/// A registered builtin lives in the catalog schema, so it is reached by naming
/// that schema and not by a bare reference.
#[test]
fn a_builtin_is_reached_through_its_own_schema() -> Result<(), LookupError> {
    let db = db("CREATE FUNCTION helper() RETURNS INT LANGUAGE sql AS 'SELECT 1';");

    assert!(db.function(Some("pg_catalog"), "coalesce").is_some());
    assert!(db.function(None, "coalesce").is_none());
    assert!(db.resolve_target_function(target("pg_catalog.coalesce"))?.is_some());
    assert!(db.resolve_target_function(target("coalesce"))?.is_none());

    // A declaration written without a schema resides in the default one, which
    // the path does carry.
    assert!(db.function(None, "helper").is_some());
    assert!(db.resolve_target_function(target("helper"))?.is_some());

    Ok(())
}

/// The identity lookup compares both parts as stored: nothing folds, and the
/// absent schema is not read as `public`.
#[test]
fn an_identity_lookup_compares_stored_parts() -> Result<(), LookupError> {
    let db = db(&format!(
        "{TWO_SCHEMAS} CREATE FUNCTION bare() RETURNS INT LANGUAGE sql AS 'SELECT 3';"
    ));

    assert!(db.function_by_stored_identity(Some("app"), "helper")?.is_some());
    assert!(db.function_by_stored_identity(Some("app"), "HELPER")?.is_none());
    assert!(db.function_by_stored_identity(None, "helper")?.is_none());
    assert!(db.function_by_stored_identity(None, "bare")?.is_some());
    assert!(db.function_by_stored_identity(Some("public"), "bare")?.is_none());
    assert!(db.function_by_stored_identity(Some("pg_catalog"), "coalesce")?.is_some());

    Ok(())
}

/// Two declarations differing only in their arguments share one name, and a
/// surface that resolves by name says so rather than picking one.
#[test]
fn overloads_of_one_name_are_reported_as_ambiguous() {
    let db = db("CREATE SCHEMA app;
         CREATE FUNCTION app.f(x INT) RETURNS INT LANGUAGE sql AS 'SELECT 1';
         CREATE FUNCTION app.f(x TEXT) RETURNS INT LANGUAGE sql AS 'SELECT 2';");

    assert!(matches!(
        db.resolve_target_function(target("app.f")),
        Err(LookupError::AmbiguousFunctionLookup { .. })
    ));
    assert!(matches!(
        db.function_by_stored_identity(Some("app"), "f"),
        Err(LookupError::AmbiguousFunctionLookup { .. })
    ));

    // The written lookup answers the first of them, as the table lookup does.
    assert!(db.function(Some("app"), "f").is_some());
}
