//! Integration tests for the public column-reference scope resolver
//! (`ColumnScope`), exercised through the prelude exactly as a downstream
//! consumer (pg2sqlite) would. Ports the evidence cases from pg2sqlite's
//! audit remediation plan phase 3: two tables declaring one column name, a
//! bare reference over a join, qualified references reading their own
//! table's column, quoted qualifiers, opaque relations poisoning a bare
//! reference, alias resolution, and the single-table scope for definitions.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unreachable)]

use sql_traits::{errors::LookupError, prelude::*};
use sqlparser::{
    ast::{Expr, Query, SelectItem, SetExpr, Statement},
    dialect::GenericDialect,
    parser::Parser,
};

const SCHEMA: &str = "
    CREATE TABLE a(payload TEXT, id INT);
    CREATE TABLE b(payload JSON, id INT);
";

fn schema_db() -> ParserDB {
    ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema parses")
}

fn query(sql: &str) -> Query {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("statement parses");
    match statements.pop().expect("one statement") {
        Statement::Query(query) => *query,
        other => panic!("expected a query, got {other:?}"),
    }
}

/// Resolves the query's single projected expression against a scope built
/// from the same query, rendering the resolved table's name.
fn resolve_projection(sql: &str, db: &ParserDB) -> Result<Option<String>, LookupError> {
    let query = query(sql);
    let scope = ColumnScope::from_query(&query, db)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(expr) = &select.projection.first().expect("one projection") else {
        panic!("expected an expression projection")
    };
    Ok(scope.resolve_column(expr)?.map(|table| table.table_name().to_string()))
}

/// Parses a bare column reference as the expression it would appear as in a
/// projection. The `FROM` relation is never resolved against a database.
fn reference(reference: &str) -> Expr {
    let query = query(&format!("SELECT {reference} FROM placeholder"));
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    match &select.projection.first().expect("one projection") {
        SelectItem::UnnamedExpr(expr) => (*expr).clone(),
        other => panic!("expected an expression projection, got {other:?}"),
    }
}

// Evidence: `SELECT to_json(a.payload) FROM a` must resolve to `a` even when
// another table declares `payload` with a different type.
#[test]
fn qualified_reference_reads_its_own_table() {
    let db = schema_db();
    let resolved = resolve_projection("SELECT a.payload FROM a", &db).expect("scope resolves");
    assert_eq!(resolved.as_deref(), Some("a"));
}

#[test]
fn qualified_reference_over_a_join_reads_its_own_table() {
    let db = schema_db();
    let resolved =
        resolve_projection("SELECT a.payload FROM a JOIN b ON a.id = b.id", &db).expect("resolves");
    assert_eq!(resolved.as_deref(), Some("a"));
}

// Measured on PostgreSQL 18.6: `column reference "payload" is ambiguous`.
#[test]
fn bare_reference_exposed_twice_is_ambiguous() {
    let db = schema_db();
    let result = resolve_projection("SELECT payload FROM a JOIN b ON a.id = b.id", &db);
    assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
}

#[test]
fn bare_reference_over_one_table_resolves() {
    let db = schema_db();
    let resolved = resolve_projection("SELECT payload FROM b", &db).expect("resolves");
    assert_eq!(resolved.as_deref(), Some("b"));
}

#[test]
fn alias_qualifier_resolves_to_the_aliased_table() {
    let db = schema_db();
    let resolved = resolve_projection("SELECT x.payload FROM a AS x", &db).expect("resolves");
    assert_eq!(resolved.as_deref(), Some("a"));
}

// Measured on PostgreSQL 18.6: a quoted alias answers only its exact
// spelling. `a.payload` against alias `"A"` errors there; the scope answers
// nothing, the contract's conservative form of the same miss.
#[test]
fn quoted_alias_qualifier_is_quote_aware() {
    let db = schema_db();
    let quoted = resolve_projection("SELECT \"A\".payload FROM a AS \"A\"", &db).expect("resolves");
    assert_eq!(quoted.as_deref(), Some("a"));
    let folded = resolve_projection("SELECT A.payload FROM a AS A", &db).expect("resolves");
    assert_eq!(folded.as_deref(), Some("a"));
    let missed = resolve_projection("SELECT a.payload FROM a AS \"A\"", &db).expect("resolves");
    assert_eq!(missed, None);
}

// An opaque relation (derived subquery, CTE, table function, unknown name)
// makes a bare reference unknowable, even when exactly one base table in the
// FROM exposes the name. A qualified reference still resolves.
#[test]
fn derived_subquery_poisons_a_bare_reference() {
    let db = schema_db();
    let bare = resolve_projection(
        "SELECT payload FROM (SELECT 1 AS other) s JOIN b ON s.other = b.id",
        &db,
    )
    .expect("resolves");
    assert_eq!(bare, None);
    let qualified = resolve_projection(
        "SELECT b.payload FROM (SELECT 1 AS other) s JOIN b ON s.other = b.id",
        &db,
    )
    .expect("resolves");
    assert_eq!(qualified.as_deref(), Some("b"));
}

#[test]
fn cte_reference_poisons_a_bare_reference() {
    let db = schema_db();
    let resolved = resolve_projection(
        "WITH t AS (SELECT 1 AS other) SELECT payload FROM t JOIN b ON t.other = b.id",
        &db,
    )
    .expect("resolves");
    assert_eq!(resolved, None);
}

// The second measured pg2sqlite defect: with only `b` declared, a qualified
// reference to undeclared `a` must answer nothing, so pg2sqlite refuses
// instead of reading `b`'s column type.
#[test]
fn reference_to_an_undeclared_relation_answers_nothing() {
    let db = ParserDB::parse::<GenericDialect>("CREATE TABLE b(payload JSON, id INT);")
        .expect("schema parses");
    let resolved = resolve_projection("SELECT a.payload FROM a", &db).expect("resolves");
    assert_eq!(resolved, None);
}

#[test]
fn qualifier_matching_no_relation_answers_nothing() {
    let db = schema_db();
    let resolved = resolve_projection("SELECT z.payload FROM a AS z2", &db).expect("resolves");
    assert_eq!(resolved, None);
}

#[test]
fn column_exposed_by_no_scoped_table_answers_nothing() {
    let db = schema_db();
    let resolved = resolve_projection("SELECT nope FROM a", &db).expect("resolves");
    assert_eq!(resolved, None);
}

// Measured on PostgreSQL 18.6: FROM a schema-qualified table qualifies the
// reference by the table name, and a three-part reference resolves.
#[test]
fn schema_qualified_from_qualifies_by_table_name() {
    let db = ParserDB::parse::<GenericDialect>(
        "CREATE SCHEMA s; CREATE TABLE s.a(payload TEXT); CREATE TABLE a(payload JSON);",
    )
    .expect("schema parses");
    let resolved = resolve_projection("SELECT a.payload FROM s.a", &db).expect("resolves");
    assert_eq!(resolved.as_deref(), Some("a"));
    let three_part = resolve_projection("SELECT s.a.payload FROM s.a", &db).expect("resolves");
    assert_eq!(three_part.as_deref(), Some("a"));
    let table = db.table(Some("\"s\""), "a").expect("schema table is found");
    assert_eq!(table.columns(&db).expect("columns resolve").count(), 1);
}

// Construction resolves the FROM relation names, so a malformed name is a
// construction error, matching projection_source_table's behavior.
#[test]
fn malformed_from_name_fails_construction() {
    let db = schema_db();
    let query = query("SELECT a.payload FROM a.b.c");
    let result = ColumnScope::from_query(&query, &db);
    assert!(matches!(result, Err(LookupError::InvalidObjectName { .. })), "got {:?}", result.err());
}

// A set operation has no single outer FROM, so its scope answers nothing.
#[test]
fn set_operation_body_answers_nothing() {
    let db = schema_db();
    let query = query("SELECT payload FROM a UNION SELECT payload FROM b");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    assert_eq!(scope.resolve_column(&reference("payload")).expect("resolves"), None);
    assert_eq!(scope.resolve_column(&reference("a.payload")).expect("resolves"), None);
}

// GROUP BY and DISTINCT do not change which relations are in scope.
#[test]
fn scope_survives_group_by_and_distinct() {
    let db = schema_db();
    let grouped = resolve_projection("SELECT payload FROM a GROUP BY payload", &db).expect("ok");
    assert_eq!(grouped.as_deref(), Some("a"));
    let distinct = resolve_projection("SELECT DISTINCT payload FROM a", &db).expect("ok");
    assert_eq!(distinct.as_deref(), Some("a"));
}

// Definition contexts (constraint checks, computed columns, index
// expressions, policy conditions, trigger bodies) resolve against the
// defined table's own columns, measured on PostgreSQL 18.6: a CHECK sees
// its own columns bare or qualified by its own name, and a CHECK naming
// another table errors. The single-table scope answers nothing for the
// other table instead of erroring.
#[test]
fn single_table_scope_resolves_the_defined_tables_columns() {
    let db = schema_db();
    let table = db.table(None, "a").expect("table a exists");
    let scope = ColumnScope::for_table(table, &db);
    assert_eq!(
        scope
            .resolve_column(&reference("payload"))
            .expect("bare resolves")
            .map(TableLike::table_name),
        Some("a")
    );
    assert_eq!(
        scope
            .resolve_column(&reference("a.payload"))
            .expect("qualified resolves")
            .map(TableLike::table_name),
        Some("a")
    );
    assert_eq!(scope.resolve_column(&reference("b.payload")).expect("resolves"), None);
    assert_eq!(scope.resolve_column(&reference("nope")).expect("resolves"), None);
}

#[test]
fn single_table_scope_qualifier_is_quote_aware() {
    let db =
        ParserDB::parse::<GenericDialect>("CREATE TABLE \"Mix\"(v INT);").expect("schema parses");
    let table = db.table(None, "\"Mix\"").expect("quoted table is found");
    let scope = ColumnScope::for_table(table, &db);
    assert_eq!(
        scope
            .resolve_column(&reference("\"Mix\".v"))
            .expect("exact qualifier resolves")
            .map(TableLike::table_name),
        Some("Mix")
    );
    assert_eq!(scope.resolve_column(&reference("mix.v")).expect("resolves"), None);
}

// The scope borrows the query only for the FROM names, so a scope built
// from a parsed query and database can resolve references parsed from
// elsewhere (a policy condition re-parsed from a string, an index
// expression).
#[test]
fn scope_resolves_references_from_outside_the_query() {
    let db = schema_db();
    let query = query("SELECT 1 FROM a JOIN b ON a.id = b.id");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    assert_eq!(
        scope.resolve_column(&reference("a.payload")).expect("resolves").map(TableLike::table_name),
        Some("a")
    );
    assert_eq!(
        scope.resolve_column(&reference("b.payload")).expect("resolves").map(TableLike::table_name),
        Some("b")
    );
}
