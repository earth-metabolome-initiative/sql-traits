//! Integration tests for the data-statement analysis traits
//! (`DataStatementLike`, `DQLLike`, `DMLLike`), exercising them through the
//! public prelude exactly as a downstream consumer (subql, connetto) would.

use sql_traits::{errors::LookupError, prelude::*};
use sqlparser::{
    ast::{Query, Statement},
    dialect::{GenericDialect, MySqlDialect},
    parser::Parser,
};

const SCHEMA: &str = "
    CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE orders (
        id INT PRIMARY KEY,
        user_id INT REFERENCES users(id),
        total INT
    );
    CREATE TABLE order_items (order_id INT, sku TEXT, qty INT, PRIMARY KEY (order_id, sku));
";

fn schema_db() -> ParserDB {
    ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema parses")
}

fn statement(sql: &str) -> Statement {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("statement parses");
    statements.pop().expect("one statement")
}

fn query(sql: &str) -> Query {
    match statement(sql) {
        Statement::Query(query) => *query,
        other => panic!("expected a query, got {other:?}"),
    }
}

fn referenced_names<S: DataStatementLike<ParserDB>>(stmt: &S, db: &ParserDB) -> Vec<String> {
    stmt.referenced_tables(db)
        .expect("referenced_tables succeeds")
        .iter()
        .map(|table| table.table_name().to_string())
        .collect()
}

fn projection_name(sql: &str, db: &ParserDB) -> Option<String> {
    query(sql)
        .projection_source_table(db)
        .expect("projection_source_table succeeds")
        .map(|table| table.table_name().to_string())
}

#[test]
fn referenced_tables_for_join_routes_both_base_tables() {
    let db = schema_db();
    let names = referenced_names(
        &query("SELECT o.total FROM orders o JOIN users u ON o.user_id = u.id"),
        &db,
    );
    assert_eq!(names, vec!["orders".to_string(), "users".to_string()]);
}

#[test]
fn referenced_tables_includes_subquery_tables() {
    let db = schema_db();
    let names = referenced_names(
        &query("SELECT id FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"),
        &db,
    );
    assert_eq!(names, vec!["users".to_string(), "orders".to_string()]);
}

#[test]
fn single_table_query_is_eligible_with_primary_key() {
    // The subql eligibility composite: a query whose output rows are rows of a
    // single base table that has a primary key can be re-executed as a
    // PK-keyed patchset.
    let db = schema_db();
    let analyzed = query("SELECT o.id, o.total FROM orders o JOIN users u ON o.user_id = u.id");

    let source = analyzed
        .projection_source_table(&db)
        .expect("projection resolves")
        .expect("single base table");
    assert_eq!(source.table_name(), "orders");
    assert!(source.has_primary_key(&db));

    let pk: Vec<&str> = source.primary_key_columns(&db).map(ColumnLike::column_name).collect();
    assert_eq!(pk, vec!["id"]);
}

#[test]
fn multi_table_projection_is_not_eligible() {
    let db = schema_db();
    assert_eq!(
        projection_name("SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id", &db,),
        None,
    );
}

#[test]
fn wildcard_over_join_is_not_eligible() {
    let db = schema_db();
    assert_eq!(
        projection_name("SELECT * FROM orders o JOIN users u ON o.user_id = u.id", &db),
        None,
    );
}

#[test]
fn aggregate_and_group_by_are_not_eligible() {
    let db = schema_db();
    assert_eq!(projection_name("SELECT COUNT(*) FROM orders", &db), None);
    assert_eq!(projection_name("SELECT user_id FROM orders GROUP BY user_id", &db), None);
}

#[test]
fn computed_projection_is_not_eligible() {
    let db = schema_db();
    assert_eq!(projection_name("SELECT total * 2 FROM orders", &db), None);
}

#[test]
fn set_operation_is_not_eligible_but_routes_all_tables() {
    let db = schema_db();
    let sql = "SELECT id FROM users UNION SELECT id FROM orders";
    assert_eq!(projection_name(sql, &db), None);
    assert_eq!(referenced_names(&query(sql), &db), vec!["users".to_string(), "orders".to_string()],);
}

#[test]
fn cte_body_routes_but_cte_reference_is_not_a_base_table() {
    let db = schema_db();
    let sql = "WITH high AS (SELECT id FROM orders WHERE total > 100) SELECT id FROM high";
    // `orders` (CTE body) is routed; `high` (the CTE name) is not a base table.
    assert_eq!(referenced_names(&query(sql), &db), vec!["orders".to_string()]);
    assert_eq!(projection_name(sql, &db), None);
}

#[test]
fn ambiguous_unqualified_column_is_an_error() {
    let db = schema_db();
    let result = query("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
        .projection_source_table(&db);
    assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
}

#[test]
fn composite_primary_key_table_projection() {
    let db = schema_db();
    let source = projection_name("SELECT order_id, sku, qty FROM order_items", &db);
    assert_eq!(source, Some("order_items".to_string()));

    let table = db.table(None, "order_items").unwrap();
    let pk: Vec<&str> = table.primary_key_columns(&db).map(ColumnLike::column_name).collect();
    assert_eq!(pk, vec!["order_id", "sku"]);
}

#[test]
fn insert_target_kind_and_referenced_tables() {
    let db = schema_db();
    let Statement::Insert(insert) = statement("INSERT INTO orders SELECT * FROM orders") else {
        panic!("expected insert");
    };
    assert_eq!(insert.kind(), DmlKind::Insert);
    assert_eq!(insert.target_table(&db).unwrap().table_name(), "orders");
    assert_eq!(referenced_names(&insert, &db), vec!["orders".to_string()]);
}

#[test]
fn update_target_kind_and_referenced_tables() {
    let db = schema_db();
    let Statement::Update(update) =
        statement("UPDATE orders SET total = 0 FROM users WHERE orders.user_id = users.id")
    else {
        panic!("expected update");
    };
    assert_eq!(update.kind(), DmlKind::Update);
    assert_eq!(update.target_table(&db).unwrap().table_name(), "orders");
    assert_eq!(referenced_names(&update, &db), vec!["orders".to_string(), "users".to_string()],);
}

#[test]
fn delete_target_kind_and_referenced_tables() {
    let db = schema_db();
    let Statement::Delete(delete) =
        statement("DELETE FROM orders WHERE user_id IN (SELECT id FROM users)")
    else {
        panic!("expected delete");
    };
    assert_eq!(delete.kind(), DmlKind::Delete);
    assert_eq!(delete.target_table(&db).unwrap().table_name(), "orders");
    assert_eq!(referenced_names(&delete, &db), vec!["orders".to_string(), "users".to_string()],);
}

#[test]
fn mysql_multi_table_delete_has_no_single_target() {
    let db = schema_db();
    let mut statements = Parser::parse_sql(
        &MySqlDialect {},
        "DELETE users, orders FROM users JOIN orders ON users.id = orders.user_id",
    )
    .expect("statement parses");
    let Statement::Delete(delete) = statements.pop().unwrap() else {
        panic!("expected delete");
    };
    assert!(matches!(delete.target_table(&db), Err(LookupError::InvalidObjectName { .. })));
}
