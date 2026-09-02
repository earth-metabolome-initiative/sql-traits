//! Integration tests for incremental schema ingestion.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use sql_traits::{
    structs::{ColumnDefinition, ColumnScope, ParserDB, ParserDBIngestor},
    traits::{ColumnLike, DatabaseLike, TableLike},
};
use sqlparser::{
    ast::{SelectItem, SetExpr, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

fn statements(sql: &str) -> Vec<Statement> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("SQL parses")
}

fn inspect_definition<R>(
    database: &ParserDB,
    inspect: impl FnOnce(ColumnDefinition<'_, '_, '_, ParserDB>) -> R,
) -> R {
    let Statement::Query(query) = statements("SELECT v.id FROM v").pop().expect("one statement")
    else {
        panic!("expected a query")
    };
    let scope = ColumnScope::from_query(&query, database).expect("query scope builds");
    let SetExpr::Select(select) = query.body.as_ref() else { panic!("expected a select") };
    let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
        panic!("expected an expression")
    };
    let definition = scope
        .resolve_column_definition(reference)
        .expect("definition resolves")
        .expect("column exists");
    inspect(definition)
}

#[test]
fn snapshots_preserve_each_incremental_schema_state() {
    let mut input = ParserDBIngestor::new::<PostgreSqlDialect>("test".to_owned());

    for statement in statements("CREATE TABLE a (id INT PRIMARY KEY);") {
        input = input.apply_statement(statement).expect("table applies");
    }
    for statement in statements("CREATE VIEW v AS SELECT id FROM a;") {
        input = input.apply_statement(statement).expect("first view applies");
    }
    let first = input.snapshot();
    assert!(inspect_definition(&first, |definition| {
        matches!(
            definition,
            ColumnDefinition::Base { table, column }
                if table.table_name() == "a" && column.column_name() == "id"
        )
    }));
    for statement in statements("DROP VIEW v;") {
        input = input.apply_statement(statement).expect("drop applies");
    }
    let dropped = input.snapshot();
    assert!(dropped.view(None, "v").is_none());

    for statement in statements("CREATE VIEW v AS SELECT id + 1 AS id FROM a;") {
        input = input.apply_statement(statement).expect("second view applies");
    }
    let second = input.finish();
    let definition = inspect_definition(&second, |definition| {
        match definition {
            ColumnDefinition::Expression { expression, .. } => expression.to_string(),
            ColumnDefinition::Base { .. } => "base".to_owned(),
            ColumnDefinition::SetOperation { .. } => "set operation".to_owned(),
            ColumnDefinition::RecursiveUnion { .. } => "recursive union".to_owned(),
            ColumnDefinition::Opaque => "opaque".to_owned(),
        }
    });
    assert_eq!(definition, "id + 1");

    assert!(inspect_definition(&first, |definition| {
        matches!(
            definition,
            ColumnDefinition::Base { table, column }
                if table.table_name() == "a" && column.column_name() == "id"
        )
    }));
}

#[test]
fn parse_options_apply_to_incremental_statements() {
    use sql_traits::structs::{AccessResolution, ParseOptions};

    let input = ParseOptions::default()
        .with_access_resolution(AccessResolution::OpenWorld)
        .ingestor::<PostgreSqlDialect>("test".to_owned());
    let input = statements("CREATE TABLE t (id INT);")
        .into_iter()
        .try_fold(input, ParserDBIngestor::apply_statement)
        .expect("table applies");
    let input = statements("GRANT SELECT ON t TO missing_role;")
        .into_iter()
        .try_fold(input, ParserDBIngestor::apply_statement)
        .expect("open world grant applies");

    assert_eq!(input.finish().table_grants().count(), 1);
}

#[test]
fn postgres_catalog_and_created_collations_survive_incremental_statements() {
    use sql_traits::{
        structs::{ParseOptions, PostgresCatalog, PostgresCatalogCollation, PostgresCatalogType},
        traits::ColumnCollation,
    };

    let catalog = PostgresCatalog::empty()
        .with_collation(PostgresCatalogCollation::new("base", false).with_deterministic(false))
        .with_collatable_type(PostgresCatalogType::new("text", false));
    let input = ParseOptions::default()
        .with_postgres_catalog(catalog)
        .ingestor::<PostgreSqlDialect>("test".to_owned());
    let input = statements("CREATE COLLATION ci FROM base;")
        .into_iter()
        .try_fold(input, ParserDBIngestor::apply_statement)
        .expect("collation applies");
    let input = statements("CREATE TABLE t (name TEXT COLLATE ci);")
        .into_iter()
        .try_fold(input, ParserDBIngestor::apply_statement)
        .expect("table applies");

    let database = input.finish();
    let table = database.table(None, "t").expect("table exists");
    let column =
        table.column("name", &database).expect("column lookup runs").expect("column exists");
    let ColumnCollation::Named(collation) =
        column.collation(&database).expect("collation metadata resolves")
    else {
        panic!("expected a named collation")
    };
    assert_eq!(collation.name().name(), "ci");
    assert_eq!(collation.postgres_deterministic(), Some(false));
}
