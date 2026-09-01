//! Integration tests for derived column definitions.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unreachable)]

use core::{fmt::Write as _, ops::ControlFlow};

use sql_traits::{errors::LookupError, prelude::*};
use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SelectItem, SetExpr,
        SetOperator, Statement, Visit, VisitMut, Visitor, VisitorMut,
    },
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

fn reference(reference: &str) -> Expr {
    let query = query(&format!("SELECT {reference} FROM placeholder"));
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    match &select.projection[0] {
        SelectItem::UnnamedExpr(expression) => expression.clone(),
        other => panic!("expected an expression projection, got {other:?}"),
    }
}

fn scope_with_independent_lifetimes<'query, 'database>(
    query: &'query Query,
    database: &'database ParserDB,
) -> ColumnScope<'query, 'database, ParserDB> {
    ColumnScope::from_query(query, database).expect("scope builds")
}

fn assert_copy<T: Copy>(_: T) {}

#[test]
fn recursive_cte_exposes_the_anchor_and_recursive_definitions() {
    let db = ParserDB::parse::<GenericDialect>("CREATE TABLE categories(id INT, parent_id INT);")
        .expect("schema parses");
    let query = query(
        "WITH RECURSIVE tree AS (\
            SELECT id, 0 AS depth FROM categories \
            UNION ALL \
            SELECT c.id, t.depth + 1 FROM categories c \
            JOIN tree t ON c.parent_id = t.id\
         ) SELECT t.depth FROM tree t",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(output_reference) = &select.projection[0] else {
        panic!("expected an expression projection")
    };
    let Some(ColumnDefinition::RecursiveUnion { anchor, recursive }) =
        scope.resolve_column_definition(output_reference).expect("definition resolves")
    else {
        panic!("expected a recursive union definition")
    };
    let ColumnDefinition::Expression { expression: anchor, .. } = anchor.definition() else {
        panic!("expected an anchor expression")
    };
    let ColumnDefinition::Expression { expression: recursive_expression, scope: recursive_scope } =
        recursive.definition()
    else {
        panic!("expected a recursive expression")
    };
    assert_eq!(anchor.to_string(), "0");
    assert_eq!(recursive_expression.to_string(), "t.depth + 1");
    assert!(matches!(
        recursive_scope
            .resolve_column_definition(&reference("t.depth"))
            .expect("seeded input resolves"),
        Some(ColumnDefinition::Expression { expression, .. })
            if expression.to_string() == "0"
    ));
}

#[test]
fn nested_set_operations_preserve_every_definition() {
    let db = schema_db();
    let query = query(
        "WITH v AS (\
            SELECT 1 AS n \
            UNION ALL (SELECT 2 INTERSECT SELECT 3)\
         ) SELECT v.n FROM v",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
        panic!("expected an expression projection")
    };
    let Some(ColumnDefinition::SetOperation { operator, left, right }) =
        scope.resolve_column_definition(reference).expect("definition resolves")
    else {
        panic!("expected a set operation definition")
    };
    assert_eq!(operator, SetOperator::Union);
    assert!(matches!(
        left.definition(),
        ColumnDefinition::Expression { expression, .. } if expression.to_string() == "1"
    ));
    let ColumnDefinition::SetOperation { operator, left, right } = right.definition() else {
        panic!("expected a nested set operation")
    };
    assert_eq!(operator, SetOperator::Intersect);
    assert!(matches!(
        left.definition(),
        ColumnDefinition::Expression { expression, .. } if expression.to_string() == "2"
    ));
    assert!(matches!(
        right.definition(),
        ColumnDefinition::Expression { expression, .. } if expression.to_string() == "3"
    ));
}

#[test]
fn base_definition_carries_the_table_and_original_column() {
    let db = schema_db();
    let query = query("WITH v AS (SELECT payload AS copy FROM b) SELECT v.copy FROM v");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
        panic!("expected an expression projection")
    };
    let Some(ColumnDefinition::Base { table, column }) =
        scope.resolve_column_definition(reference).expect("definition resolves")
    else {
        panic!("expected a base column definition")
    };
    assert_eq!(table.table_name(), "b");
    assert_eq!(column.column_name(), "payload");
}

#[test]
fn query_and_database_lifetimes_are_independent() {
    let db = schema_db();
    let query = query("SELECT payload FROM b");
    let scope = scope_with_independent_lifetimes(&query, &db);
    assert!(matches!(
        scope.resolve_column_definition(&reference("payload")).expect("definition resolves"),
        Some(ColumnDefinition::Base { .. })
    ));
}

#[test]
fn table_scope_returns_base_definitions() {
    let db = ParserDB::parse::<GenericDialect>(
        "CREATE TABLE docs(body TEXT); CREATE TABLE \"Docs\" (\"Body\" INT);",
    )
    .expect("schema parses");
    for (table_name, references) in
        [("docs", ["body", "docs.body"]), ("\"Docs\"", ["\"Body\"", "\"Docs\".\"Body\""])]
    {
        let table = db.table(None, table_name).expect("table exists");
        let scope = ColumnScope::for_table(table, &db);
        for reference_sql in references {
            let Some(ColumnDefinition::Base { table: resolved_table, column }) = scope
                .resolve_column_definition(&reference(reference_sql))
                .expect("definition resolves")
            else {
                panic!("expected a base column definition")
            };
            assert!(core::ptr::eq(resolved_table, table));
            assert_eq!(
                column.column_name(),
                reference_sql.rsplit('.').next().unwrap().trim_matches('"')
            );
        }
    }
}

#[test]
fn computed_definitions_cross_every_derived_relation_kind() {
    let db = ParserDB::parse::<GenericDialect>(
        "CREATE TABLE invoices(amount NUMERIC);
         CREATE VIEW doubled_view AS SELECT amount * 2 AS doubled FROM invoices;
         CREATE MATERIALIZED VIEW doubled_snapshot AS
             SELECT amount * 2 AS doubled FROM invoices;",
    )
    .expect("schema parses");
    for sql in [
        "WITH doubled_rows AS (\
            SELECT amount * 2 AS doubled FROM invoices\
         ) SELECT doubled FROM doubled_rows",
        "SELECT doubled FROM (SELECT amount * 2 AS doubled FROM invoices) doubled_rows",
        "SELECT doubled FROM doubled_view",
        "SELECT doubled FROM doubled_snapshot",
    ] {
        let query = query(sql);
        let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected a plain SELECT body")
        };
        let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
            panic!("expected an expression projection")
        };
        assert!(matches!(
            scope.resolve_column_definition(reference).expect("definition resolves"),
            Some(ColumnDefinition::Expression { expression, .. })
                if expression.to_string() == "amount * 2"
        ));
    }
}

#[test]
fn expression_definition_resolves_its_base_inputs() {
    let db = ParserDB::parse::<GenericDialect>("CREATE TABLE invoices(amount NUMERIC);")
        .expect("schema parses");
    let query = query("SELECT v.doubled FROM (SELECT amount * 2 AS doubled FROM invoices) v");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) =
        scope.resolve_column_definition(&reference("v.doubled")).expect("definition resolves")
    else {
        panic!("expected an expression definition")
    };
    assert_eq!(expression.to_string(), "amount * 2");
    let Some(ColumnDefinition::Base { table, column }) =
        defining_scope.resolve_column_definition(&reference("amount")).expect("input resolves")
    else {
        panic!("expected a base input")
    };
    assert_eq!(table.table_name(), "invoices");
    assert_eq!(column.column_name(), "amount");
}

#[test]
fn opaque_relations_are_distinct_from_missing_columns() {
    let db = schema_db();
    let opaque = query("SELECT g.value FROM generate_series(1, 10) g");
    let opaque_scope = ColumnScope::from_query(&opaque, &db).expect("scope builds");
    let SetExpr::Select(select) = opaque.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
        panic!("expected an expression projection")
    };
    assert!(matches!(
        opaque_scope.resolve_column_definition(reference).expect("definition resolves"),
        Some(ColumnDefinition::Opaque)
    ));

    let missing = query("SELECT no.value FROM b");
    let missing_scope = ColumnScope::from_query(&missing, &db).expect("scope builds");
    let SetExpr::Select(select) = missing.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    let SelectItem::UnnamedExpr(reference) = &select.projection[0] else {
        panic!("expected an expression projection")
    };
    assert!(
        missing_scope.resolve_column_definition(reference).expect("definition resolves").is_none()
    );
}

fn assert_opaque_resolution(from_clause: &str, reference_sql: &str, expected: bool) {
    let db = schema_db();
    let query = query(&format!("SELECT {reference_sql} FROM {from_clause}"));
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let definition =
        scope.resolve_column_definition(&reference(reference_sql)).expect("definition resolves");
    match definition {
        Some(ColumnDefinition::Opaque) => assert!(expected),
        None => assert!(!expected),
        Some(_) => panic!("expected an opaque or missing definition"),
    }
}

#[test]
fn opaque_relation_qualifiers_match_exactly() {
    for (reference_sql, expected) in [("value", true), ("g.value", true), ("other.value", false)] {
        assert_opaque_resolution("generate_series(1, 10) AS g", reference_sql, expected);
    }
}

#[test]
fn opaque_relation_qualifiers_preserve_quotes() {
    for (reference_sql, expected) in [("\"G\".value", true), ("G.value", false), ("g.value", false)]
    {
        assert_opaque_resolution("generate_series(1, 10) AS \"G\"", reference_sql, expected);
    }
}

#[test]
fn anonymous_and_any_qualifier_opaque_relations_stay_distinct() {
    assert_opaque_resolution("(SELECT count(*) FROM b)", "unknown.value", false);
    assert_opaque_resolution("(b JOIN b AS b2 ON true)", "unknown.value", true);
}

#[test]
fn three_part_opaque_qualifiers_require_matching_schema_and_relation() {
    for (from_clause, reference_sql, expected) in [
        ("generate_series(1, 10) AS g", "public.g.value", false),
        ("missing_schema.missing_table", "missing_schema.missing_table.value", true),
        ("missing_schema.missing_table", "other_schema.missing_table.value", false),
    ] {
        assert_opaque_resolution(from_clause, reference_sql, expected);
    }
}

fn expression_input_table(
    db: &ParserDB,
    sql: &str,
    output_reference: &str,
    input_reference: &str,
) -> Option<String> {
    let query = query(sql);
    let scope = ColumnScope::from_query(&query, db).expect("scope builds");
    let Some(ColumnDefinition::Expression { scope: defining_scope, .. }) = scope
        .resolve_column_definition(&reference(output_reference))
        .expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    match defining_scope
        .resolve_column_definition(&reference(input_reference))
        .expect("input definition resolves")
    {
        Some(ColumnDefinition::Base { table, .. }) => Some(table.table_name().to_owned()),
        None => None,
        Some(_) => panic!("expected a base or missing input definition"),
    }
}

#[test]
fn parent_lookup_obeys_local_shadowing_and_missing_columns() {
    let db = schema_db();
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS b, \
             LATERAL (SELECT b.payload || '' AS x FROM b AS b) AS d",
            "d.x",
            "b.payload",
        )
        .as_deref(),
        Some("b")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS b, \
             LATERAL (\
                 SELECT b.payload || '' AS x FROM (SELECT id FROM b) AS b\
             ) AS d",
            "d.x",
            "b.payload",
        ),
        None
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS outer_a, \
             LATERAL (SELECT outer_a.payload || '' AS x FROM b AS local_b) AS d",
            "d.x",
            "outer_a.payload",
        )
        .as_deref(),
        Some("a")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a, \
             LATERAL (\
                 SELECT payload || '' AS x FROM (SELECT id FROM b) AS local_b\
             ) AS d",
            "d.x",
            "payload",
        )
        .as_deref(),
        Some("a")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a, \
             LATERAL (\
                 SELECT public.a.payload || '' AS x \
                 FROM missing_schema.missing_table\
             ) AS d",
            "d.x",
            "public.a.payload",
        )
        .as_deref(),
        Some("a")
    );
}

#[test]
fn lateral_subqueries_see_only_prior_from_entries() {
    let db = schema_db();
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a, LATERAL (SELECT a.payload || '' AS x) AS d",
            "d.x",
            "a.payload",
        )
        .as_deref(),
        Some("a")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM LATERAL (SELECT a.payload || '' AS x) AS d, a",
            "d.x",
            "a.payload",
        ),
        None
    );
}

#[test]
fn non_lateral_subqueries_skip_siblings_and_reach_grandparents() {
    let db = schema_db();
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a, (SELECT a.payload || '' AS x) AS d",
            "d.x",
            "a.payload",
        ),
        None
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS outer_a, \
             LATERAL (\
                 SELECT inner_d.x \
                 FROM b AS local_b, \
                      (SELECT outer_a.payload || '' AS x) AS inner_d\
             ) AS d",
            "d.x",
            "outer_a.payload",
        )
        .as_deref(),
        Some("a")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS outer_a, \
             LATERAL (\
                 SELECT inner_d.x \
                 FROM b AS local_b, \
                      (SELECT local_b.payload || '' AS x) AS inner_d\
             ) AS d",
            "d.x",
            "local_b.payload",
        ),
        None
    );
}

#[test]
fn nested_ctes_inherit_their_query_parent() {
    let db = schema_db();
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a, \
             LATERAL (\
                 WITH c AS (SELECT a.payload || '' AS x) \
                 SELECT c.x FROM c\
             ) AS d",
            "d.x",
            "a.payload",
        )
        .as_deref(),
        Some("a")
    );
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT d.x FROM a AS outer_a, \
             LATERAL (\
                 WITH c AS (SELECT outer_a.payload || '' AS x) \
                 SELECT c.x FROM b AS outer_a, c\
             ) AS d",
            "d.x",
            "outer_a.payload",
        )
        .as_deref(),
        Some("a")
    );
}

#[test]
fn stored_views_do_not_capture_the_calling_scope() {
    let db = ParserDB::parse::<GenericDialect>(
        "CREATE TABLE a(payload TEXT);
         CREATE VIEW detached AS SELECT caller.payload || '' AS x;",
    )
    .expect("schema parses");
    assert_eq!(
        expression_input_table(
            &db,
            "SELECT detached.x FROM a AS caller, detached",
            "detached.x",
            "caller.payload",
        ),
        None
    );
}

fn select_body(query: &Query) -> &sqlparser::ast::Select {
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!("expected a plain SELECT body")
    };
    select
}

fn scalar_nested_select(expression: &Expr) -> &sqlparser::ast::Select {
    let Expr::Subquery(query) = expression else { panic!("expected a scalar subquery") };
    select_body(query)
}

fn exists_nested_select(expression: &Expr) -> &sqlparser::ast::Select {
    let Expr::Exists { subquery, .. } = expression else { panic!("expected an EXISTS expression") };
    select_body(subquery)
}

fn in_nested_select(expression: &Expr) -> &sqlparser::ast::Select {
    let Expr::InSubquery { subquery, .. } = expression else { panic!("expected an IN subquery") };
    select_body(subquery)
}

fn binary_nested_select(expression: &Expr) -> &sqlparser::ast::Select {
    let Expr::BinaryOp { left, .. } = expression else { panic!("expected a binary expression") };
    scalar_nested_select(left)
}

fn function_nested_select(expression: &Expr) -> &sqlparser::ast::Select {
    let Expr::Function(function) = expression else { panic!("expected a function") };
    let FunctionArguments::List(arguments) = &function.args else {
        panic!("expected function arguments")
    };
    let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Subquery(query)))) =
        arguments.args.first()
    else {
        panic!("expected a scalar subquery argument")
    };
    select_body(query)
}

fn indexed_nested_input_table(
    db: &ParserDB,
    sql: &str,
    output_reference: &str,
    input_reference: &str,
    nested_select: for<'expression> fn(&'expression Expr) -> &'expression sqlparser::ast::Select,
) -> Option<String> {
    let query = query(sql);
    let scope = ColumnScope::from_query(&query, db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) = scope
        .resolve_column_definition(&reference(output_reference))
        .expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    let nested_scope = defining_scope.scope_for_select(nested_select(expression))?;
    match nested_scope
        .resolve_column_definition(&reference(input_reference))
        .expect("nested input resolves")
    {
        Some(ColumnDefinition::Base { table, .. }) => Some(table.table_name().to_owned()),
        None => None,
        Some(_) => panic!("expected a base or missing nested input"),
    }
}

struct IndexedSelectOracle<'scope, 'query, 'database> {
    scope: ColumnDefinitionScope<'scope, 'query, 'database, ParserDB>,
    select_count: usize,
}

impl Visitor for IndexedSelectOracle<'_, '_, '_> {
    type Break = ();

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        let nested_scope = self.scope.scope_for_select(select).expect("nested scope is indexed");
        let Some(ColumnDefinition::Base { table, .. }) = nested_scope
            .resolve_column_definition(&reference("a.payload"))
            .expect("nested input resolves")
        else {
            panic!("expected a base nested input")
        };
        assert_eq!(table.table_name(), "a");
        self.select_count += 1;
        ControlFlow::Continue(())
    }
}

fn assert_expression_nested_selects_indexed(query: &Query, expected_select_count: usize) {
    let db = schema_db();
    let scope = ColumnScope::from_query(query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    let mut oracle = IndexedSelectOracle { scope: defining_scope, select_count: 0 };
    assert!(matches!(expression.visit(&mut oracle), ControlFlow::Continue(())));
    assert_eq!(oracle.select_count, expected_select_count);
}

#[derive(Default)]
struct NestedQueryInjector {
    query_count: usize,
}

impl VisitorMut for NestedQueryInjector {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Identifier(identifier) = expression else {
            return ControlFlow::Continue(());
        };
        if identifier.value != "needle" {
            return ControlFlow::Continue(());
        }
        *expression = Expr::Subquery(Box::new(query("SELECT a.payload FROM a")));
        self.query_count += 1;
        ControlFlow::Continue(())
    }
}

#[test]
fn expression_container_families_follow_the_sqlparser_visitor() {
    for expression in [
        "NOT (SELECT a.payload FROM a)",
        "CAST((SELECT a.payload FROM a) AS TEXT)",
        "(SELECT a.payload FROM a) IS NULL",
        "(SELECT a.payload FROM a) IS DISTINCT FROM ''",
        "(SELECT a.payload FROM a) AT TIME ZONE 'UTC'",
        "(SELECT a.payload FROM a) BETWEEN '' AND ''",
        "1 IN (0, (SELECT a.payload FROM a))",
        "POSITION((SELECT a.payload FROM a) IN 'text')",
        "SUBSTRING('text' FROM (SELECT a.id FROM a))",
        "CONVERT((SELECT a.payload FROM a), TEXT)",
        "ARRAY[0][(SELECT a.id FROM a)]",
        "ARRAY[0][(SELECT a.id FROM a):]",
        "payload:[(SELECT a.payload FROM a)]",
        "TRIM((SELECT a.payload FROM a))",
        "OVERLAY('text' PLACING (SELECT a.payload FROM a) FROM 1)",
        "CASE WHEN true THEN (SELECT a.payload FROM a) ELSE '' END",
        "ARRAY[(SELECT a.payload FROM a)]",
        "((SELECT a.payload FROM a), '')",
        "COALESCE((SELECT a.payload FROM a), '')",
        "COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM a))",
        "COUNT(*) OVER (PARTITION BY (SELECT a.payload FROM a))",
        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (SELECT a.payload FROM a))",
        "{'key': (SELECT a.payload FROM a)}",
        "MAP {'key': (SELECT a.payload FROM a)}",
        "ARRAY(SELECT a.payload FROM a)",
        "COUNT(1 ORDER BY (SELECT a.id FROM a))",
        "COUNT(*) OVER (ORDER BY (SELECT a.id FROM a))",
        "COUNT(*) OVER (ROWS BETWEEN (SELECT a.id FROM a) PRECEDING AND CURRENT ROW)",
    ] {
        let query = query(&format!("SELECT d.x FROM (SELECT {expression} AS x) AS d"));
        assert_expression_nested_selects_indexed(&query, 1);
    }
}

#[test]
fn expression_container_children_follow_the_sqlparser_visitor() {
    for expression in [
        "NOT needle",
        "CAST(needle AS TEXT)",
        "needle IS NULL",
        "needle IS DISTINCT FROM needle",
        "needle AT TIME ZONE needle",
        "needle BETWEEN needle AND needle",
        "needle IN (needle, needle)",
        "POSITION(needle IN needle)",
        "SUBSTRING(needle FROM needle FOR needle)",
        "CONVERT(needle, TEXT)",
        "ARRAY[needle][needle:needle:needle]",
        "needle:[needle]",
        "TRIM(needle FROM needle)",
        "OVERLAY(needle PLACING needle FROM needle FOR needle)",
        "CASE needle WHEN needle THEN needle ELSE needle END",
        "ARRAY[needle, needle]",
        "(needle, needle)",
        "STRUCT(needle AS value)",
        "COALESCE(needle, needle)",
        "COUNT(*) FILTER (WHERE needle)",
        "COUNT(*) OVER (PARTITION BY needle ORDER BY needle ROWS BETWEEN needle PRECEDING AND needle FOLLOWING)",
        "PERCENTILE_CONT(needle) WITHIN GROUP (ORDER BY needle)",
        "{'one': needle, 'two': needle}",
        "MAP {needle: needle}",
        "COUNT(needle ORDER BY needle)",
    ] {
        let mut query = query(&format!("SELECT d.x FROM (SELECT {expression} AS x) AS d"));
        let mut injector = NestedQueryInjector::default();
        assert!(matches!(VisitMut::visit(&mut query, &mut injector), ControlFlow::Continue(())));
        assert!(injector.query_count > 0);
        assert_expression_nested_selects_indexed(&query, injector.query_count);
    }
}

#[test]
fn grouping_expression_families_index_every_nested_select() {
    let mut query = query(
        "SELECT d.x FROM (\
             SELECT (\
                 SELECT a.payload FROM a \
                 GROUP BY GROUPING SETS ((a.payload, needle)), CUBE((needle)), ROLLUP((needle))\
             ) AS x\
         ) AS d",
    );
    let mut injector = NestedQueryInjector::default();
    assert!(matches!(VisitMut::visit(&mut query, &mut injector), ControlFlow::Continue(())));
    assert_eq!(injector.query_count, 3);
    assert_expression_nested_selects_indexed(&query, injector.query_count + 1);
}

#[test]
fn scalar_subquery_scopes_resolve_local_and_correlated_inputs() {
    let db = schema_db();
    for (sql, expected_table) in [
        (
            "SELECT d.x FROM \
             (SELECT (SELECT a.payload FROM a) AS x) AS d",
            "a",
        ),
        (
            "SELECT d.x FROM \
             (SELECT (SELECT a.payload) AS x FROM a) AS d",
            "a",
        ),
        (
            "SELECT d.x FROM \
             (SELECT (SELECT a.payload FROM b AS a) AS x FROM a) AS d",
            "b",
        ),
    ] {
        assert_eq!(
            indexed_nested_input_table(&db, sql, "d.x", "a.payload", scalar_nested_select,)
                .as_deref(),
            Some(expected_table)
        );
    }
}

#[test]
fn set_operation_arms_have_distinct_nested_scopes() {
    let db = schema_db();
    let query = query(
        "SELECT d.x FROM \
         (SELECT (SELECT payload FROM a UNION ALL SELECT payload FROM b) AS x) AS d",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    let Expr::Subquery(query) = expression else { panic!("expected a scalar subquery") };
    let SetExpr::SetOperation { left, right, .. } = query.body.as_ref() else {
        panic!("expected a set operation")
    };
    for (arm, expected_table) in [(left, "a"), (right, "b")] {
        let SetExpr::Select(select) = arm.as_ref() else { panic!("expected a SELECT arm") };
        let nested_scope =
            defining_scope.scope_for_select(select).expect("nested scope is indexed");
        let Some(ColumnDefinition::Base { table, .. }) = nested_scope
            .resolve_column_definition(&reference("payload"))
            .expect("arm input resolves")
        else {
            panic!("expected a base arm input")
        };
        assert_eq!(table.table_name(), expected_table);
    }
}

#[test]
fn query_expression_variants_share_the_nested_scope_index() {
    let db = schema_db();
    for (sql, input_reference, nested_select, expected_table) in [
        (
            "SELECT d.x FROM \
             (SELECT EXISTS (SELECT 1 FROM a) AS x) AS d",
            "a.payload",
            exists_nested_select as for<'expression> fn(&'expression Expr) -> &'expression _,
            "a",
        ),
        (
            "SELECT d.x FROM \
             (SELECT 1 IN (SELECT id FROM b) AS x) AS d",
            "b.payload",
            in_nested_select,
            "b",
        ),
        (
            "SELECT d.x FROM \
             (SELECT (SELECT id FROM a) = 1 AS x) AS d",
            "a.payload",
            binary_nested_select,
            "a",
        ),
        (
            "SELECT d.x FROM \
             (SELECT COALESCE((SELECT a.payload FROM a), '') AS x) AS d",
            "a.payload",
            function_nested_select,
            "a",
        ),
    ] {
        assert_eq!(
            indexed_nested_input_table(&db, sql, "d.x", input_reference, nested_select,).as_deref(),
            Some(expected_table)
        );
    }
}

#[test]
fn stored_view_nested_scope_uses_database_ast_references() {
    let db = ParserDB::parse::<GenericDialect>(
        "CREATE TABLE a(payload TEXT);
         CREATE VIEW nested_view AS SELECT (SELECT a.payload FROM a) AS x;",
    )
    .expect("schema parses");
    assert_eq!(
        indexed_nested_input_table(
            &db,
            "SELECT nested_view.x FROM nested_view",
            "nested_view.x",
            "a.payload",
            scalar_nested_select,
        )
        .as_deref(),
        Some("a")
    );
}

#[test]
fn nested_scope_index_recurses_through_nested_queries() {
    let db = schema_db();
    let query = query(
        "SELECT d.x FROM \
         (SELECT (SELECT (SELECT a.payload FROM a)) AS x) AS d",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    let first_select = scalar_nested_select(expression);
    let first_scope =
        defining_scope.scope_for_select(first_select).expect("first nested scope is indexed");
    let SelectItem::UnnamedExpr(second_expression) = &first_select.projection[0] else {
        panic!("expected a nested expression")
    };
    let second_select = scalar_nested_select(second_expression);
    let second_scope =
        first_scope.scope_for_select(second_select).expect("second nested scope is indexed");
    let Some(ColumnDefinition::Base { table, .. }) = second_scope
        .resolve_column_definition(&reference("a.payload"))
        .expect("nested input resolves")
    else {
        panic!("expected a base nested input")
    };
    assert_eq!(table.table_name(), "a");
}

#[test]
fn nested_scope_index_includes_selection_expressions() {
    let db = schema_db();
    let query = query(
        "SELECT d.x FROM (\
             SELECT a.payload || '' AS x FROM a \
             WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)\
         ) AS d",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { scope: defining_scope, .. }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    let SetExpr::Select(defining_select) = ({
        let SetExpr::Select(root) = query.body.as_ref() else { panic!("expected a root SELECT") };
        let sqlparser::ast::TableFactor::Derived { subquery, .. } = &root.from[0].relation else {
            panic!("expected a derived relation")
        };
        subquery.body.as_ref()
    }) else {
        panic!("expected a defining SELECT")
    };
    let Some(Expr::Exists { subquery, .. }) = &defining_select.selection else {
        panic!("expected an EXISTS predicate")
    };
    let nested_select = select_body(subquery);
    let nested_scope =
        defining_scope.scope_for_select(nested_select).expect("predicate scope is indexed");
    for (input_reference, expected_table) in [("b.payload", "b"), ("a.payload", "a")] {
        let Some(ColumnDefinition::Base { table, .. }) = nested_scope
            .resolve_column_definition(&reference(input_reference))
            .expect("predicate input resolves")
        else {
            panic!("expected a base predicate input")
        };
        assert_eq!(table.table_name(), expected_table);
    }
}

#[test]
fn alias_lists_and_quoted_names_preserve_expression_scopes() {
    let db = schema_db();
    for sql in [
        "SELECT d.\"X\" FROM \
         (SELECT a.payload || '' FROM a) AS d(\"X\")",
        "WITH d(\"X\") AS (SELECT a.payload || '' FROM a) \
         SELECT d.\"X\" FROM d",
    ] {
        assert_eq!(expression_input_table(&db, sql, "d.\"X\"", "a.payload").as_deref(), Some("a"));
    }

    let quoted_db = ParserDB::parse::<GenericDialect>("CREATE TABLE \"A\"(\"Payload\" TEXT);")
        .expect("quoted schema parses");
    assert_eq!(
        expression_input_table(
            &quoted_db,
            "SELECT d.\"X\" FROM \
             (SELECT \"A\".\"Payload\" || '' FROM \"A\") AS d(\"X\")",
            "d.\"X\"",
            "\"A\".\"Payload\"",
        )
        .as_deref(),
        Some("A")
    );
}

#[test]
fn expression_scopes_report_local_ambiguity() {
    let db = schema_db();
    let ambiguous_bare = query("SELECT d.x FROM (SELECT payload || '' AS x FROM a, b) AS d");
    let scope = ColumnScope::from_query(&ambiguous_bare, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { scope: defining_scope, .. }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    assert!(matches!(
        defining_scope.resolve_column_definition(&reference("payload")),
        Err(LookupError::AmbiguousTableLookup { .. })
    ));

    let ambiguous_qualified = query(
        "SELECT d.payload FROM \
         (SELECT a.payload, b.payload FROM a, b) AS d",
    );
    let scope = ColumnScope::from_query(&ambiguous_qualified, &db).expect("scope builds");
    assert!(matches!(
        scope.resolve_column_definition(&reference("d.payload")),
        Err(LookupError::AmbiguousTableLookup { .. })
    ));
}

#[test]
fn expression_without_relations_has_an_empty_scope() {
    let db = schema_db();
    let query = query("SELECT d.x FROM (SELECT 1 + 2 AS x) AS d");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::Expression { expression, scope: defining_scope }) =
        scope.resolve_column_definition(&reference("d.x")).expect("output definition resolves")
    else {
        panic!("expected an expression definition")
    };
    assert_eq!(expression.to_string(), "1 + 2");
    assert!(
        defining_scope
            .resolve_column_definition(&reference("missing"))
            .expect("missing input resolves")
            .is_none()
    );
}

#[test]
fn wildcard_definitions_retain_exact_base_columns() {
    let db = schema_db();
    for (sql, expected_table) in [
        ("SELECT d.payload FROM (SELECT * FROM a) AS d", "a"),
        ("SELECT d.payload FROM (SELECT a.* FROM a, b) AS d", "a"),
    ] {
        let query = query(sql);
        let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
        let Some(ColumnDefinition::Base { table, column }) =
            scope.resolve_column_definition(&reference("d.payload")).expect("definition resolves")
        else {
            panic!("expected a base wildcard definition")
        };
        assert_eq!(table.table_name(), expected_table);
        assert_eq!(column.column_name(), "payload");
    }
}

#[test]
fn merged_wildcard_columns_are_opaque() {
    let db = schema_db();
    let query = query("SELECT d.id FROM (SELECT * FROM a JOIN b USING (id)) AS d");
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    assert!(matches!(
        scope.resolve_column_definition(&reference("d.id")).expect("definition resolves"),
        Some(ColumnDefinition::Opaque)
    ));
}

#[test]
fn three_part_references_return_exact_base_definitions() {
    let db = schema_db();
    let unquoted = query("SELECT public.a.payload FROM a");
    let scope = ColumnScope::from_query(&unquoted, &db).expect("scope builds");
    let Some(ColumnDefinition::Base { table, column }) = scope
        .resolve_column_definition(&reference("public.a.payload"))
        .expect("definition resolves")
    else {
        panic!("expected a three-part base definition")
    };
    assert_eq!(table.table_name(), "a");
    assert_eq!(column.column_name(), "payload");

    let quoted_db = ParserDB::parse::<GenericDialect>(
        "CREATE SCHEMA \"S\"; CREATE TABLE \"S\".\"T\"(\"C\" INT);",
    )
    .expect("quoted schema parses");
    let quoted = query("SELECT \"S\".\"T\".\"C\" FROM \"S\".\"T\"");
    let scope = ColumnScope::from_query(&quoted, &quoted_db).expect("scope builds");
    let Some(ColumnDefinition::Base { table, column }) = scope
        .resolve_column_definition(&reference("\"S\".\"T\".\"C\""))
        .expect("definition resolves")
    else {
        panic!("expected a quoted three-part base definition")
    };
    assert_eq!(table.table_name(), "T");
    assert_eq!(column.column_name(), "C");
}

#[test]
fn set_operations_preserve_mixed_base_and_expression_children() {
    let db = schema_db();
    let query = query(
        "SELECT d.payload FROM (\
             SELECT payload FROM a \
             UNION ALL \
             SELECT payload || '' FROM a\
         ) AS d",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::SetOperation { left, right, .. }) =
        scope.resolve_column_definition(&reference("d.payload")).expect("definition resolves")
    else {
        panic!("expected a set operation definition")
    };
    assert_copy(left);
    assert_copy(right);
    assert!(matches!(left.definition(), ColumnDefinition::Base { .. }));
    assert!(matches!(
        right.definition(),
        ColumnDefinition::Expression { expression, .. }
            if expression.to_string() == "payload || ''"
    ));
}

#[test]
fn alias_lists_relabel_set_operation_definitions() {
    let db = schema_db();
    let query = query(
        "SELECT d.x FROM (\
             SELECT payload FROM a \
             UNION ALL \
             SELECT payload FROM b\
         ) AS d(x)",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    let Some(ColumnDefinition::SetOperation { left, right, .. }) =
        scope.resolve_column_definition(&reference("d.x")).expect("definition resolves")
    else {
        panic!("expected a relabeled set definition")
    };
    let ColumnDefinition::Base { table: left_table, .. } = left.definition() else {
        panic!("expected a left base definition")
    };
    let ColumnDefinition::Base { table: right_table, .. } = right.definition() else {
        panic!("expected a right base definition")
    };
    assert_eq!(left_table.table_name(), "a");
    assert_eq!(right_table.table_name(), "b");
}

#[test]
fn mutually_recursive_ctes_remain_opaque() {
    let db = schema_db();
    let query = query(
        "WITH RECURSIVE \
             x(v) AS (SELECT y.v FROM y), \
             y(v) AS (SELECT x.v FROM x) \
         SELECT x.v FROM x",
    );
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    assert!(matches!(
        scope.resolve_column_definition(&reference("x.v")).expect("definition resolves"),
        Some(ColumnDefinition::Opaque)
    ));
}

#[test]
fn wide_wildcard_chains_become_opaque_at_the_width_limit() {
    let db =
        ParserDB::parse::<GenericDialect>("CREATE TABLE seed(value INT);").expect("schema parses");
    let mut sql = String::from("WITH c0 AS (SELECT * FROM seed)");
    for level in 1..=13 {
        let previous = level - 1;
        write!(
            &mut sql,
            ", c{level} AS (SELECT * FROM c{previous} AS left_side, \
             c{previous} AS right_side)"
        )
        .expect("string writes");
    }
    sql.push_str(" SELECT c13.value FROM c13");
    let query = query(&sql);
    let scope = ColumnScope::from_query(&query, &db).expect("scope builds");
    assert!(matches!(
        scope.resolve_column_definition(&reference("c13.value")).expect("definition resolves"),
        Some(ColumnDefinition::Opaque)
    ));
}
