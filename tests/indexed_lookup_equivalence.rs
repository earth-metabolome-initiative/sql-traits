//! Differential test: the indexed table readers must answer exactly what the
//! original linear scan answered.
//!
//! The oracles here reproduce the pre-index scan arms verbatim using only the
//! public API, and are compared against `DatabaseLike::table`,
//! `DatabaseLike::resolve_target_table`, and the `ParserDB` object-name
//! resolvers over a fixture of adversarial spellings (quote states, case,
//! bare versus explicit `public`, multiple schemas, multi-entry search paths).
#![allow(clippy::expect_used)]

use sql_traits::{
    errors::LookupError,
    prelude::*,
    structs::TargetName,
    traits::TableLike,
    utils::identifier_resolution::{identifiers_match, stored_identifier_matches_lookup},
};
use sqlparser::{
    ast::{Ident, ObjectName, ObjectNamePart},
    dialect::PostgreSqlDialect,
};

type Table = sqlparser::ast::CreateTable;

fn parse(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(sql).expect("schema builds")
}

const FIXTURE: &str = "
    CREATE SCHEMA app;
    CREATE SCHEMA app_2;
    CREATE TABLE bare_a (id INT);
    CREATE TABLE public.explicit_b (id INT);
    CREATE TABLE \"Mixed\" (id INT);
    CREATE TABLE lower_quoted_name (id INT);
    CREATE TABLE app.plain (id INT);
    CREATE TABLE app.\"Keyed\" (id INT);
    CREATE TABLE app_2.other (id INT);
    CREATE TABLE \"Odd Space\" (id INT);
";

fn fixture_variants() -> Vec<(ParserDB, String)> {
    [
        "",
        "SET search_path TO app;",
        "SET search_path TO app, public;",
        "SET search_path TO \"app\", public;",
        "SET search_path TO public, app;",
        "SET search_path TO app_2, app;",
    ]
    .into_iter()
    .map(|path_sql| {
        let sql = format!("{FIXTURE}{path_sql}");
        (parse(&sql), path_sql.to_string())
    })
    .collect()
}

fn lookup_matrix() -> Vec<(Option<&'static str>, &'static str)> {
    let mut matrix = Vec::new();
    for name in [
        "bare_a",
        "BARE_A",
        "\"bare_a\"",
        "explicit_b",
        "EXPLICIT_B",
        "\"explicit_b\"",
        "Mixed",
        "mixed",
        "\"Mixed\"",
        "lower_quoted_name",
        "plain",
        "PLAIN",
        "\"plain\"",
        "Keyed",
        "\"Keyed\"",
        "other",
        "\"Odd Space\"",
        "\"odd space\"",
        "ghost",
    ] {
        for schema in [
            None,
            Some("public"),
            Some("PUBLIC"),
            Some("\"public\""),
            Some("app"),
            Some("APP"),
            Some("\"app\""),
            Some("app_2"),
        ] {
            matrix.push((schema, name));
        }
    }
    matrix
}

fn target_matrix() -> Vec<TargetName<'static>> {
    let mut matrix = Vec::new();
    for (name, quoted) in [
        ("bare_a", false),
        ("BARE_A", false),
        ("bare_a", true),
        ("explicit_b", false),
        ("Mixed", false),
        ("Mixed", true),
        ("mixed", false),
        ("plain", false),
        ("Keyed", true),
        ("Keyed", false),
        ("other", false),
        ("Odd Space", true),
        ("ghost", false),
    ] {
        matrix.push(TargetName::new(name, quoted));
        for (schema, schema_quoted) in [
            ("public", false),
            ("PUBLIC", false),
            ("public", true),
            ("app", false),
            ("APP", false),
            ("app", true),
            ("app_2", false),
        ] {
            matrix.push(TargetName::new(name, quoted).with_schema(schema, schema_quoted));
        }
    }
    matrix
}

fn table_name_matches(table: &Table, name: &str, name_quoted: bool) -> bool {
    identifiers_match(table.table_name(), table.table_name_is_quoted(), name, name_quoted)
}

fn schema_pair_matches(
    table: &Table,
    target_schema: Option<&str>,
    target_schema_quoted: bool,
) -> bool {
    match (target_schema, table.table_schema()) {
        (None, None) => true,
        (Some(target_schema), Some(table_schema)) => {
            identifiers_match(
                table_schema,
                table.table_schema_is_quoted(),
                target_schema,
                target_schema_quoted,
            )
        }
        (Some(target_schema), None) => {
            identifiers_match("public", false, target_schema, target_schema_quoted)
        }
        (None, Some(table_schema)) => {
            identifiers_match(table_schema, table.table_schema_is_quoted(), "public", false)
        }
    }
}

fn render(table: &Table) -> String {
    let name = TargetName::new(table.table_name(), table.table_name_is_quoted());
    match table.table_schema() {
        Some(schema) => name.with_schema(schema, table.table_schema_is_quoted()),
        None => name,
    }
    .to_string()
}

fn resolve_candidates<'a>(
    target: &TargetName<'_>,
    candidates: &[&'a Table],
) -> Result<Option<&'a Table>, LookupError> {
    match candidates {
        [] => Ok(None),
        [table] => Ok(Some(*table)),
        _ => {
            let mut rendered: Vec<String> = candidates.iter().copied().map(render).collect();
            rendered.sort();
            rendered.dedup();
            Err(LookupError::AmbiguousTableLookup {
                object_name: target.to_string(),
                candidates: rendered,
            })
        }
    }
}

fn outcome(result: &Result<Option<&Table>, LookupError>) -> String {
    match result {
        Ok(table) => table.map_or(String::new(), render),
        Err(error) => format!("ERR {error:?}"),
    }
}

/// Pre-index scan of `DatabaseLike::table`.
fn oracle_table<'a>(db: &'a ParserDB, schema: Option<&str>, name: &str) -> Option<&'a Table> {
    let lookup = sql_traits::utils::identifier_resolution::parse_lookup_identifier(name);
    db.tables().find(|table| {
        identifiers_match(
            table.table_name(),
            table.table_name_is_quoted(),
            lookup.value(),
            lookup.is_quoted(),
        ) && match (schema, table.table_schema()) {
            (None, None) => true,
            (Some(lookup_schema), Some(table_schema)) => {
                stored_identifier_matches_lookup(
                    table_schema,
                    table.table_schema_is_quoted(),
                    lookup_schema,
                )
            }
            (Some(lookup_schema), None) => {
                stored_identifier_matches_lookup("public", false, lookup_schema)
            }
            (None, Some(table_schema)) => {
                identifiers_match(table_schema, table.table_schema_is_quoted(), "public", false)
            }
        }
    })
}

/// Pre-index default of `DatabaseLike::resolve_target_table`.
fn oracle_resolve<'a>(
    db: &'a ParserDB,
    target: &TargetName<'_>,
) -> Result<Option<&'a Table>, LookupError> {
    let name = target.name();
    let name_quoted = target.name_is_quoted();
    let matching = |schema: Option<&str>, schema_quoted: bool| -> Vec<&Table> {
        db.tables()
            .filter(|table| {
                table_name_matches(table, name, name_quoted)
                    && schema_pair_matches(table, schema, schema_quoted)
            })
            .collect()
    };

    if target.schema().is_some() {
        return resolve_candidates(target, &matching(target.schema(), target.schema_is_quoted()));
    }

    for (entry_schema, entry_quoted) in db.search_path().collect::<Vec<_>>() {
        let candidates = matching(Some(entry_schema), entry_quoted);
        if !candidates.is_empty() {
            return resolve_candidates(target, &candidates);
        }
    }
    Ok(None)
}

fn object_name(parts: &[(&str, bool)]) -> ObjectName {
    ObjectName(
        parts
            .iter()
            .map(|(value, quoted)| {
                ObjectNamePart::Identifier(if *quoted {
                    Ident::with_quote('"', *value)
                } else {
                    Ident::new(*value)
                })
            })
            .collect(),
    )
}

fn read_part(part: &ObjectNamePart) -> (&str, bool) {
    match part {
        ObjectNamePart::Identifier(ident) => (ident.value.as_str(), ident.quote_style.is_some()),
        ObjectNamePart::Function(function_part) => {
            (function_part.name.value.as_str(), function_part.name.quote_style.is_some())
        }
    }
}

fn target_of_object_name(object_name: &ObjectName) -> TargetName<'_> {
    let parts = &object_name.0;
    let (schema_part, name_part) = match parts.len() {
        1 => (None, &parts[0]),
        _ => (Some(&parts[parts.len() - 2]), &parts[parts.len() - 1]),
    };
    let (name, quoted) = read_part(name_part);
    let target = TargetName::new(name, quoted);
    match schema_part {
        Some(part) => {
            let (schema, schema_quoted) = read_part(part);
            target.with_schema(schema, schema_quoted)
        }
        None => target,
    }
}

#[test]
fn table_lookup_matches_scan() {
    for (db, path_sql) in fixture_variants() {
        for (schema, name) in lookup_matrix() {
            let indexed = db.table(schema, name);
            let scanned = oracle_table(&db, schema, name);
            assert_eq!(
                indexed.map(render),
                scanned.map(render),
                "table({schema:?}, {name:?}) on {path_sql:?}"
            );
        }
    }
}

#[test]
fn resolve_target_table_matches_scan() {
    for (db, path_sql) in fixture_variants() {
        for target in target_matrix() {
            let shown = target.to_string();
            let indexed = db.resolve_target_table(target.clone());
            let scanned = oracle_resolve(&db, &target);
            assert_eq!(
                outcome(&indexed),
                outcome(&scanned),
                "resolve_target_table({shown}) on {path_sql:?}"
            );
        }
    }
}

#[test]
fn object_name_resolvers_match_scan() {
    let names: Vec<ObjectName> = vec![
        object_name(&[("bare_a", false)]),
        object_name(&[("BARE_A", false)]),
        object_name(&[("bare_a", true)]),
        object_name(&[("public", false), ("explicit_b", false)]),
        object_name(&[("PUBLIC", false), ("explicit_b", false)]),
        object_name(&[("app", false), ("plain", false)]),
        object_name(&[("APP", false), ("plain", false)]),
        object_name(&[("app", false), ("Keyed", false)]),
        object_name(&[("app", false), ("Keyed", true)]),
        object_name(&[("Mixed", true)]),
        object_name(&[("mixed", false)]),
        object_name(&[("Odd Space", true)]),
        object_name(&[("app_2", false), ("other", false)]),
        object_name(&[("ghost", false)]),
    ];
    for (db, path_sql) in fixture_variants() {
        for name in &names {
            let target = target_of_object_name(name);
            assert_eq!(
                outcome(&db.resolve_table_object_name(name)),
                outcome(&{
                    let candidates: Vec<&Table> = db
                        .tables()
                        .filter(|table| {
                            table_name_matches(table, target.name(), target.name_is_quoted())
                                && schema_pair_matches(
                                    table,
                                    target.schema(),
                                    target.schema_is_quoted(),
                                )
                        })
                        .collect();
                    resolve_candidates(&target, &candidates)
                }),
                "resolve_table_object_name({name}) on {path_sql:?}"
            );
            assert_eq!(
                outcome(&db.resolve_table_object_name_on_search_path(name)),
                outcome(&oracle_resolve(&db, &target)),
                "resolve_table_object_name_on_search_path({name}) on {path_sql:?}"
            );
        }
    }
}
