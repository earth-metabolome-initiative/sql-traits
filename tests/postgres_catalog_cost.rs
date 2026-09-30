//! What the built-in PostgreSQL catalog costs a parse.
//!
//! Every constructor that takes no explicit catalog starts from
//! `PostgresCatalog::postgres_18()`, whatever the dialect, so building it sits
//! on the path of every parse. These tests count allocations, since the cost of
//! that construction is the property they pin.
#![cfg(not(tarpaulin))]
#![allow(clippy::expect_used, clippy::panic)]

use sql_traits::{errors::Error, prelude::*, traits::ColumnCollation};
use sqlparser::dialect::PostgreSqlDialect;

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/counting_allocator.rs"));

/// Runs `body` and answers how many times it allocated.
fn allocations_of<T>(body: impl FnOnce() -> T) -> usize {
    let before = allocations();
    let answer = body();
    let counted = allocations() - before;
    drop(answer);
    counted
}

#[test]
fn building_the_postgres_18_catalog_allocates_nothing() {
    let counted = allocations_of(PostgresCatalog::postgres_18);
    assert_eq!(counted, 0, "building the PostgreSQL 18 catalog allocated {counted} times");
}

#[test]
fn default_parse_options_allocate_nothing() {
    let counted = allocations_of(ParseOptions::default);
    assert_eq!(counted, 0, "building the default parse options allocated {counted} times");
}

#[test]
fn a_supplied_collation_replaces_the_built_in_of_the_same_identity() -> Result<(), Error> {
    let catalog = PostgresCatalog::postgres_18()
        .with_collation(PostgresCatalogCollation::new("C", true).with_deterministic(false));

    assert_eq!(catalog.collations().count(), PostgresCatalog::postgres_18().collations().count());
    let is_c = |collation: &&PostgresCatalogCollation| {
        collation.schema() == Some("pg_catalog") && collation.name() == "C"
    };
    assert_eq!(catalog.collations().filter(is_c).count(), 1);
    let last = catalog.collations().next_back().expect("the catalog holds collations");
    assert!(is_c(&last) && !last.deterministic(), "the replacement is the newest fact");

    let db = ParseOptions::default()
        .with_postgres_catalog(catalog)
        .parse::<PostgreSqlDialect>("CREATE TABLE t (name TEXT COLLATE \"C\");")?;
    let table = db
        .table_by_target(TargetName::new("t", false), IdentifierCase::AsWritten)?
        .expect("table t was created");
    let column =
        table.column("name", &db, IdentifierCase::AsWritten)?.expect("column name was declared");
    let ColumnCollation::Named(collation) = column.collation(&db)? else {
        panic!("a declared COLLATE resolves to a named collation");
    };
    assert_eq!(collation.postgres_deterministic(), Some(false));

    Ok(())
}
