//! Tests that every `ALTER TABLE` operation accounts for itself.
//!
//! The statement match used to end in a catch-all arm that silently discarded
//! thirty-seven operations along with everything not yet implemented, so an
//! operation that changed the schema and one that changed nothing the model
//! describes were indistinguishable. The arm is gone: an operation is applied,
//! or deliberately ignored because the model carries no representation of what
//! it changes, or reported as not yet supported.
//!
//! That the accounting stays complete is enforced by the compiler rather than
//! by a test here, since the operation list is not open for extension and the
//! match has no wildcard, so a new operation in the parser library fails the
//! build until it is placed.
#![allow(clippy::expect_used)]

use sql_traits::{errors::Error, prelude::*};
use sqlparser::dialect::{GenericDialect, MySqlDialect, PostgreSqlDialect};

const TABLE: &str = "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT);";

fn parse(tail: &str) -> Result<ParserDB, Error> {
    ParserDB::parse::<PostgreSqlDialect>(&format!("{TABLE} {tail};"))
}

/// Each of these changes part of the schema the model represents, so silently
/// discarding it would leave the model wrong rather than merely coarse.
#[test]
fn an_operation_that_would_leave_the_model_wrong_is_reported() {
    let postgres = [
        "ALTER TABLE t DROP CONSTRAINT IF EXISTS nothing, RENAME CONSTRAINT c TO d",
        "ALTER TABLE t SWAP WITH other",
    ];
    for tail in postgres {
        let error = parse(tail).expect_err("the operation is not applied");
        assert!(
            matches!(&error, Error::UnsupportedAlterTableOperation { table_name, .. }
                if table_name == "t"),
            "{tail} reported {error:?}"
        );
    }

    let mysql = [
        "ALTER TABLE t DROP PRIMARY KEY",
        "ALTER TABLE t DROP FOREIGN KEY fk",
        "ALTER TABLE t DROP INDEX i",
    ];
    for tail in mysql {
        let error = ParserDB::parse::<MySqlDialect>(&format!("{TABLE} {tail};"))
            .expect_err("the operation is not applied");
        assert!(
            matches!(&error, Error::UnsupportedAlterTableOperation { table_name, .. }
                if table_name == "t"),
            "{tail} reported {error:?}"
        );
    }
}

/// The report names the operation, so a caller reading it knows which clause of
/// a multi-operation statement stopped the parse.
#[test]
fn the_report_names_the_operation() {
    let error =
        ParserDB::parse::<MySqlDialect>(&format!("{TABLE} ALTER TABLE t DROP PRIMARY KEY;"))
            .expect_err("dropping the primary key is not applied");
    assert!(
        matches!(&error, Error::UnsupportedAlterTableOperation { operation, .. }
            if operation.contains("PRIMARY KEY")),
        "the report names the operation: {error:?}"
    );
}

/// Ownership is the clearest case of something the model carries no
/// representation of, and a Postgres dump emits it for every table, so refusing
/// it would turn away ordinary input.
#[test]
fn an_ownership_change_parses_and_changes_nothing() {
    let database = parse("ALTER TABLE t OWNER TO someone").expect("ownership is not modelled");

    assert_eq!(database.tables().count(), 1);
    let table = database.table(None, "t").expect("t survives");
    assert_eq!(table.columns(&database).expect("t is in this database").count(), 3);
}

/// A representative operation from each ignored family parses and leaves the
/// model exactly as the bare table left it.
#[test]
fn operations_over_things_the_model_does_not_describe_change_nothing() {
    let bare = ParserDB::parse::<PostgreSqlDialect>(TABLE).expect("the table alone parses");
    let shape = |database: &ParserDB| {
        let table = database.table(None, "t").expect("t survives");
        (
            database.tables().count(),
            table.columns(database).expect("t is in this database").count(),
            table.unique_indices(database).expect("t is in this database").count(),
            table.check_constraints(database).expect("t is in this database").count(),
            database.indexes().count(),
            table.has_row_level_security(database).expect("t is in this database"),
        )
    };

    let ignored = [
        // Physical layout and durability.
        "ALTER TABLE t SET (fillfactor = 70)",
        "ALTER TABLE t SET LOGGED",
        "ALTER TABLE t SET UNLOGGED",
        // Rewrite rules, whose enablement has nothing to attach to.
        "ALTER TABLE t DISABLE RULE r",
        "ALTER TABLE t ENABLE ALWAYS RULE r",
        // A trigger is modelled, but whether it is armed is not.
        "ALTER TABLE t DISABLE TRIGGER g",
        "ALTER TABLE t ENABLE REPLICA TRIGGER g",
        // Replication identity and constraint validity.
        "ALTER TABLE t REPLICA IDENTITY FULL",
        "ALTER TABLE t VALIDATE CONSTRAINT c",
    ];

    for tail in ignored {
        let parsed = parse(tail);
        assert!(parsed.is_ok(), "{tail} reported {:?}", parsed.as_ref().err());
        let database = parsed.expect("the parse succeeded just above");
        assert_eq!(shape(&database), shape(&bare), "{tail} changed the model");
    }
}

/// Partition and vendor operations arrive under the dialects that spell them,
/// and are ignored just the same.
#[test]
fn vendor_operations_parse_under_their_dialects() {
    let mysql = [
        "ALTER TABLE t ALGORITHM = INPLACE",
        "ALTER TABLE t LOCK = NONE",
        "ALTER TABLE t AUTO_INCREMENT = 100",
    ];
    for tail in mysql {
        let parsed = ParserDB::parse::<MySqlDialect>(&format!("{TABLE} {tail};"));
        assert!(parsed.is_ok(), "{tail} reported {:?}", parsed.err());
    }

    let generic = ["ALTER TABLE t DROP PARTITION (p1)", "ALTER TABLE t ADD PARTITION (p1)"];
    for tail in generic {
        let parsed = ParserDB::parse::<GenericDialect>(&format!("{TABLE} {tail};"));
        assert!(parsed.is_ok(), "{tail} reported {:?}", parsed.err());
    }
}

/// An ignored operation sitting beside an applied one must not swallow it, and
/// a reported one must stop the statement wherever it sits.
#[test]
fn a_multi_operation_statement_treats_each_operation_on_its_own() {
    let database = parse("ALTER TABLE t OWNER TO someone, ADD COLUMN x INT")
        .expect("ownership is ignored, the column is added");
    let table = database.table(None, "t").expect("t survives");
    assert_eq!(table.columns(&database).expect("t is in this database").count(), 4);

    let error = ParserDB::parse::<MySqlDialect>(&format!(
        "{TABLE} ALTER TABLE t ADD COLUMN x INT, DROP PRIMARY KEY;"
    ))
    .expect_err("the second operation is not applied");
    assert!(matches!(&error, Error::UnsupportedAlterTableOperation { .. }), "got {error:?}");
}
