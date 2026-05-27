//! Implementations of [`DMLLike`] for sqlparser's `Insert`, `Update`, and
//! `Delete` nodes.
//!
//! `target_table` resolves the single base table each statement mutates and
//! reports [`LookupError::InvalidObjectName`] when there is no such target (an
//! `INSERT` into a subquery or table function, an `UPDATE` of a joined or
//! derived relation, a multi-table `DELETE`, or a name absent from the schema).

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use sqlparser::ast::{Delete, FromTable, Insert, ObjectName, TableFactor, TableObject, Update};

use crate::{
    errors::LookupError,
    traits::{DMLLike, DatabaseLike, DmlKind},
    utils::object_name::resolve_object_name,
};

/// Resolves an object name that is required to denote an existing base table.
fn resolve_required_table<'db, DB: DatabaseLike>(
    name: &ObjectName,
    database: &'db DB,
) -> Result<&'db DB::Table, LookupError> {
    match resolve_object_name(name, database)? {
        Some(table) => Ok(table),
        None => {
            Err(LookupError::InvalidObjectName {
                object_name: name.to_string(),
                reason: "no matching table in the database".to_string(),
            })
        }
    }
}

/// Resolves a table factor that is required to be a plain base table (no table
/// function arguments), reporting `reason` otherwise.
fn resolve_table_factor<'db, DB: DatabaseLike>(
    factor: &TableFactor,
    database: &'db DB,
    reason: &str,
) -> Result<&'db DB::Table, LookupError> {
    match factor {
        TableFactor::Table { name, args: None, .. } => resolve_required_table(name, database),
        other => {
            Err(LookupError::InvalidObjectName {
                object_name: other.to_string(),
                reason: reason.to_string(),
            })
        }
    }
}

impl<DB: DatabaseLike> DMLLike<DB> for Insert {
    fn target_table<'db>(&self, database: &'db DB) -> Result<&'db DB::Table, LookupError> {
        match &self.table {
            TableObject::TableName(name) => resolve_required_table(name, database),
            TableObject::TableFunction(_) | TableObject::TableQuery(_) => {
                Err(LookupError::InvalidObjectName {
                    object_name: self.table.to_string(),
                    reason: "INSERT target is not a base table".to_string(),
                })
            }
        }
    }

    fn kind(&self) -> DmlKind {
        DmlKind::Insert
    }
}

impl<DB: DatabaseLike> DMLLike<DB> for Update {
    fn target_table<'db>(&self, database: &'db DB) -> Result<&'db DB::Table, LookupError> {
        // The mutation target is the relation of `UPDATE <relation>`; any
        // `FROM` providing values is a separate source, not the target.
        resolve_table_factor(&self.table.relation, database, "UPDATE target is not a base table")
    }

    fn kind(&self) -> DmlKind {
        DmlKind::Update
    }
}

impl<DB: DatabaseLike> DMLLike<DB> for Delete {
    fn target_table<'db>(&self, database: &'db DB) -> Result<&'db DB::Table, LookupError> {
        // MySQL multi-table delete (`DELETE t1, t2 FROM ...`) has no single
        // target.
        if !self.tables.is_empty() {
            let rendered: Vec<String> = self.tables.iter().map(ToString::to_string).collect();
            return Err(LookupError::InvalidObjectName {
                object_name: rendered.join(", "),
                reason: "multi-table DELETE has no single target table".to_string(),
            });
        }

        let from = match &self.from {
            FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
        };

        match from.as_slice() {
            [single] if single.joins.is_empty() => {
                resolve_table_factor(
                    &single.relation,
                    database,
                    "DELETE target is not a base table",
                )
            }
            _ => {
                Err(LookupError::InvalidObjectName {
                    object_name: from
                        .iter()
                        .map(|t| t.relation.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    reason: "DELETE has no single base-table target".to_string(),
                })
            }
        }
    }

    fn kind(&self) -> DmlKind {
        DmlKind::Delete
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::Statement,
        dialect::{GenericDialect, MySqlDialect},
        parser::Parser,
    };

    use crate::{
        errors::LookupError,
        prelude::ParserDB,
        traits::{DMLLike, DatabaseLike, DmlKind, TableLike},
    };

    const SCHEMA: &str = "
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
    ";

    fn schema_db() -> ParserDB {
        ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema parses")
    }

    fn parse_one(sql: &str) -> Statement {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("statement parses");
        statements.pop().expect("one statement")
    }

    /// Mirrors the intended generic-routing usage, where `DB` is fixed by the
    /// database argument so `kind()` resolves without annotation.
    fn assert_target_and_kind<DB: DatabaseLike, S: DMLLike<DB>>(
        statement: &S,
        database: &DB,
        expected_target: &str,
        expected_kind: DmlKind,
    ) {
        assert_eq!(
            statement.target_table(database).expect("target resolves").table_name(),
            expected_target
        );
        assert_eq!(statement.kind(), expected_kind);
    }

    #[test]
    fn insert_target_and_kind() {
        let db = schema_db();
        let Statement::Insert(insert) = parse_one("INSERT INTO users (name) VALUES ('x')") else {
            panic!("expected insert");
        };
        assert_target_and_kind(&insert, &db, "users", DmlKind::Insert);
    }

    #[test]
    fn insert_select_resolves_target() {
        let db = schema_db();
        let Statement::Insert(insert) = parse_one("INSERT INTO orders SELECT * FROM orders") else {
            panic!("expected insert");
        };
        assert_target_and_kind(&insert, &db, "orders", DmlKind::Insert);
    }

    #[test]
    fn insert_into_unknown_table_errors() {
        let db = schema_db();
        let Statement::Insert(insert) = parse_one("INSERT INTO missing (a) VALUES (1)") else {
            panic!("expected insert");
        };
        assert!(matches!(
            DMLLike::<ParserDB>::target_table(&insert, &db),
            Err(LookupError::InvalidObjectName { .. })
        ));
    }

    #[test]
    fn update_target_and_kind() {
        let db = schema_db();
        let Statement::Update(update) = parse_one("UPDATE orders SET total = 0 WHERE id = 1")
        else {
            panic!("expected update");
        };
        assert_target_and_kind(&update, &db, "orders", DmlKind::Update);
    }

    #[test]
    fn update_with_alias_and_from_source_resolves_target() {
        let db = schema_db();
        let Statement::Update(update) =
            parse_one("UPDATE orders o SET total = 0 FROM users u WHERE o.user_id = u.id")
        else {
            panic!("expected update");
        };
        assert_target_and_kind(&update, &db, "orders", DmlKind::Update);
    }

    #[test]
    fn delete_target_and_kind() {
        let db = schema_db();
        let Statement::Delete(delete) = parse_one("DELETE FROM users WHERE id = 1") else {
            panic!("expected delete");
        };
        assert_target_and_kind(&delete, &db, "users", DmlKind::Delete);
    }

    #[test]
    fn multi_table_delete_has_no_single_target() {
        let db = schema_db();
        let mut statements = Parser::parse_sql(
            &MySqlDialect {},
            "DELETE users, orders FROM users JOIN orders ON users.id = orders.user_id",
        )
        .expect("statement parses");
        let Statement::Delete(delete) = statements.pop().unwrap() else {
            panic!("expected delete");
        };
        assert!(matches!(
            DMLLike::<ParserDB>::target_table(&delete, &db),
            Err(LookupError::InvalidObjectName { .. })
        ));
    }

    #[test]
    fn delete_with_join_has_no_single_target() {
        // A `FROM` whose relation carries a join is not a single base-table
        // target, so resolution reports an error rather than guessing.
        let db = schema_db();
        let Statement::Delete(delete) =
            parse_one("DELETE FROM users u JOIN orders o ON u.id = o.user_id")
        else {
            panic!("expected delete");
        };
        assert!(matches!(
            DMLLike::<ParserDB>::target_table(&delete, &db),
            Err(LookupError::InvalidObjectName { .. })
        ));
    }
}
