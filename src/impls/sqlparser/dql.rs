//! Implementation of [`DQLLike`] for sqlparser's `Query` node.
//!
//! `projection_source_table` builds an alias map from the outer `SELECT`'s
//! `FROM` clause (resolving each base-table relation, and treating derived
//! subqueries, table functions, CTE references, and unknown names as opaque),
//! then resolves each projected item to the base table it comes from. It is
//! deliberately strict: anything it cannot prove comes from a single base table
//! yields `Ok(None)`, since the consumer (single-table row re-execution) must
//! never be told a multi-source or computed projection is a base-table row.

use alloc::{string::String, vec::Vec};

use sqlparser::ast::{
    Expr, GroupByExpr, Ident, ObjectName, Query, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, TableFactor,
};

use crate::{
    errors::LookupError,
    traits::{ColumnLike, DQLLike, DatabaseLike, TableLike},
    utils::{
        identifier_resolution::identifiers_match,
        object_name::{object_name_last_part, render_table_candidate, resolve_object_name},
    },
};

/// A `FROM` relation that resolved to a base table, paired with the identifier
/// (alias, or table name when unaliased) used to qualify it in the projection.
struct FromTableRef<'a, 'db, DB: DatabaseLike> {
    key_value: &'a str,
    key_quoted: bool,
    table: &'db DB::Table,
}

/// Collects the CTE names introduced by the query's outer `WITH` clause.
fn collect_cte_names(query: &Query) -> Vec<(&str, bool)> {
    query
        .with
        .as_ref()
        .map(|with| {
            with.cte_tables
                .iter()
                .map(|cte| (cte.alias.name.value.as_str(), cte.alias.name.quote_style.is_some()))
                .collect()
        })
        .unwrap_or_default()
}

/// Returns whether `name` is a reference to one of the outer CTE names (a bare,
/// single-part identifier matching a declared CTE).
fn is_cte_reference(name: &ObjectName, cte_names: &[(&str, bool)]) -> bool {
    if name.0.len() != 1 {
        return false;
    }
    match object_name_last_part(name) {
        Some((value, quoted)) => {
            cte_names.iter().any(|(cte_value, cte_quoted)| {
                identifiers_match(cte_value, *cte_quoted, value, quoted)
            })
        }
        None => false,
    }
}

/// Returns whether `table` exposes a column matching `column`, applying
/// PostgreSQL identifier semantics (including the projection identifier's own
/// quoting).
fn table_exposes_column<'db, DB: DatabaseLike>(
    table: &'db DB::Table,
    column: &Ident,
    database: &'db DB,
) -> bool {
    table.columns(database).any(|candidate| {
        identifiers_match(
            candidate.column_name(),
            candidate.column_name_is_quoted(),
            column.value.as_str(),
            column.quote_style.is_some(),
        )
    })
}

/// Finds the base table whose qualifying key matches `value`/`quoted`.
fn base_for_qualifier<'db, DB: DatabaseLike>(
    bases: &[FromTableRef<'_, 'db, DB>],
    value: &str,
    quoted: bool,
) -> Option<&'db DB::Table> {
    bases
        .iter()
        .find(|base| identifiers_match(base.key_value, base.key_quoted, value, quoted))
        .map(|base| base.table)
}

/// Resolves an unqualified column to the single `FROM` base table that exposes
/// it.
fn unqualified_column_source<'db, DB: DatabaseLike>(
    bases: &[FromTableRef<'_, 'db, DB>],
    has_opaque: bool,
    column: &Ident,
    database: &'db DB,
) -> Result<Option<&'db DB::Table>, LookupError> {
    // An opaque relation (derived subquery, table function, CTE) might also
    // expose the column, and we cannot introspect it, so we cannot claim a
    // single base table.
    if has_opaque {
        return Ok(None);
    }

    let matches: Vec<&'db DB::Table> = bases
        .iter()
        .filter(|base| table_exposes_column(base.table, column, database))
        .map(|base| base.table)
        .collect();

    match matches.as_slice() {
        [] => Ok(None),
        [table] => Ok(Some(*table)),
        _ => {
            let mut candidates: Vec<String> =
                matches.iter().map(|table| render_table_candidate(*table)).collect();
            candidates.sort_unstable();
            candidates.dedup();
            Err(LookupError::AmbiguousTableLookup { object_name: column.value.clone(), candidates })
        }
    }
}

/// Resolves a single projected expression to the base table it passes through,
/// or `Ok(None)` when it is not a pass-through column of a base table.
fn column_source<'db, DB: DatabaseLike>(
    expr: &Expr,
    bases: &[FromTableRef<'_, 'db, DB>],
    has_opaque: bool,
    database: &'db DB,
) -> Result<Option<&'db DB::Table>, LookupError> {
    match expr {
        Expr::Identifier(column) => unqualified_column_source(bases, has_opaque, column, database),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let column = &parts[parts.len() - 1];
            let qualifier = &parts[parts.len() - 2];
            match base_for_qualifier(
                bases,
                qualifier.value.as_str(),
                qualifier.quote_style.is_some(),
            ) {
                Some(table) if table_exposes_column(table, column, database) => Ok(Some(table)),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// Records a single `FROM` table factor into the alias map.
fn collect_factor<'a, 'db, DB: DatabaseLike>(
    factor: &'a TableFactor,
    database: &'db DB,
    cte_names: &[(&'a str, bool)],
    bases: &mut Vec<FromTableRef<'a, 'db, DB>>,
    from_entry_count: &mut usize,
    has_opaque: &mut bool,
) -> Result<(), LookupError> {
    *from_entry_count += 1;
    match factor {
        TableFactor::Table { name, alias, args, .. } => {
            let (key_value, key_quoted) = match alias {
                Some(table_alias) => {
                    (table_alias.name.value.as_str(), table_alias.name.quote_style.is_some())
                }
                None => object_name_last_part(name).unwrap_or(("", false)),
            };
            // A table-valued function call or a CTE reference is not a base
            // table, even if a base table shares the CTE's name.
            if args.is_some() || is_cte_reference(name, cte_names) {
                *has_opaque = true;
            } else {
                match resolve_object_name(name, database)? {
                    Some(table) => bases.push(FromTableRef { key_value, key_quoted, table }),
                    None => *has_opaque = true,
                }
            }
        }
        // Derived subqueries, table functions, nested joins, and the rest are
        // opaque: their columns are not those of a single base table.
        _ => *has_opaque = true,
    }
    Ok(())
}

impl<DB: DatabaseLike> DQLLike<DB> for Query {
    fn projection_source_table<'db>(
        &self,
        database: &'db DB,
    ) -> Result<Option<&'db DB::Table>, LookupError> {
        // Only a plain SELECT body has a single projection to analyze.
        let SetExpr::Select(select) = self.body.as_ref() else {
            return Ok(None);
        };

        // DISTINCT and GROUP BY collapse rows, so the output is not keyed by a
        // base table's primary key.
        if select.distinct.is_some() {
            return Ok(None);
        }
        match &select.group_by {
            GroupByExpr::All(_) => return Ok(None),
            GroupByExpr::Expressions(expressions, _) if !expressions.is_empty() => {
                return Ok(None);
            }
            GroupByExpr::Expressions(_, _) => {}
        }

        let cte_names = collect_cte_names(self);

        let mut bases: Vec<FromTableRef<'_, 'db, DB>> = Vec::new();
        let mut from_entry_count: usize = 0;
        let mut has_opaque = false;
        for table_with_joins in &select.from {
            collect_factor(
                &table_with_joins.relation,
                database,
                &cte_names,
                &mut bases,
                &mut from_entry_count,
                &mut has_opaque,
            )?;
            for join in &table_with_joins.joins {
                collect_factor(
                    &join.relation,
                    database,
                    &cte_names,
                    &mut bases,
                    &mut from_entry_count,
                    &mut has_opaque,
                )?;
            }
        }

        let mut source: Option<&'db DB::Table> = None;
        for item in &select.projection {
            let item_source = match item {
                // `*` is a single base-table row only when the FROM is exactly
                // that one base table.
                SelectItem::Wildcard(_) => {
                    if from_entry_count == 1 && bases.len() == 1 {
                        Some(bases[0].table)
                    } else {
                        None
                    }
                }
                SelectItem::QualifiedWildcard(kind, _) => {
                    match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                            match object_name_last_part(object_name) {
                                Some((value, quoted)) => base_for_qualifier(&bases, value, quoted),
                                None => None,
                            }
                        }
                        SelectItemQualifiedWildcardKind::Expr(_) => None,
                    }
                }
                SelectItem::UnnamedExpr(expr)
                | SelectItem::ExprWithAlias { expr, .. }
                | SelectItem::ExprWithAliases { expr, .. } => {
                    column_source(expr, &bases, has_opaque, database)?
                }
            };

            match item_source {
                None => return Ok(None),
                Some(table) => {
                    match source {
                        None => source = Some(table),
                        Some(existing) => {
                            if database.table_id(existing) != database.table_id(table) {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }

        Ok(source)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};

    use crate::{
        errors::LookupError,
        prelude::ParserDB,
        traits::{DQLLike, TableLike},
    };

    const SCHEMA: &str = "
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);
    ";

    fn schema_db() -> ParserDB {
        ParserDB::parse::<GenericDialect>(SCHEMA).expect("schema parses")
    }

    fn query_of(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("query parses");
        match statements.pop().expect("one statement") {
            Statement::Query(query) => *query,
            other => panic!("expected a query, got {other:?}"),
        }
    }

    fn source_name(sql: &str, db: &ParserDB) -> Option<String> {
        query_of(sql)
            .projection_source_table(db)
            .expect("projection_source_table succeeds")
            .map(|table| table.table_name().to_string())
    }

    #[test]
    fn single_table_columns() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id, name FROM users", &db), Some("users".to_string()));
    }

    #[test]
    fn single_table_wildcard() {
        let db = schema_db();
        assert_eq!(source_name("SELECT * FROM users", &db), Some("users".to_string()));
    }

    #[test]
    fn join_qualified_single_table_columns() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id, o.total FROM orders o JOIN users u ON o.user_id = u.id", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn join_qualified_wildcard() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.* FROM orders o JOIN users u ON o.user_id = u.id", &db),
            Some("orders".to_string())
        );
    }

    #[test]
    fn join_multi_table_projection_is_none() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id", &db),
            None
        );
    }

    #[test]
    fn wildcard_over_join_is_none() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT * FROM orders o JOIN users u ON o.user_id = u.id", &db),
            None
        );
    }

    #[test]
    fn computed_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT o.id + 1 FROM orders o", &db), None);
    }

    #[test]
    fn aggregate_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT COUNT(*) FROM users", &db), None);
    }

    #[test]
    fn group_by_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT user_id FROM orders GROUP BY user_id", &db), None);
    }

    #[test]
    fn distinct_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT DISTINCT user_id FROM orders", &db), None);
    }

    #[test]
    fn subquery_in_where_keeps_single_table() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT id, name FROM users WHERE id IN (SELECT user_id FROM orders)", &db),
            Some("users".to_string())
        );
    }

    #[test]
    fn set_operation_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users UNION SELECT id FROM orders", &db), None);
    }

    #[test]
    fn cte_reference_is_not_a_base_table() {
        let db = schema_db();
        // `users` here is the CTE, not the base table, so the unqualified `id`
        // cannot be claimed as a base-table column.
        assert_eq!(
            source_name("WITH users AS (SELECT id FROM orders) SELECT id FROM users", &db),
            None
        );
    }

    #[test]
    fn derived_table_projection_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT total FROM (SELECT total FROM orders) d", &db), None);
    }

    #[test]
    fn ambiguous_unqualified_column_errors() {
        let db = schema_db();
        let result = query_of("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
            .projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
    }

    #[test]
    fn self_join_unqualified_column_is_ambiguous() {
        let db = schema_db();
        let result = query_of("SELECT name FROM users a JOIN users b ON a.id = b.id")
            .projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::AmbiguousTableLookup { .. })), "got {result:?}");
    }

    #[test]
    fn self_join_qualified_wildcard_picks_one_alias() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT a.* FROM users a JOIN users b ON a.id = b.id", &db),
            Some("users".to_string())
        );
    }

    #[test]
    fn aliased_projection_columns_keep_single_table() {
        let db = schema_db();
        assert_eq!(
            source_name("SELECT o.id AS oid, o.total AS amount FROM orders o", &db),
            Some("orders".to_string()),
        );
    }

    #[test]
    fn qualified_column_with_unknown_qualifier_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT x.id FROM orders o", &db), None);
    }

    #[test]
    fn qualified_column_absent_from_table_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT o.nope FROM orders o", &db), None);
    }

    #[test]
    fn unknown_from_table_is_opaque_and_not_eligible() {
        let db = schema_db();
        assert_eq!(source_name("SELECT anything FROM does_not_exist", &db), None);
    }

    #[test]
    fn two_part_from_name_does_not_match_schemaless_table() {
        let db = schema_db();
        // `public.users` is a two-part lookup; our `users` is schema-less, so it
        // does not match and the relation is treated as opaque.
        assert_eq!(source_name("SELECT id FROM public.users", &db), None);
    }

    #[test]
    fn group_by_all_is_none() {
        let db = schema_db();
        assert_eq!(source_name("SELECT id FROM users GROUP BY ALL", &db), None);
    }

    #[test]
    fn overqualified_from_name_errors() {
        let db = schema_db();
        let result = query_of("SELECT * FROM a.b.c").projection_source_table(&db);
        assert!(matches!(result, Err(LookupError::InvalidObjectName { .. })), "got {result:?}");
    }
}
