//! Functions to extract functions from SQL expressions.

use alloc::{sync::Arc, vec::Vec};

use sqlparser::ast::{Expr, ObjectName, ObjectNamePart};

use crate::{
    traits::{DatabaseLike, function_like::FunctionLike},
    utils::identifier_resolution::identifiers_match,
};

fn function_matches_object_name<DB: DatabaseLike>(
    function: &DB::Function,
    object_name: &ObjectName,
) -> bool {
    match object_name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => {
            identifiers_match(
                function.name(),
                function.name_is_quoted(),
                ident.value.as_str(),
                ident.quote_style.is_some(),
            )
        }
        Some(ObjectNamePart::Function(function_part)) => {
            identifiers_match(
                function.name(),
                function.name_is_quoted(),
                function_part.name.value.as_str(),
                function_part.name.quote_style.is_some(),
            )
        }
        None => false,
    }
}

pub(super) fn functions_in_expression<DB: DatabaseLike>(
    expr: &Expr,
    functions: &[Arc<DB::Function>],
) -> Vec<Arc<DB::Function>> {
    let mut result = Vec::new();

    match expr {
        Expr::Function(func) => {
            // Match by function identifier, ignoring optional schema qualifiers.
            result.extend(
                functions
                    .iter()
                    .filter(|f| function_matches_object_name::<DB>(f.as_ref(), &func.name))
                    .cloned(),
            );

            // Recursively check function arguments for nested function calls
            if let sqlparser::ast::FunctionArguments::List(args) = &func.args {
                for arg in &args.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Named {
                            arg: sqlparser::ast::FunctionArgExpr::Expr(expr),
                            ..
                        }
                        | sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(expr),
                        ) => {
                            result.extend(functions_in_expression::<DB>(expr, functions));
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { .. }
                        | sqlparser::ast::FunctionArg::Named { .. }
                        | sqlparser::ast::FunctionArg::Unnamed(_) => {}
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            result.extend(functions_in_expression::<DB>(left, functions));
            result.extend(functions_in_expression::<DB>(right, functions));
        }
        Expr::Nested(nested_expr) => {
            result.extend(functions_in_expression::<DB>(nested_expr, functions));
        }
        Expr::Between { expr, negated: _, low, high } => {
            result.extend(functions_in_expression::<DB>(expr, functions));
            result.extend(functions_in_expression::<DB>(low, functions));
            result.extend(functions_in_expression::<DB>(high, functions));
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => {
            result.extend(functions_in_expression::<DB>(expr, functions));
        }
        Expr::InList { expr, list, .. } => {
            result.extend(functions_in_expression::<DB>(expr, functions));
            for list_expr in list {
                result.extend(functions_in_expression::<DB>(list_expr, functions));
            }
        }
        Expr::InSubquery { expr, .. } => {
            result.extend(functions_in_expression::<DB>(expr, functions));
            // Note: We don't traverse into subqueries as they have their own
            // scope
        }
        _ => {}
    }

    // Remove duplicates while preserving order. BTreeSet works on raw
    // pointers (which implement `Ord`) and keeps the helper `alloc`-only
    // for a no_std-compatible build.
    let mut seen: alloc::collections::BTreeSet<*const ()> = alloc::collections::BTreeSet::new();
    result
        .into_iter()
        .filter(|func| {
            let ptr = Arc::as_ptr(func).cast::<()>();
            seen.insert(ptr)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    //! These tests exercise `functions_in_expression` indirectly by parsing
    //! schemas whose CHECK constraints reference user-defined functions in
    //! the shapes that drive the un-tested branches: no-arg calls, nested
    //! calls, and call-twice-dedup.

    use sqlparser::dialect::GenericDialect;

    use crate::{
        prelude::ParserDB,
        traits::{DatabaseLike, FunctionLike, TableLike},
    };

    /// `FunctionArguments::None` branch — a no-argument function call in a
    /// CHECK expression is correctly attributed to the defining function.
    #[test]
    fn test_no_arg_function_call_is_attributed() {
        let sql = "
            CREATE FUNCTION ping() RETURNS BOOLEAN AS 'SELECT TRUE';
            CREATE TABLE t (id INT, CHECK (ping()));
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let check = t.check_constraints(&db).next().expect("check");
        let meta = db.check_constraint_metadata(check).expect("check meta");
        let names: Vec<&str> = meta.functions().map(FunctionLike::name).collect();
        assert!(names.contains(&"ping"));
    }

    /// Same function used twice in one expression is deduplicated (BTreeSet
    /// over `Arc::as_ptr` keeps a single entry).
    #[test]
    fn test_same_function_used_twice_is_deduped() {
        let sql = "
            CREATE FUNCTION ping() RETURNS BOOLEAN AS 'SELECT TRUE';
            CREATE TABLE t (id INT, CHECK (ping() AND ping()));
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let check = t.check_constraints(&db).next().expect("check");
        let meta = db.check_constraint_metadata(check).expect("check meta");
        let names: Vec<&str> = meta.functions().map(FunctionLike::name).collect();
        assert_eq!(names.len(), 1, "ping() appears twice in source but dedups to one");
        assert_eq!(names[0], "ping");
    }

    /// Nested function calls (`f(g(...))`) — the recursion walks into the
    /// inner arg list and attributes both functions.
    #[test]
    fn test_nested_function_calls_are_both_attributed() {
        let sql = "
            CREATE FUNCTION inner_fn(x INT) RETURNS INT AS 'SELECT $1';
            CREATE FUNCTION outer_fn(x INT) RETURNS BOOLEAN AS 'SELECT $1 > 0';
            CREATE TABLE t (id INT, CHECK (outer_fn(inner_fn(id))));
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let check = t.check_constraints(&db).next().expect("check");
        let meta = db.check_constraint_metadata(check).expect("check meta");
        let names: Vec<&str> = meta.functions().map(FunctionLike::name).collect();
        assert!(names.contains(&"outer_fn"), "outer function attributed");
        assert!(names.contains(&"inner_fn"), "inner function attributed via recursion");
    }

    /// `Expr::InList` branch in `functions_in_expression`: a function
    /// call appearing inside an `IN (...)` list is attributed via the
    /// `list_expr` recursion.
    #[test]
    fn test_in_list_function_call_is_attributed() {
        let sql = "
            CREATE FUNCTION classify(x INT) RETURNS INT AS 'SELECT $1';
            CREATE TABLE t (
                id INT,
                CHECK (id IN (classify(1), classify(2), 3))
            );
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let check = t.check_constraints(&db).next().expect("check");
        let meta = db.check_constraint_metadata(check).expect("check meta");
        let names: Vec<&str> = meta.functions().map(FunctionLike::name).collect();
        assert!(names.contains(&"classify"));
    }

    /// `Expr::Between` branch: function calls in any of the three
    /// sub-expressions (`expr`, `low`, `high`) are all attributed.
    #[test]
    fn test_between_function_calls_in_all_three_positions() {
        let sql = "
            CREATE FUNCTION lo() RETURNS INT AS 'SELECT 1';
            CREATE FUNCTION hi() RETURNS INT AS 'SELECT 10';
            CREATE FUNCTION mid(x INT) RETURNS INT AS 'SELECT $1';
            CREATE TABLE t (
                id INT,
                CHECK (mid(id) BETWEEN lo() AND hi())
            );
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let check = t.check_constraints(&db).next().expect("check");
        let meta = db.check_constraint_metadata(check).expect("check meta");
        let names: Vec<&str> = meta.functions().map(FunctionLike::name).collect();
        assert!(names.contains(&"lo"));
        assert!(names.contains(&"hi"));
        assert!(names.contains(&"mid"));
    }
}
