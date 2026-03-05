//! Functions to extract functions from SQL expressions.

use std::sync::Arc;

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

    // Remove duplicates while preserving order
    let mut seen = std::collections::HashSet::new();
    result
        .into_iter()
        .filter(|func| {
            let ptr = Arc::as_ptr(func).cast::<()>();
            seen.insert(ptr)
        })
        .collect()
}
