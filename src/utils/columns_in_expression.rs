//! Functions to extract columns from SQL expressions.

use sqlparser::ast::Expr;

use crate::traits::column::ColumnLike;

/// Extracts columns from a SQL expression.
///
/// # Arguments
///
/// * `expr` - The SQL expression to extract columns from.
/// * `table_name` - The name of the table the expression belongs to.
/// * `columns` - The list of columns available in the table.
///
/// # Returns
///
/// * A vector of columns found in the expression.
///
/// # Errors
///
/// * If a column in the expression is not found in the provided list of
///   columns.
pub fn columns_in_expression<C: ColumnLike + Clone>(
    expr: &Expr,
    table_name: &str,
    columns: &[C],
) -> Result<Vec<C>, crate::errors::Error> {
    let mut result = Vec::new();

    match expr {
        Expr::Identifier(ident) => {
            if let Some(col) = columns.iter().find(|col| col.column_name() == ident.value.as_str())
            {
                result.push(col.clone());
            } else {
                return Err(crate::errors::Error::UnknownColumnInCheckConstraint {
                    column_name: ident.value.clone(),
                    table_name: table_name.to_string(),
                });
            }
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(last_ident) = idents.last() {
                if let Some(col) =
                    columns.iter().find(|col| col.column_name() == last_ident.value.as_str())
                {
                    result.push(col.clone());
                } else {
                    return Err(crate::errors::Error::UnknownColumnInCheckConstraint {
                        column_name: last_ident.value.clone(),
                        table_name: table_name.to_string(),
                    });
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            result.extend(columns_in_expression(left, table_name, columns)?);
            result.extend(columns_in_expression(right, table_name, columns)?);
        }
        Expr::Nested(nested_expr) => {
            result.extend(columns_in_expression(nested_expr, table_name, columns)?);
        }
        Expr::Between { expr, negated: _, low, high } => {
            result.extend(columns_in_expression(expr, table_name, columns)?);
            result.extend(columns_in_expression(low, table_name, columns)?);
            result.extend(columns_in_expression(high, table_name, columns)?);
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => {
            result.extend(columns_in_expression(expr, table_name, columns)?);
        }
        Expr::Function(func) => {
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
                            result.extend(columns_in_expression(expr, table_name, columns)?);
                        }
                        sqlparser::ast::FunctionArg::ExprNamed { .. }
                        | sqlparser::ast::FunctionArg::Named { .. }
                        | sqlparser::ast::FunctionArg::Unnamed(_) => {}
                    }
                }
            }
        }
        Expr::InList { expr, list, .. } => {
            result.extend(columns_in_expression(expr, table_name, columns)?);
            for list_expr in list {
                result.extend(columns_in_expression(list_expr, table_name, columns)?);
            }
        }
        Expr::InSubquery { expr, .. } => {
            result.extend(columns_in_expression(expr, table_name, columns)?);
            // Note: We don't traverse into subqueries as they have their own
            // column scope
        }
        Expr::Tuple(exprs) => {
            for expr in exprs {
                result.extend(columns_in_expression(expr, table_name, columns)?);
            }
        }
        _ => {}
    }

    // Remove duplicates while preserving order
    let mut seen = std::collections::HashSet::new();
    Ok(result.into_iter().filter(|col| seen.insert(col.clone())).collect())
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use sqlparser::{
        ast::{
            BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
            ObjectName, ObjectNamePart, Statement,
        },
        dialect::GenericDialect,
        parser::Parser,
    };

    use super::*;
    use crate::{
        structs::{ParserDB, TableAttribute},
        traits::DatabaseLike,
    };

    fn create_column(name: &str) -> <ParserDB as DatabaseLike>::Column {
        let sql = format!("CREATE TABLE t ({name} INT)");
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, &sql).expect("Failed to parse SQL");
        if let Statement::CreateTable(ct) = &ast[0] {
            let table = Rc::new(ct.clone());
            let col_def = ct.columns[0].clone();
            TableAttribute::new(table, col_def)
        } else {
            panic!("Expected CreateTable statement");
        }
    }

    #[test]
    fn test_columns_in_expression_identifier() {
        let col_a = create_column("a");
        let columns = vec![col_a.clone()];
        let expr = Expr::Identifier(Ident::new("a"));

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name(), "a");
    }

    #[test]
    fn test_columns_in_expression_compound_identifier() {
        let col_a = create_column("a");
        let columns = vec![col_a.clone()];
        let expr = Expr::CompoundIdentifier(vec![Ident::new("t"), Ident::new("a")]);

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name(), "a");
    }

    #[test]
    fn test_columns_in_expression_binary_op() {
        let col_a = create_column("a");
        let col_b = create_column("b");
        let columns = vec![col_a.clone(), col_b.clone()];
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a"))),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b"))),
        };

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].column_name(), "a");
        assert_eq!(result[1].column_name(), "b");
    }

    #[test]
    fn test_columns_in_expression_nested_and_deduplication() {
        let col_a = create_column("a");
        let columns = vec![col_a.clone()];
        // (a) AND (a)
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Nested(Box::new(Expr::Identifier(Ident::new("a"))))),
            op: BinaryOperator::And,
            right: Box::new(Expr::Identifier(Ident::new("a"))),
        };

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name(), "a");
    }

    #[test]
    fn test_columns_in_expression_function() {
        let col_a = create_column("a");
        let columns = vec![col_a.clone()];
        // my_func(a)
        let expr = Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("my_func"))]),
            args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                    Ident::new("a"),
                )))],
                clauses: vec![],
            }),
            over: None,
            filter: None,
            null_treatment: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
        });

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name(), "a");
    }

    #[test]
    fn test_columns_in_expression_between() {
        let col_a = create_column("a");
        let col_b = create_column("b");
        let col_c = create_column("c");
        let columns = vec![col_a.clone(), col_b.clone(), col_c.clone()];
        // a BETWEEN b AND c
        let expr = Expr::Between {
            expr: Box::new(Expr::Identifier(Ident::new("a"))),
            negated: false,
            low: Box::new(Expr::Identifier(Ident::new("b"))),
            high: Box::new(Expr::Identifier(Ident::new("c"))),
        };

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].column_name(), "a");
        assert_eq!(result[1].column_name(), "b");
        assert_eq!(result[2].column_name(), "c");
    }

    #[test]
    fn test_columns_in_expression_in_list() {
        let col_a = create_column("a");
        let col_b = create_column("b");
        let columns = vec![col_a.clone(), col_b.clone()];
        // a IN (b)
        let expr = Expr::InList {
            expr: Box::new(Expr::Identifier(Ident::new("a"))),
            list: vec![Expr::Identifier(Ident::new("b"))],
            negated: false,
        };

        let result: Vec<<ParserDB as DatabaseLike>::Column> =
            columns_in_expression(&expr, "t", &columns).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].column_name(), "a");
        assert_eq!(result[1].column_name(), "b");
    }

    #[test]
    fn test_columns_in_expression_unknown_column() {
        let col_a = create_column("a");
        let columns = vec![col_a.clone()];
        let expr = Expr::Identifier(Ident::new("b"));

        let result = columns_in_expression(&expr, "t", &columns);
        assert!(result.is_err());
        match result.err().unwrap() {
            crate::errors::Error::UnknownColumnInCheckConstraint { column_name, table_name } => {
                assert_eq!(column_name, "b");
                assert_eq!(table_name, "t");
            }
            _ => panic!("Unexpected error type"),
        }
    }
}
