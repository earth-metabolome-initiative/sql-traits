//! Implementation of the `FunctionLike` trait for sqlparser's `CreateFunction`
//! type.

use sqlparser::ast::{CreateFunction, CreateFunctionBody, Expr, Value, ValueWithSpan};

use crate::{
    structs::ParserDBInner,
    traits::{FunctionLike, Metadata},
    utils::{last_str, normalize_sqlparser_type},
};

impl Metadata for CreateFunction {
    type Meta = ();
}

impl FunctionLike for CreateFunction {
    type DB = ParserDBInner;

    #[inline]
    fn name(&self) -> &str {
        last_str(&self.name)
    }

    #[inline]
    fn argument_type_names<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db str> {
        self.args
            .iter()
            .flat_map(|args| args.iter().map(|arg| normalize_sqlparser_type(&arg.data_type)))
    }

    #[inline]
    fn return_type_name<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db str> {
        self.return_type.as_ref().map(normalize_sqlparser_type)
    }

    #[inline]
    fn body(&self) -> Option<&str> {
        let body_expr = match &self.function_body {
            Some(CreateFunctionBody::AsBeforeOptions { body, .. }) => body,
            Some(CreateFunctionBody::Return(expr)) => expr,
            _ => return None,
        };

        match body_expr {
            Expr::Value(ValueWithSpan { value: Value::SingleQuotedString(s), .. }) => Some(s),
            Expr::Value(ValueWithSpan { value: Value::DollarQuotedString(s), .. }) => {
                Some(&s.value)
            }
            _ => None,
        }
    }
}
