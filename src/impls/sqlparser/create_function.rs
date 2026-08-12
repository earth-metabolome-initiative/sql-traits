//! Implementation of the `FunctionLike` trait for sqlparser's `CreateFunction`
//! type.

use alloc::{borrow::Cow, format, string::ToString};

use sqlparser::ast::{
    CreateFunction, CreateFunctionBody, DataType, Expr, FunctionReturnType, FunctionSecurity,
    ObjectNamePart, Value, ValueWithSpan,
};

use crate::{
    errors::{LookupError, ObjectKind},
    structs::{FunctionMetadata, ParserDB},
    traits::{FunctionLike, Metadata},
    utils::{last_str, normalize_sqlparser_type},
};

impl Metadata for CreateFunction {
    type Meta = FunctionMetadata;
}

/// Resolves the metadata `database` holds for `function`.
///
/// A [`CreateFunction`] node and a [`ParserDB`] are independent values, so a
/// node the database does not hold (dropped, replaced, or parsed from different
/// input) has no metadata to report.
fn function_metadata<'db>(
    function: &CreateFunction,
    database: &'db ParserDB,
) -> Result<&'db FunctionMetadata, LookupError> {
    database
        .function_metadata(function)
        .ok_or_else(|| ObjectKind::Function.not_in_database(&function.name.to_string()))
}

impl FunctionLike for CreateFunction {
    type DB = ParserDB;

    #[inline]
    fn name(&self) -> &str {
        last_str(&self.name)
    }

    #[inline]
    fn name_is_quoted(&self) -> bool {
        match self.name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.quote_style.is_some(),
            Some(ObjectNamePart::Function(function_part)) => {
                function_part.name.quote_style.is_some()
            }
            None => false,
        }
    }

    #[inline]
    fn argument_type_names<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = Cow<'db, str>> {
        self.args
            .iter()
            .flat_map(|args| args.iter().map(|arg| normalize_sqlparser_type(&arg.data_type)))
    }

    #[inline]
    fn return_type_name<'db>(&'db self, _database: &'db Self::DB) -> Option<Cow<'db, str>> {
        self.return_type.as_ref().map(|rt| {
            match rt {
                FunctionReturnType::DataType(dt) => normalize_sqlparser_type(dt),
                // The SETOF marker survives the way an array's `[]` does: whether
                // the declaration names one value or a set of them is part of the
                // answer.
                FunctionReturnType::SetOf(dt) => {
                    Cow::Owned(format!("SETOF {}", normalize_sqlparser_type(dt)))
                }
            }
        })
    }

    #[inline]
    fn returns_set(&self) -> bool {
        match &self.return_type {
            Some(FunctionReturnType::SetOf(_)) => true,
            // A declared row shape is a set: PostgreSQL records `RETURNS
            // TABLE (...)` with `pg_proc.proretset = true`, exactly like
            // `SETOF`, and MSSQL's named-table return declares a table too.
            Some(FunctionReturnType::DataType(dt)) => {
                matches!(dt, DataType::Table(_) | DataType::NamedTable { .. })
            }
            None => false,
        }
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

    #[inline]
    fn security_mode(&self) -> FunctionSecurity {
        self.security.clone().unwrap_or(FunctionSecurity::Invoker)
    }

    #[inline]
    fn owner<'db>(&self, database: &'db Self::DB) -> Result<Option<&'db str>, LookupError> {
        Ok(function_metadata(self, database)?.owner())
    }
}
