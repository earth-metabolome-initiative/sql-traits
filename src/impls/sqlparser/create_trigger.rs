//! Implementation of the `TriggerLike` trait for sqlparser's `CreateTrigger`
//! type.

use sqlparser::ast::{CreateTrigger, ObjectNamePart};

use crate::{
    structs::ParserDB,
    traits::{DatabaseLike, FunctionLike, Metadata, TriggerLike},
    utils::{identifier_resolution::identifiers_match, last_str},
};

impl Metadata for CreateTrigger {
    type Meta = ();
}

impl TriggerLike for CreateTrigger {
    type DB = ParserDB;

    #[inline]
    fn name(&self) -> &str {
        last_str(&self.name)
    }

    #[inline]
    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        database.table(None, last_str(&self.table_name)).expect("table must exist")
    }

    #[inline]
    fn events(&self) -> &[sqlparser::ast::TriggerEvent] {
        &self.events
    }

    #[inline]
    fn timing(&self) -> Option<sqlparser::ast::TriggerPeriod> {
        self.period
    }

    #[inline]
    fn orientation(&self) -> Option<sqlparser::ast::TriggerObjectKind> {
        self.trigger_object
    }

    #[inline]
    fn function<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Option<&'db <Self::DB as DatabaseLike>::Function>
    where
        Self: 'db,
    {
        let (function_name, function_quoted) = self.function_name_ident()?;
        database.functions().find(|function| {
            identifiers_match(
                function.name(),
                function.name_is_quoted(),
                function_name,
                function_quoted,
            )
        })
    }

    #[inline]
    fn function_name(&self) -> Option<&str> {
        self.function_name_ident().map(|(name, _)| name)
    }

    #[inline]
    fn function_name_ident(&self) -> Option<(&str, bool)> {
        let body = self.exec_body.as_ref()?;
        match body.func_desc.name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => {
                Some((ident.value.as_str(), ident.quote_style.is_some()))
            }
            Some(ObjectNamePart::Function(function_part)) => {
                Some((function_part.name.value.as_str(), function_part.name.quote_style.is_some()))
            }
            None => None,
        }
    }
}
