//! Implementation of the `TriggerLike` trait for sqlparser's `CreateTrigger`
//! type.

use sqlparser::ast::CreateTrigger;

use crate::{
    structs::ParserDB,
    traits::{DatabaseLike, Metadata, TriggerLike},
    utils::last_str,
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
        self.exec_body.as_ref().and_then(|body| database.function(last_str(&body.func_desc.name)))
    }

    #[inline]
    fn function_name(&self) -> Option<&str> {
        self.exec_body.as_ref().map(|body| last_str(&body.func_desc.name))
    }
}
