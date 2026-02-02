//! Implementation of the `PolicyLike` trait for `CreatePolicy` struct.

use sqlparser::ast::{CreatePolicy, CreatePolicyCommand, Expr, Owner};

use crate::{
    structs::{ParserDB, metadata::PolicyMetadata},
    traits::{DatabaseLike, DocumentationMetadata, Metadata, PolicyLike},
    utils::last_str,
};

impl Metadata for CreatePolicy {
    type Meta = PolicyMetadata<Self>;
}

impl DocumentationMetadata for CreatePolicy {
    type Documentation = ();
}

impl PolicyLike for CreatePolicy {
    type DB = ParserDB;

    fn name(&self) -> &str {
        &self.name.value
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        let table_name = last_str(&self.table_name);

        database.table(None, table_name).expect("Table referenced by policy not found")
    }

    fn command(&self) -> CreatePolicyCommand {
        self.command.unwrap_or(CreatePolicyCommand::All)
    }

    fn roles<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Owner>
    where
        Self: 'db,
    {
        self.to.iter().flat_map(|roles| roles.iter())
    }

    fn using_expression<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        self.using.as_ref()
    }

    fn using_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        database.policy_metadata(self).expect("Policy must exist in database").using_functions()
    }

    fn check_expression<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        self.with_check.as_ref()
    }

    fn check_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        database.policy_metadata(self).expect("Policy must exist in database").check_functions()
    }
}
