//! Implementation of the `PolicyLike` trait for `CreatePolicy` struct.

use sqlparser::ast::{CreatePolicy, CreatePolicyCommand, CreatePolicyType, Expr, Owner};

use crate::{
    errors::{LookupError, ObjectKind},
    structs::{ParserDB, metadata::PolicyMetadata},
    traits::{DatabaseLike, DocumentationMetadata, Metadata, PolicyLike},
    utils::{
        identifier_resolution::is_public_pseudo_role,
        last_str,
        object_name::{object_name_last_part, resolve_required_table, schema_from_object_name},
    },
};

impl Metadata for CreatePolicy {
    type Meta = PolicyMetadata<Self>;
}

impl DocumentationMetadata for CreatePolicy {
    type Documentation = ();
}

/// Resolves the metadata `database` holds for `policy`.
fn policy_metadata<'db>(
    policy: &CreatePolicy,
    database: &'db ParserDB,
) -> Result<&'db PolicyMetadata<CreatePolicy>, LookupError> {
    database
        .policy_metadata(policy)
        .ok_or_else(|| ObjectKind::Policy.not_in_database(&policy.name.value))
}

impl PolicyLike for CreatePolicy {
    type DB = ParserDB;

    fn name(&self) -> &str {
        &self.name.value
    }

    fn table<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<&'db <Self::DB as DatabaseLike>::Table, LookupError>
    where
        Self: 'db,
    {
        resolve_required_table(&self.table_name, database)
    }

    fn target_table_name(&self) -> &str {
        last_str(&self.table_name)
    }

    fn target_table_name_is_quoted(&self) -> bool {
        object_name_last_part(&self.table_name).is_some_and(|(_, quoted)| quoted)
    }

    fn target_table_schema(&self) -> Option<&str> {
        schema_from_object_name(&self.table_name).map(|(schema, _)| schema)
    }

    fn target_table_schema_is_quoted(&self) -> bool {
        schema_from_object_name(&self.table_name).is_some_and(|(_, quoted)| quoted)
    }

    fn command(&self) -> CreatePolicyCommand {
        self.command.unwrap_or(CreatePolicyCommand::All)
    }

    fn policy_type(&self) -> CreatePolicyType {
        self.policy_type.unwrap_or(CreatePolicyType::Permissive)
    }

    fn roles<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Owner>
    where
        Self: 'db,
    {
        self.to.iter().flat_map(|roles| roles.iter())
    }

    fn applies_to_public(&self) -> bool {
        match &self.to {
            // No `TO` clause at all, which PostgreSQL defaults to `PUBLIC`.
            None => true,
            Some(owners) => {
                owners.is_empty()
                    || owners.iter().any(|owner| {
                        matches!(owner, Owner::Ident(ident)
                        if is_public_pseudo_role(
                            ident.value.as_str(),
                            ident.quote_style.is_some(),
                        ))
                    })
            }
        }
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
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function>, LookupError> {
        Ok(policy_metadata(self, database)?.using_functions())
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
    ) -> Result<impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function>, LookupError> {
        Ok(policy_metadata(self, database)?.check_functions())
    }
}
