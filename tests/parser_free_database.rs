//! A catalog implemented without the SQL parser, so the bodies the traits
//! carry for implementors reach a test.
//!
//! `GenericDB` overrides the faster ones, which leaves every inherited body it
//! replaces with no caller in this repository, and the library promises those
//! bodies to anybody describing a catalog from somewhere other than a parsed
//! script. The in-memory catalog here is that second implementor: it stores
//! plain strings, answers only what a catalog must answer, and inherits
//! everything else.
#![allow(clippy::expect_used)]

use std::borrow::Cow;

use sql_traits::{
    errors::LookupError,
    prelude::*,
    structs::TargetName,
    traits::{ColumnCollation, TypeMatch, grant::GrantRelation},
    utils::identifier_resolution::stored_identifier_matches_lookup,
};
use sqlparser::ast::{
    Action, ConstraintReferenceMatchKind, CreatePolicyCommand, CreatePolicyType, Expr,
    FunctionCalledOnNull, FunctionDefinitionSetParam, FunctionSecurity, Grantee, Owner, Query,
    TriggerEvent, TriggerObjectKind, TriggerPeriod,
};

/// Whether a stored schema, absent for the default one, answers a written
/// qualifier, absent for an unqualified name.
fn schema_answers(stored: Option<&str>, stored_is_quoted: bool, written: Option<&str>) -> bool {
    match (stored, written) {
        (None, None) => true,
        (Some(stored), None) => {
            stored_identifier_matches_lookup(stored, stored_is_quoted, "public")
        }
        (None, Some(written)) => stored_identifier_matches_lookup("public", false, written),
        (Some(stored), Some(written)) => {
            stored_identifier_matches_lookup(stored, stored_is_quoted, written)
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MemoryCatalog {
    catalog_name: String,
    dialect: MemoryDialect,
    tables: Vec<MemoryTable>,
    columns: Vec<MemoryColumn>,
    schemas: Vec<MemorySchema>,
    views: Vec<MemoryView>,
    materialized_views: Vec<MemoryView>,
    indexes: Vec<MemoryIndex>,
    unique_indexes: Vec<MemoryUniqueIndex>,
    foreign_keys: Vec<MemoryForeignKey>,
    check_constraints: Vec<MemoryCheckConstraint>,
    functions: Vec<MemoryFunction>,
    triggers: Vec<MemoryTrigger>,
    policies: Vec<MemoryPolicy>,
    roles: Vec<MemoryRole>,
    table_grants: Vec<MemoryTableGrant>,
    column_grants: Vec<MemoryColumnGrant>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryDialect;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryTable {
    schema: Option<String>,
    schema_is_quoted: bool,
    name: String,
    name_is_quoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryColumn {
    table_schema: Option<String>,
    table_name: String,
    name: String,
    name_is_quoted: bool,
    data_type: String,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemorySchema {
    name: String,
    name_is_quoted: bool,
}

/// Never built by these tests: a catalog with no views still has to name the
/// type its views would have.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryView {
    schema: Option<String>,
    name: String,
    materialized: bool,
    definition: Query,
    declared_column_names: Vec<(String, bool)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryIndex {
    name: Option<String>,
    schema: Option<String>,
    table_name: String,
    expression: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryUniqueIndex(MemoryIndex);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryForeignKey {
    name: Option<String>,
    host_table_name: String,
    referenced_table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryCheckConstraint {
    table_name: String,
    expression: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryFunction {
    name: String,
    schema: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryTrigger {
    name: String,
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryPolicy {
    name: String,
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryRole {
    name: String,
    name_is_quoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryTableGrant {
    table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemoryColumnGrant {
    table_name: String,
}

impl Metadata for MemoryCatalog {
    type Meta = ();
}

macro_rules! plain_metadata {
    ($($kind:ty),+ $(,)?) => {
        $(
            impl Metadata for $kind {
                type Meta = ();
            }
        )+
    };
}

plain_metadata!(
    MemoryTable,
    MemoryColumn,
    MemorySchema,
    MemoryView,
    MemoryIndex,
    MemoryUniqueIndex,
    MemoryForeignKey,
    MemoryCheckConstraint,
    MemoryFunction,
    MemoryTrigger,
    MemoryPolicy,
    MemoryRole,
    MemoryTableGrant,
    MemoryColumnGrant,
);

impl DocumentationMetadata for MemoryTable {
    type Documentation = ();
}

impl DocumentationMetadata for MemoryPolicy {
    type Documentation = ();
}

impl DatabaseLike for MemoryCatalog {
    type Table = MemoryTable;
    type View = MemoryView;
    type MaterializedView = MemoryView;
    type Column = MemoryColumn;
    type Index = MemoryIndex;
    type ForeignKey = MemoryForeignKey;
    type Function = MemoryFunction;
    type UniqueIndex = MemoryUniqueIndex;
    type CheckConstraint = MemoryCheckConstraint;
    type Trigger = MemoryTrigger;
    type Policy = MemoryPolicy;
    type Role = MemoryRole;
    type TableGrant = MemoryTableGrant;
    type ColumnGrant = MemoryColumnGrant;
    type Schema = MemorySchema;
    type Dialect = MemoryDialect;

    fn dialect(&self) -> &Self::Dialect {
        &self.dialect
    }

    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    fn number_of_tables(&self) -> usize {
        self.tables.len()
    }

    fn timezone(&self) -> Option<&str> {
        None
    }

    fn tables(&self) -> impl Iterator<Item = &Self::Table> {
        self.tables.iter()
    }

    fn triggers(&self) -> impl Iterator<Item = &Self::Trigger> {
        self.triggers.iter()
    }

    fn indexes(&self) -> impl Iterator<Item = &Self::Index> {
        self.indexes.iter()
    }

    fn functions(&self) -> impl Iterator<Item = &Self::Function> {
        self.functions.iter()
    }

    fn table(&self, schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        self.tables.iter().find(|table| {
            stored_identifier_matches_lookup(&table.name, table.name_is_quoted, table_name)
                && schema_answers(table.schema.as_deref(), table.schema_is_quoted, schema)
        })
    }

    fn views(&self) -> impl Iterator<Item = &Self::View> {
        self.views.iter()
    }

    fn materialized_views(&self) -> impl Iterator<Item = &Self::MaterializedView> {
        self.materialized_views.iter()
    }

    fn view(&self, schema: Option<&str>, view_name: &str) -> Option<&Self::View> {
        self.views.iter().find(|view| {
            stored_identifier_matches_lookup(&view.name, false, view_name)
                && schema_answers(view.schema.as_deref(), false, schema)
        })
    }

    fn materialized_view(
        &self,
        schema: Option<&str>,
        view_name: &str,
    ) -> Option<&Self::MaterializedView> {
        self.materialized_views.iter().find(|view| {
            stored_identifier_matches_lookup(&view.name, false, view_name)
                && schema_answers(view.schema.as_deref(), false, schema)
        })
    }

    fn table_id(&self, table: &Self::Table) -> Option<usize> {
        self.tables.iter().position(|candidate| candidate == table)
    }

    fn function(&self, name: &str) -> Option<&Self::Function> {
        self.functions
            .iter()
            .find(|function| stored_identifier_matches_lookup(&function.name, false, name))
    }

    fn policies(&self) -> impl Iterator<Item = &Self::Policy> {
        self.policies.iter()
    }

    fn roles(&self) -> impl Iterator<Item = &Self::Role> {
        self.roles.iter()
    }

    fn table_grants(&self) -> impl Iterator<Item = &Self::TableGrant> {
        self.table_grants.iter()
    }

    fn column_grants(&self) -> impl Iterator<Item = &Self::ColumnGrant> {
        self.column_grants.iter()
    }

    fn schemas(&self) -> impl Iterator<Item = &Self::Schema> {
        self.schemas.iter()
    }
}

impl DialectLike for MemoryDialect {
    type DB = MemoryCatalog;
    type Match = TypeMatch;

    fn is_bool(&self, database: &Self::DB, column: &MemoryColumn) -> Self::Match {
        if column.data_type(database).eq_ignore_ascii_case("boolean") {
            TypeMatch::Yes
        } else {
            TypeMatch::No
        }
    }

    fn is_uuid(&self, database: &Self::DB, column: &MemoryColumn) -> Self::Match {
        if column.data_type(database).eq_ignore_ascii_case("uuid") {
            TypeMatch::Yes
        } else {
            TypeMatch::No
        }
    }
}

impl TableLike for MemoryTable {
    type DB = MemoryCatalog;

    fn table_name(&self) -> &str {
        &self.name
    }

    fn table_name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    fn table_schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    fn table_schema_is_quoted(&self) -> bool {
        self.schema_is_quoted
    }

    fn table_doc<'db>(&'db self, _database: &'db Self::DB) -> Result<Option<&'db str>, LookupError>
    where
        Self: 'db,
    {
        Ok(None)
    }

    fn columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError>
    where
        Self: 'db,
    {
        Ok(database
            .columns
            .iter()
            .filter(|column| column.table_name == self.name && column.table_schema == self.schema))
    }

    fn local_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError>
    where
        Self: 'db,
    {
        self.columns(database)
    }

    fn inherits_from<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryTable>, LookupError>
    where
        Self: 'db,
    {
        Ok(core::iter::empty())
    }

    fn partition_root<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<Option<&'db MemoryTable>, LookupError>
    where
        Self: 'db,
    {
        Ok(None)
    }

    fn partition_strategy(&self) -> Option<PartitionStrategy> {
        None
    }

    fn primary_key_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError>
    where
        Self: 'db,
    {
        Ok(self.columns(database)?.filter(|column| column.primary_key))
    }

    fn check_constraints<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryCheckConstraint>, LookupError>
    where
        Self: 'db,
    {
        Ok(database
            .check_constraints
            .iter()
            .filter(|constraint| constraint.table_name == self.name))
    }

    fn indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryIndex>, LookupError>
    where
        Self: 'db,
    {
        Ok(database.indexes.iter().filter(|index| index.table_name == self.name))
    }

    fn unique_indices<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryUniqueIndex>, LookupError>
    where
        Self: 'db,
    {
        Ok(database.unique_indexes.iter().filter(|index| index.0.table_name == self.name))
    }

    fn foreign_keys<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryForeignKey>, LookupError>
    where
        Self: 'db,
    {
        Ok(database.foreign_keys.iter().filter(|key| key.host_table_name == self.name))
    }

    fn has_row_level_security(&self, _database: &Self::DB) -> Result<bool, LookupError> {
        Ok(false)
    }

    fn has_forced_row_level_security(&self, _database: &Self::DB) -> Result<bool, LookupError> {
        Ok(false)
    }

    fn owner<'db>(&self, _database: &'db Self::DB) -> Result<Option<&'db str>, LookupError> {
        Ok(None)
    }
}

impl ColumnLike for MemoryColumn {
    type DB = MemoryCatalog;

    fn column_name(&self) -> &str {
        &self.name
    }

    fn column_name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    fn column_doc<'db>(&'db self, _database: &'db Self::DB) -> Result<Option<&'db str>, LookupError>
    where
        Self: 'db,
    {
        Ok(None)
    }

    fn data_type<'db>(&'db self, _database: &'db Self::DB) -> Cow<'db, str> {
        Cow::Borrowed(&self.data_type)
    }

    fn collation<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<ColumnCollation<'db>, LookupError> {
        Ok(ColumnCollation::DatabaseDefault)
    }

    fn is_generated(&self) -> bool {
        false
    }

    fn is_nullable(&self, _database: &Self::DB) -> Result<bool, LookupError> {
        Ok(self.nullable)
    }

    fn default_value(&self) -> Option<String> {
        None
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db MemoryTable
    where
        Self: 'db,
    {
        database
            .tables
            .iter()
            .find(|table| table.name == self.table_name && table.schema == self.table_schema)
            .expect("every column of this catalog belongs to one of its tables")
    }
}

impl SchemaLike for MemorySchema {
    type DB = MemoryCatalog;

    fn name(&self) -> &str {
        &self.name
    }

    fn name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    fn authorization(&self) -> Option<&str> {
        None
    }
}

impl ViewLike for MemoryView {
    type DB = MemoryCatalog;

    fn view_name(&self) -> &str {
        &self.name
    }

    fn view_schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    fn is_materialized(&self) -> bool {
        self.materialized
    }

    fn definition(&self) -> &Query {
        &self.definition
    }

    fn declared_column_names(&self) -> &[(String, bool)] {
        &self.declared_column_names
    }
}

impl IndexLike for MemoryIndex {
    type DB = MemoryCatalog;

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    fn expression<'db>(&'db self, _database: &'db Self::DB) -> Result<&'db Expr, LookupError>
    where
        Self: 'db,
    {
        Ok(&self.expression)
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db MemoryTable
    where
        Self: 'db,
    {
        database
            .tables
            .iter()
            .find(|table| table.name == self.table_name)
            .expect("every index of this catalog belongs to one of its tables")
    }
}

impl IndexLike for MemoryUniqueIndex {
    type DB = MemoryCatalog;

    fn name(&self) -> Option<&str> {
        self.0.name.as_deref()
    }

    fn schema(&self) -> Option<&str> {
        self.0.schema.as_deref()
    }

    fn expression<'db>(&'db self, database: &'db Self::DB) -> Result<&'db Expr, LookupError>
    where
        Self: 'db,
    {
        self.0.expression(database)
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db MemoryTable
    where
        Self: 'db,
    {
        self.0.table(database)
    }
}

impl ForeignKeyLike for MemoryForeignKey {
    type DB = MemoryCatalog;

    fn foreign_key_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn on_delete_cascade(&self, _database: &Self::DB) -> bool {
        false
    }

    fn host_table<'db>(&'db self, database: &'db Self::DB) -> &'db MemoryTable
    where
        Self: 'db,
    {
        database
            .tables
            .iter()
            .find(|table| table.name == self.host_table_name)
            .expect("every foreign key of this catalog is declared on one of its tables")
    }

    fn referenced_table<'db>(
        &self,
        database: &'db Self::DB,
    ) -> Result<&'db MemoryTable, LookupError> {
        database
            .tables
            .iter()
            .find(|table| table.name == self.referenced_table_name)
            .ok_or(LookupError::TableNotFound { object_name: self.referenced_table_name.clone() })
    }

    fn referenced_table_name(&self) -> TargetName<'_> {
        TargetName::new(&self.referenced_table_name, false)
    }

    fn host_columns<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError>
    where
        Self: 'db,
    {
        Ok(core::iter::empty())
    }

    fn match_kind(&self, _database: &Self::DB) -> ConstraintReferenceMatchKind {
        ConstraintReferenceMatchKind::Simple
    }

    fn referenced_columns<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError>
    where
        Self: 'db,
    {
        Ok(core::iter::empty())
    }
}

impl CheckConstraintLike for MemoryCheckConstraint {
    type DB = MemoryCatalog;

    fn expression<'db>(&'db self, _database: &'db Self::DB) -> &'db Expr {
        &self.expression
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> Result<&'db MemoryTable, LookupError> {
        database
            .tables
            .iter()
            .find(|table| table.name == self.table_name)
            .ok_or(LookupError::TableNotFound { object_name: self.table_name.clone() })
    }

    fn columns<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryColumn>, LookupError> {
        Ok(core::iter::empty())
    }

    fn functions<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryFunction> + 'db, LookupError> {
        Ok(core::iter::empty())
    }
}

impl FunctionLike for MemoryFunction {
    type DB = MemoryCatalog;

    fn name(&self) -> &str {
        &self.name
    }

    fn target_name(&self) -> TargetName<'_> {
        let target = TargetName::new(&self.name, false);
        match self.schema.as_deref() {
            Some(schema) => target.with_schema(schema, false),
            None => target,
        }
    }

    fn argument_type_names<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = Cow<'db, str>> {
        core::iter::empty()
    }

    fn argument_names<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = Option<TargetName<'db>>> {
        core::iter::empty()
    }

    fn return_type_name<'db>(&'db self, _database: &'db Self::DB) -> Option<Cow<'db, str>> {
        None
    }

    fn returns_set(&self) -> bool {
        false
    }

    fn language(&self) -> Option<&str> {
        None
    }

    fn language_is_quoted(&self) -> bool {
        false
    }

    fn body(&self) -> Option<&str> {
        None
    }

    fn body_expression(&self) -> Option<&Expr> {
        None
    }

    fn configuration_parameters(&self) -> &[FunctionDefinitionSetParam] {
        &[]
    }

    fn null_input_behavior(&self) -> FunctionCalledOnNull {
        FunctionCalledOnNull::CalledOnNullInput
    }

    fn security_mode(&self) -> FunctionSecurity {
        FunctionSecurity::Invoker
    }

    fn owner<'db>(&self, _database: &'db Self::DB) -> Result<Option<&'db str>, LookupError> {
        Ok(None)
    }
}

impl TriggerLike for MemoryTrigger {
    type DB = MemoryCatalog;

    fn name(&self) -> &str {
        &self.name
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> Result<&'db MemoryTable, LookupError>
    where
        Self: 'db,
    {
        database
            .tables
            .iter()
            .find(|table| table.name == self.table_name)
            .ok_or(LookupError::TableNotFound { object_name: self.table_name.clone() })
    }

    fn target_table_name(&self) -> TargetName<'_> {
        TargetName::new(&self.table_name, false)
    }

    fn events(&self) -> &[TriggerEvent] {
        &[]
    }

    fn timing(&self) -> Option<TriggerPeriod> {
        None
    }

    fn orientation(&self) -> Option<TriggerObjectKind> {
        None
    }

    fn function<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db MemoryFunction>
    where
        Self: 'db,
    {
        None
    }

    fn function_name(&self) -> Option<&str> {
        None
    }
}

impl PolicyLike for MemoryPolicy {
    type DB = MemoryCatalog;

    fn name(&self) -> &str {
        &self.name
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> Result<&'db MemoryTable, LookupError>
    where
        Self: 'db,
    {
        database
            .tables
            .iter()
            .find(|table| table.name == self.table_name)
            .ok_or(LookupError::TableNotFound { object_name: self.table_name.clone() })
    }

    fn target_table_name(&self) -> TargetName<'_> {
        TargetName::new(&self.table_name, false)
    }

    fn command(&self) -> CreatePolicyCommand {
        CreatePolicyCommand::All
    }

    fn policy_type(&self) -> CreatePolicyType {
        CreatePolicyType::Permissive
    }

    fn roles<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Owner>
    where
        Self: 'db,
    {
        core::iter::empty()
    }

    fn applies_to_public(&self) -> bool {
        true
    }

    fn using_expression<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        None
    }

    fn using_functions<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryFunction>, LookupError> {
        Ok(core::iter::empty())
    }

    fn check_expression<'db>(&'db self, _database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        None
    }

    fn check_functions<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> Result<impl Iterator<Item = &'db MemoryFunction>, LookupError> {
        Ok(core::iter::empty())
    }
}

impl RoleLike for MemoryRole {
    type DB = MemoryCatalog;

    fn name(&self) -> &str {
        &self.name
    }

    fn name_is_quoted(&self) -> bool {
        self.name_is_quoted
    }

    fn is_superuser(&self) -> bool {
        false
    }

    fn can_create_db(&self) -> bool {
        false
    }

    fn can_create_role(&self) -> bool {
        false
    }

    fn inherits(&self) -> bool {
        true
    }

    fn can_login(&self) -> bool {
        false
    }

    fn can_bypass_rls(&self) -> bool {
        false
    }

    fn is_replication(&self) -> bool {
        false
    }

    fn connection_limit(&self) -> Option<i32> {
        None
    }

    fn member_of<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db MemoryRole> {
        core::iter::empty()
    }

    fn policies<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db MemoryPolicy> {
        core::iter::empty()
    }
}

impl GrantLike for MemoryTableGrant {
    type DB = MemoryCatalog;

    fn privileges<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Action>
    where
        Self: 'db,
    {
        core::iter::empty()
    }

    fn is_all_privileges(&self) -> bool {
        true
    }

    fn grantees<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Grantee>
    where
        Self: 'db,
    {
        core::iter::empty()
    }

    fn applies_to_public(&self) -> bool {
        true
    }

    fn target_table_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        core::iter::once(TargetName::new(&self.table_name, false))
    }

    fn target_schema_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        core::iter::empty()
    }

    fn with_grant_option(&self) -> bool {
        false
    }

    fn granted_by<'a>(&'a self, _database: &'a Self::DB) -> Option<&'a MemoryRole> {
        None
    }

    fn applies_to_role(&self, _role: &MemoryRole) -> bool {
        true
    }
}

impl TableGrantLike for MemoryTableGrant {
    fn tables<'a>(&'a self, database: &'a Self::DB) -> impl Iterator<Item = &'a MemoryTable> {
        database.tables.iter().filter(|table| table.name == self.table_name)
    }

    fn relations<'a>(
        &'a self,
        _database: &'a Self::DB,
    ) -> impl Iterator<Item = GrantRelation<'a, Self::DB>> {
        core::iter::empty()
    }

    fn applies_to_table(&self, table: &MemoryTable, _database: &Self::DB) -> bool {
        table.name == self.table_name
    }
}

impl GrantLike for MemoryColumnGrant {
    type DB = MemoryCatalog;

    fn privileges<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Action>
    where
        Self: 'db,
    {
        core::iter::empty()
    }

    fn is_all_privileges(&self) -> bool {
        true
    }

    fn grantees<'db>(&'db self, _database: &'db Self::DB) -> impl Iterator<Item = &'db Grantee>
    where
        Self: 'db,
    {
        core::iter::empty()
    }

    fn applies_to_public(&self) -> bool {
        true
    }

    fn target_table_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        core::iter::once(TargetName::new(&self.table_name, false))
    }

    fn target_schema_names(&self) -> impl Iterator<Item = TargetName<'_>> {
        core::iter::empty()
    }

    fn with_grant_option(&self) -> bool {
        false
    }

    fn granted_by<'a>(&'a self, _database: &'a Self::DB) -> Option<&'a MemoryRole> {
        None
    }

    fn applies_to_role(&self, _role: &MemoryRole) -> bool {
        true
    }
}

impl ColumnGrantLike for MemoryColumnGrant {
    fn columns<'a>(
        &'a self,
        table: &'a MemoryTable,
        database: &'a Self::DB,
    ) -> Result<impl Iterator<Item = &'a MemoryColumn>, LookupError> {
        table.columns(database)
    }

    fn table<'a>(&'a self, database: &'a Self::DB) -> Option<&'a MemoryTable> {
        database.tables.iter().find(|table| table.name == self.table_name)
    }

    fn relation<'a>(&'a self, _database: &'a Self::DB) -> Option<GrantRelation<'a, Self::DB>> {
        None
    }
}

fn table(schema: Option<&str>, name: &str) -> MemoryTable {
    MemoryTable {
        schema: schema.map(String::from),
        schema_is_quoted: false,
        name: String::from(name),
        name_is_quoted: false,
    }
}

fn column(table_schema: Option<&str>, table_name: &str, name: &str) -> MemoryColumn {
    MemoryColumn {
        table_schema: table_schema.map(String::from),
        table_name: String::from(table_name),
        name: String::from(name),
        name_is_quoted: false,
        data_type: String::from("INT"),
        nullable: false,
        primary_key: name == "id",
    }
}

/// Two tables named the same in two schemas, one of them the default schema
/// the inherited resolver walks.
fn catalog() -> MemoryCatalog {
    MemoryCatalog {
        catalog_name: String::from("memory"),
        tables: vec![table(None, "docs"), table(Some("app"), "docs"), table(Some("app"), "notes")],
        columns: vec![
            column(None, "docs", "id"),
            column(Some("app"), "docs", "id"),
            column(Some("app"), "docs", "body"),
            column(Some("app"), "notes", "id"),
        ],
        schemas: vec![MemorySchema { name: String::from("app"), name_is_quoted: false }],
        roles: vec![MemoryRole { name: String::from("reader"), name_is_quoted: false }],
        ..MemoryCatalog::default()
    }
}

/// The inherited resolver reads an unqualified name through the default schema
/// and a qualified one exactly, which is the body `GenericDB` replaces with an
/// indexed one.
#[test]
fn the_inherited_resolver_walks_the_default_schema() -> Result<(), LookupError> {
    let catalog = catalog();

    let bare = catalog
        .resolve_target_table(TargetName::new("docs", false))?
        .expect("the default schema holds a table of that name");
    assert_eq!(bare.table_schema(), None);

    let qualified = catalog
        .resolve_target_table(TargetName::new("docs", false).with_schema("app", false))?
        .expect("the qualified name resolves in its own schema");
    assert_eq!(qualified.table_schema(), Some("app"));

    // `notes` sits in a schema the path does not carry, so an unqualified
    // reference to it resolves to nothing.
    assert!(catalog.resolve_target_table(TargetName::new("notes", false))?.is_none());
    assert!(catalog.resolve_target_table(TargetName::new("absent", false))?.is_none());

    Ok(())
}

/// The inherited identity lookup compares both parts as stored, so it neither
/// folds a name nor reads the absent schema as `public`, unlike the written
/// lookup right beside it.
#[test]
fn the_inherited_identity_lookup_compares_stored_parts() {
    let catalog = catalog();

    let bare = catalog
        .table_by_stored_identity(None, "docs")
        .expect("one table is stored without a schema");
    assert_eq!(bare.table_schema(), None);

    let qualified =
        catalog.table_by_stored_identity(Some("app"), "docs").expect("one is stored in `app`");
    assert_eq!(qualified.table_schema(), Some("app"));

    assert!(catalog.table_by_stored_identity(Some("public"), "docs").is_none());
    assert!(catalog.table_by_stored_identity(None, "Docs").is_none());
    assert!(catalog.table_by_stored_identity(Some("audit"), "docs").is_none());

    // The written lookup keeps folding and keeps reading both spellings of the
    // default schema as one place.
    assert!(catalog.table(Some("public"), "docs").is_some());
    assert!(catalog.table(None, "DOCS").is_some());
}

/// The inherited view resolvers answer nothing for a catalog holding no views,
/// rather than reaching for a table of that name.
#[test]
fn the_inherited_view_resolvers_answer_only_views() -> Result<(), LookupError> {
    let catalog = catalog();

    assert!(catalog.resolve_target_view(TargetName::new("docs", false))?.is_none());
    assert!(catalog.resolve_target_materialized_view(TargetName::new("docs", false))?.is_none());
    assert!(catalog.views().next().is_none());
    assert!(catalog.materialized_views().next().is_none());

    Ok(())
}

/// The rest of what a catalog inherits, answered without a parser in sight.
#[test]
fn the_inherited_accessors_answer_from_the_catalog() -> Result<(), LookupError> {
    let catalog = catalog();

    assert!(catalog.has_tables());
    assert_eq!(catalog.number_of_tables(), 3);
    assert_eq!(catalog.table_by_id(0).map(TableLike::table_name), Some("docs"));
    assert_eq!(catalog.maximum_number_of_columns()?, 2);
    assert!(catalog.schema("app").is_some());
    assert!(catalog.schema("\"App\"").is_none());
    assert!(catalog.role("reader").is_some());
    assert!(!catalog.has_policies());
    assert!(!catalog.has_table_grants());
    assert!(!catalog.has_column_grants());
    assert!(catalog.has_schemas());
    assert!(catalog.has_roles());
    assert!(!catalog.has_rls_tables()?);
    assert_eq!(catalog.number_of_rls_tables()?, 0);
    assert_eq!(catalog.table_dag().expect("the catalog has no cycles").len(), 3);
    // No table extends another here, and a table outside every extension
    // relationship is not a root.
    assert_eq!(catalog.root_tables()?.count(), 0);

    let docs = catalog.table(Some("app"), "docs").expect("the qualified lookup finds it");
    assert_eq!(docs.columns(&catalog)?.count(), 2);
    assert_eq!(docs.primary_key_columns(&catalog)?.count(), 1);
    let first_column = docs.columns(&catalog)?.next().expect("a column");
    assert_eq!(catalog.dialect().is_bool(&catalog, first_column), TypeMatch::No);

    Ok(())
}
