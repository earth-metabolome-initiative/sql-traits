//! Error enumeration used in the `sql_traits` crate.

use alloc::{string::String, vec::Vec};

use sqlparser::parser::ParserError;

/// Kind of database object a metadata lookup was made for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    /// A base table.
    Table,
    /// An index declared by a `CREATE INDEX` statement.
    Index,
    /// A unique constraint declared inside a `CREATE TABLE` statement.
    UniqueIndex,
    /// A `CHECK` constraint declared inside a `CREATE TABLE` statement.
    CheckConstraint,
    /// A row level security policy.
    Policy,
}

impl core::fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Table => "Table",
            Self::Index => "Index",
            Self::UniqueIndex => "Unique index",
            Self::CheckConstraint => "Check constraint",
            Self::Policy => "Policy",
        })
    }
}

impl ObjectKind {
    /// Reports an object of this kind that the queried database does not hold,
    /// identified by its own `name`.
    #[must_use]
    pub fn not_in_database(self, name: &str) -> LookupError {
        LookupError::ObjectNotInDatabase { object_kind: self, object: format!("`{name}`") }
    }

    /// Reports an anonymous object of this kind that the queried database does
    /// not hold.
    ///
    /// A `CHECK` or `UNIQUE` constraint written without a name has no identity
    /// of its own, so it is reported by the table it is declared on.
    #[must_use]
    pub fn anonymous_not_in_database(self, table_name: &str) -> LookupError {
        LookupError::ObjectNotInDatabase {
            object_kind: self,
            object: format!("declared on table `{table_name}`"),
        }
    }
}

/// Errors produced by identifier-aware lookup and resolution APIs.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LookupError {
    /// The provided SQL object name is not supported by the resolver.
    #[error("Invalid object name `{object_name}`: {reason}")]
    InvalidObjectName {
        /// Original object name as rendered by sqlparser.
        object_name: String,
        /// Human-readable reason describing why the object name is invalid.
        reason: String,
    },
    /// Table resolution matched multiple candidates.
    #[error("Ambiguous table lookup `{object_name}`; candidates: {candidates:?}")]
    AmbiguousTableLookup {
        /// Lookup object name as rendered by sqlparser.
        object_name: String,
        /// Deterministically ordered list of matching candidates.
        candidates: Vec<String>,
    },
    /// Table resolution found no matching table for a name that is required to
    /// denote one.
    #[error("Table `{object_name}` not found.")]
    TableNotFound {
        /// Lookup object name as rendered by sqlparser.
        object_name: String,
    },
    /// Adding a table would create semantic lookup ambiguity.
    #[error(
        "Cannot add table `{table}` because it conflicts with existing table `{conflicting_table}`."
    )]
    TableLookupConflict {
        /// Table being inserted.
        table: String,
        /// Existing conflicting table.
        conflicting_table: String,
    },
    /// A database object handed to a metadata accessor is not present in the
    /// database being queried, for instance because it was renamed away, was
    /// dropped, or came from a different database.
    #[error("{object_kind} {object} is not present in the database being queried.")]
    ObjectNotInDatabase {
        /// Kind of object that was looked up.
        object_kind: ObjectKind,
        /// Best-effort rendering identifying the object: its own quoted name,
        /// or the table it is declared on when it has no name of its
        /// own. Build it with [`ObjectKind::not_in_database`] or
        /// [`ObjectKind::anonymous_not_in_database`] rather than by hand.
        object: String,
    },
    /// A column named by a constraint is not declared by the table that
    /// constraint resolves it against.
    #[error("Column `{column_name}` is not declared by table `{table_name}`.")]
    ColumnNotFound {
        /// Name of the table the column was looked up in.
        table_name: String,
        /// Name of the column that was not found.
        column_name: String,
    },
}

#[derive(Debug, thiserror::Error)]
/// Defines the `Error` enum representing various error types
pub enum Error {
    /// Wrapper around identifier-aware lookup errors.
    #[error(transparent)]
    IdentifierLookupError(#[from] LookupError),
    #[error("Unknown column `{column_name}` in table `{table_name}`.")]
    /// A check constraint contained columns which do not exist in the table.
    UnknownColumnInCheckConstraint {
        /// Name of the unknown column.
        column_name: String,
        /// Name of the table the check constraint belongs to.
        table_name: String,
    },
    #[error("Invalid primary key in table `{table_name}`: {reason}")]
    /// A primary key constraint referenced something other than a plain column,
    /// such as an expression like `PRIMARY KEY (a - b)`.
    InvalidPrimaryKey {
        /// Name of the table the primary key belongs to.
        table_name: String,
        /// Human-readable reason describing why the primary key is invalid.
        reason: String,
    },
    #[error(
        "Referenced table `{referenced_table}` not found for foreign key in table `{host_table}`."
    )]
    /// Error indicating that a foreign key references a table that does not
    /// exist.
    ReferencedTableNotFoundForForeignKey {
        /// Name of the referenced table.
        referenced_table: String,
        /// Name of the host table containing the foreign key.
        host_table: String,
    },
    #[error(
        "Referenced column `{referenced_column}` not found in table `{referenced_table}` for foreign key in table `{host_table}`."
    )]
    /// Error indicating that a foreign key references a column that does not
    /// exist.
    ReferencedColumnNotFoundForForeignKey {
        /// Name of the referenced column.
        referenced_column: String,
        /// Name of the referenced table.
        referenced_table: String,
        /// Name of the host table containing the foreign key.
        host_table: String,
    },
    #[error(
        "No unique constraint on table `{referenced_table}` matches the columns `{referenced_columns}` a foreign key in table `{host_table}` points at."
    )]
    /// Error indicating that a foreign key points at columns of the referenced
    /// table that no primary key, unique constraint or unique index covers.
    ///
    /// PostgreSQL, MySQL and SQLite all refuse such a constraint, since without
    /// a unique key on the far side a row could match more than one parent.
    ReferencedColumnsNotUniqueForForeignKey {
        /// Comma-separated names of the referenced columns.
        referenced_columns: String,
        /// Name of the referenced table.
        referenced_table: String,
        /// Name of the host table containing the foreign key.
        host_table: String,
    },
    #[error("Host column `{host_column}` not found in table `{host_table}` for foreign key.")]
    /// Error indicating that a foreign key references a host column that does
    /// not exist.
    HostColumnNotFoundForForeignKey {
        /// Name of the host column.
        host_column: String,
        /// Name of the host table containing the foreign key.
        host_table: String,
    },
    #[error("Table `{table_name}` not found for trigger `{trigger_name}`.")]
    /// Error indicating that a trigger references a table that does not exist.
    TableNotFoundForTrigger {
        /// Name of the table the trigger belongs to.
        table_name: String,
        /// Name of the trigger.
        trigger_name: String,
    },
    #[error("Table `{table_name}` not found for policy `{policy_name}`.")]
    /// Error indicating that a policy names a table that does not exist.
    ///
    /// A policy exists only on its table, so the database refuses one whose
    /// table is absent, and so does this crate.
    TableNotFoundForPolicy {
        /// Name of the table the policy is declared on.
        table_name: String,
        /// Name of the policy.
        policy_name: String,
    },
    #[error("Role `{role_name}` not found for the owner of `{object_name}`.")]
    /// Error indicating that an ownership statement names a role that does not
    /// exist.
    ///
    /// Covers `ALTER TABLE ... OWNER TO`, `ALTER SCHEMA ... OWNER TO` and
    /// `CREATE SCHEMA ... AUTHORIZATION`, all of which the database refuses
    /// when the role is absent. Like the other role checks this one follows
    /// [`AccessResolution`](crate::structs::AccessResolution), because a schema
    /// dump names an owner while creating no role.
    RoleNotFoundForOwner {
        /// Name of the role named as the owner.
        role_name: String,
        /// Name of the object being owned.
        object_name: String,
    },
    #[error("Column `{column_name}` not found in table `{table_name}` for a grant or revoke.")]
    /// Error indicating that a column-level grant or revoke names a column the
    /// table it applies to does not have.
    ///
    /// The statement may name several tables, and the database requires the
    /// column on each of them, so this names the table that lacks it.
    ColumnNotFoundForGrant {
        /// Name of the column the grant names.
        column_name: String,
        /// Name of the table that does not have it.
        table_name: String,
    },
    #[error("Table `{table_name}` not found for index `{index_name}`.")]
    /// Error indicating that an index references a table that does not exist.
    TableNotFoundForIndex {
        /// Name of the table the index belongs to.
        table_name: String,
        /// Name of the index.
        index_name: String,
    },
    #[error("Invalid index `{index_name}`: {reason}")]
    /// Error indicating that an index definition is invalid.
    InvalidIndex {
        /// Name of the invalid index.
        index_name: String,
        /// Reason why the index is invalid.
        reason: String,
    },
    #[error("Function `{function_name}` not found for trigger `{trigger_name}`.")]
    /// Error indicating that a trigger references a function that does not
    /// exist.
    FunctionNotFoundForTrigger {
        /// Name of the function the trigger executes.
        function_name: String,
        /// Name of the trigger.
        trigger_name: String,
    },
    /// Wrapper around SQL parser errors.
    #[cfg_attr(feature = "std", error("SQL parser error: {error} in {file:?}"))]
    #[cfg_attr(not(feature = "std"), error("SQL parser error: {error}"))]
    SqlParserError {
        /// The error from the SQL parser.
        #[source]
        error: ParserError,
        /// The file containing the offending code (only carried under the `std`
        /// feature; `no_std` consumers receive `None`).
        #[cfg(feature = "std")]
        file: Option<std::path::PathBuf>,
    },
    /// Wrapper around git errors. Only available with the `git` feature.
    #[cfg(feature = "git")]
    #[error("Git error: {0}")]
    GitError(#[from] git2::Error),
    /// Wrapper around IO errors. Only available with the `std` feature.
    #[cfg(feature = "std")]
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    /// Wrapper around sql_doc errors
    #[error("Table Doc Error: {0}")]
    TableDocError(#[from] sql_docs::error::DocError),
    /// Error indicating that a REVOKE statement uses semantics we cannot
    /// represent in the current grant model.
    #[error("Unsupported revoke statement `{statement}`: {reason}")]
    UnsupportedRevoke {
        /// The original REVOKE statement rendered by sqlparser.
        statement: String,
        /// Human-readable explanation of the unsupported semantics.
        reason: String,
    },
    #[error("Role `{role_name}` not found for a grant or revoke.")]
    /// Error indicating that a grant or a revoke names a role that does not
    /// exist.
    RoleNotFoundForGrant {
        /// Name of the undefined role.
        role_name: String,
    },
    #[error("Table `{table_name}` not found for a grant or revoke.")]
    /// Error indicating that a grant or a revoke names a table that does not
    /// exist.
    TableNotFoundForGrant {
        /// Name of the undefined table.
        table_name: String,
    },
    #[error("Function `{function_name}` not found for DROP FUNCTION statement.")]
    /// Error indicating that a DROP FUNCTION statement references a function
    /// that does not exist.
    DropFunctionNotFound {
        /// Name of the function that was not found.
        function_name: String,
    },
    #[error("Cannot drop function `{function_name}`: still referenced in the schema.")]
    /// Error indicating that a DROP FUNCTION statement references a function
    /// that is still used by other schema objects (check constraints, policies,
    /// or triggers).
    FunctionReferenced {
        /// Name of the function being dropped.
        function_name: String,
    },
    #[error("Table `{table_name}` not found for DROP TABLE statement.")]
    /// Error indicating that a DROP TABLE statement references a table
    /// that does not exist.
    DropTableNotFound {
        /// Name of the table that was not found.
        table_name: String,
    },
    #[error("Cannot drop table `{table_name}`: still referenced in the schema.")]
    /// Error indicating that a DROP TABLE statement references a table
    /// that is still referenced by foreign keys from other tables.
    TableReferenced {
        /// Name of the table being dropped.
        table_name: String,
    },
    #[error(
        "Cannot drop column `{column_name}` of table `{table_name}`: still referenced from outside the table."
    )]
    /// Error indicating that an `ALTER TABLE ... DROP COLUMN` statement names a
    /// column something outside its table depends on, without saying `CASCADE`.
    ///
    /// Indexes and constraints on the table itself are dropped along with the
    /// column and never raise this.
    ColumnReferenced {
        /// Name of the table the column belongs to.
        table_name: String,
        /// Name of the column being dropped.
        column_name: String,
    },
    #[error("Column `{column_name}` already exists on table `{table_name}`.")]
    /// Error indicating that a statement tries to introduce a column name the
    /// table already declares.
    ColumnAlreadyExists {
        /// Name of the table the column belongs to.
        table_name: String,
        /// Name of the column that already exists.
        column_name: String,
    },
    #[error(
        "Unsupported `ALTER TABLE` operation on `{table_name}`: `{operation}` changes something this model represents but does not yet apply."
    )]
    /// Error indicating that an `ALTER TABLE` operation would change part of
    /// the schema the model represents, and is not yet applied.
    ///
    /// Operations that change something the model represents at all are either
    /// applied or reported here, so nothing that would leave the model wrong is
    /// silently discarded.
    UnsupportedAlterTableOperation {
        /// Name of the table the statement targets.
        table_name: String,
        /// The operation rendered by sqlparser.
        operation: String,
    },
    #[error("Schema `{schema_name}` not found for table `{table_name}`.")]
    /// Error indicating that a `CREATE TABLE` statement qualifies its name with
    /// a schema no `CREATE SCHEMA` in the input creates.
    ///
    /// The default schema is exempt, since no dump emits a statement creating
    /// it.
    SchemaNotFoundForTable {
        /// Name of the schema that was not found.
        schema_name: String,
        /// Name of the table qualified with it.
        table_name: String,
    },
    #[error("Role `{role_name}` not found for policy `{policy_name}`.")]
    /// Error indicating that a `CREATE POLICY` statement applies to a role no
    /// `CREATE ROLE` in the input creates.
    ///
    /// Reported under [`crate::structs::AccessResolution::ClosedWorld`], the
    /// same setting that governs a grant naming an absent role, because a dump
    /// of a schema omits role creation either way.
    RoleNotFoundForPolicy {
        /// Name of the role that was not found.
        role_name: String,
        /// Name of the policy applying to it.
        policy_name: String,
    },
    #[error("Index `{index_name}` not found for DROP INDEX statement.")]
    /// Error indicating that a DROP INDEX statement references an index
    /// that does not exist.
    DropIndexNotFound {
        /// Name of the index that was not found.
        index_name: String,
    },
    #[error("Trigger `{trigger_name}` not found for DROP TRIGGER statement.")]
    /// Error indicating that a DROP TRIGGER statement references a trigger
    /// that does not exist.
    DropTriggerNotFound {
        /// Name of the trigger that was not found.
        trigger_name: String,
    },
    #[error("Policy `{policy_name}` not found for DROP POLICY statement.")]
    /// Error indicating that a DROP POLICY statement references a policy
    /// that does not exist.
    DropPolicyNotFound {
        /// Name of the policy that was not found.
        policy_name: String,
    },
    #[error("Role `{role_name}` not found for DROP ROLE statement.")]
    /// Error indicating that a DROP ROLE statement references a role
    /// that does not exist.
    DropRoleNotFound {
        /// Name of the role that was not found.
        role_name: String,
    },
    #[error("Cannot drop role `{role_name}`: still referenced by grants.")]
    /// Error indicating that a DROP ROLE statement references a role
    /// that is still used as a grantee in existing grants.
    RoleReferenced {
        /// Name of the role being dropped.
        role_name: String,
    },
    #[error("Schema `{schema_name}` already exists.")]
    /// Error indicating that a CREATE SCHEMA statement tries to create a schema
    /// that already exists.
    SchemaAlreadyExists {
        /// Name of the schema that already exists.
        schema_name: String,
    },
    #[error(
        "{object_kind} `{object_name}` cannot be created: {conflicting_kind} `{object_name}` already uses that name in the same schema."
    )]
    /// Error indicating that a statement names an index or a table with a name
    /// something else in the same schema already holds.
    ///
    /// PostgreSQL keeps index names in one pool per schema, shared with table
    /// names, and a named `UNIQUE` or `PRIMARY KEY` constraint puts the name of
    /// the index behind it in that pool too. Views and sequences share the pool
    /// as well, and this crate models neither, so the rule it enforces is the
    /// part of PostgreSQL's it can see.
    RelationNameAlreadyTaken {
        /// Kind of object the statement tried to create.
        object_kind: ObjectKind,
        /// Kind of object already holding the name.
        conflicting_kind: ObjectKind,
        /// The contested name.
        object_name: String,
    },
    #[error("Policy `{policy_name}` already exists on table `{table_name}`.")]
    /// Error indicating that a `CREATE POLICY` statement names a policy the
    /// table already carries. A policy name is unique per table, whatever
    /// command it is declared `FOR`.
    PolicyAlreadyExists {
        /// Name of the policy that already exists.
        policy_name: String,
        /// Name of the table carrying it.
        table_name: String,
    },
    #[error("Trigger `{trigger_name}` already exists on table `{table_name}`.")]
    /// Error indicating that a `CREATE TRIGGER` statement names a trigger the
    /// table already carries. A trigger name is unique per table, so the same
    /// name on another table is fine.
    TriggerAlreadyExists {
        /// Name of the trigger that already exists.
        trigger_name: String,
        /// Name of the table carrying it.
        table_name: String,
    },
    #[error("Role `{role_name}` already exists.")]
    /// Error indicating that a `CREATE ROLE` statement names a role that
    /// already exists.
    ///
    /// Unlike the checks on a role a grant, a policy or an ownership statement
    /// *names*, this one is not governed by
    /// [`AccessResolution`](crate::structs::AccessResolution): that setting
    /// excuses a dump that omits role creation, and this statement is the
    /// creation.
    RoleAlreadyExists {
        /// Name of the role that already exists.
        role_name: String,
    },
    #[error("Function `{function_name}` already exists with the same argument types.")]
    /// Error indicating that a `CREATE FUNCTION` statement repeats a signature.
    ///
    /// A function is identified by its schema, its name and its argument types
    /// with `OUT` parameters removed, so two functions may share a name as long
    /// as they take different arguments. The return type is not part of the
    /// identity. `CREATE OR REPLACE` replaces the existing function instead.
    FunctionAlreadyExists {
        /// Name of the function whose signature is repeated.
        function_name: String,
    },
    #[error(
        "Function name `{function_name}` is not unique: a `DROP FUNCTION` naming no argument list cannot say which one to drop."
    )]
    /// Error indicating that a `DROP FUNCTION` statement omits the argument
    /// list while the name it gives covers more than one function.
    AmbiguousDropFunction {
        /// The name covering more than one function.
        function_name: String,
    },
    #[error("Schema `{schema_name}` not found for DROP SCHEMA statement.")]
    /// Error indicating that a DROP SCHEMA statement references a schema
    /// that does not exist.
    DropSchemaNotFound {
        /// Name of the schema that was not found.
        schema_name: String,
    },
    #[error("Cannot drop schema `{schema_name}`: still contains objects.")]
    /// Error indicating that a DROP SCHEMA statement references a schema
    /// that still contains objects (tables, functions, etc.).
    SchemaNotEmpty {
        /// Name of the schema being dropped.
        schema_name: String,
    },
    #[error("Table `{table_name}` not found for RENAME TABLE statement.")]
    /// Error indicating that a RENAME TABLE statement references a table
    /// that does not exist.
    RenameTableNotFound {
        /// Name of the table that was not found.
        table_name: String,
    },
    #[error("Policy `{policy_name}` not found for ALTER POLICY statement.")]
    /// Error indicating that an ALTER POLICY statement references a policy
    /// that does not exist.
    AlterPolicyNotFound {
        /// Name of the policy that was not found.
        policy_name: String,
    },
    #[error("Index `{index_name}` not found for ALTER INDEX statement.")]
    /// Error indicating that an ALTER INDEX statement references an index that
    /// does not exist.
    AlterIndexNotFound {
        /// Name of the index that was not found.
        index_name: String,
    },
    #[error("Role `{role_name}` not found for ALTER ROLE statement.")]
    /// Error indicating that an ALTER ROLE statement references a role that
    /// does not exist.
    AlterRoleNotFound {
        /// Name of the role that was not found.
        role_name: String,
    },
    #[error("Table `{table_name}` not found for ALTER TABLE statement.")]
    /// Error indicating that an ALTER TABLE statement references a table that
    /// does not exist.
    AlterTableNotFound {
        /// Name of the table that was not found.
        table_name: String,
    },
    #[error(
        "Constraint `{constraint_name}` not found on table `{table_name}` for ALTER TABLE DROP CONSTRAINT statement."
    )]
    /// Error indicating that an `ALTER TABLE ... DROP CONSTRAINT` statement
    /// names a constraint the table does not declare. Constraints declared
    /// without a name have no name to drop them by.
    DropConstraintNotFound {
        /// Name of the table the constraint was looked up on.
        table_name: String,
        /// Name of the constraint that was not found.
        constraint_name: String,
    },
    #[error("Schema `{schema_name}` not found for ALTER SCHEMA statement.")]
    /// Error indicating that an ALTER SCHEMA statement references a schema
    /// that does not exist.
    AlterSchemaNotFound {
        /// Name of the schema that was not found.
        schema_name: String,
    },
    #[error("Failed to build the table dependency graph of database `{catalog_name}`: {reason}")]
    /// Error indicating that the foreign key graph of a database could not be
    /// assembled.
    TableDependencyGraph {
        /// Name of the database whose graph could not be assembled.
        catalog_name: String,
        /// Human-readable reason describing the failure.
        reason: String,
    },
    #[error("The tables of database `{catalog_name}` form a foreign key cycle.")]
    /// Error indicating that the foreign keys of a database form a cycle, so
    /// its tables have no topological order.
    CyclicTableDependencies {
        /// Name of the database whose tables form a cycle.
        catalog_name: String,
    },
}

impl From<ParserError> for Error {
    fn from(error: ParserError) -> Self {
        Error::SqlParserError {
            error,
            #[cfg(feature = "std")]
            file: None,
        }
    }
}
