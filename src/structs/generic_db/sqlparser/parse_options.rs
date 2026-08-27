//! Options controlling how SQL text is turned into a [`ParserDB`].

use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::path::Path;

use sqlparser::{ast::Statement, dialect::Dialect};

use crate::{
    errors::Error,
    impls::SqlparserDialect,
    structs::{ParserDB, PostgresCatalog},
};

/// How an access control statement is resolved against the objects the parsed
/// input creates.
///
/// Governs `GRANT`, `REVOKE` and `CREATE POLICY`, all of which name a role, and
/// which a schema dump leaves unresolved for the same reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum AccessResolution {
    /// Every role and every access target must be created by the same input.
    ///
    /// A grant or a revoke naming an absent role or table, and a policy
    /// applying to an absent role, each abort the parse. This is the default.
    #[default]
    ClosedWorld,
    /// Grants, revokes and policies may name roles, and grants and revokes may
    /// name tables, that the input does not create.
    ///
    /// Roles are cluster objects that `pg_dump` does not emit, so a dump of a
    /// schema carrying a single grant or policy has no `CREATE ROLE` to resolve
    /// against. Under this setting the statement is recorded as written. Ask
    /// [`ParserDB::unresolved_access_references`] what failed to resolve, or
    /// [`ParserDB::validate_access_targets`] to enforce closure once the whole
    /// input is in.
    OpenWorld,
}

/// Configuration for the [`ParserDB`] constructors.
///
/// The defaults reproduce the plain [`ParserDB::parse`] behaviour, so an
/// option only ever has to be named to depart from it.
///
/// # Example
///
/// ```rust
/// use sql_traits::prelude::*;
/// use sqlparser::dialect::PostgreSqlDialect;
///
/// // `app` is provisioned outside this schema, so nothing creates it here.
/// let sql = "CREATE TABLE docs (id uuid PRIMARY KEY);
///            GRANT SELECT ON docs TO app;";
///
/// assert!(ParserDB::parse::<PostgreSqlDialect>(sql).is_err());
///
/// let db = ParseOptions::default()
///     .with_access_resolution(AccessResolution::OpenWorld)
///     .parse::<PostgreSqlDialect>(sql)?;
/// assert_eq!(db.table_grants().count(), 1);
/// # Ok::<(), sql_traits::errors::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParseOptions {
    access_resolution: AccessResolution,
    postgres_catalog: PostgresCatalog,
}

impl Default for ParseOptions {
    fn default() -> Self {
        Self {
            access_resolution: AccessResolution::ClosedWorld,
            postgres_catalog: PostgresCatalog::default(),
        }
    }
}

impl ParseOptions {
    /// Sets how grants resolve against the objects the input creates.
    #[must_use]
    #[inline]
    pub fn with_access_resolution(mut self, access_resolution: AccessResolution) -> Self {
        self.access_resolution = access_resolution;
        self
    }

    /// Sets the PostgreSQL catalog used for collation and type validation.
    #[must_use]
    #[inline]
    pub fn with_postgres_catalog(mut self, postgres_catalog: PostgresCatalog) -> Self {
        self.postgres_catalog = postgres_catalog;
        self
    }

    /// Returns how grants resolve against the objects the input creates.
    #[must_use]
    #[inline]
    pub const fn access_resolution(&self) -> AccessResolution {
        self.access_resolution
    }

    /// Returns the PostgreSQL catalog used for collation and type validation.
    #[must_use]
    #[inline]
    pub const fn postgres_catalog(&self) -> &PostgresCatalog {
        &self.postgres_catalog
    }

    /// Parses SQL under these options using the specified dialect.
    ///
    /// See [`ParserDB::parse`], which this method configures.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be parsed or if there are validation
    /// errors.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// // `pg_dump` emits a revoke like this for a function whose default
    /// // execute privilege was withdrawn, naming a role it never creates.
    /// let sql = "CREATE FUNCTION f() RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql;
    ///            REVOKE ALL ON FUNCTION f() FROM app;";
    ///
    /// assert!(ParserDB::parse::<PostgreSqlDialect>(sql).is_err());
    ///
    /// let db = ParseOptions::default()
    ///     .with_access_resolution(AccessResolution::OpenWorld)
    ///     .parse::<PostgreSqlDialect>(sql)?;
    /// // Nothing granted the privilege here, so subtracting it changes nothing.
    /// assert_eq!(db.table_grants().count(), 0);
    /// # Ok::<(), sql_traits::errors::Error>(())
    /// ```
    pub fn parse<D: Dialect + Default + 'static>(self, sql: &str) -> Result<ParserDB, Error> {
        ParserDB::parse_with_options::<D>(sql, &self)
    }

    /// Builds a [`ParserDB`] from already parsed statements under these
    /// options.
    ///
    /// See [`ParserDB::from_statements`], which this method configures.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
    ///
    /// let statements = Parser::parse_sql(
    ///     &PostgreSqlDialect {},
    ///     "CREATE TABLE docs (id uuid PRIMARY KEY); GRANT SELECT ON docs TO app;",
    /// )?;
    ///
    /// assert!(ParserDB::from_statements(statements.clone(), "docs".to_string()).is_err());
    ///
    /// let db = ParseOptions::default()
    ///     .with_access_resolution(AccessResolution::OpenWorld)
    ///     .from_statements(statements, "docs".to_string())?;
    /// assert_eq!(db.table_grants().count(), 1);
    /// # Ok::<(), sql_traits::errors::Error>(())
    /// ```
    pub fn from_statements(
        self,
        statements: Vec<Statement>,
        catalog_name: String,
    ) -> Result<ParserDB, Error> {
        ParserDB::from_statements_with_options(
            statements,
            catalog_name,
            SqlparserDialect::default(),
            &self,
        )
    }

    /// Builds a [`ParserDB`] from parsed statements with an explicit dialect.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn from_statements_with_dialect(
        self,
        statements: Vec<Statement>,
        catalog_name: String,
        dialect: SqlparserDialect,
    ) -> Result<ParserDB, Error> {
        ParserDB::from_statements_with_options(statements, catalog_name, dialect, &self)
    }

    /// Parses SQL from a file or directory path under these options.
    ///
    /// See [`ParserDB::from_path`], which this method configures.
    ///
    /// # Errors
    ///
    /// Returns an error if the path doesn't exist, files can't be read, or
    /// parsing fails.
    #[cfg(feature = "std")]
    pub fn from_path<D: Dialect + Default>(self, path: &Path) -> Result<ParserDB, Error> {
        self.from_paths::<D>(&[path])
    }

    /// Parses SQL from multiple file or directory paths under these options.
    ///
    /// See [`ParserDB::from_paths`], which this method configures.
    ///
    /// # Errors
    ///
    /// Returns an error if any path doesn't exist, files can't be read, or
    /// parsing fails.
    #[cfg(feature = "std")]
    pub fn from_paths<D: Dialect + Default>(self, paths: &[&Path]) -> Result<ParserDB, Error> {
        ParserDB::from_paths_with_options::<D>(paths, &self)
    }
}
