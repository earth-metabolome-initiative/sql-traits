//! Generic database schema representations and utilities.

mod builder;
mod database;
mod sqlparser;

use std::{fmt::Debug, sync::Arc};

pub use builder::GenericDBBuilder;
pub use sqlparser::{ParserDB, ParserDBBuilder};

use crate::traits::{
    CheckConstraintLike, ColumnGrantLike, ColumnLike, ForeignKeyLike, FunctionLike, IndexLike,
    PolicyLike, RoleLike, SchemaLike, TableGrantLike, TableLike, TriggerLike, UniqueIndexLike,
};

/// A generic representation of a database schema.
pub struct GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG>
where
    T: TableLike,
    C: ColumnLike,
    I: IndexLike,
    U: UniqueIndexLike,
    F: ForeignKeyLike,
    Func: FunctionLike,
    Ch: CheckConstraintLike,
    Tr: TriggerLike,
    P: PolicyLike,
    R: RoleLike,
    S: SchemaLike,
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    /// Catalog name of the database.
    catalog_name: String,
    /// Timezone of the database.
    timezone: Option<String>,
    /// List of tables in the database.
    tables: Vec<(Arc<T>, T::Meta)>,
    /// List of columns in the database.
    columns: Vec<(Arc<C>, C::Meta)>,
    /// List of indices in the database.
    indices: Vec<(Arc<I>, I::Meta)>,
    /// List of unique indices in the database.
    unique_indices: Vec<(Arc<U>, U::Meta)>,
    /// List of foreign keys in the database.
    foreign_keys: Vec<(Arc<F>, F::Meta)>,
    /// List of functions created in the database.
    functions: Vec<(Arc<Func>, Func::Meta)>,
    /// List of triggers created in the database.
    triggers: Vec<(Arc<Tr>, Tr::Meta)>,
    /// List of policies created in the database.
    policies: Vec<(Arc<P>, P::Meta)>,
    /// List of check constraints in the database.
    check_constraints: Vec<(Arc<Ch>, Ch::Meta)>,
    /// List of roles in the database.
    roles: Vec<(Arc<R>, R::Meta)>,
    /// List of table grants in the database.
    table_grants: Vec<(Arc<TG>, TG::Meta)>,
    /// List of column grants in the database.
    column_grants: Vec<(Arc<CG>, CG::Meta)>,
    /// List of schemas in the database.
    schemas: Vec<(Arc<S>, S::Meta)>,
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG> Debug
    for GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG>
where
    T: TableLike,
    C: ColumnLike,
    I: IndexLike,
    U: UniqueIndexLike,
    F: ForeignKeyLike,
    Func: FunctionLike,
    Ch: CheckConstraintLike,
    Tr: TriggerLike,
    P: PolicyLike,
    R: RoleLike,
    S: SchemaLike,
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericDB")
            .field("catalog_name", &self.catalog_name)
            .field("timezone", &self.timezone)
            .field("tables", &self.tables.len())
            .field("columns", &self.columns.len())
            .field("indices", &self.indices.len())
            .field("unique_indices", &self.unique_indices.len())
            .field("foreign_keys", &self.foreign_keys.len())
            .field("functions", &self.functions.len())
            .field("triggers", &self.triggers.len())
            .field("policies", &self.policies.len())
            .field("check_constraints", &self.check_constraints.len())
            .field("roles", &self.roles.len())
            .field("table_grants", &self.table_grants.len())
            .field("column_grants", &self.column_grants.len())
            .field("schemas", &self.schemas.len())
            .finish()
    }
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG> Clone
    for GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG>
where
    T: TableLike,
    C: ColumnLike,
    I: IndexLike,
    U: UniqueIndexLike,
    F: ForeignKeyLike,
    Func: FunctionLike,
    Ch: CheckConstraintLike,
    Tr: TriggerLike,
    P: PolicyLike,
    R: RoleLike,
    S: SchemaLike,
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    fn clone(&self) -> Self {
        Self {
            catalog_name: self.catalog_name.clone(),
            timezone: self.timezone.clone(),
            tables: self.tables.clone(),
            columns: self.columns.clone(),
            indices: self.indices.clone(),
            unique_indices: self.unique_indices.clone(),
            foreign_keys: self.foreign_keys.clone(),
            functions: self.functions.clone(),
            triggers: self.triggers.clone(),
            policies: self.policies.clone(),
            check_constraints: self.check_constraints.clone(),
            roles: self.roles.clone(),
            table_grants: self.table_grants.clone(),
            column_grants: self.column_grants.clone(),
            schemas: self.schemas.clone(),
        }
    }
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG>
    GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG>
where
    T: TableLike,
    C: ColumnLike,
    I: IndexLike,
    U: UniqueIndexLike,
    F: ForeignKeyLike,
    Func: FunctionLike,
    Ch: CheckConstraintLike,
    Tr: TriggerLike,
    P: PolicyLike,
    R: RoleLike,
    S: SchemaLike,
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    /// Creates a new `GenericDBBuilder` instance.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn new(
        catalog_name: String,
    ) -> GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG> {
        GenericDBBuilder::new(catalog_name)
    }

    /// Returns a reference to the metadata of the specified table, if it exists
    /// in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     -- This is a test table
    ///     CREATE TABLE test_table (id INT);
    ///     ",
    /// )?;
    /// let table = db.table(None, "test_table").unwrap();
    /// let metadata = db.table_metadata(table).unwrap();
    /// assert_eq!(metadata.table_doc().and_then(|d| d.doc()), Some("This is a test table"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn table_metadata(&self, table: &T) -> Option<&T::Meta> {
        self.tables
            .binary_search_by_key(
                &(
                    table.table_schema().map(std::string::ToString::to_string),
                    table.table_name().to_string(),
                ),
                |(t, _)| {
                    (
                        t.table_schema().map(std::string::ToString::to_string),
                        t.table_name().to_string(),
                    )
                },
            )
            .ok()
            .map(|index| &self.tables[index].1)
    }

    /// Returns a reference to the metadata of the specified column, if it
    /// exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// let table = db.table(None, "t").unwrap();
    /// let column = table.column("id", &db).unwrap();
    /// // The metadata for columns in ParserDB is currently unit ()
    /// assert_eq!(db.column_metadata(column), Some(&()));
    /// # Ok(())
    /// # }
    /// ```
    pub fn column_metadata(&self, column: &C) -> Option<&C::Meta> {
        self.columns
            .binary_search_by(|(c, _)| c.as_ref().cmp(column))
            .ok()
            .map(|index| &self.columns[index].1)
    }

    /// Returns a reference to the metadata of the specified unique index, if it
    /// exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT UNIQUE);")?;
    /// let table = db.table(None, "t").unwrap();
    /// let index = table.unique_indices(&db).next().unwrap();
    /// // The metadata for unique indices in ParserDB is currently unit ()
    /// // (actually it might be struct depending on impl, let's just check existence)
    /// assert!(db.unique_index_metadata(index).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn unique_index_metadata(&self, index: &U) -> Option<&U::Meta> {
        self.unique_indices
            .binary_search_by(|(i, _)| i.as_ref().cmp(index))
            .ok()
            .map(|index| &self.unique_indices[index].1)
    }

    /// Returns a reference to the metadata of the specified check constraint,
    /// if it exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT CHECK (id > 0));")?;
    /// let table = db.table(None, "t").unwrap();
    /// let check = table.check_constraints(&db).next().unwrap();
    /// assert!(db.check_constraint_metadata(check).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn check_constraint_metadata(&self, constraint: &Ch) -> Option<&Ch::Meta> {
        self.check_constraints
            .binary_search_by(|(c, _)| c.as_ref().cmp(constraint))
            .ok()
            .map(|index| &self.check_constraints[index].1)
    }

    /// Returns a reference to the metadata of the specified foreign key, if it
    /// exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE TABLE parent (id INT PRIMARY KEY);
    ///     CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
    ///     ",
    /// )?;
    /// let child = db.table(None, "child").unwrap();
    /// let fk = child.foreign_keys(&db).next().unwrap();
    /// assert!(db.foreign_key_metadata(fk).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn foreign_key_metadata(&self, key: &F) -> Option<&F::Meta> {
        self.foreign_keys
            .binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|index| &self.foreign_keys[index].1)
    }

    /// Returns a reference to the metadata of the specified index, if it exists
    /// in the database.
    pub fn index_metadata(&self, index: &I) -> Option<&I::Meta> {
        self.indices
            .binary_search_by(|(i, _)| i.as_ref().cmp(index))
            .ok()
            .map(|index| &self.indices[index].1)
    }

    /// Returns a reference of the function by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the function to retrieve.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db =
    ///     ParserDB::parse::<GenericDialect>("CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1';")?;
    /// let func = db.function("my_func").unwrap();
    /// assert_eq!(func.name(), "my_func");
    /// assert!(db.function("non_existent").is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn function(&self, name: &str) -> Option<&Func> {
        self.functions
            .binary_search_by(|(f, _)| f.name().cmp(name))
            .ok()
            .map(|index| self.functions[index].0.as_ref())
    }

    /// Returns a reference to the metadata of the specified function, if it
    /// exists in the database.
    ///
    /// # Arguments
    ///
    /// * `function` - The function to retrieve metadata for.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db =
    ///     ParserDB::parse::<GenericDialect>("CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1';")?;
    /// let func = db.function("my_func").unwrap();
    /// assert!(db.function_metadata(func).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn function_metadata(&self, function: &Func) -> Option<&Func::Meta> {
        self.functions
            .binary_search_by(|(f, _)| f.name().cmp(function.name()))
            .ok()
            .map(|index| &self.functions[index].1)
    }

    /// Returns a reference of the trigger by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the trigger to retrieve.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE TABLE t (id INT);
    ///     CREATE FUNCTION f() RETURNS TRIGGER AS 'BEGIN END' LANGUAGE plpgsql;
    ///     CREATE TRIGGER my_trigger AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f();
    ///     ",
    /// )?;
    /// let trigger = db.trigger("my_trigger").unwrap();
    /// assert_eq!(trigger.name(), "my_trigger");
    /// assert!(db.trigger("non_existent").is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn trigger(&self, name: &str) -> Option<&Tr> {
        self.triggers
            .binary_search_by(|(t, _)| t.name().cmp(name))
            .ok()
            .map(|index| self.triggers[index].0.as_ref())
    }

    /// Returns a reference to the metadata of the specified trigger, if it
    /// exists in the database.
    ///
    /// # Arguments
    ///
    /// * `trigger` - The trigger to retrieve metadata for.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE TABLE t (id INT);
    ///     CREATE FUNCTION f() RETURNS TRIGGER AS 'BEGIN END' LANGUAGE plpgsql;
    ///     CREATE TRIGGER my_trigger AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f();
    ///     ",
    /// )?;
    /// let trigger = db.trigger("my_trigger").unwrap();
    /// assert!(db.trigger_metadata(trigger).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn trigger_metadata(&self, trigger: &Tr) -> Option<&Tr::Meta> {
        self.triggers
            .binary_search_by(|(t, _)| t.name().cmp(trigger.name()))
            .ok()
            .map(|index| &self.triggers[index].1)
    }

    /// Returns a reference to the metadata of the specified policy, if it
    /// exists in the database.
    pub fn policy_metadata(&self, policy: &P) -> Option<&P::Meta> {
        self.policies
            .binary_search_by(|(p, _)| p.name().cmp(policy.name()))
            .ok()
            .map(|index| &self.policies[index].1)
    }

    /// Returns a reference of the role by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the role to retrieve.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>("CREATE ROLE admin SUPERUSER;")?;
    /// let role = db.role("admin").unwrap();
    /// assert_eq!(role.name(), "admin");
    /// assert!(role.is_superuser());
    /// assert!(db.role("non_existent").is_none());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn role(&self, name: &str) -> Option<&R> {
        self.roles
            .binary_search_by(|(r, _)| r.name().cmp(name))
            .ok()
            .map(|index| self.roles[index].0.as_ref())
    }

    /// Returns a reference to the metadata of the specified role, if it
    /// exists in the database.
    ///
    /// # Arguments
    ///
    /// * `role` - The role to retrieve metadata for.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE ROLE admin;")?;
    /// let role = db.role("admin").unwrap();
    /// assert!(db.role_metadata(role).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn role_metadata(&self, role: &R) -> Option<&R::Meta> {
        self.roles
            .binary_search_by(|(r, _)| r.name().cmp(role.name()))
            .ok()
            .map(|index| &self.roles[index].1)
    }

    /// Returns a reference to the catalog name.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// assert_eq!(db.catalog_name(), "unknown_catalog");
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    #[inline]
    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    /// Iterates over the table and metadata
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// -- table b
    /// CREATE TABLE b (id INT);
    /// -- table a
    /// CREATE TABLE a (id INT);",
    /// )?;
    ///
    /// let mut parsed: Vec<(&str, Option<&str>)> = db
    ///     .tables_metadata()
    ///     .map(|(t, meta)| (t.table_name(), meta.table_doc().and_then(|d| d.doc())))
    ///     .collect();
    ///
    /// parsed.sort_by(|(a, _), (b, _)| a.cmp(b));
    /// assert_eq!(parsed, vec![("a", Some("table a")), ("b", Some("table b"))]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn tables_metadata(&self) -> impl Iterator<Item = (&T, &T::Meta)> {
        self.tables.iter().map(|(t, m)| (t.as_ref(), m))
    }

    /// Iterates mutably over the table and metadata
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// let mut db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     -- original doc a
    ///     CREATE TABLE a (id INT);
    ///     -- original doc b
    ///     CREATE TABLE b (id INT);
    ///     ",
    /// )?;
    /// let metadata = db.tables_metadata_mut().collect::<Vec<_>>();
    /// assert_eq!(metadata.len(), db.number_of_tables());
    /// # Ok(())
    /// # }
    /// ```
    pub fn tables_metadata_mut(&mut self) -> impl Iterator<Item = (&T, &mut T::Meta)> {
        self.tables.iter_mut().map(|(t, m)| ((*t).as_ref(), m))
    }

    /// Returns a reference to the metadata of the specified table grant, if it
    /// exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE ROLE admin;
    ///     CREATE TABLE users (id INT);
    ///     GRANT SELECT ON users TO admin;
    ///     ",
    /// )?;
    /// let grant = db.table_grants().next().unwrap();
    /// assert!(db.table_grant_metadata(grant).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn table_grant_metadata(&self, grant: &TG) -> Option<&TG::Meta> {
        self.table_grants.iter().find(|(g, _)| g.as_ref() == grant).map(|(_, m)| m)
    }

    /// Returns a reference to the metadata of the specified column grant, if it
    /// exists in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    ///     CREATE ROLE admin;
    ///     CREATE TABLE users (id INT, name TEXT);
    ///     GRANT SELECT (name) ON users TO admin;
    ///     ",
    /// )?;
    /// let grant = db.column_grants().next().unwrap();
    /// assert!(db.column_grant_metadata(grant).is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub fn column_grant_metadata(&self, grant: &CG) -> Option<&CG::Meta> {
        self.column_grants.iter().find(|(g, _)| g.as_ref() == grant).map(|(_, m)| m)
    }

    /// Returns a reference to the schema by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the schema to retrieve.
    #[must_use]
    pub fn schema(&self, name: &str) -> Option<&S> {
        self.schemas
            .binary_search_by(|(s, _)| s.name().cmp(name))
            .ok()
            .map(|index| self.schemas[index].0.as_ref())
    }

    /// Returns a reference to the metadata of the specified schema, if it
    /// exists in the database.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema to retrieve metadata for.
    pub fn schema_metadata(&self, schema: &S) -> Option<&S::Meta> {
        self.schemas
            .binary_search_by(|(s, _)| s.name().cmp(schema.name()))
            .ok()
            .map(|index| &self.schemas[index].1)
    }

    /// Iterates over the schemas and their metadata.
    pub fn schemas(&self) -> impl Iterator<Item = (&S, &S::Meta)> {
        self.schemas.iter().map(|(s, m)| (s.as_ref(), m))
    }
}
