//! Submodule providing a trait for describing SQL Database-like entities.

use std::{borrow::Borrow, fmt::Debug};

use geometric_traits::{
    impls::CSR2D,
    prelude::{GenericEdgesBuilder, Kahn, SquareCSR2D},
    traits::EdgesBuilder,
};

use crate::traits::{
    CheckConstraintLike, ColumnLike, ForeignKeyLike, FunctionLike, IndexLike, PolicyLike, RoleLike,
    TableLike, TriggerLike, UniqueIndexLike,
};

/// A trait for types that can be treated as SQL databases.
pub trait DatabaseLike: Clone + Debug {
    /// Type of the tables in the schema.
    type Table: TableLike<DB = Self>;
    /// Type of the columns in the schema.
    type Column: ColumnLike<DB = Self>;
    /// Type of the indices in the schema.
    type Index: IndexLike<DB = Self>;
    /// Type of the foreign keys in the schema.
    type ForeignKey: ForeignKeyLike<DB = Self>;
    /// Type of the functions in the schema.
    type Function: FunctionLike<DB = Self>;
    /// Type of the triggers in the schema.
    type Trigger: TriggerLike<DB = Self>;
    /// Type of the unique indexes in the schema.
    type UniqueIndex: UniqueIndexLike<DB = Self>;
    /// Type of the check constraints in the schema.
    type CheckConstraint: CheckConstraintLike<DB = Self>;
    /// Type of the policies in the schema.
    type Policy: PolicyLike<DB = Self>;
    /// Type of the roles in the schema.
    type Role: RoleLike<DB = Self>;

    /// Returns the name of the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// assert_eq!(db.catalog_name(), "unknown_catalog");
    /// # Ok(())
    /// # }
    /// ```
    fn catalog_name(&self) -> &str;

    /// Returns the number of tables in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t1 (id INT);
    /// CREATE TABLE t2 (id INT);
    /// "#,
    /// )?;
    /// assert_eq!(db.number_of_tables(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn number_of_tables(&self) -> usize;

    /// Returns the timezone of the database, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>("SET TIME ZONE 'UTC';")?;
    /// assert_eq!(db.timezone(), Some("UTC"));
    ///
    /// let db_no_tz = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// assert_eq!(db_no_tz.timezone(), None);
    /// # Ok(())
    /// # }
    /// ```
    fn timezone(&self) -> Option<&str>;

    /// Iterates over the tables defined in the schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE table1 (id INT);
    /// CREATE TABLE table2 (name TEXT);
    /// CREATE TABLE table3 (score DECIMAL);
    /// "#,
    /// )?;
    /// let table_names: Vec<&str> = db.tables().map(|t| t.table_name()).collect();
    /// assert_eq!(table_names, vec!["table1", "table2", "table3"]);
    /// # Ok(())
    /// # }
    /// ```
    fn tables(&self) -> impl Iterator<Item = &Self::Table>;

    /// Iterates over the triggers defined in the schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t (id INT);
    /// CREATE FUNCTION f() RETURNS TRIGGER AS 'BEGIN END;' LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger AFTER INSERT ON t FOR EACH ROW EXECUTE PROCEDURE f();
    /// "#,
    /// )?;
    /// let triggers: Vec<&str> = db.triggers().map(|t| t.name()).collect();
    /// assert_eq!(triggers, vec!["my_trigger"]);
    /// # Ok(())
    /// # }
    /// ```
    fn triggers(&self) -> impl Iterator<Item = &Self::Trigger>;

    /// Returns whether the database has at least one table.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db_with_tables = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE table1 (id INT);
    /// "#,
    /// )?;
    /// assert!(db_with_tables.has_tables());
    ///
    /// let db_without_tables = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// -- No tables defined
    /// "#,
    /// )?;
    /// assert!(!db_without_tables.has_tables());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn has_tables(&self) -> bool {
        self.tables().next().is_some()
    }

    /// Returns an iterator over the root tables in the database,
    /// i.e., tables which are extended by some other table and
    /// do not extend any other table. Tables which are not involved
    /// in any extension relationship are not considered root tables.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE base_table (id INT PRIMARY KEY);
    /// CREATE TABLE extended_table1 (id INT PRIMARY KEY REFERENCES base_table(id));
    /// CREATE TABLE extended_table2 (id INT PRIMARY KEY REFERENCES base_table(id));
    /// CREATE TABLE independent_table (id INT PRIMARY KEY);
    /// "#,
    /// )?;
    ///
    /// let root_table_names: Vec<&str> = db.root_tables().map(|t| t.table_name()).collect();
    /// assert_eq!(root_table_names, vec!["base_table"]);
    /// # Ok(())
    /// # }
    /// ```
    fn root_tables(&self) -> impl Iterator<Item = &Self::Table> {
        self.tables().filter(|table| !table.is_extension(self) && table.is_extended(self))
    }

    /// Returns the maximum number of columns found in any table in the
    /// database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE table1 (id INT, name TEXT);
    /// CREATE TABLE table2 (score DECIMAL, level INT, active BOOLEAN);
    /// "#,
    /// )?;
    /// assert_eq!(db.maximum_number_of_columns(), 3);
    /// # Ok(())
    /// # }
    /// ```
    fn maximum_number_of_columns(&self) -> usize {
        self.tables().map(|table| table.columns(self).count()).max().unwrap_or(0)
    }

    /// Returns tables as a Kahn's ordering based on foreign key dependencies,
    /// ignoring potential self-references which would create cycles.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE users (
    ///    id SERIAL PRIMARY KEY,
    ///   name TEXT NOT NULL
    /// );
    /// CREATE TABLE comments (
    ///   id SERIAL PRIMARY KEY,
    /// name TEXT NOT NULL,
    /// user_id INT REFERENCES users(id)
    /// );
    /// CREATE TABLE extended_comments (
    ///  id INT PRIMARY KEY REFERENCES comments(id),
    /// extra_info TEXT
    /// );
    /// "#,
    /// )?;
    /// let user_table = db.table(None, "users").unwrap();
    /// let comment_table = db.table(None, "comments").unwrap();
    /// let extended_comment_table = db.table(None, "extended_comments").unwrap();
    /// let ordered_tables = db.table_dag();
    /// assert_eq!(ordered_tables, vec![user_table, comment_table, extended_comment_table]);
    /// # Ok(())
    /// # }
    /// ```
    fn table_dag(&self) -> Vec<&Self::Table> {
        let tables = self.tables().collect::<Vec<&Self::Table>>();

        let mut edges = tables
            .iter()
            .enumerate()
            .flat_map(|(table_number, table)| {
                let tables_ref = tables.as_slice();
                table
                    .foreign_keys(self)
                    .map(Borrow::borrow)
                    .filter_map(move |fk| {
                        let referenced_table = fk.referenced_table(self).borrow();
                        // We ignore self-references to avoid cycles in the DAG.
                        if referenced_table == *table {
                            return None;
                        }
                        Some(tables_ref.binary_search(&referenced_table).unwrap_or_else(|_| panic!("Referenced table '{}' not found in database '{}' - Tables are {:?}",
                            referenced_table.table_name(),
                            self.catalog_name(),
                            tables_ref.iter().map(TableLike::table_name).collect::<Vec<&str>>())))
                    })
                    .map(move |referenced_table_number| (referenced_table_number, table_number))
            })
            .collect::<Vec<(usize, usize)>>();

        // There is no guarantee that the foreign keys in a table are ordered,
        // so it is necessary to sort and deduplicate the edges.
        edges.sort_unstable();
        // Furthermore, there is no guarantee that there are no foreign keys
        // referencing the same table, so we deduplicate the edges as well.
        edges.dedup();

        let dag: SquareCSR2D<CSR2D<usize, usize, usize>> = GenericEdgesBuilder::default()
            .expected_shape(tables.len())
            .edges(edges)
            .build()
            .expect("Failed to build table dependency DAG");
        let dag_ordering = dag.kahn().expect("Failed to compute Kahn's ordering");

        let mut ordered_tables = tables.clone();
        for (table_index, table) in dag_ordering.into_iter().zip(tables.iter()) {
            ordered_tables[table_index] = table;
        }

        ordered_tables
    }

    /// Iterates over the functions created in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS 'SELECT "Hello, " || name;';
    /// "#,
    /// )?;
    /// let function_names: Vec<&str> = db.functions().map(|f| f.name()).collect();
    ///
    /// // There will be more than two functions because the parser may add
    /// // additional built-in functions automatically. We check that certainly
    /// // our two functions are present.
    /// assert!(function_names.contains(&"add_one"));
    /// assert!(function_names.contains(&"greet"));
    ///
    /// # Ok(())
    /// # }
    /// ```
    fn functions(&self) -> impl Iterator<Item = &Self::Function>;

    /// Returns the table with the given (optional) schema and name.
    ///
    /// # Arguments
    ///
    /// * `schema` - Optional schema name of the table.
    /// * `table_name` - Name of the table.
    ///
    /// # Panics
    ///
    /// Panics if the table is not found in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE my_schema.my_table_with_schema (id INT);
    /// CREATE TABLE my_table (id INT);
    /// "#,
    /// )?;
    /// let table_with_schema = db.table(Some("my_schema"), "my_table_with_schema").unwrap();
    /// assert_eq!(table_with_schema.table_name(), "my_table_with_schema");
    /// assert_eq!(table_with_schema.table_schema(), Some("my_schema"));
    ///
    /// let table_without_schema = db.table(None, "my_table").unwrap();
    /// assert_eq!(table_without_schema.table_name(), "my_table");
    /// assert_eq!(table_without_schema.table_schema(), None);
    /// # Ok(())
    /// # }
    /// ```
    fn table(&self, schema: Option<&str>, table_name: &str) -> Option<&Self::Table>;

    /// Returns the table ID for the given table object according to its
    /// position in the database's table iterator.
    ///
    /// # Arguments
    ///
    /// * `table` - Table object to get the ID for.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE table1 (id INT);
    /// CREATE TABLE table2 (name TEXT);
    /// CREATE TABLE table3 (score DECIMAL);
    /// "#,
    /// )?;
    /// let table2 = db.table(None, "table2").expect("Table 'table2' should exist");
    /// let table2_id = db.table_id(table2).expect("Table ID for 'table2' should exist");
    /// assert_eq!(table2_id, 1);
    /// # Ok(())
    /// # }
    /// ```
    fn table_id(&self, table: &Self::Table) -> Option<usize>;

    /// Returns the function with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the function.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// "#,
    /// )?;
    /// let add_one = db.function("add_one").expect("Function 'add_one' should exist");
    /// assert_eq!(add_one.name(), "add_one");
    /// let non_existent = db.function("non_existent");
    /// assert!(non_existent.is_none());
    /// # Ok(())
    /// # }
    /// ```
    fn function(&self, name: &str) -> Option<&Self::Function>;

    /// Iterates over the policies defined in the schema.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t (id INT);
    /// CREATE POLICY my_policy ON t USING (id > 0);
    /// "#,
    /// )?;
    /// let policies: Vec<&str> = db.policies().map(|p| p.name()).collect();
    /// assert_eq!(policies, vec!["my_policy"]);
    /// # Ok(())
    /// # }
    /// ```
    fn policies(&self) -> impl Iterator<Item = &Self::Policy>;

    /// Returns whether the datavase has policies defined.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db_with_policies = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t (id INT);
    /// CREATE POLICY my_policy ON t USING (id > 0);
    /// "#,
    /// )?;
    /// assert!(db_with_policies.has_policies());
    ///
    /// let db_without_policies = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t (id INT);
    /// "#,
    /// )?;
    /// assert!(!db_without_policies.has_policies());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn has_policies(&self) -> bool {
        self.policies().next().is_some()
    }

    /// Iterates over the roles defined in the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE ROLE admin;
    /// CREATE ROLE user1;
    /// "#,
    /// )?;
    ///
    /// let roles: Vec<_> = db.roles().collect();
    /// assert_eq!(roles.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn roles(&self) -> impl Iterator<Item = &Self::Role>;

    /// Returns a role by name, if it exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>("CREATE ROLE admin SUPERUSER;")?;
    ///
    /// let admin = db.role("admin");
    /// assert!(admin.is_some());
    /// assert!(admin.unwrap().is_superuser());
    ///
    /// let nonexistent = db.role("nonexistent");
    /// assert!(nonexistent.is_none());
    /// # Ok(())
    /// # }
    /// ```
    fn role(&self, name: &str) -> Option<&Self::Role> {
        self.roles().find(|r| r.name() == name)
    }

    /// Returns whether the database has any roles defined.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db_with_roles = ParserDB::parse::<GenericDialect>("CREATE ROLE admin;")?;
    /// assert!(db_with_roles.has_roles());
    ///
    /// let db_without_roles = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// assert!(!db_without_roles.has_roles());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn has_roles(&self) -> bool {
        self.roles().next().is_some()
    }

    /// Iterates over tables that have Row Level Security (RLS) enabled.
    ///
    /// This includes tables with either regular RLS or forced RLS.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE rls_table (id INT);
    /// ALTER TABLE rls_table ENABLE ROW LEVEL SECURITY;
    /// CREATE TABLE forced_rls_table (id INT);
    /// ALTER TABLE forced_rls_table ENABLE ROW LEVEL SECURITY;
    /// ALTER TABLE forced_rls_table FORCE ROW LEVEL SECURITY;
    /// CREATE TABLE no_rls_table (id INT);
    /// "#,
    /// )?;
    ///
    /// let rls_table_names: Vec<&str> = db.rls_tables().map(|t| t.table_name()).collect();
    /// assert_eq!(rls_table_names.len(), 2);
    /// assert!(rls_table_names.contains(&"rls_table"));
    /// assert!(rls_table_names.contains(&"forced_rls_table"));
    /// assert!(!rls_table_names.contains(&"no_rls_table"));
    /// # Ok(())
    /// # }
    /// ```
    fn rls_tables(&self) -> impl Iterator<Item = &Self::Table> {
        self.tables().filter(|table| table.has_row_level_security(self))
    }

    /// Iterates over tables that have forced Row Level Security (RLS) enabled.
    ///
    /// Forced RLS means that even the table owner is subject to RLS policies.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE rls_table (id INT);
    /// ALTER TABLE rls_table ENABLE ROW LEVEL SECURITY;
    /// CREATE TABLE forced_rls_table (id INT);
    /// ALTER TABLE forced_rls_table ENABLE ROW LEVEL SECURITY;
    /// ALTER TABLE forced_rls_table FORCE ROW LEVEL SECURITY;
    /// CREATE TABLE no_rls_table (id INT);
    /// "#,
    /// )?;
    ///
    /// let forced_rls_table_names: Vec<&str> =
    ///     db.forced_rls_tables().map(|t| t.table_name()).collect();
    /// assert_eq!(forced_rls_table_names.len(), 1);
    /// assert_eq!(forced_rls_table_names[0], "forced_rls_table");
    /// # Ok(())
    /// # }
    /// ```
    fn forced_rls_tables(&self) -> impl Iterator<Item = &Self::Table> {
        self.tables().filter(|table| table.has_forced_row_level_security(self))
    }

    /// Returns whether the database has any tables with Row Level Security
    /// enabled.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db_with_rls = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t (id INT);
    /// ALTER TABLE t ENABLE ROW LEVEL SECURITY;
    /// "#,
    /// )?;
    /// assert!(db_with_rls.has_rls_tables());
    ///
    /// let db_without_rls = ParserDB::parse::<GenericDialect>("CREATE TABLE t (id INT);")?;
    /// assert!(!db_without_rls.has_rls_tables());
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn has_rls_tables(&self) -> bool {
        self.rls_tables().next().is_some()
    }

    /// Returns the number of tables with Row Level Security enabled.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     r#"
    /// CREATE TABLE t1 (id INT);
    /// ALTER TABLE t1 ENABLE ROW LEVEL SECURITY;
    /// CREATE TABLE t2 (id INT);
    /// ALTER TABLE t2 ENABLE ROW LEVEL SECURITY;
    /// CREATE TABLE t3 (id INT);
    /// "#,
    /// )?;
    /// assert_eq!(db.number_of_rls_tables(), 2);
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn number_of_rls_tables(&self) -> usize {
        self.rls_tables().count()
    }
}
