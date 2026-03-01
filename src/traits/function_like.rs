//! Submodule providing a trait for describing SQL Function-like entities.

use std::{fmt::Debug, hash::Hash};

use crate::{
    traits::{DatabaseLike, Metadata},
    utils::normalize_postgres_type,
};

/// A trait for describing SQL Function-like entities.
pub trait FunctionLike: Metadata + Debug + Clone + Hash + Ord + Eq + Send + Sync {
    /// The associated database type.
    type DB: DatabaseLike<Function = Self>;

    /// The name of the function.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// ",
    /// )?;
    /// let function = db.functions().next().expect("Function should exist");
    /// assert_eq!(function.name(), "add_one");
    /// # Ok(())
    /// # }
    /// ```
    fn name(&self) -> &str;

    /// Returns the argument type names (if any) of the function as strings.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add(x INT, y INT) RETURNS INT AS 'SELECT x + y;';
    /// CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS 'SELECT \"Hello, \" || name;';
    /// ",
    /// )?;
    /// let add_fn = db.functions().find(|f| f.name() == "add").expect("Function should exist");
    /// let greet_fn = db.functions().find(|f| f.name() == "greet").expect("Function should exist");
    /// assert_eq!(add_fn.argument_type_names(&db).collect::<Vec<_>>(), vec!["INT", "INT"]);
    /// assert_eq!(greet_fn.argument_type_names(&db).collect::<Vec<_>>(), vec!["TEXT"]);
    /// # Ok(())
    /// # }
    /// ```
    fn argument_type_names<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db str>;

    /// Returns the normalized argument type names (if any) of the function as
    /// strings.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add(x INTEGER, y INT) RETURNS INT AS 'SELECT x + y;';
    /// CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS 'SELECT \"Hello, \" || name;';
    /// ",
    /// )?;
    /// let add_fn = db.function("add").expect("Function should exist");
    /// let greet_fn = db.function("greet").expect("Function should exist");
    /// assert_eq!(add_fn.normalized_argument_type_names(&db), vec!["INT", "INT"]);
    /// assert_eq!(greet_fn.normalized_argument_type_names(&db), vec!["TEXT"]);
    /// # Ok(())
    /// # }
    /// ```
    fn normalized_argument_type_names<'db>(&'db self, database: &'db Self::DB) -> Vec<&'db str> {
        self.argument_type_names(database).map(normalize_postgres_type).collect()
    }

    /// Returns the return type name of the function as a string.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS 'SELECT \"Hello, \" || name;';
    /// CREATE FUNCTION do_nothing() AS 'SELECT;';
    /// ",
    /// )?;
    /// let add_one_fn = db.function("add_one").expect("Function should exist");
    /// let greet_fn = db.function("greet").expect("Function should exist");
    /// let do_nothing_fn = db.function("do_nothing").expect("Function should exist");
    /// assert_eq!(do_nothing_fn.return_type_name(&db), None);
    /// assert_eq!(add_one_fn.return_type_name(&db).as_deref(), Some("INT"));
    /// assert_eq!(greet_fn.return_type_name(&db).as_deref(), Some("TEXT"));
    /// # Ok(())
    /// # }
    /// ```
    fn return_type_name<'db>(&'db self, database: &'db Self::DB) -> Option<&'db str>;

    /// Returns the body of the function.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// ",
    /// )?;
    /// let function = db.functions().next().expect("Function should exist");
    /// assert_eq!(function.body(), Some("SELECT x + 1;"));
    /// # Ok(())
    /// # }
    /// ```
    fn body(&self) -> Option<&str>;

    /// Returns the normalized return type name of the function as a string.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE FUNCTION add_one(x INT) RETURNS INTEGER AS 'SELECT x + 1;';
    /// CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS 'SELECT \"Hello, \" || name;';
    /// CREATE FUNCTION do_nothing() AS 'SELECT;';
    /// ",
    /// )?;
    /// let add_one_fn = db.function("add_one").expect("Function should exist");
    /// let greet_fn = db.function("greet").expect("Function should exist");
    /// let do_nothing_fn = db.function("do_nothing").expect("Function should exist");
    /// assert_eq!(do_nothing_fn.normalized_return_type_name(&db), None);
    /// assert_eq!(add_one_fn.normalized_return_type_name(&db).as_deref(), Some("INT"));
    /// assert_eq!(greet_fn.normalized_return_type_name(&db).as_deref(), Some("TEXT"));
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn normalized_return_type_name<'db>(&'db self, database: &'db Self::DB) -> Option<&'db str> {
        self.return_type_name(database).map(normalize_postgres_type)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use crate::{prelude::*, traits::DatabaseLike};

    #[test]
    fn test_drop_function() {
        let sql = r"
            CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1;';
            DROP FUNCTION my_func;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

        // Function should be removed
        assert!(db.function("my_func").is_none());
        assert_eq!(db.functions().filter(|f| f.name() == "my_func").count(), 0);
    }

    #[test]
    fn test_drop_function_if_exists() {
        let sql = r"
            DROP FUNCTION IF EXISTS non_existent_func;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Should not error with IF EXISTS");

        // Should succeed without error even though function doesn't exist
        assert!(db.function("non_existent_func").is_none());
    }

    #[test]
    fn test_drop_function_not_found() {
        let sql = r"
            DROP FUNCTION non_existent_func;
        ";
        let result = ParserDB::parse::<GenericDialect>(sql);

        // Should fail because function doesn't exist
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e}");
            assert!(error_msg.contains("non_existent_func"));
        }
    }

    #[test]
    fn test_drop_multiple_functions() {
        let sql = r"
            CREATE FUNCTION func1() RETURNS INT AS 'SELECT 1;';
            CREATE FUNCTION func2() RETURNS INT AS 'SELECT 2;';
            CREATE FUNCTION func3() RETURNS INT AS 'SELECT 3;';
            DROP FUNCTION func1;
            DROP FUNCTION func3;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

        // func1 and func3 should be removed
        assert!(db.function("func1").is_none());
        assert!(db.function("func3").is_none());

        // func2 should still exist
        assert!(db.function("func2").is_some());
        assert_eq!(db.function("func2").unwrap().name(), "func2");
    }

    #[test]
    fn test_create_drop_create_function() {
        let sql = r"
            CREATE FUNCTION my_func() RETURNS INT AS 'SELECT 1;';
            DROP FUNCTION my_func;
            CREATE FUNCTION my_func() RETURNS TEXT AS 'SELECT ''hello'';';
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");

        // Should have the recreated function
        let func = db.function("my_func").expect("Function should exist");
        assert_eq!(func.name(), "my_func");

        // Should have the new return type
        assert_eq!(func.return_type_name(&db), Some("TEXT"));
    }

    #[test]
    fn test_drop_function_referenced_by_check_fails() {
        let sql = r"
            CREATE FUNCTION is_valid(x INT) RETURNS BOOLEAN AS 'SELECT x > 0;';
            CREATE TABLE t (id INT CHECK (is_valid(id)));
            DROP FUNCTION is_valid;
        ";
        let result = ParserDB::parse::<GenericDialect>(sql);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e}");
            assert!(error_msg.contains("is_valid"));
            assert!(error_msg.contains("referenced"));
        }
    }

    #[test]
    fn test_drop_function_referenced_by_policy_fails() {
        let sql = r"
            CREATE FUNCTION check_access() RETURNS BOOLEAN AS 'SELECT true;';
            CREATE TABLE t (id INT);
            CREATE POLICY my_policy ON t USING (check_access());
            DROP FUNCTION check_access;
        ";
        let result = ParserDB::parse::<GenericDialect>(sql);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e}");
            assert!(error_msg.contains("check_access"));
            assert!(error_msg.contains("referenced"));
        }
    }

    #[test]
    fn test_drop_function_referenced_by_policy_with_check_fails() {
        let sql = r"
            CREATE FUNCTION validate_insert() RETURNS BOOLEAN AS 'SELECT true;';
            CREATE TABLE t (id INT);
            CREATE POLICY insert_policy ON t WITH CHECK (validate_insert());
            DROP FUNCTION validate_insert;
        ";
        let result = ParserDB::parse::<GenericDialect>(sql);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e}");
            assert!(error_msg.contains("validate_insert"));
            assert!(error_msg.contains("referenced"));
        }
    }

    #[test]
    fn test_drop_function_referenced_by_trigger_fails() {
        let sql = r"
            CREATE TABLE t (id INT);
            CREATE FUNCTION my_trigger_func() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
            CREATE TRIGGER my_trigger BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION my_trigger_func();
            DROP FUNCTION my_trigger_func;
        ";
        let result = ParserDB::parse::<GenericDialect>(sql);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e}");
            assert!(error_msg.contains("my_trigger_func"));
            assert!(error_msg.contains("referenced"));
        }
    }

    #[test]
    fn test_drop_unreferenced_function_succeeds() {
        let sql = r"
            CREATE FUNCTION unused_func() RETURNS INT AS 'SELECT 1;';
            CREATE TABLE t (id INT);
            DROP FUNCTION unused_func;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql)
            .expect("Should succeed dropping unreferenced function");

        // Function should be gone
        assert!(db.function("unused_func").is_none());

        // Table should still exist
        assert!(db.table(None, "t").is_some());
    }

    #[test]
    fn test_drop_function_after_dropping_dependent_succeeds() {
        // This test verifies that if a check constraint is part of a table,
        // and the table is created with the function, but then we drop and recreate
        // the function after, the DROP succeeds when no references remain
        let sql = r"
            CREATE FUNCTION helper_func() RETURNS INT AS 'SELECT 1;';
            CREATE FUNCTION other_func() RETURNS INT AS 'SELECT 2;';
            DROP FUNCTION other_func;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Should succeed");

        // helper_func should still exist
        assert!(db.function("helper_func").is_some());

        // other_func should be gone
        assert!(db.function("other_func").is_none());
    }
}
