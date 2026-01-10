//! Submodule providing a trait for describing SQL Trigger-like entities.

use std::fmt::Debug;

use crate::traits::{DatabaseLike, Metadata};

/// A trait for types that can be treated as SQL triggers.
pub trait TriggerLike: Clone + Debug + Metadata {
    /// The database type the trigger belongs to.
    type DB: DatabaseLike;

    /// Returns the name of the trigger.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert_eq!(trigger.name(), "my_trigger");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert_eq!(trigger.name(), "my_trigger");
    /// # Ok(())
    /// # }
    /// ```
    fn name(&self) -> &str;

    /// Returns the table the trigger is associated with.
    ///
    /// # Arguments
    ///
    /// * `database` - A reference to the database instance to query the table
    ///   from.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// let table = trigger.table(&db);
    /// assert_eq!(table.table_name(), "my_table");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// let table = trigger.table(&db);
    /// assert_eq!(table.table_name(), "my_table");
    /// # Ok(())
    /// # }
    /// ```
    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db;

    /// Returns the events that fire the trigger.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT OR UPDATE ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// let events = trigger.events();
    /// assert_eq!(events.len(), 2);
    /// assert!(matches!(events[0], sqlparser::ast::TriggerEvent::Insert));
    /// assert!(matches!(events[1], sqlparser::ast::TriggerEvent::Update(_)));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT, col1 INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER UPDATE OF col1 ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// let events = trigger.events();
    /// assert_eq!(events.len(), 1);
    /// assert!(matches!(events[0], sqlparser::ast::TriggerEvent::Update(_)));
    /// # Ok(())
    /// # }
    /// ```
    fn events(&self) -> &[sqlparser::ast::TriggerEvent];

    /// Returns the timing of the trigger (BEFORE, AFTER, INSTEAD OF).
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert!(matches!(trigger.timing(), Some(sqlparser::ast::TriggerPeriod::After)));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// BEFORE INSERT ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert!(matches!(trigger.timing(), Some(sqlparser::ast::TriggerPeriod::Before)));
    /// # Ok(())
    /// # }
    /// ```
    fn timing(&self) -> Option<sqlparser::ast::TriggerPeriod>;

    /// Returns the orientation of the trigger (ROW, STATEMENT).
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert!(matches!(
    ///     trigger.orientation(),
    ///     Some(sqlparser::ast::TriggerObjectKind::ForEach(sqlparser::ast::TriggerObject::Row))
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// assert!(matches!(
    ///     trigger.orientation(),
    ///     Some(sqlparser::ast::TriggerObjectKind::ForEach(sqlparser::ast::TriggerObject::Row))
    /// ));
    /// # Ok(())
    /// # }
    /// ```
    fn orientation(&self) -> Option<sqlparser::ast::TriggerObjectKind>;

    /// Returns the function the trigger executes.
    ///
    /// # Arguments
    ///
    /// * `database` - A reference to the database instance to query the
    ///   function from.
    ///
    /// # Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE FUNCTION my_function() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// EXECUTE FUNCTION my_function();
    /// "#,
    /// )?;
    /// let trigger = db.triggers().next().unwrap();
    /// let function = trigger.function(&db).unwrap();
    /// assert_eq!(function.name(), "my_function");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SQLite Example
    ///
    /// ```rust
    /// #  fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::{dialect::SQLiteDialect, parser::Parser};
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE TRIGGER my_trigger
    /// AFTER INSERT ON my_table
    /// FOR EACH ROW
    /// BEGIN
    ///     UPDATE my_table SET id = id + 1;
    /// END;
    /// "#;
    ///
    /// let dialect = SQLiteDialect {};
    /// let statements = Parser::parse_sql(&dialect, sql)?;
    /// let db = ParserDB::from_statements(statements, "test".to_string())?;
    /// let trigger = db.triggers().next().unwrap();
    /// // SQLite triggers do not call a function object.
    /// assert!(trigger.function(&db).is_none());
    /// # Ok(())
    /// # }
    /// ```
    fn function<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Option<&'db <Self::DB as DatabaseLike>::Function>
    where
        Self: 'db;
}
