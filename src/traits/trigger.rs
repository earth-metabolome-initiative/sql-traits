//! Submodule providing a trait for describing SQL Trigger-like entities.

use std::fmt::Debug;

use crate::{
    traits::{DatabaseLike, FunctionLike, Metadata},
    utils::maintenance_trigger_parser::parse_maintenance_body,
};

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

    /// Returns whether the trigger is a maintenance trigger.
    ///
    /// A maintenance trigger is defined as a trigger that solely consists of
    /// updating values in `NEW.{column_name} = ...` and concludes by
    /// returning `NEW`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// // Example of a maintenance trigger
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE brands (id INT, edited_at TIMESTAMP);
    /// CREATE OR REPLACE FUNCTION update_brands_edited_at() RETURNS TRIGGER AS $$
    /// BEGIN
    ///     NEW.edited_at = CURRENT_TIMESTAMP;
    ///     RETURN NEW;
    /// END;
    /// $$ LANGUAGE plpgsql;
    ///
    /// CREATE TRIGGER trigger_update_brands_edited_at
    /// BEFORE UPDATE ON brands
    /// FOR EACH ROW EXECUTE FUNCTION update_brands_edited_at();
    /// "#,
    /// )?;
    ///
    /// let trigger = db.triggers().next().unwrap();
    /// assert!(trigger.is_maintenance_trigger(&db));
    ///
    /// // Example of a non-maintenance trigger (extra logic)
    /// let db2 = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE brands (id INT);
    /// CREATE OR REPLACE FUNCTION complex_trigger() RETURNS TRIGGER AS $$
    /// BEGIN
    ///     IF NEW.id > 10 THEN
    ///         NEW.id = 10;
    ///     END IF;
    ///     RETURN NEW;
    /// END;
    /// $$ LANGUAGE plpgsql;
    /// CREATE TRIGGER complex
    /// BEFORE UPDATE ON brands
    /// FOR EACH ROW EXECUTE FUNCTION complex_trigger();
    /// "#,
    /// )?;
    /// let complex = db2.triggers().next().unwrap();
    /// assert!(!complex.is_maintenance_trigger(&db2));
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn is_maintenance_trigger<'db>(&'db self, database: &'db Self::DB) -> bool {
        let Some(function) = self.function(database) else {
            return false;
        };
        let Some(body) = function.body() else {
            return false;
        };
        let table = self.table(database);

        let result = parse_maintenance_body(body, table, database);

        result.is_ok()
    }

    /// Returns the assignments in a maintenance trigger.
    /// Returns iterator of (column, expression_ast).
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE brands (id INT, edited_at TIMESTAMP, name TEXT);
    /// CREATE OR REPLACE FUNCTION update_stuff() RETURNS TRIGGER AS $$
    /// BEGIN
    ///     NEW.edited_at = CURRENT_TIMESTAMP;
    ///     NEW.name = lower(NEW.name);
    ///     RETURN NEW;
    /// END;
    /// $$ LANGUAGE plpgsql;
    ///
    /// CREATE TRIGGER trigger_update
    /// BEFORE UPDATE ON brands
    /// FOR EACH ROW EXECUTE FUNCTION update_stuff();
    /// "#,
    /// )?;
    ///
    /// let trigger = db.triggers().next().unwrap();
    /// let assignments: Vec<_> = trigger.maintenance_assignments(&db).collect();
    /// let brands_table = db.table(None, "brands").unwrap();
    /// let edited_at_column = brands_table.column("edited_at", &db).unwrap();
    /// let name_column = brands_table.column("name", &db).unwrap();
    ///
    /// assert_eq!(assignments.len(), 2);
    /// assert_eq!(assignments[0].0, edited_at_column);
    /// assert_eq!(assignments[0].1.to_string(), "CURRENT_TIMESTAMP");
    /// assert_eq!(assignments[1].0, name_column);
    /// assert_eq!(assignments[1].1.to_string(), "lower(NEW.name)");
    /// # Ok(())
    /// # }
    /// ```
    #[inline]
    fn maintenance_assignments<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = (&'db <Self::DB as DatabaseLike>::Column, sqlparser::ast::Expr)> {
        if let Some(function) = self.function(database)
            && let Some(body) = function.body()
        {
            let table = self.table(database);
            parse_maintenance_body(body, table, database).unwrap_or_default()
        } else {
            Vec::new()
        }
        .into_iter()
    }
}

impl<T: TriggerLike> TriggerLike for &T {
    type DB = T::DB;

    fn name(&self) -> &str {
        (*self).name()
    }

    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        (*self).table(database)
    }

    fn events(&self) -> &[sqlparser::ast::TriggerEvent] {
        (*self).events()
    }

    fn timing(&self) -> Option<sqlparser::ast::TriggerPeriod> {
        (*self).timing()
    }

    fn orientation(&self) -> Option<sqlparser::ast::TriggerObjectKind> {
        (*self).orientation()
    }

    fn function<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> Option<&'db <Self::DB as DatabaseLike>::Function>
    where
        Self: 'db,
    {
        (*self).function(database)
    }

    fn is_maintenance_trigger<'db>(&'db self, database: &'db Self::DB) -> bool {
        (*self).is_maintenance_trigger(database)
    }

    fn maintenance_assignments<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = (&'db <Self::DB as DatabaseLike>::Column, sqlparser::ast::Expr)> {
        (*self).maintenance_assignments(database)
    }
}
