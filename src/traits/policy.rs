//! Submodule providing a trait for describing SQL Policy-like entities.

use std::{borrow::Borrow, fmt::Debug, hash::Hash};

use sqlparser::ast::{CreatePolicyCommand, Expr, Owner};

use crate::traits::{DatabaseLike, DocumentationMetadata, Metadata};

/// A trait for types that can be treated as SQL policies.
pub trait PolicyLike:
    Debug
    + Clone
    + Hash
    + Ord
    + Eq
    + Metadata
    + DocumentationMetadata
    + Borrow<<<Self as PolicyLike>::DB as DatabaseLike>::Policy>
{
    /// The database type the policy belongs to.
    type DB: DatabaseLike;

    /// Returns the name of the policy.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert_eq!(policy.name(), "my_policy");
    /// # Ok(())
    /// # }
    /// ```
    fn name(&self) -> &str;

    /// Returns the table the policy is defined on.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert_eq!(policy.table(&db), table);
    /// # Ok(())
    /// # }
    /// ```
    fn table<'db>(&'db self, database: &'db Self::DB) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db;

    /// Returns the command the policy applies to.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::ast::CreatePolicyCommand;
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY select_policy ON my_table FOR SELECT USING (true);
    /// CREATE POLICY all_policy ON my_table USING (true);
    /// "#;
    /// let db = ParserDB::try_from(sql)?;
    /// let table = db.table(None, "my_table").unwrap();
    ///
    /// let select_policy = table.policies(&db).find(|p| p.name() == "select_policy").unwrap();
    /// assert_eq!(select_policy.command(), CreatePolicyCommand::Select);
    ///
    /// let all_policy = table.policies(&db).find(|p| p.name() == "all_policy").unwrap();
    /// assert_eq!(all_policy.command(), CreatePolicyCommand::All);
    /// # Ok(())
    /// # }
    /// ```
    fn command(&self) -> CreatePolicyCommand;

    /// Returns the roles the policy applies to.
    /// If empty, it applies to all roles (PUBLIC).
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let sql = r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table TO user1, user2 USING (true);
    /// CREATE POLICY public_policy ON my_table TO PUBLIC USING (true);
    /// "#;
    /// let db = ParserDB::try_from(sql)?;
    /// let table = db.table(None, "my_table").unwrap();
    ///
    /// let policy = table.policies(&db).find(|p| p.name() == "my_policy").unwrap();
    /// // Logic to verify roles (roles() returns iterator)
    /// assert_eq!(policy.roles().count(), 2);
    ///
    /// let public_policy = table.policies(&db).find(|p| p.name() == "public_policy").unwrap();
    /// assert_eq!(public_policy.roles().count(), 1);
    /// # Ok(())
    /// # }
    /// ```
    fn roles(&self) -> impl Iterator<Item = &Owner>;

    /// Returns the `USING` expression of the policy, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert!(policy.using_expression().is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn using_expression(&self) -> Option<&Expr>;

    /// Returns the functions used in the `USING` expression.
    fn using_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function>;

    /// Returns the `WITH CHECK` expression of the policy, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::try_from(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table WITH CHECK (id < 10);
    /// "#,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert!(policy.check_expression().is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn check_expression(&self) -> Option<&Expr>;

    /// Returns the functions used in the `WITH CHECK` expression.
    fn check_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function>;
}

impl<T: PolicyLike> PolicyLike for &T
where
    Self: Borrow<<<T as PolicyLike>::DB as DatabaseLike>::Policy>,
{
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

    fn command(&self) -> CreatePolicyCommand {
        (*self).command()
    }

    fn roles(&self) -> impl Iterator<Item = &Owner> {
        (*self).roles()
    }

    fn using_expression(&self) -> Option<&Expr> {
        (*self).using_expression()
    }

    fn using_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        (*self).using_functions(database)
    }

    fn check_expression(&self) -> Option<&Expr> {
        (*self).check_expression()
    }

    fn check_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        (*self).check_functions(database)
    }
}
