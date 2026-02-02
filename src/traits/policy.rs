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
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    ///     &GenericDialect,
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
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    ///     &GenericDialect,
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
    /// let db = ParserDB::parse(sql, &GenericDialect {})?;
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
    /// let db = ParserDB::parse(sql, &GenericDialect {})?;
    /// let table = db.table(None, "my_table").unwrap();
    ///
    /// let policy = table.policies(&db).find(|p| p.name() == "my_policy").unwrap();
    /// // Logic to verify roles (roles() returns iterator)
    /// assert_eq!(policy.roles(&db).count(), 2);
    ///
    /// let public_policy = table.policies(&db).find(|p| p.name() == "public_policy").unwrap();
    /// assert_eq!(public_policy.roles(&db).count(), 1);
    /// # Ok(())
    /// # }
    /// ```
    fn roles<'db>(&'db self, database: &'db Self::DB) -> impl Iterator<Item = &'db Owner>
    where
        Self: 'db;

    /// Returns the `USING` expression of the policy, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (id > 0);
    /// "#,
    ///     &GenericDialect,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert!(policy.using_expression(&db).is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn using_expression<'db>(&'db self, database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db;

    /// Returns the functions used in the `USING` expression.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE FUNCTION my_func() RETURNS BOOLEAN AS 'SELECT true';
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table USING (my_func());
    /// "#,
    ///     &GenericDialect,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// let functions: Vec<_> = policy.using_functions(&db).collect();
    /// assert_eq!(functions.len(), 1);
    /// assert_eq!(functions[0].name(), "my_func");
    /// # Ok(())
    /// # }
    /// ```
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
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table WITH CHECK (id < 10);
    /// "#,
    ///     &GenericDialect,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// assert!(policy.check_expression(&db).is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn check_expression<'db>(&'db self, database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db;

    /// Returns the functions used in the `WITH CHECK` expression.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse(
    ///     r#"
    /// CREATE FUNCTION check_func() RETURNS BOOLEAN AS 'SELECT true';
    /// CREATE TABLE my_table (id INT);
    /// CREATE POLICY my_policy ON my_table WITH CHECK (check_func());
    /// "#,
    ///     &GenericDialect,
    /// )?;
    /// let table = db.table(None, "my_table").unwrap();
    /// let policy = table.policies(&db).next().unwrap();
    /// let functions: Vec<_> = policy.check_functions(&db).collect();
    /// assert_eq!(functions.len(), 1);
    /// assert_eq!(functions[0].name(), "check_func");
    /// # Ok(())
    /// # }
    /// ```
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

    fn roles<'db>(&'db self, database: &'db Self::DB) -> impl Iterator<Item = &'db Owner>
    where
        Self: 'db,
    {
        (*self).roles(database)
    }

    fn using_expression<'db>(&'db self, database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        (*self).using_expression(database)
    }

    fn using_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        (*self).using_functions(database)
    }

    fn check_expression<'db>(&'db self, database: &'db Self::DB) -> Option<&'db Expr>
    where
        Self: 'db,
    {
        (*self).check_expression(database)
    }

    fn check_functions<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Function> {
        (*self).check_functions(database)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{ast::CreatePolicyCommand, dialect::GenericDialect};

    use super::*;
    use crate::{
        structs::ParserDB,
        traits::{DatabaseLike, FunctionLike, TableLike},
    };

    #[test]
    fn test_policy_ref_implementation() {
        let sql = r"
            CREATE TABLE my_table (id INT);
            CREATE FUNCTION my_func() RETURNS BOOLEAN AS 'SELECT true';
            CREATE FUNCTION check_func() RETURNS BOOLEAN AS 'SELECT true';
            CREATE POLICY my_policy ON my_table
                FOR SELECT
                TO PUBLIC
                USING (id > 0 AND my_func())
                WITH CHECK (id < 10 AND check_func());
        ";
        let db = ParserDB::parse(sql, &GenericDialect {}).expect("Failed to parse SQL");
        let table = db.table(None, "my_table").expect("Table not found");
        let policy = table.policies(&db).next().expect("Policy not found");

        // Use reference to policy
        let policy_ref = &policy;

        assert_eq!(policy_ref.name(), "my_policy");

        let policy_table = policy_ref.table(&db);
        assert_eq!(policy_table.table_name(), "my_table");

        assert_eq!(policy_ref.command(), CreatePolicyCommand::Select);

        let roles: Vec<_> = policy_ref.roles(&db).collect();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].to_string(), "PUBLIC");

        let using_expr = policy_ref.using_expression(&db);
        assert!(using_expr.is_some());
        let using_str = using_expr.unwrap().to_string();
        assert!(using_str.contains("id > 0"));
        assert!(using_str.contains("my_func()"));

        let using_funcs: Vec<_> = policy_ref.using_functions(&db).collect();
        assert_eq!(using_funcs.len(), 1);
        assert_eq!(using_funcs[0].name(), "my_func");

        let check_expr = policy_ref.check_expression(&db);
        assert!(check_expr.is_some());
        let check_str = check_expr.unwrap().to_string();
        assert!(check_str.contains("id < 10"));
        assert!(check_str.contains("check_func()"));

        let check_funcs: Vec<_> = policy_ref.check_functions(&db).collect();
        assert_eq!(check_funcs.len(), 1);
        assert_eq!(check_funcs[0].name(), "check_func");
    }
}
