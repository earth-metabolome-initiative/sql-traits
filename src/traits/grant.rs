//! Submodule providing a trait for describing SQL Grant-like entities.

use std::{borrow::Borrow, fmt::Debug, hash::Hash};

use sqlparser::ast::{Action, Grantee, ObjectName};

use crate::traits::{DatabaseLike, Metadata};

/// A trait for types that can be treated as SQL grants.
///
/// Grants in SQL are used to assign privileges on database objects to
/// roles/users. A single grant represents one or more privileges on one or more
/// objects assigned to one or more grantees.
pub trait GrantLike:
    Debug
    + Clone
    + Hash
    + Ord
    + Eq
    + Metadata
    + Borrow<<<Self as GrantLike>::DB as DatabaseLike>::Grant>
{
    /// The database type the grant belongs to.
    type DB: DatabaseLike;

    /// Returns an iterator over the privileges (actions) granted.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::ast::Action;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// GRANT SELECT, INSERT ON my_table TO my_role;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let privileges: Vec<_> = grant.privileges().collect();
    /// assert_eq!(privileges.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn privileges(&self) -> impl Iterator<Item = &Action>;

    /// Returns whether this grant represents ALL PRIVILEGES.
    ///
    /// When a grant uses `ALL PRIVILEGES`, the `privileges()` iterator
    /// will be empty. Use this method to check for that case.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// GRANT ALL PRIVILEGES ON my_table TO admin;
    /// GRANT SELECT ON my_table TO reader;
    /// ",
    /// )?;
    /// let grants: Vec<_> = db.grants().collect();
    /// let all_grant = grants.iter().find(|g| g.is_all_privileges()).unwrap();
    /// let select_grant = grants.iter().find(|g| !g.is_all_privileges()).unwrap();
    /// assert!(all_grant.privileges().next().is_none()); // empty for ALL
    /// assert!(select_grant.privileges().next().is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn is_all_privileges(&self) -> bool;

    /// Returns an iterator over the tables this grant applies to.
    ///
    /// This method handles both direct table grants (`GRANT ... ON table1,
    /// table2`) and schema-wide table grants (`GRANT ... ON ALL TABLES IN
    /// SCHEMA`). Returns an empty iterator if this grant does not apply to
    /// tables.
    ///
    /// # Example
    ///
    /// Direct table grant:
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE users (id INT);
    /// CREATE TABLE posts (id INT);
    /// GRANT SELECT ON users, posts TO reader;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let tables: Vec<_> = grant.tables(&db).collect();
    /// assert_eq!(tables.len(), 2);
    /// assert!(tables.iter().any(|t| t.table_name() == "users"));
    /// assert!(tables.iter().any(|t| t.table_name() == "posts"));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Schema-wide table grant:
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "
    /// CREATE TABLE public.users (id INT);
    /// CREATE TABLE public.posts (id INT);
    /// CREATE TABLE other_schema.data (id INT);
    /// GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let tables: Vec<_> = grant.tables(&db).collect();
    /// // Only tables in the 'public' schema are included
    /// assert_eq!(tables.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn tables<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Table>;

    /// Returns an iterator over the schemas this grant applies to.
    ///
    /// Returns `None` if this grant does not apply to specific schemas.
    ///
    /// Note: A future version may introduce a `SchemaLike` trait to return
    /// schema objects instead of `ObjectName`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>("GRANT USAGE ON SCHEMA public TO app_user;")?;
    /// let grant = db.grants().next().unwrap();
    /// assert!(grant.schemas().is_some());
    /// # Ok(())
    /// # }
    /// ```
    fn schemas(&self) -> Option<impl Iterator<Item = &ObjectName>>;

    /// Returns an iterator over the functions this grant applies to.
    ///
    /// This method handles both direct function grants (`GRANT ... ON FUNCTION
    /// func`) and schema-wide function grants (`GRANT ... ON ALL FUNCTIONS
    /// IN SCHEMA`). Returns an empty iterator if this grant does not apply
    /// to functions.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "
    /// CREATE FUNCTION add_one(x INT) RETURNS INT AS 'SELECT x + 1;';
    /// CREATE FUNCTION double_it(x INT) RETURNS INT AS 'SELECT x * 2;';
    /// GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO app_user;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// // Note: functions() returns functions based on schema matching
    /// let funcs: Vec<_> = grant.functions(&db).collect();
    /// // Results depend on whether functions have schema info
    /// # Ok(())
    /// # }
    /// ```
    fn functions<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Function>;

    /// Returns an iterator over the grantees (roles/users receiving the grant).
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// GRANT SELECT ON my_table TO role1, role2;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// assert_eq!(grant.grantees().count(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn grantees(&self) -> impl Iterator<Item = &Grantee>;

    /// Returns whether this grant includes the `WITH GRANT OPTION`.
    ///
    /// When `WITH GRANT OPTION` is specified, the grantee can grant
    /// the same privileges to other roles.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// GRANT SELECT ON my_table TO role1 WITH GRANT OPTION;
    /// GRANT INSERT ON my_table TO role2;
    /// ",
    /// )?;
    /// let grants: Vec<_> = db.grants().collect();
    /// let grant_with_option = grants.iter().find(|g| g.with_grant_option()).unwrap();
    /// let grant_without_option = grants.iter().find(|g| !g.with_grant_option()).unwrap();
    /// assert!(grant_with_option.with_grant_option());
    /// assert!(!grant_without_option.with_grant_option());
    /// # Ok(())
    /// # }
    /// ```
    fn with_grant_option(&self) -> bool;

    /// Returns the role that granted this privilege, if specified.
    ///
    /// This is the `GRANTED BY` clause in PostgreSQL. Note that the `GRANTED
    /// BY` clause cannot use pseudo-roles, only actual database roles.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// CREATE ROLE admin;
    /// GRANT SELECT ON my_table TO app_user GRANTED BY admin;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let grantor = grant.granted_by(&db).unwrap();
    /// assert_eq!(grantor.name(), "admin");
    /// # Ok(())
    /// # }
    /// ```
    fn granted_by<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> Option<&'a <Self::DB as DatabaseLike>::Role>;

    /// Returns an iterator over the columns that have privileges granted.
    ///
    /// Column-level privileges allow granting SELECT, INSERT, UPDATE, or
    /// REFERENCES on specific columns rather than the entire table.
    /// The iterator yields references to column objects from the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT, name TEXT, secret TEXT);
    /// GRANT SELECT (id, name) ON my_table TO app_user;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let table = db.table(None, "my_table").unwrap();
    /// let columns: Vec<_> = grant.privilege_columns(table, &db).collect();
    /// assert_eq!(columns.len(), 2);
    /// # Ok(())
    /// # }
    /// ```
    fn privilege_columns<'a>(
        &'a self,
        table: &'a <Self::DB as DatabaseLike>::Table,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Column>;

    /// Returns whether this grant applies to a specific table.
    ///
    /// # Arguments
    ///
    /// * `table` - The table to check against.
    /// * `database` - The database context.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    ///
    /// let db = ParserDB::parse::<GenericDialect>(
    ///     "
    /// CREATE TABLE table1 (id INT);
    /// CREATE TABLE table2 (id INT);
    /// GRANT SELECT ON table1 TO app_user;
    /// ",
    /// )?;
    /// let table1 = db.table(None, "table1").unwrap();
    /// let table2 = db.table(None, "table2").unwrap();
    /// let grant = db.grants().next().unwrap();
    /// assert!(grant.applies_to_table(table1, &db));
    /// assert!(!grant.applies_to_table(table2, &db));
    /// # Ok(())
    /// # }
    /// ```
    fn applies_to_table(
        &self,
        table: &<Self::DB as DatabaseLike>::Table,
        database: &Self::DB,
    ) -> bool;

    /// Returns whether this grant applies to a specific role.
    ///
    /// # Arguments
    ///
    /// * `role` - The role to check against.
    ///
    /// # Example
    ///
    /// ```rust
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use sql_traits::prelude::*;
    /// use sqlparser::dialect::PostgreSqlDialect;
    ///
    /// let db = ParserDB::parse::<PostgreSqlDialect>(
    ///     "
    /// CREATE TABLE my_table (id INT);
    /// CREATE ROLE app_user;
    /// CREATE ROLE admin;
    /// CREATE ROLE other_user;
    /// GRANT SELECT ON my_table TO app_user, admin;
    /// ",
    /// )?;
    /// let grant = db.grants().next().unwrap();
    /// let app_user = db.role("app_user").unwrap();
    /// let admin = db.role("admin").unwrap();
    /// let other_user = db.role("other_user").unwrap();
    /// assert!(grant.applies_to_role(app_user));
    /// assert!(grant.applies_to_role(admin));
    /// assert!(!grant.applies_to_role(other_user));
    /// # Ok(())
    /// # }
    /// ```
    fn applies_to_role(&self, role: &<Self::DB as DatabaseLike>::Role) -> bool;
}

impl<T: GrantLike> GrantLike for &T
where
    Self: Borrow<<<T as GrantLike>::DB as DatabaseLike>::Grant>,
{
    type DB = T::DB;

    fn privileges(&self) -> impl Iterator<Item = &Action> {
        (*self).privileges()
    }

    fn is_all_privileges(&self) -> bool {
        (*self).is_all_privileges()
    }

    fn tables<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Table> {
        (*self).tables(database)
    }

    fn schemas(&self) -> Option<impl Iterator<Item = &ObjectName>> {
        (*self).schemas()
    }

    fn functions<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Function> {
        (*self).functions(database)
    }

    fn grantees(&self) -> impl Iterator<Item = &Grantee> {
        (*self).grantees()
    }

    fn with_grant_option(&self) -> bool {
        (*self).with_grant_option()
    }

    fn granted_by<'a>(
        &'a self,
        database: &'a Self::DB,
    ) -> Option<&'a <Self::DB as DatabaseLike>::Role> {
        (*self).granted_by(database)
    }

    fn privilege_columns<'a>(
        &'a self,
        table: &'a <Self::DB as DatabaseLike>::Table,
        database: &'a Self::DB,
    ) -> impl Iterator<Item = &'a <Self::DB as DatabaseLike>::Column> {
        (*self).privilege_columns(table, database)
    }

    fn applies_to_table(
        &self,
        table: &<Self::DB as DatabaseLike>::Table,
        database: &Self::DB,
    ) -> bool {
        (*self).applies_to_table(table, database)
    }

    fn applies_to_role(&self, role: &<Self::DB as DatabaseLike>::Role) -> bool {
        (*self).applies_to_role(role)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::{structs::ParserDB, traits::DatabaseLike};

    #[test]
    fn test_grant_ref_implementation() {
        let sql = r"
            CREATE TABLE my_table (id INT);
            CREATE ROLE app_user;
            GRANT SELECT, INSERT ON my_table TO app_user WITH GRANT OPTION;
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");
        let grant = db.grants().next().expect("Grant not found");

        // Use reference to grant
        let grant_ref = &grant;

        let privileges: Vec<_> = grant_ref.privileges().collect();
        assert_eq!(privileges.len(), 2);

        assert!(grant_ref.with_grant_option());

        let grantees: Vec<_> = grant_ref.grantees().collect();
        assert_eq!(grantees.len(), 1);

        let table = db.table(None, "my_table").expect("Table not found");
        assert!(grant_ref.applies_to_table(table, &db));
        let app_user = db.role("app_user").expect("Role not found");
        assert!(grant_ref.applies_to_role(app_user));
    }
}
