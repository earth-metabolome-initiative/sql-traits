//! Implementation of the `RoleLike` trait for sqlparser's `CreateRole` type.

use sqlparser::ast::CreateRole;

use crate::{
    structs::ParserDB,
    traits::{DatabaseLike, Metadata, PolicyLike, RoleLike},
    utils::last_str,
};

impl Metadata for CreateRole {
    type Meta = ();
}

impl RoleLike for CreateRole {
    type DB = ParserDB;

    fn name(&self) -> &str {
        last_str(self.names.first().expect("CREATE ROLE must have a name"))
    }

    fn is_superuser(&self) -> bool {
        self.superuser == Some(true)
    }

    fn can_create_db(&self) -> bool {
        self.create_db == Some(true)
    }

    fn can_create_role(&self) -> bool {
        self.create_role == Some(true)
    }

    fn inherits(&self) -> bool {
        // Default is INHERIT in PostgreSQL, so we check if explicitly set to false
        self.inherit != Some(false)
    }

    fn can_login(&self) -> bool {
        self.login == Some(true)
    }

    fn can_bypass_rls(&self) -> bool {
        self.bypassrls == Some(true)
    }

    fn is_replication(&self) -> bool {
        self.replication == Some(true)
    }

    fn connection_limit(&self) -> Option<i32> {
        self.connection_limit.as_ref().and_then(|expr| {
            if let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Number(n, _),
                ..
            }) = expr
            {
                n.parse().ok()
            } else {
                None
            }
        })
    }

    fn member_of<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Role> {
        // IN ROLE clause specifies roles this role is a member of
        self.in_role.iter().filter_map(move |role_ident| database.role(&role_ident.value))
    }

    fn policies<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Policy> {
        let role_name = self.name();
        database.policies().filter(move |policy| {
            policy.roles(database).any(|owner| owner_matches_role(owner, role_name))
        })
    }
}

/// Helper function to check if an Owner matches a role name.
fn owner_matches_role(owner: &sqlparser::ast::Owner, role_name: &str) -> bool {
    match owner {
        sqlparser::ast::Owner::Ident(ident) => ident.value == role_name,
        sqlparser::ast::Owner::CurrentUser
        | sqlparser::ast::Owner::CurrentRole
        | sqlparser::ast::Owner::SessionUser => false,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use crate::{structs::ParserDB, traits::RoleLike};

    /// Helper to parse SQL using PostgreSQL dialect
    fn parse_postgres(sql: &str) -> ParserDB {
        let dialect = PostgreSqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql).unwrap();
        ParserDB::from_statements(statements, "test".to_string()).unwrap()
    }

    #[test]
    fn test_basic_role() {
        let db = ParserDB::parse("CREATE ROLE test_role;", &PostgreSqlDialect {}).unwrap();
        let role = db.role("test_role").unwrap();

        assert_eq!(role.name(), "test_role");
        assert!(!role.is_superuser());
        assert!(!role.can_create_db());
        assert!(!role.can_create_role());
        assert!(role.inherits()); // Default is INHERIT
        assert!(!role.can_login());
        assert!(!role.can_bypass_rls());
        assert!(!role.is_replication());
        assert!(role.connection_limit().is_none());
    }

    #[test]
    fn test_role_with_all_options() {
        let db = parse_postgres(
            "CREATE ROLE admin SUPERUSER CREATEDB CREATEROLE LOGIN BYPASSRLS REPLICATION CONNECTION LIMIT 10;",
        );
        let role = db.role("admin").unwrap();

        assert!(role.is_superuser());
        assert!(role.can_create_db());
        assert!(role.can_create_role());
        assert!(role.can_login());
        assert!(role.can_bypass_rls());
        assert!(role.is_replication());
        assert_eq!(role.connection_limit(), Some(10));
    }

    #[test]
    fn test_role_noinherit() {
        let db = parse_postgres("CREATE ROLE noinherit_role NOINHERIT;");
        let role = db.role("noinherit_role").unwrap();

        assert!(!role.inherits());
    }

    #[test]
    fn test_role_membership() {
        let db = parse_postgres(
            r"
            CREATE ROLE parent1;
            CREATE ROLE parent2;
            CREATE ROLE child IN ROLE parent1, parent2;
        ",
        );

        let child = db.role("child").unwrap();
        let memberships: Vec<_> = child.member_of(&db).collect();

        assert_eq!(memberships.len(), 2);
        let names: Vec<_> = memberships.iter().map(RoleLike::name).collect();
        assert!(names.contains(&"parent1"));
        assert!(names.contains(&"parent2"));
    }

    #[test]
    fn test_role_policies() {
        let db = parse_postgres(
            r"
            CREATE ROLE my_role;
            CREATE TABLE t1 (id INT);
            CREATE TABLE t2 (id INT);
            CREATE POLICY p1 ON t1 TO my_role USING (true);
            CREATE POLICY p2 ON t2 TO my_role USING (true);
            CREATE POLICY p3 ON t1 TO PUBLIC USING (true);
        ",
        );

        let role = db.role("my_role").unwrap();
        let policies: Vec<_> = role.policies(&db).collect();

        assert_eq!(policies.len(), 2);
    }
}
