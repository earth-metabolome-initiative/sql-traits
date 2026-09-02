//! Role lifecycle references follow PostgreSQL identity changes.
#![allow(clippy::expect_used)]

use sql_traits::{errors::Error, prelude::*};
use sqlparser::dialect::PostgreSqlDialect;

fn parse(sql: &str) -> Result<ParserDB, Error> {
    ParserDB::parse::<PostgreSqlDialect>(sql)
}

#[test]
fn rename_repoints_every_modeled_role_reference() {
    let database = parse(
        "CREATE ROLE App_Owner BYPASSRLS;
         CREATE ROLE Child_Role IN ROLE App_Owner;
         CREATE TABLE docs (id INT);
         ALTER TABLE docs OWNER TO App_Owner;
         CREATE SCHEMA created_schema AUTHORIZATION App_Owner;
         CREATE SCHEMA altered_schema;
         ALTER SCHEMA altered_schema OWNER TO App_Owner;
         CREATE POLICY docs_read ON docs TO App_Owner USING (true);
         CREATE FUNCTION f() RETURNS INT AS 'SELECT 1';
         ALTER FUNCTION f() OWNER TO App_Owner;
         GRANT SELECT ON docs TO App_Owner;
         ALTER ROLE App_Owner RENAME TO \"Renamed_Owner\";",
    )
    .expect("schema builds");

    let renamed = database.role("Renamed_Owner").expect("renamed role resolves");
    let docs = database.table(None, "docs").expect("docs exists");
    let function = database.function(None, "f").expect("f exists");
    let created_schema = database.schema("created_schema").expect("created_schema exists");
    let altered_schema = database.schema("altered_schema").expect("altered_schema exists");
    let child = database.role("child_role").expect("child resolves");
    let memberships: Vec<_> = child.member_of(&database).map(RoleLike::stored_name).collect();
    let policies: Vec<_> = renamed.policies(&database).map(PolicyLike::name).collect();

    assert!(database.role("app_owner").is_none());
    assert_eq!(docs.owner(&database), Ok(Some("Renamed_Owner")));
    assert_eq!(function.owner(&database), Ok(Some("Renamed_Owner")));
    assert_eq!(created_schema.authorization(), Some("Renamed_Owner"));
    assert_eq!(altered_schema.authorization(), Some("Renamed_Owner"));
    assert_eq!(memberships, ["Renamed_Owner"]);
    assert_eq!(policies, ["docs_read"]);
    assert_eq!(docs.can_select(renamed, &database), Ok(true));
}

#[test]
fn drop_refuses_every_blocking_role_dependency() {
    let cases = [
        (
            "table owner",
            "CREATE ROLE app_owner;
             CREATE TABLE docs (id INT);
             ALTER TABLE docs OWNER TO app_owner;
             DROP ROLE app_owner;",
        ),
        (
            "function owner",
            "CREATE ROLE app_owner;
             CREATE FUNCTION f() RETURNS INT AS 'SELECT 1';
             ALTER FUNCTION f() OWNER TO app_owner;
             DROP ROLE app_owner;",
        ),
        (
            "schema owner",
            "CREATE ROLE app_owner;
             CREATE SCHEMA app AUTHORIZATION app_owner;
             DROP ROLE app_owner;",
        ),
        (
            "policy target",
            "CREATE ROLE app_owner;
             CREATE TABLE docs (id INT);
             CREATE POLICY docs_read ON docs TO app_owner USING (true);
             DROP ROLE app_owner;",
        ),
        (
            "grant target",
            "CREATE ROLE app_owner;
             CREATE TABLE docs (id INT);
             GRANT SELECT ON docs TO app_owner;
             DROP ROLE app_owner;",
        ),
    ];

    for (dependency, sql) in cases {
        let error = parse(sql).expect_err("referenced role is not dropped");
        assert!(
            matches!(&error, Error::RoleReferenced { role_name } if role_name == "app_owner"),
            "{dependency} produced {error:?}"
        );
    }
}

#[test]
fn drop_removes_membership_before_role_recreation() {
    let database = parse(
        "CREATE ROLE parent_role;
         CREATE ROLE child_role IN ROLE parent_role;
         DROP ROLE parent_role;
         CREATE ROLE parent_role;",
    )
    .expect("schema builds");
    let child = database.role("child_role").expect("child resolves");

    assert_eq!(child.member_of(&database).count(), 0);
}
