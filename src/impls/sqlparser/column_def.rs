//! Submodule implementing the [`ColumnLike`] trait for `sqlparser`'s
//! [`ColumnDef`] struct.

use alloc::string::{String, ToString};

use sqlparser::ast::{ColumnDef, CreateTable};

use crate::{
    structs::{ParserDB, TableAttribute},
    traits::{ColumnLike, DatabaseLike, Metadata},
    utils::normalize_sqlparser_type,
};

const GENERATED_TYPES: &[&str] = &["SERIAL", "BIGSERIAL", "SMALLSERIAL"];

impl Metadata for TableAttribute<CreateTable, ColumnDef> {
    type Meta = ();
}

impl ColumnLike for TableAttribute<CreateTable, ColumnDef> {
    type DB = ParserDB;

    #[inline]
    fn column_name(&self) -> &str {
        self.attribute().name.value.as_str()
    }

    #[inline]
    fn column_name_is_quoted(&self) -> bool {
        self.attribute().name.quote_style.is_some()
    }

    #[inline]
    fn column_doc<'db>(&'db self, database: &'db Self::DB) -> Option<&'db str>
    where
        Self: 'db,
    {
        database
            .table_metadata(self.table())
            .expect("Table must exist in database")
            .table_doc()
            .and_then(|d| d.column(self.column_name()).ok().and_then(|c| c.doc()))
    }

    #[inline]
    fn data_type<'db>(&'db self, _database: &'db Self::DB) -> &'db str {
        normalize_sqlparser_type(&self.attribute().data_type)
    }

    #[inline]
    fn is_generated(&self) -> bool {
        GENERATED_TYPES.contains(&self.attribute().data_type.to_string().as_str())
    }

    #[inline]
    fn is_nullable(&self, database: &Self::DB) -> bool {
        !self
            .attribute()
            .options
            .iter()
            .any(|opt| matches!(opt.option, sqlparser::ast::ColumnOption::NotNull))
            && !self.is_primary_key(database)
    }

    #[inline]
    fn default_value(&self) -> Option<String> {
        self.attribute().options.iter().find_map(|opt| {
            if let sqlparser::ast::ColumnOption::Default(expr) = &opt.option {
                Some(expr.to_string())
            } else {
                None
            }
        })
    }

    #[inline]
    fn table<'a>(&'a self, _database: &'a Self::DB) -> &'a <Self::DB as DatabaseLike>::Table
    where
        Self: 'a,
    {
        self.table()
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::{
        GenericDialect, MsSqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect,
    };

    use crate::{
        prelude::ParserDB,
        traits::{DatabaseLike, DialectLike, TableLike, TypeMatch, TypeMatchLike},
    };

    fn parse_with<D: sqlparser::dialect::Dialect + Default + 'static>(sql: &str) -> ParserDB {
        ParserDB::parse::<D>(sql).expect("parse")
    }

    fn bool_of(db: &ParserDB, col: &str) -> TypeMatch {
        let table = db.table(None, "t").expect("table t exists");
        let column = table.column(col, db).expect("column exists");
        db.dialect().is_bool(db, column)
    }

    fn uuid_of(db: &ParserDB, col: &str) -> TypeMatch {
        let table = db.table(None, "t").expect("table t exists");
        let column = table.column(col, db).expect("column exists");
        db.dialect().is_uuid(db, column)
    }

    // ---------------- is_bool ----------------

    #[test]
    fn is_bool_mysql_tinyint_1_is_yes() {
        let db = parse_with::<MySqlDialect>("CREATE TABLE t (flag TINYINT(1));");
        assert_eq!(bool_of(&db, "flag"), TypeMatch::Yes);
    }

    #[test]
    fn is_bool_mysql_wider_tinyint_is_no() {
        for width in ["TINYINT", "TINYINT(2)", "TINYINT(4)"] {
            let sql = format!("CREATE TABLE t (flag {width});");
            let db = parse_with::<MySqlDialect>(&sql);
            assert_eq!(bool_of(&db, "flag"), TypeMatch::No, "MySQL {width}");
        }
    }

    #[test]
    fn is_bool_mysql_bool_keyword_is_yes() {
        for spelling in ["BOOL", "BOOLEAN"] {
            let sql = format!("CREATE TABLE t (flag {spelling});");
            let db = parse_with::<MySqlDialect>(&sql);
            assert_eq!(bool_of(&db, "flag"), TypeMatch::Yes, "MySQL {spelling}");
        }
    }

    #[test]
    fn is_bool_postgres_tinyint_1_is_no() {
        let db = parse_with::<PostgreSqlDialect>("CREATE TABLE t (flag TINYINT(1));");
        assert_eq!(bool_of(&db, "flag"), TypeMatch::No);
    }

    #[test]
    fn is_bool_postgres_and_mysql_agree_on_boolean_keyword() {
        let sql = "CREATE TABLE t (flag BOOLEAN);";
        assert_eq!(bool_of(&parse_with::<PostgreSqlDialect>(sql), "flag"), TypeMatch::Yes);
        assert_eq!(bool_of(&parse_with::<MySqlDialect>(sql), "flag"), TypeMatch::Yes);
    }

    #[test]
    fn is_bool_sqlite_integer_is_maybe() {
        // SQLite has no boolean; INTEGER carries 0/1 booleans by convention.
        // Users must call `.is_yes()` vs `.is_maybe()` explicitly.
        let db = parse_with::<SQLiteDialect>("CREATE TABLE t (flag INTEGER);");
        let m = bool_of(&db, "flag");
        assert!(m.is_maybe(), "expected Maybe, got {m:?}");
        assert!(!m.is_yes());
        assert!(!m.is_no());
    }

    #[test]
    fn is_bool_sqlite_boolean_keyword_is_yes() {
        // sqlparser's SQLiteDialect still parses `BOOLEAN` into
        // DataType::Boolean, and the author's declared intent is
        // unambiguous even though SQLite stores it as numeric affinity.
        let db = parse_with::<SQLiteDialect>("CREATE TABLE t (flag BOOLEAN);");
        assert_eq!(bool_of(&db, "flag"), TypeMatch::Yes);
    }

    #[test]
    fn is_bool_sqlite_text_is_no() {
        let db = parse_with::<SQLiteDialect>("CREATE TABLE t (flag TEXT);");
        assert_eq!(bool_of(&db, "flag"), TypeMatch::No);
    }

    #[test]
    fn is_bool_generic_tinyint_1_is_yes() {
        let db = parse_with::<GenericDialect>("CREATE TABLE t (flag TINYINT(1));");
        assert_eq!(bool_of(&db, "flag"), TypeMatch::Yes);
    }

    // ---------------- is_uuid ----------------

    #[test]
    fn is_uuid_postgres_uuid_keyword_is_yes() {
        let db = parse_with::<PostgreSqlDialect>("CREATE TABLE t (id UUID);");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::Yes);
    }

    #[test]
    fn is_uuid_mssql_uniqueidentifier_is_yes() {
        let db = parse_with::<MsSqlDialect>("CREATE TABLE t (id UNIQUEIDENTIFIER);");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::Yes);
    }

    #[test]
    fn is_uuid_mysql_char_36_is_maybe() {
        let db = parse_with::<MySqlDialect>("CREATE TABLE t (id CHAR(36));");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::Maybe);
    }

    #[test]
    fn is_uuid_mysql_binary_16_is_maybe() {
        let db = parse_with::<MySqlDialect>("CREATE TABLE t (id BINARY(16));");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::Maybe);
    }

    #[test]
    fn is_uuid_mysql_char_other_length_is_no() {
        let db = parse_with::<MySqlDialect>("CREATE TABLE t (id CHAR(10));");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::No);
    }

    #[test]
    fn is_uuid_postgres_char_36_is_no() {
        // Postgres has native UUID; CHAR(36) is not a UUID convention.
        let db = parse_with::<PostgreSqlDialect>("CREATE TABLE t (id CHAR(36));");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::No);
    }

    #[test]
    fn is_uuid_sqlite_text_is_maybe() {
        let db = parse_with::<SQLiteDialect>("CREATE TABLE t (id TEXT);");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::Maybe);
    }

    #[test]
    fn is_uuid_sqlite_integer_is_no() {
        let db = parse_with::<SQLiteDialect>("CREATE TABLE t (id INTEGER);");
        assert_eq!(uuid_of(&db, "id"), TypeMatch::No);
    }
}
