//! Implement the [`ForeignKeyConstraint`] trait for the `sqlparser` crate's

use alloc::{string::ToString, vec::Vec};

use sqlparser::ast::{ConstraintReferenceMatchKind, CreateTable, ForeignKeyConstraint};

use crate::{
    structs::{ParserDB, TableAttribute},
    traits::{ForeignKeyLike, Metadata, database::DatabaseLike, table::TableLike},
    utils::last_str,
};

impl Metadata for TableAttribute<CreateTable, ForeignKeyConstraint> {
    type Meta = ();
}

impl ForeignKeyLike for TableAttribute<CreateTable, ForeignKeyConstraint> {
    type DB = ParserDB;

    #[inline]
    fn foreign_key_name(&self) -> Option<&str> {
        self.attribute().name.as_ref().map(|s| s.value.as_str())
    }

    #[inline]
    fn host_table<'db>(
        &'db self,
        _database: &'db Self::DB,
    ) -> &'db <Self::DB as DatabaseLike>::Table
    where
        Self: 'db,
    {
        self.table()
    }

    fn referenced_table<'db>(
        &self,
        database: &'db Self::DB,
    ) -> &'db <Self::DB as DatabaseLike>::Table {
        let referenced_table_name = last_str(&self.attribute().foreign_table);
        database
            .tables()
            .find(|table: &&<Self::DB as DatabaseLike>::Table| {
                table.table_name() == referenced_table_name
            })
            .unwrap_or_else(|| {
                let host_table = self.host_table(database);
                panic!("Referenced table `{referenced_table_name}` not found for foreign key in table `{}`", host_table.table_name())
            })
    }

    #[inline]
    fn on_delete_cascade(&self, _database: &Self::DB) -> bool {
        matches!(self.attribute().on_delete, Some(sqlparser::ast::ReferentialAction::Cascade))
    }

    #[inline]
    fn match_kind(&self, _database: &Self::DB) -> ConstraintReferenceMatchKind {
        self.attribute().match_kind.unwrap_or(ConstraintReferenceMatchKind::Simple)
    }

    fn host_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>
    where
        Self: 'db,
    {
        let host_table = self.host_table(database);
        self.attribute().columns.iter().map(move |col_name| {
            host_table
                .columns(database)
                .find(|col: &&<Self::DB as DatabaseLike>::Column| &col.attribute().name == col_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Host column `{}` not found in table `{}` for foreign key, options: {:?}",
                        col_name,
                        host_table.table_name(),
                        host_table
                            .columns(database)
                            .map(|c| c.attribute().name.to_string())
                            .collect::<Vec<_>>()
                    )
                })
        })
    }

    fn referenced_columns<'db>(
        &'db self,
        database: &'db Self::DB,
    ) -> impl Iterator<Item = &'db <Self::DB as DatabaseLike>::Column>
    where
        Self: 'db,
    {
        let host_table = self.host_table(database);
        let referenced_table = self.referenced_table(database);
        self.attribute().referred_columns.iter().map(move |col_name| {
            referenced_table
                .columns(database)
                .find(|col: &&<Self::DB as DatabaseLike>::Column| &col.attribute().name == col_name)
                .unwrap_or_else(|| {
                    panic!(
                        "Referenced column `{}` in table `{}` not found in table `{}` for foreign key",
                        col_name,
                        host_table.table_name(),
                        referenced_table.table_name()
                    )
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use crate::{
        prelude::ParserDB,
        traits::{ColumnLike, DatabaseLike, ForeignKeyLike, TableLike},
    };

    /// An unnamed inline `REFERENCES` clause produces a foreign key whose
    /// `foreign_key_name()` returns `None`.
    #[test]
    fn test_unnamed_inline_foreign_key_has_no_name() {
        let sql = "
            CREATE TABLE parent (id INT PRIMARY KEY);
            CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let child = db.table(None, "child").unwrap();
        let fk = child.foreign_keys(&db).next().expect("FK should exist");
        assert!(fk.foreign_key_name().is_none(), "inline REFERENCES has no name");
    }

    /// `ON DELETE SET NULL` is not CASCADE, so `on_delete_cascade()` is false.
    #[test]
    fn test_on_delete_set_null_is_not_cascade() {
        let sql = "
            CREATE TABLE parent (id INT PRIMARY KEY);
            CREATE TABLE child (
                id INT PRIMARY KEY,
                parent_id INT,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL
            );
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let child = db.table(None, "child").unwrap();
        let fk = child.foreign_keys(&db).next().expect("FK should exist");
        assert!(!fk.on_delete_cascade(&db));
    }

    /// A multi-column FK reports both host and referenced columns in
    /// declaration order.
    #[test]
    fn test_multi_column_foreign_key_columns() {
        let sql = "
            CREATE TABLE parent (a INT, b INT, PRIMARY KEY (a, b));
            CREATE TABLE child (
                x INT,
                y INT,
                CONSTRAINT fk FOREIGN KEY (x, y) REFERENCES parent(a, b)
            );
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let child = db.table(None, "child").unwrap();
        let fk = child.foreign_keys(&db).next().expect("FK should exist");

        let host_names: Vec<&str> = fk.host_columns(&db).map(ColumnLike::column_name).collect();
        let ref_names: Vec<&str> =
            fk.referenced_columns(&db).map(ColumnLike::column_name).collect();
        assert_eq!(host_names, vec!["x", "y"]);
        assert_eq!(ref_names, vec!["a", "b"]);
    }

    /// A self-referential FK targets the same table as its host.
    #[test]
    fn test_self_referential_foreign_key() {
        let sql = "
            CREATE TABLE t (
                id INT PRIMARY KEY,
                parent_id INT REFERENCES t(id)
            );
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let t = db.table(None, "t").unwrap();
        let fk = t.foreign_keys(&db).next().expect("FK should exist");

        assert_eq!(fk.host_table(&db).table_name(), "t");
        assert_eq!(fk.referenced_table(&db).table_name(), "t");
    }

    /// `match_kind()` defaults to `Simple` when no `MATCH` clause is given.
    #[test]
    fn test_match_kind_defaults_to_simple() {
        use sqlparser::ast::ConstraintReferenceMatchKind;

        let sql = "
            CREATE TABLE parent (id INT PRIMARY KEY);
            CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id));
        ";
        let db = ParserDB::parse::<GenericDialect>(sql).expect("parse");
        let child = db.table(None, "child").unwrap();
        let fk = child.foreign_keys(&db).next().expect("FK should exist");
        assert!(matches!(fk.match_kind(&db), ConstraintReferenceMatchKind::Simple));
    }
}
