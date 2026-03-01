//! Thread-safety integration tests for `ParserDB`.

use std::sync::Arc;

use sql_traits::prelude::*;

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn parser_db_is_send_sync() {
    assert_send_sync::<ParserDB>();
}

#[test]
fn parser_db_can_be_shared_across_threads() {
    let sql = "
        CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
        CREATE TABLE posts (id INT PRIMARY KEY, user_id INT REFERENCES users(id));
        CREATE FUNCTION user_count() RETURNS INT AS 'SELECT 1;' LANGUAGE sql;
    ";

    let db = ParserDB::parse::<GenericDialect>(sql).expect("Failed to parse SQL");
    let shared_db = Arc::new(db);

    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&shared_db);
        handles.push(std::thread::spawn(move || {
            let users = db.table(None, "users").expect("users table should exist");
            assert_eq!(users.table_name(), "users");

            let posts = db.table(None, "posts").expect("posts table should exist");
            assert_eq!(posts.table_name(), "posts");

            let function = db.function("user_count").expect("function should exist");
            assert_eq!(function.name(), "user_count");

            let table_count = db.tables().count();
            assert_eq!(table_count, 2);
        }));
    }

    for handle in handles {
        handle.join().expect("Thread should complete without panic");
    }
}
