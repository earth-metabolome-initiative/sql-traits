use honggfuzz::fuzz;
use sql_traits::prelude::ParserDB;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

fn main() {
    loop {
        fuzz!(|sql: &str| {
            let _ = ParserDB::parse::<GenericDialect>(sql);
            let _ = ParserDB::parse::<PostgreSqlDialect>(sql);
        });
    }
}
