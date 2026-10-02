#![no_main]

use libfuzzer_sys::fuzz_target;
use sql_traits::prelude::ParserDB;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

fuzz_target!(|sql: &str| {
    if sql.len() > 1_000 {
        // Skip excessively long inputs to avoid timeouts during
        // fuzzing.
        return;
    }

    let _ = ParserDB::parse::<GenericDialect>(sql);
    let _ = ParserDB::parse::<PostgreSqlDialect>(sql);
});
