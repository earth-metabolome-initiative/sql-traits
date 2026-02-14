//! Anti-regression tests for crashes discovered by fuzzing.
//!
//! This test automatically discovers crash files from the honggfuzz workspace
//! and verifies that they no longer cause panics.

use std::path::Path;

use arbitrary::{Arbitrary, Unstructured};
use sql_traits::prelude::ParserDB;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

/// Test that parsing does not panic for both dialects.
/// Provides context about which crash file caused the failure.
fn should_not_panic_with_context(sql: &str, crash_file: &Path) {
    use std::panic;

    let result = panic::catch_unwind(|| {
        let _ = ParserDB::parse::<GenericDialect>(sql);
        let _ = ParserDB::parse::<PostgreSqlDialect>(sql);
    });

    if let Err(e) = result {
        panic!("Crash file {} caused a panic!\nSQL: {sql:?}\nPanic: {e:?}", crash_file.display());
    }
}

/// Copies the 'SIGABRT' crash files from the honggfuzz workspace
/// to the 'tests/fuzz_dialect' directory for testing and collect
/// a regression tests suite over time.
fn copy_crash_files() {
    let toml_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let crash_dir = Path::new(&toml_dir).join("fuzz/hfuzz_workspace/fuzz_dialect");
    let test_dir = Path::new(&toml_dir).join("tests/fuzz_dialect");

    if !test_dir.exists() {
        std::fs::create_dir(&test_dir).unwrap();
    }

    for entry in std::fs::read_dir(crash_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.file_name().unwrap().to_str().unwrap().starts_with("SIGABRT") {
            let file_name = path.file_name().unwrap();
            let dest_path = test_dir.join(file_name);
            std::fs::copy(&path, &dest_path).unwrap();
        }
    }
}

/// Discover and test all crash files from honggfuzz workspace.
#[test]
fn test_fuzz_crashes() {
    copy_crash_files();

    // We load the SQL statements from the 'tests/fuzz_dialect' directory, which
    // should contain the crash files copied from the honggfuzz workspace.
    let toml_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let test_dir = Path::new(&toml_dir).join("tests/fuzz_dialect");
    let crash_files: Vec<_> = std::fs::read_dir(test_dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_file() {
                let bytes = std::fs::read(&path).ok()?;
                Some((path, bytes))
            } else {
                None
            }
        })
        .collect();

    for (path, bytes) in crash_files {
        // Use arbitrary to extract the string exactly as honggfuzz does
        let mut unstructured = Unstructured::new(&bytes);
        let sql: &str = match <&str>::arbitrary(&mut unstructured) {
            Ok(s) => s,
            Err(_) => {
                // If arbitrary can't extract a valid string, skip this file
                // (the crash was likely in the arbitrary extraction itself)
                continue;
            }
        };

        // This should NOT panic - if it does, the test fails
        should_not_panic_with_context(sql, &path);
    }
}
