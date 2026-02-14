//! Anti-regression tests for crashes discovered by fuzzing.
//!
//! This test automatically discovers crash files from the honggfuzz workspace
//! and verifies that they no longer cause panics.

use std::path::Path;

use sql_traits::prelude::ParserDB;
use sqlparser::dialect::{GenericDialect, PostgreSqlDialect};

/// Test that parsing does not panic for both dialects.
fn should_not_panic(sql: &str) {
    let _ = ParserDB::parse::<GenericDialect>(sql);
    let _ = ParserDB::parse::<PostgreSqlDialect>(sql);
}

/// Discover and test all crash files from honggfuzz workspace.
#[test]
fn test_fuzz_crashes() {
    let workspace_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fuzz")
        .join("honggfuzz_workspace")
        .join("fuzz_dialect");

    if !workspace_dir.exists() {
        // No fuzzing has been run yet, nothing to test
        return;
    }

    let mut crash_count = 0;

    // Iterate over all fuzz target directories
    if let Ok(entries) = std::fs::read_dir(&workspace_dir) {
        for entry in entries.flatten() {
            let crashes_dir = entry.path().join("crashes");
            if crashes_dir.is_dir() {
                if let Ok(crashes) = std::fs::read_dir(&crashes_dir) {
                    for crash in crashes.flatten() {
                        let crash_path = crash.path();
                        if crash_path.is_file() {
                            // Read crash file content as bytes and try to interpret as UTF-8
                            if let Ok(bytes) = std::fs::read(&crash_path) {
                                let sql = String::from_utf8_lossy(&bytes);
                                // This should not panic - errors are OK
                                should_not_panic(&sql);
                                crash_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    if crash_count > 0 {
        println!("Tested {crash_count} crash file(s) without panics");
    }
}
