//! Copying a column from one table's declaration into another's.
//!
//! Two constructs take a column from another table: `INHERITS` and
//! `PARTITION OF`, which keep a lasting link to the parent, and `LIKE`, which
//! duplicates once and links nothing. They keep different amounts of the
//! original declaration, but they agree on the part that is easy to get
//! wrong: an option that would create a constraint or a sequence of the
//! copy's own is withheld, while the `NOT NULL` such an option implied is
//! kept.

use alloc::vec::Vec;

use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef};

use crate::utils::is_identity;

/// Whether the option only holds because of a constraint the copy will not
/// receive, yet still forces the column to hold a value.
///
/// A primary key and an identity both imply `NOT NULL`, and PostgreSQL keeps
/// that implication in the copy even when it withholds the option that
/// carried it.
fn implies_not_null(column: &ColumnDef) -> bool {
    column.options.iter().any(|option| {
        matches!(option.option, ColumnOption::PrimaryKey(_)) || is_identity(&option.option)
    })
}

/// Copies a column, keeping the options `keep` accepts.
///
/// A `NOT NULL` that survives only as an implication of a withheld option is
/// added back, so the copy still refuses a null the original refused.
pub(super) fn copy_column(column: &ColumnDef, keep: impl Fn(&ColumnOption) -> bool) -> ColumnDef {
    let mut options: Vec<ColumnOptionDef> =
        column.options.iter().filter(|option| keep(&option.option)).cloned().collect();

    let holds_not_null =
        options.iter().any(|option| matches!(option.option, ColumnOption::NotNull));
    if implies_not_null(column) && !holds_not_null {
        options.push(ColumnOptionDef { name: None, option: ColumnOption::NotNull });
    }

    ColumnDef { name: column.name.clone(), data_type: column.data_type.clone(), options }
}
