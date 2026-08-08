//! The columns and checks a table takes from a parent.
//!
//! PostgreSQL gives an `INHERITS` child every column of every parent, and a
//! `PARTITION OF` child every column of the table it partitions, before the
//! columns the child declares itself. Both spellings record the same kind of
//! edge, so both resolve here.
//!
//! Resolution runs once, after the last statement has been applied, so a
//! column a parent gained through `ALTER TABLE` is already in place and the
//! child needs no separate propagation. Parents resolve before children, which
//! makes a grandchild pick up an already complete parent.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use sqlparser::ast::{
    CharacterLength, ColumnDef, ColumnOption, ColumnOptionDef, CreateTable, DataType,
    ExactNumberInfo, Ident, ObjectName, TableConstraint, TimezoneInfo,
};

use super::{
    ParserDBBuilder, StoredTable,
    column_copy::{copy_column, is_identity},
};
use crate::errors::Error;

/// The tables a table takes its shape from, in the order they are written.
///
/// `INHERITS` names any number of parents and `PARTITION OF` names exactly
/// one, and PostgreSQL records both as the same edge.
pub(super) fn parent_names(create_table: &CreateTable) -> impl Iterator<Item = &ObjectName> {
    create_table.inherits.iter().flatten().chain(create_table.partition_of.iter())
}

/// The tables a table takes its shape from, for a node being rewritten.
fn parent_names_mut(create_table: &mut CreateTable) -> impl Iterator<Item = &mut ObjectName> {
    create_table.inherits.iter_mut().flatten().chain(create_table.partition_of.iter_mut())
}

/// Whether the table names `renamed` as a parent.
pub(super) fn names_parent(node: &CreateTable, renamed: &StoredTable) -> bool {
    parent_names(node).any(|name| renamed.named_by(name))
}

/// Rewrites the parent names a table lists when they name the renamed table.
///
/// PostgreSQL keeps the edge across a rename because it records the parent by
/// identity rather than by spelling, so the spelling follows the parent here.
pub(super) fn rewrite_parent_names(
    node: &mut CreateTable,
    renamed: &StoredTable,
    target: &super::RenameTarget,
) {
    for name in parent_names_mut(node) {
        if renamed.named_by(name) {
            target.rewrite(name);
        }
    }
}

/// Every table that inherits from `parent`, directly or through another.
///
/// Ordered children first so that dropping them in order never leaves a child
/// naming a parent that has already gone.
pub(super) fn descendants(builder: &ParserDBBuilder, parent: &StoredTable) -> Vec<StoredTable> {
    let mut found: Vec<StoredTable> = Vec::new();
    let mut frontier: Vec<StoredTable> = alloc::vec![parent.clone()];

    while let Some(current) = frontier.pop() {
        for (table, _) in builder.tables() {
            if !names_parent(table, &current) {
                continue;
            }
            let child = StoredTable::of(table);
            if found.contains(&child) || child == *parent {
                continue;
            }
            found.push(child.clone());
            frontier.push(child);
        }
    }

    found
}

/// Marks a column a table has just received from a parent as inherited.
pub(super) fn mark_inherited(builder: &mut ParserDBBuilder, table: &StoredTable, column: &str) {
    if let Some((_, metadata)) =
        builder.tables_mut().iter_mut().find(|(stored, _)| table.matches(stored))
    {
        let mut names = metadata.inherited_column_names().to_vec();
        names.push(column.to_string());
        metadata.set_inherited_column_names(names);
    }
}

/// Follows a parent's column rename in the record of what a child inherits.
pub(super) fn rename_inherited(
    builder: &mut ParserDBBuilder,
    table: &StoredTable,
    from: &str,
    to: &Ident,
) {
    if let Some((_, metadata)) =
        builder.tables_mut().iter_mut().find(|(stored, _)| table.matches(stored))
    {
        let names = metadata
            .inherited_column_names()
            .iter()
            .map(|name| if name == from { to.value.clone() } else { name.clone() })
            .collect();
        metadata.set_inherited_column_names(names);
    }
}

/// Whether a child receives the option along with the column.
///
/// PostgreSQL passes down what describes the column itself and withholds what
/// would create a constraint or an index of its own: a primary key, a unique
/// constraint, a foreign key, and an identity all stay with the parent.
fn is_inherited_option(option: &ColumnOption) -> bool {
    !matches!(
        option,
        ColumnOption::PrimaryKey(_) | ColumnOption::Unique(_) | ColumnOption::ForeignKey(_)
    ) && !is_identity(option)
}

/// Rewrites a parent column into the column the child receives.
fn inherited_column(column: &ColumnDef) -> ColumnDef {
    copy_column(column, is_inherited_option)
}

/// The name PostgreSQL resolves the type to, or [`None`] when the spelling is
/// not one this crate can place.
///
/// Only used to refuse a child that redeclares an inherited column with a
/// different type. An unplaceable spelling answers [`None`] and is accepted,
/// because wrongly refusing a legal schema costs more than missing a conflict
/// the parser cannot see.
fn type_identity(data_type: &DataType) -> Option<String> {
    let exact = |name: &str, info: &ExactNumberInfo| {
        match info {
            ExactNumberInfo::None => name.to_string(),
            ExactNumberInfo::Precision(p) => format!("{name}({p})"),
            ExactNumberInfo::PrecisionAndScale(p, s) => format!("{name}({p},{s})"),
        }
    };
    let sized = |name: &str, size: Option<u64>| {
        size.map_or_else(|| name.to_string(), |size| format!("{name}({size})"))
    };
    let charlen = |name: &str, length: Option<CharacterLength>| {
        match length {
            None => name.to_string(),
            Some(CharacterLength::Max) => format!("{name}(max)"),
            Some(CharacterLength::IntegerLength { length, .. }) => format!("{name}({length})"),
        }
    };

    Some(match data_type {
        DataType::Int(size) | DataType::Integer(size) | DataType::Int4(size) => {
            sized("int4", *size)
        }
        DataType::SmallInt(size) | DataType::Int2(size) => sized("int2", *size),
        DataType::BigInt(size) | DataType::Int8(size) => sized("int8", *size),
        DataType::Bool | DataType::Boolean => "bool".to_string(),
        DataType::Real | DataType::Float4 => "float4".to_string(),
        DataType::DoublePrecision | DataType::Float8 => "float8".to_string(),
        DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
            exact("numeric", info)
        }
        DataType::Text => "text".to_string(),
        DataType::Uuid => "uuid".to_string(),
        DataType::JSON => "json".to_string(),
        DataType::JSONB => "jsonb".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Bytea => "bytea".to_string(),
        DataType::Varchar(length)
        | DataType::CharacterVarying(length)
        | DataType::CharVarying(length) => charlen("varchar", *length),
        DataType::Char(length) | DataType::Character(length) => charlen("bpchar", *length),
        DataType::Timestamp(precision, timezone) => {
            let base = match timezone {
                TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => "timestamptz",
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => "timestamp",
            };
            sized(base, *precision)
        }
        _ => return None,
    })
}

/// Whether the two spellings name types PostgreSQL would refuse to merge.
///
/// Answers `false` whenever either spelling cannot be placed, so an unfamiliar
/// type never blocks a schema PostgreSQL accepts.
fn types_conflict(parent: &DataType, child: &DataType) -> bool {
    match (type_identity(parent), type_identity(child)) {
        (Some(parent), Some(child)) => parent != child,
        _ => false,
    }
}

/// Whether the two column names denote the same column.
fn same_column(left: &Ident, right: &Ident) -> bool {
    super::identifiers_match(
        left.value.as_str(),
        left.quote_style.is_some(),
        right.value.as_str(),
        right.quote_style.is_some(),
    )
}

/// The check constraints a child receives from a parent.
///
/// A check passes down whichever way it is written, so one attached to a
/// column arrives with the column and one written on its own arrives here.
fn inherited_checks(parent: &CreateTable) -> impl Iterator<Item = &TableConstraint> {
    parent.constraints.iter().filter(|constraint| matches!(constraint, TableConstraint::Check(_)))
}

/// Whether the table already carries an equivalent check.
fn holds_check(create_table: &CreateTable, candidate: &TableConstraint) -> bool {
    create_table.constraints.iter().any(|existing| existing == candidate)
}

/// Gives the node the columns and checks its parents pass down, answering the
/// names it took from a parent rather than declaring itself.
///
/// Runs while the `CREATE TABLE` statement is applied, which is when
/// PostgreSQL copies the parent's shape into the child. The parent's own node
/// is already complete by then, because a parent has to exist before the
/// table naming it, so a grandparent's columns arrive through the parent and
/// no separate walk up the chain is needed.
///
/// # Errors
///
/// Returns [`Error::ParentTableNotFound`] when a parent was never created and
/// [`Error::InheritedColumnTypeConflict`] when the child redeclares an
/// inherited column with a different type.
pub(super) fn apply_parents(
    builder: &ParserDBBuilder,
    create_table: &mut CreateTable,
) -> Result<Vec<String>, Error> {
    let parents: Vec<ObjectName> = parent_names(create_table).cloned().collect();
    if parents.is_empty() {
        return Ok(Vec::new());
    }

    let child_name = create_table.name.to_string();
    let local: Vec<Ident> = create_table.columns.iter().map(|column| column.name.clone()).collect();

    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut checks: Vec<TableConstraint> = Vec::new();

    for parent_name in &parents {
        let parent = resolve_parent(builder, parent_name, &child_name)?;

        for column in &parent.columns {
            // A second parent declaring the same column merges into the one
            // already taken, keeping the position the first parent gave it.
            if !columns.iter().any(|held| same_column(&held.name, &column.name)) {
                columns.push(inherited_column(column));
            }
        }

        for check in inherited_checks(parent) {
            if !holds_check(create_table, check) && !checks.contains(check) {
                checks.push(check.clone());
            }
        }
    }

    for column in &create_table.columns {
        if let Some(position) =
            columns.iter().position(|held| same_column(&held.name, &column.name))
        {
            if types_conflict(&columns[position].data_type, &column.data_type) {
                return Err(Error::InheritedColumnTypeConflict {
                    column_name: column.name.value.clone(),
                    child_table: child_name.clone(),
                    child_type: column.data_type.to_string(),
                    parent_table: parent_owning(builder, &parents, &column.name)
                        .unwrap_or_else(|| "a parent".to_string()),
                    parent_type: columns[position].data_type.to_string(),
                });
            }
            columns[position] = merge_declarations(&columns[position], column);
        } else {
            columns.push(column.clone());
        }
    }

    // Which columns came from a parent is not spelled by the node once they
    // join it, so the caller records it beside them, the way
    // `pg_attribute.attislocal` does.
    let inherited = columns
        .iter()
        .map(|column| &column.name)
        .filter(|name| !local.iter().any(|declared| same_column(declared, name)))
        .map(|name| name.value.clone())
        .collect();

    create_table.columns = columns;
    create_table.constraints.extend(checks);

    Ok(inherited)
}

/// Merges a parent's column with the child's redeclaration of it.
///
/// The child's declaration wins, except that a `NOT NULL` the parent states
/// survives a child that omits it, because a child cannot loosen one.
fn merge_declarations(parent: &ColumnDef, child: &ColumnDef) -> ColumnDef {
    let mut merged = child.clone();
    let parent_not_null =
        parent.options.iter().any(|option| matches!(option.option, ColumnOption::NotNull));
    let child_not_null =
        merged.options.iter().any(|option| matches!(option.option, ColumnOption::NotNull));
    if parent_not_null && !child_not_null {
        merged.options.push(ColumnOptionDef { name: None, option: ColumnOption::NotNull });
    }
    merged
}

/// Names the parent a column reaches the child from, for the conflict message.
fn parent_owning(
    builder: &ParserDBBuilder,
    parents: &[ObjectName],
    column: &Ident,
) -> Option<String> {
    parents
        .iter()
        .filter_map(|name| builder.resolve_table_object_name(name).ok().flatten())
        .find(|parent| parent.columns.iter().any(|held| same_column(&held.name, column)))
        .map(|parent| parent.name.to_string())
}

/// Resolves one parent name, refusing a table the input never created.
fn resolve_parent<'builder>(
    builder: &'builder ParserDBBuilder,
    parent_name: &ObjectName,
    child_name: &str,
) -> Result<&'builder CreateTable, Error> {
    builder.resolve_table_object_name(parent_name)?.ok_or_else(|| {
        Error::ParentTableNotFound {
            parent_table: parent_name.to_string(),
            child_table: child_name.to_string(),
        }
    })
}

/// Whether the table receives the column from a parent rather than declaring
/// it.
pub(super) fn is_inherited_column(
    builder: &ParserDBBuilder,
    table: &CreateTable,
    column: &Ident,
) -> bool {
    parent_names(table)
        .filter_map(|name| builder.resolve_table_object_name(name).ok().flatten())
        .any(|parent| parent.columns.iter().any(|held| same_column(&held.name, column)))
}
