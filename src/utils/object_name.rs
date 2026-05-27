//! Shared resolution of sqlparser [`ObjectName`] values against the tables of a
//! [`DatabaseLike`].
//!
//! These helpers are the single source of truth for turning a parsed SQL object
//! name (`table`, `schema.table`) into a resolved table, applying PostgreSQL
//! identifier semantics through [`identifiers_match`].
//! They are generic over [`TableLike`] so that both the concrete `ParserDB`
//! resolution paths and the trait-on-AST data-statement analysis share one
//! implementation.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

use crate::{
    errors::LookupError,
    traits::{DatabaseLike, TableLike},
    utils::identifier_resolution::identifiers_match,
};

/// Returns the last identifier part of an object name as `(value, quoted)`.
///
/// Both [`ObjectNamePart::Identifier`] and [`ObjectNamePart::Function`] names
/// are accepted, mirroring how sqlparser models qualified names.
pub(crate) fn object_name_last_part(object_name: &ObjectName) -> Option<(&str, bool)> {
    match object_name.0.last() {
        Some(ObjectNamePart::Identifier(ident)) => {
            Some((ident.value.as_str(), ident.quote_style.is_some()))
        }
        Some(ObjectNamePart::Function(function_part)) => {
            Some((function_part.name.value.as_str(), function_part.name.quote_style.is_some()))
        }
        None => None,
    }
}

/// Extracts the schema component (the second-to-last part) of an object name as
/// `(value, quoted)`, when the name has more than one part.
///
/// For `schema.table` this returns the `schema` part; for a bare `table` it
/// returns `None`.
pub(crate) fn schema_from_object_name(object_name: &ObjectName) -> Option<(&str, bool)> {
    if object_name.0.len() > 1 {
        match &object_name.0[object_name.0.len() - 2] {
            ObjectNamePart::Identifier(ident) => {
                Some((ident.value.as_str(), ident.quote_style.is_some()))
            }
            ObjectNamePart::Function(function_part) => {
                Some((function_part.name.value.as_str(), function_part.name.quote_style.is_some()))
            }
        }
    } else {
        None
    }
}

/// Returns whether a table matches an object name using lenient part matching:
/// the last part is the table name and the second-to-last (if any) is the
/// schema. Leading parts beyond those are ignored.
///
/// This is the matching style used by grant resolution, where object names may
/// carry catalog-qualified prefixes.
pub(crate) fn table_matches_object_name<T: TableLike>(table: &T, object_name: &ObjectName) -> bool {
    let Some((table_lookup_name, table_lookup_quoted)) = object_name_last_part(object_name) else {
        return false;
    };

    if !identifiers_match(
        table.table_name(),
        table.table_name_is_quoted(),
        table_lookup_name,
        table_lookup_quoted,
    ) {
        return false;
    }

    match (schema_from_object_name(object_name), table.table_schema()) {
        (None, None) => true,
        (Some((schema_lookup, schema_lookup_quoted)), Some(table_schema)) => {
            identifiers_match(
                table_schema,
                table.table_schema_is_quoted(),
                schema_lookup,
                schema_lookup_quoted,
            )
        }
        _ => false,
    }
}

/// Splits a one-part or two-part object name into optional schema and required
/// table identifiers, rejecting names that cannot denote a table.
///
/// # Errors
///
/// Returns [`LookupError::InvalidObjectName`] when the name is empty, has more
/// than two parts, or contains a function part.
pub(crate) fn object_name_identifiers(
    object_name: &ObjectName,
) -> Result<(Option<&Ident>, &Ident), LookupError> {
    if object_name.0.is_empty() {
        return Err(LookupError::InvalidObjectName {
            object_name: object_name.to_string(),
            reason: "name has no identifier parts".to_string(),
        });
    }
    if object_name.0.len() > 2 {
        return Err(LookupError::InvalidObjectName {
            object_name: object_name.to_string(),
            reason: "only one-part or two-part object names are supported".to_string(),
        });
    }

    let mut idents: Vec<&Ident> = Vec::with_capacity(object_name.0.len());
    for part in &object_name.0 {
        match part {
            ObjectNamePart::Identifier(ident) => idents.push(ident),
            ObjectNamePart::Function(_) => {
                return Err(LookupError::InvalidObjectName {
                    object_name: object_name.to_string(),
                    reason: "all object name parts must be identifiers".to_string(),
                });
            }
        }
    }

    if idents.len() == 1 { Ok((None, idents[0])) } else { Ok((Some(idents[0]), idents[1])) }
}

/// Returns whether a table matches the (optional schema, table) identifiers
/// using strict matching: a one-part lookup matches only schema-less tables.
pub(crate) fn table_matches_lookup_idents<T: TableLike>(
    table: &T,
    schema_ident: Option<&Ident>,
    table_ident: &Ident,
) -> bool {
    if !identifiers_match(
        table.table_name(),
        table.table_name_is_quoted(),
        table_ident.value.as_str(),
        table_ident.quote_style.is_some(),
    ) {
        return false;
    }

    match (schema_ident, table.table_schema()) {
        (None, None) => true,
        (Some(schema_ident), Some(table_schema)) => {
            identifiers_match(
                table_schema,
                table.table_schema_is_quoted(),
                schema_ident.value.as_str(),
                schema_ident.quote_style.is_some(),
            )
        }
        _ => false,
    }
}

fn quoted_identifier(value: &str) -> String {
    alloc::format!("\"{}\"", value.replace('\"', "\"\""))
}

/// Renders a table for inclusion in an ambiguity error, quoting parts that were
/// originally quoted.
pub(crate) fn render_table_candidate<T: TableLike>(table: &T) -> String {
    let table_name = if table.table_name_is_quoted() {
        quoted_identifier(table.table_name())
    } else {
        table.table_name().to_string()
    };

    match table.table_schema() {
        Some(schema_name) => {
            let schema_name = if table.table_schema_is_quoted() {
                quoted_identifier(schema_name)
            } else {
                schema_name.to_string()
            };
            alloc::format!("{schema_name}.{table_name}")
        }
        None => table_name,
    }
}

/// Resolves a single table from a list of candidate matches.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when more than one candidate
/// matches.
pub(crate) fn resolve_table_from_candidates<'a, T: TableLike>(
    object_name: &ObjectName,
    candidates: &[&'a T],
) -> Result<Option<&'a T>, LookupError> {
    match candidates {
        [] => Ok(None),
        [table] => Ok(Some(*table)),
        _ => {
            let mut rendered: Vec<String> =
                candidates.iter().copied().map(render_table_candidate).collect();
            rendered.sort_unstable();
            rendered.dedup();
            Err(LookupError::AmbiguousTableLookup {
                object_name: object_name.to_string(),
                candidates: rendered,
            })
        }
    }
}

/// Resolves a table from a one-part or two-part object name against an iterator
/// of tables.
///
/// # Errors
///
/// Returns an error when the object name is malformed for table lookup, or when
/// the lookup is ambiguous.
pub(crate) fn resolve_table_object_name_in_iter<'a, T: TableLike>(
    tables: impl Iterator<Item = &'a T>,
    object_name: &ObjectName,
) -> Result<Option<&'a T>, LookupError> {
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    let candidates: Vec<&T> = tables
        .filter(|table| table_matches_lookup_idents(*table, schema_ident, table_ident))
        .collect();
    resolve_table_from_candidates(object_name, &candidates)
}

/// Resolves a table from an object name, falling back to schema `public` for
/// unqualified names.
///
/// # Errors
///
/// Returns an error when the object name is malformed for table lookup, or when
/// the lookup is ambiguous (including a schema-less table and a `public` table
/// of the same name).
pub(crate) fn resolve_table_object_name_with_implicit_public_in_iter<'a, T: TableLike>(
    tables: impl Iterator<Item = &'a T>,
    object_name: &ObjectName,
) -> Result<Option<&'a T>, LookupError> {
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    let table_refs: Vec<&T> = tables.collect();

    if schema_ident.is_some() {
        return resolve_table_object_name_in_iter(table_refs.into_iter(), object_name);
    }

    let unqualified_candidates: Vec<&T> = table_refs
        .iter()
        .copied()
        .filter(|table| table_matches_lookup_idents(*table, None, table_ident))
        .collect();
    let unqualified = resolve_table_from_candidates(object_name, &unqualified_candidates)?;

    let public_candidates: Vec<&T> = table_refs
        .iter()
        .copied()
        .filter(|table| {
            table.table_schema().is_some_and(|schema_name| {
                identifiers_match(schema_name, table.table_schema_is_quoted(), "public", false)
            }) && identifiers_match(
                table.table_name(),
                table.table_name_is_quoted(),
                table_ident.value.as_str(),
                table_ident.quote_style.is_some(),
            )
        })
        .collect();
    let public_lookup_name = ObjectName(vec![
        ObjectNamePart::Identifier(Ident::new("public")),
        ObjectNamePart::Identifier(table_ident.clone()),
    ]);
    let public = resolve_table_from_candidates(&public_lookup_name, &public_candidates)?;

    match (unqualified, public) {
        (Some(unqualified), Some(public)) => {
            if core::ptr::eq(unqualified, public) {
                Ok(Some(unqualified))
            } else {
                let mut candidates =
                    vec![render_table_candidate(unqualified), render_table_candidate(public)];
                candidates.sort_unstable();
                candidates.dedup();
                Err(LookupError::AmbiguousTableLookup {
                    object_name: object_name.to_string(),
                    candidates,
                })
            }
        }
        (Some(table), None) | (None, Some(table)) => Ok(Some(table)),
        (None, None) => Ok(None),
    }
}

/// Resolves a one-part or two-part object name to a base table of `database`.
///
/// This is the canonical entry point for trait-on-AST analysis that needs to
/// turn an [`ObjectName`] into a [`DatabaseLike::Table`]. `Ok(None)` means no
/// table matched (for example a CTE name or a table function); an error means
/// the name is malformed or the lookup is ambiguous.
///
/// # Errors
///
/// Returns an error when the object name is malformed for table lookup, or when
/// the lookup is ambiguous.
pub(crate) fn resolve_object_name<'db, DB: DatabaseLike>(
    object_name: &ObjectName,
    database: &'db DB,
) -> Result<Option<&'db DB::Table>, LookupError> {
    resolve_table_object_name_in_iter(database.tables(), object_name)
}
