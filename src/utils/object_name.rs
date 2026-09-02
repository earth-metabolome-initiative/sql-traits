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
    borrow::Cow,
    string::{String, ToString},
    vec::Vec,
};

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

use crate::{
    errors::LookupError,
    structs::TargetName,
    traits::{DatabaseLike, TableLike, ViewLike},
    utils::identifier_resolution::{
        identifiers_match, normalize_identifier, parse_lookup_identifier,
    },
};

/// Returns the written identifier of a single object name part.
///
/// Both [`ObjectNamePart::Identifier`] and [`ObjectNamePart::Function`] names
/// are accepted, mirroring how sqlparser models qualified names.
pub(crate) fn object_name_part_value(part: &ObjectNamePart) -> &str {
    match part {
        ObjectNamePart::Identifier(ident) => ident.value.as_str(),
        ObjectNamePart::Function(function_part) => function_part.name.value.as_str(),
    }
}

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

/// Reads an object name as an unresolved [`TargetName`], taking the last part
/// as the name and the second-to-last (if any) as its qualifier.
///
/// Returns `None` only for a name with no parts, which sqlparser does not
/// produce.
pub(crate) fn target_name_from_object_name(object_name: &ObjectName) -> Option<TargetName<'_>> {
    let (name, quoted) = object_name_last_part(object_name)?;
    let target = TargetName::new(name, quoted);
    Some(match schema_from_object_name(object_name) {
        Some((schema, schema_quoted)) => target.with_schema(schema, schema_quoted),
        None => target,
    })
}

/// Returns whether a table matches an object name using lenient part matching:
/// the last part is the table name and the second-to-last (if any) is the
/// schema. Leading parts beyond those are ignored.
///
/// This is the matching style used by grant resolution, where object names may
/// carry catalog-qualified prefixes. A table stored without a schema resides in
/// the default schema, so a `public` qualifier reaches it and a table stored in
/// `public` answers an unqualified name.
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
        (Some((schema_lookup, schema_lookup_quoted)), None) => {
            identifiers_match("public", false, schema_lookup, schema_lookup_quoted)
        }
        (None, Some(table_schema)) => {
            identifiers_match(table_schema, table.table_schema_is_quoted(), "public", false)
        }
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

/// Reads an object name as a [`TargetName`], using an empty string for the
/// name when the parts list is empty. The parser never produces an empty name,
/// so a caller receiving an empty-string `TargetName` built the name by hand.
pub(crate) fn target_name_of_object_name(object_name: &ObjectName) -> TargetName<'_> {
    target_name_from_object_name(object_name).unwrap_or_else(|| TargetName::new("", false))
}

/// Reads a table's own stored name as a [`TargetName`].
pub(crate) fn target_name_of_table<T: TableLike>(table: &T) -> TargetName<'_> {
    let name = TargetName::new(table.table_name(), table.table_name_is_quoted());
    match table.table_schema() {
        Some(schema) => name.with_schema(schema, table.table_schema_is_quoted()),
        None => name,
    }
}

/// Builds a [`TargetName`] from the identifiers a strict table lookup yields.
pub(crate) fn target_name_of_idents<'a>(
    schema_ident: Option<&'a Ident>,
    table_ident: &'a Ident,
) -> TargetName<'a> {
    let name = TargetName::new(table_ident.value.as_str(), table_ident.quote_style.is_some());
    match schema_ident {
        Some(schema_ident) => {
            name.with_schema(schema_ident.value.as_str(), schema_ident.quote_style.is_some())
        }
        None => name,
    }
}

/// Normalized identity a relation is found under.
///
/// PostgreSQL keeps tables, views and materialized views in one pool of
/// names, so all three are indexed under this key and a name can hold at most
/// one of them. A relation stored without a schema and one stored in `public`
/// share a key, mirroring that the two spellings name one place.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RelationKey {
    /// Normalized schema, `public` for a schema-less relation.
    pub schema: String,
    /// Normalized relation name.
    pub name: String,
}

/// Normalizes a stored schema, folding a schema-less table into `public`.
fn stored_schema_key<T: TableLike>(table: &T) -> Cow<'_, str> {
    table.table_schema().map_or(Cow::Borrowed("public"), |schema| {
        normalize_identifier(schema, table.table_schema_is_quoted())
    })
}

/// Normalizes a stored table name.
fn stored_name_key<T: TableLike>(table: &T) -> Cow<'_, str> {
    normalize_identifier(table.table_name(), table.table_name_is_quoted())
}

/// Key of a written target name, normalizing each part once.
pub(crate) fn target_key(target: &TargetName<'_>) -> RelationKey {
    RelationKey {
        schema: target.schema().map_or_else(
            || String::from("public"),
            |schema| normalize_identifier(schema, target.schema_is_quoted()).into_owned(),
        ),
        name: normalize_identifier(target.name(), target.name_is_quoted()).into_owned(),
    }
}

/// Key of a textual lookup, parsing quoting out of each part first.
pub(crate) fn lookup_key(schema: Option<&str>, name: &str) -> RelationKey {
    let name_ident = parse_lookup_identifier(name);
    RelationKey {
        schema: schema.map_or_else(
            || String::from("public"),
            |schema| {
                let schema_ident = parse_lookup_identifier(schema);
                normalize_identifier(schema_ident.value(), schema_ident.is_quoted()).into_owned()
            },
        ),
        name: normalize_identifier(name_ident.value(), name_ident.is_quoted()).into_owned(),
    }
}

/// Key a stored table is indexed under.
pub(crate) fn stored_table_key<T: TableLike>(table: &T) -> RelationKey {
    RelationKey {
        schema: stored_schema_key(table).into_owned(),
        name: stored_name_key(table).into_owned(),
    }
}

/// Key a stored identity is found under, taking each part exactly as stored.
///
/// The index folds a schema-less relation into `public`, so this key reaches
/// the bucket holding both spellings and the caller separates them by
/// comparing the stored parts.
pub(crate) fn stored_identity_key(schema: Option<&str>, name: &str) -> RelationKey {
    RelationKey {
        schema: schema.map_or_else(|| String::from("public"), String::from),
        name: String::from(name),
    }
}

/// Key a stored view is indexed under, folding a schema-less view into
/// `public` exactly as a table is folded.
pub(crate) fn stored_view_key<V: ViewLike>(view: &V) -> RelationKey {
    RelationKey {
        schema: view.view_schema().map_or_else(
            || String::from("public"),
            |schema| normalize_identifier(schema, view.view_schema_is_quoted()).into_owned(),
        ),
        name: view.stored_view_name().into_owned(),
    }
}

/// Returns whether a stored table answers a normalized key, normalizing only
/// the stored side.
pub(crate) fn table_matches_key<T: TableLike>(table: &T, key: &RelationKey) -> bool {
    key.name == stored_name_key(table) && key.schema == stored_schema_key(table)
}

/// Reference matcher used by tests: compares a table against a written target
/// without precomputing a key.
#[cfg(test)]
pub(crate) fn table_matches_target<T: TableLike>(table: &T, target: &TargetName<'_>) -> bool {
    table_matches_key(table, &target_key(target))
}

/// Renders a table for inclusion in an ambiguity error, quoting parts that were
/// originally quoted.
pub(crate) fn render_table_candidate<T: TableLike>(table: &T) -> String {
    target_name_of_table(table).to_string()
}

/// Resolves a single relation from a list of candidate matches.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when more than one candidate
/// matches.
pub(crate) fn resolve_one_relation<'a, R>(
    target: &TargetName<'_>,
    candidates: &[&'a R],
    render: impl Fn(&R) -> String,
) -> Result<Option<&'a R>, LookupError> {
    match candidates {
        [] => Ok(None),
        [relation] => Ok(Some(*relation)),
        _ => {
            let mut rendered: Vec<String> = candidates.iter().copied().map(&render).collect();
            rendered.sort_unstable();
            rendered.dedup();
            Err(LookupError::AmbiguousTableLookup {
                object_name: target.to_string(),
                candidates: rendered,
            })
        }
    }
}

/// Renders a view for inclusion in an ambiguity error, quoting parts that were
/// originally quoted.
pub(crate) fn render_view_candidate<V: ViewLike>(view: &V) -> String {
    view.target_name().to_string()
}

/// Resolves a single table from a list of candidate matches.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when more than one candidate
/// matches.
pub(crate) fn resolve_target_from_candidates<'a, T: TableLike>(
    target: &TargetName<'_>,
    candidates: &[&'a T],
) -> Result<Option<&'a T>, LookupError> {
    resolve_one_relation(target, candidates, render_table_candidate)
}

/// Resolves a written target name against an iterator of tables, without
/// consulting any search path.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when the name matches more
/// than one table.
pub(crate) fn resolve_target_in_iter<'a, T: TableLike>(
    tables: impl Iterator<Item = &'a T>,
    target: &TargetName<'_>,
) -> Result<Option<&'a T>, LookupError> {
    let key = target_key(target);
    let candidates: Vec<&T> = tables.filter(|table| table_matches_key(*table, &key)).collect();
    resolve_target_from_candidates(target, &candidates)
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
    resolve_target_in_iter(tables, &target_name_of_idents(schema_ident, table_ident))
}

/// Resolves a written target name against an iterator of relations of one
/// kind, trying each schema on `search_path` in turn for an unqualified name.
///
/// The path is walked in order and the first schema holding a match wins,
/// which is what the database does. A relation stored without a schema resides
/// in the default schema, so it is found where `public` sits on the path
/// rather than ahead of it.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when the name matches more
/// than one relation in the schema that wins, reported under the name the
/// statement wrote.
fn resolve_relation_on_search_path<'a, 'path, R>(
    relations: impl Iterator<Item = &'a R>,
    target: &TargetName<'_>,
    search_path: impl Iterator<Item = (&'path str, bool)>,
    key_of: impl Fn(&R) -> RelationKey,
    render: impl Fn(&R) -> String,
) -> Result<Option<&'a R>, LookupError> {
    if target.schema().is_some() {
        let key = target_key(target);
        let candidates: Vec<&R> = relations.filter(|relation| key_of(relation) == key).collect();
        return resolve_one_relation(target, &candidates, render);
    }

    let name = normalize_identifier(target.name(), target.name_is_quoted()).into_owned();
    let path: Vec<String> = search_path
        .map(|(schema, quoted)| normalize_identifier(schema, quoted).into_owned())
        .collect();
    let mut winner = usize::MAX;
    let mut candidates: Vec<&'a R> = Vec::new();
    for relation in relations {
        let key = key_of(relation);
        if name != key.name {
            continue;
        }
        for (entry, path_schema) in path.iter().take(winner.saturating_add(1)).enumerate() {
            if path_schema == &key.schema {
                if entry < winner {
                    winner = entry;
                    candidates = vec![relation];
                } else {
                    candidates.push(relation);
                }
                break;
            }
        }
    }

    if winner == usize::MAX {
        return Ok(None);
    }
    // Reported under the written name: the entry qualifier is resolution
    // machinery, not something the statement spelled.
    resolve_one_relation(target, &candidates, render)
}

/// Resolves a written target name against an iterator of tables, trying each
/// schema on `search_path` in turn for an unqualified name.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when the name matches more
/// than one table in the schema that wins, reported under the name the
/// statement wrote.
pub(crate) fn resolve_target_on_search_path_in_iter<'a, 'path, T: TableLike>(
    tables: impl Iterator<Item = &'a T>,
    target: &TargetName<'_>,
    search_path: impl Iterator<Item = (&'path str, bool)>,
) -> Result<Option<&'a T>, LookupError> {
    resolve_relation_on_search_path(
        tables,
        target,
        search_path,
        stored_table_key,
        render_table_candidate,
    )
}

/// Resolves a written target name against an iterator of views of one kind,
/// trying each schema on `search_path` in turn for an unqualified name.
///
/// # Errors
///
/// Returns [`LookupError::AmbiguousTableLookup`] when the name matches more
/// than one view in the schema that wins, reported under the name the
/// statement wrote.
pub(crate) fn resolve_view_on_search_path_in_iter<'a, 'path, V: ViewLike>(
    views: impl Iterator<Item = &'a V>,
    target: &TargetName<'_>,
    search_path: impl Iterator<Item = (&'path str, bool)>,
) -> Result<Option<&'a V>, LookupError> {
    resolve_relation_on_search_path(
        views,
        target,
        search_path,
        stored_view_key,
        render_view_candidate,
    )
}

/// Resolves a table from a one-part or two-part object name, honouring
/// `search_path` for an unqualified name.
///
/// # Errors
///
/// Returns an error when the object name is malformed for table lookup, or when
/// the lookup is ambiguous.
pub(crate) fn resolve_table_object_name_on_search_path_in_iter<'a, 'path, T: TableLike>(
    tables: impl Iterator<Item = &'a T>,
    object_name: &ObjectName,
    search_path: impl Iterator<Item = (&'path str, bool)>,
) -> Result<Option<&'a T>, LookupError> {
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    resolve_target_on_search_path_in_iter(
        tables,
        &target_name_of_idents(schema_ident, table_ident),
        search_path,
    )
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
    let (schema_ident, table_ident) = object_name_identifiers(object_name)?;
    // Through the trait method, so a catalog overriding it answers every
    // accessor the same way it answers a direct lookup.
    database.resolve_target_table(target_name_of_idents(schema_ident, table_ident))
}

/// Resolves an object name that is required to denote an existing base table of
/// `database`.
///
/// Unlike [`resolve_object_name`], a name that matches no table is an error
/// rather than `Ok(None)`: use this when the referencing object could not exist
/// without its target, as with a policy or a trigger.
///
/// # Errors
///
/// Returns [`LookupError::TableNotFound`] when no table matches, and an error
/// when the object name is malformed for table lookup or the lookup is
/// ambiguous.
pub(crate) fn resolve_required_table<'db, DB: DatabaseLike>(
    object_name: &ObjectName,
    database: &'db DB,
) -> Result<&'db DB::Table, LookupError> {
    resolve_object_name(object_name, database)?
        .ok_or_else(|| LookupError::TableNotFound { object_name: object_name.to_string() })
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{CreateTable, Ident, ObjectName, ObjectNamePart, ObjectNamePartFunction, Statement},
        dialect::GenericDialect,
        parser::Parser,
    };

    use super::{
        object_name_identifiers, object_name_last_part, render_table_candidate,
        render_view_candidate, resolve_object_name, resolve_table_object_name_in_iter,
        resolve_table_object_name_on_search_path_in_iter, resolve_target_from_candidates,
        resolve_view_on_search_path_in_iter, schema_from_object_name, table_matches_object_name,
        table_matches_target, target_name_of_object_name,
    };
    use crate::{
        errors::LookupError,
        prelude::ParserDB,
        structs::TargetName,
        traits::{DatabaseLike, TableLike},
    };

    fn ident(value: &str, quoted: bool) -> Ident {
        if quoted { Ident::with_quote('"', value) } else { Ident::new(value) }
    }

    /// The path a database starts with, which is what these helpers used to
    /// hardcode.
    fn default_path() -> impl Iterator<Item = (&'static str, bool)> {
        core::iter::once(("public", false))
    }

    /// Builds an `ObjectName` from `(value, quoted)` identifier parts.
    fn obj(parts: &[(&str, bool)]) -> ObjectName {
        ObjectName(parts.iter().map(|&(v, q)| ObjectNamePart::Identifier(ident(v, q))).collect())
    }

    fn function_part(name: &str) -> ObjectNamePart {
        ObjectNamePart::Function(ObjectNamePartFunction {
            name: Ident::new(name),
            args: Vec::new(),
        })
    }

    /// Parses one `CREATE TABLE` into an owned `CreateTable`. Parsing tables
    /// individually lets a test hold relations the `ParserDB` builder would
    /// reject together (for example a schema-less and a `public` table of the
    /// same name), which the resolver functions still must handle.
    fn create_table(sql: &str) -> CreateTable {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("table parses");
        match statements.pop().expect("one statement") {
            Statement::CreateTable(create_table) => create_table,
            other => panic!("expected CREATE TABLE, got {other:?}"),
        }
    }

    /// A spread of relations covering schema-less, schema-qualified, quoted,
    /// and `public`/schema-less name collisions.
    fn fixtures() -> Vec<CreateTable> {
        vec![
            create_table("CREATE TABLE users (id INT)"),
            create_table("CREATE TABLE s.scoped (id INT)"),
            create_table("CREATE TABLE things (id INT)"),
            create_table("CREATE TABLE public.things (id INT)"),
            create_table("CREATE TABLE s.users (id INT)"),
            create_table("CREATE TABLE public.only_pub (id INT)"),
            create_table(r#"CREATE TABLE "Bar" (id INT)"#),
            create_table(r#"CREATE TABLE "S"."T" (id INT)"#),
        ]
    }

    fn find<'a>(tables: &'a [CreateTable], name: &str) -> &'a CreateTable {
        tables.iter().find(|table| table.table_name() == name).expect("table present")
    }

    #[test]
    fn object_name_last_part_variants() {
        assert_eq!(object_name_last_part(&obj(&[("t", false)])), Some(("t", false)));
        assert_eq!(object_name_last_part(&obj(&[("T", true)])), Some(("T", true)));
        assert_eq!(
            object_name_last_part(&ObjectName(vec![function_part("f")])),
            Some(("f", false))
        );
        assert_eq!(object_name_last_part(&ObjectName(Vec::new())), None);
    }

    #[test]
    fn schema_from_object_name_variants() {
        assert_eq!(schema_from_object_name(&obj(&[("t", false)])), None);
        assert_eq!(
            schema_from_object_name(&obj(&[("s", false), ("t", false)])),
            Some(("s", false))
        );
        let name =
            ObjectName(vec![function_part("f"), ObjectNamePart::Identifier(ident("t", false))]);
        assert_eq!(schema_from_object_name(&name), Some(("f", false)));
    }

    #[test]
    fn table_matches_object_name_cases() {
        let tables = fixtures();
        let users = find(&tables, "users");
        let scoped = find(&tables, "scoped");

        assert!(!table_matches_object_name(users, &ObjectName(Vec::new())));
        assert!(!table_matches_object_name(users, &obj(&[("orders", false)])));
        assert!(table_matches_object_name(users, &obj(&[("users", false)])));
        assert!(table_matches_object_name(scoped, &obj(&[("s", false), ("scoped", false)])));
        // The two spellings of the default schema are one place.
        assert!(table_matches_object_name(users, &obj(&[("public", false), ("users", false)])));
        assert!(table_matches_object_name(find(&tables, "only_pub"), &obj(&[("only_pub", false)])));
        // A non-public qualifier misses a schema-less table.
        assert!(!table_matches_object_name(users, &obj(&[("nope", false), ("users", false)])));
    }

    #[test]
    fn object_name_identifiers_cases() {
        assert!(matches!(object_name_identifiers(&obj(&[("t", false)])), Ok((None, _))));
        assert!(matches!(
            object_name_identifiers(&obj(&[("s", false), ("t", false)])),
            Ok((Some(_), _))
        ));
        assert!(matches!(
            object_name_identifiers(&ObjectName(Vec::new())),
            Err(LookupError::InvalidObjectName { .. })
        ));
        assert!(matches!(
            object_name_identifiers(&obj(&[("a", false), ("b", false), ("c", false)])),
            Err(LookupError::InvalidObjectName { .. })
        ));
        assert!(matches!(
            object_name_identifiers(&ObjectName(vec![function_part("f")])),
            Err(LookupError::InvalidObjectName { .. })
        ));
    }

    #[test]
    fn table_matches_target_cases() {
        let tables = fixtures();
        let users = find(&tables, "users");
        let scoped = find(&tables, "scoped");

        assert!(table_matches_target(users, &TargetName::new("users", false)));
        assert!(!table_matches_target(users, &TargetName::new("orders", false)));
        assert!(table_matches_target(
            scoped,
            &TargetName::new("scoped", false).with_schema("s", false)
        ));
        // The two spellings of the default schema are one place.
        assert!(table_matches_target(
            users,
            &TargetName::new("users", false).with_schema("public", false)
        ));
        assert!(table_matches_target(
            find(&tables, "only_pub"),
            &TargetName::new("only_pub", false)
        ));
        // A table in another schema stays unreachable without its qualifier.
        assert!(!table_matches_target(scoped, &TargetName::new("scoped", false)));
        assert!(!table_matches_target(
            users,
            &TargetName::new("users", false).with_schema("s", false)
        ));
    }

    #[test]
    fn render_table_candidate_quoting_and_schema() {
        let tables = fixtures();
        assert_eq!(render_table_candidate(find(&tables, "users")), "users");
        assert_eq!(render_table_candidate(find(&tables, "Bar")), "\"Bar\"");
        assert_eq!(render_table_candidate(find(&tables, "scoped")), "s.scoped");
        assert_eq!(render_table_candidate(find(&tables, "T")), "\"S\".\"T\"");
    }

    #[test]
    fn resolve_target_from_candidates_cases() {
        let tables = fixtures();
        let users = find(&tables, "users");
        let scoped = find(&tables, "scoped");
        let target = TargetName::new("users", false);

        let empty: [&CreateTable; 0] = [];
        assert!(resolve_target_from_candidates(&target, &empty).unwrap().is_none());
        assert!(resolve_target_from_candidates(&target, &[users]).unwrap().is_some());
        assert!(matches!(
            resolve_target_from_candidates(&target, &[users, scoped]),
            Err(LookupError::AmbiguousTableLookup { .. })
        ));
    }

    #[test]
    fn resolve_in_iter_rejects_overqualified_names() {
        let tables = fixtures();
        let resolved = resolve_table_object_name_in_iter(tables.iter(), &obj(&[("users", false)]))
            .expect("resolves")
            .expect("matches");
        assert_eq!(resolved.table_name(), "users");

        assert!(matches!(
            resolve_table_object_name_in_iter(
                tables.iter(),
                &obj(&[("a", false), ("b", false), ("c", false)]),
            ),
            Err(LookupError::InvalidObjectName { .. })
        ));
    }

    #[test]
    fn implicit_public_fallback_cases() {
        let tables = fixtures();

        // Qualified name delegates to the strict resolver.
        let scoped = resolve_table_object_name_on_search_path_in_iter(
            tables.iter(),
            &obj(&[("s", false), ("scoped", false)]),
            default_path(),
        )
        .expect("resolves")
        .expect("matches");
        assert_eq!(scoped.table_name(), "scoped");

        // Resolved only through the implicit `public` schema.
        let only_pub = resolve_table_object_name_on_search_path_in_iter(
            tables.iter(),
            &obj(&[("only_pub", false)]),
            default_path(),
        )
        .expect("resolves")
        .expect("matches");
        assert_eq!(only_pub.table_name(), "only_pub");

        // `things` exists both schema-less and in `public`, two tables claiming
        // one place: ambiguous, reported under the written name.
        assert!(matches!(
            resolve_table_object_name_on_search_path_in_iter(
                tables.iter(),
                &obj(&[("things", false)]),
                default_path(),
            ),
            Err(LookupError::AmbiguousTableLookup { ref object_name, .. }) if object_name == "things"
        ));

        // The first schema on the path holding the name wins.
        let on_s_first = resolve_table_object_name_on_search_path_in_iter(
            tables.iter(),
            &obj(&[("users", false)]),
            [("s", false), ("public", false)].into_iter(),
        )
        .expect("resolves")
        .expect("matches");
        assert_eq!(on_s_first.table_schema(), Some("s"));

        let on_public_first = resolve_table_object_name_on_search_path_in_iter(
            tables.iter(),
            &obj(&[("users", false)]),
            [("public", false), ("s", false)].into_iter(),
        )
        .expect("resolves")
        .expect("matches");
        assert_eq!(on_public_first.table_schema(), None, "the schema-less spelling is public's");

        // A schema-less table is unreachable when `public` is off the path.
        assert!(
            resolve_table_object_name_on_search_path_in_iter(
                tables.iter(),
                &obj(&[("only_pub", false)]),
                core::iter::once(("s", false)),
            )
            .unwrap()
            .is_none()
        );

        // No match anywhere.
        assert!(
            resolve_table_object_name_on_search_path_in_iter(
                tables.iter(),
                &obj(&[("absent", false)]),
                default_path(),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn resolve_object_name_db_entry_point() {
        let db = ParserDB::parse::<GenericDialect>("CREATE TABLE users (id INT);").expect("parses");
        let resolved = resolve_object_name(&obj(&[("users", false)]), &db)
            .expect("resolves")
            .expect("matches");
        assert_eq!(resolved.table_name(), "users");
        assert!(resolve_object_name(&obj(&[("absent", false)]), &db).unwrap().is_none());
    }

    #[test]
    fn target_name_of_empty_object_name_returns_empty_str() {
        let name = ObjectName(vec![]);
        let result = target_name_of_object_name(&name);
        assert_eq!(result.name(), "");
        assert!(!result.name_is_quoted());
        assert!(result.schema().is_none());
    }

    #[test]
    fn views_render_and_resolve_on_the_search_path() {
        let db = ParserDB::parse::<GenericDialect>(
            "CREATE SCHEMA s;
             CREATE TABLE t(id INT);
             CREATE VIEW s.v AS SELECT id FROM t;",
        )
        .expect("schema parses");
        let view = db.view(Some("s"), "v").expect("view exists");
        assert_eq!(render_view_candidate(view), "s.v");
        let resolved = resolve_view_on_search_path_in_iter(
            db.views(),
            &TargetName::new("v", false),
            [("s", false)].into_iter(),
        )
        .expect("view resolves")
        .expect("view matches");
        assert_eq!(render_view_candidate(resolved), "s.v");
    }
}
