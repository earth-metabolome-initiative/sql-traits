//! Recording, replacing and dropping the two kinds of view.
//!
//! Every rule here was measured against PostgreSQL 18.4 in Docker rather than
//! read from documentation.
//!
//! A view shares one pool of names with tables, materialized views and
//! indexes, so creating one under a taken name is refused whichever kind holds
//! it, and the two drop spellings refuse each other's kind with the server's
//! own wording. `CREATE OR REPLACE VIEW` may only add output columns on the
//! end: renaming one and dropping one are both refused, checked whenever the
//! recorded and the replacing shapes can both be read. A materialized view has
//! no replace form at all, and a plain view has no `IF NOT EXISTS` form, both
//! of which the parser accepts and the server rejects outright.

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::ops::ControlFlow;

use sqlparser::ast::{
    AlterTableOperation, CreateView, Ident, ObjectName, ObjectNamePart, Owner, Query,
    RenameTableNameKind, Visit, Visitor,
};

use super::{
    ParserDBBuilder, SchemaQualifier, object_name_last_identifier, relation_name_holder,
    require_named, search_path_qualifier, validate_relation_schema,
};
use crate::{
    errors::{Error, ObjectKind},
    structs::{MaterializedView, View, metadata::ViewMetadata},
    traits::ViewLike,
    utils::object_name::{
        RelationKey, object_name_last_part, qualifier_of, stored_table_key, stored_view_key,
        target_key, target_name_from_object_name,
    },
};

/// The kind a `CREATE VIEW` node declares.
fn declared_kind(node: &CreateView) -> ObjectKind {
    if node.materialized { ObjectKind::MaterializedView } else { ObjectKind::View }
}

/// The schema qualifier a view name carries, if it carries one.
fn view_schema_qualifier(name: &ObjectName) -> SchemaQualifier<'_> {
    qualifier_of(name).named()
}

/// Records a `CREATE VIEW` or `CREATE MATERIALIZED VIEW`.
///
/// # Errors
///
/// Refuses the two spellings the parser accepts and PostgreSQL does not, a
/// name another relation in the schema already holds, a schema the input never
/// creates, and a replacement that renames or drops an output column.
pub(super) fn create_view(
    mut builder: ParserDBBuilder,
    mut node: CreateView,
) -> Result<ParserDBBuilder, Error> {
    let kind = declared_kind(&node);
    require_named(&node.name, kind)?;

    if node.materialized && node.or_replace {
        return Err(Error::MaterializedViewCannotBeReplaced {
            view_name: rendered_name(&node.name),
        });
    }
    if !node.materialized && node.if_not_exists {
        return Err(Error::ViewIfNotExistsUnsupported { view_name: rendered_name(&node.name) });
    }

    // Where the view lands is decided before the name is read, so the
    // name-pool checks compare the schema it truly creates in rather than the
    // one the statement spelled. A temporary view goes to a schema private to
    // the session, which the path never names.
    if !node.temporary
        && view_schema_qualifier(&node.name).is_none()
        && let Some(qualifier) =
            search_path_qualifier(&builder, kind, name_of(&node.name).unwrap_or_default())?
    {
        node.name.0.insert(0, ObjectNamePart::Identifier(qualifier));
    }
    validate_relation_schema(
        &builder,
        view_schema_qualifier(&node.name),
        kind,
        name_of(&node.name).unwrap_or_default(),
    )?;

    let schema = view_schema_qualifier(&node.name);
    let Some(name_ident) = object_name_last_identifier(&node.name) else {
        return Err(Error::UnnamedObject { object_kind: kind });
    };

    // A replacement takes the recorded definition's place, so it is checked
    // against that definition and then removed before the name-pool check,
    // which would otherwise see the view being replaced as a collision.
    if node.or_replace
        && let Some(position) = plain_view_position(&builder, &node.name)
    {
        let (existing, metadata) = builder.views_mut().remove(position);
        check_replacement_columns(existing.as_ref(), &node)?;
        let Some(view) = View::from_node(&node) else {
            return Err(Error::UnnamedObject { object_kind: kind });
        };
        return Ok(builder.add_view(Arc::new(view), metadata));
    }

    let holder = relation_name_holder(&builder, name_ident, schema);

    if let Some(conflicting_kind) = holder {
        // `CREATE MATERIALIZED VIEW ... IF NOT EXISTS` skips silently when a
        // materialized view already holds the name, which is the only form
        // PostgreSQL offers this on.
        if node.if_not_exists && conflicting_kind == ObjectKind::MaterializedView {
            return Ok(builder);
        }
        return Err(Error::RelationNameAlreadyTaken {
            object_kind: kind,
            conflicting_kind,
            object_name: name_ident.value.clone(),
        });
    }

    if node.materialized {
        let Some(view) = MaterializedView::from_node(&node) else {
            return Err(Error::UnnamedObject { object_kind: kind });
        };
        Ok(builder.add_materialized_view(Arc::new(view), ViewMetadata::default()))
    } else {
        let Some(view) = View::from_node(&node) else {
            return Err(Error::UnnamedObject { object_kind: kind });
        };
        Ok(builder.add_view(Arc::new(view), ViewMetadata::default()))
    }
}

/// Checks the two rules a replacement has to keep: it may not rename an output
/// column and it may not drop one.
///
/// Only the names are checkable here. A view carries no declared types, so the
/// server's third rule, that a column's type may not change, needs a type for
/// an arbitrary expression, which this crate does not derive. When either
/// shape's names cannot be read the replacement is recorded without complaint,
/// which never refuses input the server accepts.
fn check_replacement_columns(existing: &View, node: &CreateView) -> Result<(), Error> {
    let (Some(before), Some(after)) = (existing.declared_output_names(), declared_names_of(node))
    else {
        return Ok(());
    };

    if after.len() < before.len() {
        return Err(Error::ViewColumnsDroppedByReplace {
            view_name: existing.view_name().to_string(),
        });
    }
    for ((existing_name, _), (new_name, _)) in before.iter().zip(after.iter()) {
        if existing_name != new_name {
            return Err(Error::ViewColumnRenamedByReplace {
                view_name: existing.view_name().to_string(),
                existing_column: existing_name.clone(),
                new_column: new_name.clone(),
            });
        }
    }
    Ok(())
}

/// The output names a `CREATE VIEW` node writes explicitly, or [`None`] when it
/// writes none and the definition would have to be read.
fn declared_names_of(node: &CreateView) -> Option<Vec<(String, bool)>> {
    (!node.columns.is_empty()).then(|| {
        node.columns
            .iter()
            .map(|column| (column.name.value.clone(), column.name.quote_style.is_some()))
            .collect()
    })
}

/// The position of the plain view a written `name` resolves to.
///
/// Resolved through the search path, so a bare name reaches the view a bare
/// reference would read, exactly as the table lookup beside it does. Comparing
/// the written qualifier against the stored one instead would miss every view
/// the path placed in a schema other than the default.
fn plain_view_position(builder: &ParserDBBuilder, name: &ObjectName) -> Option<usize> {
    let key = stored_view_key(builder.resolve_view_object_name(name).ok()??);
    builder.views().iter().position(|(view, _)| stored_view_key(view.as_ref()) == key)
}

/// The last part of a view name, without its quoting.
fn name_of(name: &ObjectName) -> Option<&str> {
    object_name_last_part(name).map(|(value, _)| value)
}

/// Drops the views a `DROP VIEW` or `DROP MATERIALIZED VIEW` names.
///
/// PostgreSQL checks the kind rather than treating the two spellings as
/// interchangeable: `DROP VIEW` refuses a materialized view and a table, and
/// `DROP MATERIALIZED VIEW` refuses a plain view, each pointing at the right
/// spelling. A name nothing holds is refused unless the statement wrote `IF
/// EXISTS`.
///
/// # Errors
///
/// Returns [`Error::RelationKindMismatch`] for a name held by another relation
/// kind, [`Error::RelationNotFound`] for a name nothing holds, and
/// [`Error::RelationHasDependents`] when a view reads the one being dropped
/// and the statement wrote no `CASCADE`.
pub(super) fn drop_views(
    mut builder: ParserDBBuilder,
    names: &[ObjectName],
    materialized: bool,
    if_exists: bool,
    cascade: bool,
) -> Result<ParserDBBuilder, Error> {
    let expected_kind = if materialized { ObjectKind::MaterializedView } else { ObjectKind::View };
    for name in names {
        let schema = view_schema_qualifier(name);
        let Some(name_ident) = object_name_last_identifier(name) else {
            return Err(Error::UnnamedObject { object_kind: expected_kind });
        };

        let position = if materialized {
            materialized_view_position(&builder, name)
        } else {
            plain_view_position(&builder, name)
        };
        if let Some(position) = position {
            let key = if materialized {
                stored_view_key(builder.materialized_views()[position].0.as_ref())
            } else {
                stored_view_key(builder.views()[position].0.as_ref())
            };
            if cascade {
                remove_dependent_views(&mut builder, &key);
            } else {
                refuse_dependent_views(&builder, &key, expected_kind, &rendered_name(name))?;
            }
            // Re-read the position: a cascade may have removed views ahead of
            // this one in the same collection.
            let position = if materialized {
                materialized_view_position(&builder, name)
            } else {
                plain_view_position(&builder, name)
            };
            if let Some(position) = position {
                if materialized {
                    builder.materialized_views_mut().remove(position);
                } else {
                    builder.views_mut().remove(position);
                }
            }
            continue;
        }

        // Nothing of the asked-for kind holds the name. Another relation kind
        // holding it is the wrong-spelling case PostgreSQL names, and a name
        // nothing holds is absent.
        match relation_name_holder(&builder, name_ident, schema) {
            Some(actual_kind) => {
                return Err(Error::RelationKindMismatch {
                    object_name: rendered_name(name),
                    expected_kind,
                    actual_kind,
                });
            }
            None if !if_exists => {
                return Err(Error::RelationNotFound {
                    object_kind: expected_kind,
                    object_name: rendered_name(name),
                });
            }
            None => {}
        }
    }
    Ok(builder)
}

/// The position of the materialized view a written `name` resolves to,
/// through the search path.
fn materialized_view_position(builder: &ParserDBBuilder, name: &ObjectName) -> Option<usize> {
    let key = stored_view_key(builder.resolve_materialized_view_object_name(name).ok()??);
    builder.materialized_views().iter().position(|(view, _)| stored_view_key(view.as_ref()) == key)
}

/// Refuses a `DROP TABLE` naming a view, as PostgreSQL does.
///
/// # Errors
///
/// Returns [`Error::RelationKindMismatch`] when the name is held by either
/// view kind.
pub(super) fn refuse_dropping_view_as_table(
    builder: &ParserDBBuilder,
    name: &ObjectName,
) -> Result<(), Error> {
    let Some(name_ident) = object_name_last_identifier(name) else {
        return Ok(());
    };
    match super::view_name_holder(builder, name_ident, view_schema_qualifier(name)) {
        Some(actual_kind) => {
            Err(Error::RelationKindMismatch {
                object_name: rendered_name(name),
                expected_kind: ObjectKind::Table,
                actual_kind,
            })
        }
        None => Ok(()),
    }
}

/// A view name as the statement wrote it, for an error message.
fn rendered_name(name: &ObjectName) -> String {
    name.to_string()
}

/// Applies an `ALTER TABLE` whose target is a view.
///
/// PostgreSQL accepts `ALTER TABLE` against a view for the actions a view
/// supports and refuses the rest naming the action. Of those the parser can
/// read, this handles renaming and changing the owner.
///
/// Only called when [`holds_view`] answered, so the name is known to be a
/// view's.
///
/// # Errors
///
/// Returns [`Error::AlterActionUnsupportedOnRelation`] for an action a view
/// does not support and [`Error::RelationNameAlreadyTaken`] when a rename asks
/// for a name another relation in the schema holds.
pub(super) fn alter_view(
    builder: ParserDBBuilder,
    name: &ObjectName,
    kind: ObjectKind,
    operation: &AlterTableOperation,
) -> Result<ParserDBBuilder, Error> {
    match operation {
        AlterTableOperation::RenameTable { table_name } => {
            let (RenameTableNameKind::As(new_name) | RenameTableNameKind::To(new_name)) =
                table_name;
            rename_view(builder, name, kind, new_name)
        }
        AlterTableOperation::OwnerTo { new_owner } => {
            Ok(set_view_owner(builder, name, kind, new_owner))
        }
        other => {
            Err(Error::AlterActionUnsupportedOnRelation {
                object_kind: kind,
                relation_name: rendered_name(name),
                operation: other.to_string(),
            })
        }
    }
}

/// The view kind a written relation name resolves to, if a view holds it.
///
/// Resolved through the search path, so a bare name naming a view the path
/// placed in another schema still answers.
pub(super) fn holds_view(builder: &ParserDBBuilder, name: &ObjectName) -> Option<ObjectKind> {
    if builder.resolve_view_object_name(name).ok().flatten().is_some() {
        return Some(ObjectKind::View);
    }
    builder
        .resolve_materialized_view_object_name(name)
        .ok()
        .flatten()
        .map(|_| ObjectKind::MaterializedView)
}

/// Renames a stored view, refusing a name another relation already holds.
///
/// The new name is checked against the pool in the schema the view actually
/// sits in, which for a view the search path placed is not the schema the
/// statement wrote.
fn rename_view(
    mut builder: ParserDBBuilder,
    name: &ObjectName,
    kind: ObjectKind,
    new_name: &ObjectName,
) -> Result<ParserDBBuilder, Error> {
    let Some(new_ident) = object_name_last_identifier(new_name) else {
        return Err(Error::UnnamedObject { object_kind: kind });
    };
    let position = if kind == ObjectKind::MaterializedView {
        materialized_view_position(&builder, name)
    } else {
        plain_view_position(&builder, name)
    };
    let Some(position) = position else {
        return Ok(builder);
    };

    let (current_name, current_quoted, schema) = if kind == ObjectKind::MaterializedView {
        let view = builder.materialized_views()[position].0.as_ref();
        (view.view_name().to_string(), view.view_name_is_quoted(), stored_schema_of(view))
    } else {
        let view = builder.views()[position].0.as_ref();
        (view.view_name().to_string(), view.view_name_is_quoted(), stored_schema_of(view))
    };

    // A rename to the name it already answers to changes nothing, and asking
    // the name pool first would report the view colliding with itself.
    if !crate::utils::identifier_resolution::identifiers_match(
        current_name.as_str(),
        current_quoted,
        new_ident.value.as_str(),
        new_ident.quote_style.is_some(),
    ) && let Some(conflicting_kind) = relation_name_holder(
        &builder,
        new_ident,
        schema.as_ref().map(|(value, quoted)| (value.as_str(), *quoted)),
    ) {
        return Err(Error::RelationNameAlreadyTaken {
            object_kind: kind,
            conflicting_kind,
            object_name: new_ident.value.clone(),
        });
    }

    let renamed = (new_ident.value.clone(), new_ident.quote_style.is_some());
    if kind == ObjectKind::MaterializedView {
        let (view, _) = &mut builder.materialized_views_mut()[position];
        Arc::make_mut(view).declaration_mut().set_name(renamed.0, renamed.1);
    } else {
        let (view, _) = &mut builder.views_mut()[position];
        Arc::make_mut(view).declaration_mut().set_name(renamed.0, renamed.1);
    }
    Ok(builder)
}

/// The schema a stored view sits in, with its quote state.
fn stored_schema_of<V: ViewLike>(view: &V) -> Option<(String, bool)> {
    view.view_schema().map(|schema| (schema.to_string(), view.view_schema_is_quoted()))
}

/// Records the role a view is handed to.
///
/// `CURRENT_ROLE`, `CURRENT_USER` and `SESSION_USER` name whoever runs the
/// statement, so the owner became one the input never spells and the model
/// stops naming it too, exactly as the table path does.
fn set_view_owner(
    mut builder: ParserDBBuilder,
    name: &ObjectName,
    kind: ObjectKind,
    new_owner: &Owner,
) -> ParserDBBuilder {
    let owner = match new_owner {
        Owner::Ident(ident) => Some(super::stored_role_name(ident)),
        Owner::CurrentRole | Owner::CurrentUser | Owner::SessionUser => None,
    };
    if kind == ObjectKind::MaterializedView {
        if let Some(position) = materialized_view_position(&builder, name) {
            builder.materialized_views_mut()[position].1.set_owner(owner);
        }
    } else if let Some(position) = plain_view_position(&builder, name) {
        builder.views_mut()[position].1.set_owner(owner);
    }
    builder
}

/// The roles the recorded views name as their owners.
pub(super) fn view_owner_names(builder: &ParserDBBuilder) -> Vec<String> {
    builder
        .views()
        .iter()
        .map(|(_, metadata)| metadata)
        .chain(builder.materialized_views().iter().map(|(_, metadata)| metadata))
        .filter_map(|metadata| metadata.owner().map(ToString::to_string))
        .collect()
}

/// Collects the relations a query reads and the names it binds itself.
///
/// A `WITH` item's name is not a stored relation, and a reference to it reads
/// that item rather than anything in the schema, so a definition writing
/// `WITH t AS (...) SELECT ... FROM t` does not read a table called `t`.
/// PostgreSQL agrees: dropping such a table is allowed and the view keeps
/// working.
struct RelationsRead {
    /// Every name used in a relation position.
    referenced: Vec<ObjectName>,
    /// Every name a `WITH` clause binds, at any depth.
    bound: Vec<Ident>,
}

impl Visitor for RelationsRead {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            self.bound.extend(with.cte_tables.iter().map(|cte| cte.alias.name.clone()));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.referenced.push(relation.clone());
        ControlFlow::Continue(())
    }
}

/// The normalized identity of every relation a view's definition reads.
///
/// A bare name the definition binds as a `WITH` item is left out, since a
/// reference to it reads that item. The names are collected across the whole
/// definition rather than per scope, so a bare name matching a `WITH` item
/// bound in a sibling scope is also left out. That errs towards missing a
/// dependency rather than inventing one, which is the safe direction: a
/// dependency this misses leaves a drop accepted that PostgreSQL would refuse,
/// where the opposite would refuse a drop PostgreSQL accepts.
///
/// A bare name resolves against the schema the view itself sits in and then
/// the default schema, which is the path the view's own creation walked: a
/// view created bare while the path selected `s` landed in `s`, and a bare
/// name in its definition reached `s` first too. The candidate that some
/// stored relation answers is the one taken, so a definition reading the
/// default schema because its own held nothing resolves there instead.
fn relations_read_by<V: ViewLike>(builder: &ParserDBBuilder, view: &V) -> Vec<RelationKey> {
    let mut visitor = RelationsRead { referenced: Vec::new(), bound: Vec::new() };
    let walk = view.definition().visit(&mut visitor);
    debug_assert!(walk.is_continue(), "the visitor never breaks");

    let own_schema = view.view_schema().map(|schema| {
        crate::utils::identifier_resolution::normalize_identifier(
            schema,
            view.view_schema_is_quoted(),
        )
        .into_owned()
    });

    visitor
        .referenced
        .iter()
        .filter(|name| {
            // Only a one-part name can be a `WITH` item's.
            name.0.len() != 1
                || !object_name_last_identifier(name).is_some_and(|referenced| {
                    visitor.bound.iter().any(|bound| {
                        crate::utils::identifier_resolution::identifiers_match(
                            bound.value.as_str(),
                            bound.quote_style.is_some(),
                            referenced.value.as_str(),
                            referenced.quote_style.is_some(),
                        )
                    })
                })
        })
        .filter_map(|name| {
            let target = target_name_from_object_name(name)?;
            let written = target_key(&target);
            if target.schema().is_some() {
                return Some(written);
            }
            // `target_key` already read a bare name as the default schema's,
            // so that spelling is the second candidate.
            let in_own_schema = own_schema
                .as_ref()
                .map(|schema| RelationKey { schema: schema.clone(), name: written.name.clone() });
            match in_own_schema {
                Some(candidate) if relation_key_is_held(builder, &candidate) => Some(candidate),
                _ => Some(written),
            }
        })
        .collect()
}

/// Whether any stored relation answers `key`.
fn relation_key_is_held(builder: &ParserDBBuilder, key: &RelationKey) -> bool {
    builder.tables().iter().any(|(table, _)| stored_table_key(table.as_ref()) == *key)
        || builder.views().iter().any(|(view, _)| stored_view_key(view.as_ref()) == *key)
        || builder
            .materialized_views()
            .iter()
            .any(|(view, _)| stored_view_key(view.as_ref()) == *key)
}

/// Every view reading the relation `key` names, and every view reading one of
/// those, transitively.
///
/// PostgreSQL refuses to drop a relation while anything reads it, and takes
/// the whole chain with it under `CASCADE`, so the walk closes over the chain
/// rather than stopping at the first level.
pub(super) fn dependent_views(
    builder: &ParserDBBuilder,
    key: &RelationKey,
) -> Vec<(ObjectKind, RelationKey)> {
    // Each view's read set is derived once, not once per step of the walk.
    // Deriving it means visiting a whole definition and normalizing every name
    // it reads, so re-deriving per step made one drop cost the square of the
    // number of views.
    let reads: Vec<(ObjectKind, RelationKey, Vec<RelationKey>)> = builder
        .views()
        .iter()
        .map(|(view, _)| {
            (
                ObjectKind::View,
                stored_view_key(view.as_ref()),
                relations_read_by(builder, view.as_ref()),
            )
        })
        .chain(builder.materialized_views().iter().map(|(view, _)| {
            (
                ObjectKind::MaterializedView,
                stored_view_key(view.as_ref()),
                relations_read_by(builder, view.as_ref()),
            )
        }))
        .collect();

    let mut frontier = alloc::vec![key.clone()];
    let mut found: Vec<(ObjectKind, RelationKey)> = Vec::new();

    while let Some(current) = frontier.pop() {
        for (kind, own_key, read) in &reads {
            if !read.contains(&current) || *own_key == *key {
                continue;
            }
            let reader = (*kind, own_key.clone());
            if !found.contains(&reader) {
                frontier.push(own_key.clone());
                found.push(reader);
            }
        }
    }

    found
}

/// Refuses dropping the relation `key` names while a view reads it.
///
/// # Errors
///
/// Returns [`Error::RelationHasDependents`] naming the first reader, which is
/// what PostgreSQL reports before listing the rest.
pub(super) fn refuse_dependent_views(
    builder: &ParserDBBuilder,
    key: &RelationKey,
    object_kind: ObjectKind,
    object_name: &str,
) -> Result<(), Error> {
    if let Some((dependent_kind, dependent)) = dependent_views(builder, key).first() {
        return Err(Error::RelationHasDependents {
            object_kind,
            object_name: object_name.to_string(),
            dependent_kind: *dependent_kind,
            dependent_name: dependent.name.clone(),
        });
    }
    Ok(())
}

/// Removes the views a `CASCADE` takes along with the relation `key` names.
pub(super) fn remove_dependent_views(builder: &mut ParserDBBuilder, key: &RelationKey) {
    let doomed = dependent_views(builder, key);
    builder.views_mut().retain(|(view, _)| {
        !doomed.iter().any(|(kind, dead)| {
            *kind == ObjectKind::View && *dead == stored_view_key(view.as_ref())
        })
    });
    builder.materialized_views_mut().retain(|(view, _)| {
        !doomed.iter().any(|(kind, dead)| {
            *kind == ObjectKind::MaterializedView && *dead == stored_view_key(view.as_ref())
        })
    });
}
