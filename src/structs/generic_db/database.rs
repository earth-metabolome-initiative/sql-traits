//! Implementation of the `DatabaseLike` trait for `GenericDB`.

use alloc::{string::String, vec::Vec};

use crate::{
    errors::LookupError,
    structs::{GenericDB, SchemaProfile, TargetName, generic_db::RelationSlot},
    traits::{DatabaseLike, FunctionLike, TableLike},
    utils::{
        identifier_resolution::{normalize_identifier, stored_identifier_matches_lookup},
        object_name::{
            RelationKey, lookup_key, render_view_candidate, resolve_one_relation,
            resolve_target_from_candidates, target_key,
        },
    },
};

impl<P: SchemaProfile> DatabaseLike for GenericDB<P> {
    type Table = P::Table;
    type View = P::View;
    type MaterializedView = P::MaterializedView;
    type Column = P::Column;
    type Index = P::Index;
    type ForeignKey = P::ForeignKey;
    type Function = P::Function;
    type UniqueIndex = P::UniqueIndex;
    type CheckConstraint = P::CheckConstraint;
    type Trigger = P::Trigger;
    type Policy = P::Policy;
    type Role = P::Role;
    type TableGrant = P::TableGrant;
    type ColumnGrant = P::ColumnGrant;
    type Schema = P::Schema;
    type Dialect = P::Dialect;

    #[inline]
    fn dialect(&self) -> &Self::Dialect {
        &self.dialect
    }

    #[inline]
    fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    #[inline]
    fn number_of_tables(&self) -> usize {
        self.tables.len()
    }

    #[inline]
    fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    #[inline]
    fn search_path(&self) -> impl Iterator<Item = (&str, bool)> {
        self.search_path.iter().map(|(name, quoted)| (name.as_str(), *quoted))
    }

    fn table(&self, schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        self.indexed_tables(&lookup_key(schema, table_name)).next()
    }

    fn views(&self) -> impl Iterator<Item = &Self::View> {
        self.views.iter().map(|(view, _)| view.as_ref())
    }

    fn materialized_views(&self) -> impl Iterator<Item = &Self::MaterializedView> {
        self.materialized_views.iter().map(|(view, _)| view.as_ref())
    }

    fn view(&self, schema: Option<&str>, view_name: &str) -> Option<&Self::View> {
        self.indexed_views(&lookup_key(schema, view_name)).next()
    }

    fn materialized_view(
        &self,
        schema: Option<&str>,
        view_name: &str,
    ) -> Option<&Self::MaterializedView> {
        self.indexed_materialized_views(&lookup_key(schema, view_name)).next()
    }

    fn table_id(&self, table: &Self::Table) -> Option<usize> {
        self.tables
            .binary_search_by_key(&(table.table_schema(), table.table_name()), |(t, _)| {
                (t.table_schema(), t.table_name())
            })
            .ok()
    }

    fn resolve_target_table(
        &self,
        target: TargetName<'_>,
    ) -> Result<Option<&Self::Table>, LookupError> {
        self.resolve_target_table_on_path(&target)
    }

    fn resolve_target_view(
        &self,
        target: TargetName<'_>,
    ) -> Result<Option<&Self::View>, LookupError> {
        self.resolve_target_view_on_path(&target)
    }

    fn resolve_target_materialized_view(
        &self,
        target: TargetName<'_>,
    ) -> Result<Option<&Self::MaterializedView>, LookupError> {
        self.resolve_target_materialized_view_on_path(&target)
    }

    fn table_by_id(&self, table_id: usize) -> Option<&Self::Table> {
        self.tables.get(table_id).map(|(table, _)| table.as_ref())
    }

    #[inline]
    fn tables(&self) -> impl Iterator<Item = &Self::Table> {
        self.tables.iter().map(|(table, _)| table.as_ref())
    }

    #[inline]
    fn triggers(&self) -> impl Iterator<Item = &Self::Trigger> {
        self.triggers.iter().map(|(trigger, _)| trigger.as_ref())
    }

    #[inline]
    fn indexes(&self) -> impl Iterator<Item = &Self::Index> {
        self.indices.iter().map(|(index, _)| index.as_ref())
    }

    #[inline]
    fn functions(&self) -> impl Iterator<Item = &Self::Function> {
        self.functions.iter().map(|(func, _)| func.as_ref())
    }

    fn function(&self, name: &str) -> Option<&Self::Function> {
        self.functions.iter().find_map(|(function, _)| {
            stored_identifier_matches_lookup(function.name(), function.name_is_quoted(), name)
                .then_some(function.as_ref())
        })
    }

    fn policies(&self) -> impl Iterator<Item = &Self::Policy> {
        self.policies.iter().map(|(p, _)| p.as_ref())
    }

    fn roles(&self) -> impl Iterator<Item = &Self::Role> {
        self.roles.iter().map(|(r, _)| r.as_ref())
    }

    fn table_grants(&self) -> impl Iterator<Item = &Self::TableGrant> {
        self.table_grants.iter().map(|(g, _)| g.as_ref())
    }

    fn column_grants(&self) -> impl Iterator<Item = &Self::ColumnGrant> {
        self.column_grants.iter().map(|(g, _)| g.as_ref())
    }

    fn schemas(&self) -> impl Iterator<Item = &Self::Schema> {
        self.schemas.iter().map(|(s, _)| s.as_ref())
    }
}

impl<P: SchemaProfile> GenericDB<P> {
    /// Slots of every relation whose stored identity equals `key`.
    fn indexed_relation_slots(&self, key: &RelationKey) -> &[RelationSlot] {
        self.relation_index.get(key).map_or(&[][..], Vec::as_slice)
    }

    /// Whether any relation of any kind answers `key`.
    ///
    /// Tables, views and materialized views share one pool of names, so this
    /// is what a creation asks before taking a name.
    pub(super) fn relation_key_is_taken(&self, key: &RelationKey) -> bool {
        !self.indexed_relation_slots(key).is_empty()
    }

    /// Tables whose stored identity equals `key`, in storage order.
    pub(super) fn indexed_tables(&self, key: &RelationKey) -> impl Iterator<Item = &P::Table> {
        self.indexed_relation_slots(key)
            .iter()
            .filter_map(|slot| {
                match slot {
                    RelationSlot::Table(position) => self.tables.get(*position),
                    RelationSlot::View(_) | RelationSlot::MaterializedView(_) => None,
                }
            })
            .map(|(table, _)| table.as_ref())
    }

    /// Plain views whose stored identity equals `key`, in storage order.
    pub(super) fn indexed_views(&self, key: &RelationKey) -> impl Iterator<Item = &P::View> {
        self.indexed_relation_slots(key)
            .iter()
            .filter_map(|slot| {
                match slot {
                    RelationSlot::View(position) => self.views.get(*position),
                    RelationSlot::Table(_) | RelationSlot::MaterializedView(_) => None,
                }
            })
            .map(|(view, _)| view.as_ref())
    }

    /// Materialized views whose stored identity equals `key`, in storage
    /// order.
    pub(super) fn indexed_materialized_views(
        &self,
        key: &RelationKey,
    ) -> impl Iterator<Item = &P::MaterializedView> {
        self.indexed_relation_slots(key)
            .iter()
            .filter_map(|slot| {
                match slot {
                    RelationSlot::MaterializedView(position) => {
                        self.materialized_views.get(*position)
                    }
                    RelationSlot::Table(_) | RelationSlot::View(_) => None,
                }
            })
            .map(|(view, _)| view.as_ref())
    }

    /// Resolves a written target against the index, ignoring any search path.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the name matches
    /// more than one table.
    pub(super) fn resolve_target_table_strict(
        &self,
        target: &TargetName<'_>,
    ) -> Result<Option<&P::Table>, LookupError> {
        let candidates: Vec<&P::Table> = self.indexed_tables(&target_key(target)).collect();
        resolve_target_from_candidates(target, &candidates)
    }

    /// Resolves a written target through the index and the search path.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the name matches
    /// more than one table in the schema that wins.
    pub(super) fn resolve_target_table_on_path(
        &self,
        target: &TargetName<'_>,
    ) -> Result<Option<&P::Table>, LookupError> {
        if target.schema().is_some() {
            return self.resolve_target_table_strict(target);
        }

        let name = normalize_identifier(target.name(), target.name_is_quoted()).into_owned();
        for (entry_schema, entry_quoted) in &self.search_path {
            let key = RelationKey {
                schema: normalize_identifier(entry_schema, *entry_quoted).into_owned(),
                name: name.clone(),
            };
            if self.relation_key_is_taken(&key) {
                let candidates: Vec<&P::Table> = self.indexed_tables(&key).collect();
                // Reported under the written name: the entry qualifier is
                // resolution machinery, not something the statement spelled.
                // A schema holding the name under another relation kind still
                // ends the walk, as PostgreSQL's own name resolution does.
                return resolve_target_from_candidates(target, &candidates);
            }
        }

        Ok(None)
    }

    /// Resolves a written target to a plain view, applying the search path to
    /// an unqualified name exactly as a table lookup does.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the name matches
    /// more than one view in the schema that wins.
    pub(super) fn resolve_target_view_on_path(
        &self,
        target: &TargetName<'_>,
    ) -> Result<Option<&P::View>, LookupError> {
        self.resolve_relation_on_path(
            target,
            |db, key| db.indexed_views(key).collect(),
            render_view_candidate,
        )
    }

    /// Resolves a written target to a materialized view, applying the search
    /// path to an unqualified name exactly as a table lookup does.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when the name matches
    /// more than one materialized view in the schema that wins.
    pub(super) fn resolve_target_materialized_view_on_path(
        &self,
        target: &TargetName<'_>,
    ) -> Result<Option<&P::MaterializedView>, LookupError> {
        self.resolve_relation_on_path(
            target,
            |db, key| db.indexed_materialized_views(key).collect(),
            render_view_candidate,
        )
    }

    /// Resolves a written target to one relation of a kind, walking the search
    /// path for an unqualified name.
    ///
    /// A schema holding the name under any relation kind ends the walk, as
    /// PostgreSQL's own name resolution does, so a name taken by a table is
    /// not looked for again further along the path as a view.
    fn resolve_relation_on_path<'db, R>(
        &'db self,
        target: &TargetName<'_>,
        candidates_of: impl Fn(&'db Self, &RelationKey) -> Vec<&'db R>,
        render: impl Fn(&R) -> String,
    ) -> Result<Option<&'db R>, LookupError> {
        if target.schema().is_some() {
            let candidates = candidates_of(self, &target_key(target));
            return resolve_one_relation(target, &candidates, render);
        }

        let name = normalize_identifier(target.name(), target.name_is_quoted()).into_owned();
        for (entry_schema, entry_quoted) in &self.search_path {
            let key = RelationKey {
                schema: normalize_identifier(entry_schema, *entry_quoted).into_owned(),
                name: name.clone(),
            };
            if self.relation_key_is_taken(&key) {
                let candidates = candidates_of(self, &key);
                return resolve_one_relation(target, &candidates, render);
            }
        }

        Ok(None)
    }
}
