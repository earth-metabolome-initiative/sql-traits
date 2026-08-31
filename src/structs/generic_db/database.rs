//! Implementation of the `DatabaseLike` trait for `GenericDB`.

use alloc::vec::Vec;

use crate::{
    errors::LookupError,
    structs::{GenericDB, TargetName},
    traits::{
        CheckConstraintLike, ColumnGrantLike, ColumnLike, DatabaseLike, DialectLike,
        ForeignKeyLike, FunctionLike, IndexLike, PolicyLike, RoleLike, SchemaLike, TableGrantLike,
        TableLike, TriggerLike, UniqueIndexLike,
    },
    utils::{
        identifier_resolution::{normalize_identifier, stored_identifier_matches_lookup},
        object_name::{TableTargetKey, lookup_key, resolve_target_from_candidates, target_key},
    },
};

impl<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG, D> DatabaseLike
    for GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG, D>
where
    T: TableLike<DB = Self>,
    C: ColumnLike<DB = Self>,
    I: IndexLike<DB = Self>,
    U: UniqueIndexLike<DB = Self>,
    F: ForeignKeyLike<DB = Self>,
    Func: FunctionLike<DB = Self>,
    Ch: CheckConstraintLike<DB = Self>,
    Tr: TriggerLike<DB = Self>,
    P: PolicyLike<DB = Self>,
    R: RoleLike<DB = Self>,
    S: SchemaLike<DB = Self>,
    TG: TableGrantLike<DB = Self>,
    CG: ColumnGrantLike<DB = Self>,
    D: DialectLike<DB = Self>,
{
    type Table = T;
    type Column = C;
    type Index = I;
    type ForeignKey = F;
    type Function = Func;
    type UniqueIndex = U;
    type CheckConstraint = Ch;
    type Trigger = Tr;
    type Policy = P;
    type Role = R;
    type TableGrant = TG;
    type ColumnGrant = CG;
    type Schema = S;
    type Dialect = D;

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
        let key = lookup_key(schema, table_name);
        self.indexed_table_positions(&key)
            .next()
            .and_then(|position| self.tables.get(position))
            .map(|(table, _)| table.as_ref())
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

impl<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG, D>
    GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, S, TG, CG, D>
where
    T: TableLike,
    C: ColumnLike,
    I: IndexLike,
    U: UniqueIndexLike,
    F: ForeignKeyLike,
    Func: FunctionLike,
    Ch: CheckConstraintLike,
    Tr: TriggerLike,
    P: PolicyLike,
    R: RoleLike,
    S: SchemaLike,
    TG: TableGrantLike,
    CG: ColumnGrantLike,
    D: DialectLike,
{
    /// Positions in `tables` whose stored identity equals `key`, ascending.
    fn indexed_table_positions(&self, key: &TableTargetKey) -> impl Iterator<Item = usize> {
        self.table_index.get(key).map_or(&[][..], Vec::as_slice).iter().copied()
    }

    /// Tables whose stored identity equals `key`, in storage order.
    pub(super) fn indexed_tables(&self, key: &TableTargetKey) -> impl Iterator<Item = &T> {
        self.indexed_table_positions(key)
            .filter_map(|position| self.tables.get(position))
            .map(|(table, _)| table.as_ref())
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
    ) -> Result<Option<&T>, LookupError> {
        let candidates: Vec<&T> = self.indexed_tables(&target_key(target)).collect();
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
    ) -> Result<Option<&T>, LookupError> {
        if target.schema().is_some() {
            return self.resolve_target_table_strict(target);
        }

        let name = normalize_identifier(target.name(), target.name_is_quoted()).into_owned();
        for (entry_schema, entry_quoted) in &self.search_path {
            let key = TableTargetKey {
                schema: normalize_identifier(entry_schema, *entry_quoted).into_owned(),
                name: name.clone(),
            };
            if let Some(positions) = self.table_index.get(&key) {
                let candidates: Vec<&T> = positions
                    .iter()
                    .filter_map(|position| self.tables.get(*position))
                    .map(|(table, _)| table.as_ref())
                    .collect();
                // Reported under the written name: the entry qualifier is
                // resolution machinery, not something the statement spelled.
                return resolve_target_from_candidates(target, &candidates);
            }
        }

        Ok(None)
    }
}
