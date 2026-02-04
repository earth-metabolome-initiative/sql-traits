//! Implementation of the `DatabaseLike` trait for `GenericDB`.

use crate::{
    structs::GenericDB,
    traits::{
        CheckConstraintLike, ColumnGrantLike, ColumnLike, DatabaseLike, ForeignKeyLike,
        FunctionLike, IndexLike, PolicyLike, RoleLike, TableGrantLike, TableLike, TriggerLike,
        UniqueIndexLike,
    },
};

impl<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG> DatabaseLike
    for GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
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
    TG: TableGrantLike<DB = Self>,
    CG: ColumnGrantLike<DB = Self>,
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

    fn table(&self, schema: Option<&str>, table_name: &str) -> Option<&Self::Table> {
        // The tables are sorted by schema and name, so we can use binary search.
        let key = (schema, table_name);
        self.tables
            .binary_search_by_key(&key, |(table, _)| (table.table_schema(), table.table_name()))
            .ok()
            .map(|index| self.tables[index].0.as_ref())
    }

    fn table_id(&self, table: &Self::Table) -> Option<usize> {
        self.tables
            .binary_search_by_key(&(table.table_schema(), table.table_name()), |(t, _)| {
                (t.table_schema(), t.table_name())
            })
            .ok()
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
    fn functions(&self) -> impl Iterator<Item = &Self::Function> {
        self.functions.iter().map(|(func, _)| func.as_ref())
    }

    fn function(&self, name: &str) -> Option<&Self::Function> {
        self.functions
            .binary_search_by(|(f, _)| f.name().cmp(name))
            .ok()
            .map(|index| self.functions[index].0.as_ref())
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
}
