//! Builder for constructing a `GenericDB` instance.

use std::rc::Rc;

use crate::{
    structs::GenericDB,
    traits::{
        CheckConstraintLike, ColumnGrantLike, ColumnLike, ForeignKeyLike, FunctionLike, IndexLike,
        PolicyLike, RoleLike, TableGrantLike, TableLike, TriggerLike, UniqueIndexLike,
    },
};

/// Builder for constructing a `GenericDB` instance.
pub struct GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
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
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    /// Catalog name of the database.
    catalog_name: String,
    /// Timezone of the database.
    timezone: Option<String>,
    /// List of tables in the database.
    tables: Vec<(Rc<T>, T::Meta)>,
    /// List of columns in the database.
    columns: Vec<(Rc<C>, C::Meta)>,
    /// List of indices in the database.
    indices: Vec<(Rc<I>, I::Meta)>,
    /// List of unique indices in the database.
    unique_indices: Vec<(Rc<U>, U::Meta)>,
    /// List of foreign keys in the database.
    foreign_keys: Vec<(Rc<F>, F::Meta)>,
    /// List of functions created in the database.
    functions: Vec<(Rc<Func>, Func::Meta)>,
    /// List of triggers created in the database.
    triggers: Vec<(Rc<Tr>, Tr::Meta)>,
    /// List of policies created in the database.
    policies: Vec<(Rc<P>, P::Meta)>,
    /// List of check constraints in the database.
    check_constraints: Vec<(Rc<Ch>, Ch::Meta)>,
    /// List of roles in the database.
    roles: Vec<(Rc<R>, R::Meta)>,
    /// List of table grants in the database.
    table_grants: Vec<(Rc<TG>, TG::Meta)>,
    /// List of column grants in the database.
    column_grants: Vec<(Rc<CG>, CG::Meta)>,
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
    GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
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
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    /// Returns a mutable reference to the tables list.
    pub(crate) fn tables_mut(&mut self) -> &mut Vec<(Rc<T>, T::Meta)> {
        &mut self.tables
    }

    /// Returns a mutable reference to the table grants list.
    pub(crate) fn table_grants_mut(&mut self) -> &mut Vec<(Rc<TG>, TG::Meta)> {
        &mut self.table_grants
    }

    /// Returns a mutable reference to the column grants list.
    pub(crate) fn column_grants_mut(&mut self) -> &mut Vec<(Rc<CG>, CG::Meta)> {
        &mut self.column_grants
    }

    /// Returns a mutable reference to the functions list.
    pub(crate) fn functions_mut(&mut self) -> &mut Vec<(Rc<Func>, Func::Meta)> {
        &mut self.functions
    }

    /// Returns a slice of check constraint Rc references with their metadata.
    pub(crate) fn check_constraints(&self) -> &[(Rc<Ch>, Ch::Meta)] {
        &self.check_constraints
    }

    /// Returns a slice of policy Rc references with their metadata.
    pub(crate) fn policies(&self) -> &[(Rc<P>, P::Meta)] {
        &self.policies
    }

    /// Returns a slice of trigger Rc references with their metadata.
    pub(crate) fn triggers(&self) -> &[(Rc<Tr>, Tr::Meta)] {
        &self.triggers
    }

    /// Returns a slice of foreign key Rc references with their metadata.
    pub(crate) fn foreign_keys(&self) -> &[(Rc<F>, F::Meta)] {
        &self.foreign_keys
    }

    /// Returns a mutable reference to the columns list.
    pub(crate) fn columns_mut(&mut self) -> &mut Vec<(Rc<C>, C::Meta)> {
        &mut self.columns
    }

    /// Returns a mutable reference to the indices list.
    pub(crate) fn indices_mut(&mut self) -> &mut Vec<(Rc<I>, I::Meta)> {
        &mut self.indices
    }

    /// Returns a mutable reference to the unique indices list.
    pub(crate) fn unique_indices_mut(&mut self) -> &mut Vec<(Rc<U>, U::Meta)> {
        &mut self.unique_indices
    }

    /// Returns a mutable reference to the foreign keys list.
    pub(crate) fn foreign_keys_mut(&mut self) -> &mut Vec<(Rc<F>, F::Meta)> {
        &mut self.foreign_keys
    }

    /// Returns a mutable reference to the check constraints list.
    pub(crate) fn check_constraints_mut(&mut self) -> &mut Vec<(Rc<Ch>, Ch::Meta)> {
        &mut self.check_constraints
    }

    /// Returns a mutable reference to the triggers list.
    pub(crate) fn triggers_mut(&mut self) -> &mut Vec<(Rc<Tr>, Tr::Meta)> {
        &mut self.triggers
    }

    /// Returns a mutable reference to the policies list.
    pub(crate) fn policies_mut(&mut self) -> &mut Vec<(Rc<P>, P::Meta)> {
        &mut self.policies
    }

    #[must_use]
    /// Creates a new `GenericDBBuilder` instance.
    pub fn new(catalog_name: String) -> Self {
        Self {
            catalog_name,
            timezone: None,
            tables: Vec::new(),
            columns: Vec::new(),
            indices: Vec::new(),
            unique_indices: Vec::new(),
            foreign_keys: Vec::new(),
            functions: Vec::new(),
            triggers: Vec::new(),
            policies: Vec::new(),
            check_constraints: Vec::new(),
            roles: Vec::new(),
            table_grants: Vec::new(),
            column_grants: Vec::new(),
        }
    }
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
    GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
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
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    /// Sets the timezone for the database.
    #[must_use]
    #[inline]
    pub fn timezone(mut self, timezone: String) -> Self {
        self.timezone = Some(timezone);
        self
    }

    /// Adds a table with its metadata to the builder.
    #[must_use]
    pub fn add_table(mut self, table: Rc<T>, metadata: T::Meta) -> Self {
        self.tables.push((table, metadata));
        self
    }

    /// Adds multiple tables with their metadata to the builder.
    #[must_use]
    pub fn add_tables(mut self, tables: impl IntoIterator<Item = (Rc<T>, T::Meta)>) -> Self {
        self.tables.extend(tables);
        self
    }

    /// Adds a column with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_column(mut self, column: Rc<C>, metadata: C::Meta) -> Self {
        self.columns.push((column, metadata));
        self
    }

    /// Adds multiple columns with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_columns(mut self, columns: impl IntoIterator<Item = (Rc<C>, C::Meta)>) -> Self {
        self.columns.extend(columns);
        self
    }

    /// Adds an index with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_index(mut self, index: Rc<I>, metadata: I::Meta) -> Self {
        self.indices.push((index, metadata));
        self
    }

    /// Adds multiple indices with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_indices(mut self, indices: impl IntoIterator<Item = (Rc<I>, I::Meta)>) -> Self {
        self.indices.extend(indices);
        self
    }

    /// Adds a unique index with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_unique_index(mut self, index: Rc<U>, metadata: U::Meta) -> Self {
        self.unique_indices.push((index, metadata));
        self
    }

    /// Adds multiple unique indices with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_unique_indices(
        mut self,
        indices: impl IntoIterator<Item = (Rc<U>, U::Meta)>,
    ) -> Self {
        self.unique_indices.extend(indices);
        self
    }

    /// Adds a foreign key with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_foreign_key(mut self, key: Rc<F>, metadata: F::Meta) -> Self {
        self.foreign_keys.push((key, metadata));
        self
    }

    /// Adds multiple foreign keys with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_foreign_keys(mut self, keys: impl IntoIterator<Item = (Rc<F>, F::Meta)>) -> Self {
        self.foreign_keys.extend(keys);
        self
    }

    /// Adds a function with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_function(mut self, function: Rc<Func>, metadata: Func::Meta) -> Self {
        self.functions.push((function, metadata));
        self
    }

    /// Adds a trigger with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_trigger(mut self, trigger: Rc<Tr>, metadata: Tr::Meta) -> Self {
        self.triggers.push((trigger, metadata));
        self
    }

    /// Adds a policy with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_policy(mut self, policy: Rc<P>, metadata: P::Meta) -> Self {
        self.policies.push((policy, metadata));
        self
    }

    /// Adds multiple policies with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_policies(mut self, policies: impl IntoIterator<Item = (Rc<P>, P::Meta)>) -> Self {
        self.policies.extend(policies);
        self
    }

    /// Adds multiple functions with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_functions(
        mut self,
        functions: impl IntoIterator<Item = (Rc<Func>, Func::Meta)>,
    ) -> Self {
        self.functions.extend(functions);
        self
    }

    /// Returns a vector of function Rc references.
    #[must_use]
    pub fn function_rc_vec(&self) -> Vec<Rc<Func>> {
        self.functions.iter().map(|(func_rc, _)| func_rc.clone()).collect()
    }

    /// Returns a slice of table Rc references with their metadata.
    #[must_use]
    pub fn tables(&self) -> &[(Rc<T>, T::Meta)] {
        &self.tables
    }

    /// Returns a slice of role Rc references with their metadata.
    #[must_use]
    pub fn roles(&self) -> &[(Rc<R>, R::Meta)] {
        &self.roles
    }

    /// Adds a check constraint with its metadata to the builder.
    #[must_use]
    pub fn add_check_constraint(mut self, constraint: Rc<Ch>, metadata: Ch::Meta) -> Self {
        self.check_constraints.push((constraint, metadata));
        self
    }

    /// Adds a role with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_role(mut self, role: Rc<R>, metadata: R::Meta) -> Self {
        self.roles.push((role, metadata));
        self
    }

    /// Adds multiple roles with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_roles(mut self, roles: impl IntoIterator<Item = (Rc<R>, R::Meta)>) -> Self {
        self.roles.extend(roles);
        self
    }

    /// Adds a table grant with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_table_grant(mut self, grant: Rc<TG>, metadata: TG::Meta) -> Self {
        self.table_grants.push((grant, metadata));
        self
    }

    /// Adds multiple table grants with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_table_grants(
        mut self,
        grants: impl IntoIterator<Item = (Rc<TG>, TG::Meta)>,
    ) -> Self {
        self.table_grants.extend(grants);
        self
    }

    /// Adds a column grant with its metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_column_grant(mut self, grant: Rc<CG>, metadata: CG::Meta) -> Self {
        self.column_grants.push((grant, metadata));
        self
    }

    /// Adds multiple column grants with their metadata to the builder.
    #[must_use]
    #[inline]
    pub fn add_column_grants(
        mut self,
        grants: impl IntoIterator<Item = (Rc<CG>, CG::Meta)>,
    ) -> Self {
        self.column_grants.extend(grants);
        self
    }
}

impl<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
    From<GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>>
    for GenericDB<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>
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
    TG: TableGrantLike,
    CG: ColumnGrantLike,
{
    fn from(mut builder: GenericDBBuilder<T, C, I, U, F, Func, Ch, Tr, P, R, TG, CG>) -> Self {
        let catalog_name = builder.catalog_name;

        builder.tables.sort_unstable_by_key(|(table, _)| {
            (
                table.table_schema().map(std::string::ToString::to_string),
                table.table_name().to_string(),
            )
        });

        builder.columns.sort_unstable_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));
        builder.indices.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        builder.unique_indices.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        builder.foreign_keys.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        builder.functions.sort_unstable_by(|(a, _), (b, _)| a.name().cmp(b.name()));
        builder.triggers.sort_unstable_by(|(a, _), (b, _)| a.name().cmp(b.name()));
        builder.policies.sort_unstable_by(|(a, _), (b, _)| a.name().cmp(b.name()));
        builder.check_constraints.sort_unstable_by(|(a, _), (b, _)| a.as_ref().cmp(b.as_ref()));
        builder.roles.sort_unstable_by(|(a, _), (b, _)| a.name().cmp(b.name()));
        // Grants are not sorted as their order may be significant

        GenericDB {
            catalog_name,
            timezone: builder.timezone,
            tables: builder.tables,
            columns: builder.columns,
            indices: builder.indices,
            unique_indices: builder.unique_indices,
            foreign_keys: builder.foreign_keys,
            functions: builder.functions,
            triggers: builder.triggers,
            policies: builder.policies,
            check_constraints: builder.check_constraints,
            roles: builder.roles,
            table_grants: builder.table_grants,
            column_grants: builder.column_grants,
        }
    }
}
