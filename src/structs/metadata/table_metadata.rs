//! Submodule defining a generic `TableMetadata` struct.

use std::rc::Rc;

use crate::traits::{DatabaseLike, DocumentationMetadata, TableLike};

#[derive(Debug, Clone)]
/// Metadata about a database table.
pub struct TableMetadata<T: TableLike> {
    /// The columns of the table.
    columns: Vec<Rc<<T::DB as DatabaseLike>::Column>>,
    /// The check constraints of the table.
    check_constraints: Vec<Rc<<T::DB as DatabaseLike>::CheckConstraint>>,
    /// The indices of the table.
    indices: Vec<Rc<<T::DB as DatabaseLike>::Index>>,
    /// The unique indices of the table.
    unique_indices: Vec<Rc<<T::DB as DatabaseLike>::UniqueIndex>>,
    /// The foreign keys of the table.
    foreign_keys: Vec<Rc<<T::DB as DatabaseLike>::ForeignKey>>,
    /// The columns composing the primary key of the table.
    primary_key: Vec<Rc<<T::DB as DatabaseLike>::Column>>,
    /// Whether Row Level Security is enabled for the table.
    rls_enabled: bool,
    /// Whether Row Level Security is forced for the table (applies to table
    /// owners too).
    rls_forced: bool,
    /// The optional documentation associated with the table
    documentation: Option<<T as DocumentationMetadata>::Documentation>,
}

impl<T: TableLike> Default for TableMetadata<T> {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            check_constraints: Vec::new(),
            indices: Vec::new(),
            unique_indices: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key: Vec::new(),
            rls_enabled: false,
            rls_forced: false,
            documentation: None,
        }
    }
}

impl<T: TableLike> TableMetadata<T> {
    /// Returns whether Row Level Security is enabled for the table.
    #[inline]
    pub fn rls_enabled(&self) -> bool {
        self.rls_enabled
    }

    /// Sets whether Row Level Security is enabled for the table.
    ///
    /// # Arguments
    ///
    /// * `rls_enabled` - Whether Row Level Security is enabled.
    #[inline]
    pub fn set_rls_enabled(&mut self, rls_enabled: bool) {
        self.rls_enabled = rls_enabled;
    }

    /// Returns whether Row Level Security is forced for the table.
    ///
    /// When RLS is forced, the policies apply even to the table owner,
    /// unlike regular RLS where the owner bypasses policies.
    #[inline]
    pub fn rls_forced(&self) -> bool {
        self.rls_forced
    }

    /// Sets whether Row Level Security is forced for the table.
    ///
    /// # Arguments
    ///
    /// * `rls_forced` - Whether Row Level Security is forced.
    #[inline]
    pub fn set_rls_forced(&mut self, rls_forced: bool) {
        self.rls_forced = rls_forced;
    }

    /// Returns an iterator over the references of columns of the table.
    #[inline]
    pub fn columns(&self) -> impl Iterator<Item = &<T::DB as DatabaseLike>::Column> {
        self.columns.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the Rc of columns of the table.
    #[inline]
    pub fn column_rcs(&self) -> impl Iterator<Item = &Rc<<T::DB as DatabaseLike>::Column>> {
        self.columns.iter()
    }

    /// Returns a slice of the Rc of columns of the table.
    #[must_use]
    #[inline]
    pub fn column_rc_slice(&self) -> &[Rc<<T::DB as DatabaseLike>::Column>] {
        &self.columns
    }

    /// Returns an iterator over the check constraints of the table.
    #[inline]
    pub fn check_constraints(
        &self,
    ) -> impl Iterator<Item = &<T::DB as DatabaseLike>::CheckConstraint> {
        self.check_constraints.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the Rc of check constraints of the table.
    #[inline]
    pub fn check_constraint_rcs(
        &self,
    ) -> impl Iterator<Item = &Rc<<T::DB as DatabaseLike>::CheckConstraint>> {
        self.check_constraints.iter()
    }

    /// Returns an iterator over the indices of the table.
    #[inline]
    pub fn indices(&self) -> impl Iterator<Item = &<T::DB as DatabaseLike>::Index> {
        self.indices.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the Rc of indices of the table.
    #[inline]
    pub fn index_rcs(&self) -> impl Iterator<Item = &Rc<<T::DB as DatabaseLike>::Index>> {
        self.indices.iter()
    }

    /// Returns an iterator over the unique indices of the table.
    #[inline]
    pub fn unique_indices(&self) -> impl Iterator<Item = &<T::DB as DatabaseLike>::UniqueIndex> {
        self.unique_indices.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the Rc of unique indices of the table.
    #[inline]
    pub fn unique_index_rcs(
        &self,
    ) -> impl Iterator<Item = &Rc<<T::DB as DatabaseLike>::UniqueIndex>> {
        self.unique_indices.iter()
    }

    /// Returns an iterator over the foreign keys of the table.
    #[inline]
    pub fn foreign_keys(&self) -> impl Iterator<Item = &<T::DB as DatabaseLike>::ForeignKey> {
        self.foreign_keys.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the Rc of foreign keys of the table.
    #[inline]
    pub fn foreign_key_rcs(
        &self,
    ) -> impl Iterator<Item = &Rc<<T::DB as DatabaseLike>::ForeignKey>> {
        self.foreign_keys.iter()
    }

    /// Returns an iterator over the columns composing the primary key of the
    /// table.
    #[inline]
    pub fn primary_key_columns(&self) -> impl Iterator<Item = &<T::DB as DatabaseLike>::Column> {
        self.primary_key.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns the documentation, if exists, for the table
    #[inline]
    pub fn table_doc(&self) -> Option<&<T as DocumentationMetadata>::Documentation> {
        self.documentation.as_ref()
    }

    /// Updates the `documentation` field
    #[inline]
    pub fn set_doc(&mut self, s: <T as DocumentationMetadata>::Documentation) {
        self.documentation = Some(s);
    }

    /// Adds a column to the table metadata.
    ///
    /// # Arguments
    ///
    /// * `column` - The column to add.
    #[inline]
    pub fn add_column(&mut self, column: Rc<<T::DB as DatabaseLike>::Column>) {
        self.columns.push(column);
    }

    /// Adds a check constraint to the table metadata.
    ///
    /// # Arguments
    ///
    /// * `constraint` - The check constraint to add.
    #[inline]
    pub fn add_check_constraint(
        &mut self,
        constraint: Rc<<T::DB as DatabaseLike>::CheckConstraint>,
    ) {
        self.check_constraints.push(constraint);
    }

    /// Adds an index to the table metadata.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to add.
    #[inline]
    pub fn add_index(&mut self, index: Rc<<T::DB as DatabaseLike>::Index>) {
        self.indices.push(index);
    }

    /// Adds a unique index to the table metadata.
    ///
    /// # Arguments
    ///
    /// * `index` - The unique index to add.
    #[inline]
    pub fn add_unique_index(&mut self, index: Rc<<T::DB as DatabaseLike>::UniqueIndex>) {
        self.unique_indices.push(index);
    }

    /// Adds a foreign key to the table metadata.
    ///
    /// # Arguments
    ///
    /// * `fk` - The foreign key to add.
    #[inline]
    pub fn add_foreign_key(&mut self, fk: Rc<<T::DB as DatabaseLike>::ForeignKey>) {
        self.foreign_keys.push(fk);
    }

    /// Sets the columns composing the primary key of the table.
    ///
    /// # Arguments
    ///
    /// * `pk_columns` - The columns composing the primary key.
    pub fn set_primary_key(&mut self, pk_columns: Vec<Rc<<T::DB as DatabaseLike>::Column>>) {
        self.primary_key = pk_columns;
    }
}
