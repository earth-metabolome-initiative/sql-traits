//! Per-query resolution of column references and definitions.

use core::fmt;

use sqlparser::ast::{Expr, Query, Select, SetOperator};

use crate::{
    errors::LookupError,
    impls::dql::{
        build_definition_graph,
        definition_graph::{DefinitionGraph, DefinitionId, ScopeCursor, table_graph},
    },
    traits::DatabaseLike,
};

/// The definition that determines one resolved column's declared type.
#[derive(Debug)]
pub enum ColumnDefinition<'scope, 'query, 'db, DB: DatabaseLike> {
    /// A stored table column and its table.
    Base {
        /// The table declaring the column.
        table: &'scope DB::Table,
        /// The declared column.
        column: &'scope DB::Column,
    },
    /// A projection expression and its defining scope.
    Expression {
        /// The expression defining the column.
        expression: &'scope Expr,
        /// The scope in which the expression was declared.
        scope: ColumnDefinitionScope<'scope, 'query, 'db, DB>,
    },
    /// The definitions combined by an ordinary set operation.
    SetOperation {
        /// The operation combining the definitions.
        operator: SetOperator,
        /// The left definition.
        left: ColumnDefinitionRef<'scope, 'query, 'db, DB>,
        /// The right definition.
        right: ColumnDefinitionRef<'scope, 'query, 'db, DB>,
    },
    /// The anchor and recursive definitions of a recursive union.
    RecursiveUnion {
        /// The nonrecursive anchor definition.
        anchor: ColumnDefinitionRef<'scope, 'query, 'db, DB>,
        /// The recursive definition.
        recursive: ColumnDefinitionRef<'scope, 'query, 'db, DB>,
    },
    /// A relation exposes the name without an inspectable definition.
    Opaque,
}

/// A borrowed handle to one immutable definition node.
///
/// ```compile_fail
/// use sql_traits::{prelude::ParserDB, structs::ColumnDefinitionRef};
/// let _ = ColumnDefinitionRef::<ParserDB> {};
/// ```
pub struct ColumnDefinitionRef<'scope, 'query, 'db, DB: DatabaseLike> {
    graph: &'scope DefinitionGraph<'query, 'db, DB>,
    id: DefinitionId,
}

impl<'scope, 'query, 'db, DB: DatabaseLike> ColumnDefinitionRef<'scope, 'query, 'db, DB> {
    pub(crate) fn new(graph: &'scope DefinitionGraph<'query, 'db, DB>, id: DefinitionId) -> Self {
        Self { graph, id }
    }

    /// Returns this node's definition view.
    #[must_use]
    pub fn definition(self) -> ColumnDefinition<'scope, 'query, 'db, DB> {
        self.graph.definition(self.id)
    }
}

impl<DB: DatabaseLike> Copy for ColumnDefinitionRef<'_, '_, '_, DB> {}

#[expect(clippy::expl_impl_clone_on_copy, reason = "derive would require DB: Clone")]
impl<DB: DatabaseLike> Clone for ColumnDefinitionRef<'_, '_, '_, DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: DatabaseLike> fmt::Debug for ColumnDefinitionRef<'_, '_, '_, DB> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ColumnDefinitionRef").field(&self.id).finish()
    }
}

/// A borrowed resolver for one definition-local scope.
pub struct ColumnDefinitionScope<'scope, 'query, 'db, DB: DatabaseLike> {
    graph: &'scope DefinitionGraph<'query, 'db, DB>,
    cursor: ScopeCursor,
}

impl<'scope, 'query, 'db, DB: DatabaseLike> ColumnDefinitionScope<'scope, 'query, 'db, DB> {
    pub(crate) fn new(
        graph: &'scope DefinitionGraph<'query, 'db, DB>,
        cursor: ScopeCursor,
    ) -> Self {
        Self { graph, cursor }
    }

    /// Resolves a column through this scope and its enclosing scopes.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] for an ambiguous reference
    /// and relation-name lookup errors from modeled definitions.
    pub fn resolve_column_definition(
        &self,
        reference: &Expr,
    ) -> Result<Option<ColumnDefinition<'scope, 'query, 'db, DB>>, LookupError> {
        self.graph.resolve_definition(self.cursor, reference)
    }

    /// Returns the recorded scope for this exact nested `Select`.
    #[must_use]
    pub fn scope_for_select(
        &self,
        select: &Select,
    ) -> Option<ColumnDefinitionScope<'scope, 'query, 'db, DB>> {
        self.graph.scope_for_select(self.cursor, select)
    }
}

impl<DB: DatabaseLike> Copy for ColumnDefinitionScope<'_, '_, '_, DB> {}

#[expect(clippy::expl_impl_clone_on_copy, reason = "derive would require DB: Clone")]
impl<DB: DatabaseLike> Clone for ColumnDefinitionScope<'_, '_, '_, DB> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<DB: DatabaseLike> fmt::Debug for ColumnDefinitionScope<'_, '_, '_, DB> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ColumnDefinitionScope").field(&self.cursor).finish()
    }
}

/// The relations available to one query projection or table definition.
pub struct ColumnScope<'query, 'db, DB: DatabaseLike> {
    graph: DefinitionGraph<'query, 'db, DB>,
    root: ScopeCursor,
}

impl<'query, 'db, DB: DatabaseLike> ColumnScope<'query, 'db, DB> {
    /// Builds the column scope of a query's outer body.
    ///
    /// # Errors
    ///
    /// Returns relation-name lookup errors and
    /// [`LookupError::AmbiguousTableLookup`] when a modeled relation lookup is
    /// ambiguous.
    pub fn from_query(query: &'query Query, database: &'db DB) -> Result<Self, LookupError> {
        let (graph, root) = build_definition_graph(query, database)?;
        Ok(Self { graph, root })
    }

    /// Resolves one column to its source table without following definition
    /// parents.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when more than one local
    /// relation exposes the reference.
    pub fn resolve_column(&self, reference: &Expr) -> Result<Option<&'db DB::Table>, LookupError> {
        self.graph.resolve_source(self.root, reference)
    }

    /// Resolves one column to the definition that determines its type.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::AmbiguousTableLookup`] when more than one
    /// relation exposes the reference and relation-name lookup errors from
    /// modeled definitions.
    pub fn resolve_column_definition(
        &self,
        reference: &Expr,
    ) -> Result<Option<ColumnDefinition<'_, 'query, 'db, DB>>, LookupError> {
        self.graph.resolve_definition(self.root, reference)
    }
}

impl<'db, DB: DatabaseLike> ColumnScope<'db, 'db, DB> {
    /// Builds the definition scope for one stored table.
    #[must_use]
    pub fn for_table(table: &'db DB::Table, database: &'db DB) -> Self {
        let (graph, root) = table_graph(table, database);
        Self { graph, root }
    }
}
