//! Submodule providing traits for describing SQL-like entities.

pub mod column;
pub mod database;
pub mod table;
use std::fmt::Debug;

pub use column::ColumnLike;
pub mod index;
pub use database::DatabaseLike;
pub use index::IndexLike;
pub use table::TableLike;
pub mod check_constraint;
pub use check_constraint::CheckConstraintLike;
pub mod unique_index;
pub use unique_index::UniqueIndexLike;
pub mod foreign_key;
pub use foreign_key::ForeignKeyLike;
pub mod function_like;
pub use function_like::FunctionLike;
pub mod trigger;
pub use trigger::TriggerLike;
pub mod policy;
pub use policy::PolicyLike;
pub mod role;
pub use role::RoleLike;
pub mod schema;
pub use schema::SchemaLike;
pub mod grant;
pub use grant::{ColumnGrantLike, GrantLike, TableGrantLike};

/// Trait for associating a metadata struct to a given type.
pub trait Metadata {
    /// The associated metadata type.
    type Meta: Clone + Debug;
}

impl<M: Metadata> Metadata for &M {
    type Meta = M::Meta;
}

impl<M: Metadata> Metadata for std::rc::Rc<M> {
    type Meta = M::Meta;
}

/// Trait for associating documentation struct with a given type
pub trait DocumentationMetadata {
    /// The associated documentation type
    type Documentation: Clone + Debug;
}

impl<D: DocumentationMetadata> DocumentationMetadata for &D {
    type Documentation = D::Documentation;
}
