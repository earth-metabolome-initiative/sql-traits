//! Submodule defining a generic `PolicyMetadata` struct.

use std::rc::Rc;

use crate::traits::{DatabaseLike, PolicyLike};

#[derive(Debug, Clone)]
/// Struct collecting metadata about a policy.
pub struct PolicyMetadata<U: PolicyLike> {
    /// The functions involved in the using expression.
    using_functions: Vec<Rc<<U::DB as DatabaseLike>::Function>>,
    /// The functions involved in the check expression.
    check_functions: Vec<Rc<<U::DB as DatabaseLike>::Function>>,
}

impl<U: PolicyLike> PolicyMetadata<U> {
    /// Creates a new `PolicyMetadata` instance.
    #[inline]
    #[must_use]
    pub fn new(
        using_functions: Vec<Rc<<U::DB as DatabaseLike>::Function>>,
        check_functions: Vec<Rc<<U::DB as DatabaseLike>::Function>>,
    ) -> Self {
        Self { using_functions, check_functions }
    }

    /// Returns an iterator over the functions involved in the using expression.
    #[inline]
    pub fn using_functions(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Function> {
        self.using_functions.iter().map(std::convert::AsRef::as_ref)
    }

    /// Returns an iterator over the functions involved in the check expression.
    #[inline]
    pub fn check_functions(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Function> {
        self.check_functions.iter().map(std::convert::AsRef::as_ref)
    }
}
