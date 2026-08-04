//! Submodule defining a generic `PolicyMetadata` struct.

use alloc::{sync::Arc, vec::Vec};

use crate::traits::{DatabaseLike, PolicyLike};

#[derive(Debug, Clone)]
/// Struct collecting metadata about a policy.
pub struct PolicyMetadata<U: PolicyLike> {
    /// The functions involved in the using expression.
    using_functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
    /// The functions involved in the check expression.
    check_functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
}

impl<U: PolicyLike> PolicyMetadata<U> {
    /// Creates a new `PolicyMetadata` instance.
    #[inline]
    #[must_use]
    pub fn new(
        using_functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
        check_functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>,
    ) -> Self {
        Self { using_functions, check_functions }
    }

    /// Returns an iterator over the functions involved in the using expression.
    #[inline]
    pub fn using_functions(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Function> {
        self.using_functions.iter().map(core::convert::AsRef::as_ref)
    }

    /// Replaces the functions involved in the using expression.
    ///
    /// # Arguments
    ///
    /// * `functions` - The functions the current using expression calls.
    #[inline]
    pub fn set_using_functions(&mut self, functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>) {
        self.using_functions = functions;
    }

    /// Returns an iterator over the functions involved in the check expression.
    #[inline]
    pub fn check_functions(&self) -> impl Iterator<Item = &<U::DB as DatabaseLike>::Function> {
        self.check_functions.iter().map(core::convert::AsRef::as_ref)
    }

    /// Replaces the functions involved in the check expression.
    ///
    /// # Arguments
    ///
    /// * `functions` - The functions the current check expression calls.
    #[inline]
    pub fn set_check_functions(&mut self, functions: Vec<Arc<<U::DB as DatabaseLike>::Function>>) {
        self.check_functions = functions;
    }
}
