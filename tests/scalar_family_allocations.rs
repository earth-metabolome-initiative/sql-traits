//! Pins scalar family allocation behavior.

use core::{
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
};
use std::alloc::{GlobalAlloc, Layout, System};

use sql_traits::utils::scalar_family::{ScalarFamily, scalar_family};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/scalar_family_cases.rs"));

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: The wrapper preserves `System` allocation contracts and adds atomic
// accounting only.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller provides the allocation contract required by
        // `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller provides the allocation contract required by
        // `System`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The caller provides the pointer and layout returned by this
        // allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: The caller provides the reallocation contract required by
        // `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn scalar_family_classification_does_not_allocate() {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    for (declared_type, expected) in all_scalar_family_cases() {
        let actual = black_box(scalar_family(black_box(declared_type)));
        assert_eq!(actual, black_box(expected));
    }
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(after, before);
}
