//! Pins scalar family allocation behavior outside coverage instrumentation.
#![cfg(not(tarpaulin))]

use core::{cell::Cell, hint::black_box, sync::atomic::Ordering};
use std::alloc::{GlobalAlloc, Layout, System};

use sql_traits::utils::scalar_family::{ScalarFamily, scalar_family};

include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/scalar_family_cases.rs"));

struct CountingAllocator;

thread_local! {
    /// Allocations the current thread asked for.
    ///
    /// The `const` initializer keeps the slot from allocating when it is first
    /// touched, which would recurse through this allocator.
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

/// Records one allocation for the current thread, ignoring a slot already
/// destroyed during thread teardown.
fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|allocations| allocations.set(allocations.get() + 1));
}

/// Allocations the current thread has asked for so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

// SAFETY: The wrapper preserves `System` allocation contracts and adds
// per-thread accounting only.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller provides the allocation contract required by
        // `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
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
        record_allocation();
        // SAFETY: The caller provides the reallocation contract required by
        // `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn scalar_family_classification_does_not_allocate() {
    let before = allocations();
    for (declared_type, expected) in all_scalar_family_cases() {
        let actual = black_box(scalar_family(black_box(declared_type)));
        assert_eq!(actual, black_box(expected));
    }
    let after = allocations();
    assert_eq!(after, before);
}

/// The measurement must see this thread's allocations and no others.
///
/// The allocator is process-wide, so a shared counter also sees the test
/// harness and any other thread allocating while the window is open, which is
/// what failed this file in CI over four allocations nothing here made.
#[test]
fn allocations_on_another_thread_are_not_counted() {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize},
        },
        thread,
    };

    let helper_allocations = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let counted = Arc::clone(&helper_allocations);
    let halt = Arc::clone(&stop);
    let helper = thread::spawn(move || {
        while !halt.load(Ordering::Relaxed) {
            drop(black_box(Vec::<u8>::with_capacity(64)));
            counted.fetch_add(1, Ordering::Relaxed);
        }
    });

    let start = helper_allocations.load(Ordering::Relaxed);
    let before = allocations();
    while helper_allocations.load(Ordering::Relaxed) < start + 1_000 {
        core::hint::spin_loop();
    }
    let after = allocations();

    stop.store(true, Ordering::Relaxed);
    assert!(helper.join().is_ok(), "the helper thread panicked while allocating");

    assert_eq!(after, before, "another thread's allocations were counted");
}

/// The positive control: the counter must see this thread's own allocations,
/// or a counter stuck at zero would satisfy every other test here.
#[test]
fn allocations_on_this_thread_are_counted() {
    let before = allocations();
    drop(black_box(Vec::<u8>::with_capacity(64)));
    let after = allocations();

    assert!(after > before, "this thread's own allocation went uncounted");
}
