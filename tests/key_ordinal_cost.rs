//! What the key ordinal answer costs as the key gets wider.
//!
//! `primary_key_column_ids` composes the key iterator with the column
//! iterator, and the composition can walk the columns once for the whole key
//! or once per key column. Both answer the same ordinals, so this file counts
//! allocations instead: past width one, where the key needs no storage of its
//! own, a wider key must not cost more.

#![allow(clippy::expect_used)]

use core::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
};
use std::alloc::System;

use sql_traits::{errors::Error, prelude::*};
use sqlparser::dialect::PostgreSqlDialect;

thread_local! {
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

/// Counts the allocations of the thread that opened the measurement, so a test
/// counts its own work and not that of the tests running beside it.
struct CountingAllocator;

// SAFETY: every request is forwarded to `System` with the layout it arrived
// with, and the counters are thread-local `Cell`s of `Copy` types, so the impl
// adds no shared state and keeps `System`'s own guarantees.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.get() {
            ALLOCATIONS.set(ALLOCATIONS.get() + 1);
        }
        // SAFETY: `layout` is the caller's, forwarded unchanged.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from `System.alloc` with this same `layout`,
        // since every allocation here is forwarded there.
        unsafe { System.dealloc(pointer, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `body` and answers how many times it allocated.
fn allocations<T>(body: impl FnOnce() -> T) -> usize {
    ALLOCATIONS.set(0);
    MEASURING.set(true);
    let answer = body();
    MEASURING.set(false);
    drop(answer);
    ALLOCATIONS.get()
}

#[test]
fn the_key_ordinal_cost_does_not_grow_with_the_key_width() -> Result<(), Error> {
    let db = ParserDB::parse::<PostgreSqlDialect>(
        "
        CREATE TABLE width_one (a INT, b INT, c INT, d INT, PRIMARY KEY (b));
        CREATE TABLE width_two (a INT, b INT, c INT, d INT, PRIMARY KEY (c, a));
        CREATE TABLE width_three (a INT, b INT, c INT, d INT, PRIMARY KEY (c, a, b));
        CREATE TABLE width_four (a INT, b INT, c INT, d INT, PRIMARY KEY (c, a, d, b));
        ",
    )?;
    let width_one = db.table(None, "width_one").expect("table width_one was created");
    let width_two = db.table(None, "width_two").expect("table width_two was created");
    let width_three = db.table(None, "width_three").expect("table width_three was created");
    let width_four = db.table(None, "width_four").expect("table width_four was created");

    // A cheaper wrong answer cannot pass this file.
    assert_eq!(width_one.primary_key_column_ids(&db)?, vec![1]);
    assert_eq!(width_two.primary_key_column_ids(&db)?, vec![2, 0]);
    assert_eq!(width_three.primary_key_column_ids(&db)?, vec![2, 0, 1]);
    assert_eq!(width_four.primary_key_column_ids(&db)?, vec![2, 0, 3, 1]);

    let one = allocations(|| width_one.primary_key_column_ids(&db));
    let two = allocations(|| width_two.primary_key_column_ids(&db));
    let three = allocations(|| width_three.primary_key_column_ids(&db));
    let four = allocations(|| width_four.primary_key_column_ids(&db));

    assert_eq!(
        three, two,
        "a key of width three costs {three} allocations against {two} for width two"
    );
    assert_eq!(
        four, two,
        "a key of width four costs {four} allocations against {two} for width two"
    );
    assert!(
        one <= two,
        "a key of one column costs {one} allocations, more than the {two} a wider key costs"
    );

    Ok(())
}
