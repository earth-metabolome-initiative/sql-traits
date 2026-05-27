# Data-statement analysis for sql-traits

Status: proposal / design note. Implementation TODO.

## Motivation

Downstream crates need to classify a parsed SQL statement *against a schema*, not just parse it:

- **subql** (CDC subscription engine) re-executes queries it cannot evaluate in-process. To do that it must know (a) which base tables a query references, so a change to any of them triggers re-execution (routing), and (b) whether the query's projection comes from a single base table with a primary key, which is the eligibility rule for "single-table row re-execution" - a join/subquery sits in the filter, but the output is rows of one base table, delivered downstream as a PK-keyed patchset.
- **connetto-rs** (backend/frontend sync) intercepts client mutations and must resolve the **target table** of an INSERT/UPDATE/DELETE to route and apply them.

Both must classify a statement *identically* (client and server), using the same identifier/quoting semantics. That resolution against a `DatabaseLike` is sql-traits' existing job (it already resolves DDL references - foreign keys, grants, policies, triggers - and owns the canonical identifier matching). sql-traits has no statement/query analysis yet; this note specifies adding it.

These are schema-relative **structural** facts only. Query *semantics* stay with the consumer: subql keeps WHERE->bytecode compilation, aggregate specs, its error type, and the mapping of `&Table`/`&Column` to its compact id types; connetto applies authorization and builds patchsets.

## Terminology and taxonomy

SQL statements fall into the standard ISO classes: **SQL-schema statements** (DDL: `CREATE`/`ALTER`/`DROP`, incl. `CREATE ROLE`), **SQL-data statements** (`SELECT`, `INSERT`, `UPDATE`, `DELETE`), **SQL-transaction statements** (`COMMIT`/`ROLLBACK`), DCL (`GRANT`/`REVOKE`), etc. There is no acronym "above" DML/DQL - the umbrella is just "statement". The narrower umbrella that excludes `CREATE ROLE`, `COMMIT`, ... and covers exactly the statements that reference table data is a **data statement** (ISO "SQL-data statement").

sql-traits already embodies two classes: **DDL** (the `TableLike`/`ColumnLike`/... object traits derived from `CREATE`) and **DCL** (grants/policies). This note adds the **data-statement** class as a small trait hierarchy:

```
DataStatementLike      (umbrella: references tables)   <- SQL-data statements
  ├── DQLLike          (adds projection analysis)      <- SELECT
  └── DMLLike          (adds target table + kind)      <- INSERT / UPDATE / DELETE
```

`CREATE ROLE` and friends are SQL-schema statements and correctly fall outside `DataStatementLike`.

## Design constraints (match the crate's idioms)

- **Trait-on-AST-node**, like the DDL impls (`ColumnLike for ColumnDef`, etc.). The traits are implemented on sqlparser AST nodes (`Query`, and the Insert/Update/Delete nodes).
- **Generic over the DB**, not an associated type. A statement is analyzed *against* a database passed in, and the same statement may be analyzed against different DBs - so the DB is a trait type parameter (`DataStatementLike<DB>`), unlike the DDL object traits which carry `type DB` because an object belongs to one schema.
- **The public surface speaks only resolved schema types** (`&DB::Table`, `&DB::Column`, small enums). sqlparser's `ObjectName`/`TableFactor`/aliases never appear in the public API - exactly as `TableLike` never exposes `CreateTable`. `ObjectName` is too generic (it names tables, functions, types, roles); a table-reference-in-a-query must be a resolved concept.
- **`Result<_, LookupError>`**, no panics in public APIs (some existing DDL impls panic - do not follow that here).
- no_std + alloc, edition 2024, `forbid(missing_docs)`, `deny(clippy::pedantic)`.

## Proposed public API

```rust
// src/traits/data_statement.rs
pub trait DataStatementLike<DB: DatabaseLike> {
    /// Every base table referenced anywhere in the statement - FROM, JOINs,
    /// subqueries in WHERE/HAVING, CTE bodies, set operations, and (for DML)
    /// the mutation target plus its subqueries - resolved against `db` and
    /// deduplicated. Relations that do not resolve to a base table (CTE names,
    /// table functions) are skipped, not errored.
    fn referenced_tables<'db>(&self, db: &'db DB)
        -> Result<Vec<&'db DB::Table>, LookupError>;
}

// src/traits/dql.rs
pub trait DQLLike<DB: DatabaseLike>: DataStatementLike<DB> {
    /// The single base table all projected columns come from, when there is
    /// exactly one:
    ///   - qualified columns (`t.c`), or `t.*`, all bound to the same table, or
    ///   - unqualified columns over a single-table FROM (no joins).
    /// `Ok(None)` if the projection draws from more than one table, is `*` over
    /// a join, or is an aggregate / has GROUP BY (no per-row base-table identity).
    /// `Err(AmbiguousTableLookup)` for an unqualified column that several FROM
    /// tables expose.
    fn projection_source_table<'db>(&self, db: &'db DB)
        -> Result<Option<&'db DB::Table>, LookupError>;
}

// src/traits/dml.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DmlKind { Insert, Update, Delete }

pub trait DMLLike<DB: DatabaseLike>: DataStatementLike<DB> {
    /// The table this statement mutates (`INSERT INTO t`, `UPDATE t`,
    /// `DELETE FROM t`), resolved against `db`.
    fn target_table<'db>(&self, db: &'db DB) -> Result<&'db DB::Table, LookupError>;
    /// Which mutation this is.
    fn kind(&self) -> DmlKind;
}
```

Implementations:

```rust
impl<DB: DatabaseLike> DataStatementLike<DB> for sqlparser::ast::Query { ... }
impl<DB: DatabaseLike> DQLLike<DB>          for sqlparser::ast::Query { ... }
// DataStatementLike + DMLLike for the Insert / Update / Delete AST nodes
// (sqlparser models these as Statement variants; impl on the inner structs,
//  or on a thin newtype, so callers go Statement -> node -> trait).
```

Callers get the node from a parsed `Statement` (`if let Statement::Query(q) = &stmt`). Primary-key access uses existing `TableLike`: `table.has_primary_key(db)`, `table.primary_key_columns(db)`.

Notes:
- `DMLLike::target_table` is what connetto's mutation interception needs. DML's row/PK *interest* registration (the rows a write touches) is a **runtime** concern - identities come from execution results (RETURNING / affected rows / generated keys), not from parsing - so `DMLLike` stays minimal (target + kind) with no predicate analysis.
- For a `Query` whose body is a set operation (UNION/...), `projection_source_table` returns `Ok(None)` (no single projection).

## Internal-only support (not public)

- Promote the private `ObjectName` helpers in `src/impls/sqlparser/grant.rs` (`object_name_last_part`, `schema_from_object_name`, `table_matches_object_name`) into a `pub(crate)` `utils::object_name` module, and add the canonical resolver:

  ```rust
  pub(crate) fn resolve_object_name<'db, DB: DatabaseLike>(
      obj: &ObjectName, db: &'db DB,
  ) -> Result<Option<&'db DB::Table>, LookupError>; // None = no match; Err = ambiguous
  ```

  Reuse it from the new impls (and optionally refactor grant/FK/trigger/policy onto it to cut duplication and fix the FK path's lack of identifier normalization). These stay crate-internal - `ObjectName` is not public DQL vocabulary.

- The DQL impl builds a FROM/JOIN alias -> resolved-table map (handling aliases and self-joins), then resolves each projection item to its owning table. `referenced_tables` uses sqlparser's `visit_relations`, which recurses subqueries, CTE bodies, and set operations, so the manual-walk pitfalls are handled; CTE *names* simply fail resolution and are skipped while the base tables in CTE bodies resolve.

## Reuse (existing sql-traits, do not reinvent)

- `DatabaseLike::table(schema: Option<&str>, name: &str) -> Option<&Table>`, `tables()`, `table_by_id` (src/traits/database.rs).
- `TableLike::{column, columns, primary_key_columns, has_primary_key, table_name/table_schema (+ _is_quoted)}` (src/traits/table.rs).
- `ColumnLike::{table, column_name (+ _is_quoted), data_type}` (src/traits/column.rs).
- `utils::identifier_resolution::{identifiers_match, parse_lookup_identifier, normalize_identifier}` - PostgreSQL quoting/case folding + NFC (src/utils/identifier_resolution.rs).
- `utils::columns_in_expression` - resolves column references in an `Expr` (src/utils/columns_in_expression.rs); useful for the projection walk.
- `errors::LookupError` (`AmbiguousTableLookup`, ...) (src/errors.rs).
- The `ObjectName` matching pattern in src/impls/sqlparser/grant.rs.

## Cargo / dependency change

Enable sqlparser's `visitor` feature (for `visit_relations`):

```toml
sqlparser = { version = "0.62", default-features = false, features = ["visitor"] }
```

Keep the version/fork rev aligned with the downstream consumers (subql pins a sqlparser fork rev).

## Files to add / modify

- NEW: `src/traits/data_statement.rs`, `src/traits/dql.rs`, `src/traits/dml.rs`; `src/impls/sqlparser/{data_statement,dql,dml}.rs`; `src/utils/object_name.rs`.
- MODIFY: `src/traits/mod.rs` + `prelude` (export the three traits + `DmlKind`); `src/utils/mod.rs` (export `object_name`); `src/impls/sqlparser/grant.rs` (use promoted helpers); `Cargo.toml` (visitor feature).
- NEW tests: `tests/data_statement_analysis.rs` against `ParserDB`.

## Edge cases to handle

- Qualified vs unqualified projection columns; `t.*`; `SELECT *` over a join (-> `None`); aggregate / GROUP BY projection (-> `None`).
- Join fan-out and self-joins: `projection_source_table` only needs the owning table; deduplicate by table.
- Subqueries in WHERE/HAVING/FROM: their base tables count toward `referenced_tables`; they do not change `projection_source_table` (which looks at the outer projection).
- CTEs: a reference to a CTE name does not resolve to a base table (skipped); base tables inside CTE bodies do resolve.
- Set operations: `referenced_tables` unions both sides; `projection_source_table` -> `None`.
- Quoted/case-folded identifiers via `identifier_resolution`.
- Ambiguous unqualified column over a multi-table FROM -> `Err(AmbiguousTableLookup)`.
- Schema-qualified table names (`schema.table`) resolved via `DatabaseLike::table(Some(schema), name)`.

## Test checklist (against ParserDB)

- single-table `SELECT` -> `projection_source_table == Some(t)`.
- JOIN, single-table qualified projection (`o.*`, `o.id, o.total`) -> `Some(orders)`.
- JOIN, multi-table projection (`o.id, u.name`) and `SELECT *` over a join -> `None`.
- aggregate / `GROUP BY` -> `None`.
- subquery in WHERE -> `referenced_tables` includes both; projection still single-table.
- CTE -> CTE name skipped, body base tables present in `referenced_tables`.
- set operation -> projection `None`, `referenced_tables` unions.
- ambiguous unqualified column -> `Err`.
- quoted / case-folded identifiers resolve correctly.
- DML: `INSERT INTO t ...`, `UPDATE t ... WHERE ...`, `DELETE FROM t ...` -> `target_table == t`, `kind` matches; `referenced_tables` includes `t` plus any subquery tables.
- `cargo test`, `cargo clippy` (pedantic gate), and the no_std build all green.

## Downstream (out of scope here)

subql will consume `referenced_tables` (routing) and `projection_source_table` (eligibility) in its re-execution layer, and delete its earlier arbitrary-result-set framing (the `RowSetSource` sketch and "Total = row set" doc) in favor of single-table row re-execution + aggregate re-execution. connetto will consume `DMLLike::target_table` for mutation interception and apply auth + build `sqlite-diff-rs` patchsets.
