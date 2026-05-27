# Data-statement analysis: design review and refinements

Status: review of `data-statement-analysis.md`, grounded against the current code and the sqlparser 0.62 AST. Read this alongside the original note. Where the two disagree, this document wins.

## What was verified against the code

- `DatabaseLike` exposes `table(schema, name)`, `table_id(&Table)`, `table_by_id(id)`, `tables()` (src/traits/database.rs). `table_id` gives a stable index usable for deduplication.
- `TableLike` exposes `table_name`, `table_name_is_quoted`, `table_schema`, `table_schema_is_quoted`, `columns`, `column(name, db)`, `primary_key_columns`, `has_primary_key` (src/traits/table.rs). Crucially, `column(name, db)` already applies PostgreSQL identifier resolution through `stored_identifier_matches_lookup`, so it is the correct primitive for "does this table expose a column named C".
- `ColumnLike` exposes `column_name`, `column_name_is_quoted`, `table(db)`, `data_type` (src/traits/column.rs).
- `LookupError::{InvalidObjectName, AmbiguousTableLookup}` exist (src/errors.rs) and carry rendered names plus candidate lists.
- A resolver already exists: `ParserDB::resolve_table_object_name` (src/structs/generic_db/sqlparser.rs), built on `resolve_table_object_name_in_iter` and `resolve_table_from_candidates`. Those internals use only `TableLike` methods plus `identifiers_match`, so generalizing to a `pub(crate)` generic over `DB: DatabaseLike` is mechanical. It already returns `Ok(None)` for no match and `Err(AmbiguousTableLookup)` for multiple matches, exactly the resolver contract the note proposes.
- sqlparser `visit_relations` exists behind the `visitor` feature (src/ast/visitor.rs). The `visit_relation` annotation is present on `TableFactor::Table.name`, on the INSERT target `TableObject::TableName`, and on the relations reachable from `Update.table: TableWithJoins` and `Delete.from: FromTable`. It is NOT present on bare `Vec<ObjectName>` fields such as the MySQL multi-table `Delete.tables`.

## Corrections to the original note

1. **`Statement::Update` is a real struct.** The note worried that UPDATE is an inline `Statement` variant with no nameable type, forcing a newtype. In sqlparser 0.62 it is `Statement::Update(Update)` with `Update` defined in `src/ast/dml.rs`, exactly like `Insert` and `Delete`. So `DataStatementLike` and `DMLLike` are implemented directly on `Query`, `Insert`, `Update`, and `Delete`. No newtype, no `impl on Statement` compromise. Callers still go `Statement -> &node -> trait`.

2. **`utils::columns_in_expression` is the wrong tool for the projection walk.** It is single-table (takes one `table_name` plus that table's `columns`), it matches with `col.column_name() == ident.value` (exact byte match, no quoting or case folding), and it returns `Err` on an unknown column. The projection binder needs multi-table binding, identifier resolution, and "no match" as a normal (non-error) outcome. The projection binder should instead use `TableLike::column(name, db)` against each FROM table. Do not reuse `columns_in_expression` here.

3. **The resolver is not new work.** The note frames `resolve_object_name` as new. It is a generalization of existing private code. The task is to lift `resolve_table_object_name_in_iter` and friends into `utils::object_name` as generics over `DB::Table: TableLike`, then route the existing `ParserDB` methods and the grant/FK/trigger/policy call sites through them. This matches the decision to also refactor existing callers.

## Settled decisions

These three changed the public surface or the behavior under failure, so they were settled before implementation. The chosen option is recorded under each.

### D1. How is the `visitor` capability exposed?

`referenced_tables` is built on `visit_relations`, which requires sqlparser's `visitor` feature (it derives `Visit` on the AST via the `sqlparser_derive` proc-macro). Options:

- **Enable `visitor` unconditionally** (add it to the sqlparser dependency features). The data-statement API is then always present. Cost: the `sqlparser_derive` proc-macro is always compiled. It is `no_std`-compatible (the generated code uses `core::ops::ControlFlow`).
- **Gate the whole data-statement module behind a new crate feature** (for example `data-statement`), which in turn enables `sqlparser/visitor`. Keeps the proc-macro and the new surface out of minimal builds, consistent with the recent move to make `git` opt-in.

**Decision: enable `visitor` unconditionally.** Both downstream consumers (subql, connetto) always need this surface, and a feature gate that everyone turns on is just friction. The new modules are always compiled and the new traits are always in the prelude.

**Implementation caveat (discovered during Step 2):** the published `sqlparser_derive 0.5.0` (what `sqlparser 0.62` pulls from crates.io) emits `::std::ops::ControlFlow` in the generated `Visit`/`VisitMut` impls, so enabling `visitor` breaks every `no_std` build (3201 errors inside sqlparser). This was fixed upstream in apache/datafusion-sqlparser-rs commit `e999d3d` ("Make `visitor` feature `no_std`-compatible", #2343), which emits `::core::ops::ControlFlow`, but that fix is not yet released to crates.io. The crate therefore consumes sqlparser from git via a workspace-root `[patch.crates-io]` entry, which unifies every dependent (notably `sql_docs`, whose `Dialect` type must stay the same crate as ours) onto the single patched sqlparser. With the patch in place, `visitor` compiles on `no_std`, including the `thumbv7em-none-eabihf` and `wasm32-unknown-unknown` cross-compile targets. The patch can be dropped once a crates.io sqlparser release ships the fixed derive.

### D2. Failure mode of `referenced_tables` (ambiguity and CTE-name collisions)

`visit_relations` yields every relation name in the subtree but cannot tell a base-table reference from a CTE-name reference. Both are a bare `ObjectName`. Two sub-cases:

- A visited name matches more than one base table (for example unqualified `t` present in two schemas). The resolver returns `Err(AmbiguousTableLookup)`.
- A visited name is actually a CTE reference that happens to match a base table name (CTE shadowing). Naive resolution treats it as the base table.

The consumer is CDC routing (subql), where the safe failure direction is over-inclusion (a spurious re-execution trigger is harmless, a missing one serves stale data). That argues for: resolve every visited relation against the schema, include every `Ok(Some)`, skip every `Ok(None)`, and for ambiguity either include all candidates or propagate the error. It argues against trying to subtract CTE names, because a buggy subtraction could drop a real base table.

**Decision: `referenced_tables` propagates `Err(AmbiguousTableLookup)` on a multi-candidate match.** It resolves every visited relation, includes each unique `Ok(Some)` match (deduplicated by `DatabaseLike::table_id`), skips each `Ok(None)`, and returns the resolver error when a name is ambiguous. This keeps the resolver contract uniform across the crate, at the cost of making the routing path fallible on legal ambiguous SQL (the consumer must surface or handle that error). CTE names are still NOT special-cased: a CTE reference that does not match a base table resolves to `Ok(None)` and is skipped, while a CTE reference that collides with exactly one base table name resolves to that table and is included (safe over-inclusion, and not distinguishable from a real reference without full lexical scoping). This is the one place the spec deviates from the note's "CTE names are skipped" framing.

### D3. Strictness of `projection_source_table`

This drives single-table row re-execution eligibility, where the safe failure direction is the opposite of D2: under-claiming eligibility is safe (fall back to full re-execution), over-claiming is a correctness bug (subql would deliver a PK-keyed patchset for something that is not rows of one base table). Two consequences:

- **CTE and derived relations must be treated as non-base.** When building the FROM alias map for the outer SELECT, a `TableFactor::Table` whose name matches an outer-query CTE name (from `Query.with`), or any non-`Table` factor (derived subquery, table function, `TableFactor::Table` with `args` set), maps to "not a base table". If the projection draws from such a relation, the result is `Ok(None)`. Only the outer query's CTE names are needed here, since projection only inspects the outer FROM, so this is a small, tractable scoping pass.
- **Only pass-through column projections qualify.** Every projection item must be a plain column reference (`Expr::Identifier` or `Expr::CompoundIdentifier`) or a wildcard, all bound to the same base table. Any computed expression (`o.id + 1`, a function call, a scalar subquery, a `CASE`) yields `Ok(None)`, even when it derives from a single table, because the output rows are not rows of that base table. This subsumes the note's "aggregate / GROUP BY" case: an aggregate is a function-call projection and therefore already disqualifying. `GROUP BY` present is also `Ok(None)` as a defensive belt-and-braces check.

**Decision: adopt both rules (strict).** `SELECT o.id + 1 FROM orders o` returns `Ok(None)`, not `Ok(Some(orders))`. Only pass-through column references and wildcards bound to a single base table yield `Ok(Some)`.

## Revised specification

The public trait signatures from the original note are unchanged. The behavior below pins down the parts the note left implicit.

### `referenced_tables`

```text
walk the node with sqlparser::ast::visit_relations
for each &ObjectName r:
    match resolve_object_name(r, db):
        Ok(Some(t)) -> record db.table_id(t)
        Ok(None)    -> skip          (CTE names, table functions, unknown names)
        Err(Ambiguous) -> return Err  (per D2)
dedup recorded ids, map back to &Table, return in first-seen order
```

For `Insert` / `Update` / `Delete` this automatically includes the mutation target, because the target relations carry the `visit_relation` annotation. The MySQL multi-table `Delete.tables` list is not visited by `visit_relations`. If multi-table DELETE support is wanted in `referenced_tables`, resolve `Delete.tables` explicitly and union it in. Recommendation: do union it in, so DELETE coverage is complete.

### `projection_source_table` (Query only)

```text
if Query.body is not a single SELECT (set operation, VALUES, TABLE, ...) -> Ok(None)
collect outer CTE names from Query.with
build alias_map: for each TableFactor in SELECT.from and its joins:
    TableFactor::Table { name, alias, args: None } where name is not a CTE name
        -> resolve_object_name(name, db); on Some(t) bind (alias or last-name-part) -> t
    anything else (derived, table function, args set, CTE name) -> bind key -> NonBase
if SELECT.group_by is non-empty -> Ok(None)
let mut source = None
for each projection item:
    Wildcard            -> if alias_map has exactly one base table, that table; else Ok(None)
    QualifiedWildcard(q)-> q names an alias/table; if it maps to a base table, that table; else Ok(None)
    UnnamedExpr(e) | ExprWithAlias{e,..} | ExprWithAliases{e,..}:
        if e is Expr::Identifier(c):          unqualified column
            candidates = base tables in alias_map whose `column(c, db)` is Some
            0 -> Ok(None) (treat as not-from-base); 1 -> that table; >1 -> Err(AmbiguousTableLookup)
        else if e is Expr::CompoundIdentifier([.., q, c]): qualified column
            look up q in alias_map; if base table t and t.column(c, db) is Some -> t; else Ok(None)
        else -> Ok(None)   (computed expression, function, subquery, CASE, ...)
    fold into `source`: first base table sets it; a different base table -> Ok(None)
return Ok(source)
```

Notes. Self-joins are handled because binding is by resolved base table, and the fold deduplicates to a single table. Schema-qualified FROM names resolve through `resolve_object_name`. The unqualified-column ambiguity error is the one place `projection_source_table` returns `Err`, matching the note.

### `target_table` and `kind` (DML)

```text
Insert:
    kind = Insert
    target: match Insert.table:
        TableObject::TableName(obj) -> resolve_object_name(obj, db), None -> Err(no such table)
        TableFunction(_) | TableQuery(_) -> Err(InvalidObjectName: not a base-table target)
Update:
    kind = Update
    target: Update.table is a TableWithJoins. The mutation target is `.relation`.
        if relation is TableFactor::Table { name, args: None, .. } -> resolve_object_name(name, db)
        else -> Err(InvalidObjectName: update target is not a plain table)
Delete:
    kind = Delete
    target: if Delete.tables is non-empty (MySQL multi-table delete) -> Err (no single target)
        else inspect Delete.from (FromTable::WithFromKeyword | WithoutKeyword):
            if exactly one TableWithJoins whose relation is a plain TableFactor::Table -> resolve it
            else -> Err (no single target)
```

`target_table` returns `Result<&Table, LookupError>` (not `Option`), per the note, because a DML statement that does not resolve to a single base target is an error for the consumer, not a "no match".

### Resolver and refactor (decision: also refactor existing callers)

- New `pub(crate)` module `utils::object_name` containing the generic forms: `resolve_object_name<DB>(obj, db) -> Result<Option<&DB::Table>, LookupError>`, plus the supporting `object_name_identifiers`, `table_matches_lookup_idents`, `resolve_table_from_candidates`, and the `with_implicit_public` variant, all generic over `T: TableLike`. Render candidates with the existing `render_table_candidate` logic, generalized.
- Migrate `ParserDB::resolve_table_object_name`, `resolve_table_object_name_with_implicit_public`, and `resolve_schema_ident` to call the generic forms (they keep their public signatures).
- Migrate grant resolution (`table_matches_object_name`, `schema_from_object_name`, `object_name_last_part` in src/impls/sqlparser/grant.rs) and the foreign-key, trigger, and policy resolution paths onto the shared helpers. While doing so, verify the foreign-key path applies identifier normalization through `identifiers_match` (the note claims it does not). If confirmed, the migration fixes it. Add a regression test for the FK case (quoted or mixed-case referenced table).
- Remove the now-duplicate private `object_name_last_part` that exists in both grant.rs and sqlparser.rs.

This refactor touches working code, so it lands as its own step with the existing grant/FK/trigger/policy tests green before and after, no behavior change except the FK normalization fix.

### Cargo and feature

Per D1, add `visitor` to the sqlparser features unconditionally, and patch sqlparser to the git source that carries the `no_std` visitor fix (see the D1 implementation caveat).

```toml
[dependencies]
sqlparser = { version = "0.62", default-features = false, features = ["visitor"] }

[patch.crates-io]
sqlparser = { git = "https://github.com/apache/datafusion-sqlparser-rs", branch = "main" }
```

The new modules are compiled in every configuration, so `cargo build --lib --no-default-features` and the `thumbv7em-none-eabihf` / `wasm32-unknown-unknown` cross-compiles must stay green (verified in Step 2).

### Files

- New: `src/traits/data_statement.rs`, `src/traits/dql.rs`, `src/traits/dml.rs`. New: `src/impls/sqlparser/{data_statement,dql,dml}.rs`. New: `src/utils/object_name.rs`.
- Modify: `src/traits.rs` (export the three traits and `DmlKind`), prelude, `src/utils.rs` (export `object_name`), `src/impls/sqlparser.rs` (wire new impl modules), the grant/FK/trigger/policy impls (route through the resolver), `Cargo.toml` (visitor feature).
- New tests: `tests/data_statement_analysis.rs` against `ParserDB`, covering the original note's checklist plus the additions below.

### Test additions beyond the note's checklist

- `SELECT o.id + 1 FROM orders o` -> `projection_source_table` is `Ok(None)` (computed expression, D3).
- CTE shadowing a base table name, referenced in the outer FROM: confirm the chosen D2/D3 behaviors (over-inclusion in `referenced_tables`, `Ok(None)` eligibility in `projection_source_table`).
- INSERT INTO (SELECT ...) and INSERT INTO table-function -> `target_table` is `Err`.
- UPDATE with a joined source (`UPDATE t SET ... FROM s WHERE ...`) -> `target_table` is `t`, `referenced_tables` includes `t` and `s`.
- MySQL multi-table DELETE -> `target_table` is `Err`, and (if unioned per the revised spec) `referenced_tables` includes the listed tables.
- FK regression: quoted or mixed-case referenced table resolves after the resolver refactor.
- `cargo test`, `cargo clippy --all-targets -- -D warnings` (pedantic gate), `cargo doc` with `-D warnings`, and `cargo build --lib --no-default-features` (confirm `visitor` is `no_std`-clean) all green.

## Documented limitations

- CTE-name resolution in `referenced_tables` is intentionally approximate (over-inclusive) per D2. Precise lexical scoping of CTE names across nested and recursive `WITH` is out of scope.
- `MERGE` is a data statement but is excluded by the note's taxonomy. It is not implemented. `Statement::Merge(Merge)` exists if it is wanted later.
- Set-operation queries report `Ok(None)` for `projection_source_table` even when both sides project from the same single table. Resolving that would require comparing both arms and is out of scope.
