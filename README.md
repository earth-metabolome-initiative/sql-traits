# SQL Traits

[![CI](https://github.com/earth-metabolome-initiative/sql-traits/workflows/Rust%20CI/badge.svg)](https://github.com/earth-metabolome-initiative/sql-traits/actions)
[![Security Audit](https://github.com/earth-metabolome-initiative/sql-traits/workflows/Security%20Audit/badge.svg)](https://github.com/earth-metabolome-initiative/sql-traits/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Codecov](https://codecov.io/gh/earth-metabolome-initiative/sql-traits/branch/main/graph/badge.svg)](https://codecov.io/gh/earth-metabolome-initiative/sql-traits)

Rust traits describing SQL-like objects.

At this time, the library traits are implemented for [`sqlparser`](https://github.com/apache/datafusion-sqlparser-rs) AST types, and, in a crate downhill from this one, for [`pg_diesel`](https://github.com/earth-metabolome-initiative/pg_diesel) PostgreSQL Model structs. The former is intended for introspecting a database by parsing SQL documents, while the latter operates directly on an existing PostgreSQL database.

This crate is not yet published to `crates.io` because we still need to finalize the upstream crate [`geometric-traits`](https://github.com/earth-metabolome-initiative/geometric-traits).
