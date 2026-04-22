# Feature Status

This file is the canonical feature tracker for the repository until replaced by a more structured system. Every feature listed here must always carry one of these statuses:

- `Prototype`: scaffold or design only, not production ready
- `Partial`: working capability with tests, but incomplete for production
- `Complete`: production ready, fully tested, observable, documented, and end-to-end validated

The repository now contains a limited but real prototype foundation, so only the features explicitly marked `Partial` below should be treated as implemented to any meaningful degree.

## Status Update Rules

You may move a feature:

- from `Prototype` to `Partial` only when code exists, at least one meaningful automated test exists, and the feature can be demonstrated
- from `Partial` to `Complete` only when the full definition of done is satisfied

You may not:

- skip directly from `Prototype` to `Complete`
- mark a feature `Complete` because a library claims to support it
- mark a feature `Complete` when it works for only one protocol if the requirement is protocol-equivalent
- mark a feature `Complete` without updating docs and tests
- mark a SQL-testable feature `Partial` or `Complete` unless the build/test process exercises it through the CLI with SQL

## Definition Of Done For `Complete`

A feature is `Complete` only when all of the following are true:

- end-to-end implementation exists
- automated tests cover happy path, failure path, and regression cases
- observability exists where relevant
- documentation exists for operators or developers
- security implications are addressed where relevant
- performance characteristics are known where relevant
- PostgreSQL and Flight SQL parity is satisfied where relevant
- native and external storage parity is satisfied where relevant
- if the feature is SQL-testable, build/test automation verifies it through the CLI by submitting SQL to the engine
- if the CLI path fails for a SQL-testable feature, the feature remains failed until reviewed and re-tested successfully

## Feature Matrix

| Area | Status | Notes Required Before `Partial` | Notes Required Before `Complete` |
|---|---|---|---|
| Repository scaffolding | Partial | repo layout, build tooling, linting, test harness | stable contributor workflow across supported platforms |
| Control plane | Partial | basic service with cluster metadata and query ids | production-grade routing, HA behavior, failover, observability |
| Query routing by utilization | Prototype | routing logic with tests | load-aware routing proven under concurrency and node churn |
| Single endpoint strategy | Prototype | entrypoint design and integration path | validated high-availability client behavior |
| PostgreSQL wire protocol | Partial | startup/session bootstrap plus simple-query and parameterized extended-query tests | auth and compatibility suite for supported surface area |
| Arrow Flight SQL protocol | Partial | statement query/update and metadata flow tests | metadata breadth, prepared statements, auth, and parity validation |
| Protocol equivalence | Prototype | shared session and result model | proven parity for supported user-facing capabilities |
| SQL parser and analyzer | Partial | parser and semantic scaffold | documented PostgreSQL-compatibility coverage |
| PostgreSQL syntax compatibility | Prototype | tested subset | explicit supported matrix and conformance coverage |
| PostgreSQL built-in functions | Prototype | tested subset | compatibility evidence for supported function families |
| Catalogs, databases, schemas | Partial | metadata model and basic queries | stable behavior across both protocols and admin tools |
| Tables/views metadata | Partial | persistent relation metadata with tests | stable DDL, metadata parity, and admin visibility |
| Persistent catalog/metadata store | Partial | durable metadata persistence with tests | resilient storage, migration story, and operator controls |
| Managed tables (prototype columnar snapshots) | Partial | CTAS persistence and query path with tests | broader DDL, production durability, and storage-engine maturity |
| Table schema introspection | Partial | persisted column metadata with tests | broader metadata parity and information-schema style coverage |
| Users, roles, groups | Prototype | authz model and basic tests | audited admin workflows, grants, revokes, and metadata parity |
| Single-node local query execution | Partial | tested local execution path | durable state, broader query coverage, and non-embedded protocol support |
| Native columnar storage | Partial | managed table write/read path | durability, compaction, recovery, and performance evidence |
| Native views | Partial | view definition and resolution | dependency tracking, authz, metadata, and regression coverage |
| External Parquet support | Prototype | external registration and read path | optimizer, statistics, and parity with native SQL surface |
| External Iceberg support | Prototype | catalog integration and read path | schema evolution, metadata correctness, and interoperability proof |
| Automatic storage-medium selection | Prototype | policy engine scaffold | tested policy decisions, explainability, and override path |
| Unified SQL surface for native/external | Prototype | planner abstraction | no user-facing special cases for normal querying workflows |
| Distributed planner | Prototype | multi-stage plan generation | correctness, skew handling, and metrics coverage |
| Distributed executor | Prototype | remote stage execution scaffold | resilience, retries, cancellation, and backpressure handling |
| Replication/eventual consistency | Prototype | design plus metadata hooks | failure recovery, repair flows, and consistency guarantees documented |
| Caching: query results | Prototype | cache abstraction and tests | invalidation, visibility rules, metrics, and predictable behavior |
| Caching: data blocks/segments | Prototype | cache abstraction and tests | eviction, warming, spill, and node-local safety |
| Query optimizer | Prototype | logical and physical rule scaffolding | statistics-aware distributed optimization with regressions covered |
| Logging and tracing | Prototype | structured logs and query ids | full end-to-end query traceability across nodes |
| Metrics | Prototype | core service metrics | operator-ready dashboards and alertable signals |
| Encryption at rest | Prototype | key management design and hooks | end-to-end encrypted storage path with rotation story |
| CLI | Partial | command shell scaffold | protocol selection, history, line editing, and timing UX complete |
| CLI speed measurement | Partial | timing output scaffold | accurate and documented timing behavior |
| Web console query editor | Prototype | page scaffold | query execution, messages, results, and timing complete |
| Web console explorer | Prototype | metadata browsing scaffold | stable navigation across databases, schemas, tables, and views |
| Web console admin: databases | Prototype | UI scaffold | create/manage flows with authz and audit coverage |
| Web console admin: users | Prototype | UI scaffold | role/group management with authz and audit coverage |
| Web console admin: metrics | Prototype | UI scaffold | useful operator metrics with live or near-live accuracy |
| Web console admin: logs | Prototype | UI scaffold | multi-node log exploration with query correlation |
| Test coverage discipline | Partial | baseline CI and tests | no uncovered feature claims remain |
| Kubernetes deployment | Prototype | manifests or Helm scaffold | repeatable production-grade deployment docs and checks |
| Object storage deployment | Prototype | object-store integration scaffold | tested durability, recovery, and multi-node behavior |

## Current Repository Status

- Rust workspace scaffold exists
- Control plane can admit queries, generate query IDs, validate bootstrap user/database/schema context, and persist database/schema metadata to JSON
- Control plane can persist and list view metadata in JSON
- Control plane can persist and list managed table metadata in JSON
- Control plane can persist managed table column metadata in JSON
- Prototype engine can execute tested scalar SQL through the CLI in embedded mode
- Prototype protocol crate can expose a tested PostgreSQL wire startup, simple-query path, and parameterized extended-query subset
- Prototype protocol crate can expose a tested Flight SQL statement query/update path plus basic metadata listing
- Prototype server binary can run PostgreSQL wire and Flight SQL listeners against the current engine
- Prototype metadata SQL subset exists for creating and listing databases, schemas, tables, and views, plus table column introspection
- Managed tables can be materialized from `CREATE TABLE ... AS SELECT ...` and queried from a later CLI process
- Managed tables can also be defined with explicit column schemas and populated with `INSERT INTO ... VALUES ...` across later CLI processes
- Managed table inserts support column-list insertion for the current tested embedded prototype subset
- Table and view metadata listing supports schema-scoped `SHOW TABLES FROM ...` and `SHOW VIEWS FROM ...` in the current tested embedded prototype subset
- Managed table snapshots are stored in a column-oriented JSON layout in the current prototype
- Managed tables can be described later through `SHOW COLUMNS FROM` and `DESCRIBE`
- Persisted views can be queried from a later CLI process through the shared catalog
- CLI can drive SQL in embedded mode and against the prototype PostgreSQL wire and Flight SQL listeners
- CLI-driven SQL tests are part of the build/test path, including live PostgreSQL wire and Flight SQL listener coverage
- Baseline CI workflow exists for fmt, clippy, and tests
- No distributed execution yet
- No object-storage-backed production columnar managed-table storage yet
- No external table support yet
- No deployment manifests yet
- No broad PostgreSQL extended-query compatibility, auth, or conformance suite yet
- No Flight SQL prepared statements, `SqlInfo`, or handshake auth yet

Any agent claiming otherwise is wrong and must correct the tracker immediately.
