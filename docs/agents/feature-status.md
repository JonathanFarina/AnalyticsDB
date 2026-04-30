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
| Single endpoint strategy | Complete | entrypoint design and integration path with node registration | validated high-availability client behavior with automatic failover and heartbeats |
| PostgreSQL wire protocol | Partial | startup/session bootstrap plus simple-query and parameterized extended-query tests | auth and compatibility suite for supported surface area |
| Arrow Flight SQL protocol | Partial | statement query/update and metadata flow tests | metadata breadth, prepared statements with full bind/execute/close cycle, auth, and parity validation |
| Protocol equivalence | Partial | shared session/result contract plus CLI-driven PostgreSQL/Flight SQL parity tests for an explicit supported slice including result-shape, command-tag, and session-parameter reflection assertions | proven parity for supported user-facing capabilities beyond the current narrow slice |
| SQL parser and analyzer | Partial | parser and semantic scaffold | documented PostgreSQL-compatibility coverage |
| PostgreSQL syntax compatibility | Prototype | tested subset | explicit supported matrix and conformance coverage |
| PostgreSQL built-in functions | Prototype | tested subset | compatibility evidence for supported function families |
| Catalogs, databases, schemas | Partial | metadata model and basic queries | stable behavior across both protocols and admin tools |
| Tables/views metadata | Partial | persistent relation metadata with tests | stable DDL, metadata parity, and admin visibility |
| Persistent catalog/metadata store | Partial | durable metadata persistence with tests | resilient storage, migration story, and operator controls |
| Managed tables (prototype columnar snapshots) | Partial | CTAS persistence and query path with tests | broader DDL, production durability, and storage-engine maturity |
| Managed table indexes | Partial | SQL-driven primary-key/unique index metadata plus create/alter/drop index and tested equality/`IN`/range lookup coverage | remote object-store backends, broader planner integration, multi-node maintenance semantics, and wider protocol conformance coverage |
| Table schema introspection | Partial | persisted column metadata with tests | broader metadata parity and information-schema style coverage |
| Users, roles, groups | Prototype | authz model and basic tests | audited admin workflows, grants, revokes, and metadata parity |
| Single-node local query execution | Partial | tested local execution path | durable state, broader query coverage, and non-embedded protocol support |
| Native columnar storage | Partial | managed table write/read path via Parquet | durability, compaction, recovery, and performance evidence |
| Native views | Partial | view definition and resolution | dependency tracking, authz, metadata, and regression coverage |
| External Parquet support | Partial | external registration and read path | optimizer, statistics, and parity with native SQL surface |
| External Iceberg support | Prototype | catalog integration and read path | schema evolution, metadata correctness, and interoperability proof |
| Automatic storage-medium selection | Prototype | policy engine scaffold | tested policy decisions, explainability, and override path |
| Unified SQL surface for native/external | Partial | unified planner logic | no user-facing special cases for normal querying workflows |
| Distributed planner | Prototype | multi-stage plan generation | correctness, skew handling, and metrics coverage |
| Distributed executor | Prototype | remote stage execution scaffold | resilience, retries, cancellation, and backpressure handling |
| Replication/eventual consistency | Prototype | design plus metadata hooks | failure recovery, repair flows, and consistency guarantees documented |
| Caching: query results | Prototype | cache abstraction and tests | invalidation, visibility rules, metrics, and predictable behavior |
| Caching: data blocks/segments | Prototype | cache abstraction and tests | eviction, warming, spill, and node-local safety |
| Query optimizer | Prototype | logical and physical rule scaffolding | statistics-aware distributed optimization with regressions covered |
| Logging and tracing | Partial | structured logs via tracing crate | full end-to-end query traceability across nodes |
| Metrics | Prototype | core service metrics | operator-ready dashboards and alertable signals |
| Encryption at rest | Prototype | key management design and hooks | end-to-end encrypted storage path with rotation story |
| CLI | Partial | one-shot query command plus interactive shell with protocol selection, Flight SQL TLS trust options, line editing, persistent history, multiline SQL, and initial meta commands (`\q`, `\?`, `\conninfo`) | broader psql-style meta-command coverage and polished timing UX complete |
| CLI speed measurement | Partial | timing output scaffold with detailed query/fetch, client total, render, and end-to-end timings behind `--timing` / `\timing` | accurate and documented timing behavior |
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
- Prototype protocol crate now includes tested PostgreSQL wire prototype session-setting compatibility for common `SET` / `RESET` / `SHOW` forms, including preserved `search_path` routing semantics, JDBC/libpq-style `extra_float_digits`, `SHOW ALL`, prototype `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`, and stored generic client parameters in both simple and extended query paths
- Prototype protocol crate now includes tested PostgreSQL startup `ParameterStatus` coverage for common client-expected keys (`server_version`, `client_encoding`, `DateStyle`, `TimeZone`, `standard_conforming_strings`, `search_path`, `application_name`, and `default_transaction_isolation`)
- Prototype engine now includes real DataFusion UDF implementations for PostgreSQL introspection functions (`version()`, `current_database()`, `current_schema()`, `current_user()`, `session_user()`, `current_setting('<name>')`), replacing the previous protocol-level shims
- Prototype engine now includes a PostgreSQL-compatibility rewrite plus UDF support for numeric interval scaling such as `random() * INTERVAL '5 years'`
- Prototype engine now handles transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`) as successful no-ops to support standard PostgreSQL client lifecycles
- Prototype engine now provides an integrated `pg_catalog` schema through custom DataFusion `TableProvider`s, enabling complex metadata queries, joins, and filters for `pg_tables`, `pg_views`, `pg_namespace`, `pg_database`, and `pg_roles`
- Prototype protocol crate now includes tested PostgreSQL extended-query literal rendering that preserves parameter markers inside SQL string literals while still binding typed placeholders
- Prototype protocol crate now includes tested PostgreSQL startup auth negative paths for unknown-user and wrong-password failures
- Prototype protocol crate can expose a tested Flight SQL statement query/update path plus basic metadata listing
- Prototype protocol crate now includes a shared prototype auth hook path used by PostgreSQL startup and Flight SQL handshake, with control-plane user lookup and role/auth-method metadata propagation into session context
- Prototype server now supports **TLS/SSL encryption** for Flight SQL and **Prepared Statements** with schema planning, enabling standard JDBC/ODBC connectivity
- Flight SQL statement and prepared-statement query tickets now carry the planned row schema from `GetFlightInfo`/prepare, and `DoGet` streams row batches from the shared engine row-stream path instead of re-planning and materializing the full result before encoding
- CLI-driven tests now prove a narrow PostgreSQL/Flight SQL protocol-equivalent slice for non-parameterized SQL execution, requested schema routing, schema-scoped managed table/view workflows, cross-database metadata/DDL flows (`CREATE DATABASE`, `CREATE SCHEMA <database>.<schema>`, `SHOW DATABASES`, `SHOW SCHEMAS FROM <database>`, schema-qualified table create/insert/list), SQL metadata statements, user-visible unknown-database/unknown-schema/missing-relation query errors, and user-visible duplicate-table-create/NOT NULL/INSERT-value-count command errors through live listeners
- CLI-driven tests now include a table-driven parity matrix over the current supported SQL surface that compares live PostgreSQL and Flight SQL user-visible success/error contracts
- CLI-driven tests now include a capability-level drift guard that checks README-supported SQL subset statements against matrix-covered protocol parity capabilities
- CLI-driven tests now include user-visible auth/session parity assertions for PostgreSQL and Flight SQL plus matched unknown-user auth failure behavior
- CLI-driven tests now include a strict password matrix for valid and invalid credential outcomes across live PostgreSQL and Flight SQL listeners
- CLI-driven tests now include password rotation behavior that invalidates old credentials and accepts rotated credentials across live PostgreSQL and Flight SQL listeners
- CLI-driven tests now include strict `ALTER USER ... PASSWORD ...` error-contract parity checks for unknown users, empty passwords, malformed literals, and non-admin authorization failures across PostgreSQL and Flight SQL listeners
- CLI-driven tests now include result-shape assertions (exact column names) for all metadata SQL statements (`SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW VIEWS`, `SHOW COLUMNS FROM`, `DESCRIBE`, `SELECT` scalar) through both PostgreSQL and Flight SQL wire protocols
- CLI-driven tests now include a customer-table parity regression that compares PostgreSQL and Flight SQL behavior for `SELECT`, `DESCRIBE`, `SHOW COLUMNS`, and projected `information_schema.columns` queries using the shared statement outcome contract
- CLI-driven tests now include command-tag / message consistency assertions confirming that DDL (`CREATE DATABASE`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VIEW`, `ALTER USER PASSWORD`) produces "Command completed. 0 row(s) affected." and DML INSERT produces "Command completed. N row(s) affected." identically across both wire protocols
- CLI-driven tests now include session-parameter reflection assertions verifying that database, schema, user, role, and auth_method in the response session context match the startup parameters sent through both PostgreSQL wire startup and Flight SQL header handshake
- CLI-driven tests now include an initial pg_catalog compatibility slice validating `pg_catalog.pg_tables`, `pg_catalog.pg_views`, `pg_catalog.pg_namespace`, `pg_catalog.pg_database`, and `pg_catalog.pg_roles` through both live protocol listeners, including tested projection/filter/order forms with equality + `IN` filters and mixed-direction multi-column `ORDER BY ASC|DESC` for the current constrained prototype subset
- CLI-driven tests now include an initial `information_schema` compatibility slice validating `information_schema.schemata`, `information_schema.tables`, `information_schema.columns`, `information_schema.views`, `information_schema.table_constraints`, `information_schema.key_column_usage`, `information_schema.constraint_column_usage`, `information_schema.constraint_table_usage`, and `information_schema.referential_constraints` through both live protocol listeners, including tested projection/filter/order forms with equality + `IN` filters and mixed-direction multi-column `ORDER BY ASC|DESC` for the current constrained prototype subset
- current information_schema constraint compatibility now includes deterministic prototype NOT NULL constraint rows in `table_constraints`, `constraint_column_usage`, and `constraint_table_usage` for managed-table NOT NULL columns, plus table-defined primary-key/foreign-key metadata rows in `key_column_usage` and `referential_constraints` for the current supported CREATE TABLE constraint subset
- Protocol-crate integration tests now include Flight SQL metadata API coverage (`get_db_schemas`, `get_tables`) that validates schema/table/view discovery for the current pg_catalog compatibility setup
- Prototype server binary can run PostgreSQL wire and Flight SQL listeners against the current engine
- Prototype metadata SQL subset exists for creating and listing databases, schemas, tables, and views, plus table column introspection, prototype `ALTER USER ... PASSWORD ...` rotation, and **SHOW NODES** node discovery
- Control plane supports **distributed coordination** with **leader election (coordinator)** and **heartbeat-based health tracking**
- Cluster supports **dynamic scaling** with **automatic port assignment** for new nodes joining the coordinator
- Managed cluster configuration holds common settings for ports, catalog paths, and security policies
- CLI supports **automatic failover** and transparent reconnection across multiple cluster endpoints
- Managed tables can be materialized from `CREATE TABLE ... AS SELECT ...` and queried from a later CLI process
- Managed tables can also be defined with explicit column schemas and populated with `INSERT INTO ... VALUES ...` across later CLI processes
- Managed tables now support a prototype `INSERT INTO ... SELECT ...` append path that writes through DataFusion's Parquet sink into local Parquet snapshots, with CLI-driven coverage for a bounded generated-series insert
- Prototype engine caches DataFusion session contexts per logical session and invalidates the cache after catalog/table-mutating command outcomes; managed and external Parquet relation registration now uses direct DataFusion Parquet table registration with catalog schema metadata when available
- Managed table inserts support column-list insertion for the current tested embedded prototype subset
- Managed tables now support **UPDATE**, **DELETE**, **TRUNCATE**, **DROP**, and **RENAME** operations through SQL
- Managed tables now support **ALTER TABLE** (ADD COLUMN, DROP COLUMN, RENAME COLUMN, DROP CONSTRAINT, ALTER COLUMN) for schema evolution, including physical snapshot updates to maintain Parquet readability and **CASCADE** support for DROP CONSTRAINT to handle dependent objects like foreign keys
- Managed tables now support a prototype sidecar index path for table-defined `PRIMARY KEY` / `UNIQUE` constraints plus `CREATE INDEX`, `ALTER INDEX RENAME TO`, and `DROP INDEX`; managed-table storage locations are persisted as `file://` URIs, index snapshots publish through versioned manifests, same-table mutations are serialized in-process, and equality/`IN`/prefix/range filters over the current supported managed-table slice can be answered from the sidecar instead of scanning every Parquet file, with schema-wide index-name enforcement, protected constraint-backed indexes, pre-commit duplicate validation for new unique constraints/indexes, and rebuild-safe recovery after table rewrites
- Table and view metadata listing supports schema-scoped `SHOW TABLES FROM ...` and `SHOW VIEWS FROM ...` in the current tested embedded prototype subset
- Table and view metadata now support **DROP VIEW**, **DROP SCHEMA**, **DROP DATABASE**, and **ALTER SCHEMA RENAME TO** through SQL, with tested CASCADE behavior for schemas
- Prototype engine now supports the **EXPLAIN** command to expose query plans across wire protocols
- Managed table snapshots are stored as **native Parquet files** in a schema-scoped directory
- Managed tables can be described later through `SHOW COLUMNS FROM` and `DESCRIBE`
- Persisted views can be queried from a later CLI process through the shared catalog
- CLI can drive SQL in embedded mode and against the prototype PostgreSQL wire and Flight SQL listeners
- CLI Flight SQL result handling consumes `DoGet` batches as a stream before rendering, avoiding an intermediate full `Vec<RecordBatch>` collection in the client path
- CLI-driven SQL tests are part of the build/test path, including live PostgreSQL wire and Flight SQL listener coverage
- Baseline CI workflow exists for fmt, clippy, and tests
- Prototype integrated **structured logging and tracing** via the `tracing` crate exists for both protocol paths
- No distributed execution yet
- No object-storage-backed production columnar managed-table storage yet (currently local Parquet)
- No external Iceberg table support yet
- No deployment manifests yet
- No broad PostgreSQL extended-query compatibility, auth, or conformance suite yet
- Flight SQL now supports the full bind/execute/close protocol cycle for prepared statements, enabling standard JDBC/ODBC connectivity
- broad Flight SQL `SqlInfo` coverage exists for core server identification and SQL dialect metadata
- No broad PostgreSQL/Flight SQL parity claim beyond the current explicitly tested slice

Any agent claiming otherwise is wrong and must correct the tracker immediately.
