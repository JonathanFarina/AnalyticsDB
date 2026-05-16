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
| Repository scaffolding | Complete | repo layout, build tooling, linting, test harness | stable contributor workflow across supported platforms |
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
| Managed table indexes | Partial | SQL-driven primary-key/unique index metadata plus create/alter/drop/reindex index coverage and tested equality/`IN`/range lookup coverage | remote object-store backends, broader planner integration, multi-node maintenance semantics, and wider protocol conformance coverage |
| Table schema introspection | Partial | persisted column metadata with tests | broader metadata parity and information-schema style coverage |
| Users, roles, groups | Partial | user lifecycle (create/rotate/drop) tested across PG and Flight SQL; Argon2id password hashing with per-user salt; groups with ADD/DROP USER; legacy plaintext migration path | grants, revokes, role-aware planner checks, audit log |
| Single-node local query execution | Partial | tested local execution path | durable state, broader query coverage, and non-embedded protocol support |
| Native columnar storage | Partial | managed table write/read path via Parquet, manifest-based snapshots, atomic commits (CAS), orphan vacuum, compaction (`VACUUM <table>`), object-store URI routing (`s3://`, `gs://`, `azure://`, `file://`), `file://` storage root CLI parity test | `data/` subdirectory layout, S3 mock parity tests, performance evidence, recovery documentation |
| Native views | Partial | view definition and resolution | dependency tracking, authz, metadata, and regression coverage |
| External Parquet support | Partial | external registration and read path | optimizer, statistics, and parity with native SQL surface |
| External Iceberg support | Prototype | catalog integration and read path | schema evolution, metadata correctness, and interoperability proof |
| Automatic storage-medium selection | Prototype | policy engine scaffold | tested policy decisions, explainability, and override path |
| Unified SQL surface for native/external | Partial | unified planner logic | no user-facing special cases for normal querying workflows |
| Distributed planner | Prototype | multi-stage plan generation | correctness, skew handling, and metrics coverage |
| Distributed executor | Partial | remote stage execution scaffold with concurrent fetch, zero-materialization streaming, node resilience, cancellation, backpressure, retry/idempotency, worker resource quotas, intra-cluster mTLS (`ClusterMtlsConfig` + `analyticsdb ca init`), GROUP BY / DISTINCT / ORDER BY+LIMIT distributed plans, window-function blocking, row-count-aware skew partitioner, SQLite advisory catalog leases (`DistributedRelationLock`) | distributed equivalence tests from the CLI test suite, chaos / worker-kill retry integration test, broader plan coverage (hash joins) |
| Replication/eventual consistency | Prototype | design plus metadata hooks | failure recovery, repair flows, and consistency guarantees documented |
| Caching: query results | Prototype | cache abstraction and tests | invalidation, visibility rules, metrics, and predictable behavior |
| Caching: data blocks/segments | Prototype | cache abstraction and tests | eviction, warming, spill, and node-local safety |
| Query optimizer | Prototype | logical and physical rule scaffolding | statistics-aware distributed optimization with regressions covered |
| Logging and tracing | Partial | structured logs via tracing crate | full end-to-end query traceability across nodes |
| Query log / query-level lineage | Partial | durable async Parquet-backed `system.query_log` for the non-streaming execution path, config defaults, CLI-driven PostgreSQL-wire SQL coverage, and worker sibling rows (`is_initial_query = false`) for the distributed read path | streaming Flight SQL lifecycle accounting, DataFusion stage metrics, retention sweeper, partitioned layout, and benchmark gates |
| Metrics | Prototype | core service metrics | operator-ready dashboards and alertable signals |
| Encryption at rest | Partial | S3 SSE wired via `ANALYTICSDB_S3_SSE` / `ANALYTICSDB_S3_SSE_KMS_KEY_ID` env vars and `ClusterConfig.s3_sse`/`s3_sse_kms_key_id` fields; supports `AES256`, `aws:kms`, `aws:kms:dsse` | GCS/Azure equivalent, key rotation story, CLI-driven SSE verification test |
| CLI | Partial | one-shot query command plus interactive shell with protocol selection, Flight SQL TLS trust options, line editing, persistent history, multiline SQL, and initial meta commands (`\q`, `\?`, `\conninfo`) | broader psql-style meta-command coverage and polished timing UX complete |
| CLI speed measurement | Partial | timing output scaffold with detailed query/fetch, client total, render, and end-to-end timings behind `--timing` / `\timing` | accurate and documented timing behavior |
| Web console query editor | Prototype | Vite TypeScript UI with prototype client, editor, messages, result grid, query id, and timing cards | query execution against a real web gateway, auth/session handling, and protocol parity proof |
| Web console explorer | Prototype | Vite TypeScript UI with prototype database/schema/table/view explorer backed by sample metadata | stable navigation against live metadata across databases, schemas, tables, and views |
| Web console admin: databases | Prototype | UI scaffold | create/manage flows with authz and audit coverage |
| Web console admin: users | Prototype | UI scaffold | role/group management with authz and audit coverage |
| Web console admin: metrics | Prototype | UI scaffold | useful operator metrics with live or near-live accuracy |
| Web console admin: logs | Prototype | UI scaffold | multi-node log exploration with query correlation |
| Test coverage discipline | Partial | baseline CI and tests | no uncovered feature claims remain |
| Kubernetes deployment | Partial | manifests or Helm scaffold | repeatable production-grade deployment docs and checks |
| Object storage deployment | Partial | `s3://`, `gs://`, `azure://`, `file://` URI routing via `object_store`; env-chain credential resolution; manifest-based atomic commits; optional cluster-scoped `cluster=<id>/` key prefix; optional S3 SSE config | S3 mock CI test, multi-node durability test, recovery docs |

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
- Prototype server now supports **TLS/SSL encryption** for client-facing Flight SQL, including joined nodes when cluster TLS cert/key paths are configured, plus a separate internal node communication channel for distributed partition dispatch and **Prepared Statements** with schema planning, enabling standard JDBC/ODBC connectivity
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
- Managed cluster configuration holds common settings for ports, catalog paths, and security policies; coordinator startup now applies configured Flight SQL TLS paths to join responses so dynamically assigned client endpoints use `https://`, while node-to-node dispatch uses each node's dedicated internal endpoint
- CLI supports **automatic failover** and transparent reconnection across multiple cluster endpoints
- Prototype distributed partition dispatch exists for the current managed-table SELECT and INSERT SELECT scaffold. SELECT workers support four distributed plan types: (1) **plain aggregates** (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) with 2-phase partial/final aggregation; (2) **GROUP BY aggregates** — key columns passed through on workers, coordinator re-aggregates by the same keys; (3) **DISTINCT** — both worker and coordinator run `SELECT DISTINCT` for 2-phase deduplication; (4) **ORDER BY / LIMIT** — workers return local top-N, coordinator re-sorts. Window functions (`OVER(...)`) are explicitly blocked from distribution and fall through to local execution. Other supported single-table SELECTs use the correctness-first scaffold where workers scan assigned Parquet files and the coordinator finalizes the original SELECT over merged partition rows. Partition workers receive and refine the source table catalog schema when scanning assigned Parquet files so catalog types such as NUMERIC and DATE are preserved during planning.
- Managed tables can be materialized from `CREATE TABLE ... AS SELECT ...` and queried from a later CLI process
- Managed tables can also be defined with explicit column schemas and populated with `INSERT INTO ... VALUES ...` across later CLI processes
- Managed tables now support a prototype `INSERT INTO ... SELECT ...` append path that writes through DataFusion's Parquet sink into local Parquet snapshots, with CLI-driven coverage for a bounded generated-series insert
- Prototype engine caches DataFusion session contexts per logical session and invalidates the cache after catalog/table-mutating command outcomes; managed and external Parquet relation registration now uses direct DataFusion Parquet table registration with catalog schema metadata when available
- Managed table inserts support column-list insertion for the current tested embedded prototype subset
- Managed tables now support **UPDATE**, **DELETE**, **TRUNCATE**, **DROP**, and **RENAME** operations through SQL
- Managed tables now support **ALTER TABLE** (ADD COLUMN, DROP COLUMN, RENAME COLUMN, DROP CONSTRAINT, ALTER COLUMN) for schema evolution, including physical snapshot updates to maintain Parquet readability and **CASCADE** support for DROP CONSTRAINT to handle dependent objects like foreign keys
- Managed tables now support a prototype sidecar index path for table-defined `PRIMARY KEY` / `UNIQUE` constraints plus `CREATE INDEX`, `ALTER INDEX RENAME TO`, `DROP INDEX`, and `REINDEX INDEX` / `REINDEX TABLE`; managed-table storage locations are persisted as `file://` URIs, index snapshots publish through versioned manifests, same-table mutations are serialized in-process, and equality/`IN`/prefix/range filters over the current supported managed-table slice can be answered from the sidecar instead of scanning every Parquet file, with schema-wide index-name enforcement, protected constraint-backed indexes, pre-commit duplicate validation for new unique constraints/indexes, and rebuild-safe recovery after table rewrites
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
- Prototype durable **query log / query-level lineage** now writes non-streaming query outcomes asynchronously to Parquet under `<catalog>.managed/system/query_log/` and exposes the records as `system.query_log`; coverage includes an engine SQL test and a CLI-driven PostgreSQL-wire SQL test. Current gaps remain for Flight SQL stream-finish logging, execution-plan metrics, retention, distributed worker sibling rows, date-partitioned layout, and benchmark gates.
- Distributed scatter-gather path now supports **concurrent worker fetch** and **zero-materialization streaming** via a `StreamingTableProvider`, allowing large result sets (e.g. 1B rows) to flow from workers directly to the client without coordinator-side materialization
- Distributed executor now includes **node resilience** with active failure detection (marking nodes `Unavailable`) and automatic **query retry** with re-partitioning across remaining healthy nodes, with transparent fallback to local execution if the cluster is unavailable
- Coordinator now uses a **cost-aware worker selection heuristic** to determine the optimal number of nodes based on data size (~128MB per worker target) and file count, avoiding "scatter-gather tax" on small datasets by selectively dispatching to a subset of the cluster
- Distributed path performance optimized with **Bincode serialization** for internal gRPC, **connection pooling** for worker channels, **row-count-aware greedy partitioning** (balances by Parquet row count when available; falls back to byte size), and a **FileListCache** with version epochs to eliminate redundant object store listings; per-file row counts surfaced via `list_files_with_sizes_and_rows` from the manifest
- Managed table write paths (`INSERT INTO ... SELECT`) optimized with **batched Parquet writes** (consolidating into 1M row files) and **lazy index rebuilding** in background tasks to reduce write latency
- Engine crate `lib.rs` split into focused modules (`ddl.rs`, `dml.rs`, `dispatch_impl.rs`, `dispatch_plan.rs`, `batch.rs`, `index_impl.rs`, `index_ops.rs`, `sql_rewriter.rs`, `postgres_compatibility.rs`, `schema_build.rs`, `metadata_helpers.rs`, `manifest.rs`, `storage.rs`, `system_catalog.rs`, `information_schema.rs`, `functions.rs`, `query_log/`); production code in `lib.rs` ≤ 1450 lines
- Request-path `unwrap()` / `expect()` / `panic!()` eliminated from all non-test engine and protocol code; `#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::todo, clippy::unimplemented))]` gates active on `analyticsdb-engine` and `analyticsdb-protocol`
- CI matrix covers Linux x86_64, Linux ARM64, macOS ARM64, nightly clippy lint surface, and security audit (`cargo deny` + `cargo audit`) plus release build
- Managed-table storage now routes through `object_store`-backed `store_for_location` for `s3://`, `gs://`, `azure://`/`az://`/`abfss://`, `file://`, and local paths; cloud backends use env-chain credential resolution
- Manifest-based atomic snapshots: `read_manifest`/`append_to_manifest`/`replace_manifest` with CAS loop (`PutMode::Create` / `PutMode::Update(e_tag)`) and `PutMode::Overwrite` fallback for backends without preconditions; directory-scan fallback for pre-manifest tables
- `vacuum_orphans()` prunes staged-but-uncommitted files; `compact_table()` merges small Parquet files using a row-budget heuristic (target 128 MiB per output file) and exposes `VACUUM <table>` SQL
- Index snapshots stored under `<table_prefix>/.analyticsdb_indexes/` via the same `object_store` abstraction
- Optional `cluster=<id>/` key prefix in managed-table URIs controlled by `ClusterConfig.cluster_id`
- Optional S3 server-side encryption via `ANALYTICSDB_S3_SSE` / `ANALYTICSDB_S3_SSE_KMS_KEY_ID` env vars (also propagated from `ClusterConfig.s3_sse` / `s3_sse_kms_key_id` at startup)
- `cli_file_url_storage_root_supports_full_dml_ddl_lifecycle` CLI test exercises CREATE/INSERT/SELECT/UPDATE/DELETE/TRUNCATE/DROP against an explicit `file://` storage root
- Distributed executor now includes query **cancellation** (`CancelPartition` DoAction; `CancellationToken` propagated through all worker calls; `KILL QUERY <id>` SQL), **backpressure** (bounded per-partition `mpsc::channel(16)`), **retry/idempotency** (attempt-id embedded in filenames), and **worker resource quotas** (shared `GreedyMemoryPool`, admission semaphore via `ANALYTICSDB_WORKER_MEMORY_LIMIT_MIB` / `ANALYTICSDB_MAX_CONCURRENT_QUERIES`)
- Graceful shutdown: SIGTERM/SIGINT handler cancels all in-flight queries and drains listeners
- Node heartbeat: 10 s background heartbeat per node; coordinator prunes nodes silent > 45 s to `Unavailable`; `Heartbeat` Flight DoAction for remote heartbeat
- Password storage upgraded to Argon2id (PHC string format, OsRng salt per user); `CREATE USER` and `ALTER USER ... PASSWORD` hash before writing; legacy plaintext accepted during migration window
- Intra-cluster mTLS now enforced: `ClusterMtlsConfig` uses tonic `ClientTlsConfig` + `client_ca_root()`; `analyticsdb ca init` generates ECDSA P-256 CA and leaf certs; `tls_ca_cert_path` in `ClusterConfig`
- Catalog mutation serialization now distributed: `DistributedRelationLock` acquires a 30 s SQLite advisory lease from the `table_leases` table on construction and releases it asynchronously on drop; `ControlPlane::try_acquire_relation_lease` / `release_relation_lease` exposed publicly; JSON catalog always grants (single-coordinator assumption)
- Worker-side query log rows now written: `execute_partition` (read path) records a `QueryProbe` row in `system.query_log` with `is_initial_query = false`, keyed by `initial_query_id`
- No S3 mock parity test yet; S3 parity requires `ANALYTICSDB_S3_TEST_BUCKET` env var
- No production distributed execution yet; remaining gaps are distributed equivalence tests from the CLI test suite and a chaos / worker-kill retry integration test
- No object-storage-backed production columnar managed-table storage yet (S3/GCS/Azure paths exist but are not CI-tested end-to-end)
- No external Iceberg table support yet
- No deployment manifests yet
- No broad PostgreSQL extended-query compatibility, auth, or conformance suite yet
- Flight SQL now supports the full bind/execute/close protocol cycle for prepared statements, enabling standard JDBC/ODBC connectivity
- broad Flight SQL `SqlInfo` coverage exists for core server identification and SQL dialect metadata
- No broad PostgreSQL/Flight SQL parity claim beyond the current explicitly tested slice

Any agent claiming otherwise is wrong and must correct the tracker immediately.
