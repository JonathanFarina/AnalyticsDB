# Workstreams

This file gives agents a default build order so they do not need to infer the full roadmap from scratch on every task.

All phases are currently `Prototype`.

## Phase 0: Foundations

Status: `Partial`

Goals:

- establish repository layout
- choose baseline toolchains
- create CI, linting, formatting, and test harnesses
- define status tracking discipline
- create initial architecture decision records if needed

Outputs:

- buildable workspace
- reproducible local development flow
- contributor documentation

Current evidence:

- Rust workspace and crate layout exist
- local build, fmt, lint, and test commands exist
- CI workflow exists
- CLI-driven SQL tests exist for the current prototype slice

## Phase 1: Control Plane And Catalog Skeleton

Status: `Prototype`

Goals:

- cluster identity and membership model
- query id generation
- initial catalog for databases, schemas, tables, views, users, roles, groups
- policy model for storage decisions

Outputs:

- metadata service scaffold
- initial persistence and migration story
- tests for core metadata workflows

Current evidence:

- embedded control-plane crate exists
- cluster membership model supports **node registration and discovery** via SQL (**SHOW NODES**)
- **high-availability strategy** implemented with background **heartbeats and node pruning** in the control plane
- query admission now issues query ids and **round-robin routes to registered coordinators**
- **CLI supports automatic failover** across multiple comma-separated endpoints for both PostgreSQL and Flight SQL
- bootstrap users, databases, and schemas are validated on the query path
- JSON-backed catalog persistence exists for databases and schemas
- metadata SQL subset exists for creating and listing databases, schemas, and views
- persisted views can be created and queried in later CLI sessions
- prototype managed tables can be materialized with CTAS and queried in later CLI sessions
- prototype managed tables can also be defined with explicit columns and populated through `INSERT INTO ... VALUES ...`
- prototype managed-table inserts now support tested column-list insertion with omitted nullable columns
- prototype metadata listing now supports tested schema-scoped table and view discovery
- prototype managed table snapshots are now stored as native Parquet files in schema-scoped directories
- prototype managed tables now support **UPDATE**, **DELETE**, **TRUNCATE**, **DROP**, and **RENAME** operations
- prototype managed tables now expose persisted column metadata through SQL introspection
- **ALTER SCHEMA RENAME TO** is supported and tested for managed relations physically
- **EXPLAIN** is supported across protocols to expose query plans

Remaining gaps before this phase should be considered `Partial` overall:

- no object-storage-backed production columnar managed-table storage yet (local Parquet only)
- no roles/groups yet
- no storage policy model yet

## Phase 2: Single-Node Execution Slice

Status: `Prototype`

Goals:

- local query execution path
- initial SQL parsing and analysis
- first native table path
- first external Parquet path
- shared session and result model

Outputs:

- honest, tested single-node prototype
- no false distributed claims

## Phase 3: PostgreSQL Protocol Surface

Status: `Partial`

Goals:

- connection handling
- authentication
- session lifecycle
- simple queries and result streaming
- metadata surfacing

Outputs:

- tested PostgreSQL protocol slice
- compatibility gaps explicitly listed

Current evidence:

- prototype PostgreSQL wire listener exists
- startup/session bootstrap path is tested
- simple-query execution is tested
- parameterized extended-query execution is tested
- PostgreSQL wire now includes tested JDBC-style introspection query shims for `version()`, `current_database()`, `current_schema()`, `current_user`, `session_user`, and `current_setting('<name>')`
- PostgreSQL wire now includes tested prototype transaction-isolation session compatibility for `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`, `SHOW transaction_isolation` / `SHOW TRANSACTION ISOLATION LEVEL`, and startup `default_transaction_isolation` `ParameterStatus`
- CLI-driven SQL tests now validate PostgreSQL wire execution against a live listener
- CLI-driven paired tests now prove a narrow protocol-equivalent slice with Flight SQL for non-parameterized SQL execution, requested schema routing, schema-scoped and cross-database metadata/DDL SQL statements, user-visible unknown-database/unknown-schema/missing-relation query errors, and user-visible duplicate-table-create/NOT NULL/INSERT-value-count command errors
- CLI-driven parity now also includes a broad table-driven matrix test over the current supported SQL surface for live PostgreSQL wire and Flight SQL listeners
- CLI-driven parity pg_catalog metadata slice now covers `pg_tables`, `pg_views`, `pg_namespace`, `pg_database`, and `pg_roles` for the current constrained projection/filter/order subset
- CLI-driven parity information_schema metadata slice now covers `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `constraint_column_usage`, `constraint_table_usage`, and `referential_constraints` for the current constrained projection/filter/order subset
- information_schema constraint parity now includes deterministic prototype NOT NULL rows in `table_constraints`, `constraint_column_usage`, and `constraint_table_usage`, plus table-defined primary-key/foreign-key rows in `key_column_usage` and `referential_constraints` for the current supported CREATE TABLE constraint subset

Remaining gaps before this phase should be considered `Complete`:

- no real authentication or role-aware session management
- no broad PostgreSQL compatibility suite
- no information-schema or metadata parity proof
- no broad prepared-statement / extended-query compatibility coverage

## Phase 4: Arrow Flight SQL Surface

Status: `Partial`

Goals:

- auth and session equivalence with PostgreSQL path
- metadata flows
- query submission and streaming
- prepared statement support

Outputs:

- tested Flight SQL slice
- explicit parity comparison with PostgreSQL path

Current evidence:

- prototype Flight SQL listener exists
- statement query and statement update flows are tested
- catalogs, schemas, tables, and table-types metadata flows are implemented in the prototype
- Flight SQL now supports **TLS encryption** and **Prepared Statements** (including schema planning), satisfying standard JDBC/ODBC drivers
- basic Flight SQL `SqlInfo` metadata responses are implemented and protocol-tested
- CLI-driven SQL tests now validate Flight SQL execution against a live listener
- CLI-driven paired tests now prove a narrow protocol-equivalent slice with PostgreSQL wire for non-parameterized SQL execution, requested schema routing, schema-scoped and cross-database metadata/DDL SQL statements, user-visible unknown-database/unknown-schema/missing-relation query errors, and user-visible duplicate-table-create/NOT NULL/INSERT-value-count command errors
- CLI-driven parity now also includes a broad table-driven matrix test over the current supported SQL surface for live PostgreSQL wire and Flight SQL listeners
- CLI-driven parity now also includes strict password matrix coverage for valid/invalid credential outcomes across PostgreSQL wire and Flight SQL
- control-plane catalog user records now include prototype password-rotation metadata and tests proving rotated credentials invalidate prior passwords across both protocols

Remaining gaps before this phase should be considered `Complete`:

- no prepared statements
- no broad `SqlInfo` coverage beyond a basic prototype subset
- handshake-based auth scaffold now includes prototype control-plane credential lookup and role-assumption checks, but no production credential lifecycle or full protocol auth coverage
- no broad parity proof against PostgreSQL surface

## Phase 5: Distributed Planning And Execution

Status: `Prototype`

Goals:

- scheduler
- executor model
- stage graph generation
- shuffle boundaries
- remote task execution
- cancellation and retry boundaries

Outputs:

- tested multi-node query execution
- stage-level metrics and traceability

## Phase 6: Storage Maturity

Status: `Prototype`

Goals:

- native managed columnar storage on object storage
- replication and eventual consistency behavior
- Iceberg support
- storage policy engine for native vs external choices
- cache layers for data and query results

Outputs:

- tested durable read/write paths
- documented consistency and policy behavior

## Phase 7: PostgreSQL Compatibility Expansion

Status: `Prototype`

Goals:

- broader syntax coverage
- function compatibility
- metadata compatibility
- roles and grants behavior

Outputs:

- compatibility suites
- explicit supported/unsupported matrix

## Phase 8: Operator And User Experience

Status: `Prototype`

Goals:

- CLI for PostgreSQL and Flight SQL
- timing features
- history and line editing
- Vite-based web console
- admin views for databases, users, metrics, logs

Outputs:

- usable product surfaces backed by real engine behavior

Current evidence:

- CLI now supports embedded, PostgreSQL wire, and Arrow Flight SQL query paths
- CLI timing output exists across the current prototype paths

Remaining gaps before this phase should be considered `Complete`:

- no interactive history yet
- no arrow-key line editing yet
- no richer command shell UX yet

## Phase 9: Hardening

Status: `Prototype`

Goals:

- observability end to end
- security hardening
- encryption at rest
- performance testing
- Kubernetes deployment maturity
- failure recovery validation

Outputs:

- production-readiness evidence

## Cross-Cutting Rules

- each phase should leave behind tests
- each SQL-testable feature should leave behind CLI-driven SQL coverage in the build/test path
- each phase should update `feature-status.md`
- no phase may claim `Complete` for user-visible work that only functions on one protocol
- no phase may claim `Complete` for table behavior that only functions on one storage mode
- no SQL-testable feature may be claimed successful if it fails from the CLI test client
