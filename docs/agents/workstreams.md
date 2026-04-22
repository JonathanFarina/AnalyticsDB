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
- bootstrap node metadata exists
- query admission now issues query ids before execution
- bootstrap users, databases, and schemas are validated on the query path
- JSON-backed catalog persistence exists for databases and schemas
- metadata SQL subset exists for creating and listing databases, schemas, and views
- persisted views can be created and queried in later CLI sessions
- prototype managed tables can be materialized with CTAS and queried in later CLI sessions
- prototype managed tables can also be defined with explicit columns and populated through `INSERT INTO ... VALUES ...`
- prototype managed-table inserts now support tested column-list insertion with omitted nullable columns
- prototype metadata listing now supports tested schema-scoped table and view discovery
- prototype managed table snapshots are now column-oriented on disk
- prototype managed tables now expose persisted column metadata through SQL introspection

Remaining gaps before this phase should be considered `Partial` overall:

- no object-storage-backed production columnar managed-table storage yet
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
- CLI-driven SQL tests now validate PostgreSQL wire execution against a live listener

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
- CLI-driven SQL tests now validate Flight SQL execution against a live listener

Remaining gaps before this phase should be considered `Complete`:

- no prepared statements
- no `SqlInfo` support
- no handshake-based auth
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
