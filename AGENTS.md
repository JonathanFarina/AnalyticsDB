# AnalyticsDB Agent Handbook

This repository is for a massively scalable, BigQuery-style analytical database with strict separation of compute and storage, PostgreSQL and Arrow Flight SQL protocol support, columnar execution, native and external table support, Kubernetes deployment, and object storage backing.

This file is the mandatory entrypoint for any agent working in this repository. Do not start by scanning the whole repo. Read this file first, then load only the relevant focused docs listed below.

## Mandatory Rules

1. Do not invent features, compatibility, benchmarks, or production readiness.
2. Do not mark any feature as `Complete` unless it is production ready, fully tested, documented, observable, and wired end to end.
3. Do not mark any feature as `Partial` unless there is working code and at least one validating test.
4. Every feature tracker in this repo must use exactly one of these labels: `Prototype`, `Partial`, `Complete`.
5. PostgreSQL and Arrow Flight SQL are first-class equivalent interfaces. A user-visible feature is not complete if it works through only one protocol unless the feature is explicitly protocol-specific.
6. Native and external storage are first-class SQL surfaces. Do not require special user-facing query syntax just because the physical backing differs.
7. Compute and storage must remain separable. Do not introduce designs that couple query execution to node-local durable storage.
8. No feature is done without tests. No protocol behavior is done without compatibility coverage. No observability feature is done without traceability proof.
9. Any feature that can be exercised by sending SQL into the engine must be tested in the build/test process by sending SQL through the CLI.
10. If a SQL-testable feature does not succeed from the CLI test client, the feature is failed until reviewed, fixed, and re-tested successfully.
11. When a requirement is ambiguous or conflicts with the current implementation, stop and document the conflict instead of guessing.
12. When in doubt, prefer accuracy over speed and explicit proof over confident wording.

## Load Only What You Need

Always read:

- [docs/agents/project-charter.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/project-charter.md)
- [docs/agents/guardrails.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/guardrails.md)

Then load by task:

- Architecture, engine, scheduling, storage, catalog:
  [docs/agents/system-architecture.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/system-architecture.md)
- Feature scope, current status, definition of done:
  [docs/agents/feature-status.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/feature-status.md)
- Sequencing, phases, suggested build order:
  [docs/agents/workstreams.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/workstreams.md)
- Testing strategy, especially CLI-driven SQL validation:
  [docs/agents/testing-strategy.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/testing-strategy.md)

Do not reread every file by default. Load the smallest relevant set, then update the relevant document when you change scope or status.

## Project Baseline

- Working name: `AnalyticsDB`
- Product shape: distributed analytical SQL database
- Deployment shape: Kubernetes for services, object storage for durable data, single endpoint for clients
- Primary protocols: PostgreSQL wire protocol and Arrow Flight SQL
- Core execution priorities:
  - fast regular SQL execution
  - fast distributed execution on massive datasets
  - columnar storage and execution
  - native and external tables with one SQL surface
  - observability from query acceptance through result return
- Primary operator priorities:
  - horizontal scaling of compute and storage
  - replication and eventual consistency
  - routing based on node utilization
  - optional encryption at rest
  - dynamic caching

## Recommended Technology Baseline

These are the repository defaults unless replaced by a documented architecture decision:

- Engine language: Rust
- Execution core: Apache Arrow + Apache DataFusion, with distributed execution inspired by or built on Ballista patterns
- Wire protocols:
  - PostgreSQL-compatible frontend
  - Arrow Flight SQL frontend
- Web console: TypeScript + Vite
- Deployment target: Kubernetes
- Durable storage: object storage plus replicated metadata/catalog services

Important: this baseline is a recommendation, not proof that upstream projects already satisfy all required PostgreSQL compatibility needs. Agents must not assume DataFusion or any other engine already delivers full PostgreSQL syntax, function, metadata, schema, or role parity.

## Current Truth

The repository now contains a real prototype foundation. Today that means:

- a buildable Rust workspace
- a control-plane skeleton with bootstrap node metadata, users, databases, schemas, query ID generation, and optional JSON-backed catalog persistence
- a prototype single-process SQL execution path built on DataFusion
- a protocol crate and prototype server binary for PostgreSQL wire and Arrow Flight SQL listeners
- a CLI that can submit SQL in embedded mode and can also act as a PostgreSQL wire or Arrow Flight SQL client
- a small metadata SQL subset for databases, schemas, views, and prototype `ALTER USER ... PASSWORD ...` rotation
- a managed-table prototype for `CREATE TABLE ... AS SELECT ...` backed by directories of native Parquet files
- a managed-table prototype for explicit `CREATE TABLE (...)` definitions and `INSERT INTO ... VALUES ...` writes backed by the same native Parquet directories
- persisted views that can be created through SQL and queried later through the CLI in embedded mode
- persisted managed tables that can be materialized through SQL and queried later through the CLI in embedded mode
- persisted managed tables that can be defined, inserted into, updated via `DELETE`/`TRUNCATE`, introspected, and queried later through the CLI in embedded mode
- current managed-table inserts support whole-row values plus column-list value insertion for the tested embedded prototype subset
- current metadata listing supports schema-scoped `SHOW TABLES FROM ...` and `SHOW VIEWS FROM ...` for the tested embedded prototype subset
- persisted managed tables that can describe their columns through SQL in later CLI sessions
- a PostgreSQL wire prototype that supports connection startup, simple queries, and a tested parameterized extended-query subset against the current engine path
- an Arrow Flight SQL prototype that supports statement query, statement update, **prepared statements** (with schema planning), **TLS encryption**, and basic metadata discovery for catalogs, schemas, tables, and table types
- integrated **structured logging and tracing** via the `tracing` crate, with `RUST_LOG` support
- a prototype Vite TypeScript web admin console with a local UI harness for query editing, database/schema/table/view exploration, result grids, engine messages, query IDs, and timing cards
- build/test automation that verifies current SQL behavior through the CLI

Everything beyond that remains early-stage. In particular:

- no distributed execution exists yet
- no native managed storage exists yet (currently local Parquet directories)
- no external Iceberg table path exists yet
- no web console execution path against a live AnalyticsDB web gateway exists yet
- no Kubernetes deployment assets exist yet
- no benchmark claims are valid yet
- no broad PostgreSQL compatibility claims are valid yet
- no broad PostgreSQL extended query compatibility support exists yet
- PostgreSQL startup and Flight SQL handshake now share a prototype auth-hook bootstrap with control-plane user lookup and bootstrap catalog passwords, but no production credential storage/rotation policy or full protocol auth coverage exists yet
- prototype role-assumption checks now exist at session admission, but no full role/group authorization model exists yet
- Flight SQL now supports the full bind/execute/close protocol cycle for prepared statements, enabling JDBC/ODBC connectivity
- broad Flight SQL `SqlInfo` coverage exists for core server identification and SQL dialect metadata
- prototype role-assumption is implemented at session admission; full role/group authorization model (GRANT/REVOKE hierarchy, row-level security) is not yet complete
- columnar managed-table storage uses local `.managed` Parquet directories; production object-storage backing (S3/GCS/Azure) is wired via `object_store` but not yet the default deployment target

## Required Behaviors For Agents

- Keep docs current when you change architecture, scope, or status.
- Make state transitions explicit. If a feature moves from `Prototype` to `Partial`, say what evidence justified that move.
- Preserve the product requirements even when making expedient prototypes.
- Do not simplify away distributed design constraints unless the task explicitly authorizes a temporary scaffold.
- Prefer additive scaffolding over misleading completeness.

## Document Ownership

- Product intent and hard requirements:
  [docs/agents/project-charter.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/project-charter.md)
- Architecture and component boundaries:
  [docs/agents/system-architecture.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/system-architecture.md)
- Status tracking and definition of done:
  [docs/agents/feature-status.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/feature-status.md)
- Execution discipline and anti-hallucination rules:
  [docs/agents/guardrails.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/guardrails.md)
- Suggested implementation sequence:
  [docs/agents/workstreams.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/workstreams.md)
- SQL validation discipline:
  [docs/agents/testing-strategy.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/testing-strategy.md)
