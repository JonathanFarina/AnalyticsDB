# System Architecture

## Architecture Summary

The target system is a distributed SQL database with:

- a protocol edge layer for PostgreSQL and Arrow Flight SQL
- a control plane for routing, catalog, auth, metadata, and scheduling
- a compute plane for query planning and execution
- a storage plane for native columnar data, external table access, caching, and replication
- observability services for logs, metrics, traces, and auditability
- operator and developer surfaces including CLI and web console

The design must preserve a strict separation between compute and durable storage.

## Recommended Baseline Stack

These are the current recommended defaults for this repository:

- Engine/runtime language: Rust
- In-memory columnar model: Apache Arrow
- Query execution foundation: Apache DataFusion
- Distributed execution foundation: Ballista-style scheduler/executor model
- Flight SQL support: Apache Arrow Flight SQL
- Web console: TypeScript + Vite
- Deployment target: Kubernetes
- Durable data: object storage

## Why This Baseline

This recommendation is based on current upstream documentation reviewed on 2026-04-21:

- Apache DataFusion documents itself as an extensible Rust query engine using Arrow, with SQL support, vectorized multithreaded execution, and built-in support for file formats including Parquet.
- Apache DataFusion Ballista documents a distributed scheduler/executor architecture that is deployable as native binaries or containers and includes Flight SQL support in its ecosystem.
- Apache Arrow Flight SQL documents metadata, prepared statement, query, and ingest flows over the Flight protocol.
- Rust officially supports building native binaries across major targets including macOS, Linux, and Windows.
- Vite remains a solid baseline for a modern web console.

Reference sources:

- [Apache DataFusion](https://datafusion.apache.org/)
- [Apache DataFusion Ballista Overview](https://datafusion.apache.org/ballista/user-guide/introduction.html)
- [Apache DataFusion Ballista Architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html)
- [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
- [Rust Platform Support](https://doc.rust-lang.org/rustc/platform-support.html)
- [Vite Guide](https://vite.dev/guide/)
- [Apache Iceberg](https://iceberg.apache.org/)

## Critical Caveat

Do not treat the recommended baseline as proof of full product fit.

In particular:

- DataFusion is not automatically equivalent to PostgreSQL semantics.
- Ballista-style distributed execution does not automatically satisfy all product requirements around routing, caching, replication, security, or operability.
- Iceberg and Parquet support must be integrated behind the product catalog and SQL surface, not exposed as a second-class path.

## Target Component Model

### 1. Edge Layer

Responsibilities:

- terminate PostgreSQL wire protocol connections
- terminate Arrow Flight SQL connections
- authenticate users
- create sessions
- attach trace identifiers
- normalize request metadata
- route requests to the appropriate coordinator

Rules:

- PostgreSQL and Flight SQL must land in the same logical session model
- PostgreSQL and Flight SQL adapters must consume the shared engine statement outcome contract for row-returning SQL versus command SQL; protocol-local string-prefix classification and affected-row scraping are not architectural sources of truth
- Flight SQL row-returning paths should stream from the shared engine row stream and must not re-plan solely because a client moves from `GetFlightInfo` to `DoGet`
- Flight SQL listeners must serve TLS whenever certificate/key material is configured, including dynamically joined nodes; node-to-node distributed execution uses a dedicated internal gRPC/Flight channel registered separately from the client-facing Flight SQL endpoint
- differences in wire protocol must not create different product capabilities
- error codes, metadata visibility, and auth semantics must be intentionally mapped and tested

### 2. Control Plane

Responsibilities:

- cluster membership
- node health
- query admission control
- query routing based on utilization
- logical catalog and namespace management
- user, role, and group management
- metadata authority for tables, views, statistics, and storage policies
- distributed query scheduling
- replication and consistency orchestration

Rules:

- the control plane owns truth for metadata and policy
- single-endpoint behavior is implemented here, even if exposed through a gateway or load balancer
- every query must receive a durable query identifier before execution begins

### 3. Compute Plane

Responsibilities:

- SQL parsing and semantic analysis
- PostgreSQL compatibility layer
- logical optimization
- physical planning
- distributed stage planning
- execution against native and external sources
- intermediate shuffle and aggregation
- result streaming to both protocols

Rules:

- the compute plane may cache aggressively, but may not become the durable system of record
- single-node prototype session contexts may cache DataFusion catalog/table registration per logical session, but catalog-changing commands must invalidate that cache
- a single-node fallback mode is acceptable for prototype work, but must not redefine the target architecture
- distributed plans must expose stage-level metrics and trace links

### 4. Storage Plane

Responsibilities:

- native columnar table storage using Parquet (local directory prototype)
- managed-table indexes are maintained as versioned sidecar manifests beside managed Parquet directories, with managed-table storage locations persisted as `file://` URIs; this is a production-capable acceleration path for equality, `IN`, and bounded range lookup today; the long-term architecture will extend this to remote object-storage index locations
- external table abstraction for Parquet and Iceberg
- replication workflow and durability policy
- metadata-backed snapshot management
- cache warming and eviction
- optional encryption at rest

Rules:

- native and external tables must share one SQL surface
- native storage uses Parquet for high-performance columnar scanning
- prototype managed-table bulk materialization should use DataFusion Parquet sinks instead of bespoke serial writers where possible
- storage policy selection may be automatic, but must always be inspectable
- node-local disks may be used for cache or spill, not as the primary durable database store

### 5. Observability Plane

Responsibilities:

- query lifecycle logging using the `tracing` crate
- durable query-log records exposed through `system.query_log`
- distributed tracing and correlation
- metrics emission
- audit logging for auth and DDL
- operator-facing diagnostics

Rules:

- no query path is complete without trace identifiers and structured logs
- logs must be attributable to query id, user, protocol, and node
- query-log writes must stay off the hot execution path; current implementation pushes records to an in-memory channel and a background writer persists Parquet files under the catalog-managed system directory
- the current `system.query_log` implementation is `Partial`: it is SQL-queryable and durable for the non-streaming path, and worker sibling rows (`is_initial_query = false`) are now written for the distributed read path; streaming Flight SQL finish accounting, DataFusion stage metric enrichment, retention sweeping, partitioned layout, and benchmark gates remain required before any production-ready claim
- admin UI requirements depend on this plane

## PostgreSQL Compatibility Strategy

PostgreSQL compatibility is a product commitment, not a parser setting.

Agents must design for:

- PostgreSQL dialect parsing and syntax coverage
- PostgreSQL-compatible built-in functions and type behavior
- PostgreSQL-like catalogs and information schema exposure where promised
- PostgreSQL-style schemas, databases, users, groups, and roles
- compatibility testing against real PostgreSQL expectations for supported features

If the engine core diverges from PostgreSQL behavior:

- document the gap
- keep the feature below `Complete`
- add the gap to status tracking

## Storage Strategy

The product has two user-visible logical table categories:

- native managed tables/views
- external tables/views over formats such as Parquet and Iceberg

The end user should not need special query syntax to receive first-class behavior from either.

The system may choose a preferred physical backing by policy based on:

- data volume
- mutability pattern
- latency profile
- cost profile
- retention needs
- interoperability needs

Policy decisions must be visible through metadata and observability.

## Deployment Model

Target deployment:

- protocol gateway and control-plane services in Kubernetes
- elastic compute executors in Kubernetes
- durable table data in object storage
- replicated metadata and state services managed separately from compute executors

Avoid:

- designs that require sticky routing to the same compute node for correctness
- durable dependence on executor-local state
- one-off protocol behavior in a single node that cannot scale horizontally
