# Project Charter

## Mission

Build a massively scalable analytical database in the style of BigQuery, with independent compute and storage layers, distributed execution, object storage support, PostgreSQL and Arrow Flight SQL access, and a first-class operator and user experience.

## Product Requirements

The database must support:

- independent horizontal scaling for compute nodes and storage nodes
- Kubernetes deployment for services
- object storage deployment for durable data
- equivalent PostgreSQL and Arrow Flight SQL protocols
- PostgreSQL-compatible SQL surface, metadata, schemas, and user/group structures
- lightning-fast regular SQL execution
- lightning-fast distributed execution across massive datasets
- columnar storage
- native tables and views
- external table access for formats such as Parquet and Iceberg
- automatic physical storage choice between native and external strategies based on data size and policy
- one SQL surface regardless of physical storage backing
- replication with eventual consistency
- routing based on node utilization
- a single client endpoint across many nodes
- optional encryption at rest
- dynamic query and data caching
- optimizer support for distributed sharding and SQL optimization
- full logging and traceability for each query lifecycle

## User-Facing Surfaces

### CLI

The CLI must support both PostgreSQL and Arrow Flight SQL and include:

- speed measurement capabilities
- arrow-key command line editing
- persistent command history

### Web Console

The web console must include:

- a query console similar to BigQuery
- a database explorer showing databases, schemas, tables, and views
- a results grid showing returned data and engine messages
- execution time on every query result

When logged in as an administrator, the web console must additionally include:

- database administration
- user administration
- system metrics
- log exploration across nodes

## Non-Negotiable Product Invariants

1. Compute and storage stay decoupled.
2. PostgreSQL and Arrow Flight SQL are peer interfaces.
3. Native and external storage are peer data access modes behind one SQL surface.
4. Single-endpoint connectivity is mandatory.
5. Traceability from request acceptance to result delivery is mandatory.
6. Any feature that can be validated by submitting SQL to the engine must be covered by build/test automation that submits SQL through the CLI.
7. A SQL-testable feature is failed if it does not work from the CLI test client, regardless of lower-level test success.
8. No feature is considered done without tests.
9. Status tracking is mandatory and must use `Prototype`, `Partial`, or `Complete`.

## Definitions

- `Prototype`: exploratory scaffold, incomplete behavior, not production ready
- `Partial`: some working capability with tests, but missing correctness, scale, hardening, parity, or operability
- `Complete`: production-ready implementation with tests, documentation, observability, and operational readiness

## Hard Truths Agents Must Preserve

- Full PostgreSQL compatibility is a major workstream, not a checkbox.
- Supporting both PostgreSQL and Arrow Flight SQL means metadata, auth, and result semantics must remain aligned.
- External formats such as Parquet and Iceberg are part of the core product, not bolt-ons.
- A fast single-node prototype is useful, but it is not the target architecture.
- A distributed query plan without observability is incomplete.
- A UI without engine messages, timing, and admin paths is incomplete.

## What Agents Must Do When Requirements Clash

If an implementation shortcut conflicts with the charter:

1. keep the charter as the source of truth
2. document the shortcut clearly as temporary
3. keep the feature status at `Prototype` or `Partial`
4. do not imply the shortcut satisfies the final requirement
