# ADR 002: Catalog Backing Store

**Status:** Decided  
**Blocks:** Phase B (manifest commit protocol), Phase H (Kubernetes deployment)

## Decision

Use **embedded Raft via `openraft` + `RocksDB`** as the durable backing store
for the control-plane catalog (databases, schemas, tables, users, cluster
membership, query admission state, and storage policy).

Each coordinator node participates in the Raft group. Writes go through the
leader; reads may be served from any follower with a bounded staleness window
for catalog reads, and from the leader only for admission-control decisions
that require linearizability (e.g. port assignment, manifest commits).

## Why

The project requires fully autonomous cluster deployments with no external
dependency. An external Postgres would be a simpler initial implementation but
adds an infrastructure dependency that every operator must manage separately,
and it becomes a single point of failure unless itself made HA.

Embedded Raft keeps the cluster self-contained: add nodes, and the catalog
scales and replicates automatically with the cluster. The `openraft` crate is
production-hardened and `RocksDB` via `rocksdb-rs` is the standard embedded
durable store in the Rust ecosystem.

## Alternatives Rejected

- **External Postgres:** Simpler initial implementation, but adds an
  operational dependency, a separate failure domain, and a bootstrap chicken-
  and-egg problem (you need PG to start AnalyticsDB). Revisit if the Raft
  implementation proves disproportionately complex to maintain.
- **etcd:** Same autonomous-cluster benefit as Raft but requires running a
  separate etcd process. Does not simplify the deployment model.
- **In-process SQLite (current state):** Works for single-coordinator
  prototype; not replicated, not HA, cannot survive coordinator loss.

## Consequences

- Phase H Helm chart provisions a Raft quorum (minimum 3 coordinator
  replicas) rather than an external database service.
- The control-plane crate (`analyticsdb-control`) needs a new `catalog`
  module that wraps `openraft` + `RocksDB` behind the same trait surface
  the JSON-backed catalog currently implements, so the engine crate sees no
  change.
- A Raft snapshot + log compaction strategy is required before Phase H is
  `Complete`, to bound RocksDB growth over time.
- Local development (`file://` catalog) keeps the current JSON-backed path
  until the Raft implementation is stable, then migrates.
- A catalog export/import tool (backup/restore) must be built alongside the
  Raft implementation for Phase H.
- The Raft group size must be configurable (default: 3 for HA,
  1 for local dev/test).

## Implementation Sketch

```
analyticsdb-control/
  src/
    catalog/
      mod.rs          — trait CatalogStore { ... }
      json.rs         — current JSON implementation (kept for dev/test)
      raft/
        mod.rs        — openraft state machine
        storage.rs    — RocksDB log + snapshot adapter
        client.rs     — leader-forwarding client for follower nodes
```

The trait surface isolates the engine from the backing implementation.
Raft-specific concerns (leader election, log shipping, snapshot) live entirely
within `analyticsdb-control`.
