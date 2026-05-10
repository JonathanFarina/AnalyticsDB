# Catalogue System Improvements

Tracking the rework of the cluster catalogue from a single JSON file rewritten on every change into a robust, versioned, SQLite-backed store with active sync to compute nodes.

## Goals

1. Eliminate write amplification from heartbeat traffic.
2. Replace full-file JSON rewrites with partial, ACID writes.
3. Give compute nodes an authoritative way to know their cached schema is stale.
4. Keep JSON as a human-readable export, not the live write path.

## Non-Goals

- Distributed consensus (etcd/Raft). Control node remains the single writer.
- Sharding the catalogue. It stays centralized.
- Replacing the in-memory `RwLock<CatalogState>`. That structure is fine.

## Current State (baseline)

- `cluster-catalog.json` is the single source of truth on disk.
- `ControlPlane::persist()` rewrites the whole file on every DDL **and on every heartbeat**.
- Compute nodes read the catalogue once at join time. Nothing actively syncs them after that.
- All catalogue mutations contend on a single `RwLock<CatalogState>`.

## Phases

### Phase 1 — Decouple heartbeats from catalogue persistence

**Problem:** Heartbeats hold the catalogue write lock and trigger full JSON rewrites.

**Change:**
- Move node liveness (`last_heartbeat_at_epoch_ms`, `status`) out of `CatalogState` and into a separate in-memory `RwLock<HashMap<NodeId, NodeLiveness>>` on the control plane.
- The `nodes` map in `CatalogState` keeps identity/role/endpoint (durable). Liveness becomes ephemeral, reconstructed from heartbeats after restart.
- `heartbeat()` only touches the liveness map. No `persist()` call.
- `cluster_snapshot()` joins identity + liveness when serving reads.

**Status:** [x] complete

Implementation summary:
- Added private `NodeLiveness` struct (status + last_heartbeat_at_epoch_ms).
- Added `liveness: RwLock<HashMap<String, NodeLiveness>>` to `ControlPlane`.
- `heartbeat()` now only mutates the liveness map. No catalogue write lock, no `persist()`.
- `mark_node_unavailable()` and `prune_unhealthy_nodes()` operate on the liveness map.
- `register_node()`, `join_cluster()` seed the liveness map.
- `clear_compute_nodes()` removes from both maps.
- `cluster_snapshot()` and `list_nodes()` overlay liveness onto persisted node identity via `apply_liveness()`. Nodes that have not heartbeated since the last control-plane restart report as `Unavailable`.
- New regression test `heartbeat_does_not_rewrite_catalog_file` asserts the catalogue file mtime is unchanged after 50 heartbeats.

### Phase 2 — SQLite-backed catalogue store

**Problem:** Every DDL rewrites the entire catalogue file. No partial updates, no crash safety.

**Change:**
- Introduce a `CatalogStore` trait with `load() / save_database() / delete_database() / save_relation() / delete_relation() / ...` methods.
- Implement `JsonCatalogStore` (existing behavior) and `SqliteCatalogStore` (new).
- SQLite uses WAL mode (`PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`).
- Schema: one table per catalogue entity (`databases`, `schemas`, `users`, `nodes`, `relations`, `columns`, `constraints`, `indexes`, `functions`, `config`).
- Selection via config flag `catalog_backend: "json" | "sqlite"`. Default to `sqlite` once validated.
- Migration: `migrate_json_to_sqlite()` helper that loads JSON, writes SQLite, leaves JSON untouched.

**Status:** [x] complete

Implementation summary:
- Added `rusqlite = { version = "0.32", features = ["bundled"] }` to workspace and the control crate.
- New module `crates/analyticsdb-control/src/catalog_store.rs` defining:
  - `trait CatalogStore` with `load()` / `save_state()`.
  - `JsonCatalogStore` — legacy file-per-state JSON.
  - `SqliteCatalogStore` — WAL-mode SQLite with one row per entity (databases, users, nodes, relations, functions, aggregates, collations, conversions). Config in a `meta` table. Saves are wrapped in a single transaction (atomic, crash-safe). Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`.
  - `open_store(path)` — selects the backend by file extension (`.db`/`.sqlite`/`.sqlite3` → SQLite, otherwise → JSON).
- `CatalogState` and its fields elevated to `pub(crate)` so the sibling module can construct them.
- `ControlPlane::from_catalog_path` and `persist_state` now route through the trait.
- New `ControlPlane::export_json(path)` — explicit JSON dump for debugging/backups regardless of live backend.
- Tests added: `sqlite_store_roundtrip`, `sqlite_store_overwrites_removed_entries`, `open_store_picks_backend_by_extension`. All 6 control tests pass.

Note: this phase keeps full-state save semantics (every `persist()` writes all rows). The win is ACID transactions, crash safety, and WAL concurrency — partial-update methods (`save_relation()` etc.) are a follow-up that fits cleanly on this foundation.

### Phase 3 — Catalogue versioning + push invalidation

**Problem:** Compute nodes' cached snapshots can silently drift from the control plane.

**Change:**
- Add `catalogue_version: u64` to `CatalogState` and persist it.
- Increment on every successful DDL write.
- After each write, fire-and-forget `POST /internal/catalogue-changed { new_version }` to every registered compute node.
- Compute nodes cache the snapshot tagged with version; on receiving a higher version, refetch via `cluster_snapshot()`.
- `cluster_snapshot()` response includes the version so a node can short-circuit if already current.
- Add a `If-None-Match`-style version check on every snapshot fetch to make polling cheap.

**Status:** [x] foundation complete; network wiring deferred

Implementation summary (in-process foundation):
- Added `catalogue_version: u64` to `CatalogState` (serde-default for forward compat) and to `ClusterSnapshot`.
- Added `version_tx: tokio::sync::watch::Sender<u64>` to `ControlPlane`. Each successful `persist()` bumps the version inside the catalogue write lock (so concurrent persists serialize and each get a unique, monotonically increasing version), saves, then broadcasts the new version on the watch channel.
- New public APIs:
  - `ControlPlane::catalogue_version() -> u64`
  - `ControlPlane::subscribe_catalogue_version() -> watch::Receiver<u64>`
- `ClusterSnapshot.catalogue_version` lets remote callers short-circuit a refetch when their cached version matches.
- SQLite store persists the version in the `meta` table; JSON store includes it in the serialized state.
- Tests added: `catalogue_version_increments_on_persisted_writes` (also asserts heartbeats do NOT bump version), `catalogue_version_survives_reload`.

**Deferred to a follow-up — network wiring:** Pushing invalidations from the control node to compute nodes' internal endpoints crosses the engine + server crates and the existing tonic / Arrow Flight transport. The cleanest integration point is to spawn a task in the server's main loop that calls `subscribe_catalogue_version()` and forwards each new version over a new internal RPC. The hook is in place — wiring it up should be its own focused change with its own proto definition.

### Phase 4 — Demote JSON to export-only

**Problem:** Once SQLite is the live path, JSON should not be silently maintained on every write.

**Change:**
- Remove `persist_json` calls from the hot path.
- Keep an explicit `export_catalogue(path)` function (CLI / RPC) for backups and debugging.
- Document the export workflow in `improvement.md`.

**Status:** [x] complete (defaults preserved, migration path provided)

Implementation summary:
- Added `ControlPlane::export_json(path)` (Phase 2) — explicit JSON dump regardless of live backend.
- Added `ControlPlane::migrate_json_to_sqlite(json_path, sqlite_path)` and the underlying `catalog_store::migrate_json_to_sqlite()` helper.
- Test added: `migrate_json_to_sqlite_preserves_state` confirms a JSON catalogue migrates 1:1 to SQLite.
- **Default path NOT changed.** `DEFAULT_CATALOG_PATH` stays `analyticsdb-catalog.json` because changing it would silently break existing deployments and CI configs that hardcode the legacy filename. Operators opt in by pointing `--catalog-path` at a `.db` / `.sqlite` / `.sqlite3` file. Flipping the default belongs in its own change with a release note.

**Migration workflow for an existing JSON deployment:**

```rust
// One-shot, offline:
ControlPlane::migrate_json_to_sqlite(
    "analyticsdb-catalog.json",
    "analyticsdb-catalog.db",
).await?;
```

Then update `--catalog-path` (CLI / server flags / `cluster-config.json`) to point at the new `.db` file. The original JSON file is left untouched as a backup.

**Export workflow (any live backend → JSON for inspection):**

```rust
control_plane.export_json("/tmp/catalog-snapshot.json").await?;
```

## Verification

- `cargo check --workspace` — clean.
- `cargo build --workspace` — clean.
- `cargo test --workspace --lib --no-fail-fast` — results:
  - `analyticsdb-control`: 9/9 pass (added 6 new tests across the four phases).
  - `analyticsdb-protocol`: 23/23 pass.
  - `analyticsdb-engine`: 32/33 pass — the single failure is the pre-existing flaky `concurrent_primary_key_inserts_keep_table_and_index_consistent`, confirmed broken on the baseline branch (`b4f0343`) before any of these changes. Unrelated to catalogue work.
- Manual smoke (recommended for a follow-up PR): spin up control + compute, run a `CREATE TABLE`, observe `catalogue_version()` bumping and confirm the watch receiver fires.

## Notes / Decisions

(append-only log of decisions made during implementation)

- **Pre-existing failing test (not caused by this work):** `analyticsdb-engine::tests::concurrent_primary_key_inserts_keep_table_and_index_consistent` fails on the baseline branch (`b4f0343`) as well — verified by stashing changes and re-running. Both inserts succeed when only one should. Worth filing as a separate issue.
- **Phase 1 design choice:** Liveness map starts empty after a control-plane restart. Nodes report `Unavailable` until they heartbeat. This is intentional — after a restart the control plane has no proof a node is alive, and the persisted timestamp is stale by definition. Compute nodes are expected to heartbeat shortly after the control plane comes up.
- **Phase 1 design choice:** Kept the `last_heartbeat_at_epoch_ms` field on `ClusterNode` for serde compatibility with the existing JSON file. The persisted value is now ignored — `apply_liveness()` overwrites it on every read. We can drop the field entirely in a later cleanup pass.
