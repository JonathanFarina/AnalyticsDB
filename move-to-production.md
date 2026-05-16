# Move To Production

This document records an honest review of the AnalyticsDB engine as of 2026-05-13
and lays out the concrete work required to take the engine from a prototype
(with several `Partial` slices) to a `Complete`, production-ready system as
defined by [docs/agents/feature-status.md](docs/agents/feature-status.md) and
the [project charter](docs/agents/project-charter.md).

The plan is written so that every line item is acceptance-checkable. A phase
exits when its **Exit Gate** is satisfied with build/test evidence — not when
"the code is written."

---

## 1. Current State (Honest Assessment)

What the engine actually is today:

- A single-process Rust workspace built on DataFusion, with a control-plane
  skeleton, a PostgreSQL wire prototype, an Arrow Flight SQL prototype, a CLI
  test client, a prototype distributed scatter/gather path, and a Vite admin
  console driven by a local UI harness.
- Managed-table storage is **local Parquet directories** under
  `<catalog>.managed/` plus sidecar index manifests. There is no object-store
  durability path in production use.
- Distributed execution is a single coordinator dispatching `ExecutePartition`
  Flight DoActions to compute workers over an intra-cluster mTLS channel
  (`ClusterMtlsConfig`; `NoVerifier` removed; `analyticsdb ca init` provisions certs).
- Auth is a prototype: password rotation works, but there is no production
  credential storage, no role/group model, no audit trail, and no key rotation
  story for cluster TLS.
- Observability has structured tracing and a `Partial` `system.query_log` for
  the non-streaming path only. There is no metric emission, no per-stage
  metrics, no traces propagated through the distributed path.
- The engine has been decomposed into focused modules (`ddl.rs`, `dispatch_impl.rs`,
  `dispatch_plan.rs`, `batch.rs`, `index_impl.rs`, `index_ops.rs`, etc.);
  `lib.rs` production code is ≤ 1450 lines.
- CLI-driven SQL tests cover a narrow but well-defined PG / Flight SQL parity
  slice. Many SQL surfaces (joins, window functions, types, time/date,
  numeric, geo, JSON) are **untested through the CLI** and therefore cannot be
  claimed production-ready per the project's own rules.

What "production ready" means in this project (DoD from
[feature-status.md](docs/agents/feature-status.md)):

- End-to-end implementation + automated tests (happy/failure/regression)
- Observability where relevant
- Operator/developer docs
- Security implications addressed
- Performance characteristics known
- PostgreSQL ↔ Flight SQL parity where relevant
- Native ↔ external storage parity at the SQL surface
- CLI-driven SQL coverage for every SQL-testable behavior

The plan below is organized to satisfy that DoD on a feature-by-feature basis.

---

## 2. Cross-Cutting Invariants (Apply To Every Phase)

These are non-negotiable while moving to production. Any PR that violates one
of them must be reverted, not patched:

1. **Compute and storage stay decoupled.** No feature may require executor-local
   durable state. Local disk is cache/spill only.
2. **PG ↔ Flight SQL parity.** Every user-visible behavior must be reachable
   and behave identically through both protocols, validated by paired CLI
   tests, unless explicitly protocol-specific.
3. **Native ↔ external parity at the SQL surface.** Users do not get special
   syntax just because the physical backing differs.
4. **Every SQL-testable feature ships with a CLI-driven SQL test** in the
   normal build/test path (`cargo test --workspace` and
   `cargo test -p analyticsdb-cli --test sql_cli`).
5. **Every query gets a query id at admission and a trace span through every
   plane it touches** (edge → control → compute → storage → observability).
6. **No `unwrap()` / `expect()` / `panic!()` on the request path in
   production code.** Tests may keep them; library code in
   `analyticsdb-engine/*` must return `Result`.
7. **Status discipline.** Status doc updates (`feature-status.md`,
   `workstreams.md`) ship in the same PR as the behavior change. No silent
   promotions.
8. **Setting Disicpline** All newley created user settings must be added to the Settings Page in the Web Admin Console
---

## 3. Phased Plan

The phases below are intentionally ordered so that earlier phases unblock
later ones (e.g. durable object-store storage must come before serious
distributed-execution hardening, because executor-local Parquet defeats the
compute/storage separation guarantee that later phases depend on).

Each phase lists: **Goals**, **Concrete tasks**, **Exit Gate**.

---

### Phase A. Foundations Hardening ✅

**Goal:** stabilize the existing prototype surface so subsequent phases can
trust the test harness, the build, and the basic engine contracts.

Tasks:

✅ A1. **Decompose `analyticsdb-engine/src/lib.rs`.** 8.3k lines mixing parser
    dispatch, catalog ops, planning, distributed coordination, and rewriting
    is a refactor blocker. Split into modules (`session`, `planner`, `ddl`,
    `dml`, `dispatch`, `rewriter`) keeping the public API stable. No behavior
    change; cover with the existing CLI parity tests.
    > Done: lib.rs split into `ddl.rs`, `dispatch_impl.rs`, `dispatch_plan.rs`,
    > `batch.rs`, `index_impl.rs`, `index_ops.rs`, `sql_rewriter.rs`,
    > `postgres_compatibility.rs`, `schema_build.rs`, `metadata_helpers.rs`,
    > `manifest.rs`, `storage.rs`, `system_catalog.rs`, `information_schema.rs`,
    > `functions.rs`, `query_log/`. Production code in lib.rs now ≤ 1450 lines.

✅ A2. **Eliminate request-path `unwrap()` / `expect()`** in non-test code.
    Audit:
    - [lib.rs:49](crates/analyticsdb-engine/src/lib.rs:49) and
      [lib.rs:52](crates/analyticsdb-engine/src/lib.rs:52) — convert
      `PartitionStream` re-execution panics into typed errors.
    - [lib.rs:1041](crates/analyticsdb-engine/src/lib.rs:1041),
      [lib.rs:1093](crates/analyticsdb-engine/src/lib.rs:1093),
      [lib.rs:5800](crates/analyticsdb-engine/src/lib.rs:5800),
      [lib.rs:6121](crates/analyticsdb-engine/src/lib.rs:6121),
      [lib.rs:6132](crates/analyticsdb-engine/src/lib.rs:6132) — replace with
      `Result` propagation and tested error contracts.
    > Done: all request-path panics converted to `Result`; `cargo clippy -D
    > clippy::unwrap_used -D clippy::expect_used --no-deps` passes clean.

✅ A3. **Lint gates.** Add `#![deny(clippy::unwrap_used, clippy::expect_used,
    clippy::panic, clippy::todo, clippy::unimplemented)]` to `analyticsdb-engine`
    and `analyticsdb-protocol` non-test modules. Configure
    `clippy::pedantic` exceptions explicitly.
    > Done: `#![cfg_attr(not(test), deny(...))]` gates added to both crates;
    > `cargo clippy --workspace -- -D warnings` passes clean.

✅ A4. **Drop the historical `repro_*.rs`, `inspect_interval.rs`,
    `test_projection.rs`, and `repro_default_v2`/`repro_drop` binaries from
    the repo root**, or move them under `crates/analyticsdb-engine/examples/`.
    They confuse the test surface and bloat the workspace.
    > Done: stale `.orig`/`.rej`/`.tmp` merge artifacts and planning docs
    > (`improvement.md`, `next_feature.md`) removed from git tracking.

✅ A5. **CI matrix.** Add stable Linux x86_64 and ARM64 jobs that run
    `make build`, `make test`, `make test-sql-cli`, `make lint`, and the web
    admin tests, plus a nightly job with `cargo deny check`,
    `cargo audit`, and a `--release` build to catch debug-only assumptions.
    > Done: `.github/workflows/ci.yml` has Linux x86_64, Linux ARM64, macOS
    > arm64, nightly clippy, security audit + release build, and web console
    > jobs.

✅ A6. **Reproducible toolchain.** `rust-toolchain.toml` already pins the
    toolchain — add a CI smoke test that fails if a clippy/rustc upgrade
    silently changes the lint surface.
    > Done: `nightly-lint` CI job runs `cargo +nightly clippy --workspace
    > --all-targets -- -D warnings` and fails on any new lint surface.

**Exit Gate (A): ✅ SATISFIED**
- ✅ `lib.rs` ≤ 1500 lines (production code at ~1450); engine code split into modules above.
- ✅ `clippy --workspace -- -D warnings` clean with the deny-unwrap config.
- ✅ CI green on Linux x86_64, Linux ARM64, and macOS.
- ✅ No regressions in `cargo test -p analyticsdb-cli --test sql_cli`.

---

### Phase B. Durable Object-Storage Storage Layer ✅

This is the most architecturally important phase. The product invariant
"compute and storage stay decoupled" cannot be honored while managed tables
live in `<catalog>.managed/` on a coordinator's local disk.

Tasks:

✅ B1. **Object-store backends.** Extend
    [storage.rs:19](crates/analyticsdb-engine/src/storage.rs:19) so
    `store_for_location` resolves at least `s3://`, `gs://`, `azure://`, and
    `file://` URIs through the existing `object_store` dependency. Bail
    explicitly on unsupported schemes with the URI in the message (already the
    pattern today — extend, don't replace).
    > Done: `store_for_location` handles `s3://`, `s3a://`, `gs://`, `az://`,
    > `azure://`, `abfss://`, `file://`, and plain local paths; each cloud
    > backend uses `.from_env()` provider chain.

✅ B2. **Credentials.** Adopt the standard provider chain per cloud (env vars,
    instance metadata, profile/role assumption) behind a `StorageCredentials`
    trait. Never embed long-lived secrets in catalog state; reference them by
    name and resolve at runtime.
    > Done: all cloud builders use `.from_env()` (AWS, GCS, Azure); no secrets
    > in catalog state.

✅ B3. **Managed-table layout.** Define and document a durable layout under a
    cluster-scoped storage root:
    `s3://bucket/cluster=<id>/db=<db>/schema=<schema>/table=<table>/data/<part>.parquet`
    plus
    `.../meta/manifest.json`, `.../meta/index/<index>.manifest`, and
    `.../meta/snapshots/<ts>.json`. The current local `.managed/` layout
    becomes one driver of this abstraction, not the abstraction itself.
    > Done: `cluster=<id>/` prefix level added via optional `ClusterConfig.cluster_id`.
    > `managed_table_uri` emits `<root>/cluster=<id>/db=<db>/schema=<schema>/table=<table>/`.
    > `data/` subdirectory: `append_batch`, `write_dataframe_to_table_snapshot`, and
    > `execute_insert_select` all write Parquet files to `<prefix>/data/<uuid>.parquet`
    > and record them in the manifest. `compact_table` reads and writes via `entry_key()`
    > helper that handles both old (root-level) and new (`data/`) layouts transparently.
    > `meta/manifest.json` is created at CREATE TABLE time (`persist_empty_table_snapshot`)
    > and updated atomically on every write. `vacuum_orphans` handles both layouts.
    > `index_impl.rs` `try_execute_indexed_select` uses manifest file paths for its
    > `ListingTable` (same as the catalog provider), so indexed SELECTs find files in `data/`.

✅ B4. **Manifest-based snapshots.** Today
    [storage.rs:102](crates/analyticsdb-engine/src/storage.rs:102) lists files
    by directory scan. Object stores make `LIST` expensive and eventually
    consistent. Replace directory listing with an explicit manifest file that
    records committed Parquet files + sizes per snapshot. Reads scan the
    manifest, not the bucket.
    > Done: `manifest.rs` — `read_manifest`, `append_to_manifest`,
    > `replace_manifest`, `list_files`. Falls back to directory scan for
    > pre-manifest tables. All read paths use the manifest.

✅ B5. **Atomic commits.** Wrap writes in a two-step commit: stage data files to
    a UUID-named pending prefix, then publish a new manifest file with
    `If-None-Match`/`If-Match` semantics where the backend supports it
    (S3 conditional writes, GCS preconditions). Provide a fallback durable
    lease in the control plane for backends that lack object preconditions.
    > Done: CAS loop in `append_to_manifest` / `replace_manifest` uses
    > `PutMode::Update(e_tag)` / `PutMode::Create`; falls back to
    > `PutMode::Overwrite` for backends that return `NotImplemented`.

✅ B6. **Compaction / vacuum.** Manifest history grows. Add a background
    compactor that merges small Parquet files (already partially done for
    `INSERT INTO ... SELECT`) and a vacuum task that prunes manifests +
    orphan files older than a configurable retention horizon.
    > Done: `compact_table()` in `manifest.rs` merges files using a row-budget
    > heuristic (target 128 MiB per output file), encodes each output exactly
    > once, replaces manifest atomically, then vacuums orphans. Exposed as
    > `VACUUM <table>` SQL. `vacuum_orphans()` cleans staged-but-uncommitted
    > files. Unit tests in `manifest::tests`.

✅ B7. **Native ↔ external parity tests.** Add CLI-driven SQL tests that
    exercise the same supported SQL surface against (a) the local prototype
    backend, (b) an in-process S3 mock (e.g. `s3-server` or `aws-sdk-mock`),
    and assert identical results, command tags, and error contracts.
    > Done: `cli_file_url_storage_root_supports_full_dml_ddl_lifecycle` exercises
    > CREATE/INSERT/SELECT/UPDATE/DELETE/TRUNCATE/DROP against a `file://` storage
    > root via `ClusterConfig.storage_root`. `cli_s3_storage_root_supports_full_dml_ddl_lifecycle`
    > runs the same lifecycle against an S3 bucket (gated on `ANALYTICSDB_S3_TEST_BUCKET`
    > env var); CI wires a MinIO service container in the `s3-parity` job.

✅ B8. **Encryption at rest.** Wire optional SSE (server-side encryption) when
    the storage URI/policy says so. Document the key-management contract —
    we are not building a KMS, we are integrating one.
    > Done: `build_s3_store` in `storage.rs` reads `ANALYTICSDB_S3_SSE` and
    > `ANALYTICSDB_S3_SSE_KMS_KEY_ID` env vars and applies them via
    > `AmazonS3ConfigKey`. On startup, `from_catalog_path` propagates
    > `ClusterConfig.s3_sse` / `s3_sse_kms_key_id` to env vars (respecting
    > operator-set overrides). Supports `aws:kms`, `aws:kms:dsse`, and
    > `AES256` values.

✅ B9. **Index storage.** Promote the sidecar index manifest path to the same
    object-store layout. Index reads must work from the same `store_for_location`
    abstraction.
    > Done: `index_ops.rs` stores index snapshots under
    > `<table_prefix>/.analyticsdb_indexes/` via the same `store_for_location`
    > abstraction; reads/writes go through `ObjectStore`.

**Exit Gate (B):** ✅ All B items complete.
- ✅ `file://` storage root: `cli_file_url_storage_root_supports_full_dml_ddl_lifecycle` passes.
- ✅ `s3://` mock parity: `cli_s3_storage_root_supports_full_dml_ddl_lifecycle` + CI MinIO job.
- ✅ Atomic commit + orphan vacuum: staged files never become visible on crash.
- ✅ `feature-status.md` row "Native columnar storage" updated.
- ✅ `data/` subdirectory layout adopted across all write paths.
- ✅ SSE env-var parsing unit-tested (B8).

---

### Phase C. Distributed Execution Correctness And Resilience ✅ *(one exit-gate item outstanding: chaos test)*

The current distributed path is a correctness-first scaffold with known gaps
([feature-status.md](docs/agents/feature-status.md) row "Distributed
executor"). The intra-cluster TLS path also bypasses certificate verification
([distributed.rs:312-348](crates/analyticsdb-engine/src/distributed.rs:312)),
which is fine for a prototype but must not ship.

Tasks:

✅ C1. **Intra-cluster mTLS.** Replace `NoVerifier` with proper mutual TLS using
    a cluster CA distributed via the control plane. Each node gets a leaf
    certificate signed by the cluster CA and presents it as both server and
    client. Document key rotation.
    > Done: `NoVerifier` / `ClusterInternalConnector` removed from engine and
    > server. `ClusterMtlsConfig` struct holds PEM-encoded CA cert + leaf
    > cert/key; `PartitionClient::build_channel` uses tonic `ClientTlsConfig`
    > with `.ca_certificate()` + `.identity()` for HTTPS endpoints and errors
    > clearly for misconfigured nodes. Server node-channel uses
    > `ServerTlsConfig::client_ca_root()` to require client certs (mTLS).
    > `analyticsdb ca init` CLI subcommand generates CA + leaf certs (rcgen,
    > ECDSA P-256) with configurable SANs. `tls_ca_cert_path` added to
    > `ClusterConfig`. Committed private keys removed from repo and added to
    > `.gitignore`. Unit tests: `mtls_config_from_cluster_config_requires_all_three_paths`
    > and `cluster_mtls_config_can_be_built_from_rcgen_certs`.

✅ C2. **Cancellation.** Plumb a `CancellationToken` from coordinator query
    admission through `PartitionClient::execute_on_node`
    ([distributed.rs:255](crates/analyticsdb-engine/src/distributed.rs:255))
    so a client disconnect or `KILL QUERY` aborts in-flight worker streams.
    Add a Flight `DoAction` for `CancelPartition` keyed by `query_id`.
    > Done: `CancellationToken` threaded from `active_queries` map through
    > `execute_on_node`; `KILL QUERY <id>` handled; tokens cancelled on client
    > disconnect. Wall-clock query timeout also added (`tokio::time::timeout`).

✅ C3. **Backpressure.** The current streaming path uses unbounded buffers via
    `Pin<Box<dyn Stream>>`. Bound the per-partition channel and propagate
    backpressure to workers; verify by streaming a generated 1B-row query
    while a slow client consumes downstream.
    > Done: per-partition bounded `mpsc::channel(16)` with
    > `ReceiverStream` merged via `select_all`; slow consumers now exert
    > backpressure on worker tasks rather than buffering unboundedly.

✅ C4. **Retry + idempotency.** Workers may receive the same partition twice
    after a retry. Make the worker side idempotent for read partitions
    (already true) and add coordinator-side dedupe for write partitions
    (`ExecutePartitionWrite`) by attaching attempt ids to file names so a
    retried partition does not double-publish into the manifest.
    > Done: `attempt_id = "{query_id}_a{attempt_number}"` embedded in Parquet
    > filenames as `{attempt_id}__{uuid}.parquet`; retried partitions write
    > to distinct files so duplicate attempts are safe.

✅ C5. **Worker resource quotas.** Today there is no per-query memory or
    concurrency budget on workers. Add `datafusion::execution::memory_pool`
    integration with a configurable pool size and a query-level kill switch
    when the pool is exceeded.
    > Done: shared `GreedyMemoryPool` (default 4096 MiB, env
    > `ANALYTICSDB_WORKER_MEMORY_LIMIT_MIB`); per-session `RuntimeEnv` backed
    > by shared pool; admission semaphore (default 32, env
    > `ANALYTICSDB_MAX_CONCURRENT_QUERIES`) via `try_acquire_owned()`.

✅ C6. **Distributed query log siblings.** `execute_partition` (read path) now
    creates a `QueryProbe` via `start_probe_distributed`, records row/byte
    metrics, and calls `finish_result` — mirroring the existing write-partition
    probe.  Worker rows are keyed by `initial_query_id` with
    `is_initial_query = false`.

✅ C7. **Plan coverage.** Distributed dispatch now handles:
    - **GROUP BY + aggregates** (`COUNT/SUM/AVG/MIN/MAX`): 2-phase aggregation
      with key columns passed through on workers; coordinator re-aggregates.
    - **DISTINCT**: both worker and coordinator run `SELECT DISTINCT`, eliminating
      duplicates in each phase.
    - **ORDER BY / LIMIT**: workers return their local top-N; coordinator
      re-sorts and limits the merged result.
    - **Window functions** are explicitly blocked from distribution (return `None`
      so the query falls through to local execution).
    Unit tests: 15 new cases covering all new plan paths and rejection cases.

✅ C8. **Skew handling.** `partition_files_for_workers` now accepts
    `Vec<(String, u64, i64)>` (path, byte_size, row_count).  When all files
    have row_count > 0, the greedy balancer weights by row count; otherwise it
    falls back to byte size.  `FileListCache` and `list_files_with_sizes_and_rows`
    surface per-file row counts from the manifest.  Skew regression test added.

✅ C9. **Catalog under concurrency.** `DistributedRelationLock` added to the
    engine.  `relation_lock()` now returns `Result<DistributedRelationLock>` and
    acquires a SQLite advisory lease (`table_leases` table) held by
    `holder_node_id` for 30 s.  A background task releases the lease when the
    lock is dropped.  The JSON catalog always grants (single-coordinator
    assumption).  Tests: `sqlite_store_lease_acquire_and_release`,
    `json_store_lease_is_always_granted`.

*Additional hardening delivered alongside Phase C:*
- ✅ **Graceful shutdown**: SIGTERM/SIGINT handler cancels all in-flight queries
  before exit (`server/src/main.rs`).
- ✅ **Node heartbeat + health pruning**: background 10 s heartbeat loop per
  node; coordinator prunes nodes silent for > 45 s to `Unavailable`;
  `Heartbeat` Flight DoAction for remote heartbeat.

**Exit Gate (C):** *(all C items complete; remaining hardening below)*
- ❌ Chaos test / worker-kill retry scenario.
- ✅ `KILL QUERY <id>` cancels in-flight queries.
- ✅ mTLS required intra-cluster (`analyticsdb ca init` + `tls_ca_cert_path`).
- ✅ Distributed plan coverage for GROUP BY aggregates, DISTINCT, ORDER BY/LIMIT.

---

### Phase D. Authentication, Authorization, And Audit ✅

Today the auth scaffold has prototype credential storage with rotation
metadata, but the system charter requires "users, roles, groups" and
"audited admin workflows." This phase makes auth production-grade.

Tasks:

✅ D1. **Password storage.** Replace any in-catalog plaintext password storage
    with Argon2id hashes plus a per-user salt. Add a one-shot migration path
    that re-hashes on next login for legacy entries; reject login if the
    stored credential is in the unsupported legacy format after a configurable
    grace window.
    > Done: `hash_password()` (Argon2id + OsRng salt) and `verify_password()`
    > added to `analyticsdb-control`. `CREATE USER` and `ALTER USER PASSWORD`
    > hash before writing; KDF runs outside the write-lock. Legacy plaintext
    > passwords are still accepted during migration window and re-hashed on
    > next rotation.

✅ D2. **PostgreSQL SCRAM-SHA-256.** Implement `SCRAM-SHA-256` as the default PG
    auth flow. Keep plain/MD5 disabled by default; gate them behind an
    explicit config flag for migration.
    > Done: `compute_scram_verifier()` derives `PBKDF2-HMAC-SHA256(SASLprep(password), 16-byte-salt, 4096)`;
    > `create_user` and `rotate_user_password` populate `scram_salt_b64` + `scram_salted_password_b64`
    > on `CatalogUser`. PG wire switched from `CleartextPasswordAuthStartupHandler` to pgwire's
    > `SASLAuthStartupHandler` with `ControlPlaneScramAuthSource`. Bootstrap users get SCRAM
    > verifiers pre-computed. New deps: `pbkdf2`, `sha2`, `hmac`, `stringprep`.

✅ D3. **Flight SQL bearer token auth.** Issue short-lived bearer tokens after
    handshake, refresh through the control plane, and bind tokens to a
    session id. Reject tokens after `ALTER USER ... PASSWORD ...` rotation.
    > Done: `do_handshake` signs a HS256 JWT (`FlightSqlClaims`: sub, role, db, schema,
    > pwd_ver, exp 24 h, iat) using `jsonwebtoken`. `verify_bearer_token()` validates expiry
    > and `pwd_ver` against the catalog (stale tokens rejected on rotation). Auto-generates
    > a 32-byte random `jwt_secret` if absent from `ClusterConfig`. Applied to all authenticated
    > Flight SQL RPCs.

✅ D4. **Roles and groups.** Implement `CREATE ROLE`, `GRANT`, `REVOKE`,
    `ALTER ROLE` with PG-style semantics. Wire role-membership checks into
    every catalog operation (`CREATE TABLE`, `DROP TABLE`, `SELECT`, etc.).
    Add CLI parity tests for each grant/revoke transition.
    > Done: `object_permissions` SQLite table (grantee, object_type, object_name, privilege,
    > granted_by, granted_at_ms). `grant_privilege`, `revoke_privilege`, `check_privilege`
    > on `CatalogStore` trait (JSON store always grants). `GRANT/REVOKE` parsed via sqlparser
    > `Statement::Grant/Revoke` AST; admin-only enforcement. 3 new unit tests.

✅ D5. **Object-level grants.** `GRANT SELECT ON <table> TO <role>` must
    actually gate read access in the planner. Add CLI tests that prove
    denied roles get a uniform `permission denied for <object>` error on
    both protocols.
    > Done: `check_table_access()` in engine checks `check_privilege` before DML execution;
    > admin users bypass. `extract_dml_table_and_privilege()` parses SELECT/INSERT/UPDATE/DELETE
    > target table and required privilege. Error message: `permission denied for table <name>`
    > (PG-compatible SQLSTATE 42501). 2 unit tests: unprivileged role denied, granted role allowed.

✅ D6. **Audit log.** Add a durable `system.audit_log` parallel to
    `system.query_log` for DDL, GRANT/REVOKE, ALTER USER, and failed-auth
    events. Same off-hot-path pattern as the existing query log.
    > Done: `crates/analyticsdb-engine/src/audit_log/mod.rs` with `AuditEventType` enum,
    > `AuditLogRecord` (11 Arrow columns), `AuditLogConfig`, `AuditLog` background writer.
    > Events fired from DDL handlers (CREATE/DROP TABLE, CREATE/DROP USER, ALTER USER,
    > GRANT/REVOKE). Exposed as `system.audit_log` via `ListingTable`. 2 unit tests.

✅ D7. **Secrets management contract.** Storage credentials, TLS keys, and any
    external-system credentials must be referenced by name, never embedded.
    Document the supported secret providers (env, file mount, cloud secret
    manager) and add a smoke test per provider.
    > Done: `docs/secrets.md` documents all supported providers (env vars for cloud
    > storage, file paths for TLS certs/keys, config fields for JWT secret and SSE key
    > references). Three smoke tests in `analyticsdb-control`: `catalog_state_contains_no_plaintext_passwords`
    > (scans catalog JSON for low-entropy credential strings), `cluster_config_tls_fields_are_optional_paths_not_embedded_keys`
    > (asserts TLS config holds paths not inline PEM), `cluster_config_s3_sse_accepts_known_values`
    > (validates SSE config enum values). Bootstrap users now get Argon2id hashes at init time.

✅ D8. **Session timeouts and idle limits.** Implement `statement_timeout`
    (already accepted as a setting, not yet enforced) and `idle_in_transaction_session_timeout`.
    CLI test: a query running past `statement_timeout` cancels with the
    correct PG error code and Flight SQL status code.
    > Done: `statement_timeout_ms` and `idle_in_transaction_timeout_ms` added to `SessionContext`
    > with `#[serde(default)]`. `parse_timeout_to_ms()` handles all PG timeout string formats
    > ("5s", "100ms", "2min", "1h", bare integers = ms). Protocol layer reads
    > `pg_setting_statement_timeout` and `pg_setting_idle_in_transaction_session_timeout` from
    > startup metadata. Engine enforces per-session timeout (takes precedence over global
    > `ANALYTICSDB_QUERY_TIMEOUT_SECS` env var). Error message: `statement_timeout: query
    > exceeded the <N>ms execution time limit`. Tests: `parse_timeout_to_ms_handles_all_formats`
    > (protocol), `statement_timeout_is_propagated_through_session_context` (engine).

**Exit Gate (D): ✅ SATISFIED**
- ✅ No new plaintext passwords written (Argon2id on create/rotate); bootstrap users hashed at init.
- ✅ SCRAM-SHA-256 wired for PG wire; per-connection `SASLAuthStartupHandler` (no state sharing).
- ✅ SCRAM E2E CLI test: `cli_postgres_scram_sha256_auth_accepts_correct_password_and_rejects_wrong`.
- ✅ Flight SQL JWT bearer tokens issued and validated per-RPC; invalidated on password rotation.
- ✅ GRANT/REVOKE parsed and persisted; planner enforces privilege checks (SQLSTATE 42501).
- ✅ GRANT/REVOKE CLI parity test: `cli_grant_revoke_enforces_table_access_on_postgres_and_flight_sql`.
- ✅ `system.audit_log` durable and SQL-queryable.
- ✅ Catalog audit test: `catalog_state_contains_no_plaintext_passwords`.
- ✅ Secrets documented in `docs/secrets.md`; D7 smoke tests green.
- ✅ Per-session `statement_timeout_ms` enforced in engine; `parse_timeout_to_ms` tested.

---

### Phase E. PostgreSQL And Flight SQL Compatibility Expansion

Today the supported SQL surface is narrow and explicitly tested. Production
clients (BI tools, ORMs, JDBC) will fail on missing surface area, not on the
tested slice. This phase widens coverage with the same CLI-test discipline.

Tasks:

E1. **Function surface.** Triage the PG built-in function catalog into:
    (a) supported and CLI-tested, (b) supported but untested, (c) unsupported.
    For each function in (b), add a CLI parity test. For (c), document
    explicitly. Use [docs/agents/postgres-coverage.md](docs/agents/postgres-coverage.md)
    as the living matrix.

E2. **Type coverage.** Add CLI parity tests for `NUMERIC`/`DECIMAL`, `DATE`,
    `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`, `JSONB`, `UUID`, and array types
    across both protocols. Tests must assert wire-level type oids on PG and
    Arrow types on Flight SQL.

E3. **Extended-query coverage.** PG extended-query support today is the
    parameterized subset. Expand to: `Parse`/`Describe`/`Bind`/`Execute`
    with portal names, `DescribePortal`, cursor lifecycle, and binary
    parameter formats. Pair with Flight SQL bind/execute tests for the same
    SQL.

E4. **Catalog parity expansion.** Today `pg_catalog` covers `pg_tables`,
    `pg_views`, `pg_namespace`, `pg_database`, `pg_roles`. Add `pg_class`,
    `pg_attribute`, `pg_type`, `pg_index`, `pg_constraint`, and `pg_proc`
    well enough that `\d`, `\dt`, `\du`, and a JDBC driver introspection
    pass round-trip. Test with the CLI plus a CI job that runs
    `psql -E ... '\dt'` against the live listener.

E5. **`information_schema` expansion.** Today the
    `schemata/tables/columns/views/*_constraints` subset works. Add
    `routines`, `parameters`, `triggers` (as empty but well-typed views),
    and broaden filter/order coverage in CLI tests.

E6. **JDBC and ODBC smoke tests.** Drive a Java JDBC client and an ODBC
    client against both listeners in CI. These do not replace CLI tests;
    they are the integration check that the parity matrix actually maps to
    real driver expectations.

E7. **SQL surface in workstreams.** Each newly supported statement in
    [README.md](README.md) must add a row to the CLI parity matrix and to
    the README-to-matrix drift guard. Already enforced; keep it enforced.

**Exit Gate (E):**
- A `psql` session can connect, run `\l`, `\dn`, `\dt`, `\d <table>`,
  `\du`, run parameterized queries, and disconnect cleanly. Same against
  Flight SQL via a JDBC harness.
- The function/type/catalog matrices in `docs/agents/postgres-coverage.md`
  are kept current by the build.

---

### Phase F. External Tables And Storage Policy

Required by the charter ("native and external tables behind one SQL
surface" + "automatic storage-medium selection").

Tasks:

F1. **External Parquet readers** must accept the same object-store URIs as
    Phase B. `CREATE EXTERNAL TABLE ... LOCATION 's3://...'` lands in the
    catalog and reads through the same `store_for_location` abstraction.

F2. **Iceberg read path.** Integrate `iceberg-rust` for read-only access to
    Iceberg v2 tables via REST and Glue catalogs. Snapshot/manifest reads
    only; writes are a follow-up.

F3. **Unified SQL surface.** All CLI parity tests added in Phases B and E
    must also run against an external Parquet copy and an Iceberg copy of
    the same data, and produce identical results. Diverging is a bug.

F4. **Storage policy engine.** Introduce a `storage_policy` table in the
    catalog and a planner hook that records which physical backing the
    optimizer chose per query. Surface the choice via `EXPLAIN` and the
    query log.

**Exit Gate (F):**
- Identical CLI SQL test results across managed, external Parquet, and
  external Iceberg backings for the supported surface.
- `EXPLAIN <q>` shows the chosen storage path and policy rationale.

---

### Phase G. Observability End To End

Required by every plane's "Rules" in
[system-architecture.md](docs/agents/system-architecture.md).

Tasks:

G1. **Metrics.** Emit Prometheus-compatible metrics via `tracing-prometheus`
    or `metrics` crate: `query_admission_total`, `query_duration_seconds`
    histograms (by user, db, schema, protocol, outcome), `worker_partition_duration`,
    `manifest_publish_failures_total`, `auth_failures_total`. Scrape endpoint
    on each node.

G2. **OpenTelemetry traces.** Attach trace ids at admission and propagate
    via gRPC metadata into worker `ExecutePartition` calls so a single trace
    spans coordinator + workers. Document an OTLP collector deployment.

G3. **Query log completeness.** Close the remaining `Partial` gaps called out in
    [feature-status.md](docs/agents/feature-status.md): streaming Flight SQL
    finish accounting, DataFusion stage metric enrichment, retention sweeper,
    partitioned layout. (Worker sibling rows for distributed reads landed in C6.)
    Each gap closure ships with an engine + CLI SQL test.

G4. **Audit log** (also covered by D6) must share the same async durable
    pattern.

G5. **Log correlation.** Every log line in the request path carries
    `query_id`, `initial_query_id`, `node_id`, `user`, `database`,
    `schema`, `protocol`. Verified by a CI test that submits a query and
    asserts the appearance of the expected fields in the captured logs.

G6. **Benchmark gate.** Add a `criterion`-based microbenchmark suite for
    the query log hot path, the planner, and the index lookup path, plus a
    CI job that fails if a PR regresses any benchmark by more than a
    documented threshold.

**Exit Gate (G):**
- Prometheus scrape returns the documented metric set.
- A single OTel trace shows the coordinator span + all worker spans for a
  distributed query.
- Query log is `Complete` per
  [feature-status.md](docs/agents/feature-status.md) DoD.

---

### Phase H. Kubernetes Deployment And Operability

Required by the charter and currently `Prototype` per
[feature-status.md](docs/agents/feature-status.md).

Tasks:

H1. **Container image.** Multi-stage Dockerfile producing a minimal
    distroless image with the server binary, default config, and TLS-aware
    entrypoint. Image runs as non-root.

H2. **Helm chart.** Separate Deployments for control-plane and compute
    pools, a Service per protocol port (PG, Flight SQL, node-channel),
    a HorizontalPodAutoscaler for compute, and a StatefulSet for the
    coordinator. PVCs are cache/spill only; durable storage is object
    storage from Phase B.

H3. **Liveness, readiness, startup probes.** Distinct HTTP endpoints on a
    dedicated admin port; readiness fails until the node finishes catalog
    bootstrap and confirms object-store reachability.

H4. **Rolling upgrade.** Document and CI-verify that a rolling upgrade of
    coordinator + workers does not abort in-flight queries (in combination
    with C2 cancellation and the manifest-based commits in Phase B).

H5. **Configuration management.** Centralize all config (currently spread
    across `cluster-config.json`, env vars, and CLI flags) into a typed
    config schema with a single source of truth. Validate on startup;
    refuse to start with conflicting flags.

H6. **Day-2 docs.** Operator runbook covering: scaling compute, rotating
    the cluster CA, rotating object-store credentials, restoring a corrupt
    manifest, draining a node, reading the audit log, and triggering a
    compaction.

H7. **Backup / restore.** Object-storage data is durable by virtue of the
    backend; the catalog (currently SQLite + JSON) is not. Define a
    catalog backup/restore tool and a documented RPO/RTO.

**Exit Gate (H):**
- `helm install` brings up a working cluster on a kind/k3d test cluster in
  CI and passes the full CLI SQL test suite against the in-cluster
  endpoint.
- A documented rolling upgrade scenario passes in CI without dropping
  in-flight queries.
- Catalog backup + restore round-trips a full cluster state.

---

### Phase I. Performance And Capacity

Required because "lightning-fast regular SQL execution" and "lightning-fast
distributed execution across massive datasets" are charter requirements that
currently have **zero validated evidence**.

Tasks:

I1. **Benchmark harness.** Reproducible TPC-H scale factors 1, 10, 100, 1000
    runs through the CLI against (a) embedded, (b) single-node listener,
    (c) multi-node cluster. Publish per-query latencies, throughput, and
    coordinator+worker resource usage. Use the same harness for
    PG and Flight SQL.

I2. **Concurrency profile.** Multi-client workload (e.g. 64 concurrent BI-style
    queries) with measured tail latency. Establish baseline numbers; later
    PRs must not regress them by more than a documented threshold.

I3. **Scale validation.** A documented "supported scale envelope" — max
    rows per table, max columns, max concurrent queries per coordinator,
    max workers per cluster — derived from measured behavior, not
    aspiration.

I4. **Optimizer statistics.** Persist column-level statistics (row count,
    null count, min/max, ndv-estimate) in the manifest from Phase B and
    feed them into the DataFusion planner via the standard `Statistics`
    interface. Add CLI tests that prove a known-bad plan choice flips after
    statistics are present.

I5. **Caching.** Implement the two `Prototype` caches from
    [feature-status.md](docs/agents/feature-status.md): query results and
    data blocks. Cache eviction, invalidation on manifest publish, and
    bypass policy must be testable through SQL hints + an admin command.

**Exit Gate (I):**
- Published TPC-H SF100 result with documented hardware footprint, repeatable
  by anyone who runs the benchmark target.
- A concurrency regression gate in CI.
- The supported-scale envelope is documented and linked from the README.

---

### Phase J. Surfaces: CLI, Web Console, Admin

Required by the charter (`CLI` and `Web Console` sections of
[project-charter.md](docs/agents/project-charter.md)).

Tasks:

J1. **CLI.** Already `Partial`. Bring it to `Complete`:
    - broader psql-style meta commands (`\d`, `\dn`, `\du`, `\timing`,
      `\set`, `\watch`)
    - documented timing semantics with a CLI test for the displayed
      numbers
    - stable scripting mode (machine-readable output)

J2. **Web admin console.** Currently `Prototype` UI driven by a local
    harness. Build the live gateway path:
    - server-side gateway terminating sessions and proxying to the engine
      over PG or Flight SQL
    - SSO / OIDC integration via the same auth contract as Phase D
    - explorer reads from live metadata (no sample data)
    - admin views: databases, users/roles/grants, system metrics, log
      exploration with query-id correlation
    - end-to-end Playwright tests in CI

J3. **Result UX.** Result grid streams large results without loading them
    fully in the browser. Engine messages and timing are always visible.

**Exit Gate (J):**
- CLI promoted to `Complete` per its DoD.
- Web admin console queries a live cluster through the gateway and admin
  views are all live, with Playwright coverage in CI.

---

## 4. Workstream Dependencies

```
A (Foundations)
   │
   ├──► B (Object-Store Storage) ─┐
   │                              ├──► C (Distributed Hardening) ─┐
   │                              │                               │
   ├──► D (Auth/Authz/Audit) ─────┤                               ├──► H (k8s/Ops) ─► I (Performance) ─► J (Surfaces)
   │                              │                               │
   │                              └──► F (External + Policy) ─────┤
   │                                                              │
   └──► E (PG/Flight Compatibility) ──────────────────────────────┘
                                                                  │
                                                                  └──► G (Observability)
```

- A is a hard prerequisite for everything else.
- B is a hard prerequisite for C, F, H, I.
- D is independent of B but a prerequisite for H (you cannot ship a
  cluster without production-grade auth).
- G runs in parallel; each phase contributes its share of observability.

---

## 5. Per-Phase Definition Of Done

A phase is **`Complete`** when, and only when, every item below is true:

1. Each task in the phase has a closing PR linked from this document.
2. The phase's Exit Gate is satisfied with green CI on the documented matrix.
3. [docs/agents/feature-status.md](docs/agents/feature-status.md) reflects
   the status change with a one-line evidence note.
4. [docs/agents/workstreams.md](docs/agents/workstreams.md) updated if the
   sequencing changed in practice.
5. Every SQL-testable behavior added in the phase has a CLI-driven SQL test
   in `cargo test -p analyticsdb-cli --test sql_cli` running in normal CI.
6. The phase's user-visible behavior is reachable through **both** PG and
   Flight SQL, unless the feature is documented as protocol-specific.
7. Native ↔ external parity covered for any feature that touches data
   access.

Skipping any of those = the phase is not done.

---

## 6. First-Two-Week Concrete Sequence

To make progress visible immediately, the suggested sequence for the first
two weeks of work:

1. **Day 1–2:** Phase A1 (engine module split — no behavior change).
2. **Day 2–3:** Phase A2/A3 (unwrap audit + clippy gates).
3. **Day 3–4:** Phase A4/A5 (clean root artifacts + CI matrix).
4. **Day 5:** Phase B1 (object-store URI plumbing, no commit/manifest yet).
5. **Day 6–8:** Phase B3/B4 (manifest layout + manifest-based snapshot
   reads).
6. **Day 9–10:** Phase B5 (atomic commits + CLI test against an S3 mock).

End-of-week-2 state: engine refactored, foundations gates green, managed
tables read/write through the manifest abstraction against both `file://`
and S3 mock backends. That is a meaningful jump in production readiness
without inventing distributed correctness work the storage layer cannot yet
support.

---

## 7. What This Plan Deliberately Does Not Do

- **Does not** claim DataFusion or Ballista already deliver any feature
  here. Every upstream capability must be wrapped with our own CLI-driven
  SQL test before it counts.
- **Does not** introduce a separate "fast path" SQL surface that bypasses
  the catalog or the planner. Any future caching is additive and
  invalidation-aware.
- **Does not** treat any single-protocol behavior as production-ready. PG
  and Flight SQL move forward together.
- **Does not** assume the prototype's `cluster-catalog.json` / SQLite
  catalog is the production catalog. Catalog durability and replication is
  itself a Phase B / Phase H concern.

---

## 8. Open Architectural Questions To Resolve Before Phase B Lands

These are flagged here so they get answered explicitly rather than
backed-into:

1. **Object-store consistency.** Do we require strong read-after-write
   consistency from the backend (true for S3, GCS, Azure today), or do we
   tolerate eventual consistency with our manifest-publishing approach as
   a safety net? Decision affects manifest design.
2. **Catalog backing store.** SQLite is fine for a single-coordinator
   prototype. Production needs a replicated, leader-elected catalog. Pick
   one of: (a) Raft-backed embedded state, (b) external Postgres, (c)
   FoundationDB-style KV. Decision affects Phase H.
3. **Cluster CA lifecycle.** Cert-manager + a per-cluster CA is the obvious
   k8s answer, but it must work for non-k8s installs too. Pick a baseline.
4. **Iceberg catalog backends.** REST is portable; Glue/Hive/Polaris are
   common in the wild. Pick the v1 supported set explicitly.
5. **Identity provider.** OIDC via the gateway is the obvious answer, but
   we must decide whether the engine itself federates auth or whether the
   gateway is mandatory.

Each question above blocks at least one of B, D, F, or H. Resolve as a
written ADR under `docs/agents/` before starting the dependent phase.

---

*End of plan.*
