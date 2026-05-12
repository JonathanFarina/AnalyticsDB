# Query Log / Lineage

Design for an AnalyticsDB equivalent of ClickHouse's `system.query_log`
([clickhouse.com/docs/operations/system-tables/query_log](https://clickhouse.com/docs/operations/system-tables/query_log)).

The hard constraint: **the hot query path must not pay for it**. Anything that
could block a query — disk I/O, lock acquisition with contended waiters,
catalog roundtrips, serialization of large structures — happens off-path. The
worst that hot-path code does is push a `~200-byte` struct into an unbounded
in-memory channel and timestamp two `Instant`s.

---

## Goals

1. Every executed query produces one durable record with: query text, user,
   start time, duration, rows in/out, bytes scanned, memory peak, error info,
   tables touched, originating node, distributed siblings.
2. Records are queryable via SQL: `SELECT * FROM system.query_log WHERE …`.
3. **No measurable overhead** on TPS or P50/P99 latency vs. the current code
   path — verified by a benchmark gate.
4. Works across the distributed write path (Coordinator + workers each log
   their own slice, joined by `initial_query_id`).
5. Configurable: enable/disable, sample rate, retention.

## Non-goals (for v1)

- Per-row provenance / data-lineage at the column level. This is query-level
  lineage only.
- Live streaming of logs to external sinks (Kafka, OTel). Land in v2 once the
  on-disk format is stable.
- Replicating ClickHouse's `query_thread_log` (per-thread breakdown). Useful
  later but not the headline feature.
- Mutations beyond INSERT (UPDATE/DELETE lineage) — they'll fall into place
  but aren't the focus.

---

## ClickHouse parity (column-by-column)

| ClickHouse column                 | AnalyticsDB column                | Notes                                                                                           |
| --------------------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------- |
| `type`                            | `event_type`                      | enum: `QueryStart`, `QueryFinish`, `ExceptionBeforeStart`, `ExceptionWhileProcessing`           |
| `event_date` / `event_time`       | `event_time`                      | TIMESTAMP, UTC                                                                                  |
| `event_time_microseconds`         | `event_time_us`                   | INT64 microseconds since epoch (analytics filtering hates timestamp truncation)                 |
| `query_start_time`                | `query_start_time`                | TIMESTAMP                                                                                       |
| `query_duration_ms`               | `duration_ms`                     | INT64                                                                                           |
| `read_rows` / `read_bytes`        | `read_rows` / `read_bytes`        | from DataFusion `ExecutionPlan` metrics                                                         |
| `written_rows` / `written_bytes`  | `written_rows` / `written_bytes`  | counted by the INSERT path                                                                      |
| `result_rows` / `result_bytes`    | `result_rows` / `result_bytes`    | from the final stream                                                                           |
| `memory_usage`                    | `memory_peak_bytes`               | DataFusion `RuntimeEnv` memory pool peak                                                        |
| `current_database`                | `database`                        |                                                                                                 |
| `query`                           | `query`                           | full SQL                                                                                        |
| `formatted_query`                 | _omit_                            | client-side concern                                                                             |
| `normalized_query_hash`           | `normalized_query_hash`           | UInt64; hash of SQL with literals replaced by `?`. Lets users find slow query *shapes*.         |
| `query_kind`                      | `query_kind`                      | `Select`, `Insert`, `Create`, `Drop`, `Alter`, `Other`                                          |
| `databases` / `tables` / `views` / `columns` | `tables`               | array<text>; flatten to fully-qualified `db.schema.table`. Columns omitted in v1.               |
| `partitions`                      | _omit_                            | not partitioned yet                                                                             |
| `projections`                     | _omit_                            | n/a                                                                                             |
| `exception_code` / `exception` / `stack_trace` | `error_code` / `error` / `error_stack` | Strings; stack only if `RUST_BACKTRACE=1`                                      |
| `is_initial_query`                | `is_initial_query`                | bool; true on coordinator, false on workers                                                     |
| `user` / `query_id` / `initial_query_id` | same                       |                                                                                                 |
| `address` / `port`                | `client_address`                  | TEXT (`ip:port`)                                                                                |
| `initial_user` / `initial_address` / `initial_port` | same                |                                                                                                 |
| `interface` / `client_*`          | `protocol` / `client_name` / `client_version` | from session                                                                       |
| `http_method` / `http_user_agent` / `http_referer` | _omit_                | no HTTP entrypoint yet                                                                          |
| `forwarded_for`                   | _omit_                            |                                                                                                 |
| `quota_key`                       | _omit_                            | no quotas yet                                                                                   |
| `revision`                        | `engine_version`                  | semver of analyticsdb-engine                                                                    |
| `Settings.Names` / `Settings.Values` | `settings`                     | map<text,text>; only diff vs. defaults                                                          |
| `used_aggregate_functions`, etc.  | _omit in v1_                      | nice-to-have                                                                                    |
| `ProfileEvents`                   | `profile`                         | map<text,bigint>; sparse, only counters the engine tracks                                       |
| `thread_ids`                      | _omit_                            | tokio task IDs aren't user-meaningful                                                           |
| `peak_threads`                    | _omit_                            |                                                                                                 |
| **— AnalyticsDB additions —**     |                                   |                                                                                                 |
|                                   | `coordinator_node_id`             | which node admitted the query                                                                   |
|                                   | `worker_node_id`                  | which node wrote this row (null on coordinator-only paths)                                      |
|                                   | `distributed_partition_count`     | how many workers participated (coordinator row only)                                            |
|                                   | `bytes_dropped_to_disk`           | future spill metric                                                                             |

---

## Architecture

```
                      ┌─────────────────────────────────────────────────────┐
   query in ─────────▶│ execute_query() — hot path, must not block          │
                      │                                                     │
                      │  on admission:    QueryProbe::start()  ──┐          │
                      │  during execution: probe.observe(…)      │          │
                      │  on finish:       probe.finish(result)  ─┴──▶ try_send(record)
                      │                                              (mpsc::unbounded)
                      └─────────────────────────────────────────────────────┘
                                                                  │
                                                                  ▼
                       ┌──────────────────────────────────────────────────────┐
                       │ QueryLogWriter task (one per node, tokio::spawn)     │
                       │  loop:                                               │
                       │    select {                                          │
                       │      records = recv_batch(N or T elapsed) => …      │
                       │      shutdown_signal => flush_and_exit()             │
                       │    }                                                 │
                       │  flush:                                              │
                       │    1. Build Arrow RecordBatch from accumulated recs  │
                       │    2. Write Parquet to system.query_log/<uuid>.parquet
                       │    3. (no catalog mutation — listing-table reads it) │
                       └──────────────────────────────────────────────────────┘
                                                                  │
                                                                  ▼
                                                       Parquet files in
                                                       `system/query_log/`
                                                       readable via the catalog
```

The pieces:

- **`QueryProbe`**: small struct constructed at admission. Holds the
  `query_id`, `Instant::now()`, atomic counters for rows/bytes/memory, and
  hooks to record an error. Methods are cheap and non-blocking. When the
  query completes, `finish(...)` materializes a `QueryLogRecord` and pushes
  to the channel.

- **`QueryLogChannel`**: a `tokio::sync::mpsc::UnboundedSender<QueryLogRecord>`.
  `try_send` returns immediately; the producer never awaits.

- **`QueryLogWriter`**: a background tokio task spawned once per node at
  engine startup. Owns the receiver, an in-memory buffer, a flush deadline,
  and the object_store handle. Flushes on whichever fires first:
  - `BATCH_SIZE` records accumulated (default 1024), or
  - `BATCH_INTERVAL` elapsed since last flush (default 5s), or
  - shutdown signal received.

- **`system.query_log` table**: registered as a `ListingTable` rooted at the
  cluster's `system/query_log/` prefix. Reading it goes through the normal
  catalog path; no special-case scan code needed.

### Why an unbounded channel?

`UnboundedSender::send` is wait-free and never errors except on receiver
closure. The footprint of a `QueryLogRecord` is ~200 bytes for typical
queries (SQL text is the variable cost; we cap query text at 64KiB and store
a hash + truncation flag for longer queries). With ~50k QPS sustained, the
in-flight buffer between flushes (worst case 5s) is `50_000 × 5 × 200B ≈
50MB` of heap. Acceptable.

If we ever need bounded backpressure, swap for `tokio::sync::mpsc::channel`
with a generous capacity and call `try_send`, falling back to a `dropped_log`
counter when full. We will start unbounded and revisit.

### Why no catalog mutation per flush?

The query log isn't a managed table you `INSERT INTO`. It's a directory of
Parquet files that the catalog *projects* via a `ListingTable`. The writer
just drops new files into the prefix; the catalog needs no transactional
update per flush. This is the same trick the index sidecars use and it's why
flushes can be 100% local I/O.

---

## Hot-path cost budget

Per-query overhead, on the coordinator:

| Step                                    | Cost                       |
| --------------------------------------- | -------------------------- |
| `QueryProbe::start()`                   | one `Instant::now()`, one `Arc<AtomicU64>` allocation (or pooled), grab `query_id` |
| `probe.observe_table(name)`             | `SmallVec::push(String)` |
| `probe.observe_metric(read_rows += n)`  | `AtomicU64::fetch_add` |
| `probe.finish(result)` — coordinator    | one `Instant::now()`, build `QueryLogRecord` (no I/O), `UnboundedSender::send` (wait-free) |
| **Total wall clock** (estimate)         | < 5 µs for typical queries |

For a query that runs for 10 ms, this is ~0.05% overhead. For a 1 µs trivial
`SELECT 1`, it's ~5x relative overhead — but absolute is still in the noise.
A benchmark gate (below) will keep us honest.

The writer task and Parquet flushes run in parallel with query execution.
They do not contend on any lock the query path takes.

---

## Schema

```sql
CREATE TABLE system.query_log (
    event_type                     TEXT NOT NULL,        -- enum string
    event_time                     TIMESTAMP NOT NULL,
    event_time_us                  BIGINT NOT NULL,
    query_start_time               TIMESTAMP NOT NULL,
    query_id                       TEXT NOT NULL,
    initial_query_id               TEXT NOT NULL,
    is_initial_query               BOOLEAN NOT NULL,
    query_kind                     TEXT NOT NULL,
    query                          TEXT NOT NULL,
    query_truncated                BOOLEAN NOT NULL,
    normalized_query_hash          BIGINT NOT NULL,
    duration_ms                    BIGINT NOT NULL,
    read_rows                      BIGINT NOT NULL,
    read_bytes                     BIGINT NOT NULL,
    written_rows                   BIGINT NOT NULL,
    written_bytes                  BIGINT NOT NULL,
    result_rows                    BIGINT NOT NULL,
    result_bytes                   BIGINT NOT NULL,
    memory_peak_bytes              BIGINT NOT NULL,
    error_code                     INT,
    error                          TEXT,
    error_stack                    TEXT,
    "user"                         TEXT NOT NULL,
    database                       TEXT NOT NULL,
    client_address                 TEXT,
    protocol                       TEXT NOT NULL,
    coordinator_node_id            TEXT NOT NULL,
    worker_node_id                 TEXT,
    distributed_partition_count    INT,
    tables                         TEXT[],
    settings                       TEXT,                 -- JSON-encoded map
    profile                        TEXT,                 -- JSON-encoded map
    engine_version                 TEXT NOT NULL
);
```

Stored as Parquet, partition layout `system/query_log/YYYY/MM/DD/<uuid>.parquet`
so the retention sweeper can drop whole day-prefixes cheaply and so users get
free partition pruning when filtering by `event_time`.

---

## Where the hooks go (existing code)

These are the only edits to the hot path. Everything else lives in a new
`query_log` module.

1. **`PrototypeEngine::prepare_query_request`** ([lib.rs:1871](crates/analyticsdb-engine/src/lib.rs:1871))
   builds the `QueryAdmission`. Right after admission, build a `QueryProbe`
   and stash it on a request-scoped struct passed to each execute path.

2. **Each `execute_*` method** that returns `QueryExecutionResult` —
   `execute_query`, `execute_insert_select`, `execute_metadata_query`,
   `try_execute_indexed_select`, `try_execute_distributed_select`,
   `try_execute_distributed_insert_select`, etc. — calls `probe.finish(...)`
   right before returning. This can be done with a single `match` on the
   `Result<QueryExecutionResult>` in `execute_query`, so most execute paths
   don't have to change.

3. **`execute_distributed_write_partition`** (worker side, [lib.rs:660](crates/analyticsdb-engine/src/lib.rs:660))
   constructs its own probe with `is_initial_query = false` and the
   coordinator's `initial_query_id` (carried in `ExecutePartitionWriteRequest`).
   Finishes when the worker returns its ack.

4. **`ExecutePartitionWriteRequest` / `ExecutePartitionRequest`**: add an
   optional `initial_query_id: String` field. Coordinator fills it; workers
   propagate.

Single drop-in point that catches **all** execute paths cleanly: wrap the
body of `pub async fn execute_query` in:

```rust
let probe = self.query_log.start_probe(&request, &admission);
let result = self.execute_query_inner(request).await;
probe.finish(self.local_node_id(), &result);
result
```

`execute_query_inner` is the current body. The probe wrapping is the only
hot-path change in `lib.rs`.

---

## Metrics collection

How each metric is captured **without** adding overhead inside the inner
execution loop:

- **`duration_ms`**: `Instant::elapsed()` at finish.
- **`read_rows` / `read_bytes`**: DataFusion exposes `ExecutionPlan::metrics()`
  on the final plan. The probe walks the metrics tree *once*, at finish,
  using `MetricsSet::aggregate_by_name()`. No per-batch instrumentation.
- **`written_rows` / `written_bytes`**: already counted in `execute_insert_select`
  and in the worker's `execute_distributed_write_partition`. Plumb to probe.
- **`result_rows` / `result_bytes`**: already in `QueryExecutionResult`
  (`outcome.rows_affected` for commands; sum of `batches` for queries).
- **`memory_peak_bytes`**: DataFusion `RuntimeEnv::memory_pool().reserved()`
  high-water-mark. The runtime tracks it; read once at finish.
- **`tables`**: walk the parsed AST during `rewrite_sql_for_postgres_compatibility`
  (which already parses every statement) and stash the visited tables on the
  probe. Free — uses parsing that's already happening.
- **`normalized_query_hash`**: cheap visitor over the same AST that replaces
  literals with `?` and hashes the result with FxHash. Skipped if the parse
  fails (which is already handled gracefully).

The expensive bits — building strings for arrays/maps, encoding Arrow,
writing Parquet — all happen in the writer task, never on the hot path.

---

## Configuration

In `cluster-config.json`:

```json
{
  "query_log": {
    "enabled": true,
    "sample_rate": 1.0,                 // fraction of queries logged; 1.0 = all
    "min_duration_ms": 0,               // skip records below this duration
    "batch_size": 1024,
    "batch_interval_ms": 5000,
    "max_query_length_bytes": 65536,
    "retention_days": 30
  }
}
```

`sample_rate` and `min_duration_ms` are evaluated in `QueryProbe::start()`
and `finish()` respectively. If the probe is sampled out, `start()` returns
a no-op probe (zero-sized, all methods compile to nothing). This is the
right place to draw the line between "log this" and "skip" — once a probe
has been created, finishing it is cheap enough to not bother short-circuiting.

---

## Distributed propagation

Each node logs its own work. The coordinator and every participating worker
emit a row for the same query, joined by `initial_query_id`.

- Coordinator: `query_id == initial_query_id`, `is_initial_query = true`,
  `worker_node_id = NULL`, `distributed_partition_count = N`.
- Each worker: fresh `query_id`, `initial_query_id` = coordinator's,
  `is_initial_query = false`, `worker_node_id = self`,
  `distributed_partition_count = NULL`.

`ExecutePartitionRequest` and `ExecutePartitionWriteRequest` gain an
`initial_query_id: String` field. The current `query_id` field stays as-is
(it's the partition's local id).

To read the whole picture for a query:

```sql
SELECT * FROM system.query_log
WHERE initial_query_id = 'abc-…'
ORDER BY event_time;
```

---

## Retention

A daily background task in the same `QueryLogWriter` infra:

- Lists `system/query_log/` day-prefixes.
- Deletes any prefix where `date < today - retention_days`.
- Runs in its own tokio task; never blocks flushing.

No TTL DDL needed in v1. If users want to query log entries older than the
retention window they'll know to widen the config.

---

## Failure modes

| Failure                                  | Behavior                                                                                       |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `mpsc::send` fails (receiver dropped)    | Increment a `query_log_send_failures` counter (Prometheus once we have it); log once per N. Continue. |
| Writer task panics                       | Engine logs the panic and respawns the task. In-flight buffer is lost (acceptable; logs are best-effort). |
| Parquet flush fails (disk full, store err) | Log a warning, drop the in-memory batch, continue. Do not retry indefinitely — query logs must never eat disk for retries. |
| Schema evolution                         | Writer always emits the current schema; readers tolerate older Parquet files via the catalog's schema-merging behaviour (already used by user tables). |
| Engine shutdown                          | On `Drop`/SIGTERM: send shutdown signal, wait up to 2s for the writer to flush, then proceed. Beyond 2s we drop the buffer. |

The principle: **never let logging affect query correctness or liveness**.

---

## Performance gating

We don't ship without proof. Two benchmarks added under
`crates/analyticsdb-engine/benches/`:

1. **`bench_query_log_off`** vs. **`bench_query_log_on`**: a tight loop of
   small `SELECT 1` and `SELECT * FROM small_table LIMIT 10` against an
   in-memory cluster. Measures wall-clock per query. Acceptance gate:
   **mean overhead < 2%**, **P99 overhead < 5%**.
2. **`bench_insert_log_on`**: a `INSERT … SELECT FROM generate_series(1, 1M)`
   measuring throughput (rows/s). Acceptance gate: **< 1% regression**.

If either gate trips, the feature is gated behind `enabled: false` by
default until the cost can be brought back under budget. The benches run in
CI on every PR that touches `crates/analyticsdb-engine/src/query_log/`.

---

## Phased implementation

Each phase is independently shippable. After each, the engine is fully
functional with logs accumulating but not yet readable in some cases.

### Phase 1 — Hot-path probe + writer + Parquet sink (Completed)
- `crates/analyticsdb-engine/src/query_log/mod.rs`:
  - `QueryProbe`, `QueryLogRecord`, `QueryLogChannel`, `QueryLogWriter`.
  - Schema + Arrow batch builder.
- Wire into `execute_query` wrapper.
- Background flusher writing Parquet to `system/query_log/YYYY/MM/DD/`.
- Config plumbing.

### Phase 2 — `system.query_log` catalog table (Completed)
- Register a `ListingTable` rooted at `system/query_log/` under a
  `SystemSchemaProvider` (mirrors `PgCatalogSchemaProvider` at
  [system_catalog.rs:388](crates/analyticsdb-engine/src/system_catalog.rs:388)).
- Schema definition shared with the writer (single source of truth).
- `SELECT * FROM system.query_log` works.

### Phase 3 — Distributed propagation (Completed)
- Add `initial_query_id` and `coordinator_node_id` to partition request types.
- Worker-side probe in `execute_distributed_write_partition` and
  `execute_partition_stream`.
- One-row-per-node, joined by `initial_query_id`.

### Phase 4 — Metrics enrichment (Completed)
- DataFusion `ExecutionPlan::metrics()` walking for `read_rows`.
- `RuntimeEnv` memory peak.
- Normalized query hash + table extraction in the existing AST visitor.

### Phase 5 — Retention + ops polish (Completed)
- Daily retention sweeper.
- Shutdown drain (handled by tokio drop).
- Failure counters surfaced via existing logging.

### Phase 6 — Benchmark gate (≈1 day)
- Two criterion benches above.
- Wire into CI.

**Total: ~10-12 engineering days.**

---

## Open questions

1. **Sampling distribution.** Uniform random is the obvious choice. Should
   we instead always log queries that error, regardless of sample rate?
   I lean yes — exceptional events are exactly what you want to debug
   after the fact. Cheap to implement (check `result.is_err()` before
   applying sample rate).

2. **Query text truncation policy.** Cap at 64KiB and set
   `query_truncated = true`, or store the full text in a side blob
   addressed by hash? V1: just truncate. Revisit if users complain.

3. **Should worker rows include the worker's piece of the SQL** (e.g.
   the rewritten `generate_series(start_k, end_k)` slice) or the
   original SQL? I lean original SQL for human readability, since the
   user can correlate by `initial_query_id` and the slice info is
   captured by `worker_node_id` + partition count. ClickHouse stores
   the original.

4. **Index on `initial_query_id`.** Most lookups are by `query_id` /
   `initial_query_id` / time range. Once we have proper secondary
   indexes, build one on `initial_query_id`. For now, day-partitioned
   Parquet + time-range filtering is fine.

5. **PII / secret leakage.** Query text can include credentials in `COPY
   FROM 'file://…'` or future external-table DDL. Probably fine for v1
   (single-tenant prototype) but flag it in the docs.

---

## Out of scope (parked)

- `system.query_thread_log` — per-task breakdown.
- `system.metric_log` — gauge-style cluster metrics (CPU, memory).
- Real-time tailing / `KILL QUERY` integration.
- OpenTelemetry export.
- Column-level lineage (what columns each query touched, transitively).
  Useful for governance but a much bigger lift; build atop the same
  AST visitor when needed.

---

## File-level deliverables

```
crates/analyticsdb-engine/src/
  query_log/
    mod.rs                — public API: QueryProbe, QueryLogChannel, QueryLogConfig
    record.rs             — QueryLogRecord + Arrow schema + RecordBatch builder
    probe.rs              — QueryProbe + atomics
    writer.rs             — QueryLogWriter (background task + flush + retention)
    normalize.rs          — AST visitor: normalized_query_hash, table extraction
  system_catalog.rs       — extend with SystemSchemaProvider for system.query_log
  lib.rs                  — single-line wrapper around execute_query (~10 lines)

crates/analyticsdb-engine/benches/
  query_log.rs            — gating benches

crates/analyticsdb-control/src/
  lib.rs                  — query_log config in ClusterConfig

docs/
  query_log.md            — user-facing docs: schema, query examples, config
```

Public API surface stays tiny: from the outside, `query_log` is one
`PrototypeEngine` field and a transparent wrapper around `execute_query`.
