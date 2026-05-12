# Query Log

Status: `Partial`

AnalyticsDB now has a prototype durable query log for the non-streaming execution path. Each logged query is written asynchronously to local Parquet files under the catalog-managed data root and exposed through SQL as:

```sql
SELECT * FROM system.query_log;
```

The current storage root is:

```text
<catalog-file-stem>.managed/system/query_log/
```

For example, a catalog at `/tmp/analyticsdb-catalog.json` writes query-log files under `/tmp/analyticsdb-catalog.managed/system/query_log/`.

## Configuration

Cluster config accepts:

```json
{
  "query_log": {
    "enabled": true,
    "sample_rate": 1.0,
    "min_duration_ms": 0,
    "batch_size": 1024,
    "batch_interval_ms": 5000,
    "max_query_length_bytes": 65536,
    "retention_days": 30
  }
}
```

Defaults are applied when the section is absent.

## Current Columns

`system.query_log` exposes the current v1 schema:

- `event_type`
- `event_time`
- `event_time_us`
- `query_start_time`
- `query_id`
- `initial_query_id`
- `is_initial_query`
- `query_kind`
- `query`
- `query_truncated`
- `normalized_query_hash`
- `duration_ms`
- `read_rows`
- `read_bytes`
- `written_rows`
- `written_bytes`
- `result_rows`
- `result_bytes`
- `memory_peak_bytes`
- `error_code`
- `error`
- `error_stack`
- `"user"`
- `database`
- `client_address`
- `protocol`
- `coordinator_node_id`
- `worker_node_id`
- `distributed_partition_count`
- `tables`
- `settings`
- `profile`
- `engine_version`

## Examples

```sql
SELECT query_id, query, duration_ms, result_rows
FROM system.query_log
ORDER BY event_time_us DESC
LIMIT 10;
```

```sql
SELECT query, error
FROM system.query_log
WHERE event_type = 'ExceptionWhileProcessing';
```

## Current Gaps

This is not yet production complete.

- The writer currently stores date-prefixed Parquet files directly under `system/query_log/`; the planned `YYYY/MM/DD/` partition layout remains future work because the current DataFusion listing-table registration does not discover those nested files through the root provider.
- The initial probe logs the non-streaming `execute_query` path. Full Flight SQL `DoGet` stream lifecycle accounting still needs finish-on-stream-close coverage.
- `read_rows`, `read_bytes`, `written_bytes`, and `memory_peak_bytes` are scaffolded but not yet populated from DataFusion execution-plan/runtime metrics.
- Retention config is persisted but the sweeper is not implemented yet.
- Distributed worker-side sibling rows are not emitted yet, though request structs now carry `initial_query_id` for propagation.
- Benchmark gates for query-log overhead are not wired into CI yet.
