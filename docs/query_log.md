# Query Log

Status: `Partial`

AnalyticsDB provides a durable query log. Each logged query is written asynchronously to local Parquet files under the catalog-managed data root, utilizing a `YYYY/MM/DD/` partitioned layout, and is exposed through SQL as:

```sql
SELECT * FROM system.query_log;
```

The current storage root is:

```text
<catalog-file-stem>.managed/system/query_log/
```

For example, a catalog at `/tmp/analyticsdb-catalog.json` writes query-log files under `/tmp/analyticsdb-catalog.managed/system/query_log/YYYY/MM/DD/`.

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

## Worker Rows

Distributed read queries write a sibling row per worker into `system.query_log`
with `is_initial_query = false` and `initial_query_id` set to the coordinator's
query ID. These rows carry the partition's `read_rows`, `read_bytes`, and
`duration_ms`.  Filter for coordinator rows with:

```sql
SELECT * FROM system.query_log WHERE is_initial_query = true;
```

Or explore all rows for a distributed query:

```sql
SELECT query_id, worker_node_id, is_initial_query, read_rows, duration_ms
FROM system.query_log
WHERE initial_query_id = '<coordinator-query-id>'
ORDER BY event_time_us;
```

## Current Gaps

- Streaming Flight SQL query finish is not yet logged (only non-streaming paths).
- DataFusion stage-level metrics (operator timings, spill bytes) are not yet enriched into rows.
- Retention sweeper and partitioned (`YYYY/MM/DD/`) layout are not yet implemented.
- Benchmark gates for query-log overhead are not wired into CI yet.

