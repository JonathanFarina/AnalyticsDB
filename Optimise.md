# AnalyticsDB Performance Optimization Plan

## Overview
This document outlines 10 identified performance bottlenecks in the distributed query engine, ranked by impact and implementation priority. Each optimization is designed to achieve sub-second query latency on large datasets (100M–1B+ rows) across 2–10 compute nodes.

**Current State**: Coordinator is single-threaded (target_partitions=1), worker results buffer fully in memory, fresh TCP connections per request, file lists queried on every operation.

**Goal**: Enable true distributed parallelism with streaming result pipelines, connection reuse, and aggressive caching.

---

## Tier 1: Critical Fixes 🔴
These fixes unlock massive speedups and must be completed first.

### 1. Enable Multicore Coordinator Execution
**Status**: Identified, not yet fixed  
**Impact**: 4–8x speedup on multi-node queries

**Problem**:
- Line `crates/analyticsdb-engine/src/lib.rs:3696` forces `target_partitions(1)`, serializing all work.
- Coordinator bottlenecks on a single CPU core regardless of cluster size or file count.
- Example: 100 files across 3 workers takes as long as local single-core execution.

**Fix**:
```rust
// OLD (line 3696)
config.options_mut().execution.target_partitions = 1;

// NEW
use num_cpus;
config.options_mut().execution.target_partitions = num_cpus::get();
```

**Why it works**: DataFusion's execution layer parallelizes across target_partitions threads. Restoring this allows the coordinator to max out available CPU while awaiting worker results.

**Estimated impact**:
- 2-core: ~2x
- 4-core: ~4x
- 8-core: ~6–8x (diminishing returns from network I/O)

**Implementation**: Single-line change, no risky refactoring. Test with `cargo test` and local cluster.

**Risk**: Low. This is DataFusion's intended behavior; we only disabled it as a debugging step.

---

### 2. Stream Worker Results Instead of Buffering
**Status**: Identified, not yet fixed  
**Impact**: 2–4x speedup on large result sets, prevents OOM

**Problem**:
- Lines `crates/analyticsdb-engine/src/distributed.rs:156–179` collect *all* worker batches into a `Vec<RecordBatch>` before returning.
- Lines `crates/analyticsdb-engine/src/protocol.rs:1755–1787` encode all batches into a single IPC blob.
- SELECT with 1B rows returns 100k+ batches; coordinator RAM explodes.
- Blocks result streaming to client until the last worker finishes.

**Current flow**:
```
Worker 1: Produces 1000 batches
  ↓ (buffered in memory)
Worker 2: Produces 1000 batches
  ↓ (buffered in memory)
Worker 3: Produces 1000 batches
  ↓ (all concatenated, sent to client at once)
```

**Fix**:
1. Modify `PartitionClient::execute_on_node()` to return a streaming iterator instead of `Vec<RecordBatch>`.
2. Change `ExecutePartition` DoAction response to use Flight streaming (`FlightData` messages one batch at a time).
3. Use `FlightDataEncoderBuilder` (already used in DoGet handlers, lines ~1725–1750) for IPC streaming.
4. Return a unified stream that merges results from all workers.

**Pseudo-code**:
```rust
// OLD
pub async fn execute_on_node(...) -> Result<Vec<RecordBatch>> {
    let mut all_batches = Vec::new();
    while let Some(result) = stream.next().await {
        all_batches.extend(ipc_bytes_to_batches(&result.body)?);
    }
    Ok(all_batches)
}

// NEW
pub async fn execute_on_node(...) -> Result<impl Stream<Item = Result<RecordBatch>>> {
    // Return an async stream; coordinator starts consuming results immediately
    // as workers produce them.
}
```

**Where to refactor**:
- `distributed.rs`: Return `Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>` from `execute_on_node()`.
- `protocol.rs:1755–1787`: Switch from single-blob encoding to streaming `FlightData` responses.
- `lib.rs:1077–1097`: Change concatenation to `futures::stream::select_all()` or similar.

**Estimated impact**:
- Small result sets (< 100k rows): ~10% speedup (less buffering overhead).
- Large result sets (100M+ rows): 2–4x (prevents OOM, enables pipelined processing).
- Client streaming: Results begin arriving immediately instead of waiting for all workers.

**Risk**: Medium. Changes async flow; requires careful stream handling to avoid resource leaks. Test with large result sets.

---

### 3. Eliminate Fresh TCP/TLS per Request
**Status**: Identified, not yet fixed  
**Impact**: 1.5–3x speedup on small queries, reduces latency

**Problem**:
- Lines `crates/analyticsdb-engine/src/distributed.rs:130–151, 155–179` create a fresh tonic channel per `execute_on_node()` or `write_on_node()` call.
- Building a channel includes DNS resolution, TCP SYN-ACK handshake, TLS negotiation (if https).
- Overhead: ~50–200ms per request depending on network latency.
- Example: 100-file query with 10 workers = 10 new connections = 500ms+ lost to handshakes.

**Current code** (lines 130–136):
```rust
pub async fn write_on_node(...) -> Result<ExecutePartitionWriteAck> {
    let channel = build_channel(node_endpoint).await?;  // NEW CONNECTION
    let mut client = FlightSqlServiceClient::new(channel);
    // ...
}
```

**Fix**:
1. Add a `DashMap<String, tonic::Channel>` to `PartitionClient` (thread-safe, lock-free).
2. Implement a `get_or_create_channel()` method with basic health checking (optional).
3. Reuse channels across requests; drop only on `ClusterNode` removal.

**Pseudo-code**:
```rust
pub struct PartitionClient {
    control_plane: Arc<ControlPlane>,
    channels: DashMap<String, tonic::transport::Channel>,  // NEW
}

impl PartitionClient {
    async fn get_or_create_channel(&self, endpoint: &str) -> Result<tonic::transport::Channel> {
        if let Some(ch) = self.channels.get(endpoint) {
            return Ok(ch.clone());
        }
        let ch = build_channel(endpoint).await?;
        self.channels.insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }
}
```

**Estimated impact**:
- Per-request latency: Save 50–200ms.
- 100-partition query: 500ms–2s saved.
- Coordinated writes: 1.5–3x faster (connection pool amortizes setup cost).

**Risk**: Low. Channels are cloneable in tonic. Just ensure proper cleanup on node removal.

---

## Tier 2: High-Value Fixes 🟠
These provide significant speedups and should follow Tier 1.

### 4. Implement Size-Aware Partition Assignment
**Status**: Identified, not yet fixed  
**Impact**: 1.5–2x more balanced load distribution

**Problem**:
- Lines `crates/analyticsdb-engine/src/distributed.rs:43–52` use simple round-robin partitioning.
- Doesn't account for file sizes; a 10GB file and 100MB file are assigned identically.
- Result: Worker 1 processes 10GB, Worker 2 processes 100MB; Worker 2 finishes first and idles.
- On heterogeneous files, load imbalance can lose 30–50% of potential speedup.

**Current code** (lines 43–52):
```rust
pub fn partition_files_for_workers(files: Vec<String>, num_workers: usize) -> Vec<Vec<String>> {
    let buckets = num_workers.min(files.len());
    let mut chunks: Vec<Vec<String>> = (0..buckets).map(|_| Vec::new()).collect();
    for (i, file) in files.into_iter().enumerate() {
        chunks[i % buckets].push(file);  // Round-robin, no size awareness
    }
    chunks
}
```

**Fix**:
1. Accept a `file_sizes: &[(String, u64)]` parameter (filename, byte size).
2. Use greedy assignment: sort files by size descending, assign each file to the bucket with the smallest total.

**Pseudo-code**:
```rust
pub fn partition_files_for_workers(
    files: Vec<(String, u64)>, // (filename, size)
    num_workers: usize,
) -> Vec<Vec<String>> {
    if files.is_empty() || num_workers == 0 {
        return Vec::new();
    }
    let buckets = num_workers.min(files.len());
    let mut chunks: Vec<Vec<String>> = (0..buckets).map(|_| Vec::new()).collect();
    let mut bucket_sizes: Vec<u64> = vec![0; buckets];
    
    // Sort by size descending
    let mut sorted = files;
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    
    // Greedy assignment
    for (file, size) in sorted {
        let min_idx = bucket_sizes.iter().enumerate()
            .min_by_key(|(_,s)| *s)
            .map(|(i,_)| i)
            .unwrap();
        chunks[min_idx].push(file);
        bucket_sizes[min_idx] += size;
    }
    chunks
}
```

**Where to update**:
- `crates/analyticsdb-engine/src/lib.rs` near line `~1011`: Call `list_parquet_files()` (already lists files) and fetch their sizes via object store metadata.
- Pass sizes to `partition_files_for_workers()`.

**Estimated impact**:
- Homogeneous file sizes: ~0% (round-robin already balanced).
- 10:1 size variance: ~1.5x more balanced (reduces straggler effect).
- 100:1 size variance: ~2x speedup (near-optimal load balancing).

**Risk**: Low. Deterministic algorithm; easy to test with synthetic file lists.

---

### 5. Cache File Lists with Version Epoch
**Status**: Identified, not yet fixed  
**Impact**: 10–100x speedup on repeated queries

**Problem**:
- Lines `crates/analyticsdb-engine/src/lib.rs:~1011` call `list_parquet_files()` on *every* SELECT.
- `list_parquet_files()` (in `storage.rs`) lists all objects in the table prefix via object store API.
- For tables with 1000+ files, this involves scanning the entire prefix; even with pagination, takes 50–500ms.
- Example: 10 queries on the same table = 500–5000ms wasted on redundant listing.

**Current code** (approx. line 1011):
```rust
let files = self.storage.list_parquet_files(&table_name).await?;
```

**Fix**:
1. Add a `DashMap<String, (Epoch, Vec<String>)>` to the session or engine (filename → files, with version epoch).
2. After INSERT/UPDATE/DELETE, bump the epoch for that table.
3. On SELECT, check epoch:
   - If epoch unchanged, reuse cached file list.
   - If changed, call `list_parquet_files()`, update cache, bump epoch.

**Pseudo-code**:
```rust
pub struct FileListCache {
    cache: DashMap<String, (u64, Vec<String>)>,  // table → (epoch, files)
    epochs: DashMap<String, u64>,  // table → current epoch
}

impl FileListCache {
    pub async fn get_or_list(
        &self,
        table: &str,
        storage: &Storage,
    ) -> Result<Vec<String>> {
        let current_epoch = *self.epochs.get(table).unwrap_or(&0);
        if let Some((cached_epoch, files)) = self.cache.get(table) {
            if cached_epoch == current_epoch {
                return Ok(files.clone());  // Cache hit
            }
        }
        let files = storage.list_parquet_files(table).await?;
        self.cache.insert(table.to_string(), (current_epoch, files.clone()));
        Ok(files)
    }
    
    pub fn invalidate(&self, table: &str) {
        let new_epoch = self.epochs.get(table).map(|e| *e + 1).unwrap_or(1);
        self.epochs.insert(table.to_string(), new_epoch);
    }
}
```

**Where to integrate**:
- Add `file_list_cache: Arc<FileListCache>` to engine struct (lines ~3600).
- Replace `self.storage.list_parquet_files()` with `self.file_list_cache.get_or_list()` (line ~1011).
- Call `file_list_cache.invalidate(table)` after INSERTs/UPDATEs/DELETEs.

**Estimated impact**:
- First query: No change (cache miss).
- Repeated queries (100 times on same table): 10–100x faster (list is O(1) instead of O(n files)).
- Mixed workload (multiple tables): ~5–10x on read-heavy workloads.

**Risk**: Low. Simple versioning; invalidation is straightforward. Test with concurrent INSERT + SELECT.

---

### 6. Batch Parquet Writes into Row Groups
**Status**: Identified, not yet fixed  
**Impact**: 2–5x faster INSERTs, 10–100x smaller file count

**Problem**:
- Lines `crates/analyticsdb-engine/src/lib.rs:~520–530` write one Parquet file per batch.
- Typical batch is 8192 rows; 1M-row INSERT = 122 Parquet files (122KB–1MB each).
- Table with 1B rows eventually has 1M+ tiny files; metadata overhead and compaction become bottlenecks.
- Object store API call per file (PUT) adds latency.

**Current code** (approx. lines 520–530):
```rust
for batch in output_batches {
    let uuid = uuid::Uuid::new_v4().to_string();
    let file_path = format!("{}/{}.parquet", write_prefix, uuid);
    // Write single batch to file
    write_parquet_file(&file_path, &batch)?;
}
```

**Fix**:
1. Buffer batches until they accumulate to a target size (e.g., 128MB or 1M rows).
2. Write once per target threshold.

**Pseudo-code**:
```rust
const TARGET_ROW_GROUP_ROWS: usize = 1_000_000;
let mut accumulated = Vec::new();
let mut row_count = 0;

for batch in output_batches {
    accumulated.push(batch.clone());
    row_count += batch.num_rows();
    
    if row_count >= TARGET_ROW_GROUP_ROWS {
        // Concatenate accumulated batches and write once
        let combined = datafusion::arrow::compute::concat_batches(
            &accumulated[0].schema(),
            &accumulated,
        )?;
        let uuid = uuid::Uuid::new_v4().to_string();
        let file_path = format!("{}/{}.parquet", write_prefix, uuid);
        write_parquet_file(&file_path, &combined)?;
        
        accumulated.clear();
        row_count = 0;
    }
}

// Flush remainder
if !accumulated.is_empty() {
    // Write remaining batches
}
```

**Where to update**:
- `crates/analyticsdb-engine/src/lib.rs` around `execute_distributed_write_partition()` (lines ~494–536).
- `execute_partition()` for local writes (lines ~441–486).

**Estimated impact**:
- 1M-row INSERT: 122 files → 1 file (122x reduction in file count).
- Write latency: 50–100ms (one object store PUT instead of 122).
- Query latency: Reduced file metadata overhead.
- Compaction: Fewer tiny files → less aggressive maintenance.

**Risk**: Medium. Requires careful handling of:
- Batch concatenation alignment (ensure schema consistency).
- OOM on large row groups (cap accumulation buffer).
- Handling remainder batches on incomplete row groups.

---

### 7. Fix Local INSERT Fallback Path
**Status**: Identified, not yet fixed  
**Impact**: Consistency with distributed path, prevents regression

**Problem**:
- Lines `crates/analyticsdb-engine/src/lib.rs:~1565+` contain a local (non-distributed) INSERT path that also materializes all output batches.
- This fallback is used when a table has no files or coordinator opts for local execution.
- Same OOM risk as distributed path; should be fixed in parallel.

**Fix**:
- Apply the same streaming writer technique as fix #6 to the local fallback.
- Ensure both paths use identical batch buffering logic.

**Estimated impact**:
- Local INSERTs: 2–5x faster, prevents OOM on large batches.
- Code consistency: Reduces maintenance burden.

**Risk**: Low. Same logic as fix #6, just in a different code path.

---

## Tier 3: Medium-Value Fixes 🟡
These improve efficiency and should be completed after Tier 2.

### 8. Use Bincode Instead of JSON for Payloads
**Status**: Identified, not yet fixed  
**Impact**: 10–20% faster serialization, 20–30% smaller payloads

**Problem**:
- Lines `crates/analyticsdb-engine/src/distributed.rs:140–141, 165–166` use `serde_json::to_vec(req)?` to serialize `ExecutePartitionRequest` and `ExecutePartitionWriteRequest`.
- JSON is text-based and verbose; `ExecutePartitionRequest` serialized to ~500–1000 bytes.
- Network serialization is not a critical path, but small gains add up on high-frequency operations.

**Current code** (line 140):
```rust
body: serde_json::to_vec(req)?.into(),
```

**Fix**:
```rust
body: bincode::serialize(req)?.into(),
```

**Where to update**:
- `crates/analyticsdb-engine/src/distributed.rs:140–141` (ExecutePartitionWrite).
- `crates/analyticsdb-engine/src/distributed.rs:165–166` (ExecutePartition).
- `crates/analyticsdb-engine/src/protocol.rs:~1760` (worker-side deserialization).

**Dependencies**:
- Add `bincode = "1.3"` to `Cargo.toml`.

**Estimated impact**:
- Serialization time: ~10–20% faster (binary format is more compact).
- Network payload: ~20–30% smaller.
- Practical benefit: Minimal (serialization is microseconds, not seconds).

**Risk**: Very low. Bincode is a standard Rust serialization format. Test with round-trip serde tests.

---

### 9. Make Index Rebuild Lazy
**Status**: Identified, not yet fixed  
**Impact**: 10–30% faster INSERTs (if indexes are enabled)

**Problem**:
- Lines `crates/analyticsdb-engine/src/lib.rs:~1250` may rebuild indexes synchronously after INSERT.
- If the table has indexes, this blocks the INSERT from completing until all index updates are done.
- For large batches, index rebuild can take 100–500ms.

**Current behavior**:
```
INSERT → Execute distributed write → Rebuild index (blocks) → Return to client
```

**Fix**:
- Make index rebuild asynchronous or lazy.
- Option A: Spawn a background task that rebuilds indexes after INSERT returns.
- Option B: Defer index rebuild until the next query (delta-merge approach).

**Pseudo-code** (Option A):
```rust
// After distributed write completes
let engine = self.clone();
let table = table_name.to_string();
tokio::spawn(async move {
    if let Err(e) = engine.rebuild_indexes(&table).await {
        eprintln!("Index rebuild failed for {}: {}", table, e);
    }
});
return Ok(write_result);  // Don't wait
```

**Estimated impact**:
- INSERTs with indexes: 10–30% faster (no blocking rebuild).
- Trade-off: Index queries slightly stale (old data not indexed until next read).

**Risk**: Medium. Lazy rebuild can lead to inconsistency if queries assume all data is indexed. Requires careful cache invalidation.

---

### 10. Make Coordinator a Compute-Eligible Node
**Status**: Identified, not yet fixed  
**Impact**: 1.5–2x speedup on small clusters (2–3 nodes)

**Problem**:
- Currently, the Coordinator only dispatches queries; it never executes partitions locally.
- With 2 nodes (1 coordinator, 1 compute), all work lands on the compute node; coordinator CPU idle.
- For OLAP workloads with local indexing, moving some work to the coordinator improves throughput.

**Current behavior**:
```
Coordinator (idle) → Compute Node (100% busy)
```

**Fix**:
1. Add a flag `is_compute_eligible` to the coordinator config.
2. When assigning partitions, include the coordinator's local endpoint.
3. Let the coordinator execute some partitions locally (same `execute_partition()` path as compute nodes).

**Pseudo-code**:
```rust
pub async fn list_compute_nodes(&self) -> Result<Vec<ClusterNode>> {
    let mut nodes = self.control_plane.list_nodes().await?
        .into_iter()
        .filter(|n| n.role == NodeRole::Compute && n.status == NodeStatus::Ready)
        .collect::<Vec<_>>();
    
    if self.config.is_compute_eligible && self.control_plane.is_coordinator() {
        nodes.push(ClusterNode {
            id: "local-coordinator".to_string(),
            endpoint: "localhost:50051".to_string(),  // Local endpoint
            role: NodeRole::Compute,
            status: NodeStatus::Ready,
            // ...
        });
    }
    Ok(nodes)
}
```

**Where to integrate**:
- `crates/analyticsdb-engine/src/lib.rs`: Modify `list_compute_nodes()` or partition assignment logic.
- `crates/analyticsdb-control/src/lib.rs`: Add `is_compute_eligible` config field.

**Estimated impact**:
- 2-node cluster: 1.5–2x throughput (coordinator no longer idle).
- 3+ node cluster: ~5–10% improvement (coordinator now handles 20–30% of load).

**Risk**: Medium. Coordinator becomes CPU-heavy; may impact control plane responsiveness (heartbeats, leader election). Recommend a dedicated control-plane thread pool.

---

## Bonus: Architectural Improvements

### A. Distributed Physical Plan Optimization
**Scope**: Beyond this stage, but worth noting.

The current query flow:
```
1. Coordinator parses SQL
2. Coordinator rewrites SQL for each partition
3. Workers execute identical SQL on different files
```

Future improvement: Send the physical plan (not SQL) to workers, letting them optimize locally. This enables:
- Pre-compiled execution (no per-query parsing).
- Worker-local predicate push-down below the broadcast threshold.
- Aggregation push-down (GROUP BY, COUNT on workers before coordinator merge).

---

## Implementation Roadmap

### Phase 1: Critical Fixes
1. Enable multicore coordinator (5 min)
2. Stream worker results (4–6 hours)
3. Eliminate fresh TCP/TLS (2–3 hours)

**Expected result**: 4–16x speedup on multi-node queries, OOM prevention.

---

### Phase 2: High-Value Fixes
4. Size-aware partition assignment (2–3 hours)
5. Cache file lists with epochs (3–4 hours)
6. Batch Parquet writes (4–6 hours)
7. Fix local INSERT fallback (1–2 hours)

**Expected result**: 2–5x faster INSERTs, 10–100x smaller file counts, load-balanced queries.

---

### Phase 3: Medium-Value Fixes 
8. Use bincode for payloads (1–2 hours)
9. Make index rebuild lazy (2–3 hours)
10. Coordinator as compute-eligible (3–4 hours)

**Expected result**: 10–30% latency improvements, better CPU utilization.

---

## Testing Strategy

- **Unit tests**: Verify round-robin → size-aware partitioning; file list cache invalidation.
- **Integration tests**: Run 2–3 node cluster; execute queries with 100M–1B rows; monitor memory, latency.
- **Benchmark suite**: Compare before/after latency, throughput, and file count.
- **Regression tests**: Ensure local INSERT, distributed INSERT, and mixed workloads still pass.

---

## Success Criteria

| Optimization | Target | Measurement |
|---|---|---|
| Multicore coordinator | 4–8x | Wall-clock time on multi-partition queries |
| Streaming results | 2–4x | Large result sets; peak memory usage |
| Connection pooling | 1.5–3x | Per-request latency on small queries |
| Size-aware partitioning | 1.5–2x | Load distribution variance across workers |
| File list cache | 10–100x | Latency on repeated queries (same table) |
| Batched writes | 2–5x | INSERT latency; file count reduction |
| Lazy index rebuild | 10–30% | INSERT latency (with indexes enabled) |
| Compute-eligible coordinator | 1.5–2x | Throughput on 2–3 node clusters |

---

## Risk Matrix

| Fix | Risk | Mitigation |
|---|---|---|
| Multicore | Low | Same as DataFusion defaults; well-tested. |
| Streaming | Medium | Careful async handling; test with large result sets. |
| Connection pool | Low | Standard pattern; verify health checks. |
| Size-aware partition | Low | Greedy algorithm; easy to verify. |
| File list cache | Low | Simple versioning; test concurrent operations. |
| Batched writes | Medium | Batch alignment; OOM caps; remainder handling. |
| Lazy index | Medium | Inconsistency risks; requires cache invalidation. |
| Compute-eligible | Medium | Coordinator load balancing; monitor control plane. |

---

## Expected Aggregate Speedup

Assuming sequential implementation of all fixes:

| Phase | Cumulative Speedup | Notes |
|---|---|---|
| Phase 1 (critical) | **4–16x** | Multi-core + streaming + connection reuse. |
| Phase 2 (high-value) | **8–40x** | Phase 1 + balanced load + smaller file metadata. |
| Phase 3 (medium) | **10–50x** | Marginal gains; efficiency improvements. |

**Realistic expectation**: 8–20x speedup on typical OLAP workloads (multi-node, large result sets, repeated queries).
