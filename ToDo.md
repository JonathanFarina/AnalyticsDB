# Multi-Node Distributed Execution Plan

This document tracks the tasks required to transition the `analyticsdb` engine from single-node execution to a distributed scatter-gather architecture.

## Phase 1: Shared Object Storage Abstraction
**Goal:** Enable all compute nodes to access shared table data, supporting both local filesystem and cloud object storage.
- [x] Initialize `object_store` configuration based on the table's `storage_path` (e.g., `file://`, `s3://`).
- [x] Refactor `local_managed_storage_path` and existing `std::fs` / `tokio::fs` usages to utilize the `object_store` API.
- [x] Update `append_record_batch_to_table_snapshot`, `write_index_snapshot`, and snapshot cleanup functions to write directly via the object store interface.
- [x] Verify that existing single-node tests still pass with the new local `object_store` implementation.

## Phase 2: Inter-node Communication (Arrow Flight)
**Goal:** Establish a mechanism for the Coordinator to dispatch sub-queries to Compute nodes.
- [ ] Define a custom Flight `DoAction` (e.g., `ExecutePartition`) in the `analyticsdb-server` / `analyticsdb-protocol` crates.
- [ ] Implement serialization/deserialization for task payloads (SQL/plan fragment + specific target Parquet files).
- [ ] Implement the worker-side execution of `ExecutePartition` that reads the assigned files and streams results back.
- [ ] Implement the Coordinator-side client logic to discover Compute nodes from Raft and establish Flight connections.

## Phase 3: Distributed Query Planning (Coordinator)
**Goal:** The Coordinator splits the query and merges the results.
- [ ] Enhance `execute_query` to identify if a query can be distributed.
- [ ] Implement a partitioner that divides the target table's Parquet files into chunks.
- [ ] Dispatch the chunks concurrently to the available Compute nodes via the Flight client.
- [ ] Implement a stream merger on the Coordinator to combine the incoming Flight streams from workers and apply final aggregations/sorting.

## Phase 4: Distributed Writes (Parallel Inserts)
**Goal:** Distribute `INSERT INTO ... SELECT` execution.
- [ ] Modify the distributed planner to instruct workers to write their output directly to the shared object store as distinct Parquet files.
- [ ] Collect success acknowledgments from all workers.
- [ ] Have the Coordinator commit the new files to the table's manifest/metadata in Raft and update index sidecars.
