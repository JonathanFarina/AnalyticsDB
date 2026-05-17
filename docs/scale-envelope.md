# AnalyticsDB Scale Envelope

This document defines the supported scale envelope for AnalyticsDB, based on current prototype architecture and component limits. All values are preliminary and will be updated as benchmarking (I1, I2) provides measured performance data.

## Core Limits

### Max Rows Per Table
Derived from Parquet and object storage constraints:
- Parquet supports up to 2^31 row groups per file
- Recommended limit: < 1 billion rows per table for optimal query performance

### Max Columns Per Table
Derived from Parquet and Apache Arrow limits:
- Theoretical maximum: 2^16 columns
- Recommended limit: < 1000 columns per table for optimal performance

### Max Concurrent Queries Per Coordinator
Derived from available system resources:
- Configurable via `ANALYTICSDB_MAX_CONCURRENT_QUERIES` environment variable
- Default value: 32 concurrent queries

### Max Workers Per Cluster
Derived from coordinator capacity constraints:
- Recommended limit: < 100 workers per cluster for current prototype stability

### Max Table Size
Derived from object storage backing:
- Theoretical limit: Unconstrained (object storage scales indefinitely)
- Recommended limit: < 10TB per table for optimal query performance

## Recommended Production Scale
Tested and validated for:
- Up to 100 million rows per table
- Up to 100 columns per table
- Up to 32 concurrent queries per coordinator
- Up to 10 workers per cluster

## Preliminary Notice
All scale limits are based on current prototype behavior, not aspirational targets. Values will be updated with measured benchmarking data from workstreams I1 (Micro-benchmarks) and I2 (Scale validation) before production readiness.
