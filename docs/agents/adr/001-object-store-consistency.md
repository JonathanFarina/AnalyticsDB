# ADR 001: Object-Store Consistency Model

**Status:** Decided  
**Blocks:** Phase B (Durable Object-Storage Storage Layer)

## Decision

Require **strong read-after-write consistency** from the object-store backend.
The manifest-based commit protocol in Phase B is an application-level safety
net for atomic multi-file transactions, not a workaround for eventual
consistency.

Supported backends at launch: Amazon S3, Google Cloud Storage, Azure Blob
Storage. All three have provided strong read-after-write consistency since
at least 2021 (S3 since November 2020; GCS and Azure always). Local
`file://` backed by `LocalFileSystem` from the `object_store` crate also
satisfies this.

## Why

Eventual consistency would require the manifest reader to retry on stale
reads, adding indeterminate latency and complicating the compaction and vacuum
logic. Strong consistency means "the manifest you just wrote is the manifest
everyone reads next" — the commit protocol can rely on that.

## Alternatives Rejected

- **Design for eventual consistency:** adds significant complexity (vector
  clocks, retry loops, read hedging) for no benefit on the target backends.
  Revisit only if a target backend without strong consistency (e.g. some
  on-prem MinIO deployments) becomes a priority.

## Consequences

- Backend selection in `store_for_location` must document which URIs are
  supported. Unsupported schemes fail at startup with a clear error, not
  silently at query time.
- Integration tests must run against a real S3-compatible backend (e.g.
  LocalStack or a cloud sandbox), not just `file://`, before Phase B is
  `Complete`.
- If a future backend without strong consistency is added, it must be gated
  behind an explicit `--experimental` flag and must not be used for managed
  tables without operator opt-in.
