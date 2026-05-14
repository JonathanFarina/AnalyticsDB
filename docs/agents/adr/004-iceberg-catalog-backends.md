# ADR 004: Iceberg Catalog Backends

**Status:** Decided  
**Blocks:** Phase F (External Tables And Storage Policy)

## Decision

Support **REST catalog only** at initial Iceberg launch. Other backends (AWS
Glue, Hive Metastore, Polaris, Nessie, Unity Catalog) may be added later as
pluggable drivers behind the same catalog trait, but are not required for Phase
F to be `Complete`.

The REST catalog spec is the authoritative, cloud-agnostic Iceberg catalog
interface. REST-compatible implementations include Apache Polaris, Project
Nessie, Databricks Unity Catalog, and most modern managed Iceberg platforms.

## Why

The REST catalog is:

- **Specification-driven:** backed by a formal OpenAPI spec, so any compliant
  implementation interoperates without per-backend code.
- **Cloud-agnostic:** no cloud SDK dependency required; works on-prem, in any
  cloud, and in local testing via a simple HTTP server.
- **Already the target for most new Iceberg deployments:** Glue and HMS are
  legacy paths; new platforms default to REST.

Starting with REST means we write the integration once and get compatibility
with a wide set of catalogs for free.

## Alternatives Rejected

- **AWS Glue:** Common in AWS shops. Add as a second driver in Phase F+1 if
  customer demand justifies it. The REST abstraction layer must not be
  designed in a way that makes Glue impossible to add later.
- **Hive Metastore:** Legacy. Significant operational complexity (Thrift
  protocol, HMS dependency). Low priority unless an existing customer requires
  it.
- **Nessie / Polaris / Unity Catalog (native APIs):** All expose REST catalog
  endpoints, so they are automatically supported without a native driver.

## Consequences

- The external table integration (`CREATE EXTERNAL TABLE ... USING ICEBERG`)
  takes a `catalog_uri` option pointing to a REST catalog endpoint and an
  optional `warehouse` path.
- Credentials for the REST catalog (bearer tokens, OAuth2 client credentials)
  are referenced by name from the secrets abstraction defined in Phase D, not
  embedded in DDL.
- Phase F integration tests must run against a locally embedded REST catalog
  (e.g. a minimal test fixture or an embedded Polaris instance) in CI, not
  against a real cloud catalog.
- When a future backend (e.g. Glue) is added, it must satisfy the same Phase F
  parity test matrix — identical SQL results across REST and Glue-backed
  tables for the supported surface.
- Schema evolution for Iceberg (adding/dropping columns, partition spec
  changes) is out of scope for Phase F. Phase F is read-only Iceberg access
  only. Write support is a follow-up workstream.

## Supported Iceberg Version

Iceberg **v2** format spec. v1 tables may be read if the REST catalog
advertises them, but v2 is the required baseline. Agents must not claim v1-
only features as complete v2 support.
