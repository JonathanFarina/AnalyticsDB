# ADR 003: Cluster CA And TLS Certificate Lifecycle

**Status:** Decided  
**Blocks:** Phase C (intra-cluster mTLS), Phase H (Kubernetes deployment)

## Decision

Use **cert-manager on Kubernetes** for automatic certificate issuance and
rotation. For local development and non-Kubernetes installs, ship a
**bundled self-signed CA tool** (`analyticsdb-cli ca init`) that generates a
cluster CA and per-node leaf certificates with a configurable validity window.

### Kubernetes path

- A `ClusterIssuer` backed by cert-manager's self-signed CA generator creates
  the cluster CA.
- Each coordinator and compute pod gets a `Certificate` resource that
  cert-manager rotates automatically before expiry.
- Certificates are mounted as Kubernetes Secrets into the pod's filesystem;
  the node binary watches for file changes and reloads TLS material without
  restart.
- Nodes present their leaf cert as both TLS server and mTLS client on the
  internal channel. Peers verify the CA chain, not the specific leaf — any
  cert signed by the cluster CA is trusted for intra-cluster communication.

### Non-Kubernetes / dev path

- `analyticsdb-cli ca init --output ./certs` generates:
  - `ca.crt`, `ca.key` — the cluster CA
  - `server.crt`, `server.key` — a wildcard leaf for local use (valid for
    `localhost`, `127.0.0.1`, and `*.analyticsdb.local`)
- The existing bundled `certs/` in the repo is the current prototype of this.
  It needs to be replaced by an actual tool rather than committed key material.
- Committed key material (the current `certs/` directory) must be removed from
  the repository once the tool exists — committed private keys are a security
  anti-pattern even for dev.

## Why

cert-manager is the de-facto standard for certificate management in Kubernetes
and handles the entire rotation lifecycle automatically. The self-signed CA
tool covers the non-k8s case without adding an external dependency like Vault
at install time.

## Alternatives Rejected

- **HashiCorp Vault PKI:** More powerful (supports intermediate CAs, audit,
  multi-cluster rotation). Adds Vault as a hard prerequisite before any
  cluster can start. Revisit as an opt-in integration after Phase H ships.
- **External enterprise PKI:** Same concern — adds a prerequisite. Supported
  as an opt-in by allowing operators to provide pre-issued PEM bundles instead
  of running the CA tool.

## Consequences

- The `NoVerifier` in
  [`distributed.rs:312`](../../crates/analyticsdb-engine/src/distributed.rs)
  must be replaced with a real CA-verifying rustls config as part of Phase C.
  Nodes that cannot present a cert signed by the cluster CA must be rejected
  at the TLS handshake, not just logged.
- Node bootstrap (`--join`) must receive the cluster CA certificate from the
  coordinator (already implicit in the current TLS flow) so that nodes can
  verify peers without a shared filesystem.
- Certificate rotation must not drop in-flight connections. The TLS acceptor
  must support live reload (watch the cert file; swap the Arc<ServerConfig> on
  change).
- CI uses the bundled dev cert for intra-cluster tests; a separate CI job must
  rotate the cert mid-test and assert no query is dropped, before Phase C is
  `Complete`.

## Certificate Validity Defaults

| Context | Default validity | Rotation trigger |
|---|---|---|
| Cluster CA (k8s) | 10 years | Manual or cert-manager re-issue |
| Leaf cert (k8s) | 90 days | cert-manager auto-rotate at 75% |
| Dev CA (CLI tool) | 1 year | Re-run `ca init` |
| Dev leaf | 1 year | Re-run `ca init` |
