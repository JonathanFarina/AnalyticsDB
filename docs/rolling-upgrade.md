# Rolling Upgrade Procedure

This document describes the rolling upgrade procedure for AnalyticsDB clusters
deployed on Kubernetes. A rolling upgrade allows you to update nodes without
dropping in-flight queries when combined with the graceful shutdown and
cancellation features implemented in Phase C2 and Phase C.

## Overview

AnalyticsDB supports rolling upgrades through:

1. **Health endpoints** (`/healthz`, `/readyz`) that Kubernetes probes use to
   determine node readiness.
2. **Graceful shutdown** (SIGTERM/SIGINT handlers) that cancel in-flight
   queries before exit.
3. **Readiness probe behaviour** that prevents traffic routing to nodes that
   are not ready.

## Pre-Upgrade Checklist

- [ ] Review the new release notes for breaking changes.
- [ ] Ensure object-store credentials and TLS certificates are valid for the
      new version.
- [ ] Take a backup of the catalog (SQLite or JSON) if running a single
      coordinator.
- [ ] Verify that the cluster is healthy:
  ```bash
  kubectl get pods -n analyticsdb
  kubectl get nodes  # if using bare metal
  ```

## Step-by-Step Rolling Upgrade

### Order: Control Plane First, Then Compute Pool

Always upgrade the **control plane** (coordinator) before the **compute pool**.
The control plane registers nodes and dispatches queries; upgrading it first
ensures protocol compatibility with newer compute nodes.

### 1. Upgrade Control Plane

#### Using Helm

```bash
# Update the Helm repository
helm repo update

# Upgrade the control plane with the new image version
helm upgrade analyticsdb-control \
  ./helm/analyticsdb \
  --namespace analyticsdb \
  --set controlPlane.image.tag=v0.6.0 \
  --set controlPlane.replicaCount=1 \
  -f values.yaml
```

#### Using kubectl rollout restart

```bash
# Trigger a rolling restart of the control-plane Deployment
kubectl rollout restart deployment/analyticsdb-control \
  --namespace analyticsdb

# Watch the rollout status
kubectl rollout status deployment/analyticsdb-control \
  --namespace analyticsdb --timeout=300s
```

### 2. Upgrade Compute Pool

#### Using Helm

```bash
helm upgrade analyticsdb-compute \
  ./helm/analyticsdb \
  --namespace analyticsdb \
  --set compute.image.tag=v0.6.0 \
  --set compute.replicaCount=3 \
  -f values.yaml
```

#### Using kubectl rollout restart

```bash
# Trigger a rolling restart of the compute StatefulSet or Deployment
kubectl rollout restart statefulset/analyticsdb-compute \
  --namespace analyticsdb

# Watch the rollout status
kubectl rollout status statefulset/analyticsdb-compute \
  --namespace analyticsdb --timeout=300s
```

## How Health Endpoints Support Rolling Upgrades

### Liveness (`/healthz`)

- Always returns HTTP 200 with body `OK\n`.
- Indicates the process is alive and the HTTP server is accepting connections.
- Kubernetes uses this for the `livenessProbe` to decide when to restart a
  container that has frozen.

### Readiness (`/readyz`)

- Returns HTTP 200 `OK\n` when the node has finished bootstrap and is ready
  to accept queries.
- Returns HTTP 503 `NOT READY\n` when the node is still starting up or has
  been marked not-ready.
- Kubernetes uses this for the `readinessProbe` to decide when to route
  traffic to the pod.

### Rolling Upgrade Behaviour

During a rolling upgrade, Kubernetes:

1. Starts a new pod with the updated image.
2. Waits for the new pod's `readinessProbe` to succeed (meaning the node has
   marked itself ready).
3. Only then terminates the old pod.

Because the old pod receives a SIGTERM when Kubernetes decides to terminate it,
the AnalyticsDB server:

1. Catches the SIGTERM signal in `shutdown_signal()`.
2. Calls `engine.cancel_all_queries()` to abort in-flight queries.
3. Aborts background tasks (heartbeat, pruner).
4. Exits cleanly.

Clients with in-flight queries will receive a cancellation error and can
retry against the new pod (which is already ready).

## Verification Steps

### 1. Check Rollout Status

```bash
kubectl rollout status deployment/analyticsdb-control \
  --namespace analyticsdb

kubectl rollout status statefulset/analyticsdb-compute \
  --namespace analyticsdb
```

### 2. Check Pod Health

```bash
kubectl get pods -n analyticsdb -o wide

# Verify all pods show Ready and Running
# Check the IMAGE column to confirm the new version
```

### 3. Check Logs for Clean Shutdown

```bash
# Check that the old pods logged the shutdown signal
kubectl logs -n analyticsdb deployment/analyticsdb-control --previous \
  | grep "Shutdown signal received"

# Check that in-flight queries were cancelled
kubectl logs -n analyticsdb deployment/analyticsdb-control --previous \
  | grep "cancelling in-flight queries"
```

### 4. Run Queries During Upgrade (Manual Verification)

```bash
# Terminal 1: Start a long-running query
analyticsdb-cli --host <pg-endpoint> --port 5432 \
  -c "SELECT pg_sleep(60);"

# Terminal 2: Trigger rolling upgrade while the query is running
kubectl rollout restart statefulset/analyticsdb-compute -n analyticsdb

# Expected: The query either completes before the restart, or is cancelled
# with a clear error message.
```

### 5. Verify Cluster State After Upgrade

```bash
# Connect via CLI and check cluster members
analyticsdb-cli --host <endpoint> --port 5432 \
  -c "SELECT * FROM system.query_log ORDER BY started_at DESC LIMIT 5;"

# Check that all nodes appear in the control plane
# (This requires a cluster membership SQL interface or logs inspection)
```

## In-Flight Query Handling

### Cancellation (Phase C2)

AnalyticsDB implements a `CancellationToken` that propagates from query
admission through the distributed execution path. When a node receives
SIGTERM:

1. `shutdown_signal()` resolves.
2. `engine.cancel_all_queries()` is called in `main.rs`.
3. All active `CancellationToken`s are cancelled.
4. Distributed workers watching the token abort their partition execution.
5. Clients receive an error indicating the query was cancelled.

### Graceful Shutdown (`main.rs`)

The shutdown sequence in `main.rs` ensures:

- All in-flight queries are cancelled before the process exits.
- Background tasks (heartbeat, node pruner) are aborted.
- The readiness flag is set to `false` (via dropping `ready_tx`), causing
  `/readyz` to return 503 immediately.

### What To Expect

| Scenario | Behaviour |
|----------|-----------|
| Short query (< SIGTERM arrival) | Query completes normally |
| Long query (running when SIGTERM arrives) | Query is cancelled; client receives error |
| Idle connection | Connection is closed after queries are cancelled |
| Distributed query (coordinator + workers) | Coordinator cancels, workers detect token cancellation |

## Rollback Procedure

If the upgrade causes issues:

```bash
# Rollback to the previous version using Helm
helm rollback analyticsdb-control --namespace analyticsdb

# Or with kubectl (specify previous image tag)
kubectl set image deployment/analyticsdb-control \
  analyticsdb-server=<previous-image>:<previous-tag> \
  --namespace analyticsdb

kubectl rollout status deployment/analyticsdb-control \
  --namespace analyticsdb
```

## Notes and Limitations

- **Prototype state**: The rolling upgrade procedure is documented and the
  underlying mechanisms (health endpoints, graceful shutdown, cancellation)
  are implemented, but automated CI verification is still a prototype.
- **Single-coordinator**: The current prototype uses a single control-plane
  node. During its upgrade, dispatch is temporarily unavailable. Production
  deployments should use a replicated control plane (Phase H).
- **StatefulSet vs Deployment**: Compute nodes may be deployed as a
  StatefulSet (for stable network identities) or a Deployment. The rolling
  upgrade procedure is the same; adjust the resource type in the commands
  above accordingly.
- **Drain**: There is no explicit "drain" command yet. The SIGTERM + cancel
  approach is the current mechanism for node draining.

## References

- Health endpoint implementation: `crates/analyticsdb-server/src/health.rs`
- Graceful shutdown: `crates/analyticsdb-server/src/main.rs` (shutdown_signal, cancel_all_queries)
- Cancellation token: Phase C2 in `move-to-production.md`
- Kubernetes probes: `helm/analyticsdb/templates/` (livenessProbe, readinessProbe)
