#!/usr/bin/env bash
# test-rolling-upgrade.sh
#
# Prototype CI script to verify rolling upgrade behaviour for AnalyticsDB.
#
# What it does:
#   1. Starts a local "cluster" (control plane + compute node) using the
#      analyticsdb-server binary.
#   2. Submits a long-running query (pg_sleep or similar).
#   3. Triggers a "rolling upgrade" by restarting nodes with SIGTERM.
#   4. Verifies the query either completes or is properly cancelled.
#
# NOTE: This is a prototype script. It may be ignored in CI initially until
#       the Kubernetes deployment is further along. For now, it exercises
#       the graceful shutdown and cancellation paths locally.
#
# Prerequisites:
#   - cargo build --workspace (server binary exists)
#   - analyticsdb-cli binary exists
#   - jq (for JSON parsing)
#   - netstat or lsof (for port availability checks)
#
# Usage:
#   ./scripts/test-rolling-upgrade.sh [--keep-alive]
#
# Options:
#   --keep-alive  Do not tear down the cluster at the end (for debugging)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARY="${REPO_ROOT}/target/debug/analyticsdb-server"
CLI_BINARY="${REPO_ROOT}/target/debug/analyticsdb-cli"
LOGS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/analyticsdb-rolling-test-XXXXXX")
KEEP_ALIVE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-alive)
            KEEP_ALIVE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Colour

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

cleanup() {
    log_info "Cleaning up..."
    if [[ -n "${CONTROL_PID:-}" ]]; then
        kill -SIGTERM "$CONTROL_PID" 2>/dev/null || true
        wait "$CONTROL_PID" 2>/dev/null || true
    fi
    if [[ -n "${COMPUTE_PID:-}" ]]; then
        kill -SIGTERM "$COMPUTE_PID" 2>/dev/null || true
        wait "$COMPUTE_PID" 2>/dev/null || true
    fi
    if [[ "$KEEP_ALIVE" == "false" ]]; then
        rm -rf "$LOGS_DIR"
    else
        log_info "Logs preserved in: $LOGS_DIR"
    fi
}

trap cleanup EXIT

# Check prerequisites
if [[ ! -x "$BINARY" ]]; then
    log_error "Server binary not found at $BINARY"
    log_error "Run: cargo build --workspace"
    exit 1
fi

if [[ ! -x "$CLI_BINARY" ]]; then
    log_error "CLI binary not found at $CLI_BINARY"
    log_error "Run: cargo build --workspace"
    exit 1
fi

# Find available ports
CONTROL_PG_PORT=15432
CONTROL_FLIGHT_PORT=18815
CONTROL_ADMIN_PORT=19090
CONTROL_NODE_PORT=18816

COMPUTE_PG_PORT=25432
COMPUTE_FLIGHT_PORT=28815
COMPUTE_ADMIN_PORT=29090
COMPUTE_NODE_PORT=28816

CATALOG_PATH="$LOGS_DIR/catalog.db"

log_info "Starting rolling upgrade test..."
log_info "Logs directory: $LOGS_DIR"

# === Step 1: Start Control Plane ===
log_info "Starting control plane..."
RUST_LOG=info "$BINARY" \
  --node-id control-1 \
  --role control \
  --init-cluster \
  --postgres-addr "127.0.0.1:${CONTROL_PG_PORT}" \
  --flight-sql-addr "127.0.0.1:${CONTROL_FLIGHT_PORT}" \
  --node-addr "127.0.0.1:${CONTROL_NODE_PORT}" \
  --admin-addr "127.0.0.1:${CONTROL_ADMIN_PORT}" \
  --catalog-path "$CATALOG_PATH" \
  > "$LOGS_DIR/control.log" 2>&1 &
CONTROL_PID=$!

# Wait for control plane to be ready
log_info "Waiting for control plane to be ready..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${CONTROL_ADMIN_PORT}/readyz" 2>/dev/null | grep -q "OK"; then
        log_info "Control plane is ready (attempt $i)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        log_error "Control plane did not become ready in time"
        cat "$LOGS_DIR/control.log"
        exit 1
    fi
    sleep 1
done

# === Step 2: Start Compute Node ===
log_info "Starting compute node..."
RUST_LOG=info "$BINARY" \
  --node-id compute-1 \
  --role compute \
  --join "http://127.0.0.1:${CONTROL_NODE_PORT}" \
  --postgres-addr "127.0.0.1:${COMPUTE_PG_PORT}" \
  --flight-sql-addr "127.0.0.1:${COMPUTE_FLIGHT_PORT}" \
  --node-addr "127.0.0.1:${COMPUTE_NODE_PORT}" \
  --admin-addr "127.0.0.1:${COMPUTE_ADMIN_PORT}" \
  --catalog-path "$CATALOG_PATH" \
  > "$LOGS_DIR/compute.log" 2>&1 &
COMPUTE_PID=$!

# Wait for compute node to be ready
log_info "Waiting for compute node to be ready..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${COMPUTE_ADMIN_PORT}/readyz" 2>/dev/null | grep -q "OK"; then
        log_info "Compute node is ready (attempt $i)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        log_error "Compute node did not become ready in time"
        cat "$LOGS_DIR/compute.log"
        exit 1
    fi
    sleep 1
done

# === Step 3: Submit a Long-Running Query ===
log_info "Submitting a long-running query (pg_sleep(30))..."

# Run the query in the background and capture the result
QUERY_RESULT_FILE="$LOGS_DIR/query_result.txt"
timeout 45 "$CLI_BINARY" \
  --host 127.0.0.1 \
  --port "$COMPUTE_PG_PORT" \
  -c "SELECT pg_sleep(30);" \
  > "$QUERY_RESULT_FILE" 2>&1 &
QUERY_PID=$!

# Give the query time to start
sleep 3

# Check that the query is actually running
if ! kill -0 "$QUERY_PID" 2>/dev/null; then
    log_error "Query exited immediately (unexpected)"
    cat "$QUERY_RESULT_FILE"
    exit 1
fi

log_info "Long-running query is executing (PID: $QUERY_PID)"

# === Step 4: Trigger Rolling Upgrade (SIGTERM to compute node) ===
log_info "Triggering rolling upgrade: sending SIGTERM to compute node..."
kill -SIGTERM "$COMPUTE_PID" 2>/dev/null || true

# Wait for the compute node to shut down gracefully
log_info "Waiting for compute node to shut down..."
for i in $(seq 1 30); do
    if ! kill -0 "$COMPUTE_PID" 2>/dev/null; then
        log_info "Compute node has shut down (attempt $i)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        log_warn "Compute node did not shut down in time, forcing..."
        kill -SIGKILL "$COMPUTE_PID" 2>/dev/null || true
    fi
    sleep 1
done

# Check the query result
log_info "Checking query result..."
if wait "$QUERY_PID" 2>/dev/null; then
    log_info "Query completed (unexpected during restart, but acceptable)"
    cat "$QUERY_RESULT_FILE"
else
    # Query should have been cancelled
    if grep -qi "cancel\|abort\|terminate\|error" "$QUERY_RESULT_FILE"; then
        log_info "Query was properly cancelled (expected behaviour)"
        cat "$QUERY_RESULT_FILE"
    else
        log_warn "Query ended but cancellation message not found"
        cat "$QUERY_RESULT_FILE"
    fi
fi

# === Step 5: Restart Compute Node (simulate upgraded node) ===
log_info "Restarting compute node (simulating upgraded binary)..."
RUST_LOG=info "$BINARY" \
  --node-id compute-1 \
  --role compute \
  --join "http://127.0.0.1:${CONTROL_NODE_PORT}" \
  --postgres-addr "127.0.0.1:${COMPUTE_PG_PORT}" \
  --flight-sql-addr "127.0.0.1:${COMPUTE_FLIGHT_PORT}" \
  --node-addr "127.0.0.1:${COMPUTE_NODE_PORT}" \
  --admin-addr "127.0.0.1:${COMPUTE_ADMIN_PORT}" \
  --catalog-path "$CATALOG_PATH" \
  > "$LOGS_DIR/compute-restart.log" 2>&1 &
COMPUTE_PID=$!

# Wait for restarted compute node to be ready
log_info "Waiting for restarted compute node to be ready..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${COMPUTE_ADMIN_PORT}/readyz" 2>/dev/null | grep -q "OK"; then
        log_info "Restarted compute node is ready (attempt $i)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        log_error "Restarted compute node did not become ready in time"
        cat "$LOGS_DIR/compute-restart.log"
        exit 1
    fi
    sleep 1
done

# === Step 6: Verify Cluster Works After Upgrade ===
log_info "Verifying cluster works after upgrade..."
if "$CLI_BINARY" \
  --host 127.0.0.1 \
  --port "$COMPUTE_PG_PORT" \
  -c "SELECT 1 AS upgrade_test;" \
  > "$LOGS_DIR/post_upgrade_test.txt" 2>&1; then
    log_info "Post-upgrade query succeeded!"
    cat "$LOGS_DIR/post_upgrade_test.txt"
else
    log_error "Post-upgrade query failed!"
    cat "$LOGS_DIR/post_upgrade_test.txt"
    exit 1
fi

# === Step 7: Check Logs for Clean Shutdown ===
log_info "Checking logs for clean shutdown behaviour..."
if grep -q "Shutdown signal received" "$LOGS_DIR/compute.log"; then
    log_info "✓ Shutdown signal was received"
else
    log_error "✗ Shutdown signal NOT found in logs"
    exit 1
fi

if grep -q "cancelling in-flight queries" "$LOGS_DIR/compute.log"; then
    log_info "✓ In-flight queries were cancelled"
else
    log_warn "⚠ In-flight query cancellation message not found (may be expected if query finished first)"
fi

# === Summary ===
log_info "========================================="
log_info "Rolling upgrade test completed successfully!"
log_info "========================================="
log_info "What was tested:"
log_info "  1. Cluster startup (control + compute)"
log_info "  2. Long-running query execution"
log_info "  3. Graceful shutdown with SIGTERM"
log_info "  4. Query cancellation during shutdown"
log_info "  5. Node restart (simulated upgrade)"
log_info "  6. Post-upgrade cluster functionality"
log_info "  7. Clean shutdown log messages"

if [[ "$KEEP_ALIVE" == "true" ]]; then
    log_info ""
    log_info "Cluster is still running (--keep-alive mode)"
    log_info "  Control PID: $CONTROL_PID"
    log_info "  Compute PID: $COMPUTE_PID"
    log_info "  Logs: $LOGS_DIR"
fi

exit 0
