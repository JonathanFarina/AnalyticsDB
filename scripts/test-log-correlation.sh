#!/usr/bin/env bash
# test-log-correlation.sh
# CI test script that verifies log correlation fields are present in AnalyticsDB logs.
# NOTE: This script requires the engine to compile. Currently there are pre-existing
# compile errors from other features (G1, G2, G3) that need to be fixed first.

set -euo pipefail

RED='\x1b[1;31m'
GREEN='\x1b[1;32m'
YELLOW='\x1b[1;33m'
NC='\x1b[0m'

echo -e "${YELLOW}=== AnalyticsDB Log Correlation Test ===${NC}"

# Check if binaries exist
if [ ! -x "./target/debug/analyticsdb-server" ]; then
    echo -e "${RED}Error: analyticsdb-server binary not found${NC}"
    echo "Please fix compile errors first, then build the project"
    exit 1
fi

if [ ! -x "./target/debug/analyticsdb-cli" ]; then
    echo -e "${RED}Error: analyticsdb-cli binary not found${NC}"
    echo "Please fix compile errors first, then build the project"
    exit 1
fi

# Setup
CATALOG_PATH=$(mktemp -d)/analyticsdb-catalog.db
LOG_FILE=$(mktemp)
PID_FILE=$(mktemp)

cleanup() {
    if [ -f "$PID_FILE" ]; then
        kill $(cat "$PID_FILE") 2>/dev/null || true
        sleep 1
        kill -9 $(cat "$PID_FILE") 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
    rm -f "$LOG_FILE"
}
trap cleanup EXIT

echo "Starting AnalyticsDB server..."
RUST_LOG=info ./target/debug/analyticsdb-server \
    --catalog-path "$CATALOG_PATH" \
    --postgres-addr "127.0.0.1:0" \
    --flight-sql-addr "127.0.0.1:0" \
    --node-addr "127.0.0.1:0" \
    --admin-addr "127.0.0.1:0" \
    --init-cluster \
    > "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"

# Wait for server to start
echo "Waiting for server to start..."
for i in $(seq 1 30); do
    if grep -q "Startup complete" "$LOG_FILE" 2>/dev/null; then
        echo -e "${GREEN}Server started after ${i}s${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Server failed to start within 30s${NC}"
        exit 1
    fi
    sleep 1
done

# Get PostgreSQL port
PG_PORT=$(grep -oP 'PostgreSQL protocol listening on: 127\.0\.0\.1:\K\d+' "$LOG_FILE" | tail -1)
if [ -z "$PG_PORT" ]; then
    echo -e "${RED}Could not determine PostgreSQL port${NC}"
    exit 1
fi
echo "PostgreSQL port: $PG_PORT"

sleep 2

echo ""
echo -e "${YELLOW}--- Test: Submit a query and check for correlation fields ---${NC}"

# Submit a query
./target/debug/analyticsdb-cli \
    --protocol postgres \
    --host "127.0.0.1" \
    --port "$PG_PORT" \
    --user postgres \
    --database postgres \
    --command "SELECT 1 as test_value" \
    > /dev/null 2>&1 || true

sleep 1

echo "Checking log file for correlation fields..."
FAILED=0

for field in "query_id=" "user=" "database=" "schema=" "protocol=" "node_id="; do
    if grep -q "$field" "$LOG_FILE"; then
        echo -e "  ${GREEN}✓${NC} $field found in logs"
    else
        echo -e "  ${RED}✗${NC} $field NOT found in logs"
        FAILED=1
    fi
done

echo ""
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}=== ALL CHECKS PASSED ===${NC}"
    echo ""
    echo "Sample log line with correlation fields:"
    grep -oP '.*query_id=[^ ]+ .*user=[^ ]+ .*database=[^ ]+ .*schema=[^ ]+ .*protocol=[^ ]+ .*node_id=[^ ]+.*' "$LOG_FILE" | head -1 || true
    exit 0
else
    echo -e "${RED}=== SOME CHECKS FAILED ===${NC}"
    echo ""
    echo "Last 30 lines of log file:"
    tail -30 "$LOG_FILE"
    exit 1
fi
