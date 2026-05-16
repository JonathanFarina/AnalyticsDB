#!/bin/bash
# Concurrency profile benchmark for AnalyticsDB
# Measures tail latency (p50, p95, p99) under increasing client concurrency
# Requires analyticsdb-cli built and server running

set -e

CLIENT="./target/release/analyticsdb-cli"
SERVER="127.0.0.1"
PORT="5432"
USER="postgres"
DB="postgres"
SCHEMA="public"
PASSWORD=""

# Query to run - simple COUNT query against a test table
# User should ensure the table exists before running benchmark
QUERY="SELECT COUNT(*) FROM pg_catalog.pg_tables"

# Concurrency levels to test
CONCURRENCY_LEVELS=(1 16 32 64)

# Number of queries per client at each concurrency level
QUERIES_PER_CLIENT=10

# Output file for raw latencies
LATENCY_FILE=$(mktemp)
trap "rm -f $LATENCY_FILE" EXIT

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h HOST      Server host (default: $SERVER)"
    echo "  -p PORT      Server port (default: $PORT)"
    echo "  -u USER      Database user (default: $USER)"
    echo "  -d DATABASE  Database name (default: $DB)"
    echo "  -s SCHEMA    Schema name (default: $SCHEMA)"
    echo "  -P PASSWORD  Password (default: empty)"
    echo "  -q QUERY     SQL query to run (default: $QUERY)"
    echo "  -n NUM       Queries per client (default: $QUERIES_PER_CLIENT)"
    echo ""
    echo "Example:"
    echo "  $0 -h localhost -p 5432 -u postgres -d testdb"
    exit 1
}

while getopts "h:p:u:d:s:P:q:n:" opt; do
    case $opt in
        h) SERVER="$OPTARG" ;;
        p) PORT="$OPTARG" ;;
        u) USER="$OPTARG" ;;
        d) DB="$OPTARG" ;;
        s) SCHEMA="$OPTARG" ;;
        P) PASSWORD="$OPTARG" ;;
        q) QUERY="$OPTARG" ;;
        n) QUERIES_PER_CLIENT="$OPTARG" ;;
        *) usage ;;
    esac
done

if [ ! -x "$CLIENT" ]; then
    echo "Error: analyticsdb-cli not found or not executable at $CLIENT"
    echo "Build it first: cargo build --release --package analyticsdb-cli"
    exit 1
fi

# Function to run a single query and return latency in milliseconds
run_query() {
    local start_ns=$(date +%s%N)
    $CLIENT -h "$SERVER" -p "$PORT" -u "$USER" -d "$DB" -s "$SCHEMA" \
        ${PASSWORD:+-P "$PASSWORD"} \
        --protocol postgres \
        -c "$QUERY" > /dev/null 2>&1
    local end_ns=$(date +%s%N)
    local latency_ms=$(( (end_ns - start_ns) / 1000000 ))
    echo "$latency_ms"
}

# Function to run queries for one client (sequential)
run_client() {
    local client_id=$1
    local num_queries=$2
    local latencies=()

    for ((i=0; i<num_queries; i++)); do
        local lat=$(run_query)
        echo "$lat"
    done
}

# Function to calculate percentile from sorted array
calc_percentile() {
    local percentile=$1
    local count=$2
    # Use awk for floating point math
    awk -v p="$percentile" -v n="$count" 'BEGIN { idx = int(p * (n - 1) / 100); print idx }'
}

# Function to compute statistics from latency file
compute_stats() {
    local file=$1
    local count=$(wc -l < "$file")

    if [ "$count" -eq 0 ]; then
        echo "  No successful queries"
        return
    fi

    # Sort latencies numerically
    sort -n "$file" > "${file}.sorted"

    # Calculate percentiles using awk
    awk '
    BEGIN { count = 0; sum = 0; }
    NR == 1 { min = $1; }
    { latencies[NR] = $1; sum += $1; count++; }
    END {
        max = latencies[count];
        mean = sum / count;

        # p50 (median)
        if (count % 2 == 1) {
            p50 = latencies[int(count/2) + 1];
        } else {
            p50 = (latencies[count/2] + latencies[count/2 + 1]) / 2;
        }

        # p95
        idx95 = int(0.95 * (count - 1)) + 1;
        p95 = latencies[idx95];

        # p99
        idx99 = int(0.99 * (count - 1)) + 1;
        if (idx99 < 1) idx99 = 1;
        if (idx99 > count) idx99 = count;
        p99 = latencies[idx99];

        printf "  Total queries: %d\n", count;
        printf "  Min latency:  %d ms\n", min;
        printf "  Max latency:  %d ms\n", max;
        printf "  Mean latency: %.2f ms\n", mean;
        printf "  p50 latency:  %.2f ms\n", p50;
        printf "  p95 latency:  %.2f ms\n", p95;
        printf "  p99 latency:  %.2f ms\n", p99;
    }
    ' "${file}.sorted"

    rm -f "${file}.sorted"
}

echo "=========================================="
echo "AnalyticsDB Concurrency Profile Benchmark"
echo "=========================================="
echo "Server:   $SERVER:$PORT"
echo "User:     $USER"
echo "Database: $DB"
echo "Schema:   $SCHEMA"
echo "Query:    $QUERY"
echo "Queries per client: $QUERIES_PER_CLIENT"
echo "=========================================="
echo ""

# Warm-up run
echo "Warming up (1 query)..."
run_query > /dev/null
echo ""

# Run benchmark for each concurrency level
for concurrency in "${CONCURRENCY_LEVELS[@]}"; do
    echo "=== Concurrency Level: $concurrency clients ==="

    # Clear latency file
    > "$LATENCY_FILE"

    # Start time
    local start_time=$(date +%s%N)

    # Launch clients in parallel using xargs
    export -f run_query
    export SERVER PORT USER DB SCHEMA PASSWORD QUERY CLIENT
    export LATENCY_FILE

    # Generate client tasks and pipe to xargs for parallel execution
    seq 1 "$concurrency" | xargs -P "$concurrency" -I {} bash -c '
        for i in $(seq 1 '"$QUERIES_PER_CLIENT"'); do
            lat=$(run_query)
            echo "$lat" >> "'"$LATENCY_FILE"'"
        done
    '

    # End time
    local end_time=$(date +%s%N)
    local total_time_ms=$(( (end_time - start_time) / 1000000 ))

    echo "  Total wall time: ${total_time_ms} ms"
    echo "  Results:"
    compute_stats "$LATENCY_FILE"
    echo ""
done

echo "=========================================="
echo "Benchmark complete."
echo "=========================================="
