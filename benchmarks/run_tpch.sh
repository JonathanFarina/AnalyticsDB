#!/bin/bash
# Prototype TPC-H benchmark harness
# Runs a subset of TPC-H queries via the AnalyticsDB CLI and records timings.
# Requires TPC-H data loaded into external Parquet tables.

set -e

CLIENT="./target/release/analyticsdb-cli"
SERVER="localhost"
PORT="5432"

if [ ! -x "$CLIENT" ]; then
    echo "Build analyticsdb-cli first: cargo build --release --package analyticsdb-cli"
    exit 1
fi

echo "Running TPC-H benchmark prototype..."
echo "Scale factor: $SF (env var SF, default 1)"

# Example query 1
echo "Q1: Pricing Summary Report"
time $CLIENT -h $SERVER -p $PORT -c "SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"

echo "Benchmark harness prototype complete."
