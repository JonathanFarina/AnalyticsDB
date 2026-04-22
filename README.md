# AnalyticsDB

AnalyticsDB is a prototype distributed analytical database project targeting:

- strict compute and storage separation
- PostgreSQL and Arrow Flight SQL as peer client protocols
- columnar execution and storage
- native and external table support
- Kubernetes deployment with object storage durability

The repository is currently in early prototype form. Only a narrow, honest execution slice exists today:

- a Rust workspace
- a JSON-persistable control-plane skeleton
- a prototype SQL engine wrapper built on DataFusion
- a CLI test client
- CLI-driven SQL tests that run as part of the build/test process

## Current Scope

What exists now:

- `analyticsdb-control`: bootstrap and JSON-backed control-plane state for nodes, users, databases, schemas, and query admission
- `analyticsdb-engine`: prototype single-process SQL execution wrapper
- `analyticsdb-protocol`: prototype PostgreSQL wire and Arrow Flight SQL server surfaces
- `analyticsdb-server`: prototype server binary that exposes both protocol listeners
- `analyticsdb-cli`: CLI test client that can submit SQL in embedded mode, against the prototype PostgreSQL wire and Arrow Flight SQL listeners, and through a tested parameterized PostgreSQL extended-query subset
- `analyticsdb-core`: shared request/response models

Current metadata SQL subset:

- `CREATE DATABASE <name>`
- `CREATE SCHEMA <name>`
- `CREATE SCHEMA <database>.<name>`
- `CREATE TABLE <name> AS <select>`
- `CREATE TABLE <schema>.<name> AS <select>`
- `CREATE TABLE <name> (<column> <type> [, ...])`
- `CREATE TABLE <schema>.<name> (<column> <type> [, ...])`
- `CREATE VIEW <name> AS <select>`
- `CREATE VIEW <schema>.<name> AS <select>`
- `INSERT INTO <table> VALUES (...)[, (...)]`
- `INSERT INTO <table> (<column>[, ...]) VALUES (...)[, (...)]`
- `SHOW DATABASES`
- `SHOW SCHEMAS`
- `SHOW SCHEMAS FROM <database>`
- `SHOW TABLES`
- `SHOW TABLES FROM <schema>`
- `SHOW TABLES FROM <database>.<schema>`
- `SHOW VIEWS`
- `SHOW VIEWS FROM <schema>`
- `SHOW VIEWS FROM <database>.<schema>`
- `SHOW COLUMNS FROM <table>`
- `DESCRIBE <table>`

What does not exist yet:

- distributed scheduling and execution
- object-storage-backed production columnar managed-table storage
- object-storage-backed native tables
- external Parquet or Iceberg integration
- web console
- Kubernetes deployment assets
- PostgreSQL prepared statements beyond the current parameterized prototype subset, real auth, and broad compatibility coverage
- Flight SQL prepared statements, `SqlInfo`, handshake auth, and broad parity coverage

## Why The CLI Matters

Any feature that can be exercised by sending SQL into the engine must be verified through the CLI in build/test automation. A feature that fails from the CLI test client is considered failed even if lower-level tests pass.

## Commands

```bash
make build
make test
make test-sql-cli
make fmt
make lint
```

## Example

```bash
. "$HOME/.cargo/env"
cargo run -p analyticsdb-cli -- query --sql "SELECT 1 AS one, 2 AS two"
cargo run -p analyticsdb-cli -- query --database postgres --schema public --user postgres --sql "SELECT 42 AS answer"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE DATABASE analytics"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "SHOW DATABASES"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE TABLE fact_metrics AS SELECT 11 AS metric, 'ok' AS status"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "SHOW TABLES"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "SHOW COLUMNS FROM fact_metrics"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE TABLE fact_metrics_manual (metric BIGINT NOT NULL, status TEXT, is_hot BOOLEAN)"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "INSERT INTO fact_metrics_manual VALUES (11, 'ok', true), (12, 'warn', false)"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE SCHEMA reporting"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE TABLE reporting.fact_metrics_typed (metric INTEGER NOT NULL, status VARCHAR(20), score DOUBLE PRECISION, active BOOLEAN)"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --schema reporting --sql "INSERT INTO fact_metrics_typed (metric, active, status) VALUES (11, true, 'ok''s')"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "SHOW TABLES FROM reporting"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "CREATE VIEW daily_metrics AS SELECT 7 AS metric"
cargo run -p analyticsdb-cli -- query --catalog-path /tmp/analyticsdb-catalog.json --sql "SELECT * FROM daily_metrics"
cargo run -p analyticsdb-server -- --catalog-path /tmp/analyticsdb-catalog.json --postgres-addr 127.0.0.1:55432 --flight-sql-addr 127.0.0.1:55051
cargo run -p analyticsdb-cli -- query --protocol postgres --endpoint 127.0.0.1:55432 --sql "SELECT 1 AS one"
cargo run -p analyticsdb-cli -- query --protocol postgres --endpoint 127.0.0.1:55432 --sql "SELECT $1 AS metric, $2 AS status" --param 11 --param '"ok"'
cargo run -p analyticsdb-cli -- query --protocol flight-sql --endpoint http://127.0.0.1:55051 --sql "SELECT 1 AS one"
```

## Repository Guides

- [AGENTS.md](/Users/jonathanfarina/Development/git/AnalyticsDB/AGENTS.md)
- [docs/agents/feature-status.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/feature-status.md)
- [docs/agents/testing-strategy.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/testing-strategy.md)
