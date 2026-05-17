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
- `analyticsdb-cli`: CLI test client that can submit one-shot SQL or run an interactive shell in embedded mode, against the prototype PostgreSQL wire and Arrow Flight SQL listeners, and through a tested parameterized PostgreSQL extended-query subset
- `analyticsdb-core`: shared request/response models
- `web/admin-console`: prototype Vite + TypeScript web admin console with a database explorer, SQL editor, result grid, query messages, and timing cards backed by a local UI harness

Current metadata SQL subset:

- `CREATE DATABASE <name>`
- `CREATE SCHEMA <name>`
- `CREATE SCHEMA <database>.<name>`
- `ALTER SCHEMA <name> RENAME TO <new_name>`
- `ALTER SCHEMA <database>.<name> RENAME TO <new_name>`
- `CREATE TABLE <name> AS <select>`
- `CREATE TABLE <schema>.<name> AS <select>`
- `SELECT <select-list> INTO <name> [FROM ...]`
- `CREATE TABLE <name> (<column> <type> [, ...])`
- `CREATE TABLE <schema>.<name> (<column> <type> [, ...])`
- `CREATE VIEW <name> AS <select>`
- `CREATE VIEW <schema>.<name> AS <select>`
- `INSERT INTO <table> VALUES (...)[, (...)]`
- `INSERT INTO <table> (<column>[, ...]) VALUES (...)[, (...)]`
- `UPDATE <table> SET <column> = <value> [, ...] [WHERE <condition>]`
- `DELETE FROM <table> [WHERE <condition>]`
- `TRUNCATE TABLE <table>`
- `ALTER TABLE <table> ADD COLUMN <column> <type> [<constraints>]`
- `ALTER TABLE <table> RENAME TO <new_name>`
- `SHOW DATABASES`
- `SHOW SCHEMAS`
- `SHOW SCHEMAS FROM <database>`
- `SHOW NODES`
- `SHOW TABLES`
- `SHOW TABLES FROM <schema>`
- `SHOW TABLES FROM <database>.<schema>`
- `SHOW VIEWS`
- `SHOW VIEWS FROM <schema>`
- `SHOW VIEWS FROM <database>.<schema>`
- `SHOW COLUMNS FROM <table>`
- `DESCRIBE <table>`
- `ALTER USER <name> PASSWORD '<new>'`
- `EXPLAIN <query>`
- `DROP DATABASE <name>`
- `DROP SCHEMA <name>`
- `DROP TABLE <table>`
- `DROP VIEW <view>`

What does not exist yet:

- distributed scheduling and execution
- object-storage-backed production columnar managed-table storage (currently local Parquet directories)
- object-storage-backed native tables
- external Iceberg integration
- web console execution against a live AnalyticsDB web gateway
- Kubernetes deployment assets
- PostgreSQL prepared statements beyond the current parameterized prototype subset, real auth, and broad compatibility coverage
- broad Flight SQL parity coverage beyond standard JDBC query/prepare flows

## Current Protocol-Equivalent Slice

The current prototype now has a narrow, explicitly tested protocol-equivalent slice across PostgreSQL wire and Arrow Flight SQL when driven through the CLI against live listeners.

Included today:

- non-parameterized SQL query and update execution through both protocol listeners
- requested schema/session routing for unqualified SQL in the tested prototype slice
- schema-scoped managed table creation, insertion, deletion, truncation, and querying in the tested prototype slice
- managed tables are now stored as **directories of native Parquet files**, providing columnar performance and DataFusion disk-scan execution
- managed tables have a prototype local sidecar index path for `PRIMARY KEY`, `UNIQUE`, `CREATE INDEX`, `ALTER INDEX RENAME TO`, `DROP INDEX`, and `REINDEX INDEX` / `REINDEX TABLE`; simple equality filters on single-column indexes can use the sidecar instead of scanning every managed Parquet file
- metadata exposed through the current SQL subset, including `SHOW TABLES`, `SHOW VIEWS`, `SHOW COLUMNS`, and `DESCRIBE`
- PostgreSQL catalog-compatibility schema for `pg_catalog.pg_tables`, `pg_catalog.pg_views`, `pg_catalog.pg_namespace`, `pg_catalog.pg_database`, and `pg_catalog.pg_roles` is now integrated via DataFusion `TableProvider`s, enabling complex metadata queries, joins, and filters
- initial `information_schema` compatibility SQL slice for `information_schema.schemata`, `information_schema.tables`, `information_schema.columns`, `information_schema.views`, `information_schema.table_constraints`, `information_schema.key_column_usage`, `information_schema.constraint_column_usage`, `information_schema.constraint_table_usage`, and `information_schema.referential_constraints`, including tested `SELECT *` plus constrained projection/filter/order forms (`WHERE <column> = '<value>'`, `WHERE <column> IN ('a', 'b', ...)`, `ORDER BY <column> [ASC|DESC][, <column> [ASC|DESC] ...]`) through both PostgreSQL wire and Flight SQL SQL execution paths
- current `information_schema` constraint relations expose deterministic prototype NOT NULL metadata rows for managed table columns through `table_constraints`, `constraint_column_usage`, and `constraint_table_usage`; table-defined primary-key and foreign-key metadata now appears in `key_column_usage` and `referential_constraints` for the supported CREATE TABLE constraint subset
- user-visible query error parity for the tested unknown-database and unknown-schema scenarios
- user-visible query error parity for the tested missing-relation planning error scenario in the current prototype
- user-visible command/update error parity for the tested duplicate-table-create, NOT NULL insert validation, and INSERT wrong-value-count scenarios
- cross-database metadata/DDL parity for the tested `CREATE DATABASE`, `CREATE SCHEMA <database>.<schema>`, `SHOW DATABASES`, `SHOW SCHEMAS FROM <database>`, and schema-qualified `CREATE TABLE`/`INSERT`/`SHOW TABLES FROM <database>.<schema>` flows
- CLI-driven parity tests that compare returned columns, rows, requested session context, and completion messages for the supported slice
- prototype PostgreSQL wire session-setting compatibility for common `SET` / `RESET` forms plus `SHOW <parameter>` and `SHOW ALL`, including preserved `search_path` first-entry semantics, accepted storage of common client parameters such as `extra_float_digits`, `application_name`, `client_encoding`, `statement_timeout`, and `TimeZone`, and prototype handling for `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`
- prototype PostgreSQL wire startup `ParameterStatus` compatibility for common client-expected keys (`server_version`, `client_encoding`, `DateStyle`, `TimeZone`, `standard_conforming_strings`, `search_path`, `application_name`, and `default_transaction_isolation`)
- prototype PostgreSQL wire introspection-query compatibility for common JDBC-style probes (`SELECT version()`, `SELECT current_database()`, `SELECT current_schema()`, `SELECT current_user`, `SELECT session_user`, `SELECT current_setting('<name>')`) is now implemented via real DataFusion UDFs
- prototype PostgreSQL transaction-statement support for `BEGIN`, `COMMIT`, and `ROLLBACK` as successful no-ops to support standard client lifecycles
- prototype PostgreSQL extended-query placeholder rendering now avoids rewriting parameter markers inside SQL string literals (for example, `SELECT '$1', $1`)
- prototype Flight SQL support for **TLS encryption** (`--tls-cert` and `--tls-key`) and **Prepared Statements**, enabling standard JDBC/ODBC client connectivity; joined nodes inherit configured cluster TLS paths for client-facing Flight SQL and use a separate internal node channel for distributed partition dispatch
- integrated **structured logging** via `tracing`, controllable through `RUST_LOG` env variable

Explicitly not included yet:

- PostgreSQL extended-query parity with Flight SQL
- Flight SQL prepared statements or broad `SqlInfo` coverage
- production-grade authentication hardening, role/group governance, or broad compatibility claims
- non-SQL metadata parity between PostgreSQL and Flight SQL protocol-specific metadata APIs
- broad PostgreSQL system-catalog compatibility beyond the current narrow `pg_tables` / `pg_views` / `pg_namespace` / `pg_database` / `pg_roles` subset
- broad `information_schema` compatibility beyond the current narrow `schemata` / `tables` / `columns` / `views` / `table_constraints` / `key_column_usage` / `constraint_column_usage` / `constraint_table_usage` / `referential_constraints` subset
- full PostgreSQL `SET`/`SHOW` configuration semantics, including transaction-scoped forms such as `SET TRANSACTION`, `SET ROLE`, `SET SESSION AUTHORIZATION`, or broader server-configuration introspection beyond the current prototype subset
- query-id/coordinator parity through remote protocol clients

## Why The CLI Matters

Any feature that can be exercised by sending SQL into the engine must be verified through the CLI in build/test automation. A feature that fails from the CLI test client is considered failed even if lower-level tests pass.

## Commands

```bash
make build
make test
make test-sql-cli
make fmt
make lint
make web-admin-install
make web-admin-test
make web-admin-build
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
cargo run -p analyticsdb-cli -- query --protocol flight-sql --endpoint https://127.0.0.1:50051 --tls-ca-cert certs/server.crt --tls-domain localhost --sql "SELECT 1 AS one"
cargo run -p analyticsdb-cli -- query --timing --sql "SELECT COUNT(*) AS row_count FROM fact_metrics"
cargo run -p analyticsdb-cli -- interactive --catalog-path /tmp/analyticsdb-catalog.json
cargo run -p analyticsdb-cli -- interactive --protocol postgres --endpoint 127.0.0.1:55432
cargo run -p analyticsdb-cli -- interactive --protocol flight-sql --endpoint https://127.0.0.1:50051 --tls-ca-cert certs/server.crt --tls-domain localhost
```

In interactive mode, enter SQL terminated by `;`. The shell supports keyboard line editing, persistent history in `~/.analyticsdb_history`, multiline statements, and meta commands:

- `\q` or `\quit` exits
- `\?` or `\help` shows help
- `\conninfo` prints the current protocol/session target
- `\timing [on|off]` toggles detailed timing after each statement

## Multi-Node Cluster with Dynamic Scaling

AnalyticsDB supports a distributed coordination layer for dynamic cluster scaling.

#### 1. Start the Cluster Coordinator
The first node initializes the cluster configuration.
```bash
cargo run -p analyticsdb-server -- \
  --node-id node-1 \
  --role control \
  --postgres-addr 127.0.0.1:5432 \
  --flight-sql-addr 127.0.0.1:50051 \
  --node-addr 127.0.0.1:60051 \
  --catalog-path cluster-catalog.json \
  --tls-cert certs/server.crt \
  --tls-key certs/server.key
```

#### 2. Join additional nodes (Dynamic Port Assignment)
Subsequent nodes can join the cluster by pointing to the coordinator. They will automatically receive assigned PostgreSQL, client Flight SQL, and internal node-channel ports plus common configuration.
```bash
cargo run -p analyticsdb-server -- \
  --node-id node-2 \
  --role compute \
  --join https://127.0.0.1:50051 \
  --tls-ca-cert certs/server.crt \
  --tls-domain localhost
```

For the bundled local development certificate, `--tls-domain localhost` is required because the certificate is issued for `localhost` and `127.0.0.1` is only the socket address.

#### 3. View the Cluster
Use the CLI to see the registered nodes and their dynamically assigned ports:
```bash
cargo run -p analyticsdb-cli -- query --sql "SHOW NODES"
```

#### 4. Automatic Client Failover
The CLI supports comma-separated endpoints for automatic failover:
```bash
cargo run -p analyticsdb-cli -- query \
  --endpoint 127.0.0.1:5432,127.0.0.1:5433 \
  --sql "SELECT 1"
```

## Repository Guides

- [AGENTS.md](/Users/jonathanfarina/Development/git/AnalyticsDB/AGENTS.md)
- [docs/agents/feature-status.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/feature-status.md)
- [docs/agents/testing-strategy.md](/Users/jonathanfarina/Development/git/AnalyticsDB/docs/agents/testing-strategy.md)

## Scale Envelope

For supported scale limits, configuration, and recommended production scale, see [Scale Envelope Documentation](docs/scale-envelope.md).
