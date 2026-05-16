# PostgreSQL Keyword Coverage Tracker

This file tracks the implementation and testing status of every SQL command listed in the [PostgreSQL SQL Commands documentation](https://www.postgresql.org/docs/current/sql-commands.html).

## Status Definitions

- **Complete**: Production ready, fully tested in `sql_cli.rs` or `postgres_coverage.rs`.
- **Partial**: Implemented but with known limitations or missing edge cases.
- **Shim/No-op**: Explicitly supported as a successful no-op for client compatibility (e.g., `BEGIN`).
- **Unsupported**: Not yet implemented; returns a syntax or "not implemented" error.

## Coverage Matrix

Commands are grouped by their current coverage status. Status values are unchanged from the previous matrix; this section only reorganizes the tracker for easier review.

### Complete

| Command | Test Case | Notes |
| :--- | :--- | :--- |
| ABORT | `test_transaction_shims_coverage` | Successful rollback-compatible no-op for `ABORT`, `ABORT WORK`, and `ABORT TRANSACTION` in the current prototype transaction shim |
| ALTER AGGREGATE | `test_alter_aggregate_and_collation_coverage` | Current prototype metadata object operations validate rename success plus missing-object and duplicate-name failures |
| ALTER COLLATION | `test_alter_aggregate_and_collation_coverage` | Current prototype metadata object operations validate rename success plus missing-object and duplicate-name failures |
| ALTER DATABASE | `test_alter_database_and_shims_coverage` | RENAME TO supported with relation migration |
| ALTER FUNCTION | `test_function_coverage`, `test_function_advanced_coverage` | RENAME TO, OWNER TO, and SET SCHEMA supported |
| ALTER GROUP | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | ADD/DROP USER and RENAME TO supported. |
| ALTER INDEX | `test_alter_index`, `cli_supports_create_alter_and_drop_index_statements`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | RENAME TO supported; physically updates sidecar indexes. |
| ALTER ROLE | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | ADD/DROP USER and RENAME TO supported. |
| ALTER SCHEMA | `test_alter_schema` | RENAME TO supported with automatic relation migration. |
| ALTER TABLE | `test_alter_table`, `cli_supports_rename_column_drop_column_and_drop_constraint` | RENAME TO, ADD COLUMN, DROP COLUMN, RENAME COLUMN, DROP CONSTRAINT, and ALTER COLUMN (TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT) supported; includes physical snapshot updates and schema-on-read default value support. |
| ALTER USER | `test_user_management`, `test_alter_user`, `cli_supports_user_lifecycle_across_postgres_and_flight_sql` | PASSWORD rotation supported. |
| CREATE AGGREGATE | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE COLLATION | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE CONVERSION | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE DATABASE | `test_create_database` |  |
| CREATE FUNCTION | `test_function_coverage`, `test_function_advanced_coverage` | OR REPLACE supported |
| CREATE GROUP | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | Aliased to roles; membership supported. |
| CREATE INDEX | `test_create_index`, `cli_supports_create_alter_and_drop_index_statements`, `cli_create_unique_index_failure_is_atomic`, `cli_rejects_duplicate_index_names_within_schema`, `cli_supports_index_manifests_and_broader_predicates`, `cli_supports_broader_index_predicates_across_postgres_and_flight_sql`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | Managed-table prototype supports column-list indexes with schema-wide name uniqueness, versioned sidecar manifest publication, and duplicate validation plus the current equality/`IN`/bounded-range lookup slice. |
| CREATE ROLE | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | Aliased to groups; membership supported. |
| CREATE SCHEMA | `test_create_schema` |  |
| CREATE TABLE | `test_create_table` |  |
| CREATE TABLE AS | `test_create_table_as` |  |
| CREATE USER | `test_user_management`, `cli_supports_user_lifecycle_across_postgres_and_flight_sql` | Supported with optional PASSWORD. |
| CREATE VIEW | `test_create_view` |  |
| DELETE | `test_delete` |  |
| DROP DATABASE | `test_drop_database` |  |
| DROP FUNCTION | `test_function_coverage`, `test_function_advanced_coverage` | IF EXISTS and CASCADE/RESTRICT supported |
| DROP GROUP | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | IF EXISTS supported. |
| DROP INDEX | `test_drop_index`, `cli_supports_create_alter_and_drop_index_statements`, `cli_rejects_dropping_primary_key_backing_index`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | Standalone managed-table indexes can be dropped; indexes backing `PRIMARY KEY` / `UNIQUE` constraints are intentionally protected and sidecar manifests are removed together with the standalone index. |
| DROP ROLE | `test_group_management`, `cli_supports_group_lifecycle_across_postgres_and_flight_sql` | IF EXISTS supported. |
| DROP SCHEMA | `test_drop_schema` |  |
| DROP TABLE | `test_drop_table` |  |
| DROP USER | `test_user_management`, `cli_supports_user_lifecycle_across_postgres_and_flight_sql` | IF EXISTS supported. |
| DROP VIEW | `test_drop_view` |  |
| EXPLAIN | `test_explain` |  |
| INSERT | `test_insert` |  |
| SELECT | `test_select` |  |
| SELECT INTO | `test_select_into`, `cli_supports_select_into_managed_table`, protocol parity matrix | Materializes the current supported `SELECT ... INTO <table>` slice as a managed table through the same persisted Parquet snapshot path used by CTAS |
| SHOW | `test_show` |  |
| TRUNCATE | `test_truncate` |  |
| UPDATE | `test_update` |  |
| VALUES | `test_values` |  |

### Partial

| Command | Test Case | Notes |
| :--- | :--- | :--- |
| REINDEX | `test_reindex`, `cli_supports_reindex_index_and_table_statements`, `cli_protocols_support_reindex_index_and_table` | Current prototype rebuilds local managed-table sidecar snapshots for `REINDEX INDEX [CONCURRENTLY] <name>` and `REINDEX TABLE [CONCURRENTLY] <name>`; broader PostgreSQL `REINDEX` targets/options and non-managed storage backends remain unsupported |
| RESET | `test_reset` |  |
| SET | `test_set` |  |

### Shim/No-op

| Command | Test Case | Notes |
| :--- | :--- | :--- |
| ALTER CONVERSION | `test_alter_database_and_shims_coverage` |  |
| BEGIN | `test_begin` | Successful no-op |
| COMMIT | `test_commit` | Successful no-op |
| END | `test_end` | Alias for COMMIT |
| ROLLBACK | `test_rollback` | Successful no-op |
| START TRANSACTION | `test_start_transaction` | Successful no-op |

### Unsupported

| Command | Test Case | Notes |
| :--- | :--- | :--- |
| ALTER DEFAULT PRIVILEGES | - |  |
| ALTER DOMAIN | - |  |
| ALTER EVENT TRIGGER | - |  |
| ALTER EXTENSION | - |  |
| ALTER FOREIGN DATA WRAPPER | - |  |
| ALTER FOREIGN TABLE | - |  |
| ALTER LANGUAGE | - |  |
| ALTER LARGE OBJECT | - |  |
| ALTER MATERIALIZED VIEW | - |  |
| ALTER OPERATOR | - |  |
| ALTER OPERATOR CLASS | - |  |
| ALTER OPERATOR FAMILY | - |  |
| ALTER POLICY | - |  |
| ALTER PROCEDURE | - |  |
| ALTER PUBLICATION | - |  |
| ALTER ROUTINE | - |  |
| ALTER RULE | - |  |
| ALTER SEQUENCE | - |  |
| ALTER SERVER | - |  |
| ALTER STATISTICS | - |  |
| ALTER SUBSCRIPTION | - |  |
| ALTER SYSTEM | - |  |
| ALTER TABLESPACE | - |  |
| ALTER TEXT SEARCH CONFIGURATION | - |  |
| ALTER TEXT SEARCH DICTIONARY | - |  |
| ALTER TEXT SEARCH PARSER | - |  |
| ALTER TEXT SEARCH TEMPLATE | - |  |
| ALTER TRIGGER | - |  |
| ALTER TYPE | - |  |
| ALTER USER MAPPING | - |  |
| ALTER VIEW | - |  |
| ANALYZE | - |  |
| CALL | - |  |
| CHECKPOINT | - |  |
| CLOSE | - |  |
| CLUSTER | - |  |
| COMMENT | - |  |
| COMMIT PREPARED | - |  |
| COPY | - |  |
| CREATE ACCESS METHOD | - |  |
| CREATE CAST | - |  |
| CREATE DOMAIN | - |  |
| CREATE EVENT TRIGGER | - |  |
| CREATE EXTENSION | - |  |
| CREATE FOREIGN DATA WRAPPER | - |  |
| CREATE FOREIGN TABLE | - |  |
| CREATE LANGUAGE | - |  |
| CREATE MATERIALIZED VIEW | - |  |
| CREATE OPERATOR | - |  |
| CREATE OPERATOR CLASS | - |  |
| CREATE OPERATOR FAMILY | - |  |
| CREATE POLICY | - |  |
| CREATE PROCEDURE | - |  |
| CREATE PUBLICATION | - |  |
| CREATE RULE | - |  |
| CREATE SEQUENCE | - |  |
| CREATE SERVER | - |  |
| CREATE STATISTICS | - |  |
| CREATE SUBSCRIPTION | - |  |
| CREATE TABLESPACE | - |  |
| CREATE TEXT SEARCH CONFIGURATION | - |  |
| CREATE TEXT SEARCH DICTIONARY | - |  |
| CREATE TEXT SEARCH PARSER | - |  |
| CREATE TEXT SEARCH TEMPLATE | - |  |
| CREATE TRANSFORM | - |  |
| CREATE TRIGGER | - |  |
| CREATE TYPE | - |  |
| CREATE USER MAPPING | - |  |
| DEALLOCATE | - |  |
| DECLARE | - |  |
| DISCARD | - |  |
| DO | - |  |
| DROP ACCESS METHOD | - |  |
| DROP AGGREGATE | - |  |
| DROP CAST | - |  |
| DROP COLLATION | - |  |
| DROP CONVERSION | - |  |
| DROP DOMAIN | - |  |
| DROP EVENT TRIGGER | - |  |
| DROP EXTENSION | - |  |
| DROP FOREIGN DATA WRAPPER | - |  |
| DROP FOREIGN TABLE | - |  |
| DROP LANGUAGE | - |  |
| DROP MATERIALIZED VIEW | - |  |
| DROP OPERATOR | - |  |
| DROP OPERATOR CLASS | - |  |
| DROP OPERATOR FAMILY | - |  |
| DROP OWNED | - |  |
| DROP POLICY | - |  |
| DROP PROCEDURE | - |  |
| DROP PUBLICATION | - |  |
| DROP ROUTINE | - |  |
| DROP RULE | - |  |
| DROP SEQUENCE | - |  |
| DROP SERVER | - |  |
| DROP STATISTICS | - |  |
| DROP SUBSCRIPTION | - |  |
| DROP TABLESPACE | - |  |
| DROP TEXT SEARCH CONFIGURATION | - |  |
| DROP TEXT SEARCH DICTIONARY | - |  |
| DROP TEXT SEARCH PARSER | - |  |
| DROP TEXT SEARCH TEMPLATE | - |  |
| DROP TRANSFORM | - |  |
| DROP TRIGGER | - |  |
| DROP TYPE | - |  |
| DROP USER MAPPING | - |  |
| EXECUTE | - |  |
| FETCH | - |  |
| GRANT | - |  |
| IMPORT FOREIGN SCHEMA | - |  |
| LISTEN | - |  |
| LOAD | - |  |
| LOCK | - |  |
| MERGE | - |  |
| MOVE | - |  |
| NOTIFY | - |  |
| PREPARE | - |  |
| PREPARE TRANSACTION | - |  |
| REASSIGN OWNED | - |  |
| REFRESH MATERIALIZED VIEW | - |  |
| RELEASE SAVEPOINT | - |  |
| REVOKE | - |  |
| ROLLBACK PREPARED | - |  |
| ROLLBACK TO SAVEPOINT | - |  |
| SAVEPOINT | - |  |
| SECURITY LABEL | - |  |
| SET CONSTRAINTS | - |  |
| SET ROLE | - |  |
| SET SESSION AUTHORIZATION | - |  |
| SET TRANSACTION | - |  |
| UNLISTEN | - |  |
| VACUUM | Partial | `VACUUM <table>` triggers compaction; full PostgreSQL VACUUM semantics (analyze, freeze, full) not implemented |

## SQL Surface Drift Guard (E7)

The supported SQL surface is protected by an automated drift guard that runs in CI.

### Where it lives

`crates/analyticsdb-cli/tests/sql_cli.rs` — test `cli_protocols_cover_supported_sql_surface_with_matrix_parity`.

### What it enforces

Two sets must stay identical:

1. **README bullets** — `documented_sql_capabilities_from_readme()` parses the "Current metadata SQL subset:" section of `README.md` and maps each bullet (`- \`CREATE TABLE ...\``, etc.) to a `SqlCapability` enum variant. Any bullet that has no mapping triggers a `panic!` at test time.
2. **Parity matrix** — `parity_matrix_sql_capabilities()` returns the hardcoded `BTreeSet<SqlCapability>` of every statement that the test matrix actually exercises.

The test asserts `readme_caps == matrix_caps`. If either side diverges the test fails with a diff, blocking CI.

### CI enforcement

The `test-sql-cli` step in the `rust` job runs `make test-sql-cli`, which includes this test. Any PR that modifies the README SQL subset or the parity matrix without keeping both in sync will fail CI.

### How to add a new statement

1. Add a `SqlCapability` variant to the `SqlCapability` enum in `sql_cli.rs`.
2. Add a mapping branch for the new README bullet in `documented_sql_capabilities_from_readme()`.
3. Add the variant to `parity_matrix_sql_capabilities()`.
4. Add a bullet to the "Current metadata SQL subset:" section of `README.md` that matches the prefix expected by the mapping branch.
5. Add a `SuccessCase` (or `ErrorCase`) to the parity matrix test body that exercises the new statement.

All five steps must be done together; leaving any one out fails the drift guard.
