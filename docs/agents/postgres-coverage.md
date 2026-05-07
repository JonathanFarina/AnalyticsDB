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
| CREATE AGGREGATE | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE COLLATION | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE CONVERSION | `test_alter_aggregate_and_collation_coverage` |  |
| CREATE DATABASE | `test_create_database` |  |
| CREATE FUNCTION | `test_function_coverage`, `test_function_advanced_coverage` | OR REPLACE supported |
| CREATE SCHEMA | `test_create_schema` |  |
| CREATE TABLE | `test_create_table` |  |
| CREATE TABLE AS | `test_create_table_as` |  |
| CREATE VIEW | `test_create_view` |  |
| DELETE | `test_delete` |  |
| DROP DATABASE | `test_drop_database` |  |
| DROP FUNCTION | `test_function_coverage`, `test_function_advanced_coverage` | IF EXISTS and CASCADE/RESTRICT supported |
| DROP SCHEMA | `test_drop_schema` |  |
| DROP TABLE | `test_drop_table` |  |
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
| ALTER INDEX | `cli_supports_create_alter_and_drop_index_statements`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | `RENAME TO` supported for standalone managed-table indexes; constraint-backed indexes are intentionally protected and broader PostgreSQL index DDL remains unsupported |
| ALTER SCHEMA | `test_alter_schema` | RENAME TO supported |
| ALTER TABLE | `test_alter_table`, `cli_supports_rename_column_drop_column_and_drop_constraint` | RENAME TO, ADD COLUMN, DROP COLUMN, RENAME COLUMN, DROP CONSTRAINT, and ALTER COLUMN (TYPE, SET/DROP NOT NULL, SET/DROP DEFAULT) supported |
| ALTER USER | `test_alter_user` | PASSWORD rotation supported |
| CREATE INDEX | `cli_supports_create_alter_and_drop_index_statements`, `cli_create_unique_index_failure_is_atomic`, `cli_rejects_duplicate_index_names_within_schema`, `cli_supports_index_manifests_and_broader_predicates`, `cli_supports_broader_index_predicates_across_postgres_and_flight_sql`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | Managed-table prototype supports column-list indexes with schema-wide name uniqueness, versioned sidecar manifest publication, and duplicate validation plus the current equality/`IN`/bounded-range lookup slice; external/partial/expression/concurrent index features are unsupported |
| DROP INDEX | `cli_supports_create_alter_and_drop_index_statements`, `cli_rejects_dropping_primary_key_backing_index`, `cli_supports_index_lifecycle_across_postgres_and_flight_sql` | Standalone managed-table indexes can be dropped; indexes backing `PRIMARY KEY` / `UNIQUE` constraints are intentionally protected in the current prototype and sidecar manifests are removed together with the standalone index |
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
| ALTER GROUP | - |  |
| ALTER LANGUAGE | - |  |
| ALTER LARGE OBJECT | - |  |
| ALTER MATERIALIZED VIEW | - |  |
| ALTER OPERATOR | - |  |
| ALTER OPERATOR CLASS | - |  |
| ALTER OPERATOR FAMILY | - |  |
| ALTER POLICY | - |  |
| ALTER PROCEDURE | - |  |
| ALTER PUBLICATION | - |  |
| ALTER ROLE | - |  |
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
| CREATE GROUP | - |  |
| CREATE LANGUAGE | - |  |
| CREATE MATERIALIZED VIEW | - |  |
| CREATE OPERATOR | - |  |
| CREATE OPERATOR CLASS | - |  |
| CREATE OPERATOR FAMILY | - |  |
| CREATE POLICY | - |  |
| CREATE PROCEDURE | - |  |
| CREATE PUBLICATION | - |  |
| CREATE ROLE | - |  |
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
| CREATE USER | - |  |
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
| DROP GROUP | - |  |
| DROP LANGUAGE | - |  |
| DROP MATERIALIZED VIEW | - |  |
| DROP OPERATOR | - |  |
| DROP OPERATOR CLASS | - |  |
| DROP OPERATOR FAMILY | - |  |
| DROP OWNED | - |  |
| DROP POLICY | - |  |
| DROP PROCEDURE | - |  |
| DROP PUBLICATION | - |  |
| DROP ROLE | - |  |
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
| DROP USER | - |  |
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
| VACUUM | - |  |
