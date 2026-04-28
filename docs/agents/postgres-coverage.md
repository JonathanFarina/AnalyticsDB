# PostgreSQL Keyword Coverage Tracker

This file tracks the implementation and testing status of every SQL command listed in the [PostgreSQL SQL Commands documentation](https://www.postgresql.org/docs/current/sql-commands.html).

## Status Definitions

- **Complete**: Production ready, fully tested in `sql_cli.rs` or `postgres_coverage.rs`.
- **Partial**: Implemented but with known limitations or missing edge cases.
- **Shim/No-op**: Explicitly supported as a successful no-op for client compatibility (e.g., `BEGIN`).
- **Unsupported**: Not yet implemented; returns a syntax or "not implemented" error.

## Coverage Matrix

| Command | Status | Test Case | Notes |
| :--- | :--- | :--- | :--- |
| ABORT | Complete | `test_transaction_shims_coverage` | Successful rollback-compatible no-op for `ABORT`, `ABORT WORK`, and `ABORT TRANSACTION` in the current prototype transaction shim |
| ALTER AGGREGATE | Complete | `test_alter_aggregate_and_collation_coverage` | Current prototype metadata object operations validate rename success plus missing-object and duplicate-name failures |
| ALTER COLLATION | Complete | `test_alter_aggregate_and_collation_coverage` | Current prototype metadata object operations validate rename success plus missing-object and duplicate-name failures |
| ALTER CONVERSION | Shim/No-op | `test_alter_database_and_shims_coverage` | |
| ALTER DATABASE | Complete | `test_alter_database_and_shims_coverage` | RENAME TO supported with relation migration |
| ALTER DEFAULT PRIVILEGES | Unsupported | - | |
| ALTER DOMAIN | Unsupported | - | |
| ALTER EVENT TRIGGER | Unsupported | - | |
| ALTER EXTENSION | Unsupported | - | |
| ALTER FOREIGN DATA WRAPPER | Unsupported | - | |
| ALTER FOREIGN TABLE | Unsupported | - | |
| ALTER FUNCTION | Complete | `test_function_coverage`, `test_function_advanced_coverage` | RENAME TO, OWNER TO, and SET SCHEMA supported |
| ALTER GROUP | Unsupported | - | |
| ALTER INDEX | Unsupported | - | |
| ALTER LANGUAGE | Unsupported | - | |
| ALTER LARGE OBJECT | Unsupported | - | |
| ALTER MATERIALIZED VIEW | Unsupported | - | |
| ALTER OPERATOR | Unsupported | - | |
| ALTER OPERATOR CLASS | Unsupported | - | |
| ALTER OPERATOR FAMILY | Unsupported | - | |
| ALTER POLICY | Unsupported | - | |
| ALTER PROCEDURE | Unsupported | - | |
| ALTER PUBLICATION | Unsupported | - | |
| ALTER ROLE | Unsupported | - | |
| ALTER ROUTINE | Unsupported | - | |
| ALTER RULE | Unsupported | - | |
| ALTER SCHEMA | Partial | `test_alter_schema` | RENAME TO supported |
| ALTER SEQUENCE | Unsupported | - | |
| ALTER SERVER | Unsupported | - | |
| ALTER STATISTICS | Unsupported | - | |
| ALTER SUBSCRIPTION | Unsupported | - | |
| ALTER SYSTEM | Unsupported | - | |
| ALTER TABLE | Partial | `test_alter_table` | RENAME TO and ADD COLUMN supported |
| ALTER TABLESPACE | Unsupported | - | |
| ALTER TEXT SEARCH CONFIGURATION | Unsupported | - | |
| ALTER TEXT SEARCH DICTIONARY | Unsupported | - | |
| ALTER TEXT SEARCH PARSER | Unsupported | - | |
| ALTER TEXT SEARCH TEMPLATE | Unsupported | - | |
| ALTER TRIGGER | Unsupported | - | |
| ALTER TYPE | Unsupported | - | |
| ALTER USER | Partial | `test_alter_user` | PASSWORD rotation supported |
| ALTER USER MAPPING | Unsupported | - | |
| ALTER VIEW | Unsupported | - | |
| ANALYZE | Unsupported | - | |
| BEGIN | Shim/No-op | `test_begin` | Successful no-op |
| CALL | Unsupported | - | |
| CHECKPOINT | Unsupported | - | |
| CLOSE | Unsupported | - | |
| CLUSTER | Unsupported | - | |
| COMMENT | Unsupported | - | |
| COMMIT | Shim/No-op | `test_commit` | Successful no-op |
| COMMIT PREPARED | Unsupported | - | |
| COPY | Unsupported | - | |
| CREATE ACCESS METHOD | Unsupported | - | |
| CREATE AGGREGATE | Complete | `test_alter_aggregate_and_collation_coverage` | |
| CREATE CAST | Unsupported | - | |
| CREATE COLLATION | Complete | `test_alter_aggregate_and_collation_coverage` | |
| CREATE CONVERSION | Complete | `test_alter_aggregate_and_collation_coverage` | |
| CREATE DATABASE | Complete | `test_create_database` | |
| CREATE DOMAIN | Unsupported | - | |
| CREATE EVENT TRIGGER | Unsupported | - | |
| CREATE EXTENSION | Unsupported | - | |
| CREATE FOREIGN DATA WRAPPER | Unsupported | - | |
| CREATE FOREIGN TABLE | Unsupported | - | |
| CREATE FUNCTION | Complete | `test_function_coverage`, `test_function_advanced_coverage` | OR REPLACE supported |
| CREATE GROUP | Unsupported | - | |
| CREATE INDEX | Unsupported | - | |
| CREATE LANGUAGE | Unsupported | - | |
| CREATE MATERIALIZED VIEW | Unsupported | - | |
| CREATE OPERATOR | Unsupported | - | |
| CREATE OPERATOR CLASS | Unsupported | - | |
| CREATE OPERATOR FAMILY | Unsupported | - | |
| CREATE POLICY | Unsupported | - | |
| CREATE PROCEDURE | Unsupported | - | |
| CREATE PUBLICATION | Unsupported | - | |
| CREATE ROLE | Unsupported | - | |
| CREATE RULE | Unsupported | - | |
| CREATE SCHEMA | Complete | `test_create_schema` | |
| CREATE SEQUENCE | Unsupported | - | |
| CREATE SERVER | Unsupported | - | |
| CREATE STATISTICS | Unsupported | - | |
| CREATE SUBSCRIPTION | Unsupported | - | |
| CREATE TABLE | Complete | `test_create_table` | |
| CREATE TABLE AS | Complete | `test_create_table_as` | |
| CREATE TABLESPACE | Unsupported | - | |
| CREATE TEXT SEARCH CONFIGURATION | Unsupported | - | |
| CREATE TEXT SEARCH DICTIONARY | Unsupported | - | |
| CREATE TEXT SEARCH PARSER | Unsupported | - | |
| CREATE TEXT SEARCH TEMPLATE | Unsupported | - | |
| CREATE TRANSFORM | Unsupported | - | |
| CREATE TRIGGER | Unsupported | - | |
| CREATE TYPE | Unsupported | - | |
| CREATE USER | Unsupported | - | |
| CREATE USER MAPPING | Unsupported | - | |
| CREATE VIEW | Complete | `test_create_view` | |
| DEALLOCATE | Unsupported | - | |
| DECLARE | Unsupported | - | |
| DELETE | Complete | `test_delete` | |
| DISCARD | Unsupported | - | |
| DO | Unsupported | - | |
| DROP ACCESS METHOD | Unsupported | - | |
| DROP AGGREGATE | Unsupported | - | |
| DROP CAST | Unsupported | - | |
| DROP COLLATION | Unsupported | - | |
| DROP CONVERSION | Unsupported | - | |
| DROP DATABASE | Complete | `test_drop_database` | |
| DROP DOMAIN | Unsupported | - | |
| DROP EVENT TRIGGER | Unsupported | - | |
| DROP EXTENSION | Unsupported | - | |
| DROP FOREIGN DATA WRAPPER | Unsupported | - | |
| DROP FOREIGN TABLE | Unsupported | - | |
| DROP FUNCTION | Complete | `test_function_coverage`, `test_function_advanced_coverage` | IF EXISTS and CASCADE/RESTRICT supported |
| DROP GROUP | Unsupported | - | |
| DROP INDEX | Unsupported | - | |
| DROP LANGUAGE | Unsupported | - | |
| DROP MATERIALIZED VIEW | Unsupported | - | |
| DROP OPERATOR | Unsupported | - | |
| DROP OPERATOR CLASS | Unsupported | - | |
| DROP OPERATOR FAMILY | Unsupported | - | |
| DROP OWNED | Unsupported | - | |
| DROP POLICY | Unsupported | - | |
| DROP PROCEDURE | Unsupported | - | |
| DROP PUBLICATION | Unsupported | - | |
| DROP ROLE | Unsupported | - | |
| DROP ROUTINE | Unsupported | - | |
| DROP RULE | Unsupported | - | |
| DROP SCHEMA | Complete | `test_drop_schema` | |
| DROP SEQUENCE | Unsupported | - | |
| DROP SERVER | Unsupported | - | |
| DROP STATISTICS | Unsupported | - | |
| DROP SUBSCRIPTION | Unsupported | - | |
| DROP TABLE | Complete | `test_drop_table` | |
| DROP TABLESPACE | Unsupported | - | |
| DROP TEXT SEARCH CONFIGURATION | Unsupported | - | |
| DROP TEXT SEARCH DICTIONARY | Unsupported | - | |
| DROP TEXT SEARCH PARSER | Unsupported | - | |
| DROP TEXT SEARCH TEMPLATE | Unsupported | - | |
| DROP TRANSFORM | Unsupported | - | |
| DROP TRIGGER | Unsupported | - | |
| DROP TYPE | Unsupported | - | |
| DROP USER | Unsupported | - | |
| DROP USER MAPPING | Unsupported | - | |
| DROP VIEW | Complete | `test_drop_view` | |
| END | Shim/No-op | `test_end` | Alias for COMMIT |
| EXECUTE | Unsupported | - | |
| EXPLAIN | Complete | `test_explain` | |
| FETCH | Unsupported | - | |
| GRANT | Unsupported | - | |
| IMPORT FOREIGN SCHEMA | Unsupported | - | |
| INSERT | Complete | `test_insert` | |
| LISTEN | Unsupported | - | |
| LOAD | Unsupported | - | |
| LOCK | Unsupported | - | |
| MERGE | Unsupported | - | |
| MOVE | Unsupported | - | |
| NOTIFY | Unsupported | - | |
| PREPARE | Unsupported | - | |
| PREPARE TRANSACTION | Unsupported | - | |
| REASSIGN OWNED | Unsupported | - | |
| REFRESH MATERIALIZED VIEW | Unsupported | - | |
| REINDEX | Unsupported | - | |
| RELEASE SAVEPOINT | Unsupported | - | |
| RESET | Partial | `test_reset` | |
| REVOKE | Unsupported | - | |
| ROLLBACK | Shim/No-op | `test_rollback` | Successful no-op |
| ROLLBACK PREPARED | Unsupported | - | |
| ROLLBACK TO SAVEPOINT | Unsupported | - | |
| SAVEPOINT | Unsupported | - | |
| SECURITY LABEL | Unsupported | - | |
| SELECT | Complete | `test_select` | |
| SELECT INTO | Unsupported | - | |
| SET | Partial | `test_set` | |
| SET CONSTRAINTS | Unsupported | - | |
| SET ROLE | Unsupported | - | |
| SET SESSION AUTHORIZATION | Unsupported | - | |
| SET TRANSACTION | Unsupported | - | |
| SHOW | Complete | `test_show` | |
| START TRANSACTION | Shim/No-op | `test_start_transaction` | Successful no-op |
| TRUNCATE | Complete | `test_truncate` | |
| UNLISTEN | Unsupported | - | |
| UPDATE | Complete | `test_update` | |
| VACUUM | Unsupported | - | |
| VALUES | Complete | `test_values` | |
