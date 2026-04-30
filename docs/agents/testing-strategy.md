# Testing Strategy

## Purpose

This file defines how build and test automation must validate AnalyticsDB features, especially any feature reachable through SQL.

## Core Rule

If a feature can be tested by sending SQL into the engine, it must be tested in the build/test process by sending SQL through the CLI.

If the feature fails from the CLI test client:

- the feature is failed
- a review is required
- a re-test is required before claiming success

Lower-level unit or integration tests are useful, but they do not override a failing CLI-driven SQL test.

## Test Layers

### 1. Unit Tests

Use unit tests for:

- pure functions
- planners
- metadata rules
- formatting
- small invariants

### 2. Service/Library Integration Tests

Use integration tests for:

- engine behaviors below the CLI
- storage adapters
- scheduler components
- catalog workflows

### 3. CLI-Driven SQL Tests

Use CLI-driven SQL tests for every SQL-testable feature, including:

- query execution
- SQL syntax support
- metadata visibility
- session validation for database, schema, and user context
- metadata persistence across independent client invocations
- column-oriented managed table snapshot persistence
- managed table materialization and later querying across independent client invocations
- managed table schema introspection across independent client invocations
- persisted view execution across independent client invocations
- user-visible error behavior
- result formatting and engine messages
- timing visibility

These tests must run in normal build/test automation.

## Current Prototype Enforcement

At the current repository stage:

- SQL features are validated through `analyticsdb-cli`
- build/test automation includes CLI-driven SQL tests via `cargo test --workspace`
- the dedicated CLI SQL test entrypoint is `cargo test -p analyticsdb-cli --test sql_cli`
- current CLI SQL coverage includes successful query execution, session validation failures, catalog persistence across separate CLI runs, persisted view execution, managed table materialization/query flows, schema introspection, and columnar snapshot validation at the engine layer
- current CLI SQL coverage includes successful query execution, session validation failures, catalog persistence across separate CLI runs, persisted view execution, managed table materialization/query flows, schema-defined managed table creation, column-list and full-row insert flows across separate CLI runs, schema-scoped metadata listing, schema introspection, and columnar snapshot validation at the engine layer
- current CLI SQL coverage includes live PostgreSQL wire and Arrow Flight SQL listener validation, with the CLI acting as the test client for network protocol query paths
- current CLI SQL coverage includes parameterized PostgreSQL extended-query validation through the CLI against a live PostgreSQL wire listener
- current CLI SQL coverage includes paired PostgreSQL wire and Arrow Flight SQL parity assertions for the current supported slice, including requested schema routing, schema-scoped and cross-database metadata/DDL SQL flows, user-visible unknown-database/unknown-schema/missing-relation query errors, and user-visible duplicate-table-create/NOT NULL/INSERT-value-count command errors
- current CLI SQL coverage includes a broad table-driven parity matrix test that executes the current supported SQL surface through both live protocol listeners and compares user-visible success and failure contracts
- current CLI SQL coverage includes a README-to-matrix capability drift guard that fails when documented supported SQL statements no longer match matrix-covered capabilities
- current protocol-crate integration coverage now includes Flight SQL handshake scaffold and shared auth-hook session bootstrap assertions
- current protocol-crate integration coverage now includes PostgreSQL startup auth negative-path assertions for unknown-user and wrong-password failures
- current CLI SQL coverage now includes cross-protocol auth/session parity assertions (user, role, database, schema, auth-method fields) and matched unknown-user auth failure behavior
- current CLI SQL coverage now includes strict valid/invalid password matrix assertions across live PostgreSQL wire and Flight SQL listeners
- current CLI SQL coverage now includes password-rotation invalidation checks that verify old credentials fail and rotated credentials succeed across both protocol listeners
- current CLI SQL coverage now includes strict `ALTER USER ... PASSWORD ...` error-contract parity checks for unknown users, empty passwords, malformed password literals, and non-admin authorization failures across both protocol listeners
- current CLI SQL coverage now includes PostgreSQL wire session-setting acceptance for common `SET` / `RESET` statements and single-statement `SHOW <parameter>` / `SHOW ALL` validation through the CLI against a live listener, including prototype `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...` compatibility
- current protocol-crate integration coverage now includes same-connection PostgreSQL wire assertions for `SET` / `RESET` / `SHOW` behavior, including `search_path` routing, JDBC-style `extra_float_digits`, transaction isolation reflection (`SHOW transaction_isolation`, `SHOW TRANSACTION ISOLATION LEVEL`, `current_setting('transaction_isolation')`), generic client settings, `SHOW ALL`, and explicit rejection of unsupported transaction-scoped `SET` forms
- current protocol-crate integration coverage now includes PostgreSQL startup `ParameterStatus` assertions for common client-visible keys, including `default_transaction_isolation`, and propagation of `application_name` from startup connection options
- current protocol-crate integration coverage now includes PostgreSQL wire introspection-query assertions for common JDBC probes (`SELECT version()`, `SELECT current_database()`, `SELECT current_schema()`, `SELECT current_user`, `SELECT session_user`, and `SELECT current_setting('<name>')`)
- current protocol-crate integration coverage now includes PostgreSQL extended-query assertions proving placeholder substitution does not rewrite markers embedded inside SQL string literals
- current CLI SQL coverage now includes a pg_catalog compatibility slice for `pg_catalog.pg_tables`, `pg_catalog.pg_views`, `pg_catalog.pg_namespace`, `pg_catalog.pg_database`, and `pg_catalog.pg_roles`, including tested `SELECT *` and constrained projection/filter/order forms (`=` and `IN` filters plus multi-column mixed `ORDER BY ASC|DESC`) through both live PostgreSQL wire and Flight SQL listeners
- current CLI SQL coverage now includes an `information_schema` compatibility slice for `information_schema.schemata`, `information_schema.tables`, `information_schema.columns`, `information_schema.views`, `information_schema.table_constraints`, `information_schema.key_column_usage`, `information_schema.constraint_column_usage`, `information_schema.constraint_table_usage`, and `information_schema.referential_constraints`, including tested `SELECT *` and constrained projection/filter/order forms (`=` and `IN` filters plus multi-column mixed `ORDER BY ASC|DESC`) through both live PostgreSQL wire and Flight SQL listeners
- current CLI SQL coverage now asserts deterministic prototype NOT NULL constraint rows in `information_schema.table_constraints`, `information_schema.constraint_column_usage`, and `information_schema.constraint_table_usage` for managed-table NOT NULL columns, and now also asserts table-defined primary-key/foreign-key rows in `key_column_usage` and `referential_constraints` for the supported CREATE TABLE constraint subset
- current protocol-crate integration coverage now includes Flight SQL metadata API assertions (`get_db_schemas`, `get_tables`) for schema/table/view discovery aligned with the current pg_catalog compatibility setup
- current protocol-crate and CLI SQL coverage now validates the shared statement outcome contract: row-returning metadata SQL stays row-returning across PostgreSQL and Flight SQL, Flight SQL update RPCs tolerate row-returning metadata probes with `0` affected rows, and DML/DDL update paths report affected rows without parsing human-readable messages
- current engine/protocol/CLI coverage now exercises direct Parquet relation registration, cached session-context invalidation after mutating commands, Flight SQL row-stream retrieval, and DataFusion Parquet-sink writes for bounded `INSERT INTO ... SELECT ...` / CTAS-style managed-table materialization paths
- current CLI SQL coverage now exercises the prototype managed-table index path for `PRIMARY KEY`, column/table `UNIQUE`, `CREATE INDEX`, `ALTER INDEX RENAME TO`, `DROP INDEX`, unique-key rejection, duplicate-name rejection, constraint-backed-index protection, atomic failure rollback for `CREATE UNIQUE INDEX` and `ALTER TABLE ... ADD PRIMARY KEY`, versioned index-manifest publication, indexed equality/`IN`/bounded-range lookup, and post-`TRUNCATE` index recovery through the CLI
- current CLI SQL coverage now includes PostgreSQL wire and Flight SQL listener validation for the current standalone-index lifecycle and predicate slice (`CREATE INDEX`, `ALTER INDEX RENAME TO`, `DROP INDEX`, `IN`, and bounded-range lookup) plus protocol-visible rejection of dropping a primary-key backing index

## Required Assertions For CLI SQL Tests

A CLI SQL test should assert the user-visible contract, not just process success.

At minimum, assert relevant combinations of:

- exit code
- returned rows
- returned columns
- engine message text
- execution timing visibility
- error visibility

## Status Impact

- a SQL-testable feature cannot be promoted to `Partial` or `Complete` without CLI-driven SQL coverage
- a failing CLI SQL test blocks success claims for that feature
- status docs must reflect the real test outcome
