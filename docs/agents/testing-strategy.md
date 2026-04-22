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
