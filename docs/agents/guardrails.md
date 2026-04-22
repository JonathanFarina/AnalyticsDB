# Guardrails

This file defines the execution contract for any agent working in this repository.

## Prime Directive

Follow the project requirements exactly. Do not drift into a different product because it is easier, more familiar, or better supported by a library.

## Anti-Hallucination Contract

You must not:

- claim a feature exists when it does not
- claim compatibility that has not been tested
- describe a prototype as production ready
- describe a design intention as implemented behavior
- infer parity between PostgreSQL and Flight SQL without proof
- infer parity between native and external storage without proof
- cite upstream library capabilities as proof that the repository implements them

You must:

- distinguish requirements from implementation
- distinguish architecture decisions from future possibilities
- distinguish scaffolding from usable capability
- update the feature tracker when reality changes
- prove SQL-testable features through the CLI in build/test automation before claiming success

## Required Working Sequence

For any non-trivial task:

1. read `AGENTS.md`
2. read only the relevant focused docs
3. restate the scope internally before making changes
4. implement the smallest honest increment
5. add or update tests
6. if the feature is SQL-testable, verify it through the CLI by submitting SQL as part of build/test
7. update status/docs if the scope or state changed
8. verify what actually works
9. report facts, not aspirations

## When To Stop Instead Of Guessing

Stop and document the issue when:

- a requirement is ambiguous and different interpretations change architecture
- a shortcut would break compute/storage separation
- a shortcut would make PostgreSQL and Flight SQL diverge
- a shortcut would make native and external storage diverge at the SQL surface
- a status upgrade cannot be justified with evidence
- the task depends on missing operational or security decisions
- a SQL-testable feature passes internal tests but fails when driven through the CLI

## Required Evidence By Claim Type

- "implemented": code exists and the behavior can run
- "tested": automated test exists and passes
- "SQL-tested": build/test automation submits SQL through the CLI and the feature succeeds end to end
- "compatible": explicit compatibility test or documented verified mapping exists
- "production ready": resilience, observability, docs, and operational behavior are in place
- "complete": all definition-of-done criteria are satisfied

If the evidence is weaker than the claim, lower the claim.

## Guardrails For Prototypes

Prototypes are allowed, but only if they are labeled honestly.

Prototype rules:

- keep scaffolding thin
- prefer interfaces and tests that enable future replacement
- record deliberate shortcuts
- do not bake prototype shortcuts into permanent APIs without documenting them
- do not hide missing distributed behavior behind local-only code

## Guardrails For Compatibility Work

When working on SQL, protocols, catalogs, or auth:

- treat PostgreSQL behavior as the compatibility target where the project says so
- do not assume parser acceptance equals semantic compatibility
- do not assume result shape parity without tests
- do not assume metadata parity without tests
- ensure both PostgreSQL and Flight SQL surfaces remain aligned
- require CLI-driven SQL tests in the build/test path for any feature that is reachable by SQL

## Guardrails For Storage Work

When working on storage:

- preserve compute/storage separation
- treat object storage as the durable data layer
- treat executor-local disk as cache or spill only
- make storage policy visible and testable
- keep native and external tables on one logical SQL surface

## Guardrails For Distributed Work

When working on routing, planning, execution, caching, or replication:

- assume node failure will happen
- attach query ids and trace context at admission time
- expose node-level and stage-level observability
- design for cancellation, retry boundaries, and backpressure
- do not declare scale behavior without measurement

## Documentation Rules

When a change affects product truth, update the relevant doc in the same change:

- scope or requirements changed: update `project-charter.md`
- architecture changed: update `system-architecture.md`
- status changed: update `feature-status.md`
- sequencing changed: update `workstreams.md`

## Prohibited Behavior

- silently changing scope
- silently downgrading requirements
- silently introducing a second-class protocol path
- silently introducing a second-class storage path
- silently marking work as done without tests
- silently treating a CLI failure as non-blocking for a SQL-testable feature
- silently leaving stale status docs behind
