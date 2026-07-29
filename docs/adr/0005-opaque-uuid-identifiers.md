# ADR-0005: Opaque UUID identifiers

- Status: Accepted
- Date: 2026-07-29

## Context

Resource pools and promises need stable external identities. Human-readable names can change, leak business terminology, and are unsuitable as authorization boundaries.

## Decision

Use strongly typed UUID v4 wrappers for external `ResourcePoolId` and `PromiseId`. Display names are descriptive metadata, not identity and not secrets.

## Consequences

- Pool and promise IDs cannot be accidentally interchanged by type-safe Rust code.
- IDs can be generated without central coordination.
- APIs may resolve human-readable names separately without changing identity.
- Repeated UUIDs inside hot claim structures may later be replaced by compact internal numeric IDs after boundary resolution.

## Alternatives considered

- String names as primary keys: rejected because names are mutable and comparatively expensive in hot structures.
- Raw `u64` external IDs: deferred because generation and coordination policy would become part of the public contract.
- Hashes of names: rejected because renaming and collision policy would become identity semantics.
