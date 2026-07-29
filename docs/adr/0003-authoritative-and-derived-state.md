# ADR-0003: Authoritative state and reconstructible indexes

- Status: Accepted
- Date: 2026-07-29

## Context

Fast admission needs temporal indexes, but treating an index as the only truth makes corruption and format evolution harder to recover from.

## Decision

Resource capacity and promises are authoritative. Usage and slack indexes are derived and must be rebuildable from capacity curves plus claims belonging to held and committed promises.

Durable recovery will use authoritative snapshot state and WAL records. Snapshots may include derived indexes only as an optimization.

## Consequences

- Index implementations can change without changing durable semantics.
- Recovery can discard and rebuild a corrupt or unsupported index.
- Tests can compare optimized indexes against a slow reference model.
- Startup may require index rebuilding when no usable snapshot index exists.

## Alternatives considered

- Persisting only a slack or usage index: rejected because it loses promise ownership and auditability.
- Recomputing usage for every command forever: retained as a reference model but rejected as the eventual hot path.
