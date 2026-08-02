# Architecture Decision Records

ADRs record decisions that affect PromiseDB's architecture or externally visible semantics.

## Status

- **Proposed**: under discussion and not yet relied upon.
- **Accepted**: current direction.
- **Superseded**: replaced by a later ADR.

## Format

Each ADR contains:

```text
Context
Decision
Consequences
Alternatives considered
```

ADRs explain why a decision exists. Current behavior belongs in `docs/semantics.md`; component structure belongs in `docs/architecture.md`; API details belong in Rustdoc.

## Index

- [ADR-0001: Deterministic state machine with an injected clock](0001-deterministic-state-machine.md)
- [ADR-0002: Integer UTC time and half-open intervals](0002-time-and-half-open-intervals.md)
- [ADR-0003: Authoritative state and reconstructible indexes](0003-authoritative-and-derived-state.md)
- [ADR-0004: Blocked slack timeline](0004-blocked-slack-timeline.md)
- [ADR-0005: Opaque UUID identifiers](0005-opaque-uuid-identifiers.md)
- [ADR-0006: GNU AGPL v3 licensing](0000-agpl-v3-license.md)
- [ADR-0007: Control-plane preparation and deterministic commands](0007-control-plane-command-boundary.md)
- [ADR-0008: Canonical command hashing for idempotency](0008-canonical-command-idempotency.md)
- [ADR-0009: Explicit fixed-point resource units](0009-explicit-fixed-point-units.md)
- [ADR-0010: Prepared durable transitions](0010-prepared-durable-transitions.md)
- [ADR-0011: Durable coordinator and synchronous group commit](0011-durable-coordinator-and-group-commit.md)
- [ADR-0012: Locked segmented file WAL](0012-locked-segmented-file-wal.md)
- [ADR-0013: Atomic snapshots and WAL compaction](0013-snapshots-and-compaction.md)
