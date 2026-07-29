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
- [ADR-0006: GNU AGPL v3 licensing](0006-agpl-v3-license.md)
