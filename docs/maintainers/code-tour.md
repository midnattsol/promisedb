# Code tour

## Crate entry points

```text
src/lib.rs   public module tree
src/main.rs  small executable validation flow
```

`main.rs` is not the authoritative API and is not a test suite. Engine behavior belongs in library tests.

## Domain

```text
src/domain.rs
src/domain/
```

The domain owns validated values and local lifecycle rules:

- `interval.rs`: half-open interval construction and overlap.
- `unit.rs`: immutable fixed-point unit name and subunit scale.
- `capacity_curve.rs`: ordered normalized physical capacity.
- `claim.rs`: one positive pool demand over one interval.
- `bundle.rs`: non-empty atomic claim collection.
- `promise.rs`: identity, state, versions, and local transitions.
- `resource_pool.rs`: pool identity, immutable unit, and capacity curve.
- `error.rs`: structured domain and transition errors.

Domain methods must not read clocks, inspect global engine state, or update indexes.

## Command and audit boundary

```text
src/command.rs
src/event.rs
src/idempotency.rs
```

- `command.rs` defines the deterministic mutation language and operation-specific results.
- `event.rs` defines stable audit facts emitted after successful transitions.
- `idempotency.rs` defines canonical BLAKE3 hashing and cached command responses.

IDs are generated before `Engine::apply`. The command path must not introduce randomness.

## Engine

```text
src/engine.rs
src/engine/availability.rs
src/engine/capacity_revision.rs
```

`Engine` serializes mutations and owns:

- resource pools;
- promises;
- idempotency records;
- ordered in-memory events;
- the global sequence;
- one derived slack timeline per pool.

`engine.rs` is currently large because transition publication and integration tests remain close together. Extract code only when a boundary is stable and the move improves ownership.

## Indexes

```text
src/index/
```

`SlackTimeline` is the admission hot path. It is reconstructible from capacity curves and active held or committed promises. Never make it the only source of truth.

## Documentation ownership

```text
docs/guide/        conceptual learning
docs/maintainers/  change procedures
docs/semantics.md  exact behavioral contract
docs/architecture.md component structure
docs/adr/          durable decisions and rationale
```
