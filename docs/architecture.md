# Architecture

PromiseDB is currently a single-process, in-memory state machine. The design separates authoritative domain state from reconstructible indexes.

## Components

```text
src/
├── domain.rs and domain/  validated values and promise lifecycle
├── clock.rs               command timestamp source
├── engine.rs              authoritative state and serialized transitions
├── index/                 reconstructible hot-path indexes
├── lib.rs                 library module tree
└── main.rs                executable validation flow
```

### Domain

The domain owns structural invariants for intervals, claims, bundles, promises, resource pools, and capacity curves. Domain objects do not read clocks or global state.

### Engine

`Engine` owns resource pools, promises, the global sequence number, and one derived `SlackTimeline` per pool. Public operations read the injected clock once and delegate to deterministic `*_at` transitions.

```text
public operation
→ read clock once
→ deterministic transition with explicit now
→ process due expirations
→ validate the complete operation
→ prepare affected timeline copies
→ calculate the next sequence
→ publish authoritative state and prepared indexes
→ publish the sequence and result
```

A rejected operation does not consume its own sequence. Expirations successfully processed before that rejection remain committed.

### Indexes

Indexes accelerate decisions but are not authoritative. `SlackTimeline` is derived from a pool's `CapacityCurve` and the claims of held and committed promises.

The index uses chronologically ordered slack points grouped into bounded blocks. Blocks maintain minimum and maximum effective slack plus a lazy block-wide delta. Complete blocks can be queried or adjusted without visiting each point.

The engine creates a timeline with each resource pool and keeps it synchronized with promise transitions. A hold consumes slack, while release and expiration restore it. Commit does not adjust slack because both held and committed promises consume the same capacity.

Timeline changes are first applied to copies of every affected pool's index. The engine publishes those copies only after the complete bundle succeeds, preventing partial index updates across pools.

Hold admission still runs the reference availability calculation before preparing indexed changes. The reference path is retained while differential tests establish that both implementations agree; the index is not yet the sole production admission path.

## Authoritative state

The current authoritative in-memory state is:

```text
resource pools and capacity curves
promises and their bundles
promise states and versions
global sequence number
```

Temporal usage and slack are reconstructible from that state.

## Storage boundary

Durable storage is not implemented yet. The intended ordering is:

```text
validate deterministic transition
→ append durable record
→ satisfy configured flush policy
→ publish state and result
```

Snapshots may contain derived indexes for faster startup, but recovery must be possible from authoritative snapshot state plus later WAL records.
