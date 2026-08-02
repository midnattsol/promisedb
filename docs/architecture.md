# Architecture

PromiseDB is currently a single-process durable state machine with a synchronous WAL coordinator. The design separates authoritative domain state from reconstructible indexes.

## Components

```text
src/
├── domain.rs and domain/  validated values and promise lifecycle
├── clock.rs               command timestamp source
├── engine.rs              authoritative state and prepared transitions
├── storage/               durable coordinator, WAL framing/codecs, and recovery
├── index/                 reconstructible hot-path indexes
├── lib.rs                 library module tree
└── main.rs                executable validation flow
```

### Domain

The domain owns structural invariants for intervals, fixed-point units, claims, bundles, relative claims and bundles, ordered choices, promises, resource pools, and capacity curves. Relative values use validated offsets and materialize into ordinary claims with checked timestamp arithmetic. Domain objects do not read clocks or global state. Each pool fixes a unit name and integer subunit scale; decimal conversion remains outside the engine.

### Command boundary

The control API generates entity IDs, resolves external names, and wraps every mutation with a `ClientId` and `IdempotencyKey`. It then submits a deterministic `CommandOperation` through `Engine::apply(command, now)`. The explicit timestamp is selected outside the state machine. Commands are retained in durable transitions for audit, but recovery never executes them.

Before dispatch, the engine hashes the normalized operation with BLAKE3 and looks up `(ClientId, IdempotencyKey)` in a deterministic map. Exact retries return the cached response immediately; conflicting payloads are rejected. The map stores a fixed-size command digest and complete response, and is authoritative state required by snapshots and recovery.

Versioned durable transitions are the recovery input. Stable events are exact audit facts exposed by `watch_events(from_sequence)`, but are not sufficient recovery records by themselves. One requested command may first emit multiple expiration events; each successful domain transition owns one sequence, while multiple audit events describing one capacity revision may share that transition sequence.

Queries such as `explain_unavailable`, `list_at_risk`, and `find_first_slot` are pure current-state reads. Deadline processing occurs before mutating commands or through `ProcessExpirations`. `find_first_slot` is advisory; `HoldFirstSlot` performs the same deterministic candidate search inside the authoritative mutation boundary.

### Engine

`Engine` separates its clock from cloneable `EngineState`, which owns resource pools, promises, ordered audit events, idempotency records, the global sequence number, and one derived `SlackTimeline` per pool. A separate publication revision detects stale prepared candidates even when a command emits no domain sequence. Convenience operations read the injected clock once and delegate to deterministic `*_at` transitions; the durable command boundary receives `now` explicitly.

The direct in-memory command path avoids a complete state clone:

```text
Engine::apply
→ idempotency lookup
→ checked next publication revision
→ deterministic transition on published state
→ cache exact response
→ assign the preflighted revision
```

The durable path isolates candidate effects:

```text
Engine::prepare_batch
→ pre-scan idempotency identities and checked next publication revision
→ clone EngineState once and transition commands sequentially
→ derive timestamped stable authoritative after-values
→ Engine::can_publish preflight
→ persist all durable items as one group
→ infallibly publish candidate with its prepared next revision
```

A rejected operation does not consume its own domain sequence. Expirations successfully processed before that rejection remain in the prepared transition. Every first-seen command still advances publication revision and persists one transition because its exact response and idempotency record are authoritative.

### Indexes

Indexes accelerate decisions but are not authoritative. `SlackTimeline` is derived from a pool's `CapacityCurve` and the claims of held and committed promises.

The index uses an array-of-structs (AoS) design: chronologically ordered `SlackPoint` values are grouped into bounded `SlackBlock` values. Blocks maintain minimum and maximum effective slack plus a lazy block-wide delta. Complete blocks can be queried or adjusted without visiting each point.

Stored slack is `i64`, matching the domain's `MAX_QUANTITY = i64::MAX as u64` bound. On x86_64, measurements enforced by a layout test show that changing stored slack from `i128` to `i64` reduces `SlackPoint` from 32 to 16 bytes and `SlackBlock` from 112 to 64 bytes. This improves point density and reduces aggregate metadata footprint on the timeline hot path. Candidate demand aggregation, rebuild usage calculations, and admission comparisons still use `i128` scratch arithmetic; values are narrowed with checked conversions only when a conflict-free timeline is materialized.

The current blocked AoS representation remains deliberately intact. Splitting hot and cold block metadata or switching points to a structure-of-arrays (SoA) layout could improve particular scans, but would also add indirection and complexity to updates. Those changes are benchmark-driven follow-up work, not assumptions built into this compaction.

The engine creates a timeline with each resource pool and keeps it synchronized with promise transitions. A hold consumes slack, while release and expiration restore it. Commit does not adjust slack because both held and committed promises consume the same capacity.

Capacity revision reconstructs a candidate timeline from the replacement curve and active promises. Strict mode publishes only non-negative slack; force mode may publish negative slack as explicit deficit intervals. At-risk promises are derived by intersecting active claims with those intervals, so deficit metadata remains reconstructible rather than authoritative state.

Timeline changes are first applied to copies of every affected pool's index. Every pool is evaluated so an unavailable bundle can return all conflicts in canonical order. The engine publishes prepared copies only when the complete bundle succeeds, preventing partial index updates across pools. `HoldOneOf` evaluates alternatives sequentially against unchanged current timelines, discards copies and records conflicts for each rejection, and publishes only the first feasible alternative. First-slot search also evaluates candidates sequentially against unchanged timelines, but discards conflicts rather than accumulating them; the authoritative variant carries only the first feasible materialized bundle and its prepared timeline copies into publication.

Hold admission uses `SlackTimeline` as its production hot path: preparing adjusted timeline copies both validates the complete bundle and computes the index state to publish. Ordinary holds, selected choice alternatives, and authoritative first-slot holds share the same accepted-hold publication path and emit the existing `HoldCreated` event. Replace first restores the old bundle on temporary timeline copies, evaluates the new bundle against those overrides, and publishes only the resulting final timelines. Pools used only by the old bundle remain restored; shared and new pools receive the newly adjusted timelines. The slower calculation from capacity curves and active promises is compiled only for differential tests, where it remains a correctness oracle. Resource pools and promises remain authoritative and can reconstruct every timeline.

## Authoritative state

The current authoritative in-memory state is:

```text
resource pools and capacity curves
promises and their bundles
promise states and versions
idempotency command hashes and cached responses
global sequence number
publication revision (runtime concurrency guard)
```

Temporal usage and slack are reconstructible from that state.

## Storage boundary

`storage::Database<B>` owns the engine and WAL backend and exposes no mutable bypass. Its crate-private prepare/publish boundary leaves published state untouched, preflights revision and record-sequence exhaustion, and yields a runtime candidate plus timestamped codec-stable transitions. Batch preparation clones `EngineState` once and executes commands sequentially; v1 may encode cumulative authoritative after-values relative to the original batch base, trading larger later records for avoiding per-command state clones. Storage ordering is:

```text
prepare one candidate and ordered effects
→ preflight publication and contiguous record sequences
→ stream all first-seen payloads into directly framed records in one byte vector
→ one append and selected flush/sync
→ infallibly publish the preflighted candidate
```

Recovery scans strict WAL order, validates record and event timestamps, installs durable effects without admission, and rebuilds all `SlackTimeline` indexes once at the end. It returns the next record sequence and last valid byte offset. Generic recovery rejects partial tails and never repairs files. Directory segmentation, process locking, snapshot files, and locked final-tail truncation are the next file-layer substage.
