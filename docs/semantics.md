# Semantics

## Time

`Timestamp` is an `i64` Unix UTC timestamp. The engine contains no timezone logic.

Intervals are always half-open:

```text
[start, end)
```

`start` is included and `end` is excluded. `start >= end` is invalid. Adjacent intervals such as `[0,10)` and `[10,20)` do not overlap.

## Quantity and slack

`Quantity` is a non-negative `u64` containing integer subunits. Claim quantities must be greater than zero. Capacity may be zero.

Each resource pool has an immutable `Unit { name, subunits_per_unit }`. For a unit named `watts` with `1_000` subunits per unit, quantity `1_500` represents 1.5 watts. Unit names must not be empty and the scale must be greater than zero. Decimal parsing, display formatting, and unit aliases belong to the control API; the engine operates only on integer subunits.

Slack is signed:

```text
slack = capacity - active usage
```

Positive slack is spare capacity, zero is full utilization, and negative slack is a deficit. The derived slack index uses `i128` so every difference between two `u64` values is representable.

## Capacity

A resource pool owns a piecewise-constant `CapacityCurve`. Segments are ordered, non-overlapping, and normalized. Adjacent equal-capacity segments are merged.

Capacity outside declared segments, including gaps, is zero.

## Capacity revisions and deficits

A capacity revision replaces one resource pool's complete capacity curve. Due hold expirations are processed first, and the candidate slack timeline is rebuilt from the replacement curve plus every active claim before publication.

`Strict` mode rejects the revision with `CapacityRevisionCreatesDeficit` if any resulting slack is negative. The pool, timeline, and requested revision sequence remain unchanged.

`Force` mode accepts the new physical reality even when active usage exceeds capacity. Negative slack is exposed as normalized `CapacityDeficit` intervals with positive deficit quantities and the IDs of overlapping active promises. `list_at_risk` reports those promises without selecting cancellations or victims.

A new hold cannot consume capacity in an existing deficit. An atomic replacement may leave a deficit only when its final slack is no worse than the slack before replacement; this permits moves that reduce an existing deficit without requiring an observable release-first transition.

## Claims and bundles

A claim consumes a positive quantity from one resource pool during one interval. A bundle is a non-empty collection of claims accepted or rejected atomically.

Multiple claims in the same bundle may reference the same pool and overlap. Their quantities are evaluated together.

## Hold admission

An attempted hold has two normal outcomes:

```text
Held(promise_id)
Unavailable { conflicts }
```

Insufficient capacity is not an engine error. An unavailable outcome contains every normalized blocking interval across all referenced pools. Conflicts contain the pool, interval, combined candidate demand, available slack, missing quantity, and IDs of overlapping active promises.

Conflicts are ordered canonically by interval start, resource pool ID, and interval end. Adjacent conflicts within one pool are merged only when their quantities and conflicting promise IDs are identical. Capacity gaps are reported as zero availability.

Unavailable holds do not create a promise, modify a timeline, or consume a sequence for the requested hold. Expirations processed before admission remain committed.

## Promise lifecycle

```text
Held { expires_at } → Committed → Released
         │
         └──────────→ Expired
```

- `Held` reserves capacity temporarily.
- `Committed` confirms the same reservation without changing usage.
- `Released` no longer consumes capacity.
- `Expired` is a hold that reached its deadline before commit.

Held and committed promises consume capacity only during their claim intervals. Released and expired promises do not consume capacity.

A hold is expired when:

```text
expires_at <= now
```

Due expirations are processed before each mutating command. Commit at or after the deadline fails with `HoldExpired`; the expiration transition itself remains applied.

## Commands, idempotency identity, and events

Every internal mutation is represented by a `Command` containing a common `(ClientId, IdempotencyKey)` pair and one `CommandOperation`. The control API generates resource-pool and promise IDs before command application. `Engine::apply(command, now)` therefore uses neither randomness nor the system clock.

The pair `(ClientId, IdempotencyKey)` identifies one command. PromiseDB hashes the normalized `CommandOperation` with BLAKE3 and caches its complete original response. An exact retry returns that response without processing expirations, consuming a sequence, emitting events, or inspecting current state. Reusing the pair with a different normalized operation returns `IdempotencyConflict`.

Both successful and error responses are cached. Idempotency keys are scoped by client, so different clients may use the same key independently. Bundle claim order is not significant for command identity; claims are sorted canonically before hashing.

A command describes the requested mutation, a `CommandResult` describes its immediate business outcome, and events describe successful state changes. Expiration events are emitted before the requested-operation event. Capacity revision and deficit audit events may share the revision's single transition sequence.

Queries are pure and do not process hold deadlines. Callers requiring an up-to-date deadline boundary must apply `ProcessExpirations` or another mutating command first.

## Version and sequence

Each promise has a local version beginning at one. Every successful promise transition increments it. Mutations receive an expected version and reject stale requests with `VersionConflict`.

The engine also maintains one global monotonic `SequenceNumber`. Each successful durable transition receives exactly one sequence. A rejected command does not consume a sequence, although expirations processed before it do.

## Release

Release transitions either a live hold or committed promise to `Released`. In the current-state model it removes the promise's claims from active usage. Historical facts will be preserved by durable events rather than by keeping released claims in the active usage index.

## Replace

Replace atomically changes the bundle and live state of a held or committed promise. The promise keeps its ID and `created_sequence`; its local version increments and `updated_sequence` advances.

Admission is evaluated against the final usage:

```text
final usage = current usage - old bundle + new bundle
```

The old bundle is never released as an observable intermediate state. A replacement can therefore fit when the new bundle needs capacity currently held by the same promise. The target may be `Held { expires_at }` or `Committed`; a new hold deadline must be strictly later than `now`.

Insufficient capacity returns `ReplaceOutcome::Unavailable { conflicts }`. It does not change the promise, timelines, version, or requested replacement sequence. Due expirations processed before replacement remain committed. Engine or validation failures likewise publish no partial replacement.

## Determinism

Domain transitions receive `now` explicitly. Public engine operations obtain it from an injected clock. Replay and future replication must reuse the timestamp selected for the original command.

Canonical representations use chronological ordering and do not depend on hash-map iteration, memory addresses, internal clocks, or unseeded randomness.
