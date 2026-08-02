# Semantics

## Time

`Timestamp` is an `i64` Unix UTC timestamp. The engine contains no timezone logic.

Intervals are always half-open:

```text
[start, end)
```

`start` is included and `end` is excluded. `start >= end` is invalid. Adjacent intervals such as `[0,10)` and `[10,20)` do not overlap.

## Quantity and slack

`Quantity` remains a non-negative `u64` containing integer subunits, but supported claim quantities and capacity values are bounded by the exported `MAX_QUANTITY = i64::MAX as u64`. Claim quantities must be in `1..=MAX_QUANTITY`; capacity may be zero but must not exceed `MAX_QUANTITY`. Constructors return the structured `QuantityOutOfRange` domain error for larger values.

Each resource pool has an immutable `Unit { name, subunits_per_unit }`. For a unit named `watts` with `1_000` subunits per unit, quantity `1_500` represents 1.5 watts. Unit names must not be empty and the scale must be greater than zero. Decimal parsing, display formatting, and unit aliases belong to the control API; the engine operates only on integer subunits.

Slack is signed:

```text
slack = capacity - active usage
```

Positive slack is spare capacity, zero is full utilization, and negative slack is a deficit. The derived slack index stores `Slack` as `i64`. Rebuild and admission paths aggregate usage and candidate demand in `i128`, compare against widened base slack, and narrow only representable final values with checked conversions.

## Capacity

A resource pool owns a piecewise-constant `CapacityCurve`. Segments are ordered, non-overlapping, and normalized. Adjacent equal-capacity segments are merged.

Capacity outside declared segments, including gaps, is zero.

## Capacity revisions and deficits

A capacity revision replaces one resource pool's complete capacity curve. Due hold expirations are processed first, and the candidate slack timeline is rebuilt from the replacement curve plus every active claim before publication.

`Strict` mode rejects the revision with `CapacityRevisionCreatesDeficit` if any resulting slack is negative. The pool, timeline, and requested revision sequence remain unchanged.

`Force` mode accepts the new physical reality even when active usage exceeds capacity. Negative slack is exposed as normalized `CapacityDeficit` intervals with positive deficit quantities and the IDs of overlapping active promises. `list_at_risk` reports those promises without selecting cancellations or victims.

A new hold cannot consume capacity in an existing deficit. An atomic replacement may leave a deficit only when its final slack is no worse than the slack before replacement; this permits moves that reduce an existing deficit without requiring an observable release-first transition.

## Claims and bundles

A claim consumes a positive quantity from one resource pool during one interval. A bundle is a non-empty collection of claims accepted or rejected atomically. A `Choice` is a non-empty ordered collection of alternative bundles.

A `RelativeClaim` contains a pool ID, half-open start and end offsets, and a positive quantity. Offsets may be negative, but the start offset must be less than the end offset. A `RelativeBundle` is non-empty. Materializing it at candidate start `s` translates each endpoint with checked `i64` addition, then constructs ordinary validated `Interval`, `Claim`, and `Bundle` values. Unrepresentable endpoints return `TimestampOverflow`.

Multiple claims in the same bundle may reference the same pool and overlap. Their quantities are evaluated together in `i128`, so aggregate candidate demand may exceed `i64::MAX` even though each claim is bounded. Aggregate demand above available slack is a normal unavailable result, not an index overflow, as long as the public conflict quantities remain representable as `u64`. Choice order is significant: alternatives are evaluated from index zero and the first feasible bundle is selected.

## Hold admission

An attempted hold has two normal outcomes:

```text
Held(promise_id)
Unavailable { conflicts }
```

Insufficient capacity is not an engine error. An unavailable outcome contains every normalized blocking interval across all referenced pools. Conflicts contain the pool, interval, combined candidate demand, available slack, missing quantity, and IDs of overlapping active promises.

Conflicts are ordered canonically by interval start, resource pool ID, and interval end. Adjacent conflicts within one pool are merged only when their quantities and conflicting promise IDs are identical. Capacity gaps are reported as zero availability.

Admission first evaluates disjoint demand intervals against the immutable base timeline using widened arithmetic and collects conflicts and prospective adjustments. Only a conflict-free result clones the timeline, narrows adjustments to stored `Slack`, and materializes them. Unavailable holds therefore do not create a promise, modify a timeline, or consume a sequence for the requested hold. Expirations processed before admission remain committed.

`HoldOneOf` applies the same admission rules to an ordered `Choice`, using one control-plane `PromiseId` and one `expires_at`. It stops at the first feasible alternative and creates exactly one promise containing that complete bundle. Rejected alternatives publish no promise, timeline, sequence, or event changes; prepared timeline copies are discarded.

`ChoiceOutcome::Held` identifies the promise and selected zero-based alternative index. If none fit, `Unavailable` returns one `ChoiceConflict` per alternative in choice order. Each entry contains its alternative index and that bundle's complete, canonically ordered `Vec<AvailabilityConflict>`. The requested operation then makes no state change.

## First-slot search

`find_first_slot` searches a `RelativeBundle` as a pure advisory query. `hold_first_slot` and durable `HoldFirstSlot` search and reserve within one serialized transition. Search bounds are candidate anchors, not materialized claim boundaries.

The search requires `earliest_start <= latest_start` and `step > 0`. It evaluates `earliest_start`, then repeatedly advances by `step` using checked timestamp arithmetic while the next candidate is at or before `latest_start`. The latest bound is therefore included only when stepping lands on it. Search stops safely if another step is not representable.

The first candidate whose complete materialized bundle passes ordinary indexed admission is returned as `Slot { start, bundle }`. Advisory search does not process expirations, consume capacity, emit events, or change sequence. It returns `None` when every candidate is unavailable.

An authoritative slot hold processes due expirations once, then validates the prepared promise ID, deadline, and search. A feasible candidate is published through the same accepted-hold transition as an ordinary hold, with no observable search-then-hold gap. `SlotOutcome::Held { promise_id, start }` identifies the selected anchor. `SlotOutcome::Unavailable { attempts }` reports the number of candidates examined, saturated at `u64::MAX`, without retaining every candidate's conflicts. Unavailability makes no requested-operation state change; expirations processed first remain committed.

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

Both successful and error responses are cached. Idempotency keys are scoped by client, so different clients may use the same key independently. Bundle claim order is not significant for command identity; claims are sorted canonically before hashing. Relative-bundle claim order is likewise insignificant and is sorted by pool ID, start offset, end offset, and quantity. `HoldFirstSlot` identity includes its prepared promise ID, relative bundle, earliest and latest starts, step, and deadline. Choice alternative order is significant and is preserved in the canonical representation, while each alternative bundle still uses canonical claim ordering.

A command describes the requested mutation, a `CommandResult` describes its immediate business outcome, and events describe successful state changes. A timestamped durable item records the exact response, authoritative after-values, newly emitted events, WAL timestamp, and final sequence. Expiration events are emitted before the requested-operation event. Capacity revision and deficit audit events may share the revision's single transition sequence.

`Database::apply_batch` evaluates commands in input order against one candidate. Exact retries and conflicts contribute responses but no WAL item. Every first-seen command contributes one timestamped item, including unavailable and error responses. Version 1 may repeat cumulative authoritative after-values from earlier commands in the same group; newly emitted events and idempotency identity are never cumulative.

Queries are pure and do not process hold deadlines. Callers requiring an up-to-date deadline boundary must apply `ProcessExpirations` or another mutating command first.

## Version and sequence

Each promise has a local version beginning at one. Every successful promise transition increments it. Mutations receive an expected version and reject stale requests with `VersionConflict`.

The engine maintains one global monotonic domain `SequenceNumber`. Each successful domain mutation receives exactly one sequence. A rejected or unavailable requested operation does not consume its own sequence, although expirations processed before it do.

Publication revision is separate. Every published first-seen command advances it once even when the command only creates an idempotency record or returns an error/unavailable outcome. Exact retries and idempotency conflicts publish nothing and advance neither counter. Revision arithmetic is checked: first-seen direct application returns `PublicationRevisionOverflow`, durable preparation returns `PreparationError::RevisionOverflow`, and recovery installation returns `InstallError::PublicationRevision` before state mutation.

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

Domain transitions receive `now` explicitly. Public engine operations obtain it from an injected clock. Direct `Engine::apply` executes against published state without cloning complete `EngineState`; durable preparation executes deterministic admission once against isolated candidate state.

Recovery and future replication install versioned durable effects; they never replay command admission. Snapshots retain terminal promises, ordered events, and exact idempotency responses; their WAL watermark is independent of domain sequence because rejected first-seen commands may consume WAL records without a domain mutation. Recovery also requires every emitted event timestamp to match its enclosing WAL record, and rebuilds derived indexes once after snapshot validation and all suffix effects are installed. The embedded original command and timestamp remain audit inputs, not recovery instructions. Canonical representations use chronological ordering and do not depend on hash-map iteration, memory addresses, internal clocks, or unseeded randomness.

Any append, flush, or sync failure makes the write outcome indeterminate. `Database` leaves the candidate unpublished, poisons further writes, and continues to permit immutable reads. Restart and recovery determine which complete records became durable.
