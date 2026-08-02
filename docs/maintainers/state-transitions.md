# State transitions

## Authoritative mutation boundary

`Engine::apply(command, now)` is the direct in-memory path: idempotency lookup, checked next publication revision, `apply_once` on published state, response caching, then revision assignment. It must not clone complete `EngineState` or call durable preparation. Durable orchestration uses the crate-private boundary:

```text
Engine::prepare_batch(commands)
→ Engine::can_publish(prepared)
→ encode/frame and persist timestamped transitions
→ Engine::publish_batch(prepared)
```

Nonempty batch preparation pre-scans first-seen identities, preflights the final publication revision, clones `EngineState` exactly once, executes commands sequentially through direct internal dispatch, caches exact responses in the candidate, and derives timestamped effects. It never mutates published state. Version 1 may diff each command cumulatively against the original batch base to avoid per-command clones. `can_publish` runs before I/O; `publish_batch` then replaces state without checked arithmetic or fallible domain work.

An exact idempotent retry returns before inspecting `now` or current state.

## Prepare before publishing

For multi-object operations, perform all fallible work on clones:

```text
clone promise or timeline
→ validate final state
→ calculate next version and sequence
→ publish all affected objects
```

Do not mutate one pool and then discover that another pool fails. Do not release an old promise before evaluating replacement usage. For an ordered choice, evaluate each alternative against unchanged current state, retain only conflict data for rejected alternatives, and publish only the first prepared feasible bundle.

## Expirations

Mutating transitions process holds with `expires_at <= now` first. Expirations are ordered by deadline and then promise ID. Each expiration receives its own sequence and event.

If the requested operation later fails, completed expirations remain in the same prepared durable transition and are published with its cached error. Tests must distinguish the requested operation's sequence from expiration sequences.

Queries such as `list_at_risk` and `explain_unavailable` are pure. Callers must apply `ProcessExpirations` when they need a deadline boundary without another mutation.

## Versions and sequence

- `Version` is local to one promise and starts at one.
- Client promise mutations require `expected_version`.
- Every successful promise transition increments its version.
- `SequenceNumber` is global to the engine.
- A rejected requested transition does not consume its own sequence.
- Multiple audit events describing one capacity revision may share that transition sequence.
- Publication revision advances for every first-seen command, independently of domain sequence.
- Revision exhaustion is checked before direct command execution, durable candidate cloning, or recovery mutation.

## Response caching

Idempotency records cache both `Ok(CommandResult)` and `Err(DomainError)`. Do not move cache lookup after expiration processing: retries must reproduce the original response without new state changes.

## Recovery

Decode and install `DurableTransition` effects in WAL order. Validate duplicated command identity, persisted hash, unique first-seen idempotency identity, record/event timestamp equality, contiguous/nondecreasing event sequences, final sequence, entity IDs, and restored domain invariants. Never call `Engine::apply` or another admission function during recovery. Rebuild every `SlackTimeline` from resource pools and promises once after the complete stream. Preserve `RecoveryError::last_valid_offset`; only a future locked file layer may use it to repair a final partial tail.

## Publication checklist

Before returning success from a new transition, verify:

- all domain objects contain the final state;
- every affected slack timeline matches active promises;
- version and update sequence are correct;
- global sequence advanced exactly as intended;
- events are in deterministic order;
- the command response can be cloned into idempotency state;
- the durable effect contains exact ordered new events and changed authoritative after-values;
- no `SlackTimeline` is serialized; and
- persistence completes before revision-checked publication.
