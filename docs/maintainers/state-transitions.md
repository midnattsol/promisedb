# State transitions

## Authoritative mutation boundary

The durable-facing path is:

```text
Engine::apply(command, now)
```

Its order is:

```text
canonical operation hash
→ idempotency lookup
→ operation dispatch
→ due expiration processing
→ validation and temporary-state preparation
→ sequence allocation
→ publish authoritative state and derived indexes
→ emit events
→ cache the complete command response
```

An exact idempotent retry returns before inspecting `now` or current state.

## Prepare before publishing

For multi-object operations, perform all fallible work on clones:

```text
clone promise or timeline
→ validate final state
→ calculate next version and sequence
→ publish all affected objects
```

Do not mutate one pool and then discover that another pool fails. Do not release an old promise before evaluating replacement usage.

## Expirations

Mutating transitions process holds with `expires_at <= now` first. Expirations are ordered by deadline and then promise ID. Each expiration receives its own sequence and event.

If the requested operation later fails, completed expirations remain published. Tests must distinguish the requested operation's sequence from expiration sequences.

Queries such as `list_at_risk` and `explain_unavailable` are pure. Callers must apply `ProcessExpirations` when they need a deadline boundary without another mutation.

## Versions and sequence

- `Version` is local to one promise and starts at one.
- Client promise mutations require `expected_version`.
- Every successful promise transition increments its version.
- `SequenceNumber` is global to the engine.
- A rejected requested transition does not consume its own sequence.
- Multiple audit events describing one capacity revision may share that transition sequence.

## Response caching

Idempotency records cache both `Ok(CommandResult)` and `Err(DomainError)`. Do not move cache lookup after expiration processing: retries must reproduce the original response without new state changes.

## Publication checklist

Before returning success from a new transition, verify:

- all domain objects contain the final state;
- every affected slack timeline matches active promises;
- version and update sequence are correct;
- global sequence advanced exactly as intended;
- events are in deterministic order;
- the command response can be cloned into idempotency state.
