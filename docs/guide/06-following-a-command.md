# 7. Following one command through the engine

Consider a request to hold this bundle:

```text
1 machine
1 operator
1 inspection slot
[10:00, 11:00)
```

The control API first prepares stable identities:

```text
ClientId
IdempotencyKey
PromiseId
```

It constructs `CommandOperation::Hold` and calls:

```text
Engine::apply(command, now)
```

## 1. Idempotency lookup

The engine normalizes and hashes the operation, then looks up:

```text
(ClientId, IdempotencyKey)
```

- Exact previous hash: return the stored response immediately.
- Different previous hash: return `IdempotencyConflict`.
- No record: continue.

## 2. Process expirations

Due held promises are expired in deterministic deadline-and-ID order. Each expiration restores slack, increments the promise version, receives a sequence, and emits `HoldExpired`.

## 3. Evaluate the complete bundle

Claims are grouped by resource pool. The engine evaluates every pool on temporary timeline copies.

If any claim is unavailable, the engine returns all normalized conflicts. It publishes none of the candidate timeline changes.

## 4. Publish an accepted hold

If every claim fits, the engine:

```text
creates the promise with the supplied PromiseId
publishes adjusted slack timelines
publishes the next global sequence
emits HoldCreated
```

These changes are visible together.

## 5. Cache the response

Whether the operation returned success, unavailability, or a structured error, the engine stores its command hash and response under the idempotency identity.

A later exact retry therefore observes the original response, not a newly evaluated state.

## Where to continue

- Exact behavioral rules: [Semantics](../semantics.md)
- Component and transition boundaries: [Architecture](../architecture.md)
- Reasons behind major decisions: [ADRs](../adr/README.md)
- Rust API details: run `cargo doc --no-deps --open`
