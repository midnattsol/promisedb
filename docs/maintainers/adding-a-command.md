# Adding a command

Use this checklist when extending `CommandOperation`.

## 1. Define deterministic input

Edit `src/command.rs`.

A command operation must contain every value needed for replay. IDs must already be generated. Do not place the system clock, references, pointers, or derived indexes in a command.

Add one operation-specific `CommandResult` variant. Represent normal business rejection, such as unavailable capacity, inside the result rather than as an engine failure.

## 2. Extend canonical hashing

Edit `src/idempotency.rs`.

- Assign a new permanent `OperationTag` value. Never renumber existing tags.
- Implement every field in the `write_canonical_operation` match.
- Use `CanonicalHash` implementations or add a private implementation for the new value type.
- Use fixed-width big-endian integers.
- Prefix variable-length strings and collections with lengths.
- Normalize collections only when their order is semantically irrelevant.
- Add a hash test proving that meaningful field changes alter the hash.
- Add or update a golden vector when changing the format intentionally.

Changing canonical bytes is a compatibility change because recovered idempotency records depend on them.

## 3. Dispatch once

Edit `Engine::apply_once` in `src/engine.rs`.

Keep idempotency lookup in `Engine::apply`; do not repeat it in individual transitions. Dispatch to a deterministic `*_at` method that receives `now` explicitly.

## 4. Implement the transition

Prefer this structure:

```text
process expirations
→ validate references and versions
→ prepare cloned final state
→ calculate sequence
→ publish state and indexes
→ emit events
→ return result
```

The idempotency wrapper caches the response after dispatch.

## 5. Define audit events

Edit `src/event.rs` only if existing event kinds and payloads are insufficient.

Events contain stable values, not domain references. Emit nothing for a business outcome that made no state change. Preserve expiration-before-requested-event ordering.

## 6. Test all layers

Add tests for:

- canonical hash stability and field sensitivity;
- successful dispatch;
- structured business rejection;
- failure without partial mutation;
- exact retry without sequence or event changes;
- same key with a different payload;
- expiration ordering when relevant;
- version conflict when the command targets a promise.

Update semantics for behavior, architecture for boundaries, and an ADR for durable design decisions.
