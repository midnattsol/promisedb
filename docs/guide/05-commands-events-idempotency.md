# 6. Commands, events, and idempotency

These concepts answer different questions:

```text
Command        What was requested?
CommandResult  What response did the request receive?
Event          What state change occurred?
```

## Commands

The control API prepares commands before they enter the engine. It generates entity IDs and supplies:

```text
ClientId
IdempotencyKey
CommandOperation
```

`Engine::apply(command, now)` receives time explicitly. The state machine does not read the system clock or generate random IDs.

## Results and events

A hold with insufficient capacity returns an unavailable `CommandResult`, but emits no hold-created event because no promise was created.

One command can produce several events. If old holds are due, a create-resource command might produce:

```text
HoldExpired
HoldExpired
ResourceCreated
```

Expiration events come first.

## Idempotency

The pair `(ClientId, IdempotencyKey)` identifies one request.

On the first application, PromiseDB stores:

```text
canonical command hash
original response
```

An exact retry returns the stored response without:

- applying the operation again;
- processing expirations;
- consuming another sequence;
- emitting events.

If the same pair is reused with a different operation, PromiseDB returns `IdempotencyConflict`.

## Canonical hashing

Commands are converted to a stable binary representation and hashed with BLAKE3. The format uses explicit tags, fixed byte order, lengths for strings and collections, and stable UUID bytes.

Bundle claim order has no meaning, so claims are sorted before hashing. Two bundles containing the same claims in different input orders have the same command hash. Choice alternative order controls selection, so canonical hashing preserves that order while normalizing claims inside each bundle.

Next: [Following one command through the engine](06-following-a-command.md).
