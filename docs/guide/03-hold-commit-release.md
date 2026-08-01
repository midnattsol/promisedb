# 4. Hold, commit, release, and expiration

A promise moves through a small lifecycle.

```text
Held ──commit──> Committed ──release──> Released
  │
  └──deadline──> Expired
```

## Hold

A hold is the action that asks PromiseDB to reserve a bundle temporarily.

If accepted, the new promise is in:

```text
Held { expires_at }
```

`Hold` is the operation. `Held` is the resulting promise state.

A held promise already consumes capacity. This prevents another request from taking the same capacity while the client finishes its workflow.

`HoldOneOf` performs the same transition for the first feasible bundle in an ordered `Choice`. It creates exactly one promise and reports the selected alternative index. Rejected alternatives leave no partial reservation. If all alternatives are unavailable, the result includes conflicts for every alternative and emits no `HoldCreated` event.

## Commit

Commit confirms a live hold:

```text
Held → Committed
```

It does not check capacity again and does not change total usage. Capacity was already accepted by the hold.

Commit requires the expected promise version. This prevents a stale client from modifying a newer promise revision.

## Expiration

A hold is expired when:

```text
expires_at <= now
```

Expiration releases its claims and increments its version. The engine processes due expirations before mutating commands, or through an explicit `ProcessExpirations` command.

## Release

Release ends a held or committed promise:

```text
Held or Committed → Released
```

Its claims stop consuming capacity.

## Replace

Replace atomically changes the bundle and live state of an existing promise:

```text
final usage = current usage - old bundle + new bundle
```

The old bundle is never released as a visible intermediate step. If the new state is unavailable, the original promise remains unchanged.

Next: [Slack and deficits](04-slack-and-deficits.md).
