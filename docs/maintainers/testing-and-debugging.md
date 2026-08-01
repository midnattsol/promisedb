# Testing and debugging

## Standard validation

Run from the repository root:

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings -D missing-docs' cargo doc --no-deps
git diff --check
```

Do not report success unless the command was executed. Tests currently run with Rust's normal allocator behavior; future storage and long-running tests should add explicit leak and corruption coverage where applicable.

## Test placement

Keep focused unit tests beside the owning code:

```text
domain invariant     src/domain/<type>.rs
canonical hashing    src/idempotency.rs
index behavior       src/index/slack_timeline.rs
transition behavior  src/engine.rs
public outcome type  src/engine/<outcome module>.rs
```

Use engine integration tests when correctness depends on several modules publishing together.

## What to assert after failure

For a rejected mutation, compare more than the error:

- promise value and version;
- affected timelines;
- resource-pool capacity curve;
- global sequence;
- emitted event count;
- idempotency record behavior.

Remember that due expirations processed before the requested operation may legitimately remain committed.

## Debugging availability

When indexed admission surprises you:

1. inspect the candidate claims grouped by pool;
2. list every relevant claim boundary and capacity breakpoint;
3. inspect effective slack at each segment start;
4. compare indexed admission with the slow test oracle;
5. rebuild the timeline from capacity and active promises;
6. check whether a forced deficit changes the permitted floor.

The first differing interval is usually more useful than the final boolean.

## Debugging idempotency

Check:

```text
ClientId
IdempotencyKey
OperationTag
canonical field order
bundle claim normalization
choice alternative order
cached CommandResponse
```

An exact retry must not process a newer `now`. If a retry emits events or changes sequence, lookup happened too late.

If a legitimate format change alters hashes, update the versioned hash domain or provide migration semantics; do not silently change existing tags.

## Debugging expiration

Test boundaries explicitly:

```text
now < expires_at   live
now == expires_at  expired
now > expires_at   expired
```

Verify deadline-and-ID ordering and ensure retrying expiration produces no duplicate transition.

## Before committing

- Keep unrelated fixes out of the change.
- Review `git diff --check` and the complete diff.
- Update semantics or ADRs when behavior or durable representation changes.
- Keep derived indexes reconstructible.
- Do not suppress dead code or warnings to hide incomplete integration.
