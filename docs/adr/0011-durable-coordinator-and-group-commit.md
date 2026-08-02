# ADR-0011: Durable coordinator and synchronous group commit

- Status: Accepted
- Date: 2026-08-02

## Context

Prepared transitions and WAL framing do not by themselves enforce persist-before-publish ordering. Callers must not gain mutable access to the engine or backend that can bypass ordering. Batches also need one durability boundary rather than one append and synchronization per command.

An I/O error from append, flush, or sync cannot prove whether some or all bytes became durable. Continuing writes from the old in-memory state could reuse record sequences or diverge from recoverable effects.

## Decision

`storage::Database<B: WalBackend>` owns `Engine`, the synchronous backend, durability policy, record limits, next record sequence, and a poisoned-write flag. It exposes immutable engine and backend views only.

For a nonempty command batch, `Database`:

1. prepares all commands sequentially against one cloned candidate state;
2. preflights publication and every required record sequence;
3. encodes each first-seen transition directly into its framed record within one final byte vector;
4. backpatches headers, padding, and checksums without intermediate payload or record vectors;
5. performs one backend append and the selected flush or sync;
6. publishes the already-preflighted candidate without fallible arithmetic.

Empty and retry/conflict-only batches perform no WAL I/O. Encoding, framing, preparation, and sequence failures happen before I/O and do not poison the database. Any append, flush, or sync error returns `Indeterminate`, leaves engine state unpublished, and poisons subsequent writes. Immutable reads remain available.

Recovery scans strict record order, decodes transitions, validates event timestamps against record timestamps, installs effects without command admission, and rebuilds derived indexes once after the complete stream. Errors expose the last valid byte offset but generic recovery never truncates or accepts a partial tail.

## Consequences

- Multi-command groups use one append and one durability operation while preserving command order.
- Persisted-but-unpublished records are applied exactly once after restart.
- Record sequence exhaustion is detected before I/O.
- A poisoned process must be restarted and recovered before accepting writes.
- Full recovery starts at record one; an explicit expected-sequence API supports future snapshot anchors.
- Directory layout, segment rotation, process locking, and locked final-tail repair are defined by [ADR-0012](0012-locked-segmented-file-wal.md). Generic readers still report repair metadata but never mutate files.

## Alternatives considered

### Expose engine and backend mutably

Rejected because callers could publish without persistence or append records unrelated to engine state.

### Continue after backend errors

Rejected because the durable prefix is indeterminate and record-sequence reuse could corrupt recovery ordering.

### Append each command separately

Rejected because it loses group-commit efficiency and creates additional failure boundaries without improving transition semantics.

### Automatically truncate partial tails during generic recovery

Rejected because repair requires exclusive file ownership and locking that the generic `Read` layer cannot guarantee.
