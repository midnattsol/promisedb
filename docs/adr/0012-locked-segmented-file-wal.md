# ADR-0012: Locked segmented file WAL

- Status: Accepted
- Date: 2026-08-02

## Context

The generic durable coordinator and record reader establish persist-before-publish ordering and strict record framing, but a production file database also needs process exclusion, durable database identity and format metadata, bounded files, multi-segment recovery, and a narrowly safe crash-repair policy. A lockfile created with `create_new` is not a lifetime lock: stale files cannot distinguish a live owner from a crashed process.

Repair is safe only while one coordinator owns the database directory. A partial write at the end of the active segment can be discarded because no complete record follows it. The same symptom in a sealed segment, or any checksum-valid structural/sequence corruption, cannot be inferred to be disposable.

## Decision

`FileDatabase` specializes `Database<SegmentedWal>`. Creation and opening acquire a nonblocking exclusive operating-system lock on the stable `LOCK` file using Rust 1.97 `std::fs::File` locking. The lock handle is owned by `SegmentedWal` for the coordinator lifetime. Contention is reported as `AlreadyOpen`; other lock failures remain structured.

Each database has a fixed 64-byte, versioned `MANIFEST` containing its version-4 database UUID, persisted maximum record length, and state-machine semantics version. The final 16 bytes are BLAKE3-128 over the canonical 48-byte prefix. Creation writes `MANIFEST.tmp`, synchronizes it, renames it, and synchronizes the database directory. Opening requires the caller's `RecordLimits` to exactly match the persisted value. The operational segment target is not persisted and may change between opens.

The `wal/` directory contains only canonical `20-digit-first-sequence.wal` files and recognized `*.wal.tmp` creation debris. Every segment starts with a fixed 64-byte versioned header containing magic, flags, header length, database UUID, first global record sequence, zero reserved bytes, and BLAKE3-128. Segment creation writes and synchronizes a temp header, renames it, then synchronizes `wal/`. Segment headers are validated separately and never enter `RecordReader`.

`SegmentedWal` validates each internally generated append group as contiguous records beginning at its expected sequence. If a nonempty active segment plus the complete group would exceed the target, it creates a successor named for the group's first sequence before appending. A group is never split. An oversized group is allowed in a fresh segment. `Sync` synchronizes the active file; `Flush` is not a crash-durability promise. Segment-entry durability is established independently by the temp/sync/rename/directory-sync protocol.

Opening validates names, headers, UUIDs, segment starts, record sequences, checksums, and transitions without resynchronization. `EngineRecovery` feeds segments sequentially and rebuilds derived indexes once after all feeds. A `PartialTail` is truncated to the last complete physical boundary only in the final active segment, followed by file synchronization. A partial tail in a sealed segment and every complete corruption are fatal. Errors identify the segment path and physical byte offset.

## Consequences

- A live database is excluded even within the same process when opened through another file handle; a stale `LOCK` file is harmless.
- Database identity and record allocation limits survive restarts and are checked before recovery.
- Rotation bounds ordinary segment growth while preserving one coordinator group as one backend append and one segment.
- Recovery memory is bounded by one record/transition plus authoritative engine state rather than the complete WAL.
- Persisted-but-unpublished groups are installed exactly once on restart, and appending resumes at the exact next global sequence.
- Only an unambiguously incomplete final record is repaired automatically.
- Snapshots, segment deletion/compaction, cross-host/network-filesystem lock guarantees, and background/asynchronous I/O remain out of scope.

## Alternatives considered

### Use lockfile existence as ownership

Rejected because `create_new` leaves stale ownership markers after crashes and does not represent the lifetime of a live file handle.

### Put segment headers through `RecordReader`

Rejected because segment metadata has a separate version, identity, and checksum contract and is not a logical WAL record.

### Split oversized groups across segments

Rejected because one `Database::apply_batch` must remain one backend append and preserve its single failure/durability boundary.

### Repair any partial or corrupt segment

Rejected because truncating a sealed segment can discard known later history, while resynchronization after complete corruption can silently accept an invalid sequence. Both require explicit offline/operator policy rather than automatic open-time mutation.
