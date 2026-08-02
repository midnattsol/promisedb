# ADR-0013: Atomic snapshots and WAL compaction

Status: Accepted

## Context

An indefinitely retained segmented WAL makes restart time and disk usage grow with complete history. Compaction must not lose terminal promises, audit events, or exact idempotency responses, and a crash must always leave either the old WAL path or a complete snapshot plus exact suffix.

## Decision

PromiseDB uses a manual, versioned, checksummed snapshot format anchored by the last represented WAL record sequence. The authoritative snapshot boundary retains every resource pool, all promises including terminal states, ordered retained events, all idempotency identities/hashes/responses, domain sequence, and the `u128` runtime publication revision. It excludes clocks and reconstructible slack timelines. Event pruning is not part of v1, so `events_pruned_through` is zero.

Snapshot creation first synchronizes an empty active segment at watermark plus one when the WAL sequence has a successor. It writes `SNAPSHOT.tmp` with create-new semantics, synchronizes it, renames it to the canonical 20-digit watermark name, and synchronizes the snapshot directory. Only then may covered non-active WAL segments and older snapshots be deleted, with directory synchronization. Cleanup failure after rename is explicitly reported as committed cleanup.

Open acquires the database lock, removes only recognized temp files, and selects the highest canonical snapshot. It validates the complete checksum, framing, UUID, centralized state-machine semantics version, filename/header watermark, payload canonicality, budgets, and authoritative references before accepting it. Corruption of the highest snapshot is fatal; recovery does not fall back. Only after a valid snapshot and required exact WAL suffix exist may obsolete lower prefixes be ignored or removed. Derived indexes rebuild once after suffix replay.

Manifest v2 persists all snapshot limits and rejects manifest v1. This is acceptable before a compatibility-bearing release.

## Consequences

Restart work is linear in retained snapshot state plus the post-snapshot WAL suffix, rather than complete WAL history. Snapshot size remains linear in retained pools, promises, events, and idempotency records; because v1 does not prune events or terminal entities, snapshots are not a retention policy by themselves. Full BLAKE3 checksums and explicit little-endian tags/lengths provide deterministic corruption detection without adding serialization dependencies.

The forced empty suffix segment creates a simple compaction boundary and allows obsolete prefix corruption to be ignored safely only after snapshot validation. Snapshot installation can succeed while cleanup fails, so callers must distinguish the committed-cleanup error from pre-install failure.

## Alternatives considered

- Replaying commands was rejected because admission semantics and external time must not be rerun during recovery.
- Serializing derived timelines was rejected because they are reconstructible and would create a second authoritative representation.
- Falling back to an older snapshot was rejected because it can conceal corruption and make recovery selection non-obvious.
- Serde-based formats were rejected to keep explicit stable tags, bounds, and layout control without new dependencies.
