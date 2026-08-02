# Storage status

PromiseDB has a public synchronous `Database<B: WalBackend>` coordinator over its versioned WAL envelope, raw backends, transition codec, prepared engine batches, and effect-only recovery. The coordinator owns engine and backend mutation so persistence always precedes publication.

## WAL record format version 1

`storage::record::Record` owns three values: a non-zero `RecordSequence`, an `i64` `Timestamp`, and an opaque `Vec<u8>` payload. The record layer neither knows nor decodes the payload. Durable callers place an encoded prepared transition in that payload; commands are embedded only as audit data inside the transition.

Every record consists of a fixed 32-byte header, payload bytes, zero alignment padding,
and a final 16-byte checksum:

| Byte range | Field | Encoding |
| --- | --- | --- |
| `0..4` | magic | ASCII `PDBW` |
| `4` | record format version | `u8`, currently `1` |
| `5` | flags | `u8`, must be `0` in version 1 |
| `6..8` | header length | little-endian `u16`, must be `32` |
| `8..12` | total record length | little-endian `u32` |
| `12..16` | payload length | little-endian `u32` |
| `16..24` | record sequence | little-endian `u64` |
| `24..32` | timestamp | little-endian `i64` |
| `32..` | body | payload, zero padding, checksum |

The total record length includes the header and checksum and is always divisible by
eight. Because the 32-byte header and 16-byte checksum are already aligned, padding is
`(8 - payload_len % 8) % 8`, from zero through seven bytes. Padding bytes must be zero.
The format ceiling is `4,294,967,288`, the largest 8-byte-aligned value representable by
`u32`.

The checksum is BLAKE3-128: the first 16 bytes of the BLAKE3 digest over the contiguous
`header || payload || padding`. The trailing checksum bytes are excluded from their own
coverage.

`record::encode` accepts explicit `RecordLimits`, validates representability and the
configured maximum before reserving, and builds the result with one allocation. The
default maximum total record size is 64 MiB. Limits must be at least 48 bytes, no larger
than the format ceiling, and divisible by eight.

## Reading and recovery

`RecordReader<R>` owns a generic `Read`, tracks its byte offset, and enforces strict
record sequencing. `RecordReader::new` expects `RecordSequence::FIRST`; an explicit
starting sequence can be supplied with `RecordReader::with_expected_sequence`.

The reader:

- returns clean EOF only when zero bytes are read at a record boundary;
- reports every non-empty truncated prefix as a structured partial tail;
- validates magic, version, flags, header and body lengths, sequence, zero padding, and
  checksum without resynchronizing;
- rejects a declared record size against `RecordLimits` before allocating its body;
- reports complete corruption with a structured reason and the failing record's byte
  offset; and
- tolerates ordinary short reads and interrupted reads.

`storage::recovery::recover` remains the generic opaque scanner. `recover_engine` starts at record one, while `recover_engine_with_expected` accepts an explicit sequence and preceding engine state for future snapshot anchors. `EngineRecovery::feed` extends the same strict global sequence across multiple readers; `finish` rebuilds derived indexes once after every segment. Recovery decodes and installs authoritative after-values, exact events, final domain sequence, and persisted idempotency records without executing embedded commands. Generic recovery never repairs input. The locked file layer alone may use `RecoveryError::last_valid_offset` to truncate a partial final active tail.

Record, command, and transition payload versions are distinct. The command codec retains `StorageError::UnsupportedVersion`, transition payloads report `UnsupportedTransitionVersion`, and WAL framing reports `UnsupportedRecordVersion`.

## Transition payload format version 1

The crate-private transition codec is manual and layout-independent. Every integer is little-endian, every enum has an explicit one-byte tag, UUID identities use their stable 16 bytes, and strings, nested command payloads, and collections use bounded `u32` byte/count prefixes. Changed resource pools and promises are encoded in ascending ID order; event order is preserved exactly. Decoding rejects unsupported tags, truncation, invalid UTF-8, non-canonical ordering, violated domain invariants, and trailing bytes.

A transition contains the original command, duplicated client/idempotency identity for validation, persisted 32-byte canonical command hash, exact `Result<CommandResult, DomainError>`, changed/new resource-pool and promise after-values, exact newly emitted events, and final engine `SequenceNumber`. It never contains `SlackTimeline`.

First-seen commands produce one transition even for errors and unavailable outcomes. Exact cached retries and idempotency conflicts produce none. A nonempty prepared batch clones engine state once and executes sequentially. Version 1 permits cumulative pool/promise after-values relative to the original batch base, reducing preparation cloning at the cost of larger later records in a group.

## Other implemented storage pieces

- `storage::encode_command` and `storage::decode_command` provide the deterministic version-1 representation used for the audit command nested in a transition.
- `WalBackend` defines raw `append`, `flush`, and `sync` operations.
  `MemoryWal` retains bytes and operation counters for tests. `FileWal::create`
  exclusively creates a new path, while `FileWal::open` requires an existing path.
- `Durability` and `persist` apply `None`, `Flush`, or `Sync` policy to one owned
  `Vec<u8>`.

`Database::apply_batch` preflights preparation and contiguous record sequences, then frames every durable item directly into one final group vector. Record framing reserves and backpatches each 32-byte header around a fallible append-only payload writer; transition encoding writes directly into that payload region, including its nested command length prefix. No per-transition payload or framed-record `Vec` is allocated. Framing errors roll the destination back to the current record start, preserving earlier group bytes, and no backend I/O occurs. The completed group uses one append and one selected flush/sync, then publishes infallibly. Empty and retry/conflict-only batches perform no I/O. Encoding and sequence failures do not poison. Any append, flush, or sync failure returns `Indeterminate`, leaves state unpublished, and poisons subsequent writes while reads remain available.

## Locked file database

`FileDatabase` is the specialized `Database<SegmentedWal>` production API. `create` requires a new database directory; `open` acquires an exclusive nonblocking operating-system lock on the stable `LOCK` file before inspecting or repairing any persistent state. The lock handle remains inside `SegmentedWal` for the coordinator lifetime. Lock contention returns `FileDatabaseError::AlreadyOpen`; the implementation does not use lockfile creation as lock semantics.

The database directory is:

```text
database/
├── LOCK
├── MANIFEST
├── snapshots/
│   └── 00000000000000000042.snapshot
└── wal/
    ├── 00000000000000000043.wal
    └── ...
```

### Manifest version 2

`MANIFEST` is exactly 96 canonical bytes. New databases write version 2; version 1 is intentionally rejected because no release compatibility is promised. In addition to the database UUID, record limit, and centralized state-machine semantics version, it persists the snapshot total-byte, top-level collection, string-byte, and nested-collection limits. Opening requires an exact requested match. Bytes `64..96` are the full BLAKE3 checksum over bytes `0..64`; unused bytes are zero.

The former version-1 layout was:

| Byte range | Field |
| --- | --- |
| `0..4` | ASCII `PDBM` |
| `4` | manifest version `1` |
| `5` | reserved zero |
| `6..8` | little-endian header length `64` |
| `8..24` | version-4 database UUID |
| `24..28` | little-endian persisted maximum record length |
| `28..32` | little-endian state-machine semantics version `1` |
| `32..48` | reserved zero |
| `48..64` | BLAKE3-128 over bytes `0..48` |

Creation uses `MANIFEST.tmp → sync file → rename → sync database directory`. The UUID is generated once with the existing `uuid` crate. Opening validates exact length, magic, version, reserved bytes, checksum, UUID semantics, semantics version, and `RecordLimits`. The caller must request exactly the persisted record maximum; mismatch is structured. The default remains 64 MiB.

### Segment header version 1

Every segment has a canonical 20-decimal-digit filename derived from its first global record sequence and a separate fixed 64-byte header:

| Byte range | Field |
| --- | --- |
| `0..4` | ASCII `PDBS` |
| `4` | segment version `1` |
| `5` | flags `0` |
| `6..8` | little-endian header length `64` |
| `8..24` | database UUID |
| `24..32` | little-endian first global record sequence |
| `32..48` | reserved zero |
| `48..64` | BLAKE3-128 over bytes `0..48` |

A successor is created as `*.wal.tmp → header sync → rename → wal/ directory sync`. Recognized segment temp files are removed only after locking. Segment headers never enter generic `RecordReader`; recovery seeks to physical offset 64 and reports segment paths plus physical offsets on failure.

Opening requires canonical names, strictly ordered segment starts, matching manifest UUIDs, valid header checksums, and contiguous global records. It streams segments through one `EngineRecovery` and rebuilds indexes once. Only `PartialTail` in the final active segment is repaired, by truncating to `64 + last_valid_record_offset` and synchronizing the file. Partial tails in sealed segments, complete checksum/format corruption, sequence gaps, UUID mismatches, and header failures are fatal; recovery never resynchronizes. An empty final active segment is valid.

`SegmentedWal` validates each complete append group and uses checked conversion and `u64` arithmetic for physical-length projection before any I/O. An unrepresentable segment length returns structured `StorageError::SegmentLengthOverflow` without writing or rotating. It rotates before append when a nonempty active segment plus the group would cross `segment_target`. The whole group enters one segment and remains one `WalBackend::append`; an oversized group is allowed on a fresh segment. The default target is 256 MiB and the minimum is one segment header plus one minimum record (112 bytes). The target is operational rather than persisted. `Sync` synchronizes the active file and is the default. `Flush` only flushes userspace state and is not crash durable.

## Snapshot format version 1 and compaction

Snapshots use canonical 20-digit WAL-watermark filenames ending in `.snapshot`; watermark zero represents an empty WAL. The fixed 128-byte header stores `PDBN`, version/flags/lengths, total and payload lengths, database UUID, state-machine semantics version, last represented WAL record sequence, independent domain sequence, `u128` publication revision, and `events_pruned_through` (currently required to be zero). Reserved bytes must be zero. A full 32-byte BLAKE3 digest over `header || payload` is the trailer.

The payload retains every resource pool, every promise including terminal promises, all retained events in exact order, and every idempotency identity, command hash, and exact response. Maps encode in key order. `SlackTimeline` and clocks are excluded; indexes rebuild once after snapshot validation and WAL suffix replay. Decoding enforces persisted total, top-level collection, string, and nested collection budgets before relevant allocation and validates ordering, uniqueness, entity/history references, and sequence bounds.

`create_snapshot` rejects poisoned coordinators. It first establishes and synchronizes an empty active segment at `watermark + 1` when a successor exists, captures state, then installs `snapshots/SNAPSHOT.tmp` by create-new, write, file sync, rename, and directory sync. Only after installation does it remove fully covered non-active segments and older snapshots, synchronizing each directory. A post-install cleanup failure is reported as committed cleanup with the installed path and watermark.

Open removes only the recognized snapshot temp, selects the highest canonical snapshot, and never falls back if that snapshot is corrupt. After complete UUID/semantics/checksum/header validation, obsolete lower WAL prefixes may be ignored; the required suffix must begin exactly at `watermark + 1`. Suffix records stream through `EngineRecovery`, preserving final-tail repair rules and exact append continuation. Snapshot creation and recovery are linear in retained authoritative state plus suffix size; retained terminal promises, events, and idempotency records make snapshot size linear in retention. Event pruning is not yet implemented, so `events_pruned_through` remains zero.

Asynchronous I/O and cross-host lock coordination are not implemented.
