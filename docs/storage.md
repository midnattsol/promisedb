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

`storage::recovery::recover` remains the generic opaque scanner. `recover_engine` starts at record one, while `recover_engine_with_expected` accepts an explicit sequence and preceding engine state for future snapshot anchors. Engine recovery decodes and installs authoritative after-values, exact events, final domain sequence, and persisted idempotency records without executing embedded commands. It verifies emitted event timestamps against record timestamps and rebuilds derived slack timelines once after clean EOF. Partial tails remain errors; `RecoveryError` exposes the last valid byte offset for a future locked file repair layer.

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

This stage deliberately does not implement directory layout, segment rotation, process locking, snapshots, or automatic tail truncation. Those belong to the next file-layer substage, where repair can occur only while holding the database lock.
