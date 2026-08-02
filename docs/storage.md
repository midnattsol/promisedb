# Storage status

PromiseDB now has a generic, versioned WAL record envelope in addition to its command
codec and raw WAL backends. Engine publication ordering is not yet integrated, so the
crate is still an in-memory state machine from an end-to-end durability perspective.

## WAL record format version 1

`storage::record::Record` owns three values: a non-zero `RecordSequence`, an `i64`
`Timestamp`, and an opaque `Vec<u8>` payload. The record layer neither knows nor decodes
`Command`; transitioning command codec output into record payloads is pending work.

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

`storage::recovery::recover` is a generic scanner that uses those limits and returns
validated opaque records in strict sequence order. It performs no command decoding and
does not apply records to `Engine`.

Record-version errors and command-codec version errors are distinct. The command codec
temporarily retains the compatibility name `StorageError::UnsupportedVersion`, while
WAL framing reports `StorageError::UnsupportedRecordVersion`.

## Other implemented storage pieces

- `storage::encode_command` and `storage::decode_command` provide the existing
  deterministic version-1 representation of complete commands. Connecting that codec
  to the new opaque record payload is intentionally pending.
- `WalBackend` defines raw `append`, `flush`, and `sync` operations.
  `MemoryWal` retains bytes and operation counters for tests. `FileWal::create`
  exclusively creates a new path, while `FileWal::open` requires an existing path.
- `Durability` and `persist` apply `None`, `Flush`, or `Sync` policy to one owned
  `Vec<u8>`.

No engine/WAL publication ordering has been integrated. Raw file operations, command
encoding, and validated record framing therefore do not yet constitute an end-to-end
durability guarantee.
