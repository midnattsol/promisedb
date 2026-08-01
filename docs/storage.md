# Storage status

PromiseDB has storage **boilerplate**, not an integrated durable WAL.

## Implemented

- `storage::encode_command` and `storage::decode_command` provide a deterministic,
  versioned binary representation of complete commands. Command format version `1`
  is now defined to use explicit tags, little-endian fixed-width integers,
  little-endian `u32`-length-prefixed strings and collections, and stable UUID bytes.
  It preserves bundle claim input order and ordered choice alternatives.
- Decoding rebuilds domain values through validated constructors and reports
  structured `StorageError` values.
- `WalBackend` defines raw `append`, `flush`, and `sync` operations.
  `MemoryWal` retains bytes and operation counters for tests. `FileWal::create`
  exclusively creates a new path, while `FileWal::open` requires an existing path.
- `Durability` and `persist` apply `None`, `Flush`, or `Sync` policy to one owned
  `Vec<u8>`.

## Learner-owned TODO

`storage::record` deliberately leaves record framing and reading as `todo!`. The
learner still owns these durable-format decisions:

- checksum algorithm, checksum bytes, and checksum coverage;
- clean EOF, partial-tail, and corrupt-record reader behavior.

Every WAL record has this fixed serialized 32-byte header:

| Byte range | Field | Encoding |
| --- | --- | --- |
| `0..4` | magic | ASCII `PDBW` |
| `4..8` | total remaining record length | little-endian `u32` |
| `8` | record format version | `u8` |
| `9..16` | reserved | seven zero bytes |
| `16..24` | record sequence | little-endian `u64` |
| `24..32` | timestamp | little-endian `i64` |

A different complete magic signature identifies corruption rather than a PromiseDB
record. The length counts every byte after the length field itself, including bytes
`8..32` of the fixed header, the command payload, and the as-yet-unselected checksum.

WAL records have their own mandatory monotonic `record_sequence`, separate from
engine event sequences. Record sequences start at `1`; zero means that the WAL is
empty and is not a valid `RecordSequence`. A command may produce zero, one, or many
domain events, so these counters intentionally describe different orders.

Record encoding can allocate its final `Vec<u8>`, write the header, and call the
internal `encode_command_into` helper to append the command directly before passing
that same `Vec<u8>` to `persist`; no command-payload `Vec` or boxed-slice conversion is
required.

Ignored guide tests name the remaining required cases without selecting those designs.
`storage::recovery::recover` is only compile-clean control-flow scaffolding and
cannot run until the record reader exists. It does not replay into `Engine`.

No engine publication ordering has been integrated. Consequently, the presence of
raw file operations and a command codec must not be interpreted as a durability
guarantee: PromiseDB remains an in-memory state machine until record framing,
recovery, and engine/WAL ordering are designed and implemented together.
