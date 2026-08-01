# Storage status

PromiseDB has storage **boilerplate**, not an integrated durable WAL.

## Implemented

- `storage::encode_command` and `storage::decode_command` provide a deterministic,
  versioned binary representation of complete commands. The codec uses explicit
  tags, big-endian integers, length-prefixed strings and collections, and UUID
  bytes. It preserves bundle claim input order and ordered choice alternatives.
- Decoding rebuilds domain values through validated constructors and reports
  structured `StorageError` values.
- `WalBackend` defines raw `append`, `flush`, and `sync` operations.
  `MemoryWal` retains bytes and operation counters for tests. `FileWal::create`
  exclusively creates a new path, while `FileWal::open` requires an existing path.
- `Durability` and `persist` apply `None`, `Flush`, or `Sync` policy to one owned
  immutable byte buffer.

## Learner-owned TODO

`storage::record` deliberately leaves record framing and reading as `todo!`. The
learner still owns these durable-format decisions:

- record length/header layout and version placement;
- sequence assignment semantics relative to engine publication;
- checksum algorithm, checksum bytes, and checksum coverage;
- clean EOF, partial-tail, and corrupt-record reader behavior.

Ignored guide tests name the required cases without selecting those designs.
`storage::recovery::recover` is only compile-clean control-flow scaffolding and
cannot run until the record reader exists. It does not replay into `Engine`.

No engine publication ordering has been integrated. Consequently, the presence of
raw file operations and a command codec must not be interpreted as a durability
guarantee: PromiseDB remains an in-memory state machine until record framing,
recovery, and engine/WAL ordering are designed and implemented together.
