//! Durable database coordination, transition codecs, recovery, and WAL framing.
//!
//! [`Database`] owns mutable engine/backend access and enforces persist-before-publish
//! ordering. The generic record layer remains payload-agnostic.

mod backend;
mod codec;
mod database;
mod error;
mod file;
pub mod record;
pub mod recovery;
pub(crate) mod transition;

pub use backend::{Durability, FileWal, MemoryWal, WalBackend, persist};
pub use codec::{COMMAND_FORMAT_VERSION, decode_command, encode_command};
pub use database::{
    Database, DatabaseError, DatabaseOptions, DatabasePreparationError, TimedCommand,
};
pub use error::{RecordCorruption, StorageError};
pub use file::{
    DEFAULT_SEGMENT_TARGET, FileDatabase, FileDatabaseError, FileDatabaseOptions,
    MIN_SEGMENT_TARGET, ManifestError, SEGMENT_HEADER_LEN, SegmentHeaderError, SegmentedWal,
};
pub use record::{
    Record, RecordLimits, RecordPayloadWriter, RecordReader, RecordSequence,
    encode as encode_record, encode_into as encode_record_into, encode_payload_into,
};
pub use recovery::{
    EngineRecovery, RecoveryError, RecoveryInstallError, RecoveryOutcome, recover, recover_engine,
    recover_engine_with_expected,
};
