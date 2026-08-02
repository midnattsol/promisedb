//! Storage primitives, command payload codecs, and generic WAL framing.
//!
//! Record payloads are deliberately opaque. Command codec integration and engine
//! publication ordering remain separate work.

mod backend;
mod codec;
mod error;
pub mod record;
pub mod recovery;

pub use backend::{Durability, FileWal, MemoryWal, WalBackend, persist};
pub use codec::{COMMAND_FORMAT_VERSION, decode_command, encode_command};
pub use error::{RecordCorruption, StorageError};
pub use record::{Record, RecordLimits, RecordReader, RecordSequence, encode as encode_record};
pub use recovery::recover;
