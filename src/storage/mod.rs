//! Storage primitives and WAL scaffolding.
//!
//! Command encoding and raw byte backends are implemented. Record framing,
//! checksums, recovery, and engine publication ordering are not complete.

mod backend;
mod codec;
mod error;
pub mod record;
pub mod recovery;

pub use backend::{Durability, FileWal, MemoryWal, WalBackend, persist};
pub use codec::{COMMAND_FORMAT_VERSION, decode_command, encode_command};
pub use error::StorageError;
