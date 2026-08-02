//! Canonical, checksummed engine snapshots.

mod format;
mod model;
mod reader;
mod writer;

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;

use uuid::Uuid;

use super::StorageError;

pub(crate) use format::{DIRECTORY_NAME, EXTENSION, TEMP_NAME};
pub use model::SnapshotLimits;
pub(crate) use model::{DecodedSnapshot, SnapshotFile};
pub(crate) use reader::decode;
pub(crate) use writer::encode;

/// Snapshot framing, compatibility, budget, or payload validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    /// The file is shorter than its declared framing requires.
    Truncated {
        /// Complete length required by framing.
        expected: u64,
        /// Physical bytes available.
        actual: u64,
    },
    /// The snapshot magic is invalid.
    InvalidMagic([u8; 4]),
    /// The snapshot format version is unsupported.
    UnsupportedVersion(u8),
    /// Header flags are unsupported.
    UnsupportedFlags(u8),
    /// A fixed framing field is non-canonical.
    MalformedHeader(&'static str),
    /// Reserved header bytes are non-zero.
    NonZeroReserved,
    /// The state-machine semantics version is unsupported.
    UnsupportedSemanticsVersion(u32),
    /// The database identity differs from the manifest.
    DatabaseUuidMismatch {
        /// Durable database identity from the manifest.
        expected: Uuid,
        /// Identity encoded by the snapshot.
        actual: Uuid,
    },
    /// The filename watermark differs from the header.
    FilenameWatermarkMismatch {
        /// Watermark encoded by the canonical filename.
        filename: u64,
        /// Watermark encoded by the fixed header.
        header: u64,
    },
    /// The complete BLAKE3 checksum does not match.
    ChecksumMismatch,
    /// Configured limits are internally invalid.
    InvalidLimits {
        /// Invalid limit field.
        field: &'static str,
        /// Configured value.
        value: u64,
        /// Smallest accepted value.
        minimum: u64,
    },
    /// A configured budget was exceeded before allocation.
    Limit {
        /// Budgeted field or collection.
        field: &'static str,
        /// Declared or requested value.
        value: u64,
        /// Configured or platform maximum.
        maximum: u64,
    },
    /// Memory reservation failed before writing or decoding values.
    Allocation {
        /// Buffer or collection being reserved.
        field: &'static str,
        /// Number of bytes or entries requested.
        requested: u64,
    },
    /// The canonical payload is malformed.
    MalformedPayload(StorageError),
    /// Decoded authoritative state is internally inconsistent.
    InvalidEngineState,
    /// A snapshot directory entry has a non-canonical name.
    InvalidFilename(PathBuf),
}

impl Display for SnapshotError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { expected, actual } => write!(
                f,
                "snapshot truncated: expected {expected} bytes, found {actual}"
            ),
            Self::InvalidMagic(value) => write!(f, "invalid snapshot magic {value:?}"),
            Self::UnsupportedVersion(value) => write!(f, "unsupported snapshot version {value}"),
            Self::UnsupportedFlags(value) => write!(f, "unsupported snapshot flags {value:#04x}"),
            Self::MalformedHeader(field) => write!(f, "malformed snapshot header field {field}"),
            Self::NonZeroReserved => f.write_str("snapshot reserved bytes are non-zero"),
            Self::UnsupportedSemanticsVersion(value) => {
                write!(f, "unsupported state-machine semantics version {value}")
            }
            Self::DatabaseUuidMismatch { expected, actual } => write!(
                f,
                "snapshot database UUID {actual} differs from manifest UUID {expected}"
            ),
            Self::FilenameWatermarkMismatch { filename, header } => write!(
                f,
                "snapshot filename watermark {filename} differs from header watermark {header}"
            ),
            Self::ChecksumMismatch => f.write_str("snapshot checksum mismatch"),
            Self::InvalidLimits {
                field,
                value,
                minimum,
            } => write!(
                f,
                "snapshot limit {field} is {value}, below required minimum {minimum}"
            ),
            Self::Limit {
                field,
                value,
                maximum,
            } => write!(f, "snapshot {field} {value} exceeds limit {maximum}"),
            Self::Allocation { field, requested } => write!(
                f,
                "cannot reserve {requested} bytes or entries for snapshot {field}"
            ),
            Self::MalformedPayload(error) => {
                write!(f, "malformed canonical snapshot payload: {error}")
            }
            Self::InvalidEngineState => f.write_str("snapshot authoritative state is inconsistent"),
            Self::InvalidFilename(path) => {
                write!(f, "non-canonical snapshot filename: {}", path.display())
            }
        }
    }
}

impl Error for SnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MalformedPayload(error) => Some(error),
            _ => None,
        }
    }
}
