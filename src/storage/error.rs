//! Storage and codec errors.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

use crate::domain::DomainError;

/// An error produced by command codecs, WAL backends, or recovery scaffolding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// An I/O operation failed.
    Io {
        /// Portable category reported by [`io::Error`].
        kind: io::ErrorKind,
        /// Human-readable detail retained from the original error.
        message: String,
    },
    /// The encoded value uses a format version this crate does not understand.
    UnsupportedVersion(u8),
    /// A byte tag is not valid for the named encoded value.
    InvalidTag {
        /// Encoded value whose tag was invalid.
        kind: &'static str,
        /// Invalid tag byte.
        tag: u8,
    },
    /// A length prefix cannot be represented or safely consumed.
    InvalidLength {
        /// Encoded field with the invalid length.
        field: &'static str,
        /// Invalid encoded or requested length.
        length: u64,
    },
    /// A length-prefixed string is not valid UTF-8.
    InvalidUtf8,
    /// A command payload ended before all declared fields were available.
    TruncatedPayload,
    /// A complete record failed structural or checksum validation.
    CorruptRecord(&'static str),
    /// A WAL ended after the start, but before the end, of a record.
    PartialTail,
    /// Decoded bytes violate a domain invariant.
    Domain(DomainError),
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { kind, message } => write!(formatter, "I/O error ({kind:?}): {message}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported storage format version {version}")
            }
            Self::InvalidTag { kind, tag } => write!(formatter, "invalid {kind} tag {tag}"),
            Self::InvalidLength { field, length } => {
                write!(formatter, "invalid length {length} for {field}")
            }
            Self::InvalidUtf8 => formatter.write_str("encoded string is not valid UTF-8"),
            Self::TruncatedPayload => formatter.write_str("encoded payload is truncated"),
            Self::CorruptRecord(reason) => write!(formatter, "corrupt WAL record: {reason}"),
            Self::PartialTail => formatter.write_str("WAL ends with a partial record"),
            Self::Domain(error) => {
                write!(formatter, "decoded value violates domain rules: {error}")
            }
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Domain(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        Self::Io {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
}

impl From<DomainError> for StorageError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}
