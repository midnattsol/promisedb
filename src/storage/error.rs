//! Storage, codec, and WAL framing errors.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;

use crate::domain::DomainError;

/// The structural reason a complete WAL record was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordCorruption {
    /// The four-byte record signature did not match `PDBW`.
    InvalidMagic([u8; 4]),
    /// Version 1 requires all flags to be zero.
    UnsupportedFlags(u8),
    /// The encoded fixed-header length was not 32 bytes.
    InvalidHeaderLength(u16),
    /// The total record length was below the minimum or was not 8-byte aligned.
    InvalidRecordLength(u32),
    /// The payload length was inconsistent with the total length and required padding.
    InvalidPayloadLength {
        /// Total encoded record length.
        record_len: u32,
        /// Encoded payload length.
        payload_len: u32,
    },
    /// A record encoded sequence zero, which is reserved for an empty WAL.
    InvalidSequence(u64),
    /// The encoded sequence was not the next expected WAL sequence.
    SequenceMismatch {
        /// Sequence required at this position.
        expected: u64,
        /// Sequence found in the record.
        actual: u64,
    },
    /// A record followed sequence `u64::MAX`, for which no successor exists.
    SequenceOverflow,
    /// Alignment padding contained a non-zero byte.
    NonZeroPadding {
        /// Zero-based byte index within the padding.
        index: u8,
        /// Invalid byte value.
        value: u8,
    },
    /// The stored BLAKE3-128 checksum did not match the framed bytes.
    ChecksumMismatch,
}

impl Display for RecordCorruption {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic(magic) => write!(formatter, "invalid magic {magic:?}"),
            Self::UnsupportedFlags(flags) => write!(formatter, "unsupported flags {flags:#04x}"),
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "invalid header length {length}")
            }
            Self::InvalidRecordLength(length) => {
                write!(formatter, "invalid total record length {length}")
            }
            Self::InvalidPayloadLength {
                record_len,
                payload_len,
            } => write!(
                formatter,
                "payload length {payload_len} is inconsistent with record length {record_len}"
            ),
            Self::InvalidSequence(sequence) => {
                write!(formatter, "invalid record sequence {sequence}")
            }
            Self::SequenceMismatch { expected, actual } => write!(
                formatter,
                "record sequence mismatch: expected {expected}, found {actual}"
            ),
            Self::SequenceOverflow => formatter.write_str("record sequence overflow"),
            Self::NonZeroPadding { index, value } => write!(
                formatter,
                "non-zero padding byte at index {index}: {value:#04x}"
            ),
            Self::ChecksumMismatch => formatter.write_str("checksum mismatch"),
        }
    }
}

/// An error produced by command codecs, WAL backends, or recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// An I/O operation failed.
    Io {
        /// Portable category reported by [`io::Error`].
        kind: io::ErrorKind,
        /// Human-readable detail retained from the original error.
        message: String,
    },
    /// A command payload uses a codec version this crate does not understand.
    ///
    /// This compatibility name remains for the command codec while the payload-to-record
    /// transition is pending. WAL record versions use [`Self::UnsupportedRecordVersion`].
    UnsupportedVersion(u8),
    /// A durable-transition payload uses an unsupported format version.
    UnsupportedTransitionVersion(u8),
    /// A WAL record uses a framing version this crate does not understand.
    UnsupportedRecordVersion {
        /// Byte offset at which the record starts.
        offset: u64,
        /// Unsupported framing version.
        version: u8,
    },
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
    /// An append cannot be represented in the active segment's `u64` physical length.
    SegmentLengthOverflow {
        /// Tracked physical length before the append.
        current: u64,
        /// Number of bytes requested by the append.
        append: usize,
    },
    /// A configured record limit is below 48, unaligned, or above the format ceiling.
    InvalidRecordLimit(u64),
    /// A record exceeds the configured total record-size limit.
    RecordTooLarge {
        /// Byte offset at which the record starts (zero while encoding a standalone record).
        offset: u64,
        /// Declared or requested total record length.
        length: u64,
        /// Configured maximum total record length.
        max: u32,
    },
    /// A length-prefixed string is not valid UTF-8.
    InvalidUtf8,
    /// A command payload ended before all declared fields were available.
    TruncatedPayload,
    /// A complete command payload failed codec validation.
    ///
    /// This compatibility variant belongs to the command codec. Complete WAL failures use
    /// [`Self::CorruptWalRecord`].
    CorruptRecord(&'static str),
    /// A complete WAL record failed structural, sequence, padding, or checksum validation.
    CorruptWalRecord {
        /// Byte offset at which the rejected record starts.
        offset: u64,
        /// Structured rejection reason.
        reason: RecordCorruption,
    },
    /// A WAL ended after a record began but before all declared bytes were available.
    PartialTail {
        /// Byte offset at which the partial record starts.
        offset: u64,
        /// Number of bytes required for the current framing stage.
        expected: u64,
        /// Number of bytes available from the start of this record.
        actual: u64,
    },
    /// Decoded bytes violate a domain invariant.
    Domain(DomainError),
}

impl Display for StorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { kind, message } => write!(formatter, "I/O error ({kind:?}): {message}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported command format version {version}")
            }
            Self::UnsupportedTransitionVersion(version) => {
                write!(formatter, "unsupported transition format version {version}")
            }
            Self::UnsupportedRecordVersion { offset, version } => write!(
                formatter,
                "unsupported WAL record format version {version} at offset {offset}"
            ),
            Self::InvalidTag { kind, tag } => write!(formatter, "invalid {kind} tag {tag}"),
            Self::InvalidLength { field, length } => {
                write!(formatter, "invalid length {length} for {field}")
            }
            Self::SegmentLengthOverflow { current, append } => write!(
                formatter,
                "segment length overflow: cannot append {append} bytes to physical length {current}"
            ),
            Self::InvalidRecordLimit(limit) => {
                write!(formatter, "invalid maximum WAL record length {limit}")
            }
            Self::RecordTooLarge {
                offset,
                length,
                max,
            } => write!(
                formatter,
                "WAL record at offset {offset} has length {length}, exceeding limit {max}"
            ),
            Self::InvalidUtf8 => formatter.write_str("encoded string is not valid UTF-8"),
            Self::TruncatedPayload => formatter.write_str("encoded payload is truncated"),
            Self::CorruptRecord(reason) => write!(formatter, "corrupt command payload: {reason}"),
            Self::CorruptWalRecord { offset, reason } => {
                write!(formatter, "corrupt WAL record at offset {offset}: {reason}")
            }
            Self::PartialTail {
                offset,
                expected,
                actual,
            } => write!(
                formatter,
                "WAL record at offset {offset} is partial: expected {expected} bytes, found {actual}"
            ),
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
