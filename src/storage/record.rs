//! Learner-owned WAL record framing boundary.

use std::io::Read;
use std::num::NonZeroU64;
use std::ops::Range;

use crate::command::Command;
use crate::domain::Timestamp;

use super::StorageError;

/// Four-byte signature at the beginning of every PromiseDB WAL record.
pub const MAGIC: [u8; 4] = *b"PDBW";

/// Width in bytes of the little-endian `u32` record length field.
pub const LENGTH_WIDTH: usize = size_of::<u32>();

/// Current WAL record format version reserved for the learner-owned framing design.
pub const FORMAT_VERSION: u8 = 1;

/// Serialized width of the fixed WAL record header.
pub const HEADER_LEN: usize = 32;

/// Byte range containing [`MAGIC`].
pub const MAGIC_RANGE: Range<usize> = 0..4;

/// Byte range containing the little-endian `u32` total remaining record length.
pub const RECORD_LENGTH_RANGE: Range<usize> = 4..8;

/// Byte offset containing [`FORMAT_VERSION`].
pub const FORMAT_VERSION_OFFSET: usize = 8;

/// Byte range reserved for future use. Every byte must be zero.
pub const RESERVED_RANGE: Range<usize> = 9..16;

/// Required serialized contents of [`RESERVED_RANGE`].
pub const RESERVED_HEADER_BYTES: [u8; 7] = [0; 7];

/// Byte range containing the little-endian `u64` record sequence.
pub const RECORD_SEQUENCE_RANGE: Range<usize> = 16..24;

/// Byte range containing the little-endian `i64` timestamp.
pub const TIMESTAMP_RANGE: Range<usize> = 24..32;

/// A non-zero position in the logical WAL record order.
///
/// Zero represents an empty WAL and is therefore never assigned to a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordSequence(NonZeroU64);

impl RecordSequence {
    /// Sequence assigned to the first WAL record.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Creates a record sequence, returning `None` for zero.
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric representation.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the following record sequence, or `None` on overflow.
    pub const fn next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Logical content to be framed as one WAL record.
///
/// `record_sequence` orders durable WAL records. It is deliberately independent
/// from event sequences because one command can produce zero, one, or many domain
/// events after due expirations are processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    record_sequence: RecordSequence,
    timestamp: Timestamp,
    command: Command,
}

impl Record {
    /// Creates a logical record with its WAL ordering sequence.
    pub fn new(record_sequence: RecordSequence, timestamp: Timestamp, command: Command) -> Self {
        Self {
            record_sequence,
            timestamp,
            command,
        }
    }

    /// Returns this record's position in the logical WAL order.
    pub fn record_sequence(&self) -> RecordSequence {
        self.record_sequence
    }

    /// Returns the deterministic timestamp captured for the command.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the command payload.
    pub fn command(&self) -> &Command {
        &self.command
    }
}

/// Encodes one logical record using the learner-owned framing and checksum design.
///
/// The command payload codec is available separately, but checksum coverage and its
/// algorithm are intentionally not selected by this scaffold. The fixed 32-byte header
/// begins with [`MAGIC`], followed by a little-endian `u32` length, and encodes
/// [`Record::record_sequence`] explicitly. The length counts every byte after the
/// length field itself, including the remaining header, command, and checksum.
///
/// # Panics
///
/// Panics until the learner implements the record format.
pub fn encode(_record: &Record) -> Vec<u8> {
    todo!("learner: choose and implement the WAL record framing and checksum")
}

/// Reads one learner-owned WAL record.
///
/// The eventual contract must distinguish clean EOF (`Ok(None)`), an incomplete
/// trailing record ([`StorageError::PartialTail`]), and a complete invalid record
/// ([`StorageError::CorruptRecord`]).
///
/// # Errors
///
/// Returns framing, checksum, codec, or I/O errors after implementation.
///
/// # Panics
///
/// Panics until the learner implements the record reader.
pub fn read(_reader: &mut impl Read) -> Result<Option<Record>, StorageError> {
    todo!("learner: implement WAL record reading after choosing the record format")
}

#[cfg(test)]
mod guide_tests {
    use super::{
        FORMAT_VERSION_OFFSET, HEADER_LEN, LENGTH_WIDTH, MAGIC, MAGIC_RANGE, RECORD_LENGTH_RANGE,
        RECORD_SEQUENCE_RANGE, RESERVED_HEADER_BYTES, RESERVED_RANGE, RecordSequence,
        TIMESTAMP_RANGE,
    };

    #[test]
    fn record_length_is_a_four_byte_u32() {
        assert_eq!(LENGTH_WIDTH, 4);
    }

    #[test]
    fn magic_identifies_promisedb_wal_records() {
        assert_eq!(MAGIC, *b"PDBW");
    }

    #[test]
    fn record_sequences_start_at_one_and_reject_zero() {
        assert_eq!(RecordSequence::new(0), None);
        assert_eq!(RecordSequence::FIRST.get(), 1);
        assert_eq!(RecordSequence::FIRST.next().unwrap().get(), 2);
        assert_eq!(RecordSequence::new(u64::MAX).unwrap().next(), None);
    }

    #[test]
    fn serialized_record_header_layout_is_fixed() {
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(MAGIC_RANGE, 0..4);
        assert_eq!(RECORD_LENGTH_RANGE, 4..8);
        assert_eq!(FORMAT_VERSION_OFFSET, 8);
        assert_eq!(RESERVED_RANGE, 9..16);
        assert_eq!(RESERVED_HEADER_BYTES, [0; 7]);
        assert_eq!(RECORD_SEQUENCE_RANGE, 16..24);
        assert_eq!(TIMESTAMP_RANGE, 24..32);
    }

    #[test]
    #[ignore = "learner guide: record framing is intentionally unimplemented"]
    fn record_sequence_is_explicit_and_monotonic() {
        todo!("learner: assert that record_sequence is encoded and WAL records are monotonic")
    }

    #[test]
    #[ignore = "learner guide: checksum algorithm and coverage are intentionally undecided"]
    fn checksum_is_explicit_and_verified() {
        todo!("learner: assert checksum bytes, coverage, and corrupt-checksum rejection")
    }

    #[test]
    #[ignore = "learner guide: record reader is intentionally unimplemented"]
    fn clean_eof_is_not_a_partial_tail() {
        todo!("learner: assert an empty reader returns Ok(None)")
    }

    #[test]
    #[ignore = "learner guide: record reader is intentionally unimplemented"]
    fn partial_tail_is_distinct_from_corruption() {
        todo!("learner: assert truncation returns PartialTail and checksum failure CorruptRecord")
    }
}
