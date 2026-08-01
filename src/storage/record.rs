//! Learner-owned WAL record framing boundary.

use std::io::Read;

use crate::command::Command;
use crate::domain::Timestamp;

use super::StorageError;

/// Current WAL record format version reserved for the learner-owned framing design.
pub const FORMAT_VERSION: u8 = 1;

/// Logical content to be framed as one WAL record.
///
/// Sequence assignment is deliberately undecided until engine publication ordering
/// is designed. `sequence` therefore remains `None` in the boilerplate rather than
/// asserting an ordering contract prematurely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    sequence: Option<u64>,
    timestamp: Timestamp,
    command: Command,
}

impl Record {
    /// Creates an unsequenced logical record.
    pub fn new(timestamp: Timestamp, command: Command) -> Self {
        Self {
            sequence: None,
            timestamp,
            command,
        }
    }

    /// Returns the sequence when a future publication-order design assigns one.
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
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
/// The command payload codec is available separately, but record length fields,
/// sequence semantics, checksum coverage, and checksum algorithm are intentionally
/// not selected by this scaffold.
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

    #[test]
    #[ignore = "learner guide: record layout is intentionally undecided"]
    fn length_and_version_are_explicit() {
        todo!("learner: assert the chosen explicit record length and FORMAT_VERSION fields")
    }

    #[test]
    #[ignore = "learner guide: sequence publication semantics are intentionally undecided"]
    fn sequence_is_explicit_and_monotonic() {
        todo!("learner: assert the chosen sequence field and monotonicity contract")
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
