//! WAL scanning and effect-only engine recovery.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::Read;

use crate::domain::Timestamp;
use crate::engine::{Engine, InstallError};

use super::StorageError;
use super::record::{Record, RecordLimits, RecordReader, RecordSequence};
use super::transition_codec::decode_transition;

/// Public, storage-safe classification of an effect-install failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryInstallError {
    /// The audit command identity disagreed with the persisted idempotency identity.
    CommandIdentity,
    /// The audit command did not match the persisted canonical hash.
    CommandHash,
    /// A first-seen idempotency identity appeared more than once.
    DuplicateIdempotencyIdentity,
    /// Domain event or final sequences were inconsistent.
    Sequence,
    /// An authoritative or event entity identity was inconsistent.
    EntityIdentity,
    /// Restored authoritative values were mutually inconsistent.
    DomainInvariant,
    /// The runtime publication revision was exhausted.
    PublicationRevision,
    /// A derived index could not be rebuilt.
    Index(crate::domain::DomainError),
}

impl From<InstallError> for RecoveryInstallError {
    fn from(value: InstallError) -> Self {
        match value {
            InstallError::CommandIdentity => Self::CommandIdentity,
            InstallError::CommandHash => Self::CommandHash,
            InstallError::DuplicateIdempotencyIdentity => Self::DuplicateIdempotencyIdentity,
            InstallError::Sequence => Self::Sequence,
            InstallError::EntityIdentity => Self::EntityIdentity,
            InstallError::DomainInvariant => Self::DomainInvariant,
            InstallError::PublicationRevision => Self::PublicationRevision,
            InstallError::Index(error) => Self::Index(error),
        }
    }
}

/// Structured failure while scanning or installing a WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryError {
    /// Record framing, I/O, or transition decoding failed.
    Storage {
        /// Last complete, validated record boundary safe for later file repair.
        last_valid_offset: u64,
        /// Underlying structured storage failure.
        source: StorageError,
    },
    /// An emitted event timestamp did not match its enclosing WAL record.
    TimestampMismatch {
        /// Sequence of the enclosing WAL record.
        record_sequence: RecordSequence,
        /// Timestamp stored in the WAL header.
        record_timestamp: Timestamp,
        /// Timestamp stored in the transition event.
        event_timestamp: Timestamp,
        /// Last complete boundary before the rejected record.
        last_valid_offset: u64,
    },
    /// Decoded effects failed engine installation validation.
    Install {
        /// Sequence of the rejected WAL record, if failure occurred per-record.
        record_sequence: Option<RecordSequence>,
        /// Last complete boundary before the rejected record.
        last_valid_offset: u64,
        /// Stable install failure classification.
        source: RecoveryInstallError,
    },
}

impl RecoveryError {
    /// Returns the last complete record boundary preceding the failure.
    pub fn last_valid_offset(&self) -> u64 {
        match self {
            Self::Storage {
                last_valid_offset, ..
            }
            | Self::TimestampMismatch {
                last_valid_offset, ..
            }
            | Self::Install {
                last_valid_offset, ..
            } => *last_valid_offset,
        }
    }
}

impl Display for RecoveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage {
                last_valid_offset,
                source,
            } => write!(
                formatter,
                "WAL recovery failed after offset {last_valid_offset}: {source}"
            ),
            Self::TimestampMismatch {
                record_sequence,
                record_timestamp,
                event_timestamp,
                ..
            } => write!(
                formatter,
                "event timestamp {event_timestamp} differs from record {} timestamp {record_timestamp}",
                record_sequence.get()
            ),
            Self::Install {
                record_sequence,
                source,
                ..
            } => write!(
                formatter,
                "effect installation failed at record {:?}: {source:?}",
                record_sequence.map(RecordSequence::get)
            ),
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Successfully recovered engine state and WAL continuation metadata.
pub struct RecoveryOutcome {
    engine: Engine,
    next_record_sequence: Option<RecordSequence>,
    last_valid_offset: u64,
}

impl RecoveryOutcome {
    /// Returns the recovered engine for immutable inspection.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Returns the next WAL sequence, or `None` when the sequence space is exhausted.
    pub fn next_record_sequence(&self) -> Option<RecordSequence> {
        self.next_record_sequence
    }

    /// Returns the byte offset immediately following the final valid record.
    pub fn last_valid_offset(&self) -> u64 {
        self.last_valid_offset
    }

    pub(crate) fn into_parts(self) -> (Engine, Option<RecordSequence>, u64) {
        (
            self.engine,
            self.next_record_sequence,
            self.last_valid_offset,
        )
    }
}

/// Reads and validates all logical records in strict sequence order.
///
/// This compatibility scanner returns opaque records and starts at record one.
pub fn recover<R: Read>(reader: R, limits: RecordLimits) -> Result<Vec<Record>, StorageError> {
    let mut reader = RecordReader::new(reader, limits);
    let mut records = Vec::new();
    while let Some(record) = reader.read_next()? {
        records.push(record);
    }
    Ok(records)
}

/// Recovers a new engine from a complete WAL beginning at record one.
pub fn recover_engine<R: Read>(
    reader: R,
    limits: RecordLimits,
) -> Result<RecoveryOutcome, RecoveryError> {
    recover_engine_with_expected(reader, limits, RecordSequence::FIRST, Engine::new())
}

/// Recovers effects from an explicit sequence into supplied snapshot state.
///
/// This is the anchor API for future snapshots. The caller must position `reader` at
/// the matching byte boundary and supply the authoritative engine state preceding it.
pub fn recover_engine_with_expected<R: Read>(
    reader: R,
    limits: RecordLimits,
    expected_sequence: RecordSequence,
    mut engine: Engine,
) -> Result<RecoveryOutcome, RecoveryError> {
    let mut reader = RecordReader::with_expected_sequence(reader, limits, expected_sequence);
    loop {
        let record_start = reader.offset();
        let record = match reader.read_next() {
            Ok(Some(record)) => record,
            Ok(None) => break,
            Err(source) => {
                return Err(RecoveryError::Storage {
                    last_valid_offset: record_start,
                    source,
                });
            }
        };
        let transition =
            decode_transition(record.payload()).map_err(|source| RecoveryError::Storage {
                last_valid_offset: record_start,
                source,
            })?;
        for event in transition.events() {
            if event.timestamp() != record.timestamp() {
                return Err(RecoveryError::TimestampMismatch {
                    record_sequence: record.record_sequence(),
                    record_timestamp: record.timestamp(),
                    event_timestamp: event.timestamp(),
                    last_valid_offset: record_start,
                });
            }
        }
        engine
            .install_transition(transition)
            .map_err(|source| RecoveryError::Install {
                record_sequence: Some(record.record_sequence()),
                last_valid_offset: record_start,
                source: source.into(),
            })?;
    }

    let last_valid_offset = reader.offset();
    let next_record_sequence = reader.expected_sequence();
    engine
        .rebuild_slack_timelines()
        .map_err(|source| RecoveryError::Install {
            record_sequence: None,
            last_valid_offset,
            source: source.into(),
        })?;
    Ok(RecoveryOutcome {
        engine,
        next_record_sequence,
        last_valid_offset,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::command::{ClientId, Command, CommandOperation, IdempotencyKey};
    use crate::domain::{CapacityCurve, ResourcePoolId, Unit};
    use crate::storage::RecordCorruption;
    use crate::storage::record::encode;
    use crate::storage::transition_codec::encode_transition;

    fn bytes(sequence: u64, payload: &[u8]) -> Vec<u8> {
        encode(
            &Record::new(
                RecordSequence::new(sequence).unwrap(),
                sequence as i64,
                payload.to_vec(),
            ),
            RecordLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn returns_opaque_records_in_strict_order() {
        let mut wal = bytes(1, b"first");
        wal.extend_from_slice(&bytes(2, b"second"));
        let records = recover(Cursor::new(wal), RecordLimits::default()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload(), b"first");
        assert_eq!(records[1].payload(), b"second");
    }

    #[test]
    fn rejects_gaps_and_duplicates_at_their_record_offset() {
        for second_sequence in [1, 3] {
            let first = bytes(1, b"");
            let offset = first.len() as u64;
            let mut wal = first;
            wal.extend_from_slice(&bytes(second_sequence, b""));
            assert_eq!(
                recover(Cursor::new(wal), RecordLimits::default()),
                Err(StorageError::CorruptWalRecord {
                    offset,
                    reason: RecordCorruption::SequenceMismatch {
                        expected: 2,
                        actual: second_sequence,
                    },
                })
            );
        }
    }

    fn valid_transition_payload() -> Vec<u8> {
        let command = Command::new(
            ClientId::new("recovery-tests"),
            IdempotencyKey::new("create"),
            CommandOperation::CreateResourcePool {
                resource_pool_id: ResourcePoolId::from_bytes([1; 16]),
                display_name: "pool".into(),
                unit: Unit::new("units".into(), 1).unwrap(),
                capacity_curve: CapacityCurve::empty(),
            },
        );
        let engine = Engine::new();
        let prepared = engine.prepare_batch(vec![(command, 0)]).unwrap();
        encode_transition(prepared.durable_items()[0].transition()).unwrap()
    }

    #[test]
    fn partial_tail_reports_the_last_valid_boundary() {
        let payload = valid_transition_payload();
        let first = encode(
            &Record::new(RecordSequence::FIRST, 0, payload.clone()),
            RecordLimits::default(),
        )
        .unwrap();
        let valid_offset = first.len() as u64;
        let second = encode(
            &Record::new(RecordSequence::new(2).unwrap(), 0, payload),
            RecordLimits::default(),
        )
        .unwrap();
        let mut wal = first;
        wal.extend_from_slice(&second[..10]);
        let error = match recover_engine(Cursor::new(wal), RecordLimits::default()) {
            Ok(_) => panic!("partial WAL should fail recovery"),
            Err(error) => error,
        };
        assert_eq!(error.last_valid_offset(), valid_offset);
        assert!(matches!(
            error,
            RecoveryError::Storage {
                source: StorageError::PartialTail { .. },
                ..
            }
        ));
    }
}
