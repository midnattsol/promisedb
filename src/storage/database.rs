//! Durable engine coordination and synchronous group commit.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::Read;

use crate::command::Command;
use crate::domain::Timestamp;
use crate::engine::{Engine, PreparationError};
use crate::idempotency::CommandResponse;

use super::StorageError;
use super::backend::{Durability, WalBackend, persist};
use super::record::{RecordLimits, RecordSequence, encode_payload_into};
use super::recovery::{RecoveryError, RecoveryOutcome, recover_engine};
use super::transition::encode_transition_into;

/// One command paired with its authoritative state-machine timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimedCommand {
    command: Command,
    timestamp: Timestamp,
}

impl TimedCommand {
    /// Creates a timestamped command for durable application.
    pub fn new(command: Command, timestamp: Timestamp) -> Self {
        Self { command, timestamp }
    }

    /// Returns the command.
    pub fn command(&self) -> &Command {
        &self.command
    }

    /// Returns the authoritative timestamp.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn into_parts(self) -> (Command, Timestamp) {
        (self.command, self.timestamp)
    }
}

/// Configuration applied to a durable database coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseOptions {
    /// Durability operation applied once after each nonempty group append.
    pub durability: Durability,
    /// Per-record framing and recovery limits.
    pub record_limits: RecordLimits,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Sync,
            record_limits: RecordLimits::default(),
        }
    }
}

/// Storage-safe classification of engine preparation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabasePreparationError {
    /// The prepared base no longer matches published engine state.
    StaleRevision,
    /// The runtime publication revision is exhausted.
    RevisionOverflow,
}

impl From<PreparationError> for DatabasePreparationError {
    fn from(value: PreparationError) -> Self {
        match value {
            PreparationError::StaleRevision { .. } => Self::StaleRevision,
            PreparationError::RevisionOverflow => Self::RevisionOverflow,
        }
    }
}

/// Failure returned by durable database writes or construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    /// Encoding or framing failed before any backend I/O.
    Storage(StorageError),
    /// Engine preparation or publication preflight failed before I/O.
    Preparation(DatabasePreparationError),
    /// Recovery scanning or effect installation failed.
    Recovery(RecoveryError),
    /// Backend append, flush, or sync failed after write outcome became uncertain.
    Indeterminate(StorageError),
    /// A prior indeterminate write permanently disabled further writes.
    Poisoned,
    /// No record sequence remains for another durable item.
    SequenceExhausted,
    /// An internal response cardinality invariant was violated.
    CoordinatorInvariant,
}

impl Display for DatabaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "storage preparation failed: {error}"),
            Self::Preparation(error) => write!(formatter, "engine preparation failed: {error:?}"),
            Self::Recovery(error) => write!(formatter, "database recovery failed: {error}"),
            Self::Indeterminate(error) => {
                write!(formatter, "durable write outcome is indeterminate: {error}")
            }
            Self::Poisoned => formatter.write_str("database writes are poisoned"),
            Self::SequenceExhausted => formatter.write_str("WAL record sequence is exhausted"),
            Self::CoordinatorInvariant => {
                formatter.write_str("database response cardinality invariant failed")
            }
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) | Self::Indeterminate(error) => Some(error),
            Self::Recovery(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for DatabaseError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<RecoveryError> for DatabaseError {
    fn from(value: RecoveryError) -> Self {
        Self::Recovery(value)
    }
}

/// Engine plus append-only WAL coordinated by persist-before-publish ordering.
pub struct Database<B: WalBackend> {
    engine: Engine,
    backend: B,
    durability: Durability,
    record_limits: RecordLimits,
    next_record_sequence: Option<RecordSequence>,
    poisoned: bool,
}

impl<B: WalBackend> Database<B> {
    /// Creates an empty durable database over `backend`.
    pub fn new(backend: B, options: DatabaseOptions) -> Self {
        Self {
            engine: Engine::new(),
            backend,
            durability: options.durability,
            record_limits: options.record_limits,
            next_record_sequence: Some(RecordSequence::FIRST),
            poisoned: false,
        }
    }

    /// Creates a coordinator from recovered state and continuation metadata.
    pub fn from_recovered(outcome: RecoveryOutcome, backend: B, options: DatabaseOptions) -> Self {
        let (engine, next_record_sequence, _) = outcome.into_parts();
        Self {
            engine,
            backend,
            durability: options.durability,
            record_limits: options.record_limits,
            next_record_sequence,
            poisoned: false,
        }
    }

    /// Recovers a complete WAL beginning at record one and attaches `backend`.
    pub fn recover<R: Read>(
        reader: R,
        backend: B,
        options: DatabaseOptions,
    ) -> Result<Self, DatabaseError> {
        let outcome = recover_engine(reader, options.record_limits)?;
        Ok(Self::from_recovered(outcome, backend, options))
    }

    /// Returns immutable engine state for queries.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Returns the backend for immutable diagnostics and tests.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Returns whether an indeterminate I/O result has disabled writes.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub(crate) fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    pub(crate) fn engine_snapshot(&self) -> crate::engine::EngineSnapshot {
        self.engine.capture_snapshot()
    }

    /// Returns the next record sequence, or `None` after log exhaustion.
    pub fn next_record_sequence(&self) -> Option<RecordSequence> {
        self.next_record_sequence
    }

    /// Durably applies one command.
    pub fn apply(
        &mut self,
        command: Command,
        timestamp: Timestamp,
    ) -> Result<CommandResponse, DatabaseError> {
        let mut responses = self.apply_batch(vec![TimedCommand::new(command, timestamp)])?;
        responses.pop().ok_or(DatabaseError::CoordinatorInvariant)
    }

    /// Durably applies an ordered group with one append and one durability operation.
    pub fn apply_batch(
        &mut self,
        commands: Vec<TimedCommand>,
    ) -> Result<Vec<CommandResponse>, DatabaseError> {
        if self.poisoned {
            return Err(DatabaseError::Poisoned);
        }
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        let prepared = self
            .engine
            .prepare_batch(commands.into_iter().map(TimedCommand::into_parts).collect())
            .map_err(|error| DatabaseError::Preparation(error.into()))?;
        if prepared.durable_items().is_empty() {
            return Ok(prepared.into_responses());
        }
        self.engine
            .can_publish(&prepared)
            .map_err(|error| DatabaseError::Preparation(error.into()))?;

        let mut next_sequence = self.next_record_sequence;
        let mut assigned_sequences = Vec::with_capacity(prepared.durable_items().len());
        for _ in prepared.durable_items() {
            let sequence = next_sequence.ok_or(DatabaseError::SequenceExhausted)?;
            assigned_sequences.push(sequence);
            next_sequence = sequence.next();
        }

        let mut framed = Vec::new();
        for (item, sequence) in prepared.durable_items().iter().zip(assigned_sequences) {
            encode_payload_into(
                sequence,
                item.timestamp(),
                self.record_limits,
                &mut framed,
                |payload| encode_transition_into(item.transition(), payload.destination_mut()),
            )?;
        }

        if let Err(error) = persist(&mut self.backend, framed, self.durability) {
            self.poisoned = true;
            return Err(DatabaseError::Indeterminate(error));
        }

        let responses = self.engine.publish_batch(prepared);
        self.next_record_sequence = next_sequence;
        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};

    use super::*;
    use crate::command::{ClientId, CommandOperation, CommandResult, IdempotencyKey};
    use crate::domain::{
        Bundle, CapacityCurve, CapacitySegment, Claim, DomainError, Interval, PromiseId,
        ResourcePoolId, Unit, Version,
    };
    use crate::engine::HoldOutcome;
    use crate::storage::backend::MemoryWal;
    use crate::storage::record::{Record, encode};
    use crate::storage::recovery::{RecoveryError, recover_engine};
    use crate::storage::transition::encode_transition;

    fn pool(byte: u8) -> ResourcePoolId {
        ResourcePoolId::from_bytes([byte; 16])
    }

    fn promise(byte: u8) -> PromiseId {
        PromiseId::from_bytes([byte; 16])
    }

    fn curve(capacity: u64) -> CapacityCurve {
        CapacityCurve::from_sorted(vec![CapacitySegment::new(
            Interval::new(0, 1_000).unwrap(),
            capacity,
        )])
        .unwrap()
    }

    fn bundle(pool_id: ResourcePoolId, quantity: u64) -> Bundle {
        Bundle::new(vec![
            Claim::new(pool_id, Interval::new(10, 20).unwrap(), quantity).unwrap(),
        ])
        .unwrap()
    }

    fn command(key: &str, operation: CommandOperation) -> Command {
        Command::new(
            ClientId::new("database-tests"),
            IdempotencyKey::new(key),
            operation,
        )
    }

    fn create(key: &str, capacity: u64) -> Command {
        command(
            key,
            CommandOperation::CreateResourcePool {
                resource_pool_id: pool(1),
                display_name: "pool".into(),
                unit: Unit::new("units".into(), 1).unwrap(),
                capacity_curve: curve(capacity),
            },
        )
    }

    fn hold(key: &str, quantity: u64) -> Command {
        command(
            key,
            CommandOperation::Hold {
                promise_id: promise(1),
                bundle: bundle(pool(1), quantity),
                expires_at: 100,
            },
        )
    }

    fn options(durability: Durability) -> DatabaseOptions {
        DatabaseOptions {
            durability,
            record_limits: RecordLimits::default(),
        }
    }

    #[test]
    fn group_commit_is_sequential_and_uses_one_append_and_sync() {
        let mut database = Database::new(MemoryWal::new(), options(Durability::Sync));
        let responses = database
            .apply_batch(vec![
                TimedCommand::new(create("create", 10), 0),
                TimedCommand::new(hold("hold", 2), 1),
            ])
            .unwrap();

        assert_eq!(responses.len(), 2);
        assert!(matches!(
            responses[1],
            Ok(CommandResult::HoldCompleted(HoldOutcome::Held(id))) if id == promise(1)
        ));
        assert!(database.engine().promise(promise(1)).is_some());
        assert_eq!(database.backend().append_count(), 1);
        assert_eq!(database.backend().sync_count(), 1);
        assert_eq!(database.next_record_sequence().unwrap().get(), 3);
        assert_eq!(
            crate::storage::recover(
                Cursor::new(database.backend().bytes()),
                RecordLimits::default()
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn empty_and_retry_only_batches_do_no_io_but_first_seen_rejection_is_persisted() {
        let mut database = Database::new(MemoryWal::new(), options(Durability::Flush));
        assert!(database.apply_batch(Vec::new()).unwrap().is_empty());
        assert_eq!(database.backend().append_count(), 0);

        let missing = command(
            "missing",
            CommandOperation::Hold {
                promise_id: promise(2),
                bundle: bundle(pool(9), 1),
                expires_at: 100,
            },
        );
        assert_eq!(
            database.apply(missing.clone(), 1).unwrap(),
            Err(DomainError::ResourcePoolNotFound)
        );
        assert_eq!(database.backend().append_count(), 1);
        assert_eq!(database.backend().flush_count(), 1);

        let bytes = database.backend().bytes().len();
        let responses = database
            .apply_batch(vec![
                TimedCommand::new(missing, 999),
                TimedCommand::new(
                    command("missing", CommandOperation::ProcessExpirations),
                    999,
                ),
            ])
            .unwrap();
        assert_eq!(responses[0], Err(DomainError::ResourcePoolNotFound));
        assert_eq!(responses[1], Err(DomainError::IdempotencyConflict));
        assert_eq!(database.backend().append_count(), 1);
        assert_eq!(database.backend().bytes().len(), bytes);
    }

    #[derive(Debug, Clone, Copy)]
    enum FailAt {
        Append,
        Flush,
        Sync,
    }

    #[derive(Debug)]
    struct FaultWal {
        inner: MemoryWal,
        fail_at: FailAt,
    }

    impl FaultWal {
        fn error() -> StorageError {
            StorageError::from(io::Error::other("injected WAL failure"))
        }
    }

    impl WalBackend for FaultWal {
        fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
            self.inner.append(bytes)?;
            if matches!(self.fail_at, FailAt::Append) {
                return Err(Self::error());
            }
            Ok(())
        }

        fn flush(&mut self) -> Result<(), StorageError> {
            self.inner.flush()?;
            if matches!(self.fail_at, FailAt::Flush) {
                return Err(Self::error());
            }
            Ok(())
        }

        fn sync(&mut self) -> Result<(), StorageError> {
            self.inner.sync()?;
            if matches!(self.fail_at, FailAt::Sync) {
                return Err(Self::error());
            }
            Ok(())
        }
    }

    #[test]
    fn encoding_and_sequence_preflight_errors_do_not_poison() {
        let mut limited = Database::new(
            MemoryWal::new(),
            DatabaseOptions {
                durability: Durability::Sync,
                record_limits: RecordLimits::new(crate::storage::record::MIN_RECORD_LEN).unwrap(),
            },
        );
        assert!(matches!(
            limited.apply(create("too-large", 10), 0),
            Err(DatabaseError::Storage(StorageError::RecordTooLarge { .. }))
        ));
        assert!(!limited.is_poisoned());
        assert_eq!(limited.backend().append_count(), 0);
        assert!(limited.engine().resource_pool(pool(1)).is_none());

        let engine = Engine::new();
        let prepared = engine
            .prepare_batch(vec![(create("max-record", 10), 0)])
            .unwrap();
        let payload = encode_transition(prepared.durable_items()[0].transition()).unwrap();
        let record = encode(
            &Record::new(RecordSequence::new(u64::MAX).unwrap(), 0, payload),
            RecordLimits::default(),
        )
        .unwrap();
        let outcome = crate::storage::recover_engine_with_expected(
            Cursor::new(&record),
            RecordLimits::default(),
            RecordSequence::new(u64::MAX).unwrap(),
            Engine::new(),
        )
        .unwrap();
        assert_eq!(outcome.next_record_sequence(), None);
        let mut exhausted = Database::from_recovered(
            outcome,
            MemoryWal::from_bytes(record),
            options(Durability::Sync),
        );
        assert_eq!(
            exhausted.apply(
                command("after-max", CommandOperation::ProcessExpirations),
                1,
            ),
            Err(DatabaseError::SequenceExhausted)
        );
        assert!(!exhausted.is_poisoned());
        assert_eq!(exhausted.backend().append_count(), 0);
        assert_eq!(exhausted.engine().idempotency_record_count(), 1);
    }

    #[test]
    fn every_io_failure_poison_writes_without_publishing() {
        for (fail_at, durability) in [
            (FailAt::Append, Durability::None),
            (FailAt::Flush, Durability::Flush),
            (FailAt::Sync, Durability::Sync),
        ] {
            let backend = FaultWal {
                inner: MemoryWal::new(),
                fail_at,
            };
            let mut database = Database::new(backend, options(durability));
            assert!(matches!(
                database.apply(create("create", 10), 0),
                Err(DatabaseError::Indeterminate(_))
            ));
            assert!(database.engine().resource_pool(pool(1)).is_none());
            assert!(database.is_poisoned());
            assert_eq!(
                database.apply(create("later", 10), 0),
                Err(DatabaseError::Poisoned)
            );
        }
    }

    #[test]
    fn persisted_but_unpublished_bytes_recover_exactly_once() {
        let backend = FaultWal {
            inner: MemoryWal::new(),
            fail_at: FailAt::Sync,
        };
        let mut database = Database::new(backend, options(Durability::Sync));
        assert!(matches!(
            database.apply(create("create", 10), 0),
            Err(DatabaseError::Indeterminate(_))
        ));
        assert!(database.engine().resource_pool(pool(1)).is_none());

        let bytes = database.backend().inner.bytes().to_vec();
        let recovered = recover_engine(Cursor::new(bytes), RecordLimits::default()).unwrap();
        assert!(recovered.engine().resource_pool(pool(1)).is_some());
        assert_eq!(recovered.engine().idempotency_record_count(), 1);
    }

    #[test]
    fn restart_restores_events_idempotency_indexes_and_sequence_continuation() {
        let mut original = Database::new(MemoryWal::new(), options(Durability::Sync));
        let hold_command = hold("hold", 2);
        original
            .apply_batch(vec![
                TimedCommand::new(create("create", 10), 0),
                TimedCommand::new(hold_command.clone(), 1),
            ])
            .unwrap();
        let bytes = original.backend().bytes().to_vec();
        let source_slack = original.engine().slack_timeline(pool(1)).unwrap().clone();

        let outcome = recover_engine(Cursor::new(&bytes), RecordLimits::default()).unwrap();
        assert_eq!(outcome.next_record_sequence().unwrap().get(), 3);
        assert_eq!(outcome.engine().sequence(), original.engine().sequence());
        assert_eq!(
            outcome
                .engine()
                .watch_events(crate::domain::SequenceNumber::new(0)),
            original
                .engine()
                .watch_events(crate::domain::SequenceNumber::new(0))
        );
        assert_eq!(outcome.engine().idempotency_record_count(), 2);
        assert_eq!(
            outcome.engine().slack_timeline(pool(1)),
            Some(&source_slack)
        );

        let mut restarted = Database::from_recovered(
            outcome,
            MemoryWal::from_bytes(bytes),
            options(Durability::Sync),
        );
        assert!(matches!(
            restarted.apply(hold_command, 999).unwrap(),
            Ok(CommandResult::HoldCompleted(HoldOutcome::Held(_)))
        ));
        assert_eq!(restarted.backend().append_count(), 0);

        let release_response = restarted
            .apply(
                command(
                    "release",
                    CommandOperation::Release {
                        promise_id: promise(1),
                        expected_version: Version::new(1).unwrap(),
                    },
                ),
                2,
            )
            .unwrap();
        assert!(release_response.is_ok());
        assert_eq!(restarted.backend().append_count(), 1);
        assert_eq!(restarted.next_record_sequence().unwrap().get(), 4);
        let recovered = recover_engine(
            Cursor::new(restarted.backend().bytes()),
            RecordLimits::default(),
        )
        .unwrap();
        assert_eq!(recovered.next_record_sequence().unwrap().get(), 4);
    }

    #[test]
    fn recovery_rejects_event_timestamp_mismatch() {
        let engine = Engine::new();
        let prepared = engine
            .prepare_batch(vec![(create("create", 10), 1)])
            .unwrap();
        let payload = encode_transition(prepared.durable_items()[0].transition()).unwrap();
        let bytes = encode(
            &Record::new(RecordSequence::FIRST, 2, payload),
            RecordLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            recover_engine(Cursor::new(bytes), RecordLimits::default()),
            Err(RecoveryError::TimestampMismatch { .. })
        ));
    }
}
