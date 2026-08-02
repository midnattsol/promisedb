//! Locked database directories, manifests, and segmented file-backed WAL storage.

mod manifest;
mod segment;
mod snapshot_store;

use manifest::*;
use segment::*;
use snapshot_store::*;

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::backend::{Durability, WalBackend};
use super::database::{Database, DatabaseOptions};
use super::record::{
    HEADER_LEN as RECORD_HEADER_LEN, MIN_RECORD_LEN, RecordLimits, RecordReader, RecordSequence,
};
use super::recovery::{EngineRecovery, RecoveryError};
use super::snapshot::{self, SnapshotFile};
use super::{STATE_MACHINE_SEMANTICS_VERSION, SnapshotError, SnapshotLimits, StorageError};

const LOCK_NAME: &str = "LOCK";
const MANIFEST_NAME: &str = "MANIFEST";
const MANIFEST_TEMP_NAME: &str = "MANIFEST.tmp";
const WAL_DIRECTORY_NAME: &str = "wal";
const MANIFEST_MAGIC: [u8; 4] = *b"PDBM";
const MANIFEST_VERSION: u8 = 2;
const MANIFEST_LEN: usize = 96;
const SEGMENT_MAGIC: [u8; 4] = *b"PDBS";
const SEGMENT_VERSION: u8 = 1;
/// Serialized width of a segment header.
pub const SEGMENT_HEADER_LEN: u64 = 64;
/// Default target size for a WAL segment (256 MiB).
pub const DEFAULT_SEGMENT_TARGET: u64 = 256 * 1024 * 1024;
/// Smallest accepted segment target: one header plus one minimum-sized record.
pub const MIN_SEGMENT_TARGET: u64 = SEGMENT_HEADER_LEN + MIN_RECORD_LEN as u64;

/// Configuration for a locked file-backed database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileDatabaseOptions {
    /// Durability operation applied after each nonempty group append.
    pub durability: Durability,
    /// Persisted maximum record size. Opening requires an exact manifest match.
    pub record_limits: RecordLimits,
    /// Operational segment rotation target. This value is intentionally not persisted.
    pub segment_target: u64,
    /// Persisted snapshot decoding and allocation limits.
    pub snapshot_limits: SnapshotLimits,
}

impl Default for FileDatabaseOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Sync,
            record_limits: RecordLimits::default(),
            segment_target: DEFAULT_SEGMENT_TARGET,
            snapshot_limits: SnapshotLimits::default(),
        }
    }
}

impl FileDatabaseOptions {
    fn validate(self) -> Result<Self, FileDatabaseError> {
        self.snapshot_limits
            .validate()
            .map_err(|_| FileDatabaseError::Manifest(ManifestError::InvalidSnapshotLimits))?;
        if self.segment_target < MIN_SEGMENT_TARGET {
            return Err(FileDatabaseError::InvalidSegmentTarget {
                target: self.segment_target,
                minimum: MIN_SEGMENT_TARGET,
            });
        }
        Ok(self)
    }

    fn database_options(self) -> DatabaseOptions {
        DatabaseOptions {
            durability: self.durability,
            record_limits: self.record_limits,
        }
    }
}

/// Manifest validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestError {
    /// The manifest length is not the canonical fixed width.
    InvalidLength(u64),
    /// The manifest magic is not `PDBM`.
    InvalidMagic([u8; 4]),
    /// The manifest version is unsupported.
    UnsupportedVersion(u8),
    /// The encoded header length is not 64.
    InvalidHeaderLength(u16),
    /// Reserved bytes are non-zero.
    NonZeroReserved,
    /// The database UUID is nil or is not a version-4 UUID.
    InvalidDatabaseUuid,
    /// The state-machine semantics version is unsupported.
    UnsupportedSemanticsVersion(u32),
    /// The manifest checksum does not match its canonical prefix.
    ChecksumMismatch,
    /// The persisted record limit is invalid.
    InvalidRecordLimit(u32),
    /// Persisted snapshot limits are invalid.
    InvalidSnapshotLimits,
}

impl Display for ManifestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength(length) => write!(formatter, "invalid manifest length {length}"),
            Self::InvalidMagic(magic) => write!(formatter, "invalid manifest magic {magic:?}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported manifest version {version}")
            }
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "invalid manifest header length {length}")
            }
            Self::NonZeroReserved => formatter.write_str("manifest reserved bytes are non-zero"),
            Self::InvalidDatabaseUuid => formatter.write_str("invalid manifest database UUID"),
            Self::UnsupportedSemanticsVersion(version) => {
                write!(
                    formatter,
                    "unsupported state-machine semantics version {version}"
                )
            }
            Self::ChecksumMismatch => formatter.write_str("manifest checksum mismatch"),
            Self::InvalidRecordLimit(limit) => {
                write!(formatter, "invalid persisted record limit {limit}")
            }
            Self::InvalidSnapshotLimits => formatter.write_str("invalid persisted snapshot limits"),
        }
    }
}

impl Error for ManifestError {}

/// Segment-header validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentHeaderError {
    /// The file is shorter than the fixed segment header.
    Truncated {
        /// Required fixed segment-header length.
        expected: u64,
        /// Physical bytes available in the segment file.
        actual: u64,
    },
    /// The segment magic is not `PDBS`.
    InvalidMagic([u8; 4]),
    /// The segment-header version is unsupported.
    UnsupportedVersion(u8),
    /// Version 1 flags are non-zero.
    UnsupportedFlags(u8),
    /// The encoded header length is not 64.
    InvalidHeaderLength(u16),
    /// The header database UUID differs from the manifest.
    DatabaseUuidMismatch {
        /// Durable database identity stored in the manifest.
        expected: Uuid,
        /// Database identity found in the segment header.
        actual: Uuid,
    },
    /// The first record sequence is zero.
    InvalidFirstSequence(u64),
    /// Reserved bytes are non-zero.
    NonZeroReserved,
    /// The segment-header checksum does not match.
    ChecksumMismatch,
}

impl Display for SegmentHeaderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { expected, actual } => write!(
                formatter,
                "partial segment header: expected {expected} bytes, found {actual}"
            ),
            Self::InvalidMagic(magic) => write!(formatter, "invalid segment magic {magic:?}"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported segment version {version}")
            }
            Self::UnsupportedFlags(flags) => {
                write!(formatter, "unsupported segment flags {flags:#04x}")
            }
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "invalid segment header length {length}")
            }
            Self::DatabaseUuidMismatch { expected, actual } => write!(
                formatter,
                "segment database UUID {actual} differs from manifest UUID {expected}"
            ),
            Self::InvalidFirstSequence(sequence) => {
                write!(formatter, "invalid segment first sequence {sequence}")
            }
            Self::NonZeroReserved => formatter.write_str("segment reserved bytes are non-zero"),
            Self::ChecksumMismatch => formatter.write_str("segment header checksum mismatch"),
        }
    }
}

impl Error for SegmentHeaderError {}

/// Failure while creating or opening a locked file database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileDatabaseError {
    /// The database directory already exists during creation.
    AlreadyExists(PathBuf),
    /// The database directory is already locked by another open coordinator.
    AlreadyOpen(PathBuf),
    /// Acquiring the stable lock failed for a reason other than contention.
    Lock {
        /// Stable lock-file path.
        path: PathBuf,
        /// Portable I/O error category.
        kind: io::ErrorKind,
        /// Original lock error detail.
        message: String,
    },
    /// A path-specific filesystem operation failed.
    Io {
        /// Filesystem path involved in the failed operation.
        path: PathBuf,
        /// Operation attempted on the path.
        operation: &'static str,
        /// Portable I/O error category.
        kind: io::ErrorKind,
        /// Original I/O error detail.
        message: String,
    },
    /// The manifest is structurally invalid.
    Manifest(ManifestError),
    /// The requested record limit differs from the durable manifest value.
    RecordLimitsMismatch {
        /// Maximum record length stored in the manifest.
        persisted: u32,
        /// Maximum record length requested by the opener.
        requested: u32,
    },
    /// Requested snapshot limits differ from the durable manifest values.
    SnapshotLimitsMismatch {
        /// Limits stored in the manifest.
        persisted: SnapshotLimits,
        /// Limits requested by the opener.
        requested: SnapshotLimits,
    },
    /// The operational segment target is too small.
    InvalidSegmentTarget {
        /// Requested operational segment target.
        target: u64,
        /// Smallest accepted segment target.
        minimum: u64,
    },
    /// A WAL directory entry is not a canonical segment or recognized temp file.
    InvalidSegmentName(PathBuf),
    /// No WAL segments exist.
    MissingSegments(PathBuf),
    /// A segment header is invalid.
    SegmentHeader {
        /// Segment containing the invalid header.
        path: PathBuf,
        /// Structured header validation failure.
        source: SegmentHeaderError,
    },
    /// A segment filename and its header disagree.
    SegmentNameSequence {
        /// Segment whose canonical name and header disagree.
        path: PathBuf,
        /// First sequence encoded by the filename.
        filename_sequence: u64,
        /// First sequence encoded by the segment header.
        header_sequence: u64,
    },
    /// A segment does not begin at the next expected global record sequence.
    SegmentSequence {
        /// Segment beginning at the unexpected sequence.
        path: PathBuf,
        /// Next global sequence required by preceding segments.
        expected: Option<u64>,
        /// First sequence declared by this segment.
        actual: u64,
    },
    /// Snapshot selection, framing, or state validation failed.
    Snapshot {
        /// Snapshot path being opened.
        path: PathBuf,
        /// Structured snapshot failure.
        source: SnapshotError,
    },
    /// A snapshot was installed, but obsolete-file cleanup did not complete.
    SnapshotCommittedCleanup {
        /// Installed canonical snapshot path.
        path: PathBuf,
        /// Installed WAL watermark.
        watermark: u64,
        /// Cleanup operation that failed.
        operation: &'static str,
        /// Portable I/O error category.
        kind: io::ErrorKind,
        /// Original I/O error detail.
        message: String,
    },
    /// Snapshot creation is forbidden after an indeterminate write.
    SnapshotPoisoned,
    /// Record decoding or effect installation failed in a segment.
    SegmentRecovery {
        /// Segment where recovery stopped.
        path: PathBuf,
        /// Physical file offset of the failing record or boundary.
        physical_offset: u64,
        /// Whether a later segment makes this segment immutable.
        sealed: bool,
        /// Structured record or effect recovery failure.
        source: RecoveryError,
    },
}

impl Display for FileDatabaseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists(path) => {
                write!(formatter, "database already exists: {}", path.display())
            }
            Self::AlreadyOpen(path) => {
                write!(formatter, "database is already open: {}", path.display())
            }
            Self::Lock { path, message, .. } => {
                write!(formatter, "cannot lock {}: {message}", path.display())
            }
            Self::Io {
                path,
                operation,
                message,
                ..
            } => {
                write!(
                    formatter,
                    "cannot {operation} {}: {message}",
                    path.display()
                )
            }
            Self::Manifest(source) => write!(formatter, "invalid database manifest: {source}"),
            Self::RecordLimitsMismatch {
                persisted,
                requested,
            } => write!(
                formatter,
                "record limit mismatch: manifest requires {persisted}, requested {requested}"
            ),
            Self::SnapshotLimitsMismatch {
                persisted,
                requested,
            } => write!(
                formatter,
                "snapshot limits mismatch: manifest requires {persisted:?}, requested {requested:?}"
            ),
            Self::InvalidSegmentTarget { target, minimum } => write!(
                formatter,
                "segment target {target} is below minimum {minimum}"
            ),
            Self::InvalidSegmentName(path) => {
                write!(
                    formatter,
                    "non-canonical WAL segment name: {}",
                    path.display()
                )
            }
            Self::MissingSegments(path) => {
                write!(
                    formatter,
                    "WAL directory has no segments: {}",
                    path.display()
                )
            }
            Self::SegmentHeader { path, source } => {
                write!(
                    formatter,
                    "invalid segment header in {}: {source}",
                    path.display()
                )
            }
            Self::SegmentNameSequence {
                path,
                filename_sequence,
                header_sequence,
            } => write!(
                formatter,
                "segment {} names sequence {filename_sequence} but header names {header_sequence}",
                path.display()
            ),
            Self::SegmentSequence {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "segment {} begins at {actual}, expected {expected:?}",
                path.display()
            ),
            Self::Snapshot { path, source } => {
                write!(formatter, "invalid snapshot {}: {source}", path.display())
            }
            Self::SnapshotCommittedCleanup {
                path,
                watermark,
                operation,
                message,
                ..
            } => write!(
                formatter,
                "snapshot {} at watermark {watermark} was committed, but {operation} failed: {message}",
                path.display()
            ),
            Self::SnapshotPoisoned => formatter
                .write_str("snapshot creation is disabled because database writes are poisoned"),
            Self::SegmentRecovery {
                path,
                physical_offset,
                sealed,
                source,
            } => write!(
                formatter,
                "recovery failed in {} at physical offset {physical_offset} (sealed={sealed}): {source}",
                path.display()
            ),
        }
    }
}

impl Error for FileDatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Manifest(source) => Some(source),
            Self::Snapshot { source, .. } => Some(source),
            Self::SegmentHeader { source, .. } => Some(source),
            Self::SegmentRecovery { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Manifest {
    database_uuid: Uuid,
    record_limits: RecordLimits,
    snapshot_limits: SnapshotLimits,
}

/// A rotating synchronous WAL backend that owns the database's exclusive lock.
#[derive(Debug)]
pub struct SegmentedWal {
    database_uuid: Uuid,
    wal_directory: PathBuf,
    active_path: PathBuf,
    active_file: File,
    active_len: u64,
    active_has_records: bool,
    segment_target: u64,
    record_limits: RecordLimits,
    expected_sequence: Option<RecordSequence>,
    snapshot_limits: SnapshotLimits,
    _lock: File,
}

impl SegmentedWal {
    /// Returns the durable identity shared by the manifest and every segment.
    pub fn database_uuid(&self) -> Uuid {
        self.database_uuid
    }

    /// Returns the active segment path.
    pub fn active_segment_path(&self) -> &Path {
        &self.active_path
    }

    /// Returns the configured operational rotation target.
    pub fn segment_target(&self) -> u64 {
        self.segment_target
    }

    fn inspect_batch(
        &self,
        bytes: &[u8],
    ) -> Result<(RecordSequence, Option<RecordSequence>), StorageError> {
        let expected = self
            .expected_sequence
            .ok_or(StorageError::CorruptWalRecord {
                offset: 0,
                reason: super::RecordCorruption::SequenceOverflow,
            })?;
        let mut reader = RecordReader::with_expected_sequence(bytes, self.record_limits, expected);
        let first = reader.read_next()?.ok_or(StorageError::InvalidLength {
            field: "WAL append batch",
            length: 0,
        })?;
        let first_sequence = first.record_sequence();
        while reader.read_next()?.is_some() {}
        Ok((first_sequence, reader.expected_sequence()))
    }

    fn rotate(&mut self, first_sequence: RecordSequence) -> Result<(), StorageError> {
        let path = create_segment_atomic(&self.wal_directory, self.database_uuid, first_sequence)
            .map_err(|error| StorageError::Io {
            kind: error.kind(),
            message: format!(
                "create segment {}: {error}",
                segment_path(&self.wal_directory, first_sequence).display()
            ),
        })?;
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        self.active_path = path;
        self.active_file = file;
        self.active_len = SEGMENT_HEADER_LEN;
        self.active_has_records = false;
        Ok(())
    }
}

impl WalBackend for SegmentedWal {
    fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        let (first_sequence, next_sequence) = self.inspect_batch(bytes)?;
        let append_len =
            u64::try_from(bytes.len()).map_err(|_| StorageError::SegmentLengthOverflow {
                current: self.active_len,
                append: bytes.len(),
            })?;
        let projected =
            self.active_len
                .checked_add(append_len)
                .ok_or(StorageError::SegmentLengthOverflow {
                    current: self.active_len,
                    append: bytes.len(),
                })?;
        let rotate = self.active_has_records && projected > self.segment_target;
        let resulting_len = if rotate {
            SEGMENT_HEADER_LEN.checked_add(append_len).ok_or(
                StorageError::SegmentLengthOverflow {
                    current: SEGMENT_HEADER_LEN,
                    append: bytes.len(),
                },
            )?
        } else {
            projected
        };

        if rotate {
            self.rotate(first_sequence)?;
        }
        self.active_file.write_all(bytes)?;
        self.active_len = resulting_len;
        self.active_has_records = true;
        self.expected_sequence = next_sequence;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.active_file.flush().map_err(StorageError::from)
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.active_file.sync_all().map_err(StorageError::from)
    }
}

/// Specialized locked production file database.
pub type FileDatabase = Database<SegmentedWal>;

/// Successful snapshot installation and compaction statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotOutcome {
    /// Installed canonical snapshot path.
    pub path: PathBuf,
    /// Last WAL record represented by the snapshot.
    pub watermark: u64,
    /// Number of fully covered WAL segments removed.
    pub segments_removed: usize,
    /// Number of older snapshots removed.
    pub older_snapshots_removed: usize,
    /// Complete snapshot file size.
    pub bytes: u64,
}

impl Database<SegmentedWal> {
    /// Atomically creates a database directory, manifest, and empty sequence-one segment.
    pub fn create(
        path: impl AsRef<Path>,
        options: FileDatabaseOptions,
    ) -> Result<Self, FileDatabaseError> {
        let options = options.validate()?;
        let root = path.as_ref().to_path_buf();
        match fs::create_dir(&root) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(FileDatabaseError::AlreadyExists(root));
            }
            Err(error) => return Err(path_io(&root, "create database directory", error)),
        }
        let lock = acquire_lock(&root)?;
        let wal_directory = root.join(WAL_DIRECTORY_NAME);
        fs::create_dir(&wal_directory)
            .map_err(|error| path_io(&wal_directory, "create WAL directory", error))?;
        let snapshot_directory = root.join(snapshot::DIRECTORY_NAME);
        fs::create_dir(&snapshot_directory)
            .map_err(|error| path_io(&snapshot_directory, "create snapshot directory", error))?;
        sync_directory(&root)?;

        let manifest = Manifest {
            database_uuid: Uuid::new_v4(),
            record_limits: options.record_limits,
            snapshot_limits: options.snapshot_limits,
        };
        write_manifest_atomic(&root, manifest)?;
        let active_path = create_segment_atomic(
            &wal_directory,
            manifest.database_uuid,
            RecordSequence::FIRST,
        )
        .map_err(|error| path_io(&wal_directory, "create initial WAL segment", error))?;
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_path)
            .map_err(|error| path_io(&active_path, "open active WAL segment", error))?;
        let backend = SegmentedWal {
            database_uuid: manifest.database_uuid,
            wal_directory,
            active_path,
            active_file,
            active_len: SEGMENT_HEADER_LEN,
            active_has_records: false,
            segment_target: options.segment_target,
            record_limits: options.record_limits,
            expected_sequence: Some(RecordSequence::FIRST),
            snapshot_limits: options.snapshot_limits,
            _lock: lock,
        };
        Ok(Self::new(backend, options.database_options()))
    }

    /// Opens, validates, streams, and if necessary repairs a locked database directory.
    pub fn open(
        path: impl AsRef<Path>,
        options: FileDatabaseOptions,
    ) -> Result<Self, FileDatabaseError> {
        let options = options.validate()?;
        let root = path.as_ref().to_path_buf();
        let lock = acquire_lock(&root)?;
        let manifest = read_manifest(&root)?;
        if manifest.record_limits != options.record_limits {
            return Err(FileDatabaseError::RecordLimitsMismatch {
                persisted: manifest.record_limits.max_record_len(),
                requested: options.record_limits.max_record_len(),
            });
        }
        if manifest.snapshot_limits != options.snapshot_limits {
            return Err(FileDatabaseError::SnapshotLimitsMismatch {
                persisted: manifest.snapshot_limits,
                requested: options.snapshot_limits,
            });
        }
        let wal_directory = root.join(WAL_DIRECTORY_NAME);
        let snapshot_directory = root.join(snapshot::DIRECTORY_NAME);
        cleanup_segment_temps(&wal_directory)?;
        let loaded_snapshot = load_latest_snapshot(&snapshot_directory, manifest)?;
        let exhausted_snapshot = loaded_snapshot
            .as_ref()
            .is_some_and(|(value, _)| value.wal_watermark == u64::MAX);
        let minimum_sequence = loaded_snapshot.as_ref().map_or(1, |(value, _)| {
            value
                .wal_watermark
                .checked_add(1)
                .unwrap_or(RecordSequence::FIRST.get())
        });
        let (segments, obsolete_segments) =
            read_segments(&wal_directory, manifest.database_uuid, minimum_sequence)?;

        let mut recovery = if let Some((value, _)) = loaded_snapshot {
            let next = value.wal_watermark.checked_add(1);
            let engine = value.engine;
            if let Some(next) = next {
                EngineRecovery::with_expected(
                    RecordSequence::new(next).expect("checked successor is non-zero"),
                    engine,
                )
            } else {
                EngineRecovery::from_exhausted_snapshot(engine)
            }
        } else {
            EngineRecovery::new()
        };
        let mut final_valid_offset = 0;
        if !exhausted_snapshot {
            for (index, segment) in segments.iter().enumerate() {
                let sealed = index + 1 != segments.len();
                let expected = recovery.next_record_sequence();
                if expected != Some(segment.first_sequence) {
                    return Err(FileDatabaseError::SegmentSequence {
                        path: segment.path.clone(),
                        expected: expected.map(RecordSequence::get),
                        actual: segment.first_sequence.get(),
                    });
                }
                let mut file = File::open(&segment.path).map_err(|error| {
                    path_io(&segment.path, "open WAL segment for recovery", error)
                })?;
                file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN))
                    .map_err(|error| path_io(&segment.path, "seek past segment header", error))?;
                match recovery.feed(file, options.record_limits) {
                    Ok(offset) => final_valid_offset = offset,
                    Err(source) => {
                        let local_offset = source.last_valid_offset();
                        let repairable = !sealed
                            && matches!(
                                source,
                                RecoveryError::Storage {
                                    source: StorageError::PartialTail { .. },
                                    ..
                                }
                            );
                        if repairable {
                            let physical_length = SEGMENT_HEADER_LEN + local_offset;
                            let file = OpenOptions::new().write(true).open(&segment.path).map_err(
                                |error| {
                                    path_io(&segment.path, "open segment for tail repair", error)
                                },
                            )?;
                            file.set_len(physical_length).map_err(|error| {
                                path_io(&segment.path, "truncate partial WAL tail", error)
                            })?;
                            file.sync_all().map_err(|error| {
                                path_io(&segment.path, "sync repaired WAL segment", error)
                            })?;
                            final_valid_offset = local_offset;
                        } else {
                            return Err(FileDatabaseError::SegmentRecovery {
                                path: segment.path.clone(),
                                physical_offset: SEGMENT_HEADER_LEN
                                    + recovery_error_offset(&source),
                                sealed,
                                source,
                            });
                        }
                    }
                }
            }
        }
        let outcome = recovery.finish(final_valid_offset).map_err(|source| {
            let segment = segments.last().expect("segments are nonempty");
            FileDatabaseError::SegmentRecovery {
                path: segment.path.clone(),
                physical_offset: SEGMENT_HEADER_LEN + recovery_error_offset(&source),
                sealed: false,
                source,
            }
        })?;
        let expected_sequence = outcome.next_record_sequence();
        if !obsolete_segments.is_empty() {
            for path in obsolete_segments {
                fs::remove_file(&path)
                    .map_err(|error| path_io(&path, "remove obsolete WAL prefix", error))?;
            }
            sync_directory(&wal_directory)?;
        }
        let active = segments.last().expect("segments are nonempty");
        let active_len = fs::metadata(&active.path)
            .map_err(|error| path_io(&active.path, "inspect active WAL segment", error))?
            .len();
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active.path)
            .map_err(|error| path_io(&active.path, "open active WAL segment", error))?;
        let backend = SegmentedWal {
            database_uuid: manifest.database_uuid,
            wal_directory,
            active_path: active.path.clone(),
            active_file,
            active_len,
            active_has_records: active_len > SEGMENT_HEADER_LEN,
            segment_target: options.segment_target,
            record_limits: options.record_limits,
            expected_sequence,
            snapshot_limits: options.snapshot_limits,
            _lock: lock,
        };
        Ok(Self::from_recovered(
            outcome,
            backend,
            options.database_options(),
        ))
    }

    /// Atomically installs a snapshot and then compacts fully covered storage.
    pub fn create_snapshot(&mut self) -> Result<SnapshotOutcome, FileDatabaseError> {
        if self.is_poisoned() {
            return Err(FileDatabaseError::SnapshotPoisoned);
        }
        let watermark = self
            .next_record_sequence()
            .map_or(u64::MAX, |sequence| sequence.get() - 1);
        if let Some(next) = self.next_record_sequence()
            && self.backend().active_has_records
        {
            self.backend_mut()
                .rotate(next)
                .map_err(|error| FileDatabaseError::Io {
                    path: self.backend().wal_directory.clone(),
                    operation: "rotate WAL before snapshot",
                    kind: match &error {
                        StorageError::Io { kind, .. } => *kind,
                        _ => io::ErrorKind::Other,
                    },
                    message: error.to_string(),
                })?;
        }
        let snapshot_file = SnapshotFile {
            database_uuid: self.database_uuid(),
            semantics_version: STATE_MACHINE_SEMANTICS_VERSION,
            wal_watermark: watermark,
            engine: self.engine_snapshot(),
        };
        let bytes =
            snapshot::encode(&snapshot_file, self.backend().snapshot_limits).map_err(|source| {
                FileDatabaseError::Snapshot {
                    path: self
                        .backend()
                        .wal_directory
                        .parent()
                        .expect("WAL has root")
                        .join(snapshot::DIRECTORY_NAME)
                        .join(snapshot_name(watermark)),
                    source,
                }
            })?;
        let snapshot_bytes =
            u64::try_from(bytes.len()).map_err(|_| FileDatabaseError::Snapshot {
                path: self
                    .backend()
                    .wal_directory
                    .parent()
                    .expect("WAL has root")
                    .join(snapshot::DIRECTORY_NAME)
                    .join(snapshot_name(watermark)),
                source: SnapshotError::Limit {
                    field: "total bytes",
                    value: u64::MAX,
                    maximum: self.backend().snapshot_limits.max_total_bytes,
                },
            })?;
        let snapshot_directory = self
            .backend()
            .wal_directory
            .parent()
            .expect("WAL has root")
            .join(snapshot::DIRECTORY_NAME);
        cleanup_snapshot_temp(&snapshot_directory)?;
        let target = snapshot_directory.join(snapshot_name(watermark));
        let temp = snapshot_directory.join(snapshot::TEMP_NAME);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|error| path_io(&temp, "create snapshot temp file", error))?;
        file.write_all(&bytes)
            .map_err(|error| path_io(&temp, "write snapshot temp file", error))?;
        file.sync_all()
            .map_err(|error| path_io(&temp, "sync snapshot temp file", error))?;
        fs::rename(&temp, &target).map_err(|error| path_io(&target, "install snapshot", error))?;
        sync_directory(&snapshot_directory)?;

        let active_path = self.backend().active_path.clone();
        let mut segments_removed = 0;
        let entries = fs::read_dir(&self.backend().wal_directory).map_err(|error| {
            committed_cleanup(
                &target,
                watermark,
                "read WAL directory for compaction",
                error,
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                committed_cleanup(
                    &target,
                    watermark,
                    "read WAL directory entry for compaction",
                    error,
                )
            })?;
            let path = entry.path();
            if path != active_path {
                fs::remove_file(&path).map_err(|error| {
                    committed_cleanup(&target, watermark, "remove covered WAL segment", error)
                })?;
                segments_removed += 1;
            }
        }
        sync_directory_io(&self.backend().wal_directory).map_err(|error| {
            committed_cleanup(&target, watermark, "sync compacted WAL directory", error)
        })?;
        let mut older_snapshots_removed = 0;
        for entry in fs::read_dir(&snapshot_directory).map_err(|error| {
            committed_cleanup(&target, watermark, "read snapshots for cleanup", error)
        })? {
            let entry = entry.map_err(|error| {
                committed_cleanup(&target, watermark, "read snapshot cleanup entry", error)
            })?;
            let path = entry.path();
            if path != target {
                fs::remove_file(&path).map_err(|error| {
                    committed_cleanup(&target, watermark, "remove older snapshot", error)
                })?;
                older_snapshots_removed += 1;
            }
        }
        sync_directory_io(&snapshot_directory).map_err(|error| {
            committed_cleanup(&target, watermark, "sync snapshot cleanup", error)
        })?;
        Ok(SnapshotOutcome {
            path: target,
            watermark,
            segments_removed,
            older_snapshots_removed,
            bytes: snapshot_bytes,
        })
    }

    /// Returns the durable database UUID.
    pub fn database_uuid(&self) -> Uuid {
        self.backend().database_uuid()
    }
}

#[derive(Debug)]
struct SegmentInfo {
    path: PathBuf,
    first_sequence: RecordSequence,
}

fn acquire_lock(root: &Path) -> Result<File, FileDatabaseError> {
    let path = root.join(LOCK_NAME);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| path_io(&path, "open database lock", error))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(FileDatabaseError::AlreadyOpen(root.to_path_buf())),
        Err(TryLockError::Error(error)) => Err(FileDatabaseError::Lock {
            path,
            kind: error.kind(),
            message: error.to_string(),
        }),
    }
}

fn read_fixed<const N: usize>(
    path: &Path,
    operation: &'static str,
) -> Result<[u8; N], FileDatabaseError> {
    let mut file = File::open(path).map_err(|error| path_io(path, operation, error))?;
    let length = file
        .metadata()
        .map_err(|error| path_io(path, operation, error))?
        .len();
    if length < N as u64 {
        return Err(FileDatabaseError::Manifest(ManifestError::InvalidLength(
            length,
        )));
    }
    let mut bytes = [0_u8; N];
    file.read_exact(&mut bytes)
        .map_err(|error| path_io(path, operation, error))?;
    Ok(bytes)
}

fn recovery_error_offset(error: &RecoveryError) -> u64 {
    match error {
        RecoveryError::Storage {
            source: StorageError::PartialTail { offset, .. },
            ..
        }
        | RecoveryError::Storage {
            source: StorageError::CorruptWalRecord { offset, .. },
            ..
        }
        | RecoveryError::Storage {
            source: StorageError::UnsupportedRecordVersion { offset, .. },
            ..
        }
        | RecoveryError::Storage {
            source: StorageError::RecordTooLarge { offset, .. },
            ..
        } => *offset,
        _ => error.last_valid_offset(),
    }
}

fn sync_directory(path: &Path) -> Result<(), FileDatabaseError> {
    sync_directory_io(path).map_err(|error| path_io(path, "sync directory", error))
}

fn sync_directory_io(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn path_io(path: &Path, operation: &'static str, error: io::Error) -> FileDatabaseError {
    FileDatabaseError::Io {
        path: path.to_path_buf(),
        operation,
        kind: error.kind(),
        message: error.to_string(),
    }
}

const _: () = assert!(SEGMENT_HEADER_LEN as usize >= RECORD_HEADER_LEN);

#[cfg(test)]
mod tests;
