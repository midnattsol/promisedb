//! Locked database directories, manifests, and segmented file-backed WAL storage.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use uuid::Uuid;

use super::StorageError;
use super::backend::{Durability, WalBackend};
use super::database::{Database, DatabaseOptions};
use super::record::{
    HEADER_LEN as RECORD_HEADER_LEN, MIN_RECORD_LEN, RecordLimits, RecordReader, RecordSequence,
};
use super::recovery::{EngineRecovery, RecoveryError};

const LOCK_NAME: &str = "LOCK";
const MANIFEST_NAME: &str = "MANIFEST";
const MANIFEST_TEMP_NAME: &str = "MANIFEST.tmp";
const WAL_DIRECTORY_NAME: &str = "wal";
const MANIFEST_MAGIC: [u8; 4] = *b"PDBM";
const MANIFEST_VERSION: u8 = 1;
const MANIFEST_LEN: usize = 64;
const STATE_MACHINE_SEMANTICS_VERSION: u32 = 1;
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
}

impl Default for FileDatabaseOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Sync,
            record_limits: RecordLimits::default(),
            segment_target: DEFAULT_SEGMENT_TARGET,
        }
    }
}

impl FileDatabaseOptions {
    fn validate(self) -> Result<Self, FileDatabaseError> {
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
        sync_directory(&root)?;

        let manifest = Manifest {
            database_uuid: Uuid::new_v4(),
            record_limits: options.record_limits,
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
        let wal_directory = root.join(WAL_DIRECTORY_NAME);
        cleanup_segment_temps(&wal_directory)?;
        let segments = read_segments(&wal_directory, manifest.database_uuid)?;

        let mut recovery = EngineRecovery::new();
        let mut final_valid_offset = 0;
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
            let mut file = File::open(&segment.path)
                .map_err(|error| path_io(&segment.path, "open WAL segment for recovery", error))?;
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
                            |error| path_io(&segment.path, "open segment for tail repair", error),
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
                            physical_offset: SEGMENT_HEADER_LEN + recovery_error_offset(&source),
                            sealed,
                            source,
                        });
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
            _lock: lock,
        };
        Ok(Self::from_recovered(
            outcome,
            backend,
            options.database_options(),
        ))
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

fn manifest_bytes(manifest: Manifest) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0_u8; MANIFEST_LEN];
    bytes[0..4].copy_from_slice(&MANIFEST_MAGIC);
    bytes[4] = MANIFEST_VERSION;
    bytes[6..8].copy_from_slice(&(MANIFEST_LEN as u16).to_le_bytes());
    bytes[8..24].copy_from_slice(manifest.database_uuid.as_bytes());
    bytes[24..28].copy_from_slice(&manifest.record_limits.max_record_len().to_le_bytes());
    bytes[28..32].copy_from_slice(&STATE_MACHINE_SEMANTICS_VERSION.to_le_bytes());
    let checksum = blake3::hash(&bytes[..48]);
    bytes[48..64].copy_from_slice(&checksum.as_bytes()[..16]);
    bytes
}

fn write_manifest_atomic(root: &Path, manifest: Manifest) -> Result<(), FileDatabaseError> {
    let temp = root.join(MANIFEST_TEMP_NAME);
    let target = root.join(MANIFEST_NAME);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|error| path_io(&temp, "create manifest temp file", error))?;
    file.write_all(&manifest_bytes(manifest))
        .map_err(|error| path_io(&temp, "write manifest temp file", error))?;
    file.sync_all()
        .map_err(|error| path_io(&temp, "sync manifest temp file", error))?;
    fs::rename(&temp, &target).map_err(|error| path_io(&target, "install manifest", error))?;
    sync_directory(root)
}

fn read_manifest(root: &Path) -> Result<Manifest, FileDatabaseError> {
    let path = root.join(MANIFEST_NAME);
    let length = fs::metadata(&path)
        .map_err(|error| path_io(&path, "inspect manifest", error))?
        .len();
    if length != MANIFEST_LEN as u64 {
        return Err(FileDatabaseError::Manifest(ManifestError::InvalidLength(
            length,
        )));
    }
    let bytes = read_fixed::<MANIFEST_LEN>(&path, "read manifest")?;
    let magic: [u8; 4] = bytes[0..4].try_into().expect("fixed range");
    if magic != MANIFEST_MAGIC {
        return Err(FileDatabaseError::Manifest(ManifestError::InvalidMagic(
            magic,
        )));
    }
    if bytes[4] != MANIFEST_VERSION {
        return Err(FileDatabaseError::Manifest(
            ManifestError::UnsupportedVersion(bytes[4]),
        ));
    }
    if u16::from_le_bytes(bytes[6..8].try_into().expect("fixed range")) != MANIFEST_LEN as u16 {
        return Err(FileDatabaseError::Manifest(
            ManifestError::InvalidHeaderLength(u16::from_le_bytes(
                bytes[6..8].try_into().expect("fixed range"),
            )),
        ));
    }
    if bytes[5] != 0 || bytes[32..48].iter().any(|byte| *byte != 0) {
        return Err(FileDatabaseError::Manifest(ManifestError::NonZeroReserved));
    }
    let checksum = blake3::hash(&bytes[..48]);
    if bytes[48..64] != checksum.as_bytes()[..16] {
        return Err(FileDatabaseError::Manifest(ManifestError::ChecksumMismatch));
    }
    let uuid = Uuid::from_bytes(bytes[8..24].try_into().expect("fixed range"));
    if uuid.is_nil() || uuid.get_version_num() != 4 {
        return Err(FileDatabaseError::Manifest(
            ManifestError::InvalidDatabaseUuid,
        ));
    }
    let semantics = u32::from_le_bytes(bytes[28..32].try_into().expect("fixed range"));
    if semantics != STATE_MACHINE_SEMANTICS_VERSION {
        return Err(FileDatabaseError::Manifest(
            ManifestError::UnsupportedSemanticsVersion(semantics),
        ));
    }
    let max_record_len = u32::from_le_bytes(bytes[24..28].try_into().expect("fixed range"));
    let record_limits = RecordLimits::new(max_record_len).map_err(|_| {
        FileDatabaseError::Manifest(ManifestError::InvalidRecordLimit(max_record_len))
    })?;
    Ok(Manifest {
        database_uuid: uuid,
        record_limits,
    })
}

fn segment_name(sequence: RecordSequence) -> String {
    format!("{:020}.wal", sequence.get())
}

fn segment_path(directory: &Path, sequence: RecordSequence) -> PathBuf {
    directory.join(segment_name(sequence))
}

fn parse_segment_name(name: &str) -> Option<RecordSequence> {
    if name.len() != 24
        || !name.ends_with(".wal")
        || !name[..20].bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let value = name[..20].parse().ok()?;
    let sequence = RecordSequence::new(value)?;
    (segment_name(sequence) == name).then_some(sequence)
}

fn segment_header(
    database_uuid: Uuid,
    first_sequence: RecordSequence,
) -> [u8; SEGMENT_HEADER_LEN as usize] {
    let mut bytes = [0_u8; SEGMENT_HEADER_LEN as usize];
    bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
    bytes[4] = SEGMENT_VERSION;
    bytes[6..8].copy_from_slice(&(SEGMENT_HEADER_LEN as u16).to_le_bytes());
    bytes[8..24].copy_from_slice(database_uuid.as_bytes());
    bytes[24..32].copy_from_slice(&first_sequence.get().to_le_bytes());
    let checksum = blake3::hash(&bytes[..48]);
    bytes[48..64].copy_from_slice(&checksum.as_bytes()[..16]);
    bytes
}

fn create_segment_atomic(
    directory: &Path,
    database_uuid: Uuid,
    first_sequence: RecordSequence,
) -> io::Result<PathBuf> {
    let target = segment_path(directory, first_sequence);
    let temp = directory.join(format!("{}.tmp", segment_name(first_sequence)));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(&segment_header(database_uuid, first_sequence))?;
    file.sync_all()?;
    fs::rename(&temp, &target)?;
    sync_directory_io(directory)?;
    Ok(target)
}

fn cleanup_segment_temps(directory: &Path) -> Result<(), FileDatabaseError> {
    let entries =
        fs::read_dir(directory).map_err(|error| path_io(directory, "read WAL directory", error))?;
    let mut removed = false;
    for entry in entries {
        let entry = entry.map_err(|error| path_io(directory, "read WAL directory entry", error))?;
        let path = entry.path();
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".wal.tmp"))
        {
            fs::remove_file(&path)
                .map_err(|error| path_io(&path, "remove temporary WAL segment", error))?;
            removed = true;
        }
    }
    if removed {
        sync_directory(directory)?;
    }
    Ok(())
}

fn read_segments(
    directory: &Path,
    database_uuid: Uuid,
) -> Result<Vec<SegmentInfo>, FileDatabaseError> {
    let entries =
        fs::read_dir(directory).map_err(|error| path_io(directory, "read WAL directory", error))?;
    let mut segments = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| path_io(directory, "read WAL directory entry", error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| path_io(&path, "inspect WAL directory entry", error))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(FileDatabaseError::InvalidSegmentName(path));
        };
        if !file_type.is_file() {
            return Err(FileDatabaseError::InvalidSegmentName(path));
        }
        let Some(filename_sequence) = parse_segment_name(name) else {
            return Err(FileDatabaseError::InvalidSegmentName(path));
        };
        let header_sequence = read_segment_header(&path, database_uuid)?;
        if filename_sequence != header_sequence {
            return Err(FileDatabaseError::SegmentNameSequence {
                path,
                filename_sequence: filename_sequence.get(),
                header_sequence: header_sequence.get(),
            });
        }
        segments.push(SegmentInfo {
            path,
            first_sequence: header_sequence,
        });
    }
    segments.sort_by_key(|segment| segment.first_sequence);
    if segments.is_empty() {
        return Err(FileDatabaseError::MissingSegments(directory.to_path_buf()));
    }
    Ok(segments)
}

fn read_segment_header(
    path: &Path,
    database_uuid: Uuid,
) -> Result<RecordSequence, FileDatabaseError> {
    let bytes = read_fixed::<{ SEGMENT_HEADER_LEN as usize }>(path, "read segment header")
        .map_err(|error| match error {
            FileDatabaseError::Manifest(ManifestError::InvalidLength(actual)) => {
                FileDatabaseError::SegmentHeader {
                    path: path.to_path_buf(),
                    source: SegmentHeaderError::Truncated {
                        expected: SEGMENT_HEADER_LEN,
                        actual,
                    },
                }
            }
            other => other,
        })?;
    let magic: [u8; 4] = bytes[0..4].try_into().expect("fixed range");
    let failure = if magic != SEGMENT_MAGIC {
        Some(SegmentHeaderError::InvalidMagic(magic))
    } else if bytes[4] != SEGMENT_VERSION {
        Some(SegmentHeaderError::UnsupportedVersion(bytes[4]))
    } else if bytes[5] != 0 {
        Some(SegmentHeaderError::UnsupportedFlags(bytes[5]))
    } else {
        let length = u16::from_le_bytes(bytes[6..8].try_into().expect("fixed range"));
        if u64::from(length) != SEGMENT_HEADER_LEN {
            Some(SegmentHeaderError::InvalidHeaderLength(length))
        } else if bytes[32..48].iter().any(|byte| *byte != 0) {
            Some(SegmentHeaderError::NonZeroReserved)
        } else {
            let checksum = blake3::hash(&bytes[..48]);
            (bytes[48..64] != checksum.as_bytes()[..16])
                .then_some(SegmentHeaderError::ChecksumMismatch)
        }
    };
    if let Some(source) = failure {
        return Err(FileDatabaseError::SegmentHeader {
            path: path.to_path_buf(),
            source,
        });
    }
    let actual_uuid = Uuid::from_bytes(bytes[8..24].try_into().expect("fixed range"));
    if actual_uuid != database_uuid {
        return Err(FileDatabaseError::SegmentHeader {
            path: path.to_path_buf(),
            source: SegmentHeaderError::DatabaseUuidMismatch {
                expected: database_uuid,
                actual: actual_uuid,
            },
        });
    }
    let sequence_value = u64::from_le_bytes(bytes[24..32].try_into().expect("fixed range"));
    RecordSequence::new(sequence_value).ok_or_else(|| FileDatabaseError::SegmentHeader {
        path: path.to_path_buf(),
        source: SegmentHeaderError::InvalidFirstSequence(sequence_value),
    })
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
mod tests {
    use super::*;
    use crate::command::{ClientId, Command, CommandOperation, IdempotencyKey};
    use crate::domain::{CapacityCurve, ResourcePoolId, Unit};

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            Self {
                path: std::env::temp_dir().join(format!("promisedb-file-test-{}", Uuid::new_v4())),
            }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn options(segment_target: u64) -> FileDatabaseOptions {
        FileDatabaseOptions {
            segment_target,
            ..FileDatabaseOptions::default()
        }
    }

    fn create_pool(byte: u8) -> Command {
        Command::new(
            ClientId::new("file-tests"),
            IdempotencyKey::new(format!("create-{byte}")),
            CommandOperation::CreateResourcePool {
                resource_pool_id: ResourcePoolId::from_bytes([byte; 16]),
                display_name: format!("pool-{byte}"),
                unit: Unit::new("unit".into(), 1).unwrap(),
                capacity_curve: CapacityCurve::empty(),
            },
        )
    }

    fn wal_paths(root: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<_> = fs::read_dir(root.join(WAL_DIRECTORY_NAME))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
            .collect();
        paths.sort();
        paths
    }

    fn mutate(path: &Path, update: impl FnOnce(&mut Vec<u8>)) {
        let mut bytes = fs::read(path).unwrap();
        update(&mut bytes);
        fs::write(path, bytes).unwrap();
    }

    fn resign_header(bytes: &mut [u8]) {
        let checksum = blake3::hash(&bytes[..48]);
        bytes[48..64].copy_from_slice(&checksum.as_bytes()[..16]);
    }

    fn resign_first_record(bytes: &mut [u8]) {
        let start = SEGMENT_HEADER_LEN as usize;
        let length = u32::from_le_bytes(bytes[start + 8..start + 12].try_into().unwrap()) as usize;
        let checksum_start = start + length - 16;
        let checksum = blake3::hash(&bytes[start..checksum_start]);
        bytes[checksum_start..start + length].copy_from_slice(&checksum.as_bytes()[..16]);
    }

    fn encoded_empty_record() -> Vec<u8> {
        super::super::record::encode(
            &super::super::record::Record::new(RecordSequence::FIRST, 0, Vec::new()),
            RecordLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn defaults_are_sync_and_production_sized() {
        let defaults = FileDatabaseOptions::default();
        assert_eq!(defaults.durability, Durability::Sync);
        assert_eq!(defaults.record_limits.max_record_len(), 64 * 1024 * 1024);
        assert_eq!(defaults.segment_target, 256 * 1024 * 1024);
        assert!(matches!(
            options(MIN_SEGMENT_TARGET - 1).validate(),
            Err(FileDatabaseError::InvalidSegmentTarget { .. })
        ));
    }

    #[test]
    fn append_tracks_the_exact_checked_physical_length() {
        let directory = TestDirectory::new();
        let database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let uuid = database.database_uuid();
        let active_path = database.backend().active_segment_path().to_path_buf();
        drop(database);

        let lock = acquire_lock(&directory.path).unwrap();
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_path)
            .unwrap();
        let mut backend = SegmentedWal {
            database_uuid: uuid,
            wal_directory: directory.path.join(WAL_DIRECTORY_NAME),
            active_path,
            active_file,
            active_len: SEGMENT_HEADER_LEN,
            active_has_records: false,
            segment_target: u64::MAX,
            record_limits: RecordLimits::default(),
            expected_sequence: Some(RecordSequence::FIRST),
            _lock: lock,
        };
        let bytes = encoded_empty_record();

        backend.append(&bytes).unwrap();

        let expected_len = SEGMENT_HEADER_LEN + u64::try_from(bytes.len()).unwrap();
        assert_eq!(backend.active_len, expected_len);
        assert_eq!(
            fs::metadata(&backend.active_path).unwrap().len(),
            expected_len
        );
        assert_eq!(
            backend.expected_sequence,
            Some(RecordSequence::new(2).unwrap())
        );
    }

    #[test]
    fn segment_length_overflow_fails_before_write_or_rotation_mutation() {
        let directory = TestDirectory::new();
        let database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let uuid = database.database_uuid();
        let active_path = database.backend().active_segment_path().to_path_buf();
        drop(database);

        let bytes = encoded_empty_record();
        let append_len = u64::try_from(bytes.len()).unwrap();
        let active_len = u64::MAX - append_len + 1;
        let lock = acquire_lock(&directory.path).unwrap();
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_path)
            .unwrap();
        let mut backend = SegmentedWal {
            database_uuid: uuid,
            wal_directory: directory.path.join(WAL_DIRECTORY_NAME),
            active_path: active_path.clone(),
            active_file,
            active_len,
            active_has_records: true,
            segment_target: u64::MAX - 1,
            record_limits: RecordLimits::default(),
            expected_sequence: Some(RecordSequence::FIRST),
            _lock: lock,
        };
        let physical_len = fs::metadata(&active_path).unwrap().len();
        let paths = wal_paths(&directory.path);

        assert_eq!(
            backend.append(&bytes),
            Err(StorageError::SegmentLengthOverflow {
                current: active_len,
                append: bytes.len(),
            })
        );
        assert_eq!(backend.active_len, active_len);
        assert_eq!(backend.active_path, active_path);
        assert!(backend.active_has_records);
        assert_eq!(backend.expected_sequence, Some(RecordSequence::FIRST));
        assert_eq!(
            fs::metadata(&backend.active_path).unwrap().len(),
            physical_len
        );
        assert_eq!(wal_paths(&directory.path), paths);
    }

    #[test]
    fn stable_lock_excludes_an_open_in_same_process_and_releases_on_drop() {
        let directory = TestDirectory::new();
        let database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert!(directory.path.join(LOCK_NAME).is_file());
        assert!(matches!(
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
            Err(FileDatabaseError::AlreadyOpen(path)) if path == directory.path
        ));
        drop(database);
        FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
    }

    #[test]
    fn manifest_is_canonical_and_corruption_and_limit_mismatch_are_structured() {
        let directory = TestDirectory::new();
        let database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let uuid = database.database_uuid();
        drop(database);
        assert_eq!(
            fs::read(directory.path.join(MANIFEST_NAME)).unwrap(),
            manifest_bytes(Manifest {
                database_uuid: uuid,
                record_limits: RecordLimits::default(),
            })
        );

        let mismatched = FileDatabaseOptions {
            record_limits: RecordLimits::new(1024).unwrap(),
            ..FileDatabaseOptions::default()
        };
        assert!(matches!(
            FileDatabase::open(&directory.path, mismatched),
            Err(FileDatabaseError::RecordLimitsMismatch { persisted, requested })
                if persisted == 64 * 1024 * 1024 && requested == 1024
        ));

        mutate(&directory.path.join(MANIFEST_NAME), |bytes| bytes[48] ^= 1);
        assert!(matches!(
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
            Err(FileDatabaseError::Manifest(ManifestError::ChecksumMismatch))
        ));
    }

    #[test]
    fn segment_names_are_canonical_and_uuid_is_bound_to_manifest() {
        let directory = TestDirectory::new();
        drop(FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap());
        let segment = wal_paths(&directory.path).pop().unwrap();
        let noncanonical = segment.parent().unwrap().join("1.wal");
        fs::rename(&segment, &noncanonical).unwrap();
        assert!(matches!(
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
            Err(FileDatabaseError::InvalidSegmentName(path)) if path == noncanonical
        ));
        fs::rename(&noncanonical, &segment).unwrap();

        mutate(&segment, |bytes| {
            bytes[8..24].copy_from_slice(Uuid::new_v4().as_bytes());
            resign_header(bytes);
        });
        assert!(matches!(
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
            Err(FileDatabaseError::SegmentHeader {
                source: SegmentHeaderError::DatabaseUuidMismatch { .. },
                ..
            })
        ));
    }

    #[test]
    fn rotates_whole_groups_and_streams_recovery_across_segments() {
        let directory = TestDirectory::new();
        let options = options(MIN_SEGMENT_TARGET);
        let mut database = FileDatabase::create(&directory.path, options).unwrap();
        let uuid = database.database_uuid();
        let _ = database.apply(create_pool(1), 1).unwrap();
        let _ = database.apply(create_pool(2), 2).unwrap();
        let _ = database.apply(create_pool(3), 3).unwrap();
        assert_eq!(wal_paths(&directory.path).len(), 3);
        assert_eq!(database.next_record_sequence().unwrap().get(), 4);
        drop(database);

        let mut reopened = FileDatabase::open(&directory.path, options).unwrap();
        assert_eq!(reopened.database_uuid(), uuid);
        for byte in 1..=3 {
            assert!(
                reopened
                    .engine()
                    .resource_pool(ResourcePoolId::from_bytes([byte; 16]))
                    .is_some()
            );
        }
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 4);
        let _ = reopened.apply(create_pool(4), 4).unwrap();
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 5);
    }

    #[test]
    fn oversized_group_occupies_a_fresh_segment_without_splitting() {
        let directory = TestDirectory::new();
        let options = options(MIN_SEGMENT_TARGET);
        let mut database = FileDatabase::create(&directory.path, options).unwrap();
        database
            .apply_batch(vec![
                super::super::TimedCommand::new(create_pool(1), 1),
                super::super::TimedCommand::new(create_pool(2), 2),
            ])
            .unwrap();
        assert_eq!(wal_paths(&directory.path).len(), 1);
        let _ = database.apply(create_pool(3), 3).unwrap();
        assert_eq!(wal_paths(&directory.path).len(), 2);
    }

    #[test]
    fn final_partial_tail_is_truncated_and_sequence_continues() {
        let directory = TestDirectory::new();
        let mut database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let _ = database.apply(create_pool(1), 1).unwrap();
        let active = database.backend().active_segment_path().to_path_buf();
        drop(database);
        let valid_length = fs::metadata(&active).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&active)
            .unwrap()
            .write_all(&[0xaa; 10])
            .unwrap();

        let mut reopened =
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert_eq!(fs::metadata(&active).unwrap().len(), valid_length);
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 2);
        let _ = reopened.apply(create_pool(2), 2).unwrap();
        drop(reopened);
        let reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 3);
    }

    #[test]
    fn partial_tail_in_sealed_segment_is_fatal() {
        let directory = TestDirectory::new();
        let options = options(MIN_SEGMENT_TARGET);
        let mut database = FileDatabase::create(&directory.path, options).unwrap();
        let _ = database.apply(create_pool(1), 1).unwrap();
        let _ = database.apply(create_pool(2), 2).unwrap();
        drop(database);
        let sealed = wal_paths(&directory.path)[0].clone();
        OpenOptions::new()
            .append(true)
            .open(&sealed)
            .unwrap()
            .write_all(&[0xbb; 10])
            .unwrap();
        assert!(matches!(
            FileDatabase::open(&directory.path, options),
            Err(FileDatabaseError::SegmentRecovery {
                sealed: true,
                source: RecoveryError::Storage {
                    source: StorageError::PartialTail { .. },
                    ..
                },
                ..
            })
        ));
    }

    #[test]
    fn complete_checksum_corruption_is_never_repaired() {
        let directory = TestDirectory::new();
        let mut database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let _ = database.apply(create_pool(1), 1).unwrap();
        drop(database);
        let segment = wal_paths(&directory.path).pop().unwrap();
        mutate(&segment, |bytes| {
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
        });
        let length = fs::metadata(&segment).unwrap().len();
        assert!(matches!(
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
            Err(FileDatabaseError::SegmentRecovery {
                sealed: false,
                source: RecoveryError::Storage {
                    source: StorageError::CorruptWalRecord {
                        reason: super::super::RecordCorruption::ChecksumMismatch,
                        ..
                    },
                    ..
                },
                ..
            })
        ));
        assert_eq!(fs::metadata(segment).unwrap().len(), length);
    }

    #[test]
    fn sequence_gap_is_fatal_without_resynchronization() {
        let directory = TestDirectory::new();
        let options = options(MIN_SEGMENT_TARGET);
        let mut database = FileDatabase::create(&directory.path, options).unwrap();
        let _ = database.apply(create_pool(1), 1).unwrap();
        let _ = database.apply(create_pool(2), 2).unwrap();
        drop(database);
        let second = wal_paths(&directory.path)[1].clone();
        mutate(&second, |bytes| {
            let sequence = SEGMENT_HEADER_LEN as usize + 16;
            bytes[sequence..sequence + 8].copy_from_slice(&3_u64.to_le_bytes());
            resign_first_record(bytes);
        });
        assert!(matches!(
            FileDatabase::open(&directory.path, options),
            Err(FileDatabaseError::SegmentRecovery {
                source: RecoveryError::Storage {
                    source: StorageError::CorruptWalRecord {
                        reason: super::super::RecordCorruption::SequenceMismatch {
                            expected: 2,
                            actual: 3
                        },
                        ..
                    },
                    ..
                },
                ..
            })
        ));
    }

    #[test]
    fn empty_final_active_segment_is_valid() {
        let directory = TestDirectory::new();
        let mut database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let _ = database.apply(create_pool(1), 1).unwrap();
        let uuid = database.database_uuid();
        drop(database);
        create_segment_atomic(
            &directory.path.join(WAL_DIRECTORY_NAME),
            uuid,
            RecordSequence::new(2).unwrap(),
        )
        .unwrap();
        let reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 2);
        assert!(
            reopened
                .backend()
                .active_segment_path()
                .ends_with("00000000000000000002.wal")
        );
    }

    #[test]
    fn recovered_retry_does_not_duplicate_apply() {
        let directory = TestDirectory::new();
        let command = create_pool(1);
        let mut database =
            FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
        let first_response = database.apply(command.clone(), 1).unwrap();
        drop(database);
        let mut reopened =
            FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert_eq!(reopened.apply(command, 999).unwrap(), first_response);
        assert_eq!(reopened.next_record_sequence().unwrap().get(), 2);
    }

    #[test]
    fn recognized_segment_temp_files_are_removed_while_opening_locked() {
        let directory = TestDirectory::new();
        drop(FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap());
        let temp = directory
            .path
            .join(WAL_DIRECTORY_NAME)
            .join("00000000000000000002.wal.tmp");
        fs::write(&temp, b"crash debris").unwrap();
        let database = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
        assert!(!temp.exists());
        drop(database);
    }
}
