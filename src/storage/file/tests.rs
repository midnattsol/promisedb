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
    let database = FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
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
        snapshot_limits: SnapshotLimits::default(),
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
    let database = FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
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
        snapshot_limits: SnapshotLimits::default(),
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
    let database = FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
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
    let database = FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
    let uuid = database.database_uuid();
    drop(database);
    assert_eq!(
        fs::read(directory.path.join(MANIFEST_NAME)).unwrap(),
        manifest_bytes(Manifest {
            database_uuid: uuid,
            record_limits: RecordLimits::default(),
            snapshot_limits: SnapshotLimits::default(),
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

    let mut reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
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
    let mut reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
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

#[test]
fn empty_snapshot_at_watermark_zero_reopens_and_cleans_temp() {
    let directory = TestDirectory::new();
    let mut database =
        FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
    let outcome = database.create_snapshot().unwrap();
    assert_eq!(outcome.watermark, 0);
    assert!(outcome.path.ends_with("00000000000000000000.snapshot"));
    drop(database);
    let temp = directory
        .path
        .join(snapshot::DIRECTORY_NAME)
        .join(snapshot::TEMP_NAME);
    fs::write(&temp, b"interrupted snapshot").unwrap();
    let reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
    assert_eq!(reopened.engine().sequence().get(), 0);
    assert_eq!(reopened.next_record_sequence().unwrap().get(), 1);
    assert!(!temp.exists());
}

#[test]
fn snapshot_round_trip_replays_suffix_and_preserves_retry_events_and_compaction() {
    let directory = TestDirectory::new();
    let mut database =
        FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
    let first = create_pool(1);
    let first_response = database.apply(first.clone(), 11).unwrap();
    let outcome = database.create_snapshot().unwrap();
    assert_eq!(outcome.watermark, 1);
    assert_eq!(outcome.segments_removed, 1);
    assert!(
        database
            .backend()
            .active_segment_path()
            .ends_with("00000000000000000002.wal")
    );
    let _ = database.apply(create_pool(2), 22).unwrap();
    let expected_events = database
        .engine()
        .watch_events(crate::domain::SequenceNumber::new(0))
        .to_vec();
    drop(database);

    crate::engine::reset_snapshot_recovery_counts();
    let mut reopened = FileDatabase::open(&directory.path, FileDatabaseOptions::default()).unwrap();
    assert_eq!(crate::engine::snapshot_recovery_counts(), (1, 1));
    assert!(
        reopened
            .engine()
            .resource_pool(ResourcePoolId::from_bytes([1; 16]))
            .is_some()
    );
    assert!(
        reopened
            .engine()
            .resource_pool(ResourcePoolId::from_bytes([2; 16]))
            .is_some()
    );
    assert_eq!(
        reopened
            .engine()
            .watch_events(crate::domain::SequenceNumber::new(0)),
        expected_events
    );
    assert_eq!(reopened.engine().idempotency_record_count(), 2);
    assert_eq!(reopened.apply(first, 999).unwrap(), first_response);
    assert_eq!(reopened.next_record_sequence().unwrap().get(), 3);
}

#[test]
fn corrupt_highest_snapshot_is_fatal_without_fallback() {
    let directory = TestDirectory::new();
    let mut database =
        FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
    let _ = database.apply(create_pool(1), 1).unwrap();
    let first = database.create_snapshot().unwrap();
    let first_bytes = fs::read(&first.path).unwrap();
    let _ = database.apply(create_pool(2), 2).unwrap();
    let highest = database.create_snapshot().unwrap();
    let older = directory
        .path
        .join(snapshot::DIRECTORY_NAME)
        .join(snapshot_name(1));
    fs::write(older, first_bytes).unwrap();
    mutate(&highest.path, |bytes| bytes[128] ^= 1);
    drop(database);
    assert!(matches!(
        FileDatabase::open(&directory.path, FileDatabaseOptions::default()),
        Err(FileDatabaseError::Snapshot {
            source: SnapshotError::ChecksumMismatch,
            ..
        })
    ));
}

#[test]
fn hostile_snapshot_lengths_and_counts_fail_before_large_allocation() {
    let directory = TestDirectory::new();
    let mut database =
        FileDatabase::create(&directory.path, FileDatabaseOptions::default()).unwrap();
    let outcome = database.create_snapshot().unwrap();
    let uuid = database.database_uuid();
    drop(database);
    let limits = SnapshotLimits {
        max_total_bytes: 4096,
        max_collection_items: 32,
        max_string_bytes: 64,
        max_nested_items: 32,
    };

    let mut huge_length = fs::read(&outcome.path).unwrap();
    huge_length[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
    huge_length[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(matches!(
        snapshot::decode(
            &huge_length,
            limits,
            uuid,
            STATE_MACHINE_SEMANTICS_VERSION,
            0
        ),
        Err(SnapshotError::Limit {
            field: "total bytes",
            ..
        })
    ));

    let mut huge_count = fs::read(&outcome.path).unwrap();
    huge_count[128..132].copy_from_slice(&u32::MAX.to_le_bytes());
    let checksum_start = huge_count.len() - 32;
    let checksum = blake3::hash(&huge_count[..checksum_start]);
    huge_count[checksum_start..].copy_from_slice(checksum.as_bytes());
    assert!(matches!(
        snapshot::decode(
            &huge_count,
            limits,
            uuid,
            STATE_MACHINE_SEMANTICS_VERSION,
            0
        ),
        Err(SnapshotError::MalformedPayload(
            StorageError::InvalidLength {
                field: "resource pools",
                ..
            }
        ))
    ));

    assert!(matches!(
        SnapshotLimits {
            max_total_bytes: 159,
            ..SnapshotLimits::default()
        }
        .validate(),
        Err(SnapshotError::InvalidLimits {
            field: "maximum total bytes",
            ..
        })
    ));
}

#[test]
fn snapshot_decoder_handles_arbitrary_bytes_without_panicking() {
    let limits = SnapshotLimits {
        max_total_bytes: 4096,
        max_collection_items: 32,
        max_string_bytes: 64,
        max_nested_items: 32,
    };
    let uuid = Uuid::new_v4();
    let mut state = 0x1234_5678_u64;
    for length in 0..512 {
        let mut bytes = vec![0_u8; length];
        for byte in &mut bytes {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (state >> 56) as u8;
        }
        let result = std::panic::catch_unwind(|| {
            snapshot::decode(&bytes, limits, uuid, STATE_MACHINE_SEMANTICS_VERSION, 0)
        });
        assert!(result.is_ok());
    }
}
