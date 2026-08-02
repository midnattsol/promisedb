use super::*;

pub(super) fn segment_name(sequence: RecordSequence) -> String {
    format!("{:020}.wal", sequence.get())
}

pub(super) fn segment_path(directory: &Path, sequence: RecordSequence) -> PathBuf {
    directory.join(segment_name(sequence))
}

pub(super) fn parse_segment_name(name: &str) -> Option<RecordSequence> {
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

pub(super) fn segment_header(
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

pub(super) fn create_segment_atomic(
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

pub(super) fn cleanup_segment_temps(directory: &Path) -> Result<(), FileDatabaseError> {
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

pub(super) fn read_segments(
    directory: &Path,
    database_uuid: Uuid,
    minimum_sequence: u64,
) -> Result<(Vec<SegmentInfo>, Vec<PathBuf>), FileDatabaseError> {
    let entries =
        fs::read_dir(directory).map_err(|error| path_io(directory, "read WAL directory", error))?;
    let mut segments = Vec::new();
    let mut obsolete = Vec::new();
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
        if filename_sequence.get() < minimum_sequence {
            obsolete.push(path);
            continue;
        }
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
    Ok((segments, obsolete))
}

pub(super) fn read_segment_header(
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
