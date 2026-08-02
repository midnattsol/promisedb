use super::*;

pub(super) fn load_latest_snapshot(
    directory: &Path,
    manifest: Manifest,
) -> Result<Option<(snapshot::DecodedSnapshot, PathBuf)>, FileDatabaseError> {
    cleanup_snapshot_temp(directory)?;
    let mut latest: Option<(u64, PathBuf)> = None;
    for entry in fs::read_dir(directory)
        .map_err(|error| path_io(directory, "read snapshot directory", error))?
    {
        let entry =
            entry.map_err(|error| path_io(directory, "read snapshot directory entry", error))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| path_io(&path, "inspect snapshot entry", error))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(FileDatabaseError::Snapshot {
                path: path.clone(),
                source: SnapshotError::InvalidFilename(path),
            });
        };
        let Some(watermark) = parse_snapshot_name(name) else {
            return Err(FileDatabaseError::Snapshot {
                path: path.clone(),
                source: SnapshotError::InvalidFilename(path),
            });
        };
        if !file_type.is_file() {
            return Err(FileDatabaseError::Snapshot {
                path: path.clone(),
                source: SnapshotError::InvalidFilename(path),
            });
        }
        if latest
            .as_ref()
            .is_none_or(|(current, _)| watermark > *current)
        {
            latest = Some((watermark, path));
        }
    }
    let Some((watermark, path)) = latest else {
        return Ok(None);
    };
    let length = fs::metadata(&path)
        .map_err(|error| path_io(&path, "inspect snapshot", error))?
        .len();
    if length > manifest.snapshot_limits.max_total_bytes {
        return Err(FileDatabaseError::Snapshot {
            path,
            source: SnapshotError::Limit {
                field: "total bytes",
                value: length,
                maximum: manifest.snapshot_limits.max_total_bytes,
            },
        });
    }
    let capacity = usize::try_from(length).map_err(|_| FileDatabaseError::Snapshot {
        path: path.clone(),
        source: SnapshotError::Limit {
            field: "total bytes",
            value: length,
            maximum: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
        },
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| FileDatabaseError::Snapshot {
            path: path.clone(),
            source: SnapshotError::Allocation {
                field: "total bytes",
                requested: length,
            },
        })?;
    bytes.resize(capacity, 0);
    File::open(&path)
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|error| path_io(&path, "read snapshot", error))?;
    let decoded = snapshot::decode(
        &bytes,
        manifest.snapshot_limits,
        manifest.database_uuid,
        STATE_MACHINE_SEMANTICS_VERSION,
        watermark,
    )
    .map_err(|source| FileDatabaseError::Snapshot {
        path: path.clone(),
        source,
    })?;
    Ok(Some((decoded, path)))
}

pub(super) fn snapshot_name(watermark: u64) -> String {
    format!("{watermark:020}{}", snapshot::EXTENSION)
}

pub(super) fn parse_snapshot_name(name: &str) -> Option<u64> {
    if name.len() != 29
        || !name.ends_with(snapshot::EXTENSION)
        || !name[..20].bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let value = name[..20].parse().ok()?;
    (snapshot_name(value) == name).then_some(value)
}

pub(super) fn cleanup_snapshot_temp(directory: &Path) -> Result<(), FileDatabaseError> {
    let temp = directory.join(snapshot::TEMP_NAME);
    match fs::remove_file(&temp) {
        Ok(()) => sync_directory(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(path_io(&temp, "remove temporary snapshot", error)),
    }
}

pub(super) fn committed_cleanup(
    path: &Path,
    watermark: u64,
    operation: &'static str,
    error: io::Error,
) -> FileDatabaseError {
    FileDatabaseError::SnapshotCommittedCleanup {
        path: path.to_path_buf(),
        watermark,
        operation,
        kind: error.kind(),
        message: error.to_string(),
    }
}
