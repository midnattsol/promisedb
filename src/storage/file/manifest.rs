use super::*;

pub(super) fn manifest_bytes(manifest: Manifest) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0_u8; MANIFEST_LEN];
    bytes[0..4].copy_from_slice(&MANIFEST_MAGIC);
    bytes[4] = MANIFEST_VERSION;
    bytes[6..8].copy_from_slice(&(MANIFEST_LEN as u16).to_le_bytes());
    bytes[8..24].copy_from_slice(manifest.database_uuid.as_bytes());
    bytes[24..28].copy_from_slice(&manifest.record_limits.max_record_len().to_le_bytes());
    bytes[28..32].copy_from_slice(&STATE_MACHINE_SEMANTICS_VERSION.to_le_bytes());
    bytes[32..40].copy_from_slice(&manifest.snapshot_limits.max_total_bytes.to_le_bytes());
    bytes[40..44].copy_from_slice(&manifest.snapshot_limits.max_collection_items.to_le_bytes());
    bytes[44..48].copy_from_slice(&manifest.snapshot_limits.max_string_bytes.to_le_bytes());
    bytes[48..52].copy_from_slice(&manifest.snapshot_limits.max_nested_items.to_le_bytes());
    let checksum = blake3::hash(&bytes[..64]);
    bytes[64..96].copy_from_slice(checksum.as_bytes());
    bytes
}

pub(super) fn write_manifest_atomic(
    root: &Path,
    manifest: Manifest,
) -> Result<(), FileDatabaseError> {
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

pub(super) fn read_manifest(root: &Path) -> Result<Manifest, FileDatabaseError> {
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
    if bytes[5] != 0 || bytes[52..64].iter().any(|byte| *byte != 0) {
        return Err(FileDatabaseError::Manifest(ManifestError::NonZeroReserved));
    }
    let checksum = blake3::hash(&bytes[..64]);
    if bytes[64..96] != *checksum.as_bytes() {
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
    let snapshot_limits = SnapshotLimits {
        max_total_bytes: u64::from_le_bytes(bytes[32..40].try_into().expect("fixed range")),
        max_collection_items: u32::from_le_bytes(bytes[40..44].try_into().expect("fixed range")),
        max_string_bytes: u32::from_le_bytes(bytes[44..48].try_into().expect("fixed range")),
        max_nested_items: u32::from_le_bytes(bytes[48..52].try_into().expect("fixed range")),
    };
    snapshot_limits
        .validate()
        .map_err(|_| FileDatabaseError::Manifest(ManifestError::InvalidSnapshotLimits))?;
    Ok(Manifest {
        database_uuid: uuid,
        record_limits,
        snapshot_limits,
    })
}
