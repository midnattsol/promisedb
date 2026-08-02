//! Bounded snapshot v1 decoding and canonical validation.

use uuid::Uuid;

use crate::command::{ClientId, IdempotencyKey};
use crate::domain::SequenceNumber;
use crate::engine::{Engine, EngineSnapshot, PublicationRevision};
use crate::idempotency::CommandHash;
use crate::storage::StorageError;
use crate::storage::transition::Reader;

use super::format::{CHECKSUM_LEN, HEADER_LEN, MAGIC, VERSION};
use super::{DecodedSnapshot, SnapshotError, SnapshotLimits};

pub(crate) fn decode(
    bytes: &[u8],
    limits: SnapshotLimits,
    expected_uuid: Uuid,
    expected_semantics: u32,
    filename_watermark: u64,
) -> Result<DecodedSnapshot, SnapshotError> {
    let limits = limits.validate()?;
    let actual_len = u64::try_from(bytes.len()).map_err(|_| SnapshotError::Limit {
        field: "total bytes",
        value: u64::MAX,
        maximum: limits.max_total_bytes,
    })?;
    if actual_len > limits.max_total_bytes {
        return Err(SnapshotError::Limit {
            field: "total bytes",
            value: actual_len,
            maximum: limits.max_total_bytes,
        });
    }
    let header_len = u64::try_from(HEADER_LEN).expect("fixed header length fits u64");
    if bytes.len() < HEADER_LEN {
        return Err(SnapshotError::Truncated {
            expected: header_len,
            actual: actual_len,
        });
    }
    let magic = fixed::<4>(bytes, 0, "magic")?;
    if magic != MAGIC {
        return Err(SnapshotError::InvalidMagic(magic));
    }
    if bytes[4] != VERSION {
        return Err(SnapshotError::UnsupportedVersion(bytes[4]));
    }
    if bytes[5] != 0 {
        return Err(SnapshotError::UnsupportedFlags(bytes[5]));
    }
    if u16::from_le_bytes(fixed(bytes, 6, "header_len")?)
        != u16::try_from(HEADER_LEN).expect("fixed header length fits u16")
    {
        return Err(SnapshotError::MalformedHeader("header_len"));
    }
    if u16::from_le_bytes(fixed(bytes, 8, "checksum_len")?)
        != u16::try_from(CHECKSUM_LEN).expect("fixed checksum length fits u16")
    {
        return Err(SnapshotError::MalformedHeader("checksum_len"));
    }
    if bytes[10..16]
        .iter()
        .chain(&bytes[52..56])
        .chain(&bytes[96..128])
        .any(|value| *value != 0)
    {
        return Err(SnapshotError::NonZeroReserved);
    }
    let total_len = u64::from_le_bytes(fixed(bytes, 16, "total_len")?);
    let payload_len = u64::from_le_bytes(fixed(bytes, 24, "payload_len")?);
    if total_len > limits.max_total_bytes {
        return Err(SnapshotError::Limit {
            field: "total bytes",
            value: total_len,
            maximum: limits.max_total_bytes,
        });
    }
    let checksum_len = u64::try_from(CHECKSUM_LEN).expect("fixed checksum length fits u64");
    let expected_total = header_len
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(checksum_len))
        .ok_or(SnapshotError::MalformedHeader("payload_len"))?;
    if total_len != expected_total {
        return Err(SnapshotError::MalformedHeader("total_len"));
    }
    if actual_len != total_len {
        return Err(SnapshotError::Truncated {
            expected: total_len,
            actual: actual_len,
        });
    }
    let database_uuid = Uuid::from_bytes(fixed(bytes, 32, "database_uuid")?);
    if database_uuid != expected_uuid {
        return Err(SnapshotError::DatabaseUuidMismatch {
            expected: expected_uuid,
            actual: database_uuid,
        });
    }
    let semantics_version = u32::from_le_bytes(fixed(bytes, 48, "semantics_version")?);
    if semantics_version != expected_semantics {
        return Err(SnapshotError::UnsupportedSemanticsVersion(
            semantics_version,
        ));
    }
    let wal_watermark = u64::from_le_bytes(fixed(bytes, 56, "wal_watermark")?);
    if wal_watermark != filename_watermark {
        return Err(SnapshotError::FilenameWatermarkMismatch {
            filename: filename_watermark,
            header: wal_watermark,
        });
    }
    let checksum_start = bytes
        .len()
        .checked_sub(CHECKSUM_LEN)
        .ok_or(SnapshotError::MalformedHeader("checksum_len"))?;
    if blake3::hash(&bytes[..checksum_start]).as_bytes() != &bytes[checksum_start..] {
        return Err(SnapshotError::ChecksumMismatch);
    }

    let nested = limit_usize("maximum nested items", limits.max_nested_items)?;
    let collections = limit_usize("maximum collection items", limits.max_collection_items)?;
    let strings = limit_usize("maximum string bytes", limits.max_string_bytes)?;
    let mut reader = Reader::new_bounded(
        &bytes[HEADER_LEN..checksum_start],
        nested.max(collections),
        strings,
    );
    let pool_count = top_count(&mut reader, "resource pools", collections)?;
    let mut resource_pools = reserved_vec("resource pools", reader.safe_capacity(pool_count))?;
    for _ in 0..pool_count {
        resource_pools.push(reader.resource_pool().map_err(map_codec)?);
    }
    let promise_count = top_count(&mut reader, "promises", collections)?;
    let mut promises = reserved_vec("promises", reader.safe_capacity(promise_count))?;
    for _ in 0..promise_count {
        promises.push(reader.promise().map_err(map_codec)?);
    }
    let event_count = top_count(&mut reader, "events", collections)?;
    let mut events = reserved_vec("events", reader.safe_capacity(event_count))?;
    for _ in 0..event_count {
        events.push(reader.event().map_err(map_codec)?);
    }
    let record_count = top_count(&mut reader, "idempotency records", collections)?;
    let mut idempotency_records =
        reserved_vec("idempotency records", reader.safe_capacity(record_count))?;
    for _ in 0..record_count {
        let client = ClientId::new(reader.string("client id").map_err(map_codec)?);
        let key = IdempotencyKey::new(reader.string("idempotency key").map_err(map_codec)?);
        let hash = reader.take(32).map_err(map_codec)?;
        let response = reader.response().map_err(map_codec)?;
        idempotency_records.push((
            client,
            key,
            CommandHash::from_bytes(
                hash.try_into()
                    .map_err(|_| SnapshotError::MalformedHeader("command_hash"))?,
            ),
            response,
        ));
    }
    if !reader.is_empty() {
        return Err(SnapshotError::MalformedPayload(
            StorageError::CorruptRecord("trailing snapshot payload bytes"),
        ));
    }
    let snapshot = EngineSnapshot {
        resource_pools,
        promises,
        events,
        idempotency_records,
        sequence: SequenceNumber::new(u64::from_le_bytes(fixed(bytes, 64, "domain_sequence")?)),
        publication_revision: PublicationRevision::new(u128::from_le_bytes(fixed(
            bytes,
            72,
            "publication_revision",
        )?)),
        events_pruned_through: SequenceNumber::new(u64::from_le_bytes(fixed(
            bytes,
            88,
            "events_pruned_through",
        )?)),
    };
    let engine = Engine::restore_snapshot_unindexed(snapshot)
        .map_err(|_| SnapshotError::InvalidEngineState)?;
    Ok(DecodedSnapshot {
        wal_watermark,
        engine,
    })
}

fn fixed<const N: usize>(
    bytes: &[u8],
    offset: usize,
    field: &'static str,
) -> Result<[u8; N], SnapshotError> {
    let end = offset
        .checked_add(N)
        .ok_or(SnapshotError::MalformedHeader(field))?;
    bytes
        .get(offset..end)
        .ok_or(SnapshotError::MalformedHeader(field))?
        .try_into()
        .map_err(|_| SnapshotError::MalformedHeader(field))
}

fn limit_usize(field: &'static str, value: u32) -> Result<usize, SnapshotError> {
    usize::try_from(value).map_err(|_| SnapshotError::Limit {
        field,
        value: u64::from(value),
        maximum: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
    })
}

fn top_count(
    reader: &mut Reader<'_>,
    field: &'static str,
    maximum: usize,
) -> Result<usize, SnapshotError> {
    let value = reader.count(field).map_err(map_codec)?;
    if value > maximum {
        return Err(SnapshotError::Limit {
            field,
            value: u64::try_from(value).unwrap_or(u64::MAX),
            maximum: u64::try_from(maximum).unwrap_or(u64::MAX),
        });
    }
    Ok(value)
}

fn reserved_vec<T>(field: &'static str, capacity: usize) -> Result<Vec<T>, SnapshotError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| SnapshotError::Allocation {
            field,
            requested: u64::try_from(capacity).unwrap_or(u64::MAX),
        })?;
    Ok(values)
}

fn map_codec(error: StorageError) -> SnapshotError {
    SnapshotError::MalformedPayload(error)
}
