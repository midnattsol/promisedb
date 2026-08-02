//! Canonical snapshot v1 encoding.

use crate::event::EventData;
use crate::storage::transition::Writer;

use super::format::{CHECKSUM_LEN, HEADER_LEN, MAGIC, VERSION};
use super::{SnapshotError, SnapshotFile, SnapshotLimits};

pub(crate) fn encode(
    file: &SnapshotFile,
    limits: SnapshotLimits,
) -> Result<Vec<u8>, SnapshotError> {
    let limits = limits.validate()?;
    let collection_limit = limit_usize("maximum collection items", limits.max_collection_items)?;
    let nested_limit = limit_usize("maximum nested items", limits.max_nested_items)?;
    let string_limit = limit_usize("maximum string bytes", limits.max_string_bytes)?;
    check_items(
        "resource pools",
        file.engine.resource_pools.len(),
        collection_limit,
    )?;
    check_items("promises", file.engine.promises.len(), collection_limit)?;
    check_items("events", file.engine.events.len(), collection_limit)?;
    check_items(
        "idempotency records",
        file.engine.idempotency_records.len(),
        collection_limit,
    )?;
    for pool in &file.engine.resource_pools {
        check_string("display name", pool.display_name(), string_limit)?;
        check_string("unit name", pool.unit().name(), string_limit)?;
        check_items(
            "capacity segments",
            pool.capacity_curve().segments().len(),
            nested_limit,
        )?;
    }
    for promise in &file.engine.promises {
        check_items(
            "bundle claims",
            promise.bundle().claims().len(),
            nested_limit,
        )?;
    }
    for event in &file.engine.events {
        if let EventData::Deficit {
            affected_promise_ids,
            ..
        } = event.data()
        {
            check_items(
                "affected promise ids",
                affected_promise_ids.len(),
                nested_limit,
            )?;
        }
    }
    for (client, key, _, _) in &file.engine.idempotency_records {
        check_string("client id", client.as_str(), string_limit)?;
        check_string("idempotency key", key.as_str(), string_limit)?;
    }

    let mut payload = Vec::new();
    payload
        .try_reserve_exact(16)
        .map_err(|_| SnapshotError::Allocation {
            field: "payload bytes",
            requested: 16,
        })?;
    {
        let mut w = Writer::new(&mut payload);
        w.len("resource pools", file.engine.resource_pools.len())
            .map_err(SnapshotError::MalformedPayload)?;
        for value in &file.engine.resource_pools {
            w.resource_pool(value)
                .map_err(SnapshotError::MalformedPayload)?;
        }
        w.len("promises", file.engine.promises.len())
            .map_err(SnapshotError::MalformedPayload)?;
        for value in &file.engine.promises {
            w.promise(value).map_err(SnapshotError::MalformedPayload)?;
        }
        w.len("events", file.engine.events.len())
            .map_err(SnapshotError::MalformedPayload)?;
        for value in &file.engine.events {
            w.event(value).map_err(SnapshotError::MalformedPayload)?;
        }
        w.len("idempotency records", file.engine.idempotency_records.len())
            .map_err(SnapshotError::MalformedPayload)?;
        for (client, key, hash, response) in &file.engine.idempotency_records {
            w.string("client id", client.as_str())
                .map_err(SnapshotError::MalformedPayload)?;
            w.string("idempotency key", key.as_str())
                .map_err(SnapshotError::MalformedPayload)?;
            w.raw(hash.as_bytes());
            w.response(response)
                .map_err(SnapshotError::MalformedPayload)?;
        }
    }
    let payload_len = u64::try_from(payload.len()).map_err(|_| SnapshotError::Limit {
        field: "payload bytes",
        value: u64::MAX,
        maximum: limits.max_total_bytes,
    })?;
    let total_len = u64::try_from(HEADER_LEN)
        .expect("fixed header length fits u64")
        .checked_add(payload_len)
        .and_then(|value| {
            value.checked_add(u64::try_from(CHECKSUM_LEN).expect("fixed checksum length fits u64"))
        })
        .ok_or(SnapshotError::Limit {
            field: "total bytes",
            value: u64::MAX,
            maximum: limits.max_total_bytes,
        })?;
    if total_len > limits.max_total_bytes {
        return Err(SnapshotError::Limit {
            field: "total bytes",
            value: total_len,
            maximum: limits.max_total_bytes,
        });
    }
    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[6..8].copy_from_slice(
        &u16::try_from(HEADER_LEN)
            .expect("fixed header length fits u16")
            .to_le_bytes(),
    );
    header[8..10].copy_from_slice(
        &u16::try_from(CHECKSUM_LEN)
            .expect("fixed checksum length fits u16")
            .to_le_bytes(),
    );
    header[16..24].copy_from_slice(&total_len.to_le_bytes());
    header[24..32].copy_from_slice(&payload_len.to_le_bytes());
    header[32..48].copy_from_slice(file.database_uuid.as_bytes());
    header[48..52].copy_from_slice(&file.semantics_version.to_le_bytes());
    header[56..64].copy_from_slice(&file.wal_watermark.to_le_bytes());
    header[64..72].copy_from_slice(&file.engine.sequence.get().to_le_bytes());
    header[72..88].copy_from_slice(&file.engine.publication_revision.get().to_le_bytes());
    header[88..96].copy_from_slice(&file.engine.events_pruned_through.get().to_le_bytes());
    let total_capacity = usize::try_from(total_len).map_err(|_| SnapshotError::Limit {
        field: "total bytes",
        value: total_len,
        maximum: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(total_capacity)
        .map_err(|_| SnapshotError::Allocation {
            field: "total bytes",
            requested: total_len,
        })?;
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&payload);
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    Ok(bytes)
}

fn limit_usize(field: &'static str, value: u32) -> Result<usize, SnapshotError> {
    usize::try_from(value).map_err(|_| SnapshotError::Limit {
        field,
        value: u64::from(value),
        maximum: u64::try_from(usize::MAX).unwrap_or(u64::MAX),
    })
}

fn check_items(field: &'static str, value: usize, maximum: usize) -> Result<(), SnapshotError> {
    if value > maximum {
        Err(SnapshotError::Limit {
            field,
            value: u64::try_from(value).unwrap_or(u64::MAX),
            maximum: u64::try_from(maximum).unwrap_or(u64::MAX),
        })
    } else {
        Ok(())
    }
}

fn check_string(field: &'static str, value: &str, maximum: usize) -> Result<(), SnapshotError> {
    if value.len() > maximum {
        Err(SnapshotError::Limit {
            field,
            value: u64::try_from(value.len()).unwrap_or(u64::MAX),
            maximum: u64::try_from(maximum).unwrap_or(u64::MAX),
        })
    } else {
        Ok(())
    }
}
