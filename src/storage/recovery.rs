//! Generic WAL recovery scanning without engine application.

use std::io::Read;

use super::StorageError;
use super::record::{Record, RecordLimits, RecordReader};

/// Reads and validates all logical records in strict sequence order.
///
/// Recovery starts at [`super::record::RecordSequence::FIRST`], applies `limits` before
/// allocating each body, and stops at clean EOF. It deliberately returns opaque records;
/// command decoding and engine application are separate concerns.
///
/// # Errors
///
/// Returns the first I/O, partial-tail, size, structural, sequence, padding, or checksum
/// error. The scanner never attempts to resynchronize after malformed bytes.
pub fn recover<R: Read>(reader: R, limits: RecordLimits) -> Result<Vec<Record>, StorageError> {
    let mut reader = RecordReader::new(reader, limits);
    let mut records = Vec::new();
    while let Some(record) = reader.read_next()? {
        records.push(record);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::storage::RecordCorruption;
    use crate::storage::record::{RecordSequence, encode};

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
}
