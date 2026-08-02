//! Versioned, bounded WAL record framing.

use std::io::Read;
use std::num::NonZeroU64;
use std::ops::Range;

use crate::domain::Timestamp;

use super::{RecordCorruption, StorageError};

/// Four-byte signature at the beginning of every PromiseDB WAL record.
pub const MAGIC: [u8; 4] = *b"PDBW";
/// Current WAL record format version.
pub const FORMAT_VERSION: u8 = 1;
/// Flags accepted by format version 1.
pub const FORMAT_FLAGS: u8 = 0;
/// Serialized width of the fixed WAL record header.
pub const HEADER_LEN: usize = 32;
/// Serialized width of the trailing BLAKE3 checksum.
pub const CHECKSUM_LEN: usize = 16;
/// Smallest possible serialized record.
pub const MIN_RECORD_LEN: u32 = 48;
/// Largest 8-byte-aligned length representable by the format's `u32` field.
pub const FORMAT_MAX_RECORD_LEN: u32 = !7;
/// Default maximum total record size (64 MiB).
pub const DEFAULT_MAX_RECORD_LEN: u32 = 64 * 1024 * 1024;
/// Width in bytes of a serialized `u32` length field.
pub const LENGTH_WIDTH: usize = size_of::<u32>();

/// Byte range containing [`MAGIC`].
pub const MAGIC_RANGE: Range<usize> = 0..4;
/// Byte offset containing [`FORMAT_VERSION`].
pub const FORMAT_VERSION_OFFSET: usize = 4;
/// Byte offset containing [`FORMAT_FLAGS`].
pub const FLAGS_OFFSET: usize = 5;
/// Byte range containing the little-endian fixed-header length.
pub const HEADER_LENGTH_RANGE: Range<usize> = 6..8;
/// Byte range containing the little-endian total record length.
pub const RECORD_LENGTH_RANGE: Range<usize> = 8..12;
/// Byte range containing the little-endian payload length.
pub const PAYLOAD_LENGTH_RANGE: Range<usize> = 12..16;
/// Byte range containing the little-endian record sequence.
pub const RECORD_SEQUENCE_RANGE: Range<usize> = 16..24;
/// Byte range containing the little-endian timestamp.
pub const TIMESTAMP_RANGE: Range<usize> = 24..32;

/// A non-zero position in the logical WAL record order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordSequence(NonZeroU64);

impl RecordSequence {
    /// Sequence assigned to the first WAL record.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Creates a record sequence, returning `None` for zero.
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric representation.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the following record sequence, or `None` on overflow.
    pub const fn next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Limits applied before allocating storage for a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLimits {
    max_record_len: u32,
}

impl RecordLimits {
    /// Creates limits with the given maximum total encoded record length.
    ///
    /// The limit must be at least 48 bytes, no greater than the format ceiling, and
    /// divisible by eight.
    pub const fn new(max_record_len: u32) -> Result<Self, StorageError> {
        if max_record_len < MIN_RECORD_LEN
            || max_record_len > FORMAT_MAX_RECORD_LEN
            || !max_record_len.is_multiple_of(8)
        {
            return Err(StorageError::InvalidRecordLimit(max_record_len as u64));
        }
        Ok(Self { max_record_len })
    }

    /// Returns the maximum permitted total encoded record length.
    pub const fn max_record_len(self) -> u32 {
        self.max_record_len
    }
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_record_len: DEFAULT_MAX_RECORD_LEN,
        }
    }
}

/// Logical content framed as one WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    record_sequence: RecordSequence,
    timestamp: Timestamp,
    payload: Vec<u8>,
}

impl Record {
    /// Creates a logical record around an owned opaque payload.
    pub fn new(record_sequence: RecordSequence, timestamp: Timestamp, payload: Vec<u8>) -> Self {
        Self {
            record_sequence,
            timestamp,
            payload,
        }
    }

    /// Returns this record's position in logical WAL order.
    pub fn record_sequence(&self) -> RecordSequence {
        self.record_sequence
    }

    /// Returns the deterministic timestamp associated with this record.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the opaque record payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes the record and returns its opaque payload.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Encodes one record using format version 1.
///
/// The checksum is the first 16 bytes of BLAKE3 over the contiguous header, payload,
/// and zero padding. The checksum itself is excluded.
///
/// # Errors
///
/// Returns an error if the payload cannot be represented by the format or the total
/// record length exceeds `limits`.
pub fn encode(record: &Record, limits: RecordLimits) -> Result<Vec<u8>, StorageError> {
    let payload_len =
        u32::try_from(record.payload.len()).map_err(|_| StorageError::InvalidLength {
            field: "record payload",
            length: u64::try_from(record.payload.len()).unwrap_or(u64::MAX),
        })?;
    let padding_len = padding_len(payload_len);
    let record_len = MIN_RECORD_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(padding_len as u32))
        .filter(|length| *length <= FORMAT_MAX_RECORD_LEN)
        .ok_or(StorageError::InvalidLength {
            field: "WAL record",
            length: u64::from(payload_len) + u64::from(MIN_RECORD_LEN),
        })?;
    if record_len > limits.max_record_len {
        return Err(StorageError::RecordTooLarge {
            offset: 0,
            length: u64::from(record_len),
            max: limits.max_record_len,
        });
    }

    let capacity = usize::try_from(record_len).map_err(|_| StorageError::InvalidLength {
        field: "WAL record",
        length: u64::from(record_len),
    })?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(capacity)
        .map_err(|_| StorageError::InvalidLength {
            field: "WAL record allocation",
            length: u64::from(record_len),
        })?;
    encoded.extend_from_slice(&MAGIC);
    encoded.push(FORMAT_VERSION);
    encoded.push(FORMAT_FLAGS);
    encoded.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    encoded.extend_from_slice(&record_len.to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&record.record_sequence.get().to_le_bytes());
    encoded.extend_from_slice(&record.timestamp.to_le_bytes());
    encoded.extend_from_slice(&record.payload);
    encoded.resize(encoded.len() + padding_len, 0);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(&checksum.as_bytes()[..CHECKSUM_LEN]);
    debug_assert_eq!(encoded.len(), capacity);
    Ok(encoded)
}

/// A bounded, non-resynchronizing reader for a sequence of WAL records.
#[derive(Debug)]
pub struct RecordReader<R> {
    reader: R,
    limits: RecordLimits,
    offset: u64,
    expected_sequence: Option<RecordSequence>,
}

impl<R: Read> RecordReader<R> {
    /// Creates a reader positioned at offset zero and expecting [`RecordSequence::FIRST`].
    pub fn new(reader: R, limits: RecordLimits) -> Self {
        Self::with_expected_sequence(reader, limits, RecordSequence::FIRST)
    }

    /// Creates a reader positioned at offset zero and expecting an explicit sequence.
    pub fn with_expected_sequence(
        reader: R,
        limits: RecordLimits,
        expected_sequence: RecordSequence,
    ) -> Self {
        Self {
            reader,
            limits,
            offset: 0,
            expected_sequence: Some(expected_sequence),
        }
    }

    /// Returns the byte offset at the next record boundary.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the next expected sequence, or `None` after sequence exhaustion.
    pub fn expected_sequence(&self) -> Option<RecordSequence> {
        self.expected_sequence
    }

    /// Returns the wrapped reader.
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// Reads and validates the next record without attempting resynchronization.
    ///
    /// Clean EOF is returned only when zero bytes are read at a record boundary. Any
    /// shorter prefix is a [`StorageError::PartialTail`]. Size limits and structural
    /// header fields are checked before allocating the record body.
    pub fn read_next(&mut self) -> Result<Option<Record>, StorageError> {
        let record_offset = self.offset;
        let mut header = [0_u8; HEADER_LEN];
        let header_read = read_fully(&mut self.reader, &mut header)?;
        if header_read == 0 {
            return Ok(None);
        }
        if header_read != HEADER_LEN {
            return Err(StorageError::PartialTail {
                offset: record_offset,
                expected: HEADER_LEN as u64,
                actual: header_read as u64,
            });
        }

        let parsed = parse_header(&header, record_offset, self.limits, self.expected_sequence)?;
        let remaining_len = usize::try_from(parsed.record_len)
            .expect("u32 record length must fit usize")
            - HEADER_LEN;
        let mut remainder = Vec::new();
        remainder
            .try_reserve_exact(remaining_len)
            .map_err(|_| StorageError::InvalidLength {
                field: "WAL record allocation",
                length: u64::from(parsed.record_len),
            })?;
        remainder.resize(remaining_len, 0);
        let remainder_read = read_fully(&mut self.reader, &mut remainder)?;
        if remainder_read != remaining_len {
            return Err(StorageError::PartialTail {
                offset: record_offset,
                expected: u64::from(parsed.record_len),
                actual: (HEADER_LEN + remainder_read) as u64,
            });
        }

        let payload_len = parsed.payload_len as usize;
        let padding_len = padding_len(parsed.payload_len);
        for (index, value) in remainder[payload_len..payload_len + padding_len]
            .iter()
            .copied()
            .enumerate()
        {
            if value != 0 {
                return Err(corrupt(
                    record_offset,
                    RecordCorruption::NonZeroPadding {
                        index: index as u8,
                        value,
                    },
                ));
            }
        }

        let checksum_start = payload_len + padding_len;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&header);
        hasher.update(&remainder[..checksum_start]);
        let checksum = hasher.finalize();
        if remainder[checksum_start..] != checksum.as_bytes()[..CHECKSUM_LEN] {
            return Err(corrupt(record_offset, RecordCorruption::ChecksumMismatch));
        }

        remainder.truncate(payload_len);
        self.offset = self
            .offset
            .checked_add(u64::from(parsed.record_len))
            .expect("u32 record length cannot overflow a u64 WAL offset");
        self.expected_sequence = parsed.sequence.next();
        Ok(Some(Record::new(
            parsed.sequence,
            parsed.timestamp,
            remainder,
        )))
    }
}

#[derive(Debug)]
struct ParsedHeader {
    record_len: u32,
    payload_len: u32,
    sequence: RecordSequence,
    timestamp: Timestamp,
}

fn parse_header(
    header: &[u8; HEADER_LEN],
    offset: u64,
    limits: RecordLimits,
    expected_sequence: Option<RecordSequence>,
) -> Result<ParsedHeader, StorageError> {
    let magic: [u8; 4] = header[MAGIC_RANGE].try_into().expect("fixed range");
    if magic != MAGIC {
        return Err(corrupt(offset, RecordCorruption::InvalidMagic(magic)));
    }
    let version = header[FORMAT_VERSION_OFFSET];
    if version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedRecordVersion { offset, version });
    }
    let flags = header[FLAGS_OFFSET];
    if flags != FORMAT_FLAGS {
        return Err(corrupt(offset, RecordCorruption::UnsupportedFlags(flags)));
    }
    let header_len =
        u16::from_le_bytes(header[HEADER_LENGTH_RANGE].try_into().expect("fixed range"));
    if usize::from(header_len) != HEADER_LEN {
        return Err(corrupt(
            offset,
            RecordCorruption::InvalidHeaderLength(header_len),
        ));
    }
    let record_len =
        u32::from_le_bytes(header[RECORD_LENGTH_RANGE].try_into().expect("fixed range"));
    if !(MIN_RECORD_LEN..=FORMAT_MAX_RECORD_LEN).contains(&record_len)
        || !record_len.is_multiple_of(8)
    {
        return Err(corrupt(
            offset,
            RecordCorruption::InvalidRecordLength(record_len),
        ));
    }
    if record_len > limits.max_record_len {
        return Err(StorageError::RecordTooLarge {
            offset,
            length: u64::from(record_len),
            max: limits.max_record_len,
        });
    }
    let payload_len = u32::from_le_bytes(
        header[PAYLOAD_LENGTH_RANGE]
            .try_into()
            .expect("fixed range"),
    );
    let expected_len = MIN_RECORD_LEN
        .checked_add(payload_len)
        .and_then(|length| length.checked_add(padding_len(payload_len) as u32));
    if expected_len != Some(record_len) {
        return Err(corrupt(
            offset,
            RecordCorruption::InvalidPayloadLength {
                record_len,
                payload_len,
            },
        ));
    }
    let sequence_value = u64::from_le_bytes(
        header[RECORD_SEQUENCE_RANGE]
            .try_into()
            .expect("fixed range"),
    );
    let sequence = RecordSequence::new(sequence_value)
        .ok_or_else(|| corrupt(offset, RecordCorruption::InvalidSequence(sequence_value)))?;
    match expected_sequence {
        Some(expected) if sequence != expected => {
            return Err(corrupt(
                offset,
                RecordCorruption::SequenceMismatch {
                    expected: expected.get(),
                    actual: sequence.get(),
                },
            ));
        }
        None => return Err(corrupt(offset, RecordCorruption::SequenceOverflow)),
        Some(_) => {}
    }
    let timestamp = i64::from_le_bytes(header[TIMESTAMP_RANGE].try_into().expect("fixed range"));
    Ok(ParsedHeader {
        record_len,
        payload_len,
        sequence,
        timestamp,
    })
}

const fn padding_len(payload_len: u32) -> usize {
    ((8 - payload_len % 8) % 8) as usize
}

fn corrupt(offset: u64, reason: RecordCorruption) -> StorageError {
    StorageError::CorruptWalRecord { offset, reason }
}

fn read_fully(reader: &mut impl Read, buffer: &mut [u8]) -> Result<usize, StorageError> {
    let mut read = 0;
    while read < buffer.len() {
        match reader.read(&mut buffer[read..]) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor, Read};

    use super::*;

    fn record(sequence: u64, payload: &[u8]) -> Record {
        Record::new(
            RecordSequence::new(sequence).unwrap(),
            -1_234_567_890,
            payload.to_vec(),
        )
    }

    fn encoded(sequence: u64, payload: &[u8]) -> Vec<u8> {
        encode(&record(sequence, payload), RecordLimits::default()).unwrap()
    }

    fn resign(bytes: &mut [u8]) {
        let checksum_start = bytes.len() - CHECKSUM_LEN;
        let checksum = blake3::hash(&bytes[..checksum_start]);
        bytes[checksum_start..].copy_from_slice(&checksum.as_bytes()[..CHECKSUM_LEN]);
    }

    #[test]
    fn golden_layout_and_checksum_are_stable() {
        let bytes = encoded(1, &[0xaa, 0xbb, 0xcc]);
        assert_eq!(bytes.len(), 56);
        assert_eq!(&bytes[0..4], b"PDBW");
        assert_eq!(bytes[4], 1);
        assert_eq!(bytes[5], 0);
        assert_eq!(&bytes[6..8], &32_u16.to_le_bytes());
        assert_eq!(&bytes[8..12], &56_u32.to_le_bytes());
        assert_eq!(&bytes[12..16], &3_u32.to_le_bytes());
        assert_eq!(&bytes[16..24], &1_u64.to_le_bytes());
        assert_eq!(&bytes[24..32], &(-1_234_567_890_i64).to_le_bytes());
        assert_eq!(&bytes[32..35], &[0xaa, 0xbb, 0xcc]);
        assert_eq!(&bytes[35..40], &[0; 5]);
        assert_eq!(
            &bytes[40..56],
            &[
                0x45, 0x94, 0x7f, 0x40, 0xdf, 0x44, 0x19, 0x07, 0x53, 0x1a, 0xbd, 0xc7, 0x6c, 0xc4,
                0xba, 0xd8,
            ]
        );
    }

    #[test]
    fn every_truncated_prefix_is_a_partial_tail_and_empty_is_clean_eof() {
        let bytes = encoded(1, b"truncation coverage");
        for length in 0..bytes.len() {
            let mut reader =
                RecordReader::new(Cursor::new(&bytes[..length]), RecordLimits::default());
            if length == 0 {
                assert_eq!(reader.read_next(), Ok(None));
            } else {
                assert!(matches!(
                    reader.read_next(),
                    Err(StorageError::PartialTail { offset: 0, actual, .. }) if actual == length as u64
                ));
            }
        }
        let mut reader = RecordReader::new(Cursor::new(bytes), RecordLimits::default());
        assert!(reader.read_next().unwrap().is_some());
        assert_eq!(reader.read_next(), Ok(None));
    }

    struct ShortReader<R> {
        inner: R,
        max: usize,
        interrupt_once: bool,
    }

    impl<R: Read> Read for ShortReader<R> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.interrupt_once {
                self.interrupt_once = false;
                return Err(io::ErrorKind::Interrupted.into());
            }
            let limit = buffer.len().min(self.max);
            self.inner.read(&mut buffer[..limit])
        }
    }

    #[test]
    fn handles_short_reads_and_interrupted_reads() {
        let expected = record(1, b"opaque");
        let bytes = encode(&expected, RecordLimits::default()).unwrap();
        let source = ShortReader {
            inner: Cursor::new(bytes),
            max: 1,
            interrupt_once: true,
        };
        let mut reader = RecordReader::new(source, RecordLimits::default());
        assert_eq!(reader.read_next(), Ok(Some(expected)));
    }

    #[test]
    fn detects_checksum_corruption_in_header_and_payload() {
        for index in [24, 32] {
            let mut bytes = encoded(1, b"payload");
            bytes[index] ^= 0x80;
            let mut reader = RecordReader::new(Cursor::new(bytes), RecordLimits::default());
            assert_eq!(
                reader.read_next(),
                Err(corrupt(0, RecordCorruption::ChecksumMismatch))
            );
        }
        let mut bytes = encoded(1, b"payload");
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let mut reader = RecordReader::new(Cursor::new(bytes), RecordLimits::default());
        assert_eq!(
            reader.read_next(),
            Err(corrupt(0, RecordCorruption::ChecksumMismatch))
        );
    }

    #[test]
    fn detects_structural_header_and_padding_corruption() {
        let cases = [
            (0, RecordCorruption::InvalidMagic(*b"QDBW")),
            (5, RecordCorruption::UnsupportedFlags(1)),
        ];
        for (index, reason) in cases {
            let mut bytes = encoded(1, b"abc");
            bytes[index] ^= 1;
            let mut reader = RecordReader::new(Cursor::new(bytes), RecordLimits::default());
            assert_eq!(reader.read_next(), Err(corrupt(0, reason)));
        }

        let mut bytes = encoded(1, b"abc");
        bytes[35] = 7;
        resign(&mut bytes);
        let mut reader = RecordReader::new(Cursor::new(bytes), RecordLimits::default());
        assert_eq!(
            reader.read_next(),
            Err(corrupt(
                0,
                RecordCorruption::NonZeroPadding { index: 0, value: 7 }
            ))
        );
    }

    #[test]
    fn distinguishes_record_versions_and_sequence_errors() {
        let mut version = encoded(1, b"");
        version[4] = 2;
        let mut reader = RecordReader::new(Cursor::new(version), RecordLimits::default());
        assert_eq!(
            reader.read_next(),
            Err(StorageError::UnsupportedRecordVersion {
                offset: 0,
                version: 2
            })
        );

        for (actual, expected) in [(2, 1), (1, 2)] {
            let bytes = encoded(actual, b"");
            let mut reader = RecordReader::with_expected_sequence(
                Cursor::new(bytes),
                RecordLimits::default(),
                RecordSequence::new(expected).unwrap(),
            );
            assert_eq!(
                reader.read_next(),
                Err(corrupt(
                    0,
                    RecordCorruption::SequenceMismatch { expected, actual }
                ))
            );
        }
    }

    #[test]
    fn tracks_offsets_and_sequences_across_records() {
        let first = encoded(1, b"one");
        let second = encoded(2, b"two");
        let first_len = first.len() as u64;
        let mut wal = first;
        wal.extend_from_slice(&second);
        let mut reader = RecordReader::new(Cursor::new(wal), RecordLimits::default());
        assert_eq!(reader.read_next().unwrap().unwrap().payload(), b"one");
        assert_eq!(reader.offset(), first_len);
        assert_eq!(reader.expected_sequence().unwrap().get(), 2);
        assert_eq!(reader.read_next().unwrap().unwrap().payload(), b"two");
        assert_eq!(reader.read_next(), Ok(None));
    }

    #[test]
    fn validates_limits_and_rejects_declared_size_before_body_allocation() {
        assert_eq!(
            RecordLimits::new(47),
            Err(StorageError::InvalidRecordLimit(47))
        );
        assert_eq!(
            RecordLimits::new(49),
            Err(StorageError::InvalidRecordLimit(49))
        );
        assert!(RecordLimits::new(FORMAT_MAX_RECORD_LEN).is_ok());
        assert_eq!(RecordLimits::default().max_record_len(), 64 * 1024 * 1024);

        let limits = RecordLimits::new(MIN_RECORD_LEN).unwrap();
        assert_eq!(
            encode(&record(1, b"x"), limits),
            Err(StorageError::RecordTooLarge {
                offset: 0,
                length: 56,
                max: 48
            })
        );

        let mut header = [0_u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC);
        header[4] = FORMAT_VERSION;
        header[6..8].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
        header[8..12].copy_from_slice(&56_u32.to_le_bytes());
        header[12..16].copy_from_slice(&1_u32.to_le_bytes());
        header[16..24].copy_from_slice(&1_u64.to_le_bytes());
        let mut reader = RecordReader::new(Cursor::new(header), limits);
        assert_eq!(
            reader.read_next(),
            Err(StorageError::RecordTooLarge {
                offset: 0,
                length: 56,
                max: 48
            })
        );
    }

    #[test]
    fn public_layout_constants_match_version_one() {
        assert_eq!(LENGTH_WIDTH, 4);
        assert_eq!(MAGIC_RANGE, 0..4);
        assert_eq!(FORMAT_VERSION_OFFSET, 4);
        assert_eq!(FLAGS_OFFSET, 5);
        assert_eq!(HEADER_LENGTH_RANGE, 6..8);
        assert_eq!(RECORD_LENGTH_RANGE, 8..12);
        assert_eq!(PAYLOAD_LENGTH_RANGE, 12..16);
        assert_eq!(RECORD_SEQUENCE_RANGE, 16..24);
        assert_eq!(TIMESTAMP_RANGE, 24..32);
        assert_eq!(RecordSequence::new(0), None);
        assert_eq!(RecordSequence::FIRST.next().unwrap().get(), 2);
        assert_eq!(RecordSequence::new(u64::MAX).unwrap().next(), None);
    }
}
