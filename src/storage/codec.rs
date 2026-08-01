//! Deterministic, versioned command payload codec.

use crate::command::{ClientId, Command, CommandOperation, IdempotencyKey};
use crate::domain::{
    Bundle, CapacityCurve, CapacitySegment, Choice, Claim, Interval, PromiseId, RelativeBundle,
    RelativeClaim, ReplacementState, ResourcePoolId, Unit, Version,
};
use crate::engine::CapacityRevisionMode;

use super::StorageError;

/// Current command payload format version.
///
/// Version 1 is defined to use little-endian byte order for every fixed-width
/// integer and length field.
pub const COMMAND_FORMAT_VERSION: u8 = 1;

const CREATE_POOL: u8 = 1;
const REVISE_CAPACITY: u8 = 2;
const HOLD: u8 = 3;
const COMMIT: u8 = 4;
const RELEASE: u8 = 5;
const REPLACE: u8 = 6;
const PROCESS_EXPIRATIONS: u8 = 7;
const HOLD_ONE_OF: u8 = 8;
const HOLD_FIRST_SLOT: u8 = 9;

/// Encodes a complete command into its deterministic binary payload.
///
/// Version 1 uses explicit one-byte tags, little-endian fixed-width integers,
/// little-endian four-byte length prefixes, and stable UUID bytes. Bundle claim
/// order and choice alternative order are preserved exactly.
///
/// # Errors
///
/// Returns [`StorageError::InvalidLength`] if a string or collection exceeds the
/// format's `u32` length limit.
pub fn encode_command(command: &Command) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    encode_command_into(command, &mut bytes)?;
    Ok(bytes)
}

/// Appends a complete command to an existing destination buffer.
///
/// # Errors
///
/// Returns [`StorageError::InvalidLength`] if a string or collection exceeds the
/// format's `u32` length limit.
pub(super) fn encode_command_into(
    command: &Command,
    destination: &mut Vec<u8>,
) -> Result<(), StorageError> {
    let mut writer = Writer::new(destination);
    writer.byte(COMMAND_FORMAT_VERSION);
    writer.string("client_id", command.client_id().as_str())?;
    writer.string("idempotency_key", command.idempotency_key().as_str())?;
    writer.operation(command.operation())
}

/// Decodes a complete deterministic command payload.
///
/// Domain values are rebuilt through their validated constructors. Trailing bytes
/// are rejected rather than silently ignored.
///
/// # Errors
///
/// Returns a structured [`StorageError`] for unsupported versions, malformed or
/// truncated data, invalid UTF-8, and violated domain invariants.
pub fn decode_command(bytes: &[u8]) -> Result<Command, StorageError> {
    let mut reader = Reader::new(bytes);
    let version = reader.byte()?;
    if version != COMMAND_FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }

    let client_id = ClientId::new(reader.string("client_id")?);
    let idempotency_key = IdempotencyKey::new(reader.string("idempotency_key")?);
    let operation = reader.operation()?;
    if !reader.is_empty() {
        return Err(StorageError::CorruptRecord(
            "trailing command payload bytes",
        ));
    }
    Ok(Command::new(client_id, idempotency_key, operation))
}

struct Writer<'a> {
    bytes: &'a mut Vec<u8>,
}

impl<'a> Writer<'a> {
    fn new(bytes: &'a mut Vec<u8>) -> Self {
        Self { bytes }
    }
    fn byte(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn len(&mut self, field: &'static str, length: usize) -> Result<(), StorageError> {
        let length = u32::try_from(length).map_err(|_| StorageError::InvalidLength {
            field,
            length: length as u64,
        })?;
        self.u32(length);
        Ok(())
    }

    fn string(&mut self, field: &'static str, value: &str) -> Result<(), StorageError> {
        self.len(field, value.len())?;
        self.bytes.extend_from_slice(value.as_bytes());
        Ok(())
    }

    fn pool_id(&mut self, id: ResourcePoolId) {
        self.bytes.extend_from_slice(&id.as_bytes());
    }

    fn promise_id(&mut self, id: PromiseId) {
        self.bytes.extend_from_slice(&id.as_bytes());
    }

    fn interval(&mut self, interval: Interval) {
        self.i64(interval.start());
        self.i64(interval.end());
    }

    fn unit(&mut self, unit: &Unit) -> Result<(), StorageError> {
        self.string("unit name", unit.name())?;
        self.u64(unit.subunits_per_unit());
        Ok(())
    }

    fn curve(&mut self, curve: &CapacityCurve) -> Result<(), StorageError> {
        self.len("capacity segments", curve.segments().len())?;
        for segment in curve.segments() {
            self.interval(segment.interval());
            self.u64(segment.capacity());
        }
        Ok(())
    }

    fn claim(&mut self, claim: &Claim) {
        self.pool_id(claim.pool_id());
        self.interval(claim.interval());
        self.u64(claim.quantity());
    }

    fn bundle(&mut self, bundle: &Bundle) -> Result<(), StorageError> {
        self.len("bundle claims", bundle.claims().len())?;
        for claim in bundle.claims() {
            self.claim(claim);
        }
        Ok(())
    }

    fn choice(&mut self, choice: &Choice) -> Result<(), StorageError> {
        self.len("choice alternatives", choice.alternatives().len())?;
        for bundle in choice.alternatives() {
            self.bundle(bundle)?;
        }
        Ok(())
    }

    fn relative_bundle(&mut self, bundle: &RelativeBundle) -> Result<(), StorageError> {
        self.len("relative bundle claims", bundle.claims().len())?;
        for claim in bundle.claims() {
            self.pool_id(claim.pool_id());
            self.i64(claim.start_offset());
            self.i64(claim.end_offset());
            self.u64(claim.quantity());
        }
        Ok(())
    }

    fn operation(&mut self, operation: &CommandOperation) -> Result<(), StorageError> {
        match operation {
            CommandOperation::CreateResourcePool {
                resource_pool_id,
                display_name,
                unit,
                capacity_curve,
            } => {
                self.byte(CREATE_POOL);
                self.pool_id(*resource_pool_id);
                self.string("display name", display_name)?;
                self.unit(unit)?;
                self.curve(capacity_curve)?;
            }
            CommandOperation::ReviseCapacity {
                resource_pool_id,
                capacity_curve,
                mode,
            } => {
                self.byte(REVISE_CAPACITY);
                self.pool_id(*resource_pool_id);
                self.curve(capacity_curve)?;
                self.byte(match mode {
                    CapacityRevisionMode::Strict => 1,
                    CapacityRevisionMode::Force => 2,
                });
            }
            CommandOperation::Hold {
                promise_id,
                bundle,
                expires_at,
            } => {
                self.byte(HOLD);
                self.promise_id(*promise_id);
                self.bundle(bundle)?;
                self.i64(*expires_at);
            }
            CommandOperation::HoldOneOf {
                promise_id,
                choice,
                expires_at,
            } => {
                self.byte(HOLD_ONE_OF);
                self.promise_id(*promise_id);
                self.choice(choice)?;
                self.i64(*expires_at);
            }
            CommandOperation::HoldFirstSlot {
                promise_id,
                relative_bundle,
                earliest_start,
                latest_start,
                step,
                expires_at,
            } => {
                self.byte(HOLD_FIRST_SLOT);
                self.promise_id(*promise_id);
                self.relative_bundle(relative_bundle)?;
                self.i64(*earliest_start);
                self.i64(*latest_start);
                self.i64(*step);
                self.i64(*expires_at);
            }
            CommandOperation::Commit {
                promise_id,
                expected_version,
            } => {
                self.byte(COMMIT);
                self.promise_id(*promise_id);
                self.u64(expected_version.get());
            }
            CommandOperation::Release {
                promise_id,
                expected_version,
            } => {
                self.byte(RELEASE);
                self.promise_id(*promise_id);
                self.u64(expected_version.get());
            }
            CommandOperation::Replace {
                promise_id,
                expected_version,
                new_bundle,
                new_state,
            } => {
                self.byte(REPLACE);
                self.promise_id(*promise_id);
                self.u64(expected_version.get());
                self.bundle(new_bundle)?;
                match new_state {
                    ReplacementState::Held { expires_at } => {
                        self.byte(1);
                        self.i64(*expires_at);
                    }
                    ReplacementState::Committed => self.byte(2),
                }
            }
            CommandOperation::ProcessExpirations => self.byte(PROCESS_EXPIRATIONS),
        }
        Ok(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(StorageError::TruncatedPayload)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StorageError::TruncatedPayload)?;
        self.offset = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StorageError> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, StorageError> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn i64(&mut self) -> Result<i64, StorageError> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(bytes))
    }

    fn count(&mut self, field: &'static str) -> Result<usize, StorageError> {
        usize::try_from(self.u32()?).map_err(|_| StorageError::InvalidLength {
            field,
            length: u64::from(u32::MAX),
        })
    }

    fn string(&mut self, field: &'static str) -> Result<String, StorageError> {
        let length = self.count(field)?;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| StorageError::InvalidUtf8)
    }

    fn pool_id(&mut self) -> Result<ResourcePoolId, StorageError> {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(self.take(16)?);
        Ok(ResourcePoolId::from_bytes(bytes))
    }

    fn promise_id(&mut self) -> Result<PromiseId, StorageError> {
        let mut bytes = [0; 16];
        bytes.copy_from_slice(self.take(16)?);
        Ok(PromiseId::from_bytes(bytes))
    }

    fn version(&mut self) -> Result<Version, StorageError> {
        let value = self.u64()?;
        Version::new(value).ok_or(StorageError::CorruptRecord(
            "promise version must be non-zero",
        ))
    }

    fn interval(&mut self) -> Result<Interval, StorageError> {
        Ok(Interval::new(self.i64()?, self.i64()?)?)
    }

    fn unit(&mut self) -> Result<Unit, StorageError> {
        Ok(Unit::new(self.string("unit name")?, self.u64()?)?)
    }

    fn curve(&mut self) -> Result<CapacityCurve, StorageError> {
        let count = self.count("capacity segments")?;
        let mut segments = Vec::with_capacity(count.min(self.bytes.len() - self.offset));
        for _ in 0..count {
            segments.push(CapacitySegment::new(self.interval()?, self.u64()?));
        }
        Ok(CapacityCurve::from_sorted(segments)?)
    }

    fn claim(&mut self) -> Result<Claim, StorageError> {
        Ok(Claim::new(self.pool_id()?, self.interval()?, self.u64()?)?)
    }

    fn bundle(&mut self) -> Result<Bundle, StorageError> {
        let count = self.count("bundle claims")?;
        let mut claims = Vec::with_capacity(count.min(self.bytes.len() - self.offset));
        for _ in 0..count {
            claims.push(self.claim()?);
        }
        Ok(Bundle::new(claims)?)
    }

    fn choice(&mut self) -> Result<Choice, StorageError> {
        let count = self.count("choice alternatives")?;
        let mut alternatives = Vec::with_capacity(count.min(self.bytes.len() - self.offset));
        for _ in 0..count {
            alternatives.push(self.bundle()?);
        }
        Ok(Choice::new(alternatives)?)
    }

    fn relative_bundle(&mut self) -> Result<RelativeBundle, StorageError> {
        let count = self.count("relative bundle claims")?;
        let mut claims = Vec::with_capacity(count.min(self.bytes.len() - self.offset));
        for _ in 0..count {
            claims.push(RelativeClaim::new(
                self.pool_id()?,
                self.i64()?,
                self.i64()?,
                self.u64()?,
            )?);
        }
        Ok(RelativeBundle::new(claims)?)
    }

    fn operation(&mut self) -> Result<CommandOperation, StorageError> {
        let tag = self.byte()?;
        match tag {
            CREATE_POOL => Ok(CommandOperation::CreateResourcePool {
                resource_pool_id: self.pool_id()?,
                display_name: self.string("display name")?,
                unit: self.unit()?,
                capacity_curve: self.curve()?,
            }),
            REVISE_CAPACITY => {
                let resource_pool_id = self.pool_id()?;
                let capacity_curve = self.curve()?;
                let mode = match self.byte()? {
                    1 => CapacityRevisionMode::Strict,
                    2 => CapacityRevisionMode::Force,
                    tag => {
                        return Err(StorageError::InvalidTag {
                            kind: "capacity revision mode",
                            tag,
                        });
                    }
                };
                Ok(CommandOperation::ReviseCapacity {
                    resource_pool_id,
                    capacity_curve,
                    mode,
                })
            }
            HOLD => Ok(CommandOperation::Hold {
                promise_id: self.promise_id()?,
                bundle: self.bundle()?,
                expires_at: self.i64()?,
            }),
            HOLD_ONE_OF => Ok(CommandOperation::HoldOneOf {
                promise_id: self.promise_id()?,
                choice: self.choice()?,
                expires_at: self.i64()?,
            }),
            HOLD_FIRST_SLOT => Ok(CommandOperation::HoldFirstSlot {
                promise_id: self.promise_id()?,
                relative_bundle: self.relative_bundle()?,
                earliest_start: self.i64()?,
                latest_start: self.i64()?,
                step: self.i64()?,
                expires_at: self.i64()?,
            }),
            COMMIT => Ok(CommandOperation::Commit {
                promise_id: self.promise_id()?,
                expected_version: self.version()?,
            }),
            RELEASE => Ok(CommandOperation::Release {
                promise_id: self.promise_id()?,
                expected_version: self.version()?,
            }),
            REPLACE => {
                let promise_id = self.promise_id()?;
                let expected_version = self.version()?;
                let new_bundle = self.bundle()?;
                let new_state = match self.byte()? {
                    1 => ReplacementState::Held {
                        expires_at: self.i64()?,
                    },
                    2 => ReplacementState::Committed,
                    tag => {
                        return Err(StorageError::InvalidTag {
                            kind: "replacement state",
                            tag,
                        });
                    }
                };
                Ok(CommandOperation::Replace {
                    promise_id,
                    expected_version,
                    new_bundle,
                    new_state,
                })
            }
            PROCESS_EXPIRATIONS => Ok(CommandOperation::ProcessExpirations),
            tag => Err(StorageError::InvalidTag {
                kind: "command operation",
                tag,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DomainError;

    fn pool(byte: u8) -> ResourcePoolId {
        ResourcePoolId::from_bytes([byte; 16])
    }

    fn promise(byte: u8) -> PromiseId {
        PromiseId::from_bytes([byte; 16])
    }

    fn version(value: u64) -> Version {
        Version::new(value).unwrap()
    }

    fn claim(pool_id: ResourcePoolId, start: i64, end: i64, quantity: u64) -> Claim {
        Claim::new(pool_id, Interval::new(start, end).unwrap(), quantity).unwrap()
    }

    fn bundle() -> Bundle {
        Bundle::new(vec![claim(pool(2), 20, 30, 4), claim(pool(1), 10, 15, 3)]).unwrap()
    }

    fn curve() -> CapacityCurve {
        CapacityCurve::from_sorted(vec![
            CapacitySegment::new(Interval::new(-10, 0).unwrap(), 5),
            CapacitySegment::new(Interval::new(10, 20).unwrap(), 8),
        ])
        .unwrap()
    }

    fn command(operation: CommandOperation) -> Command {
        Command::new(
            ClientId::new("client-a"),
            IdempotencyKey::new("request-1"),
            operation,
        )
    }

    #[test]
    fn command_encoding_is_byte_exact_little_endian_and_appends_to_destination() {
        let expected = vec![
            1, // format version
            8,
            0,
            0,
            0,
            b'c',
            b'l',
            b'i',
            b'e',
            b'n',
            b't',
            b'-',
            b'a',
            9,
            0,
            0,
            0,
            b'r',
            b'e',
            b'q',
            b'u',
            b'e',
            b's',
            b't',
            b'-',
            b'1',
            PROCESS_EXPIRATIONS,
        ];
        let command = command(CommandOperation::ProcessExpirations);

        assert_eq!(encode_command(&command).unwrap(), expected);

        let mut destination = vec![0xaa];
        encode_command_into(&command, &mut destination).unwrap();
        assert_eq!(destination[0], 0xaa);
        assert_eq!(&destination[1..], expected);
    }

    #[test]
    fn fixed_width_values_are_byte_exact_little_endian() {
        let mut bytes = Vec::new();
        let mut writer = Writer::new(&mut bytes);
        writer.u32(0x0102_0304);
        writer.u64(0x0102_0304_0506_0708);
        writer.i64(0x0102_0304_0506_0708);

        assert_eq!(
            bytes,
            [
                0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x08, 0x07,
                0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            ]
        );
    }

    #[test]
    fn every_command_variant_round_trips() {
        let commands = vec![
            command(CommandOperation::CreateResourcePool {
                resource_pool_id: pool(1),
                display_name: "Primary pool".into(),
                unit: Unit::new("widgets".into(), 1_000).unwrap(),
                capacity_curve: curve(),
            }),
            command(CommandOperation::ReviseCapacity {
                resource_pool_id: pool(1),
                capacity_curve: curve(),
                mode: CapacityRevisionMode::Force,
            }),
            command(CommandOperation::Hold {
                promise_id: promise(1),
                bundle: bundle(),
                expires_at: 50,
            }),
            command(CommandOperation::HoldOneOf {
                promise_id: promise(2),
                choice: Choice::new(vec![
                    Bundle::new(vec![claim(pool(1), 1, 2, 1)]).unwrap(),
                    Bundle::new(vec![claim(pool(2), 3, 4, 2)]).unwrap(),
                ])
                .unwrap(),
                expires_at: 60,
            }),
            command(CommandOperation::HoldFirstSlot {
                promise_id: promise(3),
                relative_bundle: RelativeBundle::new(vec![
                    RelativeClaim::new(pool(2), -2, 3, 4).unwrap(),
                    RelativeClaim::new(pool(1), 0, 5, 2).unwrap(),
                ])
                .unwrap(),
                earliest_start: 100,
                latest_start: 200,
                step: 10,
                expires_at: 80,
            }),
            command(CommandOperation::Commit {
                promise_id: promise(4),
                expected_version: version(2),
            }),
            command(CommandOperation::Release {
                promise_id: promise(5),
                expected_version: version(3),
            }),
            command(CommandOperation::Replace {
                promise_id: promise(6),
                expected_version: version(4),
                new_bundle: bundle(),
                new_state: ReplacementState::Held { expires_at: 90 },
            }),
            command(CommandOperation::Replace {
                promise_id: promise(7),
                expected_version: version(5),
                new_bundle: bundle(),
                new_state: ReplacementState::Committed,
            }),
            command(CommandOperation::ProcessExpirations),
        ];

        for expected in commands {
            let encoded = encode_command(&expected).unwrap();
            assert_eq!(decode_command(&encoded).unwrap(), expected);
            assert_eq!(
                encode_command(&decode_command(&encoded).unwrap()).unwrap(),
                encoded
            );
        }
    }

    #[test]
    fn codec_preserves_bundle_claim_and_choice_order() {
        let expected = command(CommandOperation::HoldOneOf {
            promise_id: promise(9),
            choice: Choice::new(vec![
                bundle(),
                Bundle::new(vec![claim(pool(3), 40, 50, 7)]).unwrap(),
            ])
            .unwrap(),
            expires_at: 100,
        });

        let decoded = decode_command(&encode_command(&expected).unwrap()).unwrap();
        let CommandOperation::HoldOneOf { choice, .. } = decoded.operation() else {
            panic!("expected HoldOneOf");
        };
        assert_eq!(choice.alternatives()[0].claims()[0].pool_id(), pool(2));
        assert_eq!(choice.alternatives()[0].claims()[1].pool_id(), pool(1));
        assert_eq!(choice.alternatives()[1].claims()[0].pool_id(), pool(3));
    }

    #[test]
    fn rejects_unsupported_version_invalid_tag_utf8_and_trailing_bytes() {
        assert_eq!(
            decode_command(&[2]),
            Err(StorageError::UnsupportedVersion(2))
        );

        let mut invalid_tag = vec![COMMAND_FORMAT_VERSION];
        invalid_tag.extend_from_slice(&0_u32.to_le_bytes());
        invalid_tag.extend_from_slice(&0_u32.to_le_bytes());
        invalid_tag.push(255);
        assert_eq!(
            decode_command(&invalid_tag),
            Err(StorageError::InvalidTag {
                kind: "command operation",
                tag: 255,
            })
        );

        let invalid_utf8 = [COMMAND_FORMAT_VERSION, 1, 0, 0, 0, 0xff];
        assert_eq!(
            decode_command(&invalid_utf8),
            Err(StorageError::InvalidUtf8)
        );

        let mut trailing = encode_command(&command(CommandOperation::ProcessExpirations)).unwrap();
        trailing.push(0);
        assert_eq!(
            decode_command(&trailing),
            Err(StorageError::CorruptRecord(
                "trailing command payload bytes"
            ))
        );
    }

    #[test]
    fn rejects_every_truncated_prefix_of_a_valid_payload() {
        let encoded = encode_command(&command(CommandOperation::CreateResourcePool {
            resource_pool_id: pool(1),
            display_name: "pool".into(),
            unit: Unit::new("units".into(), 10).unwrap(),
            capacity_curve: curve(),
        }))
        .unwrap();

        for end in 0..encoded.len() {
            assert!(
                matches!(
                    decode_command(&encoded[..end]),
                    Err(StorageError::TruncatedPayload)
                ),
                "prefix ending at {end} was not reported as truncated"
            );
        }
    }

    #[test]
    fn rejects_decoded_domain_violations() {
        let mut encoded = vec![COMMAND_FORMAT_VERSION];
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        encoded.push(HOLD);
        encoded.extend_from_slice(&[1; 16]);
        encoded.extend_from_slice(&1_u32.to_le_bytes());
        encoded.extend_from_slice(&[2; 16]);
        encoded.extend_from_slice(&10_i64.to_le_bytes());
        encoded.extend_from_slice(&10_i64.to_le_bytes());
        encoded.extend_from_slice(&1_u64.to_le_bytes());
        encoded.extend_from_slice(&20_i64.to_le_bytes());

        assert_eq!(
            decode_command(&encoded),
            Err(StorageError::Domain(DomainError::InvalidInterval))
        );
    }
}
