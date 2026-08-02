//! Stable codec for durable prepared-transition effects.

use crate::command::{ClientId, CommandResult, IdempotencyKey};
use crate::domain::{
    Bundle, CapacityCurve, CapacitySegment, Claim, DomainError, Interval, Promise, PromiseId,
    PromiseState, ResourcePool, ResourcePoolId, SequenceNumber, Unit, Version,
};
use crate::engine::{
    AvailabilityConflict, CapacityDeficit, CapacityRevisionOutcome, ChoiceConflict, ChoiceOutcome,
    DurableTransition, HoldOutcome, ReplaceOutcome, SlotOutcome,
};
use crate::event::{Event, EventData, EventKind};
use crate::idempotency::{CommandHash, CommandResponse};

use super::codec::encode_command_into;
use super::{StorageError, decode_command};

pub(crate) const TRANSITION_FORMAT_VERSION: u8 = 1;

#[cfg(test)]
pub(crate) fn encode_transition(transition: &DurableTransition) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::new();
    encode_transition_into(transition, &mut bytes)?;
    Ok(bytes)
}

pub(crate) fn encode_transition_into(
    transition: &DurableTransition,
    destination: &mut Vec<u8>,
) -> Result<(), StorageError> {
    let start = destination.len();
    let result = (|| {
        destination.push(TRANSITION_FORMAT_VERSION);
        let command_length_offset = destination.len();
        destination.extend_from_slice(&0_u32.to_le_bytes());
        let command_start = destination.len();
        encode_command_into(transition.command(), destination)?;
        let command_length = destination.len() - command_start;
        let command_length =
            u32::try_from(command_length).map_err(|_| StorageError::InvalidLength {
                field: "command",
                length: u64::try_from(command_length).unwrap_or(u64::MAX),
            })?;
        destination[command_length_offset..command_length_offset + 4]
            .copy_from_slice(&command_length.to_le_bytes());

        let mut w = Writer(destination);
        w.string("client_id", transition.client_id().as_str())?;
        w.string("idempotency_key", transition.idempotency_key().as_str())?;
        w.raw(transition.command_hash().as_bytes());
        w.response(transition.response())?;
        w.len("resource pools", transition.resource_pools().len())?;
        for pool in transition.resource_pools() {
            w.resource_pool(pool)?;
        }
        w.len("promises", transition.promises().len())?;
        for promise in transition.promises() {
            w.promise(promise)?;
        }
        w.len("events", transition.events().len())?;
        for event in transition.events() {
            w.event(event)?;
        }
        w.u64(transition.final_sequence().get());
        Ok(())
    })();
    if result.is_err() {
        destination.truncate(start);
    }
    result
}

pub(crate) fn decode_transition(bytes: &[u8]) -> Result<DurableTransition, StorageError> {
    let mut r = Reader::new(bytes);
    let version = r.u8()?;
    if version != TRANSITION_FORMAT_VERSION {
        return Err(StorageError::UnsupportedTransitionVersion(version));
    }
    let command = decode_command(r.bytes("command")?)?;
    let client_id = ClientId::new(r.string("client_id")?);
    let idempotency_key = IdempotencyKey::new(r.string("idempotency_key")?);
    let mut hash = [0; 32];
    hash.copy_from_slice(r.take(32)?);
    let command_hash = CommandHash::from_bytes(hash);
    let response = r.response()?;

    let pool_count = r.count("resource pools")?;
    let mut resource_pools = Vec::with_capacity(r.safe_capacity(pool_count));
    let mut previous_pool = None;
    for _ in 0..pool_count {
        let pool = r.resource_pool()?;
        if previous_pool.is_some_and(|id| id >= pool.id()) {
            return Err(StorageError::CorruptRecord(
                "non-canonical resource pool order",
            ));
        }
        previous_pool = Some(pool.id());
        resource_pools.push(pool);
    }

    let promise_count = r.count("promises")?;
    let mut promises = Vec::with_capacity(r.safe_capacity(promise_count));
    let mut previous_promise = None;
    for _ in 0..promise_count {
        let promise = r.promise()?;
        if previous_promise.is_some_and(|id| id >= promise.id()) {
            return Err(StorageError::CorruptRecord("non-canonical promise order"));
        }
        previous_promise = Some(promise.id());
        promises.push(promise);
    }

    let event_count = r.count("events")?;
    let mut events = Vec::with_capacity(r.safe_capacity(event_count));
    let mut previous_sequence = None;
    for _ in 0..event_count {
        let event = r.event()?;
        if previous_sequence.is_some_and(|sequence| sequence > event.sequence()) {
            return Err(StorageError::CorruptRecord(
                "events are not sequence ordered",
            ));
        }
        previous_sequence = Some(event.sequence());
        events.push(event);
    }
    let final_sequence = SequenceNumber::new(r.u64()?);
    if !r.is_empty() {
        return Err(StorageError::CorruptRecord(
            "trailing transition payload bytes",
        ));
    }
    Ok(DurableTransition::restore(
        command,
        client_id,
        idempotency_key,
        command_hash,
        response,
        resource_pools,
        promises,
        events,
        final_sequence,
    ))
}

struct Writer<'a>(&'a mut Vec<u8>);

impl Writer<'_> {
    fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }
    fn u128(&mut self, value: u128) {
        self.raw(&value.to_le_bytes());
    }
    fn i64(&mut self, value: i64) {
        self.raw(&value.to_le_bytes());
    }
    fn len(&mut self, field: &'static str, value: usize) -> Result<(), StorageError> {
        let value = u32::try_from(value).map_err(|_| StorageError::InvalidLength {
            field,
            length: value as u64,
        })?;
        self.u32(value);
        Ok(())
    }
    fn bytes(&mut self, field: &'static str, value: &[u8]) -> Result<(), StorageError> {
        self.len(field, value.len())?;
        self.raw(value);
        Ok(())
    }
    fn string(&mut self, field: &'static str, value: &str) -> Result<(), StorageError> {
        self.bytes(field, value.as_bytes())
    }
    fn pool_id(&mut self, id: ResourcePoolId) {
        self.raw(&id.as_bytes());
    }
    fn promise_id(&mut self, id: PromiseId) {
        self.raw(&id.as_bytes());
    }
    fn interval(&mut self, value: Interval) {
        self.i64(value.start());
        self.i64(value.end());
    }
    fn curve(&mut self, value: &CapacityCurve) -> Result<(), StorageError> {
        self.len("capacity segments", value.segments().len())?;
        for segment in value.segments() {
            self.interval(segment.interval());
            self.u64(segment.capacity());
        }
        Ok(())
    }
    fn bundle(&mut self, value: &Bundle) -> Result<(), StorageError> {
        self.len("bundle claims", value.claims().len())?;
        for claim in value.claims() {
            self.pool_id(claim.pool_id());
            self.interval(claim.interval());
            self.u64(claim.quantity());
        }
        Ok(())
    }
    fn resource_pool(&mut self, value: &ResourcePool) -> Result<(), StorageError> {
        self.pool_id(value.id());
        self.string("display name", value.display_name())?;
        self.string("unit name", value.unit().name())?;
        self.u64(value.unit().subunits_per_unit());
        self.curve(value.capacity_curve())
    }
    fn promise(&mut self, value: &Promise) -> Result<(), StorageError> {
        self.promise_id(value.id());
        match value.state() {
            PromiseState::Held { expires_at } => {
                self.u8(1);
                self.i64(expires_at);
            }
            PromiseState::Committed => self.u8(2),
            PromiseState::Released => self.u8(3),
            PromiseState::Expired => self.u8(4),
        }
        self.bundle(value.bundle())?;
        self.u64(value.version().get());
        self.u64(value.created_sequence().get());
        self.u64(value.updated_sequence().get());
        Ok(())
    }
    fn ids(&mut self, field: &'static str, ids: &[PromiseId]) -> Result<(), StorageError> {
        self.len(field, ids.len())?;
        for id in ids {
            self.promise_id(*id);
        }
        Ok(())
    }
    fn conflict(&mut self, value: &AvailabilityConflict) -> Result<(), StorageError> {
        self.pool_id(value.resource_pool_id());
        self.interval(value.blocking_interval());
        self.u64(value.required_quantity());
        self.u64(value.available_quantity());
        self.u64(value.deficit_quantity());
        self.ids("conflicting promise ids", value.conflicting_promise_ids())
    }
    fn conflicts(&mut self, values: &[AvailabilityConflict]) -> Result<(), StorageError> {
        self.len("availability conflicts", values.len())?;
        for value in values {
            self.conflict(value)?;
        }
        Ok(())
    }
    fn deficit(&mut self, value: &CapacityDeficit) -> Result<(), StorageError> {
        self.pool_id(value.resource_pool_id());
        self.interval(value.interval());
        self.u64(value.quantity());
        self.ids("affected promise ids", value.affected_promise_ids())
    }
    fn capacity_outcome(&mut self, value: &CapacityRevisionOutcome) -> Result<(), StorageError> {
        self.u64(value.sequence().get());
        self.len("capacity deficits", value.deficits().len())?;
        for deficit in value.deficits() {
            self.deficit(deficit)?;
        }
        self.ids("affected promise ids", value.affected_promise_ids())
    }
    fn response(&mut self, value: &CommandResponse) -> Result<(), StorageError> {
        match value {
            Ok(result) => {
                self.u8(1);
                self.result(result)
            }
            Err(error) => {
                self.u8(2);
                self.domain_error(*error);
                Ok(())
            }
        }
    }
    fn result(&mut self, value: &CommandResult) -> Result<(), StorageError> {
        match value {
            CommandResult::ResourcePoolCreated { resource_pool_id } => {
                self.u8(1);
                self.pool_id(*resource_pool_id);
            }
            CommandResult::CapacityRevised(outcome) => {
                self.u8(2);
                self.capacity_outcome(outcome)?;
            }
            CommandResult::HoldCompleted(HoldOutcome::Held(id)) => {
                self.u8(3);
                self.u8(1);
                self.promise_id(*id);
            }
            CommandResult::HoldCompleted(HoldOutcome::Unavailable { conflicts }) => {
                self.u8(3);
                self.u8(2);
                self.conflicts(conflicts)?;
            }
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                promise_id,
                alternative_index,
            }) => {
                self.u8(4);
                self.u8(1);
                self.promise_id(*promise_id);
                self.len("alternative index", *alternative_index)?;
            }
            CommandResult::ChoiceCompleted(ChoiceOutcome::Unavailable { conflicts }) => {
                self.u8(4);
                self.u8(2);
                self.len("choice conflicts", conflicts.len())?;
                for conflict in conflicts {
                    self.len("alternative index", conflict.alternative_index())?;
                    self.conflicts(conflict.conflicts())?;
                }
            }
            CommandResult::SlotCompleted(SlotOutcome::Held { promise_id, start }) => {
                self.u8(5);
                self.u8(1);
                self.promise_id(*promise_id);
                self.i64(*start);
            }
            CommandResult::SlotCompleted(SlotOutcome::Unavailable { attempts }) => {
                self.u8(5);
                self.u8(2);
                self.u128(*attempts);
            }
            CommandResult::PromiseCommitted {
                promise_id,
                version,
            } => {
                self.u8(6);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReleased {
                promise_id,
                version,
            } => {
                self.u8(7);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReplaced(ReplaceOutcome::Replaced {
                promise_id,
                version,
            }) => {
                self.u8(8);
                self.u8(1);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReplaced(ReplaceOutcome::Unavailable { conflicts }) => {
                self.u8(8);
                self.u8(2);
                self.conflicts(conflicts)?;
            }
            CommandResult::ExpirationsProcessed { expired_count } => {
                self.u8(9);
                self.u64(u64::try_from(*expired_count).map_err(|_| {
                    StorageError::InvalidLength {
                        field: "expired count",
                        length: u64::MAX,
                    }
                })?);
            }
        }
        Ok(())
    }
    fn domain_error(&mut self, value: DomainError) {
        self.u8(match value {
            DomainError::InvalidInterval => 1,
            DomainError::UnsortedCapacitySegments => 2,
            DomainError::OverlappingCapacitySegments => 3,
            DomainError::InvalidUnitName => 4,
            DomainError::InvalidUnitScale => 5,
            DomainError::InvalidQuantity => 6,
            DomainError::QuantityOutOfRange => 7,
            DomainError::QuantityOverflow => 8,
            DomainError::IndexOverflow => 9,
            DomainError::InvalidExpiration => 10,
            DomainError::EmptyBundle => 11,
            DomainError::EmptyRelativeBundle => 12,
            DomainError::EmptyChoice => 13,
            DomainError::InvalidSearchRange => 14,
            DomainError::InvalidStep => 15,
            DomainError::TimestampOverflow => 16,
            DomainError::ResourcePoolAlreadyExists => 17,
            DomainError::ResourcePoolNotFound => 18,
            DomainError::CapacityExceeded => 19,
            DomainError::CapacityRevisionCreatesDeficit => 20,
            DomainError::PromiseAlreadyExists => 21,
            DomainError::PromiseNotFound => 22,
            DomainError::InvalidPromiseState => 23,
            DomainError::IdempotencyConflict => 24,
            DomainError::VersionConflict => 25,
            DomainError::VersionOverflow => 26,
            DomainError::SequenceOverflow => 27,
            DomainError::SystemTimeOutOfRange => 28,
            DomainError::HoldExpired => 29,
            DomainError::HoldNotExpired => 30,
            DomainError::InvalidPromiseHistory => 31,
            DomainError::PublicationRevisionOverflow => 32,
        });
    }
    fn event(&mut self, value: &Event) -> Result<(), StorageError> {
        self.u64(value.sequence().get());
        self.i64(value.timestamp());
        self.u8(match value.kind() {
            EventKind::ResourceCreated => 1,
            EventKind::CapacityRevised => 2,
            EventKind::HoldCreated => 3,
            EventKind::HoldCommitted => 4,
            EventKind::PromiseReleased => 5,
            EventKind::PromiseReplaced => 6,
            EventKind::HoldExpired => 7,
            EventKind::DeficitCreated => 8,
            EventKind::DeficitChanged => 9,
            EventKind::DeficitResolved => 10,
        });
        match value.data() {
            EventData::ResourcePool { resource_pool_id } => {
                self.u8(1);
                self.pool_id(*resource_pool_id);
            }
            EventData::Promise {
                promise_id,
                version,
            } => {
                self.u8(2);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            EventData::Deficit {
                resource_pool_id,
                interval,
                quantity,
                affected_promise_ids,
            } => {
                self.u8(3);
                self.pool_id(*resource_pool_id);
                self.interval(*interval);
                self.u64(*quantity);
                self.ids("affected promise ids", affected_promise_ids)?;
            }
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
    fn safe_capacity(&self, count: usize) -> usize {
        count.min(self.bytes.len().saturating_sub(self.offset))
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
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], StorageError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(N)?);
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }
    fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }
    fn u128(&mut self) -> Result<u128, StorageError> {
        Ok(u128::from_le_bytes(self.fixed()?))
    }
    fn i64(&mut self) -> Result<i64, StorageError> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }
    fn count(&mut self, field: &'static str) -> Result<usize, StorageError> {
        usize::try_from(self.u32()?).map_err(|_| StorageError::InvalidLength {
            field,
            length: u64::from(u32::MAX),
        })
    }
    fn bytes(&mut self, field: &'static str) -> Result<&'a [u8], StorageError> {
        let length = self.count(field)?;
        self.take(length)
    }
    fn string(&mut self, field: &'static str) -> Result<String, StorageError> {
        String::from_utf8(self.bytes(field)?.to_vec()).map_err(|_| StorageError::InvalidUtf8)
    }
    fn pool_id(&mut self) -> Result<ResourcePoolId, StorageError> {
        Ok(ResourcePoolId::from_bytes(self.fixed()?))
    }
    fn promise_id(&mut self) -> Result<PromiseId, StorageError> {
        Ok(PromiseId::from_bytes(self.fixed()?))
    }
    fn sequence(&mut self) -> Result<SequenceNumber, StorageError> {
        Ok(SequenceNumber::new(self.u64()?))
    }
    fn version(&mut self) -> Result<Version, StorageError> {
        Version::new(self.u64()?).ok_or(StorageError::CorruptRecord("zero promise version"))
    }
    fn interval(&mut self) -> Result<Interval, StorageError> {
        Ok(Interval::new(self.i64()?, self.i64()?)?)
    }
    fn curve(&mut self) -> Result<CapacityCurve, StorageError> {
        let count = self.count("capacity segments")?;
        let mut values = Vec::with_capacity(self.safe_capacity(count));
        for _ in 0..count {
            values.push(CapacitySegment::new(self.interval()?, self.u64()?));
        }
        Ok(CapacityCurve::from_sorted(values)?)
    }
    fn bundle(&mut self) -> Result<Bundle, StorageError> {
        let count = self.count("bundle claims")?;
        let mut values = Vec::with_capacity(self.safe_capacity(count));
        for _ in 0..count {
            values.push(Claim::new(self.pool_id()?, self.interval()?, self.u64()?)?);
        }
        Ok(Bundle::new(values)?)
    }
    fn resource_pool(&mut self) -> Result<ResourcePool, StorageError> {
        let id = self.pool_id()?;
        let display_name = self.string("display name")?;
        let unit = Unit::new(self.string("unit name")?, self.u64()?)?;
        let curve = self.curve()?;
        Ok(ResourcePool::with_id(id, display_name, unit, curve))
    }
    fn promise(&mut self) -> Result<Promise, StorageError> {
        let id = self.promise_id()?;
        let state = match self.u8()? {
            1 => PromiseState::Held {
                expires_at: self.i64()?,
            },
            2 => PromiseState::Committed,
            3 => PromiseState::Released,
            4 => PromiseState::Expired,
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "promise state",
                    tag,
                });
            }
        };
        let bundle = self.bundle()?;
        let version = self.version()?;
        let created = self.sequence()?;
        let updated = self.sequence()?;
        Ok(Promise::restore(
            id, state, bundle, version, created, updated,
        )?)
    }
    fn ids(&mut self, field: &'static str) -> Result<Vec<PromiseId>, StorageError> {
        let count = self.count(field)?;
        let mut ids = Vec::with_capacity(self.safe_capacity(count));
        for _ in 0..count {
            let id = self.promise_id()?;
            if ids.last().is_some_and(|previous| *previous >= id) {
                return Err(StorageError::CorruptRecord(
                    "promise IDs are not sorted and unique",
                ));
            }
            ids.push(id);
        }
        Ok(ids)
    }
    fn conflict(&mut self) -> Result<AvailabilityConflict, StorageError> {
        AvailabilityConflict::restore(
            self.pool_id()?,
            self.interval()?,
            self.u64()?,
            self.u64()?,
            self.u64()?,
            self.ids("conflicting promise ids")?,
        )
        .ok_or(StorageError::CorruptRecord("invalid availability conflict"))
    }
    fn conflicts(&mut self) -> Result<Vec<AvailabilityConflict>, StorageError> {
        let count = self.count("availability conflicts")?;
        if count == 0 {
            return Err(StorageError::CorruptRecord(
                "empty unavailable conflict list",
            ));
        }
        let mut values = Vec::with_capacity(self.safe_capacity(count));
        for _ in 0..count {
            values.push(self.conflict()?);
        }
        Ok(values)
    }
    fn deficit(&mut self) -> Result<CapacityDeficit, StorageError> {
        CapacityDeficit::restore(
            self.pool_id()?,
            self.interval()?,
            self.u64()?,
            self.ids("affected promise ids")?,
        )
        .ok_or(StorageError::CorruptRecord("invalid capacity deficit"))
    }
    fn capacity_outcome(&mut self) -> Result<CapacityRevisionOutcome, StorageError> {
        let sequence = self.sequence()?;
        let count = self.count("capacity deficits")?;
        let mut deficits = Vec::with_capacity(self.safe_capacity(count));
        for _ in 0..count {
            deficits.push(self.deficit()?);
        }
        CapacityRevisionOutcome::restore(sequence, deficits, self.ids("affected promise ids")?)
            .ok_or(StorageError::CorruptRecord(
                "invalid capacity revision outcome",
            ))
    }
    fn response(&mut self) -> Result<CommandResponse, StorageError> {
        match self.u8()? {
            1 => Ok(Ok(self.result()?)),
            2 => Ok(Err(self.domain_error()?)),
            tag => Err(StorageError::InvalidTag {
                kind: "command response",
                tag,
            }),
        }
    }
    fn result(&mut self) -> Result<CommandResult, StorageError> {
        match self.u8()? {
            1 => Ok(CommandResult::ResourcePoolCreated {
                resource_pool_id: self.pool_id()?,
            }),
            2 => Ok(CommandResult::CapacityRevised(self.capacity_outcome()?)),
            3 => Ok(CommandResult::HoldCompleted(match self.u8()? {
                1 => HoldOutcome::Held(self.promise_id()?),
                2 => HoldOutcome::Unavailable {
                    conflicts: self.conflicts()?,
                },
                tag => {
                    return Err(StorageError::InvalidTag {
                        kind: "hold outcome",
                        tag,
                    });
                }
            })),
            4 => Ok(CommandResult::ChoiceCompleted(match self.u8()? {
                1 => ChoiceOutcome::Held {
                    promise_id: self.promise_id()?,
                    alternative_index: self.count("alternative index")?,
                },
                2 => {
                    let count = self.count("choice conflicts")?;
                    if count == 0 {
                        return Err(StorageError::CorruptRecord("empty choice conflict list"));
                    }
                    let mut values = Vec::with_capacity(self.safe_capacity(count));
                    for expected in 0..count {
                        let index = self.count("alternative index")?;
                        if index != expected {
                            return Err(StorageError::CorruptRecord(
                                "non-canonical choice conflict index",
                            ));
                        }
                        values.push(
                            ChoiceConflict::restore(index, self.conflicts()?)
                                .ok_or(StorageError::CorruptRecord("invalid choice conflict"))?,
                        );
                    }
                    ChoiceOutcome::Unavailable { conflicts: values }
                }
                tag => {
                    return Err(StorageError::InvalidTag {
                        kind: "choice outcome",
                        tag,
                    });
                }
            })),
            5 => Ok(CommandResult::SlotCompleted(match self.u8()? {
                1 => SlotOutcome::Held {
                    promise_id: self.promise_id()?,
                    start: self.i64()?,
                },
                2 => {
                    let attempts = self.u128()?;
                    if attempts == 0 {
                        return Err(StorageError::CorruptRecord("zero slot attempts"));
                    }
                    SlotOutcome::Unavailable { attempts }
                }
                tag => {
                    return Err(StorageError::InvalidTag {
                        kind: "slot outcome",
                        tag,
                    });
                }
            })),
            6 => Ok(CommandResult::PromiseCommitted {
                promise_id: self.promise_id()?,
                version: self.version()?,
            }),
            7 => Ok(CommandResult::PromiseReleased {
                promise_id: self.promise_id()?,
                version: self.version()?,
            }),
            8 => Ok(CommandResult::PromiseReplaced(match self.u8()? {
                1 => ReplaceOutcome::Replaced {
                    promise_id: self.promise_id()?,
                    version: self.version()?,
                },
                2 => ReplaceOutcome::Unavailable {
                    conflicts: self.conflicts()?,
                },
                tag => {
                    return Err(StorageError::InvalidTag {
                        kind: "replace outcome",
                        tag,
                    });
                }
            })),
            9 => Ok(CommandResult::ExpirationsProcessed {
                expired_count: usize::try_from(self.u64()?).map_err(|_| {
                    StorageError::InvalidLength {
                        field: "expired count",
                        length: u64::MAX,
                    }
                })?,
            }),
            tag => Err(StorageError::InvalidTag {
                kind: "command result",
                tag,
            }),
        }
    }
    fn domain_error(&mut self) -> Result<DomainError, StorageError> {
        Ok(match self.u8()? {
            1 => DomainError::InvalidInterval,
            2 => DomainError::UnsortedCapacitySegments,
            3 => DomainError::OverlappingCapacitySegments,
            4 => DomainError::InvalidUnitName,
            5 => DomainError::InvalidUnitScale,
            6 => DomainError::InvalidQuantity,
            7 => DomainError::QuantityOutOfRange,
            8 => DomainError::QuantityOverflow,
            9 => DomainError::IndexOverflow,
            10 => DomainError::InvalidExpiration,
            11 => DomainError::EmptyBundle,
            12 => DomainError::EmptyRelativeBundle,
            13 => DomainError::EmptyChoice,
            14 => DomainError::InvalidSearchRange,
            15 => DomainError::InvalidStep,
            16 => DomainError::TimestampOverflow,
            17 => DomainError::ResourcePoolAlreadyExists,
            18 => DomainError::ResourcePoolNotFound,
            19 => DomainError::CapacityExceeded,
            20 => DomainError::CapacityRevisionCreatesDeficit,
            21 => DomainError::PromiseAlreadyExists,
            22 => DomainError::PromiseNotFound,
            23 => DomainError::InvalidPromiseState,
            24 => DomainError::IdempotencyConflict,
            25 => DomainError::VersionConflict,
            26 => DomainError::VersionOverflow,
            27 => DomainError::SequenceOverflow,
            28 => DomainError::SystemTimeOutOfRange,
            29 => DomainError::HoldExpired,
            30 => DomainError::HoldNotExpired,
            31 => DomainError::InvalidPromiseHistory,
            32 => DomainError::PublicationRevisionOverflow,
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "domain error",
                    tag,
                });
            }
        })
    }
    fn event(&mut self) -> Result<Event, StorageError> {
        let sequence = self.sequence()?;
        let timestamp = self.i64()?;
        let kind = match self.u8()? {
            1 => EventKind::ResourceCreated,
            2 => EventKind::CapacityRevised,
            3 => EventKind::HoldCreated,
            4 => EventKind::HoldCommitted,
            5 => EventKind::PromiseReleased,
            6 => EventKind::PromiseReplaced,
            7 => EventKind::HoldExpired,
            8 => EventKind::DeficitCreated,
            9 => EventKind::DeficitChanged,
            10 => EventKind::DeficitResolved,
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "event kind",
                    tag,
                });
            }
        };
        let data = match self.u8()? {
            1 => EventData::ResourcePool {
                resource_pool_id: self.pool_id()?,
            },
            2 => EventData::Promise {
                promise_id: self.promise_id()?,
                version: self.version()?,
            },
            3 => EventData::Deficit {
                resource_pool_id: self.pool_id()?,
                interval: self.interval()?,
                quantity: self.u64()?,
                affected_promise_ids: self.ids("affected promise ids")?,
            },
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "event data",
                    tag,
                });
            }
        };
        Event::restore(sequence, timestamp, kind, data)
            .ok_or(StorageError::CorruptRecord("invalid event"))
    }
}
