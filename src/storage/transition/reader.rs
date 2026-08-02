//! Bounded decoding and validation for durable prepared-transition effects.

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

use super::super::{StorageError, decode_command};
use super::TRANSITION_FORMAT_VERSION;
use super::format::{
    choice_outcome, command_result, domain_error, event_data, event_kind, hold_outcome,
    promise_state, replace_outcome, response, slot_outcome,
};

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

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    max_collection_items: usize,
    max_string_bytes: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self::new_bounded(bytes, usize::MAX, usize::MAX)
    }
    pub(crate) fn new_bounded(
        bytes: &'a [u8],
        max_collection_items: usize,
        max_string_bytes: usize,
    ) -> Self {
        Self {
            bytes,
            offset: 0,
            max_collection_items,
            max_string_bytes,
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
    pub(crate) fn safe_capacity(&self, count: usize) -> usize {
        count.min(self.bytes.len().saturating_sub(self.offset))
    }
    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
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
    pub(crate) fn u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }
    pub(crate) fn u128(&mut self) -> Result<u128, StorageError> {
        Ok(u128::from_le_bytes(self.fixed()?))
    }
    fn i64(&mut self) -> Result<i64, StorageError> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }
    pub(crate) fn count(&mut self, field: &'static str) -> Result<usize, StorageError> {
        let value = usize::try_from(self.u32()?).map_err(|_| StorageError::InvalidLength {
            field,
            length: u64::from(u32::MAX),
        })?;
        if value > self.max_collection_items {
            return Err(StorageError::InvalidLength {
                field,
                length: value as u64,
            });
        }
        Ok(value)
    }
    fn bytes(&mut self, field: &'static str) -> Result<&'a [u8], StorageError> {
        let length = self.count(field)?;
        self.take(length)
    }
    pub(crate) fn string(&mut self, field: &'static str) -> Result<String, StorageError> {
        let length = self.count(field)?;
        if length > self.max_string_bytes {
            return Err(StorageError::InvalidLength {
                field,
                length: length as u64,
            });
        }
        String::from_utf8(self.take(length)?.to_vec()).map_err(|_| StorageError::InvalidUtf8)
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
    pub(crate) fn resource_pool(&mut self) -> Result<ResourcePool, StorageError> {
        let id = self.pool_id()?;
        let display_name = self.string("display name")?;
        let unit = Unit::new(self.string("unit name")?, self.u64()?)?;
        let curve = self.curve()?;
        Ok(ResourcePool::with_id(id, display_name, unit, curve))
    }
    pub(crate) fn promise(&mut self) -> Result<Promise, StorageError> {
        let id = self.promise_id()?;
        let state = match self.u8()? {
            promise_state::HELD => PromiseState::Held {
                expires_at: self.i64()?,
            },
            promise_state::COMMITTED => PromiseState::Committed,
            promise_state::RELEASED => PromiseState::Released,
            promise_state::EXPIRED => PromiseState::Expired,
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
    pub(crate) fn response(&mut self) -> Result<CommandResponse, StorageError> {
        match self.u8()? {
            response::SUCCESS => Ok(Ok(self.result()?)),
            response::DOMAIN_ERROR => Ok(Err(self.domain_error()?)),
            tag => Err(StorageError::InvalidTag {
                kind: "command response",
                tag,
            }),
        }
    }
    fn result(&mut self) -> Result<CommandResult, StorageError> {
        match self.u8()? {
            command_result::RESOURCE_POOL_CREATED => Ok(CommandResult::ResourcePoolCreated {
                resource_pool_id: self.pool_id()?,
            }),
            command_result::CAPACITY_REVISED => {
                Ok(CommandResult::CapacityRevised(self.capacity_outcome()?))
            }
            command_result::HOLD_COMPLETED => {
                Ok(CommandResult::HoldCompleted(match self.u8()? {
                    hold_outcome::HELD => HoldOutcome::Held(self.promise_id()?),
                    hold_outcome::UNAVAILABLE => HoldOutcome::Unavailable {
                        conflicts: self.conflicts()?,
                    },
                    tag => {
                        return Err(StorageError::InvalidTag {
                            kind: "hold outcome",
                            tag,
                        });
                    }
                }))
            }
            command_result::CHOICE_COMPLETED => {
                Ok(CommandResult::ChoiceCompleted(match self.u8()? {
                    choice_outcome::HELD => ChoiceOutcome::Held {
                        promise_id: self.promise_id()?,
                        alternative_index: self.count("alternative index")?,
                    },
                    choice_outcome::UNAVAILABLE => {
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
                                ChoiceConflict::restore(index, self.conflicts()?).ok_or(
                                    StorageError::CorruptRecord("invalid choice conflict"),
                                )?,
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
                }))
            }
            command_result::SLOT_COMPLETED => {
                Ok(CommandResult::SlotCompleted(match self.u8()? {
                    slot_outcome::HELD => SlotOutcome::Held {
                        promise_id: self.promise_id()?,
                        start: self.i64()?,
                    },
                    slot_outcome::UNAVAILABLE => {
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
                }))
            }
            command_result::PROMISE_COMMITTED => Ok(CommandResult::PromiseCommitted {
                promise_id: self.promise_id()?,
                version: self.version()?,
            }),
            command_result::PROMISE_RELEASED => Ok(CommandResult::PromiseReleased {
                promise_id: self.promise_id()?,
                version: self.version()?,
            }),
            command_result::PROMISE_REPLACED => {
                Ok(CommandResult::PromiseReplaced(match self.u8()? {
                    replace_outcome::REPLACED => ReplaceOutcome::Replaced {
                        promise_id: self.promise_id()?,
                        version: self.version()?,
                    },
                    replace_outcome::UNAVAILABLE => ReplaceOutcome::Unavailable {
                        conflicts: self.conflicts()?,
                    },
                    tag => {
                        return Err(StorageError::InvalidTag {
                            kind: "replace outcome",
                            tag,
                        });
                    }
                }))
            }
            command_result::EXPIRATIONS_PROCESSED => Ok(CommandResult::ExpirationsProcessed {
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
            domain_error::INVALID_INTERVAL => DomainError::InvalidInterval,
            domain_error::UNSORTED_CAPACITY_SEGMENTS => DomainError::UnsortedCapacitySegments,
            domain_error::OVERLAPPING_CAPACITY_SEGMENTS => DomainError::OverlappingCapacitySegments,
            domain_error::INVALID_UNIT_NAME => DomainError::InvalidUnitName,
            domain_error::INVALID_UNIT_SCALE => DomainError::InvalidUnitScale,
            domain_error::INVALID_QUANTITY => DomainError::InvalidQuantity,
            domain_error::QUANTITY_OUT_OF_RANGE => DomainError::QuantityOutOfRange,
            domain_error::QUANTITY_OVERFLOW => DomainError::QuantityOverflow,
            domain_error::INDEX_OVERFLOW => DomainError::IndexOverflow,
            domain_error::INVALID_EXPIRATION => DomainError::InvalidExpiration,
            domain_error::EMPTY_BUNDLE => DomainError::EmptyBundle,
            domain_error::EMPTY_RELATIVE_BUNDLE => DomainError::EmptyRelativeBundle,
            domain_error::EMPTY_CHOICE => DomainError::EmptyChoice,
            domain_error::INVALID_SEARCH_RANGE => DomainError::InvalidSearchRange,
            domain_error::INVALID_STEP => DomainError::InvalidStep,
            domain_error::TIMESTAMP_OVERFLOW => DomainError::TimestampOverflow,
            domain_error::RESOURCE_POOL_ALREADY_EXISTS => DomainError::ResourcePoolAlreadyExists,
            domain_error::RESOURCE_POOL_NOT_FOUND => DomainError::ResourcePoolNotFound,
            domain_error::CAPACITY_EXCEEDED => DomainError::CapacityExceeded,
            domain_error::CAPACITY_REVISION_CREATES_DEFICIT => {
                DomainError::CapacityRevisionCreatesDeficit
            }
            domain_error::PROMISE_ALREADY_EXISTS => DomainError::PromiseAlreadyExists,
            domain_error::PROMISE_NOT_FOUND => DomainError::PromiseNotFound,
            domain_error::INVALID_PROMISE_STATE => DomainError::InvalidPromiseState,
            domain_error::IDEMPOTENCY_CONFLICT => DomainError::IdempotencyConflict,
            domain_error::VERSION_CONFLICT => DomainError::VersionConflict,
            domain_error::VERSION_OVERFLOW => DomainError::VersionOverflow,
            domain_error::SEQUENCE_OVERFLOW => DomainError::SequenceOverflow,
            domain_error::SYSTEM_TIME_OUT_OF_RANGE => DomainError::SystemTimeOutOfRange,
            domain_error::HOLD_EXPIRED => DomainError::HoldExpired,
            domain_error::HOLD_NOT_EXPIRED => DomainError::HoldNotExpired,
            domain_error::INVALID_PROMISE_HISTORY => DomainError::InvalidPromiseHistory,
            domain_error::PUBLICATION_REVISION_OVERFLOW => DomainError::PublicationRevisionOverflow,
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "domain error",
                    tag,
                });
            }
        })
    }
    pub(crate) fn event(&mut self) -> Result<Event, StorageError> {
        let sequence = self.sequence()?;
        let timestamp = self.i64()?;
        let kind = match self.u8()? {
            event_kind::RESOURCE_CREATED => EventKind::ResourceCreated,
            event_kind::CAPACITY_REVISED => EventKind::CapacityRevised,
            event_kind::HOLD_CREATED => EventKind::HoldCreated,
            event_kind::HOLD_COMMITTED => EventKind::HoldCommitted,
            event_kind::PROMISE_RELEASED => EventKind::PromiseReleased,
            event_kind::PROMISE_REPLACED => EventKind::PromiseReplaced,
            event_kind::HOLD_EXPIRED => EventKind::HoldExpired,
            event_kind::DEFICIT_CREATED => EventKind::DeficitCreated,
            event_kind::DEFICIT_CHANGED => EventKind::DeficitChanged,
            event_kind::DEFICIT_RESOLVED => EventKind::DeficitResolved,
            tag => {
                return Err(StorageError::InvalidTag {
                    kind: "event kind",
                    tag,
                });
            }
        };
        let data = match self.u8()? {
            event_data::RESOURCE_POOL => EventData::ResourcePool {
                resource_pool_id: self.pool_id()?,
            },
            event_data::PROMISE => EventData::Promise {
                promise_id: self.promise_id()?,
                version: self.version()?,
            },
            event_data::DEFICIT => EventData::Deficit {
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
