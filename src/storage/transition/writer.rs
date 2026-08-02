//! Encoding for durable prepared-transition effects.

use crate::command::CommandResult;
use crate::domain::{
    Bundle, CapacityCurve, DomainError, Interval, Promise, PromiseId, PromiseState, ResourcePool,
    ResourcePoolId,
};
use crate::engine::{
    AvailabilityConflict, CapacityDeficit, CapacityRevisionOutcome, ChoiceOutcome,
    DurableTransition, HoldOutcome, ReplaceOutcome, SlotOutcome,
};
use crate::event::{Event, EventData, EventKind};
use crate::idempotency::CommandResponse;

use super::super::StorageError;
use super::super::codec::encode_command_into;
use super::TRANSITION_FORMAT_VERSION;
use super::format::{
    choice_outcome, command_result, domain_error, event_data, event_kind, hold_outcome,
    promise_state, replace_outcome, response, slot_outcome,
};

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

pub(crate) struct Writer<'a>(&'a mut Vec<u8>);

impl<'a> Writer<'a> {
    pub(crate) fn new(destination: &'a mut Vec<u8>) -> Self {
        Self(destination)
    }
    pub(crate) fn raw(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }
    pub(crate) fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }
    pub(crate) fn u128(&mut self, value: u128) {
        self.raw(&value.to_le_bytes());
    }
    fn i64(&mut self, value: i64) {
        self.raw(&value.to_le_bytes());
    }
    pub(crate) fn len(&mut self, field: &'static str, value: usize) -> Result<(), StorageError> {
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
    pub(crate) fn string(&mut self, field: &'static str, value: &str) -> Result<(), StorageError> {
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
    pub(crate) fn resource_pool(&mut self, value: &ResourcePool) -> Result<(), StorageError> {
        self.pool_id(value.id());
        self.string("display name", value.display_name())?;
        self.string("unit name", value.unit().name())?;
        self.u64(value.unit().subunits_per_unit());
        self.curve(value.capacity_curve())
    }
    pub(crate) fn promise(&mut self, value: &Promise) -> Result<(), StorageError> {
        self.promise_id(value.id());
        match value.state() {
            PromiseState::Held { expires_at } => {
                self.u8(promise_state::HELD);
                self.i64(expires_at);
            }
            PromiseState::Committed => self.u8(promise_state::COMMITTED),
            PromiseState::Released => self.u8(promise_state::RELEASED),
            PromiseState::Expired => self.u8(promise_state::EXPIRED),
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
    pub(crate) fn response(&mut self, value: &CommandResponse) -> Result<(), StorageError> {
        match value {
            Ok(result) => {
                self.u8(response::SUCCESS);
                self.result(result)
            }
            Err(error) => {
                self.u8(response::DOMAIN_ERROR);
                self.domain_error(*error);
                Ok(())
            }
        }
    }
    fn result(&mut self, value: &CommandResult) -> Result<(), StorageError> {
        match value {
            CommandResult::ResourcePoolCreated { resource_pool_id } => {
                self.u8(command_result::RESOURCE_POOL_CREATED);
                self.pool_id(*resource_pool_id);
            }
            CommandResult::CapacityRevised(outcome) => {
                self.u8(command_result::CAPACITY_REVISED);
                self.capacity_outcome(outcome)?;
            }
            CommandResult::HoldCompleted(HoldOutcome::Held(id)) => {
                self.u8(command_result::HOLD_COMPLETED);
                self.u8(hold_outcome::HELD);
                self.promise_id(*id);
            }
            CommandResult::HoldCompleted(HoldOutcome::Unavailable { conflicts }) => {
                self.u8(command_result::HOLD_COMPLETED);
                self.u8(hold_outcome::UNAVAILABLE);
                self.conflicts(conflicts)?;
            }
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                promise_id,
                alternative_index,
            }) => {
                self.u8(command_result::CHOICE_COMPLETED);
                self.u8(choice_outcome::HELD);
                self.promise_id(*promise_id);
                self.len("alternative index", *alternative_index)?;
            }
            CommandResult::ChoiceCompleted(ChoiceOutcome::Unavailable { conflicts }) => {
                self.u8(command_result::CHOICE_COMPLETED);
                self.u8(choice_outcome::UNAVAILABLE);
                self.len("choice conflicts", conflicts.len())?;
                for conflict in conflicts {
                    self.len("alternative index", conflict.alternative_index())?;
                    self.conflicts(conflict.conflicts())?;
                }
            }
            CommandResult::SlotCompleted(SlotOutcome::Held { promise_id, start }) => {
                self.u8(command_result::SLOT_COMPLETED);
                self.u8(slot_outcome::HELD);
                self.promise_id(*promise_id);
                self.i64(*start);
            }
            CommandResult::SlotCompleted(SlotOutcome::Unavailable { attempts }) => {
                self.u8(command_result::SLOT_COMPLETED);
                self.u8(slot_outcome::UNAVAILABLE);
                self.u128(*attempts);
            }
            CommandResult::PromiseCommitted {
                promise_id,
                version,
            } => {
                self.u8(command_result::PROMISE_COMMITTED);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReleased {
                promise_id,
                version,
            } => {
                self.u8(command_result::PROMISE_RELEASED);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReplaced(ReplaceOutcome::Replaced {
                promise_id,
                version,
            }) => {
                self.u8(command_result::PROMISE_REPLACED);
                self.u8(replace_outcome::REPLACED);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            CommandResult::PromiseReplaced(ReplaceOutcome::Unavailable { conflicts }) => {
                self.u8(command_result::PROMISE_REPLACED);
                self.u8(replace_outcome::UNAVAILABLE);
                self.conflicts(conflicts)?;
            }
            CommandResult::ExpirationsProcessed { expired_count } => {
                self.u8(command_result::EXPIRATIONS_PROCESSED);
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
            DomainError::InvalidInterval => domain_error::INVALID_INTERVAL,
            DomainError::UnsortedCapacitySegments => domain_error::UNSORTED_CAPACITY_SEGMENTS,
            DomainError::OverlappingCapacitySegments => domain_error::OVERLAPPING_CAPACITY_SEGMENTS,
            DomainError::InvalidUnitName => domain_error::INVALID_UNIT_NAME,
            DomainError::InvalidUnitScale => domain_error::INVALID_UNIT_SCALE,
            DomainError::InvalidQuantity => domain_error::INVALID_QUANTITY,
            DomainError::QuantityOutOfRange => domain_error::QUANTITY_OUT_OF_RANGE,
            DomainError::QuantityOverflow => domain_error::QUANTITY_OVERFLOW,
            DomainError::IndexOverflow => domain_error::INDEX_OVERFLOW,
            DomainError::InvalidExpiration => domain_error::INVALID_EXPIRATION,
            DomainError::EmptyBundle => domain_error::EMPTY_BUNDLE,
            DomainError::EmptyRelativeBundle => domain_error::EMPTY_RELATIVE_BUNDLE,
            DomainError::EmptyChoice => domain_error::EMPTY_CHOICE,
            DomainError::InvalidSearchRange => domain_error::INVALID_SEARCH_RANGE,
            DomainError::InvalidStep => domain_error::INVALID_STEP,
            DomainError::TimestampOverflow => domain_error::TIMESTAMP_OVERFLOW,
            DomainError::ResourcePoolAlreadyExists => domain_error::RESOURCE_POOL_ALREADY_EXISTS,
            DomainError::ResourcePoolNotFound => domain_error::RESOURCE_POOL_NOT_FOUND,
            DomainError::CapacityExceeded => domain_error::CAPACITY_EXCEEDED,
            DomainError::CapacityRevisionCreatesDeficit => {
                domain_error::CAPACITY_REVISION_CREATES_DEFICIT
            }
            DomainError::PromiseAlreadyExists => domain_error::PROMISE_ALREADY_EXISTS,
            DomainError::PromiseNotFound => domain_error::PROMISE_NOT_FOUND,
            DomainError::InvalidPromiseState => domain_error::INVALID_PROMISE_STATE,
            DomainError::IdempotencyConflict => domain_error::IDEMPOTENCY_CONFLICT,
            DomainError::VersionConflict => domain_error::VERSION_CONFLICT,
            DomainError::VersionOverflow => domain_error::VERSION_OVERFLOW,
            DomainError::SequenceOverflow => domain_error::SEQUENCE_OVERFLOW,
            DomainError::SystemTimeOutOfRange => domain_error::SYSTEM_TIME_OUT_OF_RANGE,
            DomainError::HoldExpired => domain_error::HOLD_EXPIRED,
            DomainError::HoldNotExpired => domain_error::HOLD_NOT_EXPIRED,
            DomainError::InvalidPromiseHistory => domain_error::INVALID_PROMISE_HISTORY,
            DomainError::PublicationRevisionOverflow => domain_error::PUBLICATION_REVISION_OVERFLOW,
        });
    }
    pub(crate) fn event(&mut self, value: &Event) -> Result<(), StorageError> {
        self.u64(value.sequence().get());
        self.i64(value.timestamp());
        self.u8(match value.kind() {
            EventKind::ResourceCreated => event_kind::RESOURCE_CREATED,
            EventKind::CapacityRevised => event_kind::CAPACITY_REVISED,
            EventKind::HoldCreated => event_kind::HOLD_CREATED,
            EventKind::HoldCommitted => event_kind::HOLD_COMMITTED,
            EventKind::PromiseReleased => event_kind::PROMISE_RELEASED,
            EventKind::PromiseReplaced => event_kind::PROMISE_REPLACED,
            EventKind::HoldExpired => event_kind::HOLD_EXPIRED,
            EventKind::DeficitCreated => event_kind::DEFICIT_CREATED,
            EventKind::DeficitChanged => event_kind::DEFICIT_CHANGED,
            EventKind::DeficitResolved => event_kind::DEFICIT_RESOLVED,
        });
        match value.data() {
            EventData::ResourcePool { resource_pool_id } => {
                self.u8(event_data::RESOURCE_POOL);
                self.pool_id(*resource_pool_id);
            }
            EventData::Promise {
                promise_id,
                version,
            } => {
                self.u8(event_data::PROMISE);
                self.promise_id(*promise_id);
                self.u64(version.get());
            }
            EventData::Deficit {
                resource_pool_id,
                interval,
                quantity,
                affected_promise_ids,
            } => {
                self.u8(event_data::DEFICIT);
                self.pool_id(*resource_pool_id);
                self.interval(*interval);
                self.u64(*quantity);
                self.ids("affected promise ids", affected_promise_ids)?;
            }
        }
        Ok(())
    }
}
