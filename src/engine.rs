//! Authoritative in-memory state and admission control.
//!
//! The engine owns resource pools, accepted promises, and the global sequence.
//! It evaluates bundles against active held and committed claims without storing
//! a second authoritative usage counter.

mod availability;
mod capacity_revision;
#[cfg(test)]
mod prepared_transition_tests;

pub use availability::{
    AvailabilityConflict, ChoiceConflict, ChoiceOutcome, HoldOutcome, ReplaceOutcome, Slot,
    SlotOutcome,
};
pub use capacity_revision::{
    AtRiskPromise, CapacityDeficit, CapacityRevisionMode, CapacityRevisionOutcome,
};

use crate::clock::{Clock, SystemClock};
use crate::command::{ClientId, Command, CommandOperation, CommandResult, IdempotencyKey};
use crate::domain::DomainError;
use crate::domain::{
    Bundle, CapacityCurve, Choice, Claim, Interval, Promise, PromiseId, PromiseState, Quantity,
    RelativeBundle, ReplacementState, ResourcePool, ResourcePoolId, SequenceNumber, Timestamp,
    Unit, Version,
};
use crate::event::{Event, EventData, EventKind};
use crate::idempotency::{CommandHash, CommandResponse, IdempotencyRecord, hash_operation};
use crate::index::{Slack, SlackTimeline};
use std::collections::BTreeMap;

/// The result of evaluating candidate claims against one resource pool.
enum PoolAdmission {
    /// Every claim fits and the returned timeline contains their consumption.
    Available(SlackTimeline),
    /// The claims do not fit in one or more intervals.
    Unavailable(Vec<AvailabilityConflict>),
}

/// The result of evaluating every resource pool referenced by one bundle.
enum BundleAdmission {
    /// Every pool fits and the returned timelines are ready to publish.
    Available(BTreeMap<ResourcePoolId, SlackTimeline>),
    /// The bundle does not fit and no timeline may be published.
    Unavailable(Vec<AvailabilityConflict>),
}

/// Prepared result of searching a relative bundle over candidate starts.
enum SlotSearch {
    Found {
        slot: Slot,
        timelines: BTreeMap<ResourcePoolId, SlackTimeline>,
    },
    Unavailable(u128),
}

#[derive(Clone, Copy)]
struct SlotRange {
    earliest: Timestamp,
    latest: Timestamp,
    step: i64,
}

/// The single-node state machine for PromiseDB.
///
/// All mutating operations are serialized through this value. Resource pools
/// and promises are authoritative; temporal usage is derived from active
/// promises when availability is evaluated.
pub struct Engine {
    clock: Box<dyn Clock>,
    state: EngineState,
    publication_revision: PublicationRevision,
}

/// Authoritative and derived engine state, separated from the non-cloneable clock.
#[derive(Debug, PartialEq, Eq)]
struct EngineState {
    resource_pools: BTreeMap<ResourcePoolId, ResourcePool>,
    slack_timelines: BTreeMap<ResourcePoolId, SlackTimeline>,
    promises: BTreeMap<PromiseId, Promise>,
    events: Vec<Event>,
    idempotency_records: BTreeMap<(ClientId, IdempotencyKey), IdempotencyRecord>,
    sequence: SequenceNumber,
}

impl Clone for EngineState {
    fn clone(&self) -> Self {
        #[cfg(test)]
        ENGINE_STATE_CLONES.with(|count| count.set(count.get() + 1));
        Self {
            resource_pools: self.resource_pools.clone(),
            slack_timelines: self.slack_timelines.clone(),
            promises: self.promises.clone(),
            events: self.events.clone(),
            idempotency_records: self.idempotency_records.clone(),
            sequence: self.sequence,
        }
    }
}

#[cfg(test)]
thread_local! {
    static ENGINE_STATE_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static SNAPSHOT_RESTORES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static INDEX_REBUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Resets thread-local snapshot recovery instrumentation.
#[cfg(test)]
pub(crate) fn reset_snapshot_recovery_counts() {
    SNAPSHOT_RESTORES.set(0);
    INDEX_REBUILDS.set(0);
}

/// Returns snapshot restores and index rebuilds on this test thread.
#[cfg(test)]
pub(crate) fn snapshot_recovery_counts() -> (usize, usize) {
    (SNAPSHOT_RESTORES.get(), INDEX_REBUILDS.get())
}

impl EngineState {
    fn empty() -> Self {
        Self {
            resource_pools: BTreeMap::new(),
            slack_timelines: BTreeMap::new(),
            promises: BTreeMap::new(),
            events: Vec::new(),
            idempotency_records: BTreeMap::new(),
            sequence: SequenceNumber::new(0),
        }
    }
}

/// Monotonic publication generation, independent of the domain event sequence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PublicationRevision(u128);

impl PublicationRevision {
    pub(crate) fn new(value: u128) -> Self {
        Self(value)
    }

    pub(crate) fn get(self) -> u128 {
        self.0
    }
}

/// Canonical authoritative state captured at a snapshot boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EngineSnapshot {
    pub(crate) resource_pools: Vec<ResourcePool>,
    pub(crate) promises: Vec<Promise>,
    pub(crate) events: Vec<Event>,
    pub(crate) idempotency_records: Vec<(ClientId, IdempotencyKey, CommandHash, CommandResponse)>,
    pub(crate) sequence: SequenceNumber,
    pub(crate) publication_revision: PublicationRevision,
    pub(crate) events_pruned_through: SequenceNumber,
}

/// A durable, codec-stable description of one first-seen command's effects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableTransition {
    command: Command,
    client_id: ClientId,
    idempotency_key: IdempotencyKey,
    command_hash: CommandHash,
    response: CommandResponse,
    resource_pools: Vec<ResourcePool>,
    promises: Vec<Promise>,
    events: Vec<Event>,
    final_sequence: SequenceNumber,
}

/// One durable transition paired with its authoritative record timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimestampedTransition {
    timestamp: Timestamp,
    transition: DurableTransition,
}

/// One ordered group-commit candidate prepared from a single state clone.
pub(crate) struct PreparedBatch {
    base_revision: PublicationRevision,
    next_revision: PublicationRevision,
    candidate: Option<EngineState>,
    responses: Vec<CommandResponse>,
    durable_items: Vec<TimestampedTransition>,
}

/// Publication failures that must be detected before persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparationError {
    StaleRevision {
        expected: PublicationRevision,
        actual: PublicationRevision,
    },
    RevisionOverflow,
}

/// Validation failures while installing durable effects during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InstallError {
    CommandIdentity,
    CommandHash,
    DuplicateIdempotencyIdentity,
    Sequence,
    EntityIdentity,
    DomainInvariant,
    PublicationRevision,
    Index(DomainError),
}

impl TimestampedTransition {
    pub(crate) fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub(crate) fn transition(&self) -> &DurableTransition {
        &self.transition
    }
}

impl PreparedBatch {
    pub(crate) fn durable_items(&self) -> &[TimestampedTransition] {
        &self.durable_items
    }

    pub(crate) fn into_responses(self) -> Vec<CommandResponse> {
        self.responses
    }
}

impl DurableTransition {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        command: Command,
        client_id: ClientId,
        idempotency_key: IdempotencyKey,
        command_hash: CommandHash,
        response: CommandResponse,
        resource_pools: Vec<ResourcePool>,
        promises: Vec<Promise>,
        events: Vec<Event>,
        final_sequence: SequenceNumber,
    ) -> Self {
        Self {
            command,
            client_id,
            idempotency_key,
            command_hash,
            response,
            resource_pools,
            promises,
            events,
            final_sequence,
        }
    }

    pub(crate) fn command(&self) -> &Command {
        &self.command
    }
    pub(crate) fn client_id(&self) -> &ClientId {
        &self.client_id
    }
    pub(crate) fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }
    pub(crate) fn command_hash(&self) -> CommandHash {
        self.command_hash
    }
    pub(crate) fn response(&self) -> &CommandResponse {
        &self.response
    }
    pub(crate) fn resource_pools(&self) -> &[ResourcePool] {
        &self.resource_pools
    }
    pub(crate) fn promises(&self) -> &[Promise] {
        &self.promises
    }
    pub(crate) fn events(&self) -> &[Event] {
        &self.events
    }
    pub(crate) fn final_sequence(&self) -> SequenceNumber {
        self.final_sequence
    }
}

fn response_references_valid(
    response: &CommandResponse,
    pools: &BTreeMap<ResourcePoolId, ResourcePool>,
    promises: &BTreeMap<PromiseId, Promise>,
    sequence: SequenceNumber,
) -> bool {
    let conflicts_valid = |conflicts: &[AvailabilityConflict]| {
        conflicts.iter().all(|conflict| {
            pools.contains_key(&conflict.resource_pool_id())
                && conflict
                    .conflicting_promise_ids()
                    .iter()
                    .all(|id| promises.contains_key(id))
        })
    };
    match response {
        Err(_) => true,
        Ok(CommandResult::ResourcePoolCreated { resource_pool_id }) => {
            pools.contains_key(resource_pool_id)
        }
        Ok(CommandResult::CapacityRevised(outcome)) => {
            outcome.sequence() <= sequence
                && outcome.deficits().iter().all(|deficit| {
                    pools.contains_key(&deficit.resource_pool_id())
                        && deficit
                            .affected_promise_ids()
                            .iter()
                            .all(|id| promises.contains_key(id))
                })
                && outcome
                    .affected_promise_ids()
                    .iter()
                    .all(|id| promises.contains_key(id))
        }
        Ok(CommandResult::HoldCompleted(HoldOutcome::Held(id))) => promises.contains_key(id),
        Ok(CommandResult::HoldCompleted(HoldOutcome::Unavailable { conflicts })) => {
            conflicts_valid(conflicts)
        }
        Ok(CommandResult::ChoiceCompleted(ChoiceOutcome::Held { promise_id, .. })) => {
            promises.contains_key(promise_id)
        }
        Ok(CommandResult::ChoiceCompleted(ChoiceOutcome::Unavailable { conflicts })) => conflicts
            .iter()
            .all(|conflict| conflicts_valid(conflict.conflicts())),
        Ok(CommandResult::SlotCompleted(SlotOutcome::Held { promise_id, .. })) => {
            promises.contains_key(promise_id)
        }
        Ok(CommandResult::SlotCompleted(SlotOutcome::Unavailable { .. }))
        | Ok(CommandResult::ExpirationsProcessed { .. }) => true,
        Ok(CommandResult::PromiseCommitted {
            promise_id,
            version,
        })
        | Ok(CommandResult::PromiseReleased {
            promise_id,
            version,
        }) => promises
            .get(promise_id)
            .is_some_and(|promise| *version <= promise.version()),
        Ok(CommandResult::PromiseReplaced(ReplaceOutcome::Replaced {
            promise_id,
            version,
        })) => promises
            .get(promise_id)
            .is_some_and(|promise| *version <= promise.version()),
        Ok(CommandResult::PromiseReplaced(ReplaceOutcome::Unavailable { conflicts })) => {
            conflicts_valid(conflicts)
        }
    }
}

impl Engine {
    /// Creates an empty engine backed by the host system clock.
    pub fn new() -> Self {
        Self::with_clock(SystemClock)
    }

    /// Creates an empty engine using the provided clock.
    ///
    /// Supplying the clock makes command timestamps replaceable without changing
    /// the deterministic state transitions or the concrete [`Engine`] type.
    pub fn with_clock(clock: impl Clock + 'static) -> Self {
        Self {
            clock: Box::new(clock),
            state: EngineState::empty(),
            publication_revision: PublicationRevision::default(),
        }
    }

    /// Captures authoritative state in canonical map-key order.
    pub(crate) fn capture_snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            resource_pools: self.state.resource_pools.values().cloned().collect(),
            promises: self.state.promises.values().cloned().collect(),
            events: self.state.events.clone(),
            idempotency_records: self
                .state
                .idempotency_records
                .iter()
                .map(|((client, key), record)| {
                    (
                        client.clone(),
                        key.clone(),
                        record.command_hash(),
                        record.response().clone(),
                    )
                })
                .collect(),
            sequence: self.state.sequence,
            publication_revision: self.publication_revision,
            events_pruned_through: SequenceNumber::new(0),
        }
    }

    /// Validates and restores authoritative snapshot state without derived indexes.
    pub(crate) fn restore_snapshot_unindexed(
        snapshot: EngineSnapshot,
    ) -> Result<Self, InstallError> {
        #[cfg(test)]
        SNAPSHOT_RESTORES.set(SNAPSHOT_RESTORES.get() + 1);
        if snapshot.events_pruned_through.get() != 0 {
            return Err(InstallError::DomainInvariant);
        }
        let sequence = snapshot.sequence;
        let mut resource_pools = BTreeMap::new();
        let mut previous_pool = None;
        for pool in snapshot.resource_pools {
            let id = pool.id();
            if previous_pool.is_some_and(|previous| previous >= id) {
                return Err(InstallError::EntityIdentity);
            }
            previous_pool = Some(id);
            resource_pools.insert(id, pool);
        }
        let mut promises = BTreeMap::new();
        let mut previous_promise = None;
        for promise in snapshot.promises {
            let id = promise.id();
            if previous_promise.is_some_and(|previous| previous >= id)
                || promise.created_sequence().get() == 0
                || promise.created_sequence() > promise.updated_sequence()
                || promise.updated_sequence() > sequence
                || promise
                    .bundle()
                    .claims()
                    .iter()
                    .any(|claim| !resource_pools.contains_key(&claim.pool_id()))
            {
                return Err(InstallError::DomainInvariant);
            }
            previous_promise = Some(id);
            promises.insert(id, promise);
        }
        let mut previous_event_sequence = None;
        for event in &snapshot.events {
            if event.sequence() > sequence
                || previous_event_sequence.is_some_and(|previous| previous > event.sequence())
            {
                return Err(InstallError::Sequence);
            }
            previous_event_sequence = Some(event.sequence());
            let valid = match event.data() {
                EventData::ResourcePool { resource_pool_id } => {
                    resource_pools.contains_key(resource_pool_id)
                }
                EventData::Promise {
                    promise_id,
                    version,
                } => promises
                    .get(promise_id)
                    .is_some_and(|promise| *version <= promise.version()),
                EventData::Deficit {
                    resource_pool_id,
                    affected_promise_ids,
                    ..
                } => {
                    resource_pools.contains_key(resource_pool_id)
                        && affected_promise_ids
                            .iter()
                            .all(|id| promises.contains_key(id))
                }
            };
            if !valid {
                return Err(InstallError::EntityIdentity);
            }
        }
        let mut idempotency_records = BTreeMap::new();
        let mut previous_identity: Option<(ClientId, IdempotencyKey)> = None;
        for (client, key, hash, response) in snapshot.idempotency_records {
            if !response_references_valid(&response, &resource_pools, &promises, sequence) {
                return Err(InstallError::EntityIdentity);
            }
            let identity = (client, key);
            if previous_identity
                .as_ref()
                .is_some_and(|previous| previous >= &identity)
            {
                return Err(InstallError::DuplicateIdempotencyIdentity);
            }
            previous_identity = Some(identity.clone());
            idempotency_records.insert(identity, IdempotencyRecord::new(hash, response));
        }
        Ok(Self {
            clock: Box::new(SystemClock),
            state: EngineState {
                resource_pools,
                slack_timelines: BTreeMap::new(),
                promises,
                events: snapshot.events,
                idempotency_records,
                sequence,
            },
            publication_revision: snapshot.publication_revision,
        })
    }

    /// Returns a resource pool by ID.
    pub fn resource_pool(&self, id: ResourcePoolId) -> Option<&ResourcePool> {
        self.state.resource_pools.get(&id)
    }
    /// Returns the derived slack timeline for a resource pool by ID.
    ///
    /// The timeline is an acceleration index reconstructed from the pool's
    /// capacity curve and active promises; it is not authoritative state.
    pub fn slack_timeline(&self, id: ResourcePoolId) -> Option<&SlackTimeline> {
        self.state.slack_timelines.get(&id)
    }

    /// Returns the latest sequence committed by the engine.
    pub fn sequence(&self) -> SequenceNumber {
        self.state.sequence
    }

    /// Returns a promise by ID.
    pub fn promise(&self, id: PromiseId) -> Option<&Promise> {
        self.state.promises.get(&id)
    }

    /// Returns the number of retained idempotency responses.
    pub fn idempotency_record_count(&self) -> usize {
        self.state.idempotency_records.len()
    }

    /// Returns emitted events whose sequence is at least `from_sequence`.
    ///
    /// Events are returned in global sequence order. Multiple audit events for one
    /// transition may share its sequence and retain their emission order.
    pub fn watch_events(&self, from_sequence: SequenceNumber) -> &[Event] {
        let first = self
            .state
            .events
            .partition_point(|event| event.sequence() < from_sequence);
        &self.state.events[first..]
    }

    /// Installs decoded authoritative effects without executing command admission.
    pub(crate) fn install_transition(
        &mut self,
        transition: DurableTransition,
    ) -> Result<(), InstallError> {
        let next_revision = self
            .publication_revision
            .0
            .checked_add(1)
            .map(PublicationRevision)
            .ok_or(InstallError::PublicationRevision)?;
        if transition.command.client_id() != &transition.client_id
            || transition.command.idempotency_key() != &transition.idempotency_key
        {
            return Err(InstallError::CommandIdentity);
        }
        if hash_operation(transition.command.operation()) != transition.command_hash {
            return Err(InstallError::CommandHash);
        }
        let identity = (
            transition.client_id.clone(),
            transition.idempotency_key.clone(),
        );
        if self.state.idempotency_records.contains_key(&identity) {
            return Err(InstallError::DuplicateIdempotencyIdentity);
        }

        let current_sequence = self.state.sequence;
        if transition.final_sequence < current_sequence {
            return Err(InstallError::Sequence);
        }
        if transition.events.is_empty() {
            if transition.final_sequence != current_sequence {
                return Err(InstallError::Sequence);
            }
        } else {
            let mut previous = current_sequence;
            for event in &transition.events {
                let sequence = event.sequence();
                if sequence < previous {
                    return Err(InstallError::Sequence);
                }
                if sequence > previous {
                    if previous.get().checked_add(1) != Some(sequence.get()) {
                        return Err(InstallError::Sequence);
                    }
                    previous = sequence;
                } else if previous == current_sequence {
                    return Err(InstallError::Sequence);
                }
            }
            if previous != transition.final_sequence {
                return Err(InstallError::Sequence);
            }
        }

        let mut candidate = self.state.clone();
        candidate.slack_timelines.clear();
        let mut previous_pool = None;
        for pool in transition.resource_pools {
            let id = pool.id();
            if previous_pool.is_some_and(|previous| previous >= id) {
                return Err(InstallError::EntityIdentity);
            }
            previous_pool = Some(id);
            candidate.resource_pools.insert(id, pool);
        }
        let mut previous_promise = None;
        for promise in transition.promises {
            let id = promise.id();
            if previous_promise.is_some_and(|previous| previous >= id)
                || promise.created_sequence() > promise.updated_sequence()
                || promise.updated_sequence() > transition.final_sequence
            {
                return Err(InstallError::DomainInvariant);
            }
            previous_promise = Some(id);
            candidate.promises.insert(id, promise);
        }
        if candidate.promises.values().any(|promise| {
            promise
                .bundle()
                .claims()
                .iter()
                .any(|claim| !candidate.resource_pools.contains_key(&claim.pool_id()))
        }) {
            return Err(InstallError::DomainInvariant);
        }
        for event in &transition.events {
            let valid_identity = match event.data() {
                EventData::ResourcePool { resource_pool_id } => {
                    candidate.resource_pools.contains_key(resource_pool_id)
                }
                EventData::Promise { promise_id, .. } => {
                    candidate.promises.contains_key(promise_id)
                }
                EventData::Deficit {
                    resource_pool_id,
                    affected_promise_ids,
                    ..
                } => {
                    candidate.resource_pools.contains_key(resource_pool_id)
                        && affected_promise_ids
                            .iter()
                            .all(|id| candidate.promises.contains_key(id))
                }
            };
            if !valid_identity {
                return Err(InstallError::EntityIdentity);
            }
        }
        candidate.events.extend(transition.events);
        candidate.sequence = transition.final_sequence;
        candidate.idempotency_records.insert(
            identity,
            IdempotencyRecord::new(transition.command_hash, transition.response),
        );
        self.state = candidate;
        self.publication_revision = next_revision;
        Ok(())
    }

    /// Reconstructs every derived slack timeline from authoritative state.
    pub(crate) fn rebuild_slack_timelines(&mut self) -> Result<(), InstallError> {
        #[cfg(test)]
        INDEX_REBUILDS.set(INDEX_REBUILDS.get() + 1);
        let active_promises: Vec<&Promise> = self.state.promises.values().collect();
        let mut timelines = BTreeMap::new();
        for (pool_id, pool) in &self.state.resource_pools {
            let timeline = SlackTimeline::from_capacity_and_promises(
                pool.capacity_curve(),
                *pool_id,
                &active_promises,
            )
            .map_err(|_| InstallError::Index(DomainError::IndexOverflow))?;
            timelines.insert(*pool_id, timeline);
        }
        self.state.slack_timelines = timelines;
        Ok(())
    }

    /// Applies one deterministic command at an authoritative timestamp.
    ///
    /// Keeping `now` outside [`Command`] prevents clients from choosing state-machine
    /// time while allowing replay to reuse the original authoritative timestamp.
    /// Exact retries return their cached response before inspecting `now` or state.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::IdempotencyConflict`] when a client reuses a key for a
    /// different normalized operation, or the original operation-specific error.
    pub fn apply(
        &mut self,
        command: Command,
        now: Timestamp,
    ) -> Result<CommandResult, DomainError> {
        let command_hash = hash_operation(command.operation());
        let identity = (
            command.client_id().clone(),
            command.idempotency_key().clone(),
        );
        if let Some(record) = self.state.idempotency_records.get(&identity) {
            if record.command_hash() != command_hash {
                return Err(DomainError::IdempotencyConflict);
            }
            return record.response().clone();
        }

        let next_revision = self
            .publication_revision
            .0
            .checked_add(1)
            .map(PublicationRevision)
            .ok_or(DomainError::PublicationRevisionOverflow)?;
        let response = self.apply_once(command.into_operation(), now);
        self.state.idempotency_records.insert(
            identity,
            IdempotencyRecord::new(command_hash, response.clone()),
        );
        self.publication_revision = next_revision;
        response
    }

    /// Prepares ordered commands against one candidate cloned from published state.
    pub(crate) fn prepare_batch(
        &self,
        commands: Vec<(Command, Timestamp)>,
    ) -> Result<PreparedBatch, PreparationError> {
        if commands.is_empty() {
            return Ok(PreparedBatch {
                base_revision: self.publication_revision,
                next_revision: self.publication_revision,
                candidate: None,
                responses: Vec::new(),
                durable_items: Vec::new(),
            });
        }

        let mut new_identities = BTreeMap::new();
        for (command, _) in &commands {
            let identity = (
                command.client_id().clone(),
                command.idempotency_key().clone(),
            );
            if !self.state.idempotency_records.contains_key(&identity) {
                new_identities
                    .entry(identity)
                    .or_insert_with(|| hash_operation(command.operation()));
            }
        }
        let revision_delta =
            u128::try_from(new_identities.len()).map_err(|_| PreparationError::RevisionOverflow)?;
        let next_revision = self
            .publication_revision
            .0
            .checked_add(revision_delta)
            .map(PublicationRevision)
            .ok_or(PreparationError::RevisionOverflow)?;

        let base = &self.state;
        let mut candidate_engine = Self {
            clock: Box::new(SystemClock),
            state: base.clone(),
            publication_revision: self.publication_revision,
        };
        let mut responses = Vec::with_capacity(commands.len());
        let mut durable_items = Vec::with_capacity(new_identities.len());

        for (command, timestamp) in commands {
            let command_hash = hash_operation(command.operation());
            let identity = (
                command.client_id().clone(),
                command.idempotency_key().clone(),
            );
            if let Some(record) = candidate_engine.state.idempotency_records.get(&identity) {
                let response = if record.command_hash() == command_hash {
                    record.response().clone()
                } else {
                    Err(DomainError::IdempotencyConflict)
                };
                responses.push(response);
                continue;
            }

            let prior_event_count = candidate_engine.state.events.len();
            let response = candidate_engine.apply_once(command.operation().clone(), timestamp);
            candidate_engine.state.idempotency_records.insert(
                identity,
                IdempotencyRecord::new(command_hash, response.clone()),
            );
            let resource_pools = candidate_engine
                .state
                .resource_pools
                .iter()
                .filter(|(id, value)| base.resource_pools.get(id) != Some(*value))
                .map(|(_, value)| value.clone())
                .collect();
            let promises = candidate_engine
                .state
                .promises
                .iter()
                .filter(|(id, value)| base.promises.get(id) != Some(*value))
                .map(|(_, value)| value.clone())
                .collect();
            let events = candidate_engine.state.events[prior_event_count..].to_vec();
            let transition = DurableTransition {
                client_id: command.client_id().clone(),
                idempotency_key: command.idempotency_key().clone(),
                command,
                command_hash,
                response: response.clone(),
                resource_pools,
                promises,
                events,
                final_sequence: candidate_engine.state.sequence,
            };
            responses.push(response);
            durable_items.push(TimestampedTransition {
                timestamp,
                transition,
            });
        }

        Ok(PreparedBatch {
            base_revision: self.publication_revision,
            next_revision,
            candidate: Some(candidate_engine.state),
            responses,
            durable_items,
        })
    }

    /// Verifies that a prepared batch still targets the published revision.
    pub(crate) fn can_publish(&self, prepared: &PreparedBatch) -> Result<(), PreparationError> {
        if !prepared.durable_items.is_empty() && prepared.base_revision != self.publication_revision
        {
            return Err(PreparationError::StaleRevision {
                expected: prepared.base_revision,
                actual: self.publication_revision,
            });
        }
        Ok(())
    }

    /// Publishes a preflighted batch without validation or arithmetic.
    pub(crate) fn publish_batch(&mut self, prepared: PreparedBatch) -> Vec<CommandResponse> {
        if !prepared.durable_items.is_empty()
            && let Some(candidate) = prepared.candidate
        {
            self.state = candidate;
            self.publication_revision = prepared.next_revision;
        }
        prepared.responses
    }

    fn apply_once(
        &mut self,
        operation: CommandOperation,
        now: Timestamp,
    ) -> Result<CommandResult, DomainError> {
        match operation {
            CommandOperation::CreateResourcePool {
                resource_pool_id,
                display_name,
                unit,
                capacity_curve,
            } => {
                self.create_resource_pool_at(
                    resource_pool_id,
                    display_name,
                    unit,
                    capacity_curve,
                    now,
                )?;
                Ok(CommandResult::ResourcePoolCreated { resource_pool_id })
            }
            CommandOperation::ReviseCapacity {
                resource_pool_id,
                capacity_curve,
                mode,
            } => self
                .revise_capacity_at(resource_pool_id, capacity_curve, mode, now)
                .map(CommandResult::CapacityRevised),
            CommandOperation::Hold {
                promise_id,
                bundle,
                expires_at,
            } => self
                .hold_with_id_at(promise_id, bundle, expires_at, now)
                .map(CommandResult::HoldCompleted),
            CommandOperation::HoldOneOf {
                promise_id,
                choice,
                expires_at,
            } => self
                .hold_choice_at(promise_id, choice, expires_at, now)
                .map(CommandResult::ChoiceCompleted),
            CommandOperation::HoldFirstSlot {
                promise_id,
                relative_bundle,
                earliest_start,
                latest_start,
                step,
                expires_at,
            } => self
                .hold_slot_at(
                    promise_id,
                    relative_bundle,
                    SlotRange {
                        earliest: earliest_start,
                        latest: latest_start,
                        step,
                    },
                    expires_at,
                    now,
                )
                .map(CommandResult::SlotCompleted),
            CommandOperation::Commit {
                promise_id,
                expected_version,
            } => {
                let version = self.commit_at(promise_id, expected_version, now)?;
                Ok(CommandResult::PromiseCommitted {
                    promise_id,
                    version,
                })
            }
            CommandOperation::Release {
                promise_id,
                expected_version,
            } => {
                let version = self.release_at(promise_id, expected_version, now)?;
                Ok(CommandResult::PromiseReleased {
                    promise_id,
                    version,
                })
            }
            CommandOperation::Replace {
                promise_id,
                expected_version,
                new_bundle,
                new_state,
            } => self
                .replace_at(promise_id, expected_version, new_bundle, new_state, now)
                .map(CommandResult::PromiseReplaced),
            CommandOperation::ProcessExpirations => {
                let expired_count = self.process_expirations(now)?;
                Ok(CommandResult::ExpirationsProcessed { expired_count })
            }
        }
    }

    /// Calculates, but does not commit, the next global sequence.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::SequenceOverflow`] when the current sequence is
    /// `u64::MAX`.
    pub(crate) fn next_sequence(&self) -> Result<SequenceNumber, DomainError> {
        self.state.sequence.next()
    }

    /// Expires every due hold in deterministic deadline-and-ID order.
    ///
    /// Each expired hold receives and commits one global sequence. Calling this
    /// method again with the same `now` is safe and produces no transitions.
    ///
    /// # Errors
    ///
    /// Returns an error when sequence or promise version arithmetic overflows,
    /// or when a selected promise unexpectedly disappears.
    fn process_expirations(&mut self, now: Timestamp) -> Result<usize, DomainError> {
        let mut due_holds: Vec<(Timestamp, PromiseId)> = self
            .state
            .promises
            .iter()
            .filter_map(|(promise_id, promise)| match promise.state() {
                PromiseState::Held { expires_at } if expires_at <= now => {
                    Some((expires_at, *promise_id))
                }
                _ => None,
            })
            .collect();

        due_holds.sort_unstable();

        let mut expired_count = 0;
        for (_, promise_id) in due_holds {
            let next_sequence = self.next_sequence()?;
            let mut expired_promise = self
                .state
                .promises
                .get(&promise_id)
                .ok_or(DomainError::PromiseNotFound)?
                .clone();

            expired_promise.expire(now, next_sequence)?;
            let adjusted_timelines = self.restored_timelines(expired_promise.bundle())?;
            let version = expired_promise.version();

            self.state.promises.insert(promise_id, expired_promise);
            self.state.slack_timelines.extend(adjusted_timelines);
            self.state.sequence = next_sequence;
            self.state.events.push(Event::new(
                next_sequence,
                now,
                EventKind::HoldExpired,
                EventData::Promise {
                    promise_id,
                    version,
                },
            ));
            expired_count += 1;
        }

        Ok(expired_count)
    }

    /// Creates a resource pool with a predetermined ID and timestamp.
    ///
    /// Accepting the ID explicitly allows replay to reproduce the original pool
    /// identity. Due hold expirations are processed before the pool is created.
    /// The creation sequence is only published after validation succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration processing fails, the ID already exists,
    /// index construction fails, or the global sequence overflows.
    pub(crate) fn create_resource_pool_at(
        &mut self,
        pool_id: ResourcePoolId,
        display_name: String,
        unit: Unit,
        capacity_curve: CapacityCurve,
        now: Timestamp,
    ) -> Result<ResourcePoolId, DomainError> {
        self.process_expirations(now)?;

        if self.state.resource_pools.contains_key(&pool_id) {
            return Err(DomainError::ResourcePoolAlreadyExists);
        }

        let pool = ResourcePool::with_id(pool_id, display_name, unit, capacity_curve);
        let slack_timeline = SlackTimeline::from_capacity_curve(pool.capacity_curve())
            .map_err(|_| DomainError::IndexOverflow)?;
        let next_sequence = self.next_sequence()?;

        self.state.resource_pools.insert(pool_id, pool);
        self.state.slack_timelines.insert(pool_id, slack_timeline);
        self.state.sequence = next_sequence;
        self.state.events.push(Event::new(
            next_sequence,
            now,
            EventKind::ResourceCreated,
            EventData::ResourcePool {
                resource_pool_id: pool_id,
            },
        ));

        Ok(pool_id)
    }

    /// Creates a resource pool using an automatically generated ID and the clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp, expiration
    /// processing or index construction fails, or the sequence overflows.
    pub fn create_resource_pool(
        &mut self,
        display_name: String,
        unit: Unit,
        capacity_curve: CapacityCurve,
    ) -> Result<ResourcePoolId, DomainError> {
        let now = self.clock.now()?;
        let pool_id = ResourcePoolId::generate();
        self.create_resource_pool_at(pool_id, display_name, unit, capacity_curve, now)
    }

    /// Replaces one pool's capacity curve using an authoritative timestamp.
    ///
    /// The candidate timeline is rebuilt from the replacement curve and all active
    /// promises before any state is published. Strict mode rejects a resulting
    /// deficit; force mode accepts it and reports affected promises.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration processing fails, the pool does not exist,
    /// index reconstruction overflows, strict mode would create a deficit, or the
    /// global sequence is exhausted.
    pub(crate) fn revise_capacity_at(
        &mut self,
        pool_id: ResourcePoolId,
        capacity_curve: CapacityCurve,
        mode: CapacityRevisionMode,
        now: Timestamp,
    ) -> Result<CapacityRevisionOutcome, DomainError> {
        self.process_expirations(now)?;

        let mut revised_pool = self
            .state
            .resource_pools
            .get(&pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?
            .clone();
        let current_timeline = self
            .state
            .slack_timelines
            .get(&pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?;
        let previous_deficits = self.capacity_deficits(pool_id, current_timeline)?;
        let active_promises: Vec<&Promise> = self.state.promises.values().collect();
        let revised_timeline =
            SlackTimeline::from_capacity_and_promises(&capacity_curve, pool_id, &active_promises)
                .map_err(|_| DomainError::IndexOverflow)?;
        let deficits = self.capacity_deficits(pool_id, &revised_timeline)?;

        if mode == CapacityRevisionMode::Strict && !deficits.is_empty() {
            return Err(DomainError::CapacityRevisionCreatesDeficit);
        }

        let next_sequence = self.next_sequence()?;
        revised_pool.replace_capacity_curve(capacity_curve);
        let mut affected_promise_ids: Vec<PromiseId> = deficits
            .iter()
            .flat_map(|deficit| deficit.affected_promise_ids.iter().copied())
            .collect();
        affected_promise_ids.sort_unstable();
        affected_promise_ids.dedup();

        self.state.resource_pools.insert(pool_id, revised_pool);
        self.state.slack_timelines.insert(pool_id, revised_timeline);
        self.state.sequence = next_sequence;
        self.state.events.push(Event::new(
            next_sequence,
            now,
            EventKind::CapacityRevised,
            EventData::ResourcePool {
                resource_pool_id: pool_id,
            },
        ));
        for previous in &previous_deficits {
            if !deficits
                .iter()
                .any(|current| current.interval.overlaps(&previous.interval))
            {
                self.state.events.push(Event::new(
                    next_sequence,
                    now,
                    EventKind::DeficitResolved,
                    Self::deficit_event_data(previous),
                ));
            }
        }
        for current in &deficits {
            let unchanged = previous_deficits.iter().any(|previous| {
                previous.interval == current.interval && previous.quantity == current.quantity
            });
            if unchanged {
                continue;
            }
            let kind = if previous_deficits
                .iter()
                .any(|previous| previous.interval.overlaps(&current.interval))
            {
                EventKind::DeficitChanged
            } else {
                EventKind::DeficitCreated
            };
            self.state.events.push(Event::new(
                next_sequence,
                now,
                kind,
                Self::deficit_event_data(current),
            ));
        }

        Ok(CapacityRevisionOutcome {
            sequence: next_sequence,
            deficits,
            affected_promise_ids,
        })
    }

    /// Replaces one pool's capacity curve using one timestamp from the clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock or deterministic revision transition fails.
    pub fn revise_capacity(
        &mut self,
        pool_id: ResourcePoolId,
        capacity_curve: CapacityCurve,
        mode: CapacityRevisionMode,
    ) -> Result<CapacityRevisionOutcome, DomainError> {
        let now = self.clock.now()?;
        self.revise_capacity_at(pool_id, capacity_curve, mode, now)
    }

    /// Lists active promises overlapping current deficit intervals.
    ///
    /// This is a pure current-state query. Callers that require deadline processing
    /// must first apply [`CommandOperation::ProcessExpirations`]. Optional pool and
    /// time filters restrict which deficits are considered.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing requested pool or index arithmetic overflow.
    pub fn list_at_risk(
        &self,
        resource_pool_id: Option<ResourcePoolId>,
        time_range: Option<Interval>,
    ) -> Result<Vec<AtRiskPromise>, DomainError> {
        if resource_pool_id.is_some_and(|pool_id| !self.state.resource_pools.contains_key(&pool_id))
        {
            return Err(DomainError::ResourcePoolNotFound);
        }

        let mut promises_by_id: BTreeMap<PromiseId, Vec<CapacityDeficit>> = BTreeMap::new();
        for (pool_id, timeline) in &self.state.slack_timelines {
            if resource_pool_id.is_some_and(|requested| requested != *pool_id) {
                continue;
            }

            for deficit in self.capacity_deficits(*pool_id, timeline)? {
                if time_range.is_some_and(|range| !range.overlaps(&deficit.interval)) {
                    continue;
                }
                for promise_id in &deficit.affected_promise_ids {
                    promises_by_id
                        .entry(*promise_id)
                        .or_default()
                        .push(deficit.clone());
                }
            }
        }

        Ok(promises_by_id
            .into_iter()
            .map(|(promise_id, deficits)| AtRiskPromise {
                promise_id,
                deficits,
            })
            .collect())
    }

    /// Explains every interval that prevents a bundle from being admitted.
    ///
    /// This is a pure current-state query and never consumes candidate capacity or
    /// processes deadlines.
    ///
    /// # Errors
    ///
    /// Returns an error when admission evaluation fails.
    pub fn explain_unavailable(
        &self,
        bundle: &Bundle,
    ) -> Result<Vec<AvailabilityConflict>, DomainError> {
        match self.evaluate_bundle_admission(bundle)? {
            BundleAdmission::Available(_) => Ok(Vec::new()),
            BundleAdmission::Unavailable(conflicts) => Ok(conflicts),
        }
    }

    /// Finds the first feasible materialization in an inclusive candidate range.
    ///
    /// Candidates are evaluated deterministically from `earliest_start`, advancing
    /// by `step` while the next start remains at or before `latest_start`. This is a
    /// pure advisory query: it does not process expirations or mutate engine state.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidSearchRange`] for reversed bounds,
    /// [`DomainError::InvalidStep`] for a non-positive step, or an error raised by
    /// timestamp materialization or indexed admission.
    pub fn find_first_slot(
        &self,
        relative_bundle: &RelativeBundle,
        earliest_start: Timestamp,
        latest_start: Timestamp,
        step: i64,
    ) -> Result<Option<Slot>, DomainError> {
        let range = SlotRange {
            earliest: earliest_start,
            latest: latest_start,
            step,
        };
        match self.search_slots(relative_bundle, range)? {
            SlotSearch::Found { slot, .. } => Ok(Some(slot)),
            SlotSearch::Unavailable(_) => Ok(None),
        }
    }

    fn search_slots(
        &self,
        relative_bundle: &RelativeBundle,
        range: SlotRange,
    ) -> Result<SlotSearch, DomainError> {
        if range.earliest > range.latest {
            return Err(DomainError::InvalidSearchRange);
        }
        if range.step <= 0 {
            return Err(DomainError::InvalidStep);
        }

        let mut start = range.earliest;
        let mut attempts = 0_u128;
        loop {
            attempts += 1;
            let bundle = relative_bundle.materialize(start)?;
            if let BundleAdmission::Available(timelines) =
                self.evaluate_bundle_admission(&bundle)?
            {
                return Ok(SlotSearch::Found {
                    slot: Slot { start, bundle },
                    timelines,
                });
            }

            if start == range.latest {
                break;
            }
            let Some(next) = start.checked_add(range.step) else {
                break;
            };
            if next > range.latest {
                break;
            }
            start = next;
        }

        Ok(SlotSearch::Unavailable(attempts))
    }

    /// Searches and holds the first feasible slot under a prepared promise ID.
    ///
    /// Due expirations are processed once before duplicate-ID, deadline, and search
    /// validation. The selected materialized bundle and prepared timelines are
    /// published together through the ordinary accepted-hold transition.
    ///
    /// # Errors
    ///
    /// Returns an error for expiration processing failure, a duplicate promise ID,
    /// invalid deadline or search inputs, timestamp or admission arithmetic failure,
    /// a missing pool, or sequence exhaustion.
    fn hold_slot_at(
        &mut self,
        promise_id: PromiseId,
        relative_bundle: RelativeBundle,
        range: SlotRange,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<SlotOutcome, DomainError> {
        self.process_expirations(now)?;

        if self.state.promises.contains_key(&promise_id) {
            return Err(DomainError::PromiseAlreadyExists);
        }
        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        match self.search_slots(&relative_bundle, range)? {
            SlotSearch::Found { slot, timelines } => {
                let start = slot.start;
                self.accept_hold(promise_id, slot.bundle, expires_at, now, timelines)?;
                Ok(SlotOutcome::Held { promise_id, start })
            }
            SlotSearch::Unavailable(attempts) => Ok(SlotOutcome::Unavailable { attempts }),
        }
    }

    /// Searches and holds the first feasible slot using one clock timestamp.
    ///
    /// The created promise receives an engine-generated ID. Durable callers should
    /// submit [`CommandOperation::HoldFirstSlot`] with a control-plane ID instead.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock or deterministic slot transition fails.
    pub fn hold_first_slot(
        &mut self,
        relative_bundle: RelativeBundle,
        earliest_start: Timestamp,
        latest_start: Timestamp,
        step: i64,
        expires_at: Timestamp,
    ) -> Result<SlotOutcome, DomainError> {
        let now = self.clock.now()?;
        self.hold_slot_at(
            PromiseId::generate(),
            relative_bundle,
            SlotRange {
                earliest: earliest_start,
                latest: latest_start,
                step,
            },
            expires_at,
            now,
        )
    }

    /// Atomically holds a bundle using an authoritative timestamp.
    ///
    /// Due expirations are processed before the candidate bundle is evaluated.
    /// Rejected holds do not create a promise or consume a sequence for the hold
    /// itself; expiration transitions processed first remain committed.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid deadline, a missing pool, arithmetic
    /// overflow, or sequence exhaustion. Insufficient capacity is returned as
    /// [`HoldOutcome::Unavailable`].
    pub(crate) fn hold_at(
        &mut self,
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<HoldOutcome, DomainError> {
        self.hold_with_id_at(PromiseId::generate(), bundle, expires_at, now)
    }

    /// Atomically holds a bundle under an identity prepared by the control API.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate promise ID, invalid deadline, missing pool,
    /// arithmetic overflow, or sequence exhaustion.
    pub(crate) fn hold_with_id_at(
        &mut self,
        promise_id: PromiseId,
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<HoldOutcome, DomainError> {
        self.process_expirations(now)?;

        if self.state.promises.contains_key(&promise_id) {
            return Err(DomainError::PromiseAlreadyExists);
        }
        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        let adjusted_timelines = match self.evaluate_bundle_admission(&bundle)? {
            BundleAdmission::Available(timelines) => timelines,
            BundleAdmission::Unavailable(conflicts) => {
                return Ok(HoldOutcome::Unavailable { conflicts });
            }
        };
        self.accept_hold(promise_id, bundle, expires_at, now, adjusted_timelines)?;
        Ok(HoldOutcome::Held(promise_id))
    }

    /// Holds the first feasible bundle in an ordered choice under a prepared ID.
    ///
    /// Due expirations are processed before alternatives are evaluated. Rejected
    /// alternatives only produce conflict data; their prepared timeline copies are
    /// discarded. Evaluation stops after the first feasible alternative.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate promise ID, invalid deadline, missing pool,
    /// arithmetic overflow, or sequence exhaustion.
    pub(crate) fn hold_choice_at(
        &mut self,
        promise_id: PromiseId,
        choice: Choice,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<ChoiceOutcome, DomainError> {
        self.process_expirations(now)?;

        if self.state.promises.contains_key(&promise_id) {
            return Err(DomainError::PromiseAlreadyExists);
        }
        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        let mut all_conflicts = Vec::with_capacity(choice.alternatives().len());
        for (alternative_index, bundle) in choice.alternatives().iter().enumerate() {
            match self.evaluate_bundle_admission(bundle)? {
                BundleAdmission::Available(timelines) => {
                    self.accept_hold(promise_id, bundle.clone(), expires_at, now, timelines)?;
                    return Ok(ChoiceOutcome::Held {
                        promise_id,
                        alternative_index,
                    });
                }
                BundleAdmission::Unavailable(conflicts) => {
                    all_conflicts.push(ChoiceConflict {
                        alternative_index,
                        conflicts,
                    });
                }
            }
        }

        Ok(ChoiceOutcome::Unavailable {
            conflicts: all_conflicts,
        })
    }

    fn accept_hold(
        &mut self,
        promise_id: PromiseId,
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
        timelines: BTreeMap<ResourcePoolId, SlackTimeline>,
    ) -> Result<(), DomainError> {
        let next_sequence = self.next_sequence()?;
        let promise = Promise::with_id(promise_id, bundle, expires_at, now, next_sequence)?;
        let version = promise.version();

        self.state.promises.insert(promise_id, promise);
        self.state.slack_timelines.extend(timelines);
        self.state.sequence = next_sequence;
        self.state.events.push(Event::new(
            next_sequence,
            now,
            EventKind::HoldCreated,
            EventData::Promise {
                promise_id,
                version,
            },
        ));
        Ok(())
    }

    /// Atomically holds a bundle using one timestamp read from the engine's clock.
    ///
    /// The deterministic transition is delegated to `hold_at`, allowing replay
    /// and future replication to apply a previously chosen timestamp directly.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp or the bundle
    /// cannot be evaluated safely. Insufficient capacity is a normal
    /// [`HoldOutcome::Unavailable`] result.
    pub fn hold(
        &mut self,
        bundle: Bundle,
        expires_at: Timestamp,
    ) -> Result<HoldOutcome, DomainError> {
        let now = self.clock.now()?;
        self.hold_at(bundle, expires_at, now)
    }

    /// Holds the first feasible alternative using one timestamp from the clock.
    ///
    /// The created promise receives an engine-generated ID. Durable callers should
    /// instead submit [`CommandOperation::HoldOneOf`] with a control-plane ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp or an alternative
    /// cannot be evaluated safely. Insufficient capacity for every alternative is a
    /// normal [`ChoiceOutcome::Unavailable`] result.
    pub fn hold_one_of(
        &mut self,
        choice: Choice,
        expires_at: Timestamp,
    ) -> Result<ChoiceOutcome, DomainError> {
        let now = self.clock.now()?;
        self.hold_choice_at(PromiseId::generate(), choice, expires_at, now)
    }

    /// Commits a held promise using an authoritative timestamp.
    ///
    /// Due expirations are applied first. The sequence assigned to the commit is
    /// only published after the promise transition succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration processing fails, the promise does not
    /// exist, its version or state is invalid, or arithmetic overflows.
    pub(crate) fn commit_at(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
        now: Timestamp,
    ) -> Result<Version, DomainError> {
        self.process_expirations(now)?;

        let new_sequence = self.next_sequence()?;
        let promise = self
            .state
            .promises
            .get_mut(&promise_id)
            .ok_or(DomainError::PromiseNotFound)?;

        if promise.state() == PromiseState::Expired {
            return Err(DomainError::HoldExpired);
        }

        let new_version = promise.commit(expected_version, now, new_sequence)?;

        self.state.sequence = new_sequence;
        self.state.events.push(Event::new(
            new_sequence,
            now,
            EventKind::HoldCommitted,
            EventData::Promise {
                promise_id,
                version: new_version,
            },
        ));

        Ok(new_version)
    }

    /// Commits a held promise using one timestamp read from the engine's clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp, the promise
    /// does not exist, its version or state is invalid, or arithmetic overflows.
    pub fn commit(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
    ) -> Result<Version, DomainError> {
        let now = self.clock.now()?;
        self.commit_at(promise_id, expected_version, now)
    }

    /// Releases a held or committed promise using an authoritative timestamp.
    ///
    /// Due expirations are applied first. The sequence assigned to the release is
    /// only published after the promise transition succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration processing fails, the promise does not
    /// exist, its version or state is invalid, or arithmetic overflows.
    pub(crate) fn release_at(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
        now: Timestamp,
    ) -> Result<Version, DomainError> {
        self.process_expirations(now)?;

        let new_sequence = self.next_sequence()?;
        let mut released_promise = self
            .state
            .promises
            .get(&promise_id)
            .ok_or(DomainError::PromiseNotFound)?
            .clone();

        if released_promise.state() == PromiseState::Expired {
            return Err(DomainError::HoldExpired);
        }

        let new_version = released_promise.release(expected_version, now, new_sequence)?;
        let adjusted_timelines = self.restored_timelines(released_promise.bundle())?;

        self.state.promises.insert(promise_id, released_promise);
        self.state.slack_timelines.extend(adjusted_timelines);
        self.state.sequence = new_sequence;
        self.state.events.push(Event::new(
            new_sequence,
            now,
            EventKind::PromiseReleased,
            EventData::Promise {
                promise_id,
                version: new_version,
            },
        ));

        Ok(new_version)
    }

    /// Releases a held or committed promise using one timestamp from the clock.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp, the promise
    /// does not exist, its version or state is invalid, or arithmetic overflows.
    pub fn release(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
    ) -> Result<Version, DomainError> {
        let now = self.clock.now()?;
        self.release_at(promise_id, expected_version, now)
    }

    /// Atomically replaces a live promise using an authoritative timestamp.
    ///
    /// Admission is evaluated against the final state: the old bundle is removed
    /// on temporary timelines before the new bundle is applied. The original
    /// promise and all authoritative timelines remain unchanged if admission fails.
    /// Due expirations processed before replacement remain committed.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration processing fails, the promise does not
    /// exist, its version, state, or new deadline is invalid, or arithmetic
    /// overflows. Insufficient capacity is returned as
    /// [`ReplaceOutcome::Unavailable`].
    pub(crate) fn replace_at(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
        new_bundle: Bundle,
        new_state: ReplacementState,
        now: Timestamp,
    ) -> Result<ReplaceOutcome, DomainError> {
        self.process_expirations(now)?;

        let mut replaced_promise = self
            .state
            .promises
            .get(&promise_id)
            .ok_or(DomainError::PromiseNotFound)?
            .clone();
        if replaced_promise.state() == PromiseState::Expired {
            return Err(DomainError::HoldExpired);
        }

        let old_bundle = replaced_promise.bundle().clone();
        let next_sequence = self.next_sequence()?;
        let new_version = replaced_promise.replace(
            expected_version,
            new_bundle.clone(),
            new_state,
            now,
            next_sequence,
        )?;
        let mut final_timelines = self.restored_timelines(&old_bundle)?;

        let adjusted_timelines =
            match self.evaluate_bundle_with_overrides(&new_bundle, &final_timelines)? {
                BundleAdmission::Available(timelines) => timelines,
                BundleAdmission::Unavailable(mut conflicts) => {
                    for conflict in &mut conflicts {
                        conflict
                            .conflicting_promise_ids
                            .retain(|conflicting_id| *conflicting_id != promise_id);
                    }
                    return Ok(ReplaceOutcome::Unavailable { conflicts });
                }
            };
        final_timelines.extend(adjusted_timelines);

        self.state.promises.insert(promise_id, replaced_promise);
        self.state.slack_timelines.extend(final_timelines);
        self.state.sequence = next_sequence;
        self.state.events.push(Event::new(
            next_sequence,
            now,
            EventKind::PromiseReplaced,
            EventData::Promise {
                promise_id,
                version: new_version,
            },
        ));

        Ok(ReplaceOutcome::Replaced {
            promise_id,
            version: new_version,
        })
    }

    /// Atomically replaces a live promise using one timestamp from the clock.
    ///
    /// The promise keeps its ID and creation sequence, receives the requested live
    /// state and bundle, and advances by one local version on success.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp or replacement
    /// validation fails. Insufficient capacity is a normal
    /// [`ReplaceOutcome::Unavailable`] result.
    pub fn replace(
        &mut self,
        promise_id: PromiseId,
        expected_version: Version,
        new_bundle: Bundle,
        new_state: ReplacementState,
    ) -> Result<ReplaceOutcome, DomainError> {
        let now = self.clock.now()?;
        self.replace_at(promise_id, expected_version, new_bundle, new_state, now)
    }

    /// Checks an atomic bundle against committed usage in every referenced pool.
    ///
    /// Claims are grouped by pool so overlapping candidate claims are evaluated
    /// together. Due hold expirations must be processed before calling this
    /// function.
    #[cfg(test)]
    fn check_availability(&self, bundle: &Bundle) -> Result<bool, DomainError> {
        let mut claims_by_pool: BTreeMap<ResourcePoolId, Vec<&Claim>> = BTreeMap::new();

        for claim in bundle.claims() {
            claims_by_pool
                .entry(claim.pool_id())
                .or_default()
                .push(claim);
        }

        for (pool_id, candidate_claims) in claims_by_pool {
            if !self.check_pool_availability(pool_id, &candidate_claims)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Checks all candidate claims for one pool as a single hypothetical change.
    ///
    /// The timeline is divided at every relevant claim boundary. Active and
    /// candidate usage is recomputed for each resulting half-open segment using
    /// checked arithmetic.
    #[cfg(test)]
    fn check_pool_availability(
        &self,
        pool_id: ResourcePoolId,
        candidate_claims: &[&Claim],
    ) -> Result<bool, DomainError> {
        let pool = self
            .state
            .resource_pools
            .get(&pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?;

        let mut active_claims: Vec<&Claim> = Vec::new();
        let mut breakpoints = Vec::new();

        for candidate_claim in candidate_claims {
            let interval = candidate_claim.interval();
            breakpoints.push(interval.start());
            breakpoints.push(interval.end());
        }

        for capacity_segment in pool.capacity_curve().segments() {
            let interval = capacity_segment.interval();
            breakpoints.push(interval.start());
            breakpoints.push(interval.end());
        }

        for promise in self.state.promises.values() {
            if !matches!(
                promise.state(),
                PromiseState::Held { .. } | PromiseState::Committed
            ) {
                continue;
            }

            for active_claim in promise.bundle().claims() {
                if active_claim.pool_id() != pool_id {
                    continue;
                }

                let active_interval = active_claim.interval();
                if !candidate_claims
                    .iter()
                    .any(|candidate_claim| active_interval.overlaps(&candidate_claim.interval()))
                {
                    continue;
                }

                active_claims.push(active_claim);
                breakpoints.push(active_interval.start());
                breakpoints.push(active_interval.end());
            }
        }

        breakpoints.sort_unstable();
        breakpoints.dedup();

        for segment in breakpoints.windows(2) {
            let segment_start = segment[0];
            let mut active_usage: Quantity = 0;
            let mut candidate_usage: Quantity = 0;

            for active_claim in &active_claims {
                if active_claim.interval().contains(segment_start) {
                    active_usage = active_usage
                        .checked_add(active_claim.quantity())
                        .ok_or(DomainError::QuantityOverflow)?;
                }
            }

            for candidate_claim in candidate_claims {
                if candidate_claim.interval().contains(segment_start) {
                    candidate_usage = candidate_usage
                        .checked_add(candidate_claim.quantity())
                        .ok_or(DomainError::QuantityOverflow)?;
                }
            }

            if candidate_usage == 0 {
                continue;
            }

            let final_usage = active_usage
                .checked_add(candidate_usage)
                .ok_or(DomainError::QuantityOverflow)?;

            if final_usage > pool.capacity_at(segment_start) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn evaluate_pool_admission(
        &self,
        pool_id: ResourcePoolId,
        candidate_claims: &[&Claim],
    ) -> Result<PoolAdmission, DomainError> {
        let timeline = self
            .slack_timeline(pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?;
        self.evaluate_pool_on_timeline(pool_id, candidate_claims, timeline, None)
    }

    /// Evaluates one pool against a caller-provided base timeline.
    ///
    /// Replace uses this entry point after restoring the old bundle on temporary
    /// timeline copies. The provided timeline is cloned and never mutated.
    fn evaluate_pool_on_timeline(
        &self,
        pool_id: ResourcePoolId,
        candidate_claims: &[&Claim],
        base_timeline: &SlackTimeline,
        deficit_floor: Option<&SlackTimeline>,
    ) -> Result<PoolAdmission, DomainError> {
        let mut demand_events: Vec<(Timestamp, i128)> = Vec::new();

        for claim in candidate_claims {
            let interval = claim.interval();
            let quantity = i128::from(claim.quantity());
            demand_events.push((interval.start(), quantity));
            demand_events.push((interval.end(), -quantity));
        }

        for point in base_timeline
            .effective_points()
            .map_err(|_| DomainError::IndexOverflow)?
        {
            if candidate_claims
                .iter()
                .any(|claim| claim.interval().contains(point.timestamp()))
            {
                demand_events.push((point.timestamp(), 0));
            }
        }

        demand_events.sort_unstable_by_key(|event| event.0);
        let mut normalized_events: Vec<(Timestamp, i128)> = Vec::new();

        for (timestamp, delta) in demand_events {
            if let Some((last_timestamp, last_delta)) = normalized_events.last_mut()
                && *last_timestamp == timestamp
            {
                *last_delta = last_delta
                    .checked_add(delta)
                    .ok_or(DomainError::QuantityOverflow)?;
                continue;
            }

            normalized_events.push((timestamp, delta));
        }

        let mut conflicts: Vec<AvailabilityConflict> = Vec::new();
        let mut adjustments: Vec<(Interval, i128)> = Vec::new();
        let mut current_demand: i128 = 0;

        for window in normalized_events.windows(2) {
            current_demand = current_demand
                .checked_add(window[0].1)
                .ok_or(DomainError::QuantityOverflow)?;
            let required_quantity =
                Quantity::try_from(current_demand).map_err(|_| DomainError::QuantityOverflow)?;
            if required_quantity == 0 {
                continue;
            }

            let interval = Interval::new(window[0].0, window[1].0)?;
            let slack = base_timeline
                .slack_at(interval.start())
                .map_err(|_| DomainError::IndexOverflow)?;
            let wide_slack = i128::from(slack);
            let final_slack = wide_slack
                .checked_sub(current_demand)
                .ok_or(DomainError::QuantityOverflow)?;
            let minimum_allowed_slack = match deficit_floor {
                Some(timeline) => i128::from(
                    timeline
                        .slack_at(interval.start())
                        .map_err(|_| DomainError::IndexOverflow)?
                        .min(0),
                ),
                None => 0,
            };
            let available_quantity = if slack <= 0 {
                0
            } else {
                Quantity::try_from(slack).map_err(|_| DomainError::IndexOverflow)?
            };

            if final_slack < minimum_allowed_slack {
                let conflicting_promise_ids = self
                    .state
                    .promises
                    .iter()
                    .filter_map(|(promise_id, promise)| {
                        let active = matches!(
                            promise.state(),
                            PromiseState::Held { .. } | PromiseState::Committed
                        );
                        let overlaps = promise.bundle().claims().iter().any(|claim| {
                            claim.pool_id() == pool_id && claim.interval().overlaps(&interval)
                        });
                        (active && overlaps).then_some(*promise_id)
                    })
                    .collect();
                let conflict = AvailabilityConflict {
                    resource_pool_id: pool_id,
                    blocking_interval: interval,
                    required_quantity,
                    available_quantity,
                    deficit_quantity: required_quantity - available_quantity,
                    conflicting_promise_ids,
                };

                if let Some(previous) = conflicts.last_mut()
                    && previous.blocking_interval.end() == interval.start()
                    && previous.required_quantity == conflict.required_quantity
                    && previous.available_quantity == conflict.available_quantity
                    && previous.deficit_quantity == conflict.deficit_quantity
                    && previous.conflicting_promise_ids == conflict.conflicting_promise_ids
                {
                    previous.blocking_interval =
                        Interval::new(previous.blocking_interval.start(), interval.end())?;
                } else {
                    conflicts.push(conflict);
                }
            }

            adjustments.push((interval, current_demand));
        }

        if !conflicts.is_empty() {
            return Ok(PoolAdmission::Unavailable(conflicts));
        }

        let adjustments: Vec<(Interval, Slack)> = adjustments
            .into_iter()
            .map(|(interval, demand)| {
                Slack::try_from(demand)
                    .map(|demand| (interval, demand))
                    .map_err(|_| DomainError::IndexOverflow)
            })
            .collect::<Result<_, _>>()?;
        let mut slack_timeline = base_timeline.clone();
        for (interval, demand) in adjustments {
            let delta = demand.checked_neg().ok_or(DomainError::IndexOverflow)?;
            slack_timeline
                .apply_delta(interval, delta)
                .map_err(|_| DomainError::IndexOverflow)?;
        }

        Ok(PoolAdmission::Available(slack_timeline))
    }

    /// Evaluates every pool in a bundle against current engine timelines.
    fn evaluate_bundle_admission(&self, bundle: &Bundle) -> Result<BundleAdmission, DomainError> {
        self.evaluate_bundle_with_overrides(bundle, &BTreeMap::new())
    }

    /// Evaluates a bundle using temporary timelines when an override is present.
    ///
    /// Pools absent from `overrides` use the engine's current timeline.
    /// Replace supplies restored old-bundle timelines through this map.
    fn evaluate_bundle_with_overrides(
        &self,
        bundle: &Bundle,
        overrides: &BTreeMap<ResourcePoolId, SlackTimeline>,
    ) -> Result<BundleAdmission, DomainError> {
        let mut claims_by_pool: BTreeMap<ResourcePoolId, Vec<&Claim>> = BTreeMap::new();
        for claim in bundle.claims() {
            claims_by_pool
                .entry(claim.pool_id())
                .or_default()
                .push(claim);
        }

        let mut adjusted_timelines = BTreeMap::new();
        let mut conflicts = Vec::new();
        for (pool_id, claims) in claims_by_pool {
            let pool_admission = if let Some(timeline) = overrides.get(&pool_id) {
                let current_timeline = self
                    .slack_timeline(pool_id)
                    .ok_or(DomainError::ResourcePoolNotFound)?;
                self.evaluate_pool_on_timeline(pool_id, &claims, timeline, Some(current_timeline))?
            } else {
                self.evaluate_pool_admission(pool_id, &claims)?
            };
            match pool_admission {
                PoolAdmission::Available(timeline) => {
                    adjusted_timelines.insert(pool_id, timeline);
                }
                PoolAdmission::Unavailable(pool_conflicts) => {
                    conflicts.extend(pool_conflicts);
                }
            }
        }

        if conflicts.is_empty() {
            Ok(BundleAdmission::Available(adjusted_timelines))
        } else {
            conflicts.sort_unstable_by_key(|conflict| {
                (
                    conflict.blocking_interval().start(),
                    conflict.resource_pool_id(),
                    conflict.blocking_interval().end(),
                )
            });
            Ok(BundleAdmission::Unavailable(conflicts))
        }
    }

    fn deficit_event_data(deficit: &CapacityDeficit) -> EventData {
        EventData::Deficit {
            resource_pool_id: deficit.resource_pool_id,
            interval: deficit.interval,
            quantity: deficit.quantity,
            affected_promise_ids: deficit.affected_promise_ids.clone(),
        }
    }

    /// Converts index-level negative slack into public capacity deficits.
    fn capacity_deficits(
        &self,
        pool_id: ResourcePoolId,
        timeline: &SlackTimeline,
    ) -> Result<Vec<CapacityDeficit>, DomainError> {
        timeline
            .deficit_intervals()
            .map_err(|_| DomainError::IndexOverflow)?
            .into_iter()
            .map(|deficit| {
                let interval = deficit.interval();
                let quantity = deficit.amount();
                let affected_promise_ids = self
                    .state
                    .promises
                    .iter()
                    .filter_map(|(promise_id, promise)| {
                        let active = matches!(
                            promise.state(),
                            PromiseState::Held { .. } | PromiseState::Committed
                        );
                        let overlaps = promise.bundle().claims().iter().any(|claim| {
                            claim.pool_id() == pool_id && claim.interval().overlaps(&interval)
                        });
                        (active && overlaps).then_some(*promise_id)
                    })
                    .collect();

                Ok(CapacityDeficit {
                    resource_pool_id: pool_id,
                    interval,
                    quantity,
                    affected_promise_ids,
                })
            })
            .collect()
    }

    /// Prepares timeline copies after active bundle usage is removed.
    ///
    /// This method never mutates engine state. Its returned map may be published
    /// only after the corresponding release or expiration transition succeeds.
    fn restored_timelines(
        &self,
        bundle: &Bundle,
    ) -> Result<BTreeMap<ResourcePoolId, SlackTimeline>, DomainError> {
        let mut claims_by_pool: BTreeMap<ResourcePoolId, Vec<&Claim>> = BTreeMap::new();
        for claim in bundle.claims() {
            claims_by_pool
                .entry(claim.pool_id())
                .or_default()
                .push(claim);
        }

        let mut restored_timelines = BTreeMap::new();
        for (pool_id, claims) in claims_by_pool {
            let mut timeline = self
                .state
                .slack_timelines
                .get(&pool_id)
                .ok_or(DomainError::ResourcePoolNotFound)?
                .clone();
            for claim in claims {
                let quantity =
                    Slack::try_from(claim.quantity()).map_err(|_| DomainError::IndexOverflow)?;
                timeline
                    .apply_delta(claim.interval(), quantity)
                    .map_err(|_| DomainError::IndexOverflow)?;
            }
            restored_timelines.insert(pool_id, timeline);
        }

        Ok(restored_timelines)
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{CapacitySegment, MAX_QUANTITY, RelativeClaim};

    const NOW: Timestamp = 0;
    const EXPIRES_AT: Timestamp = 1_000;

    #[derive(Clone, Copy)]
    struct FixedClock(Timestamp);

    impl Clock for FixedClock {
        fn now(&self) -> Result<Timestamp, DomainError> {
            Ok(self.0)
        }
    }

    fn unit(name: &str, subunits_per_unit: u64) -> Unit {
        Unit::new(name.into(), subunits_per_unit).expect("the unit should be valid")
    }

    fn constant_capacity_curve(capacity: Quantity) -> CapacityCurve {
        let interval = Interval::new(Timestamp::MIN, Timestamp::MAX)
            .expect("the constant-capacity interval should be valid");
        CapacityCurve::from_sorted(vec![CapacitySegment::new(interval, capacity)])
            .expect("the constant capacity curve should be valid")
    }

    fn engine_with_pool(capacity: Quantity) -> (Engine, ResourcePoolId) {
        let mut engine = Engine::new();
        let pool = ResourcePool::new(
            "Test pool".into(),
            unit("units", 1),
            constant_capacity_curve(capacity),
        );
        let pool_id = pool.id();
        let timeline = SlackTimeline::from_capacity_curve(pool.capacity_curve())
            .expect("the slack timeline should be created");
        engine.state.resource_pools.insert(pool_id, pool);
        engine.state.slack_timelines.insert(pool_id, timeline);
        (engine, pool_id)
    }

    fn variable_capacity_curve() -> CapacityCurve {
        CapacityCurve::from_sorted(vec![
            CapacitySegment::new(
                Interval::new(0, 10).expect("the first interval should be valid"),
                10,
            ),
            CapacitySegment::new(
                Interval::new(10, 20).expect("the second interval should be valid"),
                5,
            ),
        ])
        .expect("the variable capacity curve should be valid")
    }

    fn create_pool_with_capacity_curve(
        engine: &mut Engine,
        capacity_curve: CapacityCurve,
    ) -> ResourcePoolId {
        let pool_id = ResourcePoolId::generate();
        engine
            .create_resource_pool_at(
                pool_id,
                "Variable pool".into(),
                unit("units", 1),
                capacity_curve,
                NOW,
            )
            .expect("the resource pool should be created");
        pool_id
    }

    fn engine_with_capacity_curve(capacity_curve: CapacityCurve) -> (Engine, ResourcePoolId) {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = create_pool_with_capacity_curve(&mut engine, capacity_curve);
        (engine, pool_id)
    }

    fn claim(
        pool_id: ResourcePoolId,
        start: Timestamp,
        end: Timestamp,
        quantity: Quantity,
    ) -> Claim {
        let interval = Interval::new(start, end).expect("the interval should be valid");
        Claim::new(pool_id, interval, quantity).expect("the claim should be valid")
    }

    fn bundle(claims: Vec<Claim>) -> Bundle {
        Bundle::new(claims).expect("the bundle should be valid")
    }

    fn relative_bundle(claims: Vec<RelativeClaim>) -> RelativeBundle {
        RelativeBundle::new(claims).expect("the relative bundle should be valid")
    }

    fn relative_claim(
        pool_id: ResourcePoolId,
        start_offset: i64,
        end_offset: i64,
        quantity: Quantity,
    ) -> RelativeClaim {
        RelativeClaim::new(pool_id, start_offset, end_offset, quantity)
            .expect("the relative claim should be valid")
    }

    fn choice(alternatives: Vec<Bundle>) -> Choice {
        Choice::new(alternatives).expect("the choice should be valid")
    }

    fn command_with_key(key: &str, operation: CommandOperation) -> Command {
        Command::new(
            crate::command::ClientId::new("test-client"),
            crate::command::IdempotencyKey::new(key),
            operation,
        )
    }

    fn command(operation: CommandOperation) -> Command {
        command_with_key(&format!("command-{operation:?}"), operation)
    }

    fn add_held_promise_at(
        engine: &mut Engine,
        claim: Claim,
        expires_at: Timestamp,
        sequence: u64,
    ) -> PromiseId {
        let bundle = bundle(vec![claim]);
        let adjusted_timelines = match engine
            .evaluate_bundle_admission(&bundle)
            .expect("the held promise should be evaluated")
        {
            BundleAdmission::Available(timelines) => timelines,
            BundleAdmission::Unavailable(_) => panic!("the held promise should fit"),
        };
        let promise = Promise::with_id(
            PromiseId::generate(),
            bundle,
            expires_at,
            NOW,
            SequenceNumber::new(sequence),
        )
        .expect("the promise should be valid");
        let promise_id = promise.id();
        engine.state.promises.insert(promise_id, promise);
        engine.state.slack_timelines.extend(adjusted_timelines);
        engine.state.sequence = SequenceNumber::new(sequence);
        promise_id
    }

    fn add_held_promise(engine: &mut Engine, claim: Claim, sequence: u64) -> PromiseId {
        add_held_promise_at(engine, claim, EXPIRES_AT, sequence)
    }

    fn indexed_availability(engine: &Engine, bundle: &Bundle) -> Result<bool, DomainError> {
        match engine.evaluate_bundle_admission(bundle)? {
            BundleAdmission::Available(_) => Ok(true),
            BundleAdmission::Unavailable(_) => Ok(false),
        }
    }

    fn held_promise_id(outcome: HoldOutcome) -> PromiseId {
        match outcome {
            HoldOutcome::Held(promise_id) => promise_id,
            HoldOutcome::Unavailable { .. } => panic!("the bundle should be held"),
        }
    }

    #[test]
    fn finds_the_exact_earliest_slot() {
        let (engine, pool_id) = engine_with_pool(1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]);

        let slot = engine
            .find_first_slot(&relative, 20, 40, 5)
            .unwrap()
            .expect("the earliest slot should fit");

        assert_eq!(slot.start(), 20);
        assert_eq!(
            slot.bundle().claims()[0].interval(),
            Interval::new(20, 30).unwrap()
        );
    }

    #[test]
    fn advances_by_step_and_includes_the_latest_start() {
        let (mut engine, pool_id) = engine_with_pool(1);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 1), 1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]);

        let slot = engine
            .find_first_slot(&relative, 0, 10, 5)
            .unwrap()
            .expect("the inclusive latest candidate should fit");

        assert_eq!(slot.start(), 10);
    }

    #[test]
    fn returns_none_when_no_slot_is_feasible() {
        let (mut engine, pool_id) = engine_with_pool(1);
        add_held_promise(&mut engine, claim(pool_id, 0, 30, 1), 1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]);

        assert_eq!(engine.find_first_slot(&relative, 0, 20, 10), Ok(None));
    }

    #[test]
    fn validates_slot_search_inputs() {
        let (engine, pool_id) = engine_with_pool(1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 1, 1)]);

        assert_eq!(
            engine.find_first_slot(&relative, 2, 1, 1),
            Err(DomainError::InvalidSearchRange)
        );
        assert_eq!(
            engine.find_first_slot(&relative, 0, 1, 0),
            Err(DomainError::InvalidStep)
        );
        assert_eq!(
            engine.find_first_slot(&relative, 0, 1, -1),
            Err(DomainError::InvalidStep)
        );
    }

    #[test]
    fn stops_safely_when_the_next_candidate_overflows() {
        let (engine, pool_id) = engine_with_pool(0);
        let relative = relative_bundle(vec![relative_claim(pool_id, -1, 0, 1)]);

        assert_eq!(
            engine.find_first_slot(&relative, Timestamp::MAX - 1, Timestamp::MAX, 2),
            Ok(None)
        );
    }

    #[test]
    fn reports_slot_materialization_overflow() {
        let (engine, pool_id) = engine_with_pool(1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 1, 1)]);

        assert_eq!(
            engine.find_first_slot(&relative, Timestamp::MAX, Timestamp::MAX, 1),
            Err(DomainError::TimestampOverflow)
        );
    }

    #[test]
    fn finds_a_slot_across_variable_capacity() {
        let curve = CapacityCurve::from_sorted(vec![
            CapacitySegment::new(Interval::new(0, 10).unwrap(), 5),
            CapacitySegment::new(Interval::new(10, 20).unwrap(), 10),
        ])
        .unwrap();
        let (engine, pool_id) = engine_with_capacity_curve(curve);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 10, 6)]);

        let slot = engine
            .find_first_slot(&relative, 0, 10, 10)
            .unwrap()
            .expect("the higher-capacity segment should fit");

        assert_eq!(slot.start(), 10);
        assert_eq!(
            slot.bundle().claims()[0].interval(),
            Interval::new(10, 20).unwrap()
        );
    }

    #[test]
    fn searches_multiple_pools_with_relative_offsets_atomically() {
        let (mut engine, first_pool) = engine_with_pool(1);
        let second_pool = create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(1));
        add_held_promise(&mut engine, claim(first_pool, 0, 5, 1), 2);
        add_held_promise(&mut engine, claim(second_pool, 5, 10, 1), 3);
        let relative = relative_bundle(vec![
            relative_claim(first_pool, 0, 5, 1),
            relative_claim(second_pool, -5, 0, 1),
        ]);

        let slot = engine
            .find_first_slot(&relative, 0, 5, 5)
            .unwrap()
            .expect("the second multi-pool candidate should fit");

        assert_eq!(slot.start(), 5);
        assert_eq!(
            slot.bundle().claims()[0].interval(),
            Interval::new(5, 10).unwrap()
        );
        assert_eq!(
            slot.bundle().claims()[1].interval(),
            Interval::new(0, 5).unwrap()
        );
    }

    #[test]
    fn advisory_slot_search_does_not_mutate_state() {
        let (engine, pool_id) = engine_with_pool(1);
        let relative = relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]);
        let sequence = engine.sequence();
        let slack = engine.slack_timeline(pool_id).unwrap().clone();

        assert!(
            engine
                .find_first_slot(&relative, 0, 10, 1)
                .unwrap()
                .is_some()
        );
        assert_eq!(engine.sequence(), sequence);
        assert!(engine.state.promises.is_empty());
        assert!(engine.state.events.is_empty());
        assert_eq!(engine.slack_timeline(pool_id), Some(&slack));
    }

    #[test]
    fn authoritative_slot_hold_publishes_the_materialized_bundle() {
        let (mut engine, pool_id) = engine_with_pool(1);
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldFirstSlot {
                    promise_id,
                    relative_bundle: relative_bundle(vec![relative_claim(pool_id, -2, 3, 1)]),
                    earliest_start: 10,
                    latest_start: 20,
                    step: 5,
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .unwrap();

        assert_eq!(
            result,
            CommandResult::SlotCompleted(SlotOutcome::Held {
                promise_id,
                start: 10,
            })
        );
        assert_eq!(
            engine.promise(promise_id).unwrap().bundle().claims()[0].interval(),
            Interval::new(8, 13).unwrap()
        );
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(10), Ok(0));
        assert_eq!(
            engine.state.events.last().unwrap().kind(),
            EventKind::HoldCreated
        );
    }

    #[test]
    fn slot_hold_processes_expirations_before_searching() {
        let (mut engine, pool_id) = engine_with_pool(1);
        let expired_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 1), 10, 1);
        let promise_id = PromiseId::generate();
        let outcome = engine
            .hold_slot_at(
                promise_id,
                relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]),
                SlotRange {
                    earliest: 0,
                    latest: 0,
                    step: 1,
                },
                100,
                10,
            )
            .unwrap();

        assert_eq!(
            outcome,
            SlotOutcome::Held {
                promise_id,
                start: 0
            }
        );
        assert_eq!(
            engine.promise(expired_id).unwrap().state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 3);
        assert_eq!(
            engine
                .state
                .events
                .iter()
                .map(Event::kind)
                .collect::<Vec<_>>(),
            vec![EventKind::HoldExpired, EventKind::HoldCreated]
        );
    }

    #[test]
    fn exact_slot_retry_returns_the_cached_outcome() {
        let (mut engine, pool_id) = engine_with_pool(1);
        let operation = CommandOperation::HoldFirstSlot {
            promise_id: PromiseId::generate(),
            relative_bundle: relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]),
            earliest_start: 0,
            latest_start: 10,
            step: 5,
            expires_at: EXPIRES_AT,
        };

        let first = engine
            .apply(command_with_key("slot", operation.clone()), NOW)
            .unwrap();
        let sequence = engine.sequence();
        let event_count = engine.state.events.len();
        let second = engine
            .apply(command_with_key("slot", operation), EXPIRES_AT)
            .unwrap();

        assert_eq!(first, second);
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.state.events.len(), event_count);
        assert_eq!(engine.state.promises.len(), 1);
    }

    #[test]
    fn changed_slot_payload_conflicts_with_an_idempotency_key() {
        let (mut engine, pool_id) = engine_with_pool(1);
        let promise_id = PromiseId::generate();
        let operation = |step| CommandOperation::HoldFirstSlot {
            promise_id,
            relative_bundle: relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]),
            earliest_start: 0,
            latest_start: 10,
            step,
            expires_at: EXPIRES_AT,
        };

        engine
            .apply(command_with_key("slot", operation(5)), NOW)
            .unwrap();
        assert_eq!(
            engine.apply(command_with_key("slot", operation(10)), NOW),
            Err(DomainError::IdempotencyConflict)
        );
    }

    #[test]
    fn unavailable_slot_hold_reports_attempts_without_mutation() {
        let (mut engine, pool_id) = engine_with_pool(0);
        let sequence = engine.sequence();
        let slack = engine.slack_timeline(pool_id).unwrap().clone();
        let outcome = engine
            .hold_slot_at(
                PromiseId::generate(),
                relative_bundle(vec![relative_claim(pool_id, 0, 10, 1)]),
                SlotRange {
                    earliest: 0,
                    latest: 20,
                    step: 10,
                },
                EXPIRES_AT,
                NOW,
            )
            .unwrap();

        assert_eq!(outcome, SlotOutcome::Unavailable { attempts: 3 });
        assert_eq!(engine.sequence(), sequence);
        assert!(engine.state.promises.is_empty());
        assert!(engine.state.events.is_empty());
        assert_eq!(engine.slack_timeline(pool_id), Some(&slack));
    }

    #[test]
    fn public_slot_hold_uses_the_injected_clock() {
        let (mut engine, pool_id) = engine_with_pool(1);
        engine.clock = Box::new(FixedClock(100));

        assert_eq!(
            engine.hold_first_slot(
                relative_bundle(vec![relative_claim(pool_id, 0, 1, 1)]),
                0,
                0,
                1,
                100,
            ),
            Err(DomainError::InvalidExpiration)
        );
    }

    #[test]
    fn apply_dispatches_mutations_with_control_plane_ids() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = ResourcePoolId::generate();
        let promise_id = PromiseId::generate();

        let created = engine
            .apply(
                command(CommandOperation::CreateResourcePool {
                    resource_pool_id: pool_id,
                    display_name: "Command pool".into(),
                    unit: unit("units", 1),
                    capacity_curve: constant_capacity_curve(10),
                }),
                NOW,
            )
            .expect("the pool should be created");
        assert_eq!(
            created,
            CommandResult::ResourcePoolCreated {
                resource_pool_id: pool_id
            }
        );

        let held = engine
            .apply(
                command(CommandOperation::Hold {
                    promise_id,
                    bundle: bundle(vec![claim(pool_id, 0, 10, 4)]),
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .expect("the bundle should be held");
        assert_eq!(
            held,
            CommandResult::HoldCompleted(HoldOutcome::Held(promise_id))
        );

        let held_version = engine.promise(promise_id).unwrap().version();
        let committed = engine
            .apply(
                command(CommandOperation::Commit {
                    promise_id,
                    expected_version: held_version,
                }),
                NOW,
            )
            .expect("the hold should commit");
        let CommandResult::PromiseCommitted {
            version: committed_version,
            ..
        } = committed
        else {
            panic!("the result should report a commit");
        };

        let replaced = engine
            .apply(
                command(CommandOperation::Replace {
                    promise_id,
                    expected_version: committed_version,
                    new_bundle: bundle(vec![claim(pool_id, 10, 20, 5)]),
                    new_state: ReplacementState::Committed,
                }),
                NOW,
            )
            .expect("the promise should be replaced");
        let CommandResult::PromiseReplaced(ReplaceOutcome::Replaced {
            version: replaced_version,
            ..
        }) = replaced
        else {
            panic!("the result should report a replacement");
        };

        let revised = engine
            .apply(
                command(CommandOperation::ReviseCapacity {
                    resource_pool_id: pool_id,
                    capacity_curve: constant_capacity_curve(12),
                    mode: CapacityRevisionMode::Strict,
                }),
                NOW,
            )
            .expect("capacity should be revised");
        assert!(matches!(revised, CommandResult::CapacityRevised(_)));

        let released = engine
            .apply(
                command(CommandOperation::Release {
                    promise_id,
                    expected_version: replaced_version,
                }),
                NOW,
            )
            .expect("the promise should release");
        assert!(matches!(released, CommandResult::PromiseReleased { .. }));

        let kinds: Vec<EventKind> = engine
            .watch_events(SequenceNumber::new(0))
            .iter()
            .map(Event::kind)
            .collect();
        assert_eq!(
            kinds,
            vec![
                EventKind::ResourceCreated,
                EventKind::HoldCreated,
                EventKind::HoldCommitted,
                EventKind::PromiseReplaced,
                EventKind::CapacityRevised,
                EventKind::PromiseReleased,
            ]
        );
        assert_eq!(engine.sequence().get(), 6);
    }

    #[test]
    fn exact_hold_retry_returns_the_original_response_without_reapplying() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        let operation = CommandOperation::Hold {
            promise_id,
            bundle: bundle(vec![claim(pool_id, 0, 10, 4)]),
            expires_at: EXPIRES_AT,
        };

        let first = engine
            .apply(command_with_key("hold-1", operation.clone()), NOW)
            .expect("the first hold should succeed");
        let sequence = engine.sequence();
        let event_count = engine.state.events.len();
        let second = engine
            .apply(command_with_key("hold-1", operation), EXPIRES_AT)
            .expect("the retry should return its cached success");

        assert_eq!(first, second);
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.state.events.len(), event_count);
        assert_eq!(engine.state.promises.len(), 1);
        assert_eq!(engine.idempotency_record_count(), 1);
    }

    #[test]
    fn reordered_bundle_claims_are_the_same_idempotent_operation() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        let first_claim = claim(pool_id, 0, 5, 2);
        let second_claim = claim(pool_id, 5, 10, 3);
        let first = CommandOperation::Hold {
            promise_id,
            bundle: bundle(vec![first_claim.clone(), second_claim.clone()]),
            expires_at: EXPIRES_AT,
        };
        let reordered = CommandOperation::Hold {
            promise_id,
            bundle: bundle(vec![second_claim, first_claim]),
            expires_at: EXPIRES_AT,
        };

        let original = engine
            .apply(command_with_key("ordered-hold", first), NOW)
            .unwrap();
        let retry = engine
            .apply(command_with_key("ordered-hold", reordered), NOW)
            .unwrap();

        assert_eq!(original, retry);
        assert_eq!(engine.state.promises.len(), 1);
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn hold_one_of_selects_the_second_alternative_when_the_first_is_unavailable() {
        let (mut engine, pool_id) = engine_with_pool(5);
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldOneOf {
                    promise_id,
                    choice: choice(vec![
                        bundle(vec![claim(pool_id, 0, 10, 6)]),
                        bundle(vec![claim(pool_id, 0, 10, 4)]),
                    ]),
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .expect("the second alternative should be held");

        assert_eq!(
            result,
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                promise_id,
                alternative_index: 1,
            })
        );
        assert_eq!(
            engine.promise(promise_id).unwrap().bundle().claims()[0].quantity(),
            4
        );
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(0), Ok(1));
        assert_eq!(engine.sequence().get(), 1);
        assert_eq!(engine.state.events.len(), 1);
    }

    #[test]
    fn hold_one_of_stops_after_the_first_feasible_alternative() {
        let (mut engine, pool_id) = engine_with_pool(5);
        let missing_pool_id = ResourcePoolId::generate();
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldOneOf {
                    promise_id,
                    choice: choice(vec![
                        bundle(vec![claim(pool_id, 0, 10, 2)]),
                        bundle(vec![claim(missing_pool_id, 0, 10, 1)]),
                    ]),
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .expect("the missing pool in the later alternative must not be evaluated");

        assert!(matches!(
            result,
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                alternative_index: 0,
                ..
            })
        ));
        assert_eq!(engine.state.promises.len(), 1);
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(0), Ok(3));
    }

    #[test]
    fn hold_one_of_returns_conflicts_for_every_alternative() {
        let (mut engine, pool_id) = engine_with_pool(0);
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldOneOf {
                    promise_id,
                    choice: choice(vec![
                        bundle(vec![claim(pool_id, 0, 10, 1)]),
                        bundle(vec![claim(pool_id, 10, 20, 2)]),
                    ]),
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .expect("unavailability should be a normal outcome");
        let CommandResult::ChoiceCompleted(ChoiceOutcome::Unavailable { conflicts }) = result
        else {
            panic!("every alternative should be unavailable");
        };

        assert_eq!(conflicts.len(), 2);
        assert_eq!(conflicts[0].alternative_index(), 0);
        assert_eq!(conflicts[0].conflicts().len(), 1);
        assert_eq!(conflicts[0].conflicts()[0].required_quantity(), 1);
        assert_eq!(conflicts[1].alternative_index(), 1);
        assert_eq!(conflicts[1].conflicts().len(), 1);
        assert_eq!(conflicts[1].conflicts()[0].required_quantity(), 2);
        assert!(engine.promise(promise_id).is_none());
        assert_eq!(engine.sequence().get(), 0);
        assert!(engine.state.events.is_empty());
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(0), Ok(0));
    }

    #[test]
    fn rejected_multi_pool_alternative_leaves_no_partial_consumption() {
        let (mut engine, first_pool_id) = engine_with_pool(5);
        let second_pool_id =
            create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(0));
        let baseline_sequence = engine.sequence().get();
        let baseline_events = engine.state.events.len();
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldOneOf {
                    promise_id,
                    choice: choice(vec![
                        bundle(vec![
                            claim(first_pool_id, 0, 10, 5),
                            claim(second_pool_id, 0, 10, 1),
                        ]),
                        bundle(vec![claim(first_pool_id, 0, 10, 5)]),
                    ]),
                    expires_at: EXPIRES_AT,
                }),
                NOW,
            )
            .expect("the second alternative should fit atomically");

        assert!(matches!(
            result,
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                alternative_index: 1,
                ..
            })
        ));
        assert_eq!(
            engine.slack_timeline(first_pool_id).unwrap().slack_at(0),
            Ok(0)
        );
        assert_eq!(
            engine.slack_timeline(second_pool_id).unwrap().slack_at(0),
            Ok(0)
        );
        assert_eq!(engine.state.promises.len(), 1);
        assert_eq!(engine.sequence().get(), baseline_sequence + 1);
        assert_eq!(engine.state.events.len(), baseline_events + 1);
    }

    #[test]
    fn exact_hold_one_of_retry_does_not_reapply_or_process_expirations() {
        let (mut engine, pool_id) = engine_with_pool(5);
        let promise_id = PromiseId::generate();
        let operation = CommandOperation::HoldOneOf {
            promise_id,
            choice: choice(vec![bundle(vec![claim(pool_id, 0, 10, 2)])]),
            expires_at: EXPIRES_AT,
        };
        let first = engine
            .apply(command_with_key("hold-one-of", operation.clone()), NOW)
            .unwrap();
        let sequence = engine.sequence();
        let event_count = engine.state.events.len();
        let retry = engine
            .apply(command_with_key("hold-one-of", operation), EXPIRES_AT)
            .unwrap();

        assert_eq!(retry, first);
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.state.events.len(), event_count);
        assert!(matches!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Held { .. }
        ));
        assert_eq!(engine.idempotency_record_count(), 1);
    }

    #[test]
    fn hold_one_of_processes_expirations_before_selecting() {
        let (mut engine, pool_id) = engine_with_pool(1);
        let expired_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 1), 100, 1);
        let promise_id = PromiseId::generate();
        let result = engine
            .apply(
                command(CommandOperation::HoldOneOf {
                    promise_id,
                    choice: choice(vec![bundle(vec![claim(pool_id, 0, 10, 1)])]),
                    expires_at: EXPIRES_AT,
                }),
                100,
            )
            .expect("the alternative should fit after expiration");

        assert!(matches!(
            result,
            CommandResult::ChoiceCompleted(ChoiceOutcome::Held {
                alternative_index: 0,
                ..
            })
        ));
        assert_eq!(
            engine.promise(expired_id).unwrap().state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 3);
        assert_eq!(engine.state.events.len(), 2);
        assert_eq!(engine.state.events[0].kind(), EventKind::HoldExpired);
        assert_eq!(engine.state.events[1].kind(), EventKind::HoldCreated);
        assert_eq!(engine.state.events[0].sequence().get(), 2);
        assert_eq!(engine.state.events[1].sequence().get(), 3);
    }

    #[test]
    fn idempotency_keys_are_scoped_by_client() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let first = Command::new(
            crate::command::ClientId::new("first-client"),
            crate::command::IdempotencyKey::new("shared-key"),
            CommandOperation::Hold {
                promise_id: PromiseId::generate(),
                bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
                expires_at: EXPIRES_AT,
            },
        );
        let second = Command::new(
            crate::command::ClientId::new("second-client"),
            crate::command::IdempotencyKey::new("shared-key"),
            CommandOperation::Hold {
                promise_id: PromiseId::generate(),
                bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
                expires_at: EXPIRES_AT,
            },
        );

        engine.apply(first, NOW).unwrap();
        engine.apply(second, NOW).unwrap();

        assert_eq!(engine.state.promises.len(), 2);
        assert_eq!(engine.idempotency_record_count(), 2);
    }

    #[test]
    fn reusing_an_idempotency_key_with_another_payload_is_rejected() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        engine
            .apply(
                command_with_key(
                    "conflicting-hold",
                    CommandOperation::Hold {
                        promise_id,
                        bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
                        expires_at: EXPIRES_AT,
                    },
                ),
                NOW,
            )
            .unwrap();
        let sequence = engine.sequence();
        let event_count = engine.state.events.len();

        let result = engine.apply(
            command_with_key(
                "conflicting-hold",
                CommandOperation::Hold {
                    promise_id,
                    bundle: bundle(vec![claim(pool_id, 0, 10, 2)]),
                    expires_at: EXPIRES_AT,
                },
            ),
            NOW,
        );

        assert_eq!(result, Err(DomainError::IdempotencyConflict));
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.state.events.len(), event_count);
    }

    #[test]
    fn duplicate_commit_and_release_return_their_original_versions() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        engine
            .apply(
                command_with_key(
                    "hold-before-transitions",
                    CommandOperation::Hold {
                        promise_id,
                        bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
                        expires_at: EXPIRES_AT,
                    },
                ),
                NOW,
            )
            .unwrap();
        let held_version = engine.promise(promise_id).unwrap().version();
        let commit = CommandOperation::Commit {
            promise_id,
            expected_version: held_version,
        };
        let committed = engine
            .apply(command_with_key("commit-once", commit.clone()), NOW)
            .unwrap();
        let committed_retry = engine
            .apply(command_with_key("commit-once", commit), NOW)
            .unwrap();
        assert_eq!(committed, committed_retry);

        let committed_version = engine.promise(promise_id).unwrap().version();
        let release = CommandOperation::Release {
            promise_id,
            expected_version: committed_version,
        };
        let released = engine
            .apply(command_with_key("release-once", release.clone()), NOW)
            .unwrap();
        let sequence = engine.sequence();
        let released_retry = engine
            .apply(command_with_key("release-once", release), NOW)
            .unwrap();

        assert_eq!(released, released_retry);
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.promise(promise_id).unwrap().version().get(), 3);
    }

    #[test]
    fn an_error_response_is_stable_after_state_changes() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        let expected_version = Promise::with_id(
            promise_id,
            bundle(vec![claim(pool_id, 20, 30, 1)]),
            EXPIRES_AT,
            NOW,
            SequenceNumber::new(1),
        )
        .unwrap()
        .version();
        let missing_commit = CommandOperation::Commit {
            promise_id,
            expected_version,
        };

        let first = engine.apply(
            command_with_key("missing-commit", missing_commit.clone()),
            NOW,
        );
        assert_eq!(first, Err(DomainError::PromiseNotFound));
        engine
            .apply(
                command_with_key(
                    "create-missing-promise",
                    CommandOperation::Hold {
                        promise_id,
                        bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
                        expires_at: EXPIRES_AT,
                    },
                ),
                NOW,
            )
            .unwrap();

        let retry = engine.apply(command_with_key("missing-commit", missing_commit), NOW);

        assert_eq!(retry, Err(DomainError::PromiseNotFound));
        assert!(matches!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Held { .. }
        ));
    }

    #[test]
    fn unavailable_hold_response_is_cached() {
        let (mut engine, pool_id) = engine_with_pool(0);
        let promise_id = PromiseId::generate();
        let operation = CommandOperation::Hold {
            promise_id,
            bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
            expires_at: EXPIRES_AT,
        };
        let first = engine
            .apply(command_with_key("unavailable-hold", operation.clone()), NOW)
            .expect("unavailability should be a normal response");
        assert!(matches!(
            first,
            CommandResult::HoldCompleted(HoldOutcome::Unavailable { .. })
        ));
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(1),
                CapacityRevisionMode::Strict,
                NOW,
            )
            .unwrap();
        let sequence = engine.sequence();

        let retry = engine
            .apply(command_with_key("unavailable-hold", operation), NOW)
            .expect("the original unavailability should be returned");

        assert_eq!(retry, first);
        assert_eq!(engine.sequence(), sequence);
        assert!(engine.promise(promise_id).is_none());
    }

    #[test]
    fn expiration_events_precede_the_requested_command_event() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let first_pool_id = ResourcePoolId::generate();
        engine
            .apply(
                command(CommandOperation::CreateResourcePool {
                    resource_pool_id: first_pool_id,
                    display_name: "First".into(),
                    unit: unit("units", 1),
                    capacity_curve: constant_capacity_curve(10),
                }),
                NOW,
            )
            .unwrap();
        engine
            .apply(
                command(CommandOperation::Hold {
                    promise_id: PromiseId::generate(),
                    bundle: bundle(vec![claim(first_pool_id, 0, 10, 1)]),
                    expires_at: 100,
                }),
                NOW,
            )
            .unwrap();

        engine
            .apply(
                command(CommandOperation::CreateResourcePool {
                    resource_pool_id: ResourcePoolId::generate(),
                    display_name: "Second".into(),
                    unit: unit("units", 1),
                    capacity_curve: constant_capacity_curve(1),
                }),
                100,
            )
            .expect("expiration and creation should succeed");

        let events = engine.watch_events(SequenceNumber::new(3));
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), EventKind::HoldExpired);
        assert_eq!(events[0].sequence().get(), 3);
        assert_eq!(events[1].kind(), EventKind::ResourceCreated);
        assert_eq!(events[1].sequence().get(), 4);
    }

    #[test]
    fn explicit_expiration_command_processes_deadlines() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 1), 100, 1);

        let result = engine
            .apply(command(CommandOperation::ProcessExpirations), 100)
            .expect("due holds should expire");

        assert_eq!(
            result,
            CommandResult::ExpirationsProcessed { expired_count: 1 }
        );
        assert_eq!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Expired
        );
        assert_eq!(
            engine.watch_events(SequenceNumber::new(2))[0].kind(),
            EventKind::HoldExpired
        );
    }

    #[test]
    fn capacity_revisions_emit_deficit_lifecycle_events() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);

        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .unwrap();
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(8),
                CapacityRevisionMode::Strict,
                NOW,
            )
            .unwrap();

        let kinds: Vec<EventKind> = engine.state.events.iter().map(Event::kind).collect();
        assert_eq!(
            kinds,
            vec![
                EventKind::CapacityRevised,
                EventKind::DeficitCreated,
                EventKind::CapacityRevised,
                EventKind::DeficitResolved,
            ]
        );
        assert_eq!(
            engine.state.events[0].sequence(),
            engine.state.events[1].sequence()
        );
        assert_eq!(
            engine.state.events[2].sequence(),
            engine.state.events[3].sequence()
        );
    }

    #[test]
    fn duplicate_control_plane_promise_id_is_rejected_without_an_event() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = PromiseId::generate();
        let first = command(CommandOperation::Hold {
            promise_id,
            bundle: bundle(vec![claim(pool_id, 0, 10, 1)]),
            expires_at: EXPIRES_AT,
        });
        engine.apply(first, NOW).unwrap();
        let sequence = engine.sequence();
        let event_count = engine.state.events.len();

        let result = engine.apply(
            command(CommandOperation::Hold {
                promise_id,
                bundle: bundle(vec![claim(pool_id, 10, 20, 1)]),
                expires_at: EXPIRES_AT,
            }),
            NOW,
        );

        assert_eq!(result, Err(DomainError::PromiseAlreadyExists));
        assert_eq!(engine.sequence(), sequence);
        assert_eq!(engine.state.events.len(), event_count);
    }

    #[test]
    fn create_resource_pool_at_publishes_the_pool_and_sequence() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = ResourcePoolId::generate();

        let created_id = engine
            .create_resource_pool_at(
                pool_id,
                "Machine pool".into(),
                unit("machines", 100),
                constant_capacity_curve(10),
                NOW,
            )
            .expect("the resource pool should be created");

        let pool = engine
            .resource_pool(created_id)
            .expect("the resource pool should exist");
        assert_eq!(created_id, pool_id);
        assert_eq!(pool.display_name(), "Machine pool");
        assert_eq!(pool.unit().name(), "machines");
        assert_eq!(pool.unit().subunits_per_unit(), 100);
        assert_eq!(pool.capacity_at(NOW), 10);

        let timeline = engine
            .slack_timeline(created_id)
            .expect("the resource pool should have a slack timeline");
        assert_eq!(timeline.slack_at(NOW), Ok(10));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn hold_accepts_a_claim_within_a_higher_capacity_segment() {
        let (mut engine, pool_id) = engine_with_capacity_curve(variable_capacity_curve());
        let candidate = bundle(vec![claim(pool_id, 0, 10, 7)]);

        engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("the higher-capacity segment should admit the claim");

        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(3)
        );
    }

    #[test]
    fn hold_rejects_a_claim_within_a_lower_capacity_segment() {
        let (mut engine, pool_id) = engine_with_capacity_curve(variable_capacity_curve());
        let candidate = bundle(vec![claim(pool_id, 10, 20, 7)]);

        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("unavailability should be a normal outcome");
        let HoldOutcome::Unavailable { conflicts } = outcome else {
            panic!("the lower-capacity segment should reject the claim");
        };

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(10, 20).unwrap()
        );
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn hold_rejects_a_claim_crossing_into_a_lower_capacity_segment() {
        let (mut engine, pool_id) = engine_with_capacity_curve(variable_capacity_curve());
        let candidate = bundle(vec![claim(pool_id, 5, 15, 7)]);

        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("unavailability should be a normal outcome");
        let HoldOutcome::Unavailable { conflicts } = outcome else {
            panic!("crossing into lower capacity should reject the claim");
        };

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(10, 15).unwrap()
        );
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn an_unknown_resource_pool_has_no_slack_timeline() {
        let engine = Engine::with_clock(FixedClock(NOW));

        assert!(engine.slack_timeline(ResourcePoolId::generate()).is_none());
    }

    #[test]
    fn an_empty_capacity_curve_creates_a_pool_with_zero_slack() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = ResourcePoolId::generate();

        let created_id = engine
            .create_resource_pool_at(
                pool_id,
                "Unavailable pool".into(),
                unit("machines", 1),
                CapacityCurve::empty(),
                NOW,
            )
            .expect("zero capacity should be valid");

        assert_eq!(created_id, pool_id);
        assert_eq!(
            engine
                .resource_pool(pool_id)
                .expect("the resource pool should exist")
                .capacity_at(NOW),
            0
        );
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(NOW),
            Ok(0)
        );
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn duplicate_resource_pool_id_does_not_replace_or_consume_a_sequence() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = ResourcePoolId::generate();
        engine
            .create_resource_pool_at(
                pool_id,
                "Original".into(),
                unit("machines", 100),
                constant_capacity_curve(10),
                NOW,
            )
            .expect("the first resource pool should be created");

        let result = engine.create_resource_pool_at(
            pool_id,
            "Replacement".into(),
            unit("people", 1),
            constant_capacity_curve(20),
            NOW,
        );

        assert_eq!(result, Err(DomainError::ResourcePoolAlreadyExists));
        let pool = engine
            .resource_pool(pool_id)
            .expect("the original resource pool should remain");
        assert_eq!(pool.display_name(), "Original");
        assert_eq!(pool.unit().name(), "machines");
        assert_eq!(pool.unit().subunits_per_unit(), 100);
        assert_eq!(pool.capacity_at(NOW), 10);
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the original timeline should remain")
                .slack_at(NOW),
            Ok(10)
        );
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn public_hold_uses_the_injected_clock() {
        let mut engine = Engine::with_clock(FixedClock(100));
        let candidate = bundle(vec![claim(ResourcePoolId::generate(), 0, 10, 1)]);

        assert_eq!(
            engine.hold(candidate, 100),
            Err(DomainError::InvalidExpiration)
        );
    }

    #[test]
    fn commit_at_uses_the_provided_time_and_publishes_the_transition() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let expected_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();

        let new_version = engine
            .commit_at(promise_id, expected_version, NOW)
            .expect("the live hold should commit");

        assert_eq!(new_version.get(), 2);
        assert_eq!(engine.sequence().get(), 2);
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Committed
        );
    }

    #[test]
    fn hold_updates_the_derived_slack_timeline() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 4)]);

        engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("the bundle should be held");

        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(6)
        );
    }

    #[test]
    fn release_at_releases_a_hold_and_restores_capacity() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let reserved_claim = claim(pool_id, 0, 10, 10);
        let promise_id = add_held_promise(&mut engine, reserved_claim, 1);
        let expected_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();

        let new_version = engine
            .release_at(promise_id, expected_version, NOW)
            .expect("the live hold should release");

        assert_eq!(new_version.get(), 2);
        assert_eq!(engine.sequence().get(), 2);
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Released
        );
        assert_eq!(
            engine.check_availability(&bundle(vec![claim(pool_id, 0, 10, 10)])),
            Ok(true)
        );
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(10)
        );
    }

    #[test]
    fn release_at_releases_a_committed_promise() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let held_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();
        let committed_version = engine
            .commit_at(promise_id, held_version, NOW)
            .expect("the live hold should commit");

        let released_version = engine
            .release_at(promise_id, committed_version, NOW)
            .expect("the committed promise should release");

        assert_eq!(released_version.get(), 3);
        assert_eq!(engine.sequence().get(), 3);
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Released
        );
    }

    #[test]
    fn strict_capacity_revision_rejects_a_deficit_without_mutation() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        let original_pool = engine.resource_pool(pool_id).unwrap().clone();
        let original_timeline = engine.slack_timeline(pool_id).unwrap().clone();

        let result = engine.revise_capacity_at(
            pool_id,
            constant_capacity_curve(5),
            CapacityRevisionMode::Strict,
            NOW,
        );

        assert_eq!(result, Err(DomainError::CapacityRevisionCreatesDeficit));
        assert_eq!(engine.resource_pool(pool_id), Some(&original_pool));
        assert_eq!(engine.slack_timeline(pool_id), Some(&original_timeline));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn forced_capacity_revision_reports_deficits_and_affected_promises() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);

        let outcome = engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");

        assert_eq!(outcome.sequence().get(), 2);
        assert_eq!(outcome.affected_promise_ids(), &[promise_id]);
        assert_eq!(outcome.deficits().len(), 1);
        assert_eq!(outcome.deficits()[0].resource_pool_id(), pool_id);
        assert_eq!(
            outcome.deficits()[0].interval(),
            Interval::new(0, 10).unwrap()
        );
        assert_eq!(outcome.deficits()[0].quantity(), 3);
        assert_eq!(outcome.deficits()[0].affected_promise_ids(), &[promise_id]);
        assert_eq!(engine.resource_pool(pool_id).unwrap().capacity_at(5), 5);
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(-3));
    }

    #[test]
    fn forced_capacity_revision_supports_the_maximum_deficit() {
        let (mut engine, pool_id) = engine_with_pool(MAX_QUANTITY);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, MAX_QUANTITY), 1);

        let outcome = engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(0),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the maximum representable deficit should be supported");

        assert_eq!(outcome.deficits().len(), 1);
        assert_eq!(outcome.deficits()[0].quantity(), MAX_QUANTITY);
        assert_eq!(outcome.deficits()[0].affected_promise_ids(), &[promise_id]);
        assert_eq!(
            engine.slack_timeline(pool_id).unwrap().slack_at(5),
            Ok(-Slack::MAX)
        );
    }

    #[test]
    fn strict_capacity_increase_applies_without_deficits() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);

        let outcome = engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(12),
                CapacityRevisionMode::Strict,
                NOW,
            )
            .expect("the capacity increase should be applied");

        assert!(outcome.deficits().is_empty());
        assert!(outcome.affected_promise_ids().is_empty());
        assert_eq!(outcome.sequence().get(), 2);
        assert_eq!(engine.resource_pool(pool_id).unwrap().capacity_at(5), 12);
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(4));
    }

    #[test]
    fn later_capacity_revision_resolves_a_forced_deficit() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");

        let outcome = engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(8),
                CapacityRevisionMode::Strict,
                NOW,
            )
            .expect("the deficit should be resolved");

        assert!(outcome.deficits().is_empty());
        assert!(
            engine
                .list_at_risk(None, None)
                .expect("at-risk promises should be listed")
                .is_empty()
        );
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(0));
        assert_eq!(engine.sequence().get(), 3);
    }

    #[test]
    fn capacity_revision_processes_due_expirations_first() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 8), 100, 1);

        let outcome = engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(0),
                CapacityRevisionMode::Strict,
                100,
            )
            .expect("expired usage should not create a deficit");

        assert!(outcome.deficits().is_empty());
        assert_eq!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 3);
    }

    #[test]
    fn list_at_risk_supports_pool_and_time_filters() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");

        let matching = engine
            .list_at_risk(Some(pool_id), Some(Interval::new(5, 15).unwrap()))
            .expect("the at-risk promises should be listed");
        let outside = engine
            .list_at_risk(Some(pool_id), Some(Interval::new(10, 20).unwrap()))
            .expect("the filtered result should be listed");

        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].promise_id(), promise_id);
        assert_eq!(matching[0].deficits().len(), 1);
        assert!(outside.is_empty());
    }

    #[test]
    fn forced_deficit_blocks_new_holds_and_is_explainable() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");
        let candidate = bundle(vec![claim(pool_id, 0, 10, 1)]);

        let conflicts = engine
            .explain_unavailable(&candidate)
            .expect("the unavailable bundle should be explained");
        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("unavailability should be a normal outcome");

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(0, 10).unwrap()
        );
        assert!(matches!(outcome, HoldOutcome::Unavailable { .. }));
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn replacement_may_reduce_a_forced_deficit_without_resolving_it() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");
        let expected_version = engine.promise(promise_id).unwrap().version();

        let outcome = engine
            .replace_at(
                promise_id,
                expected_version,
                bundle(vec![claim(pool_id, 0, 10, 6)]),
                ReplacementState::Committed,
                NOW,
            )
            .expect("a deficit-improving replacement should be evaluated");

        assert!(matches!(outcome, ReplaceOutcome::Replaced { .. }));
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(-1));
    }

    #[test]
    fn replacement_cannot_worsen_a_forced_deficit() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        engine
            .revise_capacity_at(
                pool_id,
                constant_capacity_curve(5),
                CapacityRevisionMode::Force,
                NOW,
            )
            .expect("the forced revision should be applied");
        let original_promise = engine.promise(promise_id).unwrap().clone();
        let expected_version = original_promise.version();

        let outcome = engine
            .replace_at(
                promise_id,
                expected_version,
                bundle(vec![claim(pool_id, 0, 10, 9)]),
                ReplacementState::Committed,
                NOW,
            )
            .expect("unavailability should be a normal outcome");

        assert!(matches!(outcome, ReplaceOutcome::Unavailable { .. }));
        assert_eq!(engine.promise(promise_id), Some(&original_promise));
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(-3));
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn replacement_fits_only_after_removing_the_old_bundle() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 8), 1);
        let original = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .clone();
        let replacement = bundle(vec![claim(pool_id, 0, 10, 10)]);

        let outcome = engine
            .replace_at(
                promise_id,
                original.version(),
                replacement.clone(),
                ReplacementState::Committed,
                NOW,
            )
            .expect("the final replacement state should fit");

        assert_eq!(
            outcome,
            ReplaceOutcome::Replaced {
                promise_id,
                version: original.version().next().unwrap(),
            }
        );
        let replaced = engine
            .promise(promise_id)
            .expect("the replaced promise should exist");
        assert_eq!(replaced.id(), original.id());
        assert_eq!(replaced.created_sequence(), original.created_sequence());
        assert_eq!(replaced.bundle(), &replacement);
        assert_eq!(replaced.state(), PromiseState::Committed);
        assert_eq!(replaced.version().get(), 2);
        assert_eq!(replaced.updated_sequence(), engine.sequence());
        assert_eq!(engine.sequence().get(), 2);
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(0)
        );
    }

    #[test]
    fn unavailable_replacement_preserves_promise_timelines_and_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 20, 4), 1);
        let original_promise = engine.promise(promise_id).unwrap().clone();
        let original_timeline = engine.slack_timeline(pool_id).unwrap().clone();
        let original_sequence = engine.sequence();
        let replacement = bundle(vec![claim(pool_id, 0, 5, 11), claim(pool_id, 10, 15, 12)]);

        let outcome = engine
            .replace_at(
                promise_id,
                original_promise.version(),
                replacement,
                ReplacementState::Committed,
                NOW,
            )
            .expect("unavailability should be a normal outcome");
        let ReplaceOutcome::Unavailable { conflicts } = outcome else {
            panic!("the replacement should be unavailable");
        };

        assert_eq!(conflicts.len(), 2);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(0, 5).unwrap()
        );
        assert_eq!(
            conflicts[1].blocking_interval(),
            Interval::new(10, 15).unwrap()
        );
        assert!(
            conflicts
                .iter()
                .all(|conflict| !conflict.conflicting_promise_ids().contains(&promise_id))
        );
        assert_eq!(engine.promise(promise_id), Some(&original_promise));
        assert_eq!(engine.slack_timeline(pool_id), Some(&original_timeline));
        assert_eq!(engine.sequence(), original_sequence);
    }

    #[test]
    fn replacement_moves_usage_between_resource_pools() {
        let (mut engine, old_pool_id) = engine_with_pool(10);
        let new_pool_id = create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(7));
        let promise_id = add_held_promise(&mut engine, claim(old_pool_id, 0, 10, 6), 2);
        let expected_version = engine.promise(promise_id).unwrap().version();

        engine
            .replace_at(
                promise_id,
                expected_version,
                bundle(vec![claim(new_pool_id, 0, 10, 7)]),
                ReplacementState::Committed,
                NOW,
            )
            .expect("the cross-pool replacement should fit");

        assert_eq!(
            engine.slack_timeline(old_pool_id).unwrap().slack_at(5),
            Ok(10)
        );
        assert_eq!(
            engine.slack_timeline(new_pool_id).unwrap().slack_at(5),
            Ok(0)
        );
        assert_eq!(engine.sequence().get(), 3);
    }

    #[test]
    fn replacement_can_change_a_commitment_into_a_live_hold() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 4), 1);
        let held_version = engine.promise(promise_id).unwrap().version();
        let committed_version = engine
            .commit_at(promise_id, held_version, NOW)
            .expect("the promise should commit");

        let outcome = engine
            .replace_at(
                promise_id,
                committed_version,
                bundle(vec![claim(pool_id, 10, 20, 5)]),
                ReplacementState::Held { expires_at: 500 },
                NOW,
            )
            .expect("the commitment should become a hold");

        assert_eq!(
            outcome,
            ReplaceOutcome::Replaced {
                promise_id,
                version: committed_version.next().unwrap(),
            }
        );
        assert_eq!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Held { expires_at: 500 }
        );
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(10));
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(15), Ok(5));
    }

    #[test]
    fn failed_replacement_validation_preserves_engine_state() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 4), 1);
        let original_promise = engine.promise(promise_id).unwrap().clone();
        let original_timeline = engine.slack_timeline(pool_id).unwrap().clone();
        let wrong_version = original_promise.version().next().unwrap();

        let result = engine.replace_at(
            promise_id,
            wrong_version,
            bundle(vec![claim(pool_id, 10, 20, 4)]),
            ReplacementState::Committed,
            NOW,
        );

        assert_eq!(result, Err(DomainError::VersionConflict));
        assert_eq!(engine.promise(promise_id), Some(&original_promise));
        assert_eq!(engine.slack_timeline(pool_id), Some(&original_timeline));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn replacement_processes_due_expiration_before_failing() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);
        let expected_version = engine.promise(promise_id).unwrap().version();

        let result = engine.replace_at(
            promise_id,
            expected_version,
            bundle(vec![claim(pool_id, 10, 20, 10)]),
            ReplacementState::Committed,
            100,
        );

        assert_eq!(result, Err(DomainError::HoldExpired));
        assert_eq!(
            engine.promise(promise_id).unwrap().state(),
            PromiseState::Expired
        );
        assert_eq!(engine.slack_timeline(pool_id).unwrap().slack_at(5), Ok(10));
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn public_replace_uses_the_injected_clock_for_new_hold_deadlines() {
        let (mut engine, pool_id) = engine_with_pool(10);
        engine.clock = Box::new(FixedClock(100));
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 1), 1);
        let expected_version = engine.promise(promise_id).unwrap().version();

        let result = engine.replace(
            promise_id,
            expected_version,
            bundle(vec![claim(pool_id, 0, 10, 1)]),
            ReplacementState::Held { expires_at: 100 },
        );

        assert_eq!(result, Err(DomainError::InvalidExpiration));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn commit_at_the_deadline_expires_the_hold_and_returns_hold_expired() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);
        let expected_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();

        let result = engine.commit_at(promise_id, expected_version, 100);

        assert_eq!(result, Err(DomainError::HoldExpired));
        let promise = engine
            .promise(promise_id)
            .expect("the promise should exist");
        assert_eq!(promise.state(), PromiseState::Expired);
        assert_eq!(promise.version().get(), 2);
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn commit_after_the_deadline_expires_the_hold_and_returns_hold_expired() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);
        let expected_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();

        let result = engine.commit_at(promise_id, expected_version, 101);

        assert_eq!(result, Err(DomainError::HoldExpired));
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn release_at_the_deadline_expires_the_hold_and_returns_hold_expired() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);
        let expected_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();

        let result = engine.release_at(promise_id, expected_version, 100);

        assert_eq!(result, Err(DomainError::HoldExpired));
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn commit_version_conflict_preserves_the_hold_and_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let wrong_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version()
            .next()
            .expect("the version should increment");

        let result = engine.commit_at(promise_id, wrong_version, NOW);

        assert_eq!(result, Err(DomainError::VersionConflict));
        assert!(matches!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Held { .. }
        ));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn release_version_conflict_preserves_the_hold_and_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let wrong_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version()
            .next()
            .expect("the version should increment");

        let result = engine.release_at(promise_id, wrong_version, NOW);

        assert_eq!(result, Err(DomainError::VersionConflict));
        assert!(matches!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Held { .. }
        ));
        assert_eq!(engine.sequence().get(), 1);
    }

    #[test]
    fn missing_promise_does_not_consume_a_commit_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let detached = Promise::with_id(
            PromiseId::generate(),
            bundle(vec![claim(pool_id, 0, 10, 1)]),
            EXPIRES_AT,
            NOW,
            SequenceNumber::new(1),
        )
        .expect("the detached promise should be valid");

        let result = engine.commit_at(detached.id(), detached.version(), NOW);

        assert_eq!(result, Err(DomainError::PromiseNotFound));
        assert_eq!(engine.sequence().get(), 0);
    }

    #[test]
    fn missing_promise_does_not_consume_a_release_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let detached = Promise::with_id(
            PromiseId::generate(),
            bundle(vec![claim(pool_id, 0, 10, 1)]),
            EXPIRES_AT,
            NOW,
            SequenceNumber::new(1),
        )
        .expect("the detached promise should be valid");

        let result = engine.release_at(detached.id(), detached.version(), NOW);

        assert_eq!(result, Err(DomainError::PromiseNotFound));
        assert_eq!(engine.sequence().get(), 0);
    }

    #[test]
    fn duplicate_commit_preserves_the_committed_promise_and_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let held_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();
        let committed_version = engine
            .commit_at(promise_id, held_version, NOW)
            .expect("the first commit should succeed");

        let result = engine.commit_at(promise_id, committed_version, NOW);

        assert_eq!(result, Err(DomainError::InvalidPromiseState));
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Committed
        );
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn duplicate_release_preserves_the_released_promise_and_sequence() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 10), 1);
        let held_version = engine
            .promise(promise_id)
            .expect("the promise should exist")
            .version();
        let released_version = engine
            .release_at(promise_id, held_version, NOW)
            .expect("the first release should succeed");

        let result = engine.release_at(promise_id, released_version, NOW);

        assert_eq!(result, Err(DomainError::InvalidPromiseState));
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Released
        );
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn expiration_sequence_remains_committed_when_the_command_fails() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let expiring_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 1), 100, 1);
        let detached = Promise::with_id(
            PromiseId::generate(),
            bundle(vec![claim(pool_id, 20, 30, 1)]),
            EXPIRES_AT,
            NOW,
            SequenceNumber::new(2),
        )
        .expect("the detached promise should be valid");

        let result = engine.commit_at(detached.id(), detached.version(), 100);

        assert_eq!(result, Err(DomainError::PromiseNotFound));
        assert_eq!(
            engine
                .promise(expiring_id)
                .expect("the expiring promise should exist")
                .state(),
            PromiseState::Expired
        );
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn availability_fails_when_the_pool_does_not_exist() {
        let engine = Engine::new();
        let candidate = bundle(vec![claim(ResourcePoolId::generate(), 0, 10, 1)]);
        let reference_result = engine.check_availability(&candidate);
        let indexed_result = indexed_availability(&engine, &candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Err(DomainError::ResourcePoolNotFound));
    }

    #[test]
    fn hold_rejects_a_missing_pool_without_mutating_the_engine() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let missing_pool_id = ResourcePoolId::generate();
        let candidate = bundle(vec![claim(missing_pool_id, 0, 10, 1)]);

        let result = engine.hold_at(candidate, EXPIRES_AT, NOW);

        assert_eq!(result, Err(DomainError::ResourcePoolNotFound));
        assert!(engine.state.promises.is_empty());
        assert!(engine.state.slack_timelines.is_empty());
        assert_eq!(engine.sequence().get(), 0);
    }

    #[test]
    fn an_unreserved_pool_has_its_full_capacity_available() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 10)]);
        let reference_result = engine.check_availability(&candidate);
        let indexed_result = indexed_availability(&engine, &candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(true));
    }

    #[test]
    fn reservations_in_consecutive_intervals_are_not_added_together() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 5, 6), 1);
        add_held_promise(&mut engine, claim(pool_id, 5, 10, 6), 2);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 4)]);

        assert_eq!(engine.check_availability(&candidate), Ok(true));
    }

    #[test]
    fn simultaneous_reservations_are_added_together() {
        let (mut engine, pool_id) = engine_with_pool(10);
        add_held_promise(&mut engine, claim(pool_id, 0, 6, 3), 1);
        add_held_promise(&mut engine, claim(pool_id, 5, 10, 3), 2);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 5)]);

        assert_eq!(engine.check_availability(&candidate), Ok(false));
    }

    #[test]
    fn pool_admission_reports_combined_candidate_demand() {
        let (engine, pool_id) = engine_with_pool(6);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 4), claim(pool_id, 5, 15, 3)]);
        let candidate_claims: Vec<&Claim> = candidate.claims().iter().collect();

        let admission = engine
            .evaluate_pool_admission(pool_id, &candidate_claims)
            .expect("the pool should be evaluated");
        let PoolAdmission::Unavailable(conflicts) = admission else {
            panic!("the overlapping demand should be unavailable");
        };

        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(5, 10).unwrap()
        );
        assert_eq!(conflicts[0].required_quantity(), 7);
        assert_eq!(conflicts[0].available_quantity(), 6);
        assert_eq!(conflicts[0].deficit_quantity(), 1);
    }

    #[test]
    fn aggregate_demand_above_i64_max_is_unavailable_without_mutation() {
        let (mut engine, pool_id) = engine_with_pool(MAX_QUANTITY);
        let candidate = bundle(vec![
            claim(pool_id, 0, 10, MAX_QUANTITY),
            claim(pool_id, 0, 10, 1),
        ]);
        let original_timeline = engine.slack_timeline(pool_id).unwrap().clone();

        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("wide aggregate demand should be a normal admission result");
        let HoldOutcome::Unavailable { conflicts } = outcome else {
            panic!("demand above maximum slack should be unavailable");
        };

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].required_quantity(), MAX_QUANTITY + 1);
        assert_eq!(conflicts[0].available_quantity(), MAX_QUANTITY);
        assert_eq!(conflicts[0].deficit_quantity(), 1);
        assert_eq!(engine.slack_timeline(pool_id), Some(&original_timeline));
        assert!(engine.state.promises.is_empty());
        assert_eq!(engine.sequence().get(), 0);
    }

    #[test]
    fn pool_admission_reports_overlapping_active_promises() {
        let (mut engine, pool_id) = engine_with_pool(6);
        let active_promise_id = add_held_promise(&mut engine, claim(pool_id, 0, 10, 4), 1);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 3)]);
        let candidate_claims: Vec<&Claim> = candidate.claims().iter().collect();

        let admission = engine
            .evaluate_pool_admission(pool_id, &candidate_claims)
            .expect("the pool should be evaluated");
        let PoolAdmission::Unavailable(conflicts) = admission else {
            panic!("the active promise should block the candidate");
        };

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].conflicting_promise_ids(), &[active_promise_id]);
    }

    #[test]
    fn available_pool_admission_returns_an_adjusted_copy() {
        let (engine, pool_id) = engine_with_pool(6);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 3), claim(pool_id, 5, 15, 3)]);
        let candidate_claims: Vec<&Claim> = candidate.claims().iter().collect();

        let admission = engine
            .evaluate_pool_admission(pool_id, &candidate_claims)
            .expect("the pool should be evaluated");
        let PoolAdmission::Available(adjusted_timeline) = admission else {
            panic!("the exact-fit demand should be available");
        };

        assert_eq!(adjusted_timeline.slack_at(2), Ok(3));
        assert_eq!(adjusted_timeline.slack_at(7), Ok(0));
        assert_eq!(adjusted_timeline.slack_at(12), Ok(3));
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the original timeline should exist")
                .slack_at(7),
            Ok(6)
        );
    }

    #[test]
    fn timeline_overrides_allow_admission_after_restoring_old_usage() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let old_claim = claim(pool_id, 0, 10, 8);
        add_held_promise(&mut engine, old_claim.clone(), 1);
        let old_bundle = bundle(vec![old_claim]);
        let new_bundle = bundle(vec![claim(pool_id, 0, 10, 10)]);

        assert!(matches!(
            engine
                .evaluate_bundle_admission(&new_bundle)
                .expect("the current state should be evaluated"),
            BundleAdmission::Unavailable(_)
        ));

        let restored_timelines = engine
            .restored_timelines(&old_bundle)
            .expect("the old usage should be restorable");
        let admission = engine
            .evaluate_bundle_with_overrides(&new_bundle, &restored_timelines)
            .expect("the replacement state should be evaluated");
        let BundleAdmission::Available(adjusted_timelines) = admission else {
            panic!("the replacement should fit after restoring old usage");
        };

        assert_eq!(
            adjusted_timelines
                .get(&pool_id)
                .expect("the adjusted timeline should exist")
                .slack_at(5),
            Ok(0)
        );
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the current timeline should remain unchanged")
                .slack_at(5),
            Ok(2)
        );
    }

    #[test]
    fn reference_and_indexed_availability_agree_with_active_usage() {
        let capacity_curve = constant_capacity_curve(10);
        let (mut engine, pool_id) = engine_with_capacity_curve(capacity_curve);
        add_held_promise(&mut engine, claim(pool_id, 0, 10, 4), 2);
        let fitting_candidate = bundle(vec![claim(pool_id, 0, 10, 6)]);
        let reference_result = engine.check_availability(&fitting_candidate);
        let indexed_result = indexed_availability(&engine, &fitting_candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(true));

        let exceeding_candidate = bundle(vec![claim(pool_id, 0, 10, 7)]);
        let reference_result = engine.check_availability(&exceeding_candidate);
        let indexed_result = indexed_availability(&engine, &exceeding_candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(false));
    }

    #[test]
    fn reference_and_indexed_availability_agree_across_multiple_pools() {
        let (mut engine, first_pool_id) = engine_with_capacity_curve(constant_capacity_curve(10));
        let second_pool_id =
            create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(5));

        let fitting_candidate = bundle(vec![
            claim(first_pool_id, 0, 10, 10),
            claim(second_pool_id, 0, 10, 5),
        ]);
        let reference_result = engine.check_availability(&fitting_candidate);
        let indexed_result = indexed_availability(&engine, &fitting_candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(true));

        let exceeding_candidate = bundle(vec![
            claim(first_pool_id, 0, 10, 10),
            claim(second_pool_id, 0, 10, 6),
        ]);
        let reference_result = engine.check_availability(&exceeding_candidate);
        let indexed_result = indexed_availability(&engine, &exceeding_candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(false));
    }

    #[test]
    fn reference_and_indexed_availability_agree_across_a_capacity_gap() {
        let capacity_curve = CapacityCurve::from_sorted(vec![
            CapacitySegment::new(
                Interval::new(0, 5).expect("the first interval should be valid"),
                10,
            ),
            CapacitySegment::new(
                Interval::new(10, 15).expect("the second interval should be valid"),
                10,
            ),
        ])
        .expect("the gapped capacity curve should be valid");
        let (engine, pool_id) = engine_with_capacity_curve(capacity_curve);
        let candidate = bundle(vec![claim(pool_id, 0, 15, 1)]);

        let reference_result = engine.check_availability(&candidate);
        let indexed_result = indexed_availability(&engine, &candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(false));
    }

    #[test]
    fn reference_and_indexed_availability_agree_for_overlapping_candidate_claims() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 6), claim(pool_id, 5, 15, 5)]);

        let reference_result = engine.check_availability(&candidate);
        let indexed_result = indexed_availability(&engine, &candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(false));
    }

    #[test]
    fn reference_and_indexed_availability_agree_for_adjacent_candidate_claims() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 5, 10), claim(pool_id, 5, 10, 10)]);

        let reference_result = engine.check_availability(&candidate);
        let indexed_result = indexed_availability(&engine, &candidate);

        assert_eq!(reference_result, indexed_result);
        assert_eq!(reference_result, Ok(true));
    }

    #[test]
    fn unavailable_hold_returns_all_pool_conflicts_in_time_order() {
        let (mut engine, later_pool_id) = engine_with_capacity_curve(constant_capacity_curve(5));
        let earlier_pool_id =
            create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(5));
        let candidate = bundle(vec![
            claim(later_pool_id, 10, 20, 6),
            claim(earlier_pool_id, 0, 10, 7),
        ]);
        let sequence_before = engine.sequence();

        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("unavailability should be a normal outcome");
        let HoldOutcome::Unavailable { conflicts } = outcome else {
            panic!("both pools should be unavailable");
        };

        assert_eq!(conflicts.len(), 2);
        assert_eq!(conflicts[0].resource_pool_id(), earlier_pool_id);
        assert_eq!(
            conflicts[0].blocking_interval(),
            Interval::new(0, 10).unwrap()
        );
        assert_eq!(conflicts[1].resource_pool_id(), later_pool_id);
        assert_eq!(
            conflicts[1].blocking_interval(),
            Interval::new(10, 20).unwrap()
        );
        assert!(engine.state.promises.is_empty());
        assert_eq!(engine.sequence(), sequence_before);
    }

    #[test]
    fn failed_multi_pool_adjustment_does_not_mutate_any_timeline() {
        let (mut engine, first_pool_id) = engine_with_capacity_curve(constant_capacity_curve(10));
        let second_pool_id =
            create_pool_with_capacity_curve(&mut engine, constant_capacity_curve(5));
        let candidate = bundle(vec![
            claim(first_pool_id, 0, 10, 3),
            claim(second_pool_id, 0, 10, 6),
        ]);
        let first_timeline_before = engine
            .slack_timeline(first_pool_id)
            .expect("the first timeline should exist")
            .clone();
        let second_timeline_before = engine
            .slack_timeline(second_pool_id)
            .expect("the second timeline should exist")
            .clone();
        let sequence_before = engine.sequence();

        let outcome = engine
            .hold_at(candidate, EXPIRES_AT, NOW)
            .expect("unavailability should be a normal outcome");

        assert!(matches!(outcome, HoldOutcome::Unavailable { .. }));
        assert_eq!(
            engine.slack_timeline(first_pool_id),
            Some(&first_timeline_before)
        );
        assert_eq!(
            engine.slack_timeline(second_pool_id),
            Some(&second_timeline_before)
        );
        assert_eq!(engine.sequence(), sequence_before);
    }

    #[test]
    fn overlapping_candidate_claims_are_checked_together() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 6), claim(pool_id, 0, 10, 6)]);

        assert_eq!(engine.check_availability(&candidate), Ok(false));
    }

    #[test]
    fn consecutive_candidate_claims_are_not_added_together() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 5, 10), claim(pool_id, 5, 10, 10)]);

        assert_eq!(engine.check_availability(&candidate), Ok(true));
    }

    #[test]
    fn expires_a_hold_at_its_deadline_and_is_safe_to_retry() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let promise_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);

        assert_eq!(engine.process_expirations(99), Ok(0));
        assert_eq!(engine.sequence().get(), 1);
        assert!(matches!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Held { expires_at: 100 }
        ));

        assert_eq!(engine.process_expirations(100), Ok(1));
        assert_eq!(engine.sequence().get(), 2);
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(10)
        );
        assert_eq!(
            engine
                .promise(promise_id)
                .expect("the promise should exist")
                .state(),
            PromiseState::Expired
        );

        assert_eq!(engine.process_expirations(100), Ok(0));
        assert_eq!(engine.sequence().get(), 2);
    }

    #[test]
    fn hold_processes_expirations_before_checking_capacity() {
        let (mut engine, pool_id) = engine_with_pool(10);
        let expired_id = add_held_promise_at(&mut engine, claim(pool_id, 0, 10, 10), 100, 1);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 10)]);

        let new_id = held_promise_id(
            engine
                .hold_at(candidate, 200, 100)
                .expect("the expired hold should release capacity"),
        );

        assert_eq!(
            engine
                .promise(expired_id)
                .expect("the old promise should exist")
                .state(),
            PromiseState::Expired
        );
        assert!(matches!(
            engine
                .promise(new_id)
                .expect("the new promise should exist")
                .state(),
            PromiseState::Held { expires_at: 200 }
        ));
        assert_eq!(engine.sequence().get(), 3);
        assert_eq!(
            engine
                .slack_timeline(pool_id)
                .expect("the timeline should exist")
                .slack_at(5),
            Ok(0)
        );
    }
}
