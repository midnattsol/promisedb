//! Authoritative in-memory state and admission control.
//!
//! The engine owns resource pools, accepted promises, and the global sequence.
//! It evaluates bundles against active held and committed claims without storing
//! a second authoritative usage counter.

mod availability;
mod capacity_revision;

pub use availability::{AvailabilityConflict, HoldOutcome, ReplaceOutcome};
pub use capacity_revision::{
    AtRiskPromise, CapacityDeficit, CapacityRevisionMode, CapacityRevisionOutcome,
};

use crate::clock::{Clock, SystemClock};
use crate::command::{Command, CommandResult};
use crate::domain::DomainError;
use crate::domain::{
    Bundle, CapacityCurve, Claim, Interval, Promise, PromiseId, PromiseState, Quantity,
    ReplacementState, ResourcePool, ResourcePoolId, SequenceNumber, Timestamp, Version,
};
use crate::event::Event;
use crate::index::SlackTimeline;
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

/// The single-node state machine for PromiseDB.
///
/// All mutating operations are serialized through this value. Resource pools
/// and promises are authoritative; temporal usage is derived from active
/// promises when availability is evaluated.
pub struct Engine {
    clock: Box<dyn Clock>,
    resource_pools: BTreeMap<ResourcePoolId, ResourcePool>,
    slack_timelines: BTreeMap<ResourcePoolId, SlackTimeline>,
    promises: BTreeMap<PromiseId, Promise>,
    events: Vec<Event>,
    sequence: SequenceNumber,
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
            resource_pools: BTreeMap::new(),
            slack_timelines: BTreeMap::new(),
            promises: BTreeMap::new(),
            events: Vec::new(),
            sequence: SequenceNumber::new(0),
        }
    }

    /// Returns a resource pool by ID.
    pub fn resource_pool(&self, id: ResourcePoolId) -> Option<&ResourcePool> {
        self.resource_pools.get(&id)
    }
    /// Returns the derived slack timeline for a resource pool by ID.
    ///
    /// The timeline is an acceleration index reconstructed from the pool's
    /// capacity curve and active promises; it is not authoritative state.
    pub fn slack_timeline(&self, id: ResourcePoolId) -> Option<&SlackTimeline> {
        self.slack_timelines.get(&id)
    }

    /// Returns the latest sequence committed by the engine.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns a promise by ID.
    pub fn promise(&self, id: PromiseId) -> Option<&Promise> {
        self.promises.get(&id)
    }

    /// Returns emitted events whose sequence is at least `from_sequence`.
    ///
    /// Events are returned in global sequence order. The current scaffold remains
    /// empty until command variants define their stable event payloads.
    pub fn watch_events(&self, from_sequence: SequenceNumber) -> &[Event] {
        let first = self
            .events
            .partition_point(|event| event.sequence() < from_sequence);
        &self.events[first..]
    }

    /// Applies one deterministic command at an authoritative timestamp.
    ///
    /// Command variants and dispatch arms are intentionally supplied by the next
    /// design step. Keeping `now` outside [`Command`] prevents clients from choosing
    /// state-machine time while allowing replay to reuse the original timestamp.
    ///
    /// # Errors
    ///
    /// Returns an operation-specific domain error after command variants are added.
    pub fn apply(
        &mut self,
        command: Command,
        _now: Timestamp,
    ) -> Result<CommandResult, DomainError> {
        match command {}
    }

    /// Calculates, but does not commit, the next global sequence.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::SequenceOverflow`] when the current sequence is
    /// `u64::MAX`.
    pub(crate) fn next_sequence(&self) -> Result<SequenceNumber, DomainError> {
        self.sequence.next()
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
                .promises
                .get(&promise_id)
                .ok_or(DomainError::PromiseNotFound)?
                .clone();

            expired_promise.expire(now, next_sequence)?;
            let adjusted_timelines = self.restored_timelines(expired_promise.bundle())?;

            self.promises.insert(promise_id, expired_promise);
            self.slack_timelines.extend(adjusted_timelines);
            self.sequence = next_sequence;
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
        unit: String,
        capacity_curve: CapacityCurve,
        now: Timestamp,
    ) -> Result<ResourcePoolId, DomainError> {
        self.process_expirations(now)?;

        if self.resource_pools.contains_key(&pool_id) {
            return Err(DomainError::ResourcePoolAlreadyExists);
        }

        let pool = ResourcePool::with_id(pool_id, display_name, unit, capacity_curve);
        let slack_timeline = SlackTimeline::from_capacity_curve(pool.capacity_curve())
            .map_err(|_| DomainError::IndexOverflow)?;
        let next_sequence = self.next_sequence()?;

        self.resource_pools.insert(pool_id, pool);
        self.slack_timelines.insert(pool_id, slack_timeline);
        self.sequence = next_sequence;

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
        unit: String,
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

        let current_pool = self
            .resource_pools
            .get(&pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?;
        let active_promises: Vec<&Promise> = self.promises.values().collect();
        let revised_timeline =
            SlackTimeline::from_capacity_and_promises(&capacity_curve, pool_id, &active_promises)
                .map_err(|_| DomainError::IndexOverflow)?;
        let deficits = self.capacity_deficits(pool_id, &revised_timeline)?;

        if mode == CapacityRevisionMode::Strict && !deficits.is_empty() {
            return Err(DomainError::CapacityRevisionCreatesDeficit);
        }

        let next_sequence = self.next_sequence()?;
        let mut revised_pool = current_pool.clone();
        revised_pool.replace_capacity_curve(capacity_curve);
        let mut affected_promise_ids: Vec<PromiseId> = deficits
            .iter()
            .flat_map(|deficit| deficit.affected_promise_ids.iter().copied())
            .collect();
        affected_promise_ids.sort_unstable();
        affected_promise_ids.dedup();

        self.resource_pools.insert(pool_id, revised_pool);
        self.slack_timelines.insert(pool_id, revised_timeline);
        self.sequence = next_sequence;

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

    /// Lists active promises overlapping current deficit intervals at `now`.
    ///
    /// Optional pool and time filters restrict which deficits are considered. Due
    /// expirations are processed before the derived result is calculated.
    ///
    /// # Errors
    ///
    /// Returns an error for a missing requested pool, expiration failure, or index
    /// arithmetic overflow.
    pub(crate) fn list_at_risk_at(
        &mut self,
        resource_pool_id: Option<ResourcePoolId>,
        time_range: Option<Interval>,
        now: Timestamp,
    ) -> Result<Vec<AtRiskPromise>, DomainError> {
        self.process_expirations(now)?;

        if resource_pool_id.is_some_and(|pool_id| !self.resource_pools.contains_key(&pool_id)) {
            return Err(DomainError::ResourcePoolNotFound);
        }

        let mut promises_by_id: BTreeMap<PromiseId, Vec<CapacityDeficit>> = BTreeMap::new();
        for (pool_id, timeline) in &self.slack_timelines {
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

    /// Lists active promises overlapping current deficit intervals.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock or deterministic query transition fails.
    pub fn list_at_risk(
        &mut self,
        resource_pool_id: Option<ResourcePoolId>,
        time_range: Option<Interval>,
    ) -> Result<Vec<AtRiskPromise>, DomainError> {
        let now = self.clock.now()?;
        self.list_at_risk_at(resource_pool_id, time_range, now)
    }

    /// Explains every interval that prevents a bundle from being admitted at `now`.
    ///
    /// This query never consumes candidate capacity. Due expirations are processed
    /// first so the explanation observes the same state as a subsequent command.
    ///
    /// # Errors
    ///
    /// Returns an error when expiration or admission evaluation fails.
    pub(crate) fn explain_unavailable_at(
        &mut self,
        bundle: &Bundle,
        now: Timestamp,
    ) -> Result<Vec<AvailabilityConflict>, DomainError> {
        self.process_expirations(now)?;
        match self.evaluate_bundle_admission(bundle)? {
            BundleAdmission::Available(_) => Ok(Vec::new()),
            BundleAdmission::Unavailable(conflicts) => Ok(conflicts),
        }
    }

    /// Explains every interval that currently prevents a bundle from admission.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock or deterministic query transition fails.
    pub fn explain_unavailable(
        &mut self,
        bundle: &Bundle,
    ) -> Result<Vec<AvailabilityConflict>, DomainError> {
        let now = self.clock.now()?;
        self.explain_unavailable_at(bundle, now)
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
        self.process_expirations(now)?;

        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        let adjusted_timelines = match self.evaluate_bundle_admission(&bundle)? {
            BundleAdmission::Available(timelines) => timelines,
            BundleAdmission::Unavailable(conflicts) => {
                return Ok(HoldOutcome::Unavailable { conflicts });
            }
        };
        let next_sequence = self.next_sequence()?;
        let promise = Promise::new(bundle, expires_at, now, next_sequence)?;
        let promise_id = promise.id();

        self.promises.insert(promise_id, promise);
        self.slack_timelines.extend(adjusted_timelines);
        self.sequence = next_sequence;

        Ok(HoldOutcome::Held(promise_id))
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
            .promises
            .get_mut(&promise_id)
            .ok_or(DomainError::PromiseNotFound)?;

        if promise.state() == PromiseState::Expired {
            return Err(DomainError::HoldExpired);
        }

        let new_version = promise.commit(expected_version, now, new_sequence)?;

        self.sequence = new_sequence;

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
            .promises
            .get(&promise_id)
            .ok_or(DomainError::PromiseNotFound)?
            .clone();

        if released_promise.state() == PromiseState::Expired {
            return Err(DomainError::HoldExpired);
        }

        let new_version = released_promise.release(expected_version, now, new_sequence)?;
        let adjusted_timelines = self.restored_timelines(released_promise.bundle())?;

        self.promises.insert(promise_id, released_promise);
        self.slack_timelines.extend(adjusted_timelines);
        self.sequence = new_sequence;

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

        self.promises.insert(promise_id, replaced_promise);
        self.slack_timelines.extend(final_timelines);
        self.sequence = next_sequence;

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

        for promise in self.promises.values() {
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
        let mut slack_timeline = base_timeline.clone();
        let mut demand_events: Vec<(Timestamp, i128)> = Vec::new();

        for claim in candidate_claims {
            let interval = claim.interval();
            let quantity = i128::from(claim.quantity());
            demand_events.push((interval.start(), quantity));
            demand_events.push((interval.end(), -quantity));
        }

        for point in slack_timeline
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
            let slack = slack_timeline
                .slack_at(interval.start())
                .map_err(|_| DomainError::IndexOverflow)?;
            let final_slack = slack
                .checked_sub(current_demand)
                .ok_or(DomainError::IndexOverflow)?;
            let minimum_allowed_slack = match deficit_floor {
                Some(timeline) => timeline
                    .slack_at(interval.start())
                    .map_err(|_| DomainError::IndexOverflow)?
                    .min(0),
                None => 0,
            };
            let available_quantity = if slack <= 0 {
                0
            } else {
                Quantity::try_from(slack).map_err(|_| DomainError::IndexOverflow)?
            };

            if final_slack < minimum_allowed_slack {
                let conflicting_promise_ids = self
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

            slack_timeline
                .apply_delta(interval, -current_demand)
                .map_err(|_| DomainError::IndexOverflow)?;
        }

        if conflicts.is_empty() {
            Ok(PoolAdmission::Available(slack_timeline))
        } else {
            Ok(PoolAdmission::Unavailable(conflicts))
        }
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
                let quantity =
                    Quantity::try_from(deficit.amount()).map_err(|_| DomainError::IndexOverflow)?;
                let affected_promise_ids = self
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
                .slack_timelines
                .get(&pool_id)
                .ok_or(DomainError::ResourcePoolNotFound)?
                .clone();
            for claim in claims {
                timeline
                    .apply_delta(claim.interval(), i128::from(claim.quantity()))
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
    use crate::domain::CapacitySegment;

    const NOW: Timestamp = 0;
    const EXPIRES_AT: Timestamp = 1_000;

    #[derive(Clone, Copy)]
    struct FixedClock(Timestamp);

    impl Clock for FixedClock {
        fn now(&self) -> Result<Timestamp, DomainError> {
            Ok(self.0)
        }
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
            "units".into(),
            constant_capacity_curve(capacity),
        );
        let pool_id = pool.id();
        let timeline = SlackTimeline::from_capacity_curve(pool.capacity_curve())
            .expect("the slack timeline should be created");
        engine.resource_pools.insert(pool_id, pool);
        engine.slack_timelines.insert(pool_id, timeline);
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
                "units".into(),
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
        let promise = Promise::new(bundle, expires_at, NOW, SequenceNumber::new(sequence))
            .expect("the promise should be valid");
        let promise_id = promise.id();
        engine.promises.insert(promise_id, promise);
        engine.slack_timelines.extend(adjusted_timelines);
        engine.sequence = SequenceNumber::new(sequence);
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
    fn create_resource_pool_at_publishes_the_pool_and_sequence() {
        let mut engine = Engine::with_clock(FixedClock(NOW));
        let pool_id = ResourcePoolId::generate();

        let created_id = engine
            .create_resource_pool_at(
                pool_id,
                "Machine pool".into(),
                "machines".into(),
                constant_capacity_curve(10),
                NOW,
            )
            .expect("the resource pool should be created");

        let pool = engine
            .resource_pool(created_id)
            .expect("the resource pool should exist");
        assert_eq!(created_id, pool_id);
        assert_eq!(pool.display_name(), "Machine pool");
        assert_eq!(pool.unit(), "machines");
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
                "machines".into(),
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
                "machines".into(),
                constant_capacity_curve(10),
                NOW,
            )
            .expect("the first resource pool should be created");

        let result = engine.create_resource_pool_at(
            pool_id,
            "Replacement".into(),
            "people".into(),
            constant_capacity_curve(20),
            NOW,
        );

        assert_eq!(result, Err(DomainError::ResourcePoolAlreadyExists));
        let pool = engine
            .resource_pool(pool_id)
            .expect("the original resource pool should remain");
        assert_eq!(pool.display_name(), "Original");
        assert_eq!(pool.unit(), "machines");
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
                .list_at_risk_at(None, None, NOW)
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
            .list_at_risk_at(Some(pool_id), Some(Interval::new(5, 15).unwrap()), NOW)
            .expect("the at-risk promises should be listed");
        let outside = engine
            .list_at_risk_at(Some(pool_id), Some(Interval::new(10, 20).unwrap()), NOW)
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
            .explain_unavailable_at(&candidate, NOW)
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
        let detached = Promise::new(
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
        let detached = Promise::new(
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
        let detached = Promise::new(
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
        assert!(engine.promises.is_empty());
        assert!(engine.slack_timelines.is_empty());
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
        assert!(engine.promises.is_empty());
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
