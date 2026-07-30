//! Authoritative in-memory state and admission control.
//!
//! The engine owns resource pools, accepted promises, and the global sequence.
//! It evaluates bundles against active held and committed claims without storing
//! a second authoritative usage counter.

use crate::clock::{Clock, SystemClock};
use crate::domain::DomainError;
use crate::domain::{
    Bundle, CapacityCurve, Claim, Promise, PromiseId, PromiseState, Quantity, ResourcePool,
    ResourcePoolId, SequenceNumber, Timestamp, Version,
};
use crate::index::SlackTimeline;
use std::collections::BTreeMap;

/// The direction of a derived timeline adjustment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimelineAdjustment {
    /// Consumes slack when a promise becomes active.
    Consume,
    /// Restores slack when a promise stops being active.
    Restore,
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
            let adjusted_timelines =
                self.adjusted_timelines(expired_promise.bundle(), TimelineAdjustment::Restore)?;

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

    /// Atomically holds a bundle using an authoritative timestamp.
    ///
    /// Due expirations are processed before the candidate bundle is evaluated.
    /// Rejected holds do not create a promise or consume a sequence for the hold
    /// itself; expiration transitions processed first remain committed.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid deadline, a missing pool, arithmetic
    /// overflow, unavailable capacity, or sequence exhaustion.
    pub(crate) fn hold_at(
        &mut self,
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<PromiseId, DomainError> {
        self.process_expirations(now)?;

        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        if !self.check_availability(&bundle)? {
            return Err(DomainError::CapacityExceeded);
        }

        let adjusted_timelines = self.adjusted_timelines(&bundle, TimelineAdjustment::Consume)?;
        let next_sequence = self.next_sequence()?;
        let promise = Promise::new(bundle, expires_at, now, next_sequence)?;
        let promise_id = promise.id();

        self.promises.insert(promise_id, promise);
        self.slack_timelines.extend(adjusted_timelines);
        self.sequence = next_sequence;

        Ok(promise_id)
    }

    /// Atomically holds a bundle using one timestamp read from the engine's clock.
    ///
    /// The deterministic transition is delegated to `hold_at`, allowing replay
    /// and future replication to apply a previously chosen timestamp directly.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot provide a timestamp or when the
    /// bundle fails validation or admission.
    pub fn hold(
        &mut self,
        bundle: Bundle,
        expires_at: Timestamp,
    ) -> Result<PromiseId, DomainError> {
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
        let adjusted_timelines =
            self.adjusted_timelines(released_promise.bundle(), TimelineAdjustment::Restore)?;

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

    /// Checks an atomic bundle against committed usage in every referenced pool.
    ///
    /// Claims are grouped by pool so overlapping candidate claims are evaluated
    /// together. Due hold expirations must be processed before calling this
    /// function.
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

    fn adjusted_timelines(
        &self,
        bundle: &Bundle,
        adjustment: TimelineAdjustment,
    ) -> Result<BTreeMap<ResourcePoolId, SlackTimeline>, DomainError> {
        let mut claims_by_pool: BTreeMap<ResourcePoolId, Vec<&Claim>> = BTreeMap::new();

        for claim in bundle.claims() {
            claims_by_pool
                .entry(claim.pool_id())
                .or_default()
                .push(claim);
        }

        let mut adjusted_timelines = BTreeMap::new();

        for (pool_id, claims) in claims_by_pool {
            let mut timeline = self
                .slack_timelines
                .get(&pool_id)
                .ok_or(DomainError::ResourcePoolNotFound)?
                .clone();

            for claim in claims {
                let quantity = i128::from(claim.quantity());
                let claim_delta = match adjustment {
                    TimelineAdjustment::Consume => -quantity,
                    TimelineAdjustment::Restore => quantity,
                };

                if adjustment == TimelineAdjustment::Consume {
                    let available = timeline
                        .minimum_slack(claim.interval())
                        .map_err(|_| DomainError::IndexOverflow)?;
                    if available < quantity {
                        return Err(DomainError::CapacityExceeded);
                    }
                }

                timeline
                    .apply_delta(claim.interval(), claim_delta)
                    .map_err(|_| DomainError::IndexOverflow)?;
            }

            adjusted_timelines.insert(pool_id, timeline);
        }

        Ok(adjusted_timelines)
    }

    /// Simulates candidate claims against a pool's derived slack index.
    ///
    /// The real timeline is left untouched. This test-only path is used to
    /// compare the index with the authoritative reference calculation before
    /// the index becomes part of admission control.
    #[cfg(test)]
    fn check_pool_availability_indexed(
        &self,
        pool_id: ResourcePoolId,
        candidate_claims: &[&Claim],
    ) -> Result<bool, DomainError> {
        let mut simulated_timeline = self
            .slack_timelines
            .get(&pool_id)
            .ok_or(DomainError::ResourcePoolNotFound)?
            .clone();

        for candidate_claim in candidate_claims {
            let quantity = i128::from(candidate_claim.quantity());
            let minimum_slack = simulated_timeline
                .minimum_slack(candidate_claim.interval())
                .map_err(|_| DomainError::IndexOverflow)?;

            if minimum_slack < quantity {
                return Ok(false);
            }

            simulated_timeline
                .apply_delta(candidate_claim.interval(), -quantity)
                .map_err(|_| DomainError::IndexOverflow)?;
        }

        Ok(true)
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
    use crate::domain::{CapacitySegment, Interval};

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
        let adjusted_timelines = engine
            .adjusted_timelines(&bundle, TimelineAdjustment::Consume)
            .expect("the held promise should fit");
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

        assert_eq!(
            engine.check_availability(&candidate),
            Err(DomainError::ResourcePoolNotFound)
        );
    }

    #[test]
    fn an_unreserved_pool_has_its_full_capacity_available() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 10)]);

        assert_eq!(engine.check_availability(&candidate), Ok(true));
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
    fn overlapping_candidate_claims_are_checked_together() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 10, 6), claim(pool_id, 0, 10, 6)]);
        let candidate_claims: Vec<&Claim> = candidate.claims().iter().collect();

        assert_eq!(engine.check_availability(&candidate), Ok(false));
        assert_eq!(
            engine.check_pool_availability_indexed(pool_id, &candidate_claims),
            Ok(false)
        );
    }

    #[test]
    fn consecutive_candidate_claims_are_not_added_together() {
        let (engine, pool_id) = engine_with_pool(10);
        let candidate = bundle(vec![claim(pool_id, 0, 5, 10), claim(pool_id, 5, 10, 10)]);
        let candidate_claims: Vec<&Claim> = candidate.claims().iter().collect();

        assert_eq!(engine.check_availability(&candidate), Ok(true));
        assert_eq!(
            engine.check_pool_availability_indexed(pool_id, &candidate_claims),
            Ok(true)
        );
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

        let new_id = engine
            .hold_at(candidate, 200, 100)
            .expect("the expired hold should release capacity");

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
