//! Public types for capacity revisions and derived deficits.

use crate::domain::{Interval, PromiseId, Quantity, ResourcePoolId, SequenceNumber};

/// Controls whether a capacity revision may introduce a deficit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityRevisionMode {
    /// Reject the revision when active usage would exceed the new capacity.
    Strict,
    /// Accept physical reality and expose any resulting deficits.
    Force,
}

/// A normalized interval where active usage exceeds physical capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityDeficit {
    pub(super) resource_pool_id: ResourcePoolId,
    pub(super) interval: Interval,
    pub(super) quantity: Quantity,
    pub(super) affected_promise_ids: Vec<PromiseId>,
}

impl CapacityDeficit {
    pub(crate) fn restore(
        resource_pool_id: ResourcePoolId,
        interval: Interval,
        quantity: Quantity,
        affected_promise_ids: Vec<PromiseId>,
    ) -> Option<Self> {
        (quantity > 0 && affected_promise_ids.windows(2).all(|ids| ids[0] < ids[1])).then_some(
            Self {
                resource_pool_id,
                interval,
                quantity,
                affected_promise_ids,
            },
        )
    }

    /// Returns the resource pool in deficit.
    pub fn resource_pool_id(&self) -> ResourcePoolId {
        self.resource_pool_id
    }

    /// Returns the half-open interval where the deficit exists.
    pub fn interval(&self) -> Interval {
        self.interval
    }

    /// Returns the amount by which usage exceeds capacity.
    pub fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns active promises overlapping this deficit.
    pub fn affected_promise_ids(&self) -> &[PromiseId] {
        &self.affected_promise_ids
    }
}

/// The result of an applied capacity revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityRevisionOutcome {
    pub(super) sequence: SequenceNumber,
    pub(super) deficits: Vec<CapacityDeficit>,
    pub(super) affected_promise_ids: Vec<PromiseId>,
}

impl CapacityRevisionOutcome {
    pub(crate) fn restore(
        sequence: SequenceNumber,
        deficits: Vec<CapacityDeficit>,
        affected_promise_ids: Vec<PromiseId>,
    ) -> Option<Self> {
        (sequence.get() > 0 && affected_promise_ids.windows(2).all(|ids| ids[0] < ids[1]))
            .then_some(Self {
                sequence,
                deficits,
                affected_promise_ids,
            })
    }

    /// Returns the sequence assigned to the applied revision.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns all resulting deficit intervals in chronological order.
    pub fn deficits(&self) -> &[CapacityDeficit] {
        &self.deficits
    }

    /// Returns all affected promises, sorted and deduplicated.
    pub fn affected_promise_ids(&self) -> &[PromiseId] {
        &self.affected_promise_ids
    }
}

/// A live promise that overlaps one or more current deficits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtRiskPromise {
    pub(super) promise_id: PromiseId,
    pub(super) deficits: Vec<CapacityDeficit>,
}

impl AtRiskPromise {
    /// Returns the promise considered at risk.
    pub fn promise_id(&self) -> PromiseId {
        self.promise_id
    }

    /// Returns matching deficits that overlap the promise.
    pub fn deficits(&self) -> &[CapacityDeficit] {
        &self.deficits
    }
}
