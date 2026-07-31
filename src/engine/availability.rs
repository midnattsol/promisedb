//! Structured outcomes produced by admission evaluation.

use crate::domain::{Interval, PromiseId, Quantity, ResourcePoolId};

/// A time-bounded reason why a candidate bundle cannot be admitted.
///
/// Conflicts describe candidate demand relative to the slack that existed before
/// the attempted hold. PromiseDB may return multiple conflicts for one bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityConflict {
    pub(super) resource_pool_id: ResourcePoolId,
    pub(super) blocking_interval: Interval,
    pub(super) required_quantity: Quantity,
    pub(super) available_quantity: Quantity,
    pub(super) deficit_quantity: Quantity,
    pub(super) conflicting_promise_ids: Vec<PromiseId>,
}

impl AvailabilityConflict {
    /// Returns the resource pool whose slack is insufficient.
    pub fn resource_pool_id(&self) -> ResourcePoolId {
        self.resource_pool_id
    }

    /// Returns the half-open interval during which the conflict applies.
    pub fn blocking_interval(&self) -> Interval {
        self.blocking_interval
    }

    /// Returns the candidate bundle's combined demand during the interval.
    pub fn required_quantity(&self) -> Quantity {
        self.required_quantity
    }

    /// Returns the non-negative slack available before the candidate bundle.
    pub fn available_quantity(&self) -> Quantity {
        self.available_quantity
    }

    /// Returns the additional quantity required to admit the candidate bundle.
    pub fn deficit_quantity(&self) -> Quantity {
        self.deficit_quantity
    }

    /// Returns active promises whose claims overlap the blocking pool and interval.
    ///
    /// IDs emitted by the engine are sorted and deduplicated.
    pub fn conflicting_promise_ids(&self) -> &[PromiseId] {
        &self.conflicting_promise_ids
    }
}

/// The normal business outcome of an attempted hold.
///
/// Engine failures and invalid requests remain in the outer `Result`; insufficient
/// capacity is represented by [`HoldOutcome::Unavailable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HoldOutcome {
    /// The complete bundle was held atomically under the returned promise ID.
    Held(PromiseId),
    /// The bundle was not held because one or more intervals lack capacity.
    Unavailable {
        /// All normalized conflicts in deterministic order.
        conflicts: Vec<AvailabilityConflict>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conflict() -> AvailabilityConflict {
        AvailabilityConflict {
            resource_pool_id: ResourcePoolId::generate(),
            blocking_interval: Interval::new(10, 20).expect("the interval should be valid"),
            required_quantity: 7,
            available_quantity: 4,
            deficit_quantity: 3,
            conflicting_promise_ids: Vec::new(),
        }
    }

    #[test]
    fn exposes_structured_conflict_details() {
        let conflict = conflict();

        assert_eq!(conflict.blocking_interval(), Interval::new(10, 20).unwrap());
        assert_eq!(conflict.required_quantity(), 7);
        assert_eq!(conflict.available_quantity(), 4);
        assert_eq!(conflict.deficit_quantity(), 3);
        assert!(conflict.conflicting_promise_ids().is_empty());
    }

    #[test]
    fn unavailable_outcome_can_contain_multiple_conflicts() {
        let first = conflict();
        let second = conflict();
        let outcome = HoldOutcome::Unavailable {
            conflicts: vec![first, second],
        };

        let HoldOutcome::Unavailable { conflicts } = outcome else {
            panic!("the outcome should be unavailable");
        };
        assert_eq!(conflicts.len(), 2);
    }
}
