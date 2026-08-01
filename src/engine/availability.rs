//! Structured outcomes produced by admission evaluation.

use crate::domain::{Bundle, Interval, PromiseId, Quantity, ResourcePoolId, Timestamp, Version};

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

/// The first feasible materialized bundle found in a candidate range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slot {
    pub(super) start: Timestamp,
    pub(super) bundle: Bundle,
}

impl Slot {
    /// Returns the candidate anchor selected by the search.
    pub fn start(&self) -> Timestamp {
        self.start
    }

    /// Returns the materialized bundle feasible at the selected anchor.
    pub fn bundle(&self) -> &Bundle {
        &self.bundle
    }
}

/// The normal business outcome of searching for and holding a slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotOutcome {
    /// The first feasible slot was held atomically.
    Held {
        /// The predetermined identity of the created promise.
        promise_id: PromiseId,
        /// The selected candidate anchor.
        start: Timestamp,
    },
    /// No candidate was feasible and no promise was created.
    Unavailable {
        /// Exact number of candidates evaluated.
        attempts: u128,
    },
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

/// Conflicts that prevented one alternative in an ordered choice from being held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChoiceConflict {
    pub(super) alternative_index: usize,
    pub(super) conflicts: Vec<AvailabilityConflict>,
}

impl ChoiceConflict {
    /// Returns the zero-based position of the unavailable alternative.
    pub fn alternative_index(&self) -> usize {
        self.alternative_index
    }

    /// Returns every normalized conflict for this alternative.
    pub fn conflicts(&self) -> &[AvailabilityConflict] {
        &self.conflicts
    }
}

/// The normal business outcome of attempting to hold an ordered choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChoiceOutcome {
    /// The first feasible alternative was held atomically.
    Held {
        /// The predetermined identity of the created promise.
        promise_id: PromiseId,
        /// The zero-based position of the selected alternative.
        alternative_index: usize,
    },
    /// No alternative was feasible and no promise was created.
    Unavailable {
        /// Conflicts for every alternative, in choice order.
        conflicts: Vec<ChoiceConflict>,
    },
}

/// The normal business outcome of an attempted atomic replacement.
///
/// Engine failures and invalid requests remain in the outer `Result`; insufficient
/// capacity is represented by [`ReplaceOutcome::Unavailable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplaceOutcome {
    /// The promise was replaced while preserving its identity.
    Replaced {
        /// The unchanged identity of the replaced promise.
        promise_id: PromiseId,
        /// The promise's new local version.
        version: Version,
    },
    /// The original promise was preserved because the replacement lacks capacity.
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
