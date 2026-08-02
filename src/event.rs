//! Stable audit records emitted by successful state transitions.

use crate::domain::{
    Interval, PromiseId, Quantity, ResourcePoolId, SequenceNumber, Timestamp, Version,
};

/// The kind of durable state transition represented by an [`Event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// A resource pool was created.
    ResourceCreated,
    /// A resource pool's capacity curve was revised.
    CapacityRevised,
    /// A temporary promise was created.
    HoldCreated,
    /// A held promise was committed.
    HoldCommitted,
    /// A promise was released.
    PromiseReleased,
    /// A live promise was replaced atomically.
    PromiseReplaced,
    /// A held promise expired.
    HoldExpired,
    /// A forced capacity revision created a deficit.
    DeficitCreated,
    /// An existing deficit changed magnitude or boundaries.
    DeficitChanged,
    /// A previously existing deficit was resolved.
    DeficitResolved,
}

/// Minimal stable data needed to audit an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventData {
    /// A resource-pool transition.
    ResourcePool {
        /// Pool involved in the transition.
        resource_pool_id: ResourcePoolId,
    },
    /// A promise transition and its resulting version.
    Promise {
        /// Promise involved in the transition.
        promise_id: PromiseId,
        /// Version after the transition.
        version: Version,
    },
    /// A capacity deficit transition.
    Deficit {
        /// Pool containing the deficit.
        resource_pool_id: ResourcePoolId,
        /// Interval affected by the transition.
        interval: Interval,
        /// Positive deficit magnitude.
        quantity: Quantity,
        /// Active promises overlapping the interval.
        affected_promise_ids: Vec<PromiseId>,
    },
}

/// A stable, ordered audit record for one successful state transition.
///
/// Durable prepared transitions, not commands or events alone, are the recovery input.
/// Events contain exact audit facts but no references into engine state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    sequence: SequenceNumber,
    timestamp: Timestamp,
    kind: EventKind,
    data: EventData,
}

impl Event {
    /// Restores an event after validating that its kind matches its payload.
    pub(crate) fn restore(
        sequence: SequenceNumber,
        timestamp: Timestamp,
        kind: EventKind,
        data: EventData,
    ) -> Option<Self> {
        let valid = matches!(
            (kind, &data),
            (
                EventKind::ResourceCreated | EventKind::CapacityRevised,
                EventData::ResourcePool { .. }
            ) | (
                EventKind::HoldCreated
                    | EventKind::HoldCommitted
                    | EventKind::PromiseReleased
                    | EventKind::PromiseReplaced
                    | EventKind::HoldExpired,
                EventData::Promise { .. }
            ) | (
                EventKind::DeficitCreated | EventKind::DeficitChanged | EventKind::DeficitResolved,
                EventData::Deficit { .. }
            )
        );
        if !valid || sequence.get() == 0 {
            return None;
        }
        if let EventData::Deficit {
            quantity,
            affected_promise_ids,
            ..
        } = &data
            && (*quantity == 0 || !affected_promise_ids.windows(2).all(|ids| ids[0] < ids[1]))
        {
            return None;
        }
        Some(Self {
            sequence,
            timestamp,
            kind,
            data,
        })
    }

    pub(crate) fn new(
        sequence: SequenceNumber,
        timestamp: Timestamp,
        kind: EventKind,
        data: EventData,
    ) -> Self {
        Self {
            sequence,
            timestamp,
            kind,
            data,
        }
    }

    /// Returns the global sequence assigned to this transition.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the authoritative timestamp supplied to the transition.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the transition kind.
    pub fn kind(&self) -> EventKind {
        self.kind
    }

    /// Returns the stable audit payload.
    pub fn data(&self) -> &EventData {
        &self.data
    }
}
