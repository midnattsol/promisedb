//! Stable audit records emitted by successful state transitions.

use crate::domain::{PromiseId, ResourcePoolId, SequenceNumber, Timestamp, Version};

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

/// An entity referenced by an event without relying on memory layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventEntity {
    /// A resource pool involved in the transition.
    ResourcePool(ResourcePoolId),
    /// A promise involved in the transition.
    Promise(PromiseId),
}

/// A stable, ordered audit record for one successful state transition.
///
/// Event-specific audit payloads are intentionally deferred until the command
/// language is designed. IDs are represented as values; events never store
/// pointers or references into engine state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    sequence: SequenceNumber,
    timestamp: Timestamp,
    kind: EventKind,
    entities: Vec<EventEntity>,
    promise_version: Option<Version>,
}

impl Event {
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

    /// Returns referenced entities in canonical order.
    pub fn entities(&self) -> &[EventEntity] {
        &self.entities
    }

    /// Returns the resulting promise version when the event concerns a promise.
    pub fn promise_version(&self) -> Option<Version> {
        self.promise_version
    }
}
