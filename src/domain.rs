//! Core domain types for PromiseDB.
//!
//! The domain models capacity pools and atomic groups of time-bounded claims.
//! Its constructors enforce the structural invariants required before an engine
//! can evaluate availability.

mod bundle;
mod capacity_curve;
mod claim;
mod error;
mod interval;
mod promise;
mod resource_pool;
mod unit;

pub use bundle::Bundle;
pub use capacity_curve::{CapacityCurve, CapacitySegment};
pub use claim::Claim;
pub use error::DomainError;
pub use interval::Interval;
pub use promise::{Promise, PromiseId, PromiseState, ReplacementState, SequenceNumber, Version};
pub use resource_pool::{ResourcePool, ResourcePoolId};
pub use unit::Unit;
/// An integer UTC timestamp.
pub type Timestamp = i64;

/// A non-negative integer count of a resource pool's configured subunits.
pub type Quantity = u64;
