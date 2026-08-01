//! Errors raised while constructing and transitioning domain values.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// An error raised while validating data or applying a PromiseDB transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainError {
    /// An interval has an end that is not greater than its start.
    InvalidInterval,
    /// Capacity segments expected in chronological order are out of order.
    UnsortedCapacitySegments,
    /// Two capacity segments overlap in time.
    OverlappingCapacitySegments,
    /// A resource unit has an empty or whitespace-only name.
    InvalidUnitName,
    /// A resource unit declares zero subunits per displayed unit.
    InvalidUnitScale,
    /// A claim has a zero quantity.
    InvalidQuantity,
    /// Capacity or usage arithmetic overflowed.
    QuantityOverflow,
    /// A derived index value cannot be represented without overflowing.
    IndexOverflow,
    /// A hold expiration deadline is not in the future.
    InvalidExpiration,
    /// A bundle contains no claims.
    EmptyBundle,
    /// A choice contains no alternative bundles.
    EmptyChoice,
    /// A resource pool already exists with the same identifier.
    ResourcePoolAlreadyExists,
    /// A referenced resource pool does not exist.
    ResourcePoolNotFound,
    /// A claim bundle would exceed a resource pool's available capacity.
    CapacityExceeded,
    /// A strict capacity revision would make active usage exceed capacity.
    CapacityRevisionCreatesDeficit,
    /// A promise already exists with the supplied identifier.
    PromiseAlreadyExists,
    /// A referenced promise does not exist.
    PromiseNotFound,
    /// An operation is not allowed from the promise's current state.
    InvalidPromiseState,
    /// An idempotency key was reused with a different normalized command.
    IdempotencyConflict,
    /// The expected promise version does not match its current version.
    VersionConflict,
    /// A promise version cannot be incremented without overflowing.
    VersionOverflow,
    /// The global sequence number cannot be incremented without overflowing.
    SequenceOverflow,
    /// The system clock cannot be represented as a PromiseDB timestamp.
    SystemTimeOutOfRange,
    /// A held promise has reached its expiration deadline.
    HoldExpired,
    /// A held promise has not reached its expiration deadline yet.
    HoldNotExpired,
}

impl Display for DomainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidInterval => "interval start must be less than its end",
            Self::UnsortedCapacitySegments => "capacity segments must be chronologically ordered",
            Self::OverlappingCapacitySegments => "capacity segments must not overlap",
            Self::InvalidUnitName => "unit name must not be empty",
            Self::InvalidUnitScale => "subunits per unit must be greater than zero",
            Self::InvalidQuantity => "quantity must be greater than zero",
            Self::QuantityOverflow => "capacity or usage arithmetic overflowed",
            Self::IndexOverflow => "derived index arithmetic overflowed",
            Self::InvalidExpiration => "expiration deadline must be in the future",
            Self::EmptyBundle => "bundle must contain at least one claim",
            Self::EmptyChoice => "choice must contain at least one alternative bundle",
            Self::ResourcePoolAlreadyExists => {
                "resource pool already exists with the same identifier"
            }
            Self::ResourcePoolNotFound => "resource pool does not exist",
            Self::CapacityExceeded => "claim bundle exceeds available resource pool capacity",
            Self::CapacityRevisionCreatesDeficit => {
                "strict capacity revision would create a deficit"
            }
            Self::PromiseAlreadyExists => "promise already exists with the same identifier",
            Self::PromiseNotFound => "promise does not exist",
            Self::InvalidPromiseState => "operation is not allowed from the current promise state",
            Self::IdempotencyConflict => "idempotency key was reused with a different command",
            Self::VersionConflict => "expected promise version does not match the current version",
            Self::VersionOverflow => "promise version cannot be incremented",
            Self::SequenceOverflow => "global sequence number cannot be incremented",
            Self::SystemTimeOutOfRange => {
                "system clock cannot be represented as a PromiseDB timestamp"
            }
            Self::HoldExpired => "held promise has expired",
            Self::HoldNotExpired => "held promise has not expired yet",
        };

        formatter.write_str(message)
    }
}

impl Error for DomainError {}
