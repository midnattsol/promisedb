//! Errors raised while constructing and transitioning domain values.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// An error caused by invalid PromiseDB domain data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainError {
    /// An interval has an end that is not greater than its start.
    InvalidInterval,
    /// A claim or resource pool has a zero quantity.
    InvalidQuantity,
    /// A hold expiration deadline is not in the future.
    InvalidExpiration,
    /// A bundle contains no claims.
    EmptyBundle,
    /// An operation is not allowed from the promise's current state.
    InvalidPromiseState,
    /// The expected promise version does not match its current version.
    VersionConflict,
    /// A promise version cannot be incremented without overflowing.
    VersionOverflow,
    /// A held promise has reached its expiration deadline.
    HoldExpired,
    /// A held promise has not reached its expiration deadline yet.
    HoldNotExpired,
}

impl Display for DomainError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidInterval => "interval start must be less than its end",
            Self::InvalidQuantity => "quantity must be greater than zero",
            Self::InvalidExpiration => "expiration deadline must be in the future",
            Self::EmptyBundle => "bundle must contain at least one claim",
            Self::InvalidPromiseState => "operation is not allowed from the current promise state",
            Self::VersionConflict => "expected promise version does not match the current version",
            Self::VersionOverflow => "promise version cannot be incremented",
            Self::HoldExpired => "held promise has expired",
            Self::HoldNotExpired => "held promise has not expired yet",
        };

        formatter.write_str(message)
    }
}

impl Error for DomainError {}
