//! Errors raised while constructing or updating derived indexes.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// An error raised when an index cannot preserve its invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexError {
    /// Slack points expected in chronological order are out of order.
    UnsortedSlackPoints,
    /// Two slack points have the same timestamp.
    DuplicateSlackTimestamp,
    /// Stored slack arithmetic exceeded the representable `i64` range.
    SlackOverflow,
    /// A point range is reversed or extends beyond its block.
    InvalidPointRange,
    /// Claim events would make reconstructed usage negative.
    InconsistentUsage,
}

impl Display for IndexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::UnsortedSlackPoints => "slack points must be chronologically ordered",
            Self::DuplicateSlackTimestamp => "slack points must not have duplicate timestamps",
            Self::SlackOverflow => "slack arithmetic overflowed",
            Self::InvalidPointRange => "point range is invalid for the slack block",
            Self::InconsistentUsage => "claim events produce inconsistent temporal usage",
        };

        formatter.write_str(message)
    }
}

impl Error for IndexError {}
