//! Time sources used at the boundary of the state machine.
//!
//! A clock chooses the timestamp for a new command. The engine then passes that
//! timestamp explicitly through its deterministic transition logic. Replayed or
//! replicated commands can therefore reuse their recorded timestamp instead of
//! consulting the local machine's clock.

use crate::domain::{DomainError, Timestamp};
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of authoritative timestamps for new commands.
///
/// Implementations should return whole UTC seconds since the Unix epoch. A clock
/// is consulted once per command; domain objects never read it directly.
pub trait Clock {
    /// Returns the timestamp to use for the current command.
    ///
    /// # Errors
    ///
    /// Returns an error when the clock cannot produce a representable timestamp.
    fn now(&self) -> Result<Timestamp, DomainError>;
}

/// A clock backed by the host operating system.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Result<Timestamp, DomainError> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DomainError::SystemTimeOutOfRange)?;

        Timestamp::try_from(elapsed.as_secs()).map_err(|_| DomainError::SystemTimeOutOfRange)
    }
}
