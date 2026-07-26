#![expect(
    dead_code,
    reason = "promise transitions will be used by the in-memory engine"
)]

//! Promise identity, state, and lifecycle transitions.

use uuid::Uuid;

use super::{Bundle, DomainError, Timestamp};

/// The local revision of a promise.
///
/// Versions start at one and increase after every successful promise
/// transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(u64);

impl Version {
    const INITIAL: Self = Self(1);

    /// Returns the numeric representation of this version.
    pub fn get(self) -> u64 {
        self.0
    }

    /// Returns the next version.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::VersionOverflow`] when this version is `u64::MAX`.
    fn next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::VersionOverflow)
    }
}

/// A position in PromiseDB's global transition order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// Creates a sequence number from its numeric representation.
    ///
    /// Sequence allocation is the responsibility of the engine. This
    /// constructor does not enforce monotonicity.
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric representation of this sequence number.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// The opaque identifier of a [`Promise`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PromiseId(Uuid);

impl PromiseId {
    fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

/// The lifecycle state of a promise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromiseState {
    /// A temporary reservation that must be committed before its deadline.
    Held { expires_at: Timestamp },
    /// A confirmed commitment. Claim intervals still determine actual usage.
    Committed,
    /// A manually released promise that no longer consumes capacity.
    Released,
    /// A hold that reached its deadline before being committed.
    Expired,
}

/// An accepted atomic bundle with a versioned lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promise {
    id: PromiseId,
    state: PromiseState,
    bundle: Bundle,
    version: Version,
    created_sequence: SequenceNumber,
    updated_sequence: SequenceNumber,
}

impl Promise {
    /// Creates a held promise at version one.
    ///
    /// `now` and `sequence` must come from the authoritative engine rather than
    /// from an external client.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidExpiration`] when `expires_at <= now`.
    pub(crate) fn new(
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<Self, DomainError> {
        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        Ok(Self {
            id: PromiseId::generate(),
            state: PromiseState::Held { expires_at },
            bundle,
            version: Version::INITIAL,
            created_sequence: sequence,
            updated_sequence: sequence,
        })
    }

    /// Returns the promise's opaque identifier.
    pub fn id(&self) -> PromiseId {
        self.id
    }

    /// Returns the promise's current lifecycle state.
    pub fn state(&self) -> PromiseState {
        self.state
    }

    /// Returns the sequence that created this promise.
    pub fn created_sequence(&self) -> SequenceNumber {
        self.created_sequence
    }

    /// Returns the sequence of the latest successful transition.
    pub fn updated_sequence(&self) -> SequenceNumber {
        self.updated_sequence
    }

    /// Returns the atomically accepted bundle.
    pub fn bundle(&self) -> &Bundle {
        &self.bundle
    }

    /// Returns the promise's current local version.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Commits a live hold.
    ///
    /// # Errors
    ///
    /// Returns:
    ///
    /// - [`DomainError::VersionConflict`] when `expected_version` is stale.
    /// - [`DomainError::HoldExpired`] when the hold deadline has been reached.
    /// - [`DomainError::InvalidPromiseState`] when the promise is not held.
    /// - [`DomainError::VersionOverflow`] when the version cannot be incremented.
    pub(crate) fn commit(
        &mut self,
        expected_version: Version,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<(), DomainError> {
        if expected_version != self.version {
            return Err(DomainError::VersionConflict);
        }

        match self.state {
            PromiseState::Held { expires_at } if expires_at <= now => Err(DomainError::HoldExpired),
            PromiseState::Held { .. } => {
                let next_version = self.version.next()?;
                self.version = next_version;
                self.updated_sequence = sequence;
                self.state = PromiseState::Committed;
                Ok(())
            }
            _ => Err(DomainError::InvalidPromiseState),
        }
    }

    /// Releases a live hold or committed promise.
    ///
    /// # Errors
    ///
    /// Returns:
    ///
    /// - [`DomainError::VersionConflict`] when `expected_version` is stale.
    /// - [`DomainError::HoldExpired`] when a held promise's deadline was reached.
    /// - [`DomainError::InvalidPromiseState`] from a terminal state.
    /// - [`DomainError::VersionOverflow`] when the version cannot be incremented.
    pub(crate) fn release(
        &mut self,
        expected_version: Version,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<(), DomainError> {
        if expected_version != self.version {
            return Err(DomainError::VersionConflict);
        }

        match self.state {
            PromiseState::Held { expires_at } if expires_at <= now => Err(DomainError::HoldExpired),
            PromiseState::Held { .. } | PromiseState::Committed => {
                let next_version = self.version.next()?;
                self.version = next_version;
                self.updated_sequence = sequence;
                self.state = PromiseState::Released;
                Ok(())
            }
            _ => Err(DomainError::InvalidPromiseState),
        }
    }

    /// Expires a hold whose deadline has been reached.
    ///
    /// Expiration is an internal transition and therefore does not require an
    /// expected version from a client.
    ///
    /// # Errors
    ///
    /// Returns:
    ///
    /// - [`DomainError::HoldNotExpired`] when `now < expires_at`.
    /// - [`DomainError::InvalidPromiseState`] when the promise is not held.
    /// - [`DomainError::VersionOverflow`] when the version cannot be incremented.
    pub(crate) fn expire(
        &mut self,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<(), DomainError> {
        match self.state {
            PromiseState::Held { expires_at } if expires_at > now => {
                Err(DomainError::HoldNotExpired)
            }
            PromiseState::Held { .. } => {
                let next_version = self.version.next()?;
                self.version = next_version;
                self.updated_sequence = sequence;
                self.state = PromiseState::Expired;
                Ok(())
            }
            _ => Err(DomainError::InvalidPromiseState),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Claim, Interval, ResourcePoolId};

    const CREATED_SEQUENCE: SequenceNumber = SequenceNumber(10);
    const UPDATED_SEQUENCE: SequenceNumber = SequenceNumber(11);
    const NOW: Timestamp = 100;
    const EXPIRES_AT: Timestamp = 200;

    fn bundle() -> Bundle {
        let interval = Interval::new(1_000, 2_000).expect("the interval should be valid");
        let claim =
            Claim::new(ResourcePoolId::generate(), interval, 1).expect("the claim should be valid");
        Bundle::new(vec![claim]).expect("the bundle should be valid")
    }

    fn held_promise() -> Promise {
        Promise::new(bundle(), EXPIRES_AT, NOW, CREATED_SEQUENCE)
            .expect("the promise should be valid")
    }

    #[test]
    fn creates_a_held_promise_at_version_one() {
        let promise = held_promise();

        assert_eq!(
            promise.state(),
            PromiseState::Held {
                expires_at: EXPIRES_AT
            }
        );
        assert_eq!(promise.version().get(), 1);
        assert_eq!(promise.created_sequence(), CREATED_SEQUENCE);
        assert_eq!(promise.updated_sequence(), CREATED_SEQUENCE);
        assert_eq!(promise.bundle().claims().len(), 1);
    }

    #[test]
    fn rejects_a_deadline_equal_to_now() {
        let result = Promise::new(bundle(), NOW, NOW, CREATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::InvalidExpiration));
    }

    #[test]
    fn rejects_a_deadline_before_now() {
        let result = Promise::new(bundle(), NOW - 1, NOW, CREATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::InvalidExpiration));
    }

    #[test]
    fn commits_a_live_hold() {
        let mut promise = held_promise();

        promise
            .commit(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the live hold should commit");

        assert_eq!(promise.state(), PromiseState::Committed);
        assert_eq!(promise.version().get(), 2);
        assert_eq!(promise.created_sequence(), CREATED_SEQUENCE);
        assert_eq!(promise.updated_sequence(), UPDATED_SEQUENCE);
    }

    #[test]
    fn rejects_commit_with_a_stale_version_without_mutation() {
        let mut promise = held_promise();
        let original_state = promise.state();
        let original_sequence = promise.updated_sequence();

        let result = promise.commit(Version(2), NOW, UPDATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::VersionConflict));
        assert_eq!(promise.state(), original_state);
        assert_eq!(promise.version().get(), 1);
        assert_eq!(promise.updated_sequence(), original_sequence);
    }

    #[test]
    fn rejects_commit_at_the_deadline_without_mutation() {
        let mut promise = held_promise();

        let result = promise.commit(Version(1), EXPIRES_AT, UPDATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::HoldExpired));
        assert_eq!(
            promise.state(),
            PromiseState::Held {
                expires_at: EXPIRES_AT
            }
        );
        assert_eq!(promise.version().get(), 1);
        assert_eq!(promise.updated_sequence(), CREATED_SEQUENCE);
    }

    #[test]
    fn rejects_commit_from_a_non_held_state() {
        let mut promise = held_promise();
        promise
            .release(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should release");

        let result = promise.commit(Version(2), NOW, SequenceNumber(12));

        assert_eq!(result, Err(DomainError::InvalidPromiseState));
    }

    #[test]
    fn version_overflow_does_not_partially_commit() {
        let mut promise = held_promise();
        promise.version = Version(u64::MAX);

        let result = promise.commit(Version(u64::MAX), NOW, UPDATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::VersionOverflow));
        assert_eq!(
            promise.state(),
            PromiseState::Held {
                expires_at: EXPIRES_AT
            }
        );
        assert_eq!(promise.version().get(), u64::MAX);
        assert_eq!(promise.updated_sequence(), CREATED_SEQUENCE);
    }

    #[test]
    fn releases_a_live_hold() {
        let mut promise = held_promise();

        promise
            .release(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should release");

        assert_eq!(promise.state(), PromiseState::Released);
        assert_eq!(promise.version().get(), 2);
        assert_eq!(promise.updated_sequence(), UPDATED_SEQUENCE);
    }

    #[test]
    fn releases_a_committed_promise() {
        let mut promise = held_promise();
        promise
            .commit(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should commit");

        promise
            .release(Version(2), NOW, SequenceNumber(12))
            .expect("the committed promise should release");

        assert_eq!(promise.state(), PromiseState::Released);
        assert_eq!(promise.version().get(), 3);
        assert_eq!(promise.updated_sequence(), SequenceNumber(12));
    }

    #[test]
    fn rejects_release_of_an_expired_hold_without_mutation() {
        let mut promise = held_promise();

        let result = promise.release(Version(1), EXPIRES_AT, UPDATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::HoldExpired));
        assert_eq!(
            promise.state(),
            PromiseState::Held {
                expires_at: EXPIRES_AT
            }
        );
        assert_eq!(promise.version().get(), 1);
    }

    #[test]
    fn expires_a_hold_at_its_deadline() {
        let mut promise = held_promise();

        promise
            .expire(EXPIRES_AT, UPDATED_SEQUENCE)
            .expect("the hold should expire at its deadline");

        assert_eq!(promise.state(), PromiseState::Expired);
        assert_eq!(promise.version().get(), 2);
        assert_eq!(promise.updated_sequence(), UPDATED_SEQUENCE);
    }

    #[test]
    fn does_not_expire_a_hold_before_its_deadline() {
        let mut promise = held_promise();

        let result = promise.expire(EXPIRES_AT - 1, UPDATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::HoldNotExpired));
        assert_eq!(
            promise.state(),
            PromiseState::Held {
                expires_at: EXPIRES_AT
            }
        );
        assert_eq!(promise.version().get(), 1);
        assert_eq!(promise.updated_sequence(), CREATED_SEQUENCE);
    }

    #[test]
    fn rejects_expiration_from_a_non_held_state() {
        let mut promise = held_promise();
        promise
            .commit(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should commit");

        let result = promise.expire(EXPIRES_AT, SequenceNumber(12));

        assert_eq!(result, Err(DomainError::InvalidPromiseState));
        assert_eq!(promise.state(), PromiseState::Committed);
    }
}
