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
    pub(crate) fn next(self) -> Result<Self, DomainError> {
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

    /// Returns the next global sequence number.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::SequenceOverflow`] when this sequence is `u64::MAX`.
    pub(crate) fn next(self) -> Result<Self, DomainError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(DomainError::SequenceOverflow)
    }
}

/// The opaque identifier of a [`Promise`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PromiseId(Uuid);

impl PromiseId {
    /// Generates an opaque identity for command preparation.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

/// The lifecycle state of a promise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromiseState {
    /// A temporary reservation that must be committed before its deadline.
    Held {
        /// The first timestamp at which the hold is considered expired.
        expires_at: Timestamp,
    },
    /// A confirmed commitment. Claim intervals still determine actual usage.
    Committed,
    /// A manually released promise that no longer consumes capacity.
    Released,
    /// A hold that reached its deadline before being committed.
    Expired,
}

/// A live state requested for an atomic promise replacement.
///
/// Terminal states are intentionally excluded: replace changes an active
/// commitment rather than releasing or expiring it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementState {
    /// Replace the promise with a temporary hold using a new deadline.
    Held {
        /// The first timestamp at which the replacement hold is expired.
        expires_at: Timestamp,
    },
    /// Replace the promise with a confirmed commitment.
    Committed,
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
    /// Creates a held promise using an identity prepared before state-machine entry.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidExpiration`] when `expires_at <= now`.
    pub(crate) fn with_id(
        id: PromiseId,
        bundle: Bundle,
        expires_at: Timestamp,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<Self, DomainError> {
        if expires_at <= now {
            return Err(DomainError::InvalidExpiration);
        }

        Ok(Self {
            id,
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

    /// Commits a live hold and returns its new version.
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
    ) -> Result<Version, DomainError> {
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
                Ok(next_version)
            }
            _ => Err(DomainError::InvalidPromiseState),
        }
    }

    /// Releases a live hold or committed promise and returns its new version.
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
    ) -> Result<Version, DomainError> {
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
                Ok(next_version)
            }
            _ => Err(DomainError::InvalidPromiseState),
        }
    }

    /// Replaces the bundle and live state while preserving promise identity.
    ///
    /// A successful replacement increments the local version and updates the
    /// transition sequence. The creation sequence and promise ID remain unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale version, a terminal or expired source promise,
    /// an invalid replacement deadline, or version overflow.
    pub(crate) fn replace(
        &mut self,
        expected_version: Version,
        new_bundle: Bundle,
        new_state: ReplacementState,
        now: Timestamp,
        sequence: SequenceNumber,
    ) -> Result<Version, DomainError> {
        if expected_version != self.version {
            return Err(DomainError::VersionConflict);
        }

        match self.state {
            PromiseState::Held { expires_at } if expires_at <= now => {
                return Err(DomainError::HoldExpired);
            }
            PromiseState::Held { .. } | PromiseState::Committed => {}
            PromiseState::Released | PromiseState::Expired => {
                return Err(DomainError::InvalidPromiseState);
            }
        }

        if let ReplacementState::Held { expires_at } = new_state
            && expires_at <= now
        {
            return Err(DomainError::InvalidExpiration);
        }

        let next_version = self.version.next()?;
        let next_state = match new_state {
            ReplacementState::Held { expires_at } => PromiseState::Held { expires_at },
            ReplacementState::Committed => PromiseState::Committed,
        };

        self.bundle = new_bundle;
        self.state = next_state;
        self.version = next_version;
        self.updated_sequence = sequence;

        Ok(next_version)
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
        Promise::with_id(
            PromiseId::generate(),
            bundle(),
            EXPIRES_AT,
            NOW,
            CREATED_SEQUENCE,
        )
        .expect("the promise should be valid")
    }

    fn replacement_bundle() -> Bundle {
        let interval = Interval::new(2_000, 3_000).expect("the interval should be valid");
        let claim =
            Claim::new(ResourcePoolId::generate(), interval, 2).expect("the claim should be valid");
        Bundle::new(vec![claim]).expect("the bundle should be valid")
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
        let result = Promise::with_id(PromiseId::generate(), bundle(), NOW, NOW, CREATED_SEQUENCE);

        assert_eq!(result, Err(DomainError::InvalidExpiration));
    }

    #[test]
    fn rejects_a_deadline_before_now() {
        let result = Promise::with_id(
            PromiseId::generate(),
            bundle(),
            NOW - 1,
            NOW,
            CREATED_SEQUENCE,
        );

        assert_eq!(result, Err(DomainError::InvalidExpiration));
    }

    #[test]
    fn commits_a_live_hold() {
        let mut promise = held_promise();

        let new_version = promise
            .commit(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the live hold should commit");

        assert_eq!(new_version, Version(2));
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

        let new_version = promise
            .release(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should release");

        assert_eq!(new_version, Version(2));
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
    fn replaces_a_hold_with_a_new_hold_and_preserves_identity() {
        let mut promise = held_promise();
        let original_id = promise.id();
        let original_created_sequence = promise.created_sequence();
        let replacement = replacement_bundle();

        let version = promise
            .replace(
                Version(1),
                replacement.clone(),
                ReplacementState::Held { expires_at: 300 },
                NOW,
                UPDATED_SEQUENCE,
            )
            .expect("the live hold should be replaced");

        assert_eq!(promise.id(), original_id);
        assert_eq!(promise.created_sequence(), original_created_sequence);
        assert_eq!(promise.updated_sequence(), UPDATED_SEQUENCE);
        assert_eq!(promise.bundle(), &replacement);
        assert_eq!(promise.state(), PromiseState::Held { expires_at: 300 });
        assert_eq!(version, Version(2));
        assert_eq!(promise.version(), version);
    }

    #[test]
    fn replaces_a_hold_with_a_commitment() {
        let mut promise = held_promise();

        promise
            .replace(
                Version(1),
                replacement_bundle(),
                ReplacementState::Committed,
                NOW,
                UPDATED_SEQUENCE,
            )
            .expect("the live hold should be replaced");

        assert_eq!(promise.state(), PromiseState::Committed);
    }

    #[test]
    fn replaces_a_commitment_with_either_live_state() {
        let mut committed = held_promise();
        committed
            .commit(Version(1), NOW, UPDATED_SEQUENCE)
            .expect("the hold should commit");
        let mut held = committed.clone();

        committed
            .replace(
                Version(2),
                replacement_bundle(),
                ReplacementState::Committed,
                NOW,
                SequenceNumber(12),
            )
            .expect("the commitment should remain committed");
        held.replace(
            Version(2),
            replacement_bundle(),
            ReplacementState::Held { expires_at: 300 },
            NOW,
            SequenceNumber(12),
        )
        .expect("the commitment should become a hold");

        assert_eq!(committed.state(), PromiseState::Committed);
        assert_eq!(held.state(), PromiseState::Held { expires_at: 300 });
    }

    #[test]
    fn failed_replacement_validation_preserves_the_promise() {
        let original = held_promise();

        for (version, state, now, expected_error) in [
            (
                Version(2),
                ReplacementState::Committed,
                NOW,
                DomainError::VersionConflict,
            ),
            (
                Version(1),
                ReplacementState::Held { expires_at: NOW },
                NOW,
                DomainError::InvalidExpiration,
            ),
            (
                Version(1),
                ReplacementState::Committed,
                EXPIRES_AT,
                DomainError::HoldExpired,
            ),
        ] {
            let mut promise = original.clone();
            let result =
                promise.replace(version, replacement_bundle(), state, now, UPDATED_SEQUENCE);
            assert_eq!(result, Err(expected_error));
            assert_eq!(promise, original);
        }
    }

    #[test]
    fn rejects_replacement_from_terminal_states() {
        for state in [PromiseState::Released, PromiseState::Expired] {
            let mut promise = held_promise();
            promise.state = state;
            let original = promise.clone();

            let result = promise.replace(
                Version(1),
                replacement_bundle(),
                ReplacementState::Committed,
                NOW,
                UPDATED_SEQUENCE,
            );

            assert_eq!(result, Err(DomainError::InvalidPromiseState));
            assert_eq!(promise, original);
        }
    }

    #[test]
    fn version_overflow_does_not_partially_replace() {
        let mut promise = held_promise();
        promise.version = Version(u64::MAX);
        let original = promise.clone();

        let result = promise.replace(
            Version(u64::MAX),
            replacement_bundle(),
            ReplacementState::Committed,
            NOW,
            UPDATED_SEQUENCE,
        );

        assert_eq!(result, Err(DomainError::VersionOverflow));
        assert_eq!(promise, original);
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
