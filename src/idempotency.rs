//! Deterministic command hashing and cached idempotency responses.

use crate::command::{CommandOperation, CommandResult};
use crate::domain::{
    Bundle, CapacityCurve, Claim, Interval, PromiseId, ReplacementState, ResourcePoolId, Unit,
    Version,
};
use crate::engine::CapacityRevisionMode;

const COMMAND_HASH_DOMAIN: &[u8] = b"promisedb-command-v1\0";

trait CanonicalHash {
    fn update_hash(&self, hasher: &mut blake3::Hasher);
}

fn update_length(length: usize, hasher: &mut blake3::Hasher) {
    hasher.update(&(length as u64).to_be_bytes());
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationTag {
    CreateResourcePool = 1,
    ReviseCapacity = 2,
    Hold = 3,
    Commit = 4,
    Release = 5,
    Replace = 6,
    ProcessExpirations = 7,
}

impl OperationTag {
    fn for_operation(operation: &CommandOperation) -> Self {
        match operation {
            CommandOperation::CreateResourcePool { .. } => Self::CreateResourcePool,
            CommandOperation::ReviseCapacity { .. } => Self::ReviseCapacity,
            CommandOperation::Hold { .. } => Self::Hold,
            CommandOperation::Commit { .. } => Self::Commit,
            CommandOperation::Release { .. } => Self::Release,
            CommandOperation::Replace { .. } => Self::Replace,
            CommandOperation::ProcessExpirations => Self::ProcessExpirations,
        }
    }
}

impl CanonicalHash for OperationTag {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&[*self as u8]);
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RevisionModeTag {
    Strict = 1,
    Force = 2,
}

impl CanonicalHash for RevisionModeTag {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&[*self as u8]);
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplacementStateTag {
    Held = 1,
    Committed = 2,
}

impl CanonicalHash for ReplacementStateTag {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&[*self as u8]);
    }
}

impl CanonicalHash for str {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        update_length(self.len(), hasher);
        hasher.update(self.as_bytes());
    }
}

impl CanonicalHash for u64 {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.to_be_bytes());
    }
}

impl CanonicalHash for i64 {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.to_be_bytes());
    }
}

impl CanonicalHash for ResourcePoolId {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.as_bytes());
    }
}

impl CanonicalHash for PromiseId {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.as_bytes());
    }
}

impl CanonicalHash for Version {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        self.get().update_hash(hasher);
    }
}

impl CanonicalHash for Interval {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        self.start().update_hash(hasher);
        self.end().update_hash(hasher);
    }
}

impl CanonicalHash for Unit {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        self.name().update_hash(hasher);
        self.subunits_per_unit().update_hash(hasher);
    }
}

impl CanonicalHash for CapacityCurve {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        update_length(self.segments().len(), hasher);
        for segment in self.segments() {
            segment.interval().update_hash(hasher);
            segment.capacity().update_hash(hasher);
        }
    }
}

impl CanonicalHash for Claim {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        self.pool_id().update_hash(hasher);
        self.interval().update_hash(hasher);
        self.quantity().update_hash(hasher);
    }
}

impl CanonicalHash for Bundle {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        let mut claims: Vec<&Claim> = self.claims().iter().collect();
        claims.sort_unstable_by_key(|claim| {
            let interval = claim.interval();
            (
                claim.pool_id().as_bytes(),
                interval.start(),
                interval.end(),
                claim.quantity(),
            )
        });
        update_length(claims.len(), hasher);
        for claim in claims {
            claim.update_hash(hasher);
        }
    }
}

impl CanonicalHash for CapacityRevisionMode {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        let tag = match self {
            Self::Strict => RevisionModeTag::Strict,
            Self::Force => RevisionModeTag::Force,
        };
        tag.update_hash(hasher);
    }
}

impl CanonicalHash for ReplacementState {
    fn update_hash(&self, hasher: &mut blake3::Hasher) {
        match self {
            Self::Held { expires_at } => {
                ReplacementStateTag::Held.update_hash(hasher);
                expires_at.update_hash(hasher);
            }
            Self::Committed => ReplacementStateTag::Committed.update_hash(hasher),
        }
    }
}

/// A deterministic BLAKE3 digest of one normalized command operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandHash([u8; 32]);

impl CommandHash {
    /// Returns the 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A response cached for an idempotent command retry.
pub type CommandResponse = Result<CommandResult, crate::domain::DomainError>;

/// The data retained to answer an exact command retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    command_hash: CommandHash,
    response: CommandResponse,
}

impl IdempotencyRecord {
    /// Creates a record from a normalized command hash and its original response.
    pub fn new(command_hash: CommandHash, response: CommandResponse) -> Self {
        Self {
            command_hash,
            response,
        }
    }

    /// Returns the normalized command hash.
    pub fn command_hash(&self) -> CommandHash {
        self.command_hash
    }

    /// Returns the original response cached for exact retries.
    pub fn response(&self) -> &CommandResponse {
        &self.response
    }
}

/// Hashes a command operation after writing its canonical representation.
///
/// The client ID and idempotency key are deliberately excluded: they form the map
/// key, while this digest detects reuse of that key with a different operation.
pub fn hash_operation(operation: &CommandOperation) -> CommandHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(COMMAND_HASH_DOMAIN);
    write_canonical_operation(operation, &mut hasher);
    CommandHash(*hasher.finalize().as_bytes())
}

/// Writes the stable binary representation of one operation into `hasher`.
///
/// This is the learner-owned part of command idempotency. The representation must:
///
/// - assign an explicit byte tag to every operation and enum variant;
/// - encode integers with a fixed byte order;
/// - encode strings and collections with explicit lengths;
/// - encode UUID-backed IDs as their 16 stable bytes;
/// - sort bundle claims by pool ID, interval start, interval end, and quantity;
/// - never depend on Rust memory layout or `Hash` implementations.
fn write_canonical_operation(operation: &CommandOperation, hasher: &mut blake3::Hasher) {
    OperationTag::for_operation(operation).update_hash(hasher);

    match operation {
        CommandOperation::CreateResourcePool {
            resource_pool_id,
            display_name,
            unit,
            capacity_curve,
        } => {
            resource_pool_id.update_hash(hasher);
            display_name.as_str().update_hash(hasher);
            unit.update_hash(hasher);
            capacity_curve.update_hash(hasher);
        }
        CommandOperation::ReviseCapacity {
            resource_pool_id,
            capacity_curve,
            mode,
        } => {
            resource_pool_id.update_hash(hasher);
            capacity_curve.update_hash(hasher);
            mode.update_hash(hasher);
        }
        CommandOperation::Hold {
            promise_id,
            bundle,
            expires_at,
        } => {
            promise_id.update_hash(hasher);
            bundle.update_hash(hasher);
            expires_at.update_hash(hasher);
        }
        CommandOperation::Commit {
            promise_id,
            expected_version,
        }
        | CommandOperation::Release {
            promise_id,
            expected_version,
        } => {
            promise_id.update_hash(hasher);
            expected_version.update_hash(hasher);
        }
        CommandOperation::Replace {
            promise_id,
            expected_version,
            new_bundle,
            new_state,
        } => {
            promise_id.update_hash(hasher);
            expected_version.update_hash(hasher);
            new_bundle.update_hash(hasher);
            new_state.update_hash(hasher);
        }
        CommandOperation::ProcessExpirations => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOperation;
    use crate::domain::{Bundle, Claim, Interval, ResourcePoolId, Unit};

    fn claim(pool_id: ResourcePoolId, start: i64, end: i64, quantity: u64) -> Claim {
        Claim::new(pool_id, Interval::new(start, end).unwrap(), quantity).unwrap()
    }

    #[test]
    fn bundle_claim_order_does_not_change_the_hash() {
        let first_pool = ResourcePoolId::generate();
        let second_pool = ResourcePoolId::generate();
        let first_claim = claim(first_pool, 0, 10, 2);
        let second_claim = claim(second_pool, 5, 20, 1);
        let promise_id = crate::domain::PromiseId::generate();
        let first = CommandOperation::Hold {
            promise_id,
            bundle: Bundle::new(vec![first_claim.clone(), second_claim.clone()]).unwrap(),
            expires_at: 100,
        };
        let reordered = CommandOperation::Hold {
            promise_id,
            bundle: Bundle::new(vec![second_claim, first_claim]).unwrap(),
            expires_at: 100,
        };

        assert_eq!(hash_operation(&first), hash_operation(&reordered));
    }

    #[test]
    fn changing_a_payload_field_changes_the_hash() {
        let pool_id = ResourcePoolId::generate();
        let promise_id = crate::domain::PromiseId::generate();
        let first = CommandOperation::Hold {
            promise_id,
            bundle: Bundle::new(vec![claim(pool_id, 0, 10, 1)]).unwrap(),
            expires_at: 100,
        };
        let changed = CommandOperation::Hold {
            promise_id,
            bundle: Bundle::new(vec![claim(pool_id, 0, 10, 2)]).unwrap(),
            expires_at: 100,
        };

        assert_ne!(hash_operation(&first), hash_operation(&changed));
    }

    #[test]
    fn unit_scale_changes_the_create_resource_hash() {
        let pool_id = ResourcePoolId::generate();
        let operation = |subunits_per_unit| CommandOperation::CreateResourcePool {
            resource_pool_id: pool_id,
            display_name: "Power".into(),
            unit: Unit::new("watts".into(), subunits_per_unit).unwrap(),
            capacity_curve: CapacityCurve::empty(),
        };

        assert_ne!(
            hash_operation(&operation(1)),
            hash_operation(&operation(1_000))
        );
    }

    #[test]
    fn process_expirations_hash_is_stable() {
        assert_eq!(
            hash_operation(&CommandOperation::ProcessExpirations).as_bytes(),
            &[
                88, 184, 95, 131, 65, 71, 100, 252, 161, 90, 142, 141, 78, 131, 136, 247, 6, 163,
                158, 52, 193, 103, 42, 6, 131, 30, 117, 226, 191, 197, 143, 113,
            ]
        );
    }

    #[test]
    fn operation_variants_have_distinct_hashes() {
        let pool_id = ResourcePoolId::generate();
        let hold = CommandOperation::Hold {
            promise_id: crate::domain::PromiseId::generate(),
            bundle: Bundle::new(vec![claim(pool_id, 0, 10, 1)]).unwrap(),
            expires_at: 100,
        };

        assert_ne!(
            hash_operation(&hold),
            hash_operation(&CommandOperation::ProcessExpirations)
        );
    }
}
