//! Deterministic command hashing and cached idempotency responses.

use crate::command::{CommandOperation, CommandResult};

const COMMAND_HASH_DOMAIN: &[u8] = b"promisedb-command-v1\0";

/// A deterministic BLAKE3 digest of one normalized command operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandHash([u8; 32]);

impl CommandHash {
    /// Returns the 32-byte digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// The data retained to answer an exact command retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyRecord {
    command_hash: CommandHash,
    result: CommandResult,
}

impl IdempotencyRecord {
    /// Creates a record from a normalized command hash and its original result.
    pub fn new(command_hash: CommandHash, result: CommandResult) -> Self {
        Self {
            command_hash,
            result,
        }
    }

    /// Returns the normalized command hash.
    pub fn command_hash(&self) -> CommandHash {
        self.command_hash
    }

    /// Returns the original result cached for exact retries.
    pub fn result(&self) -> &CommandResult {
        &self.result
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
    let _ = (operation, hasher);
    todo!("write the canonical CommandOperation representation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOperation;
    use crate::domain::{Bundle, Claim, Interval, ResourcePoolId};

    fn claim(pool_id: ResourcePoolId, start: i64, end: i64, quantity: u64) -> Claim {
        Claim::new(pool_id, Interval::new(start, end).unwrap(), quantity).unwrap()
    }

    #[test]
    #[ignore = "implement write_canonical_operation"]
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
    #[ignore = "implement write_canonical_operation"]
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
    #[ignore = "implement write_canonical_operation"]
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
