//! Capacity-bearing resource pools.

use uuid::Uuid;

use super::{DomainError, Quantity};

/// The opaque identifier of a [`ResourcePool`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourcePoolId(Uuid);

impl ResourcePoolId {
    /// Generates a random opaque identifier.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}

/// A named pool with finite capacity measured in one opaque unit.
///
/// PromiseDB does not interpret or convert the unit. Examples include
/// `"machines"`, `"people"`, and `"watts"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourcePool {
    id: ResourcePoolId,
    display_name: String,
    unit: String,
    capacity: Quantity,
}

impl ResourcePool {
    /// Creates a resource pool with a generated identifier.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidQuantity`] when `capacity` is zero.
    pub fn new(
        display_name: String,
        unit: String,
        capacity: Quantity,
    ) -> Result<Self, DomainError> {
        Self::with_id(ResourcePoolId::generate(), display_name, unit, capacity)
    }

    /// Creates a resource pool with an engine-provided identifier.
    ///
    /// This constructor lets deterministic transitions and replay preserve the
    /// identifier chosen when the resource pool was originally created.
    pub(crate) fn with_id(
        id: ResourcePoolId,
        display_name: String,
        unit: String,
        capacity: Quantity,
    ) -> Result<Self, DomainError> {
        if capacity == 0 {
            return Err(DomainError::InvalidQuantity);
        }

        Ok(Self {
            id,
            display_name,
            unit,
            capacity,
        })
    }

    /// Returns the pool's opaque identifier.
    pub fn id(&self) -> ResourcePoolId {
        self.id
    }

    /// Returns the human-readable display name.
    pub fn display_name(&self) -> &str {
        self.display_name.as_str()
    }

    /// Returns the opaque unit used by capacities and claims.
    pub fn unit(&self) -> &str {
        self.unit.as_str()
    }

    /// Returns the pool's capacity.
    pub fn capacity(&self) -> Quantity {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_a_resource_pool() {
        let pool = ResourcePool::new("Main machine pool".into(), "machines".into(), 10)
            .expect("the resource pool should be valid");

        assert!(!pool.id().0.is_nil());
        assert_eq!(pool.display_name(), "Main machine pool");
        assert_eq!(pool.unit(), "machines");
        assert_eq!(pool.capacity(), 10);
    }

    #[test]
    fn rejects_zero_capacity() {
        let result = ResourcePool::new("Unavailable pool".into(), "machines".into(), 0);

        assert_eq!(result, Err(DomainError::InvalidQuantity));
    }
}
