//! Capacity-bearing resource pools.

use uuid::Uuid;

use super::{CapacityCurve, CapacitySegment, DomainError, Interval, Quantity, Timestamp};

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
    capacity_curve: CapacityCurve,
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

    /// Creates a resource pool with a generated identifier and capacity curve.
    pub fn with_capacity_curve(
        display_name: String,
        unit: String,
        capacity_curve: CapacityCurve,
    ) -> Self {
        Self::with_id_and_capacity_curve(
            ResourcePoolId::generate(),
            display_name,
            unit,
            capacity_curve,
        )
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

        let interval = Interval::new(Timestamp::MIN, Timestamp::MAX)?;
        let capacity_curve =
            CapacityCurve::from_sorted(vec![CapacitySegment::new(interval, capacity)])?;

        Ok(Self::with_id_and_capacity_curve(
            id,
            display_name,
            unit,
            capacity_curve,
        ))
    }

    /// Creates a resource pool with an engine-provided ID and validated curve.
    pub(crate) fn with_id_and_capacity_curve(
        id: ResourcePoolId,
        display_name: String,
        unit: String,
        capacity_curve: CapacityCurve,
    ) -> Self {
        Self {
            id,
            display_name,
            unit,
            capacity_curve,
        }
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

    /// Returns the pool's capacity curve.
    pub fn capacity_curve(&self) -> &CapacityCurve {
        &self.capacity_curve
    }

    /// Returns the pool's capacity at `timestamp`.
    pub fn capacity_at(&self, timestamp: Timestamp) -> Quantity {
        self.capacity_curve.capacity_at(timestamp)
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
        assert_eq!(pool.capacity_at(0), 10);
    }

    #[test]
    fn stores_and_queries_a_variable_capacity_curve() {
        let curve = CapacityCurve::from_sorted(vec![
            CapacitySegment::new(
                Interval::new(0, 100).expect("the interval should be valid"),
                10,
            ),
            CapacitySegment::new(
                Interval::new(100, 200).expect("the interval should be valid"),
                8,
            ),
        ])
        .expect("the capacity curve should be valid");
        let pool = ResourcePool::with_capacity_curve(
            "Variable machine pool".into(),
            "machines".into(),
            curve,
        );

        assert_eq!(pool.capacity_at(-1), 0);
        assert_eq!(pool.capacity_at(50), 10);
        assert_eq!(pool.capacity_at(150), 8);
        assert_eq!(pool.capacity_at(200), 0);
        assert_eq!(pool.capacity_curve().segments().len(), 2);
    }

    #[test]
    fn rejects_zero_capacity() {
        let result = ResourcePool::new("Unavailable pool".into(), "machines".into(), 0);

        assert_eq!(result, Err(DomainError::InvalidQuantity));
    }
}
