//! Time-bounded claims against resource pools.

use super::{DomainError, Interval, Quantity, ResourcePoolId};

/// A positive quantity requested from a resource pool during an interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pool_id: ResourcePoolId,
    interval: Interval,
    quantity: Quantity,
}

impl Claim {
    /// Creates a validated claim.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidQuantity`] when `quantity` is zero.
    pub fn new(
        pool_id: ResourcePoolId,
        interval: Interval,
        quantity: Quantity,
    ) -> Result<Self, DomainError> {
        if quantity == 0 {
            return Err(DomainError::InvalidQuantity);
        }

        Ok(Self {
            pool_id,
            interval,
            quantity,
        })
    }

    /// Returns the identifier of the claimed resource pool.
    pub fn pool_id(&self) -> ResourcePoolId {
        self.pool_id
    }

    /// Returns the claimed interval.
    pub fn interval(&self) -> Interval {
        self.interval
    }

    /// Returns the claimed quantity.
    pub fn quantity(&self) -> Quantity {
        self.quantity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_interval() -> Interval {
        Interval::new(10, 20).expect("the interval should be valid")
    }

    #[test]
    fn creates_a_positive_claim() {
        let pool_id = ResourcePoolId::generate();
        let claim =
            Claim::new(pool_id, valid_interval(), 3).expect("the claim quantity should be valid");

        assert_eq!(claim.pool_id(), pool_id);
        assert_eq!(claim.interval(), valid_interval());
        assert_eq!(claim.quantity(), 3);
    }

    #[test]
    fn rejects_a_zero_quantity() {
        let result = Claim::new(ResourcePoolId::generate(), valid_interval(), 0);

        assert_eq!(result, Err(DomainError::InvalidQuantity));
    }
}
