//! Claims positioned relative to a candidate start timestamp.

use super::{Claim, DomainError, Interval, MAX_QUANTITY, Quantity, ResourcePoolId, Timestamp};

/// A positive pool demand over a half-open interval relative to a candidate start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeClaim {
    pool_id: ResourcePoolId,
    start_offset: i64,
    end_offset: i64,
    quantity: Quantity,
}

impl RelativeClaim {
    /// Creates a validated relative claim.
    ///
    /// Offsets may be negative, allowing a workflow claim to begin before its
    /// candidate anchor.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidInterval`] when `start_offset >= end_offset`,
    /// [`DomainError::InvalidQuantity`] when `quantity` is zero, or
    /// [`DomainError::QuantityOutOfRange`] when it exceeds [`MAX_QUANTITY`].
    pub fn new(
        pool_id: ResourcePoolId,
        start_offset: i64,
        end_offset: i64,
        quantity: Quantity,
    ) -> Result<Self, DomainError> {
        if start_offset >= end_offset {
            return Err(DomainError::InvalidInterval);
        }
        if quantity == 0 {
            return Err(DomainError::InvalidQuantity);
        }
        if quantity > MAX_QUANTITY {
            return Err(DomainError::QuantityOutOfRange);
        }

        Ok(Self {
            pool_id,
            start_offset,
            end_offset,
            quantity,
        })
    }

    /// Returns the identifier of the claimed resource pool.
    pub fn pool_id(&self) -> ResourcePoolId {
        self.pool_id
    }

    /// Returns the inclusive start offset from the candidate anchor.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Returns the exclusive end offset from the candidate anchor.
    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }

    /// Returns the requested quantity in the pool's configured subunits.
    pub fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Materializes this relative claim at `start`.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::TimestampOverflow`] when either translated endpoint
    /// is not representable. Existing [`Interval`] and [`Claim`] validation is
    /// applied to the materialized values.
    pub fn materialize(&self, start: Timestamp) -> Result<Claim, DomainError> {
        let interval_start = start
            .checked_add(self.start_offset)
            .ok_or(DomainError::TimestampOverflow)?;
        let interval_end = start
            .checked_add(self.end_offset)
            .ok_or(DomainError::TimestampOverflow)?;
        let interval = Interval::new(interval_start, interval_end)?;
        Claim::new(self.pool_id, interval, self.quantity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_offsets_and_quantity() {
        let pool_id = ResourcePoolId::generate();

        assert_eq!(
            RelativeClaim::new(pool_id, 2, 2, 1),
            Err(DomainError::InvalidInterval)
        );
        assert_eq!(
            RelativeClaim::new(pool_id, 3, 2, 1),
            Err(DomainError::InvalidInterval)
        );
        assert_eq!(
            RelativeClaim::new(pool_id, -2, 3, 0),
            Err(DomainError::InvalidQuantity)
        );
        assert_eq!(
            RelativeClaim::new(pool_id, -2, 3, MAX_QUANTITY + 1),
            Err(DomainError::QuantityOutOfRange)
        );
    }

    #[test]
    fn accepts_the_maximum_quantity() {
        let claim = RelativeClaim::new(ResourcePoolId::generate(), -2, 3, MAX_QUANTITY)
            .expect("the maximum quantity should be valid");

        assert_eq!(claim.quantity(), MAX_QUANTITY);
    }

    #[test]
    fn materializes_negative_offsets() {
        let pool_id = ResourcePoolId::generate();
        let claim = RelativeClaim::new(pool_id, -5, 10, 3)
            .unwrap()
            .materialize(20)
            .unwrap();

        assert_eq!(claim.pool_id(), pool_id);
        assert_eq!(claim.interval(), Interval::new(15, 30).unwrap());
        assert_eq!(claim.quantity(), 3);
    }

    #[test]
    fn rejects_timestamp_overflow() {
        let claim = RelativeClaim::new(ResourcePoolId::generate(), -1, 1, 1).unwrap();

        assert_eq!(
            claim.materialize(Timestamp::MAX),
            Err(DomainError::TimestampOverflow)
        );
    }
}
