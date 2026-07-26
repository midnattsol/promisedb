//! Atomic groups of claims.

use super::{Claim, DomainError};

/// A non-empty set of claims that must be accepted or rejected atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bundle {
    claims: Vec<Claim>,
}

impl Bundle {
    /// Creates a bundle from one or more claims.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::EmptyBundle`] when `claims` is empty.
    pub fn new(claims: Vec<Claim>) -> Result<Self, DomainError> {
        if claims.is_empty() {
            return Err(DomainError::EmptyBundle);
        }

        Ok(Self { claims })
    }

    /// Returns the claims in their original order.
    pub fn claims(&self) -> &[Claim] {
        &self.claims
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Interval, ResourcePoolId};

    fn valid_claim(quantity: u64) -> Claim {
        let interval = Interval::new(10, 20).expect("the interval should be valid");
        Claim::new(ResourcePoolId::generate(), interval, quantity)
            .expect("the claim should be valid")
    }

    #[test]
    fn creates_a_non_empty_bundle() {
        let bundle =
            Bundle::new(vec![valid_claim(1), valid_claim(2)]).expect("the bundle should be valid");

        assert_eq!(bundle.claims().len(), 2);
        assert_eq!(bundle.claims()[0].quantity(), 1);
        assert_eq!(bundle.claims()[1].quantity(), 2);
    }

    #[test]
    fn rejects_an_empty_bundle() {
        assert_eq!(Bundle::new(Vec::new()), Err(DomainError::EmptyBundle));
    }
}
