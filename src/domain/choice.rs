//! Ordered alternatives of atomic claim bundles.

use super::{Bundle, DomainError};

/// A non-empty ordered collection of alternative bundles.
///
/// Alternatives are considered in order, and the first bundle that can be admitted
/// is selected. Bundle claim order remains semantically irrelevant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    alternatives: Vec<Bundle>,
}

impl Choice {
    /// Creates a choice from one or more alternative bundles.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::EmptyChoice`] when `alternatives` is empty.
    pub fn new(alternatives: Vec<Bundle>) -> Result<Self, DomainError> {
        if alternatives.is_empty() {
            return Err(DomainError::EmptyChoice);
        }

        Ok(Self { alternatives })
    }

    /// Returns the alternative bundles in selection order.
    pub fn alternatives(&self) -> &[Bundle] {
        &self.alternatives
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Claim, Interval, ResourcePoolId};

    fn bundle(quantity: u64) -> Bundle {
        let claim = Claim::new(
            ResourcePoolId::generate(),
            Interval::new(10, 20).expect("the interval should be valid"),
            quantity,
        )
        .expect("the claim should be valid");
        Bundle::new(vec![claim]).expect("the bundle should be valid")
    }

    #[test]
    fn preserves_alternative_order() {
        let choice = Choice::new(vec![bundle(1), bundle(2)]).expect("the choice should be valid");

        assert_eq!(choice.alternatives()[0].claims()[0].quantity(), 1);
        assert_eq!(choice.alternatives()[1].claims()[0].quantity(), 2);
    }

    #[test]
    fn rejects_an_empty_choice() {
        assert_eq!(Choice::new(Vec::new()), Err(DomainError::EmptyChoice));
    }
}
