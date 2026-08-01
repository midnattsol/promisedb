//! Atomic bundles positioned relative to a candidate start timestamp.

use super::{Bundle, DomainError, RelativeClaim, Timestamp};

/// A non-empty group of relative claims materialized and admitted atomically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelativeBundle {
    claims: Vec<RelativeClaim>,
}

impl RelativeBundle {
    /// Creates a relative bundle from one or more claims.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::EmptyRelativeBundle`] when `claims` is empty.
    pub fn new(claims: Vec<RelativeClaim>) -> Result<Self, DomainError> {
        if claims.is_empty() {
            return Err(DomainError::EmptyRelativeBundle);
        }

        Ok(Self { claims })
    }

    /// Returns the relative claims in their original order.
    pub fn claims(&self) -> &[RelativeClaim] {
        &self.claims
    }

    /// Materializes every claim at the same candidate start.
    ///
    /// # Errors
    ///
    /// Returns an error when timestamp translation or ordinary claim and bundle
    /// validation fails.
    pub fn materialize(&self, start: Timestamp) -> Result<Bundle, DomainError> {
        let claims = self
            .claims
            .iter()
            .map(|claim| claim.materialize(start))
            .collect::<Result<Vec<_>, _>>()?;
        Bundle::new(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Interval, ResourcePoolId};

    #[test]
    fn rejects_an_empty_relative_bundle() {
        assert_eq!(
            RelativeBundle::new(Vec::new()),
            Err(DomainError::EmptyRelativeBundle)
        );
    }

    #[test]
    fn materializes_all_claims_at_one_anchor() {
        let first_pool = ResourcePoolId::generate();
        let second_pool = ResourcePoolId::generate();
        let relative = RelativeBundle::new(vec![
            RelativeClaim::new(first_pool, 0, 10, 2).unwrap(),
            RelativeClaim::new(second_pool, -5, 5, 1).unwrap(),
        ])
        .unwrap();
        let bundle = relative.materialize(20).unwrap();

        assert_eq!(
            bundle.claims()[0].interval(),
            Interval::new(20, 30).unwrap()
        );
        assert_eq!(
            bundle.claims()[1].interval(),
            Interval::new(15, 25).unwrap()
        );
    }
}
