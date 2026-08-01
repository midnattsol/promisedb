//! Immutable fixed-point units for resource-pool quantities.

use super::DomainError;

/// The unit and fixed-point scale used by one resource pool.
///
/// `subunits_per_unit` defines how many integer [`super::Quantity`] values equal
/// one displayed unit. For example, `1_000` permits milliwatt precision when the
/// unit name is `"watts"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unit {
    name: String,
    subunits_per_unit: u64,
}

impl Unit {
    /// Creates an immutable unit and its fixed-point scale.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidUnitName`] for an empty or whitespace-only
    /// name, or [`DomainError::InvalidUnitScale`] when `subunits_per_unit` is zero.
    pub fn new(name: String, subunits_per_unit: u64) -> Result<Self, DomainError> {
        if name.trim().is_empty() {
            return Err(DomainError::InvalidUnitName);
        }
        if subunits_per_unit == 0 {
            return Err(DomainError::InvalidUnitScale);
        }

        Ok(Self {
            name,
            subunits_per_unit,
        })
    }

    /// Returns the human-readable major-unit name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of integer subunits in one displayed unit.
    pub fn subunits_per_unit(&self) -> u64 {
        self.subunits_per_unit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_a_scaled_unit() {
        let unit = Unit::new("watts".into(), 1_000).expect("the unit should be valid");

        assert_eq!(unit.name(), "watts");
        assert_eq!(unit.subunits_per_unit(), 1_000);
    }

    #[test]
    fn rejects_an_empty_or_whitespace_name() {
        assert_eq!(
            Unit::new(String::new(), 1),
            Err(DomainError::InvalidUnitName)
        );
        assert_eq!(
            Unit::new("   ".into(), 1),
            Err(DomainError::InvalidUnitName)
        );
    }

    #[test]
    fn rejects_a_zero_scale() {
        assert_eq!(
            Unit::new("watts".into(), 0),
            Err(DomainError::InvalidUnitScale)
        );
    }
}
