//! Validated half-open time intervals.

use super::{DomainError, Timestamp};

/// A half-open UTC time interval `[start, end)`.
///
/// The start is included and the end is excluded. Consequently, `[a, b)` and
/// `[b, c)` are adjacent and do not overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    start: Timestamp,
    end: Timestamp,
}

impl Interval {
    /// Creates a validated half-open interval.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidInterval`] when `start >= end`.
    pub fn new(start: Timestamp, end: Timestamp) -> Result<Self, DomainError> {
        if start >= end {
            return Err(DomainError::InvalidInterval);
        }

        Ok(Self { start, end })
    }

    /// Returns the inclusive start timestamp.
    pub fn start(&self) -> Timestamp {
        self.start
    }

    /// Returns the exclusive end timestamp.
    pub fn end(&self) -> Timestamp {
        self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_a_valid_interval() {
        let interval = Interval::new(10, 20).expect("the interval should be valid");

        assert_eq!(interval.start(), 10);
        assert_eq!(interval.end(), 20);
    }

    #[test]
    fn rejects_an_empty_interval() {
        assert_eq!(Interval::new(10, 10), Err(DomainError::InvalidInterval));
    }

    #[test]
    fn rejects_a_reversed_interval() {
        assert_eq!(Interval::new(20, 10), Err(DomainError::InvalidInterval));
    }
}
