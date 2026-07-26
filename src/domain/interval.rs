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

    /// Returns whether `timestamp` belongs to this half-open interval.
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.start && timestamp < self.end
    }

    /// Returns whether this interval overlaps another half-open interval.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
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

    #[test]
    fn adjacent_intervals_do_not_overlap() {
        let left = Interval::new(0, 10).expect("the interval should be valid");
        let right = Interval::new(10, 20).expect("the interval should be valid");

        assert!(!left.overlaps(&right));
        assert!(!right.overlaps(&left));
    }

    #[test]
    fn intersecting_intervals_overlap() {
        let left = Interval::new(0, 11).expect("the interval should be valid");
        let right = Interval::new(10, 20).expect("the interval should be valid");

        assert!(left.overlaps(&right));
        assert!(right.overlaps(&left));
    }

    #[test]
    fn identical_intervals_overlap() {
        let interval = Interval::new(0, 10).expect("the interval should be valid");

        assert!(interval.overlaps(&interval));
    }

    #[test]
    fn containing_intervals_overlap() {
        let outer = Interval::new(0, 20).expect("the interval should be valid");
        let inner = Interval::new(5, 10).expect("the interval should be valid");

        assert!(outer.overlaps(&inner));
        assert!(inner.overlaps(&outer));
    }
}
