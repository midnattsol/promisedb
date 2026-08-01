//! Time-varying capacity for resource pools.
//!
//! A capacity segment declares the finite capacity available during one
//! half-open interval. A capacity curve owns the ordered, normalized segments
//! that describe how a resource pool's capacity changes over time.
use super::Timestamp;
use super::{DomainError, Interval, MAX_QUANTITY, Quantity};

/// A constant amount of capacity available during one interval.
///
/// Unlike claim quantities, segment capacity may be zero to represent planned
/// downtime or complete unavailability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacitySegment {
    interval: Interval,
    capacity: Quantity,
}

impl CapacitySegment {
    /// Creates a capacity segment for an already validated interval.
    pub fn new(interval: Interval, capacity: Quantity) -> Self {
        Self { interval, capacity }
    }

    /// Returns the interval during which this capacity applies.
    pub fn interval(&self) -> Interval {
        self.interval
    }

    /// Returns available capacity in the resource pool's configured subunits.
    pub fn capacity(&self) -> Quantity {
        self.capacity
    }
}

/// An ordered, normalized description of capacity over time.
///
/// Intervals not covered by a segment have zero capacity. Construction and
/// normalization will enforce ordering, reject overlaps, and merge adjacent
/// segments that have equal capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapacityCurve {
    segments: Vec<CapacitySegment>,
}

impl CapacityCurve {
    /// Creates a curve with zero capacity at every timestamp.
    pub fn empty() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Creates a curve from segments already ordered by interval start.
    ///
    /// Validation and normalization happen in one linear pass. Adjacent segments
    /// with equal capacity are merged, while gaps retain their implicit zero
    /// capacity.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::QuantityOutOfRange`] when any capacity exceeds
    /// [`MAX_QUANTITY`], [`DomainError::UnsortedCapacitySegments`] when interval
    /// starts are not in chronological order, or
    /// [`DomainError::OverlappingCapacitySegments`] when two segments overlap.
    pub fn from_sorted(segments: Vec<CapacitySegment>) -> Result<Self, DomainError> {
        Self::validate_capacities(&segments)?;
        let segments = Self::normalize_sorted(segments)?;
        Ok(Self { segments })
    }

    /// Creates a curve from segments supplied in any order.
    ///
    /// Segments are sorted deterministically before being validated and
    /// normalized.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::QuantityOutOfRange`] when any capacity exceeds
    /// [`MAX_QUANTITY`], or [`DomainError::OverlappingCapacitySegments`] when two
    /// segments overlap.
    pub fn from_unsorted(mut segments: Vec<CapacitySegment>) -> Result<Self, DomainError> {
        Self::validate_capacities(&segments)?;
        segments.sort_by_key(|segment| {
            (
                segment.interval().start(),
                segment.interval().end(),
                segment.capacity(),
            )
        });

        Self::from_sorted(segments)
    }

    fn validate_capacities(segments: &[CapacitySegment]) -> Result<(), DomainError> {
        if segments
            .iter()
            .any(|segment| segment.capacity() > MAX_QUANTITY)
        {
            return Err(DomainError::QuantityOutOfRange);
        }
        Ok(())
    }

    fn normalize_sorted(
        segments: Vec<CapacitySegment>,
    ) -> Result<Vec<CapacitySegment>, DomainError> {
        let mut normalized: Vec<CapacitySegment> = Vec::with_capacity(segments.len());
        let mut previous_start = None;

        for segment in segments {
            let interval = segment.interval();

            if previous_start.is_some_and(|start| interval.start() < start) {
                return Err(DomainError::UnsortedCapacitySegments);
            }
            previous_start = Some(interval.start());

            if let Some(previous) = normalized.last_mut() {
                let previous_interval = previous.interval();

                if previous_interval.overlaps(&interval) {
                    return Err(DomainError::OverlappingCapacitySegments);
                }

                if previous_interval.end() == interval.start()
                    && previous.capacity() == segment.capacity()
                {
                    let merged_interval = Interval::new(previous_interval.start(), interval.end())?;
                    *previous = CapacitySegment::new(merged_interval, segment.capacity());
                    continue;
                }
            }

            normalized.push(segment);
        }

        Ok(normalized)
    }

    /// Returns the normalized capacity segments in chronological order.
    pub fn segments(&self) -> &[CapacitySegment] {
        &self.segments
    }

    /// Returns the capacity available at `timestamp`.
    ///
    /// Timestamps before, after, or in a gap between declared segments have zero
    /// capacity.
    pub fn capacity_at(&self, timestamp: Timestamp) -> Quantity {
        let point = self
            .segments
            .partition_point(|segment| segment.interval().start() <= timestamp);
        if point == 0 {
            return 0;
        }
        let segment = &self.segments[point - 1];
        if segment.interval().contains(timestamp) {
            segment.capacity()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(start: i64, end: i64, capacity: Quantity) -> CapacitySegment {
        CapacitySegment::new(
            Interval::new(start, end).expect("the interval should be valid"),
            capacity,
        )
    }

    #[test]
    fn creates_an_empty_curve() {
        let curve = CapacityCurve::empty();

        assert!(curve.segments().is_empty());
    }

    #[test]
    fn accepts_the_maximum_capacity() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, MAX_QUANTITY)])
            .expect("the maximum capacity should be valid");

        assert_eq!(curve.capacity_at(50), MAX_QUANTITY);
    }

    #[test]
    fn constructors_reject_out_of_range_capacity_before_normalizing() {
        let sorted = vec![segment(0, 100, 1), segment(100, 200, MAX_QUANTITY + 1)];
        let unsorted = vec![segment(100, 200, MAX_QUANTITY + 1), segment(0, 150, 1)];

        assert_eq!(
            CapacityCurve::from_sorted(sorted),
            Err(DomainError::QuantityOutOfRange)
        );
        assert_eq!(
            CapacityCurve::from_unsorted(unsorted),
            Err(DomainError::QuantityOutOfRange)
        );
    }

    #[test]
    fn from_sorted_merges_adjacent_segments_with_equal_capacity() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(100, 200, 10)])
            .expect("the curve should be valid");

        assert_eq!(curve.segments(), &[segment(0, 200, 10)]);
    }

    #[test]
    fn from_sorted_keeps_adjacent_segments_with_different_capacity() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(100, 200, 8)])
            .expect("the curve should be valid");

        assert_eq!(
            curve.segments(),
            &[segment(0, 100, 10), segment(100, 200, 8)]
        );
    }

    #[test]
    fn from_sorted_preserves_gaps() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(200, 300, 10)])
            .expect("the curve should be valid");

        assert_eq!(
            curve.segments(),
            &[segment(0, 100, 10), segment(200, 300, 10)]
        );
    }

    #[test]
    fn from_sorted_rejects_segments_out_of_order() {
        let result = CapacityCurve::from_sorted(vec![segment(100, 200, 10), segment(0, 100, 10)]);

        assert_eq!(result, Err(DomainError::UnsortedCapacitySegments));
    }

    #[test]
    fn from_sorted_rejects_overlapping_segments() {
        let result = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(50, 150, 8)]);

        assert_eq!(result, Err(DomainError::OverlappingCapacitySegments));
    }

    #[test]
    fn from_unsorted_orders_and_normalizes_segments() {
        let curve = CapacityCurve::from_unsorted(vec![
            segment(200, 300, 8),
            segment(100, 200, 8),
            segment(0, 100, 10),
        ])
        .expect("the curve should be valid");

        assert_eq!(
            curve.segments(),
            &[segment(0, 100, 10), segment(100, 300, 8)]
        );
    }

    #[test]
    fn from_unsorted_rejects_overlapping_segments() {
        let result = CapacityCurve::from_unsorted(vec![segment(50, 150, 8), segment(0, 100, 10)]);

        assert_eq!(result, Err(DomainError::OverlappingCapacitySegments));
    }

    #[test]
    fn capacity_at_observes_half_open_segment_boundaries() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(100, 200, 8)])
            .expect("the curve should be valid");

        assert_eq!(curve.capacity_at(-1), 0);
        assert_eq!(curve.capacity_at(0), 10);
        assert_eq!(curve.capacity_at(99), 10);
        assert_eq!(curve.capacity_at(100), 8);
        assert_eq!(curve.capacity_at(199), 8);
        assert_eq!(curve.capacity_at(200), 0);
    }

    #[test]
    fn capacity_at_returns_zero_inside_a_gap() {
        let curve = CapacityCurve::from_sorted(vec![segment(0, 100, 10), segment(200, 300, 8)])
            .expect("the curve should be valid");

        assert_eq!(curve.capacity_at(150), 0);
    }

    #[test]
    fn capacity_at_returns_zero_for_an_empty_curve() {
        assert_eq!(CapacityCurve::empty().capacity_at(100), 0);
    }
}
