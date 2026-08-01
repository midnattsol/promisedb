//! Blocked temporal index of available capacity.
//!
//! A slack timeline records the timestamps at which available capacity changes.
//! Points are stored in small contiguous blocks so local insertions do not move
//! the entire timeline. Block aggregates will later support fast range checks
//! and lazy range adjustments.

use super::IndexError;
use crate::domain::{
    CapacityCurve, Claim, Interval, Promise, PromiseState, Quantity, ResourcePoolId, Timestamp,
};
use std::collections::BTreeMap;
use std::ops::Range;

/// Signed available capacity.
///
/// Positive values represent spare capacity, zero represents full utilization,
/// and negative values represent a deficit created by forced capacity changes.
pub type Slack = i64;

/// A half-open interval where effective slack is negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlackDeficit {
    interval: Interval,
    amount: Quantity,
}

impl SlackDeficit {
    /// Creates a normalized deficit interval.
    pub fn new(interval: Interval, amount: Quantity) -> Self {
        Self { interval, amount }
    }

    /// Returns the interval over which the deficit applies.
    pub fn interval(self) -> Interval {
        self.interval
    }

    /// Returns the positive magnitude of the deficit.
    pub fn amount(self) -> Quantity {
        self.amount
    }
}

const MAX_POINTS_PER_BLOCK: usize = 256;

#[derive(Debug, Default, Clone, Copy)]
struct TimelineEvent {
    capacity: Option<i128>,
    usage_started: u128,
    usage_ended: u128,
}

/// A timestamp at which the slack changes.
///
/// The stored value applies from `timestamp` until the next point. While a block
/// has a nonzero delta, `slack` is the point's unmaterialized base value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlackPoint {
    timestamp: Timestamp,
    slack: Slack,
}

impl SlackPoint {
    /// Creates a point at which `slack` becomes the new base value.
    pub fn new(timestamp: Timestamp, slack: Slack) -> Self {
        Self { timestamp, slack }
    }

    /// Returns the timestamp at which this value starts applying.
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the stored base slack before any block adjustment.
    pub fn slack(&self) -> Slack {
        self.slack
    }
}

/// A contiguous group of chronologically ordered slack points.
///
/// `minimum_slack` represents the effective minimum after `slack_delta`. The
/// delta applies to every point in the block and may be materialized into
/// individual points when a partial update needs to inspect the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackBlock {
    points: Vec<SlackPoint>,
    minimum_slack: Option<Slack>,
    maximum_slack: Option<Slack>,
    slack_delta: Slack,
}

impl SlackBlock {
    /// Creates an empty block with no slack aggregates or block delta.
    pub fn empty() -> Self {
        Self {
            points: Vec::new(),
            minimum_slack: None,
            maximum_slack: None,
            slack_delta: 0,
        }
    }

    /// Creates a block from points already ordered by timestamp.
    ///
    /// Consecutive points with equal slack are normalized into one point. An
    /// empty input creates an empty block with no minimum slack.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::UnsortedSlackPoints`] when timestamps decrease, or
    /// [`IndexError::DuplicateSlackTimestamp`] when timestamps are repeated.
    pub fn from_sorted_points(points: Vec<SlackPoint>) -> Result<Self, IndexError> {
        if points.is_empty() {
            return Ok(Self::empty());
        }

        let mut normalized: Vec<SlackPoint> = Vec::with_capacity(points.len());
        let mut minimum_slack: Option<Slack> = None;
        let mut maximum_slack: Option<Slack> = None;

        for point in points {
            if let Some(previous) = normalized.last() {
                if point.timestamp() < previous.timestamp() {
                    return Err(IndexError::UnsortedSlackPoints);
                }
                if point.timestamp() == previous.timestamp() {
                    return Err(IndexError::DuplicateSlackTimestamp);
                }
                if point.slack() == previous.slack() {
                    continue;
                }
            }

            minimum_slack =
                Some(minimum_slack.map_or(point.slack(), |minimum| minimum.min(point.slack())));
            maximum_slack =
                Some(maximum_slack.map_or(point.slack(), |maximum| maximum.max(point.slack())));
            normalized.push(point);
        }

        Ok(Self {
            points: normalized,
            minimum_slack,
            maximum_slack,
            slack_delta: 0,
        })
    }

    /// Creates a block from points supplied in any order.
    ///
    /// Points are sorted in place by timestamp before being validated and
    /// normalized by [`SlackBlock::from_sorted_points`].
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DuplicateSlackTimestamp`] when timestamps are
    /// repeated.
    pub fn from_unsorted_points(mut points: Vec<SlackPoint>) -> Result<Self, IndexError> {
        points.sort_unstable_by_key(|point| point.timestamp());
        Self::from_sorted_points(points)
    }

    /// Returns the block's points in chronological order.
    pub fn points(&self) -> &[SlackPoint] {
        &self.points
    }

    /// Returns the effective minimum slack, or `None` for an empty block.
    pub fn minimum_slack(&self) -> Option<Slack> {
        self.minimum_slack
    }

    /// Returns the effective maximum slack, or `None` for an empty block.
    pub fn maximum_slack(&self) -> Option<Slack> {
        self.maximum_slack
    }

    /// Returns the block-wide slack change not materialized into the points.
    pub fn slack_delta(&self) -> Slack {
        self.slack_delta
    }

    /// Lazily applies the same slack delta to every point in the block.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] when the effective minimum or maximum
    /// slack cannot represent the adjustment.
    pub fn apply_delta(&mut self, delta: Slack) -> Result<(), IndexError> {
        let (Some(current_minimum), Some(current_maximum)) =
            (self.minimum_slack, self.maximum_slack)
        else {
            return Ok(());
        };
        let new_minimum = current_minimum
            .checked_add(delta)
            .ok_or(IndexError::SlackOverflow)?;
        let new_maximum = current_maximum
            .checked_add(delta)
            .ok_or(IndexError::SlackOverflow)?;
        let new_delta = match self.slack_delta.checked_add(delta) {
            Some(new_delta) => new_delta,
            None => {
                self.materialize_delta()?;
                delta
            }
        };

        self.slack_delta = new_delta;
        self.minimum_slack = Some(new_minimum);
        self.maximum_slack = Some(new_maximum);
        Ok(())
    }

    /// Applies the block delta to every stored point and resets it to zero.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] without mutation when any adjusted
    /// point is not representable.
    pub fn materialize_delta(&mut self) -> Result<(), IndexError> {
        if self.slack_delta == 0 {
            return Ok(());
        }

        let mut materialized_points = Vec::with_capacity(self.points.len());

        for point in &self.points {
            let new_slack = point
                .slack
                .checked_add(self.slack_delta)
                .ok_or(IndexError::SlackOverflow)?;
            materialized_points.push(SlackPoint::new(point.timestamp(), new_slack));
        }
        self.points = materialized_points;
        self.slack_delta = 0;
        Ok(())
    }

    /// Materializes the block and applies a delta to a half-open point-index range.
    ///
    /// Adjacent points with equal resulting slack are normalized. The operation
    /// publishes no changes if validation or arithmetic fails.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::InvalidPointRange`] for reversed or out-of-bounds
    /// indices, or [`IndexError::SlackOverflow`] for unrepresentable arithmetic.
    pub fn apply_delta_to_point_range(
        &mut self,
        range: Range<usize>,
        delta: Slack,
    ) -> Result<(), IndexError> {
        if range.start > range.end || range.end > self.points.len() {
            return Err(IndexError::InvalidPointRange);
        }
        if range.is_empty() {
            return Ok(());
        }

        let mut adjusted_points: Vec<SlackPoint> = Vec::with_capacity(self.points.len());
        let mut minimum_slack: Option<Slack> = None;
        let mut maximum_slack: Option<Slack> = None;

        for (index, point) in self.points.iter().enumerate() {
            let effective_slack = point
                .slack()
                .checked_add(self.slack_delta)
                .ok_or(IndexError::SlackOverflow)?;
            let adjusted_slack = if range.contains(&index) {
                effective_slack
                    .checked_add(delta)
                    .ok_or(IndexError::SlackOverflow)?
            } else {
                effective_slack
            };

            if adjusted_points
                .last()
                .is_some_and(|previous| previous.slack() == adjusted_slack)
            {
                continue;
            }

            minimum_slack =
                Some(minimum_slack.map_or(adjusted_slack, |minimum| minimum.min(adjusted_slack)));
            maximum_slack =
                Some(maximum_slack.map_or(adjusted_slack, |maximum| maximum.max(adjusted_slack)));
            adjusted_points.push(SlackPoint::new(point.timestamp(), adjusted_slack));
        }

        self.points = adjusted_points;
        self.minimum_slack = minimum_slack;
        self.maximum_slack = maximum_slack;
        self.slack_delta = 0;

        Ok(())
    }

    fn from_sorted_points_preserving_boundaries(
        points: Vec<SlackPoint>,
    ) -> Result<Self, IndexError> {
        let mut previous_timestamp = None;
        let mut minimum_slack: Option<Slack> = None;
        let mut maximum_slack: Option<Slack> = None;

        for point in &points {
            if previous_timestamp.is_some_and(|timestamp| point.timestamp() < timestamp) {
                return Err(IndexError::UnsortedSlackPoints);
            }
            if previous_timestamp == Some(point.timestamp()) {
                return Err(IndexError::DuplicateSlackTimestamp);
            }
            previous_timestamp = Some(point.timestamp());
            minimum_slack =
                Some(minimum_slack.map_or(point.slack(), |minimum| minimum.min(point.slack())));
            maximum_slack =
                Some(maximum_slack.map_or(point.slack(), |maximum| maximum.max(point.slack())));
        }

        Ok(Self {
            points,
            minimum_slack,
            maximum_slack,
            slack_delta: 0,
        })
    }

    fn recompute_aggregates(&mut self) -> Result<(), IndexError> {
        let mut minimum_slack: Option<Slack> = None;
        let mut maximum_slack: Option<Slack> = None;

        for point in &self.points {
            let slack = point
                .slack()
                .checked_add(self.slack_delta)
                .ok_or(IndexError::SlackOverflow)?;
            minimum_slack = Some(minimum_slack.map_or(slack, |minimum| minimum.min(slack)));
            maximum_slack = Some(maximum_slack.map_or(slack, |maximum| maximum.max(slack)));
        }

        self.minimum_slack = minimum_slack;
        self.maximum_slack = maximum_slack;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlackBound {
    Minimum,
    Maximum,
}

/// A blocked, normalized slack index for one resource pool.
///
/// A timeline is derived from the pool's capacity curve and the claims of held
/// and committed promises. An empty timeline has zero slack at every timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlackTimeline {
    blocks: Vec<SlackBlock>,
}

impl SlackTimeline {
    /// Creates an empty timeline with zero slack at every timestamp.
    pub fn empty() -> Self {
        Self { blocks: Vec::new() }
    }

    /// Builds the initial slack index from physical capacity.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] if a derived value is not
    /// representable as [`Slack`].
    pub fn from_capacity_curve(capacity_curve: &CapacityCurve) -> Result<Self, IndexError> {
        Self::from_capacity_and_claims(capacity_curve, &[])
    }

    /// Rebuilds slack from physical capacity and active claims.
    ///
    /// The caller must provide only claims belonging to held or committed
    /// promises for one resource pool. Events at the same timestamp are
    /// aggregated before slack is calculated, making rebuild deterministic.
    ///
    /// # Errors
    ///
    /// Returns an error when arithmetic overflows or claim events would make
    /// reconstructed usage negative.
    pub fn from_capacity_and_claims(
        capacity_curve: &CapacityCurve,
        active_claims: &[&Claim],
    ) -> Result<Self, IndexError> {
        let mut events: BTreeMap<Timestamp, TimelineEvent> = BTreeMap::new();

        for segment in capacity_curve.segments() {
            events
                .entry(segment.interval().start())
                .or_default()
                .capacity = Some(i128::from(segment.capacity()));
            events.entry(segment.interval().end()).or_default().capacity = Some(0);
        }

        for claim in active_claims {
            let quantity = u128::from(claim.quantity());
            let start_event = events.entry(claim.interval().start()).or_default();
            start_event.usage_started = start_event
                .usage_started
                .checked_add(quantity)
                .ok_or(IndexError::SlackOverflow)?;
            let end_event = events.entry(claim.interval().end()).or_default();
            end_event.usage_ended = end_event
                .usage_ended
                .checked_add(quantity)
                .ok_or(IndexError::SlackOverflow)?;
        }

        let mut capacity: i128 = 0;
        let mut usage: u128 = 0;
        let mut points = Vec::with_capacity(events.len());

        for (timestamp, event) in events {
            if let Some(new_capacity) = event.capacity {
                capacity = new_capacity;
            }
            usage = usage
                .checked_sub(event.usage_ended)
                .ok_or(IndexError::InconsistentUsage)?;
            usage = usage
                .checked_add(event.usage_started)
                .ok_or(IndexError::SlackOverflow)?;
            let signed_usage = i128::try_from(usage).map_err(|_| IndexError::SlackOverflow)?;
            let wide_slack = capacity
                .checked_sub(signed_usage)
                .ok_or(IndexError::SlackOverflow)?;
            let slack = Slack::try_from(wide_slack).map_err(|_| IndexError::SlackOverflow)?;

            if points
                .last()
                .is_none_or(|previous: &SlackPoint| previous.slack() != slack)
            {
                points.push(SlackPoint::new(timestamp, slack));
            }
        }

        Ok(Self {
            blocks: Self::blocks_from_sorted_points(points)?,
        })
    }

    /// Rebuilds one pool's slack from capacity and current promises.
    ///
    /// Only claims from held and committed promises that reference `pool_id` are
    /// included. The caller must process due expirations before rebuilding.
    ///
    /// # Errors
    ///
    /// Returns an error when arithmetic overflows or active claim events are
    /// inconsistent.
    pub fn from_capacity_and_promises(
        capacity_curve: &CapacityCurve,
        pool_id: ResourcePoolId,
        promises: &[&Promise],
    ) -> Result<Self, IndexError> {
        let active_claims: Vec<&Claim> = promises
            .iter()
            .filter(|promise| {
                matches!(
                    promise.state(),
                    PromiseState::Held { .. } | PromiseState::Committed
                )
            })
            .flat_map(|promise| promise.bundle().claims())
            .filter(|claim| claim.pool_id() == pool_id)
            .collect();

        Self::from_capacity_and_claims(capacity_curve, &active_claims)
    }

    /// Returns the timeline's blocks in chronological order.
    pub fn blocks(&self) -> &[SlackBlock] {
        &self.blocks
    }

    /// Returns the effective slack at `timestamp`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] if the stored block delta cannot be
    /// combined with its point value.
    pub fn slack_at(&self, timestamp: Timestamp) -> Result<Slack, IndexError> {
        let block_index = self
            .blocks
            .partition_point(|block| block.points[0].timestamp() <= timestamp);
        if block_index == 0 {
            return Ok(0);
        }

        let block = &self.blocks[block_index - 1];
        let point_index = block
            .points
            .partition_point(|point| point.timestamp() <= timestamp);
        let point = &block.points[point_index - 1];
        point
            .slack()
            .checked_add(block.slack_delta)
            .ok_or(IndexError::SlackOverflow)
    }

    /// Returns the minimum effective slack throughout `interval`.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] if a stored block is inconsistent.
    pub fn minimum_slack(&self, interval: Interval) -> Result<Slack, IndexError> {
        self.slack_bound(interval, SlackBound::Minimum)
    }

    /// Returns normalized intervals whose effective slack is negative.
    ///
    /// Each effective point defines the value from its timestamp until the next
    /// point. Adjacent intervals with the same deficit magnitude are merged.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] when an effective slack value is
    /// inconsistent with the stored representation.
    pub fn deficit_intervals(&self) -> Result<Vec<SlackDeficit>, IndexError> {
        let points = self.effective_points()?;
        let mut deficit_per_interval: Vec<SlackDeficit> = Vec::new();
        for window in points.windows(2) {
            let current = window[0];
            let next = window[1];

            if current.slack() < 0 {
                let interval = Interval::new(current.timestamp(), next.timestamp())
                    .map_err(|_| IndexError::InvalidPointRange)?;
                let amount = current.slack().unsigned_abs();
                deficit_per_interval.push(SlackDeficit::new(interval, amount));
            }
        }
        Ok(deficit_per_interval)
    }

    /// Applies `delta` throughout the half-open `interval`.
    ///
    /// Complete interior blocks receive a lazy block delta. Only boundary blocks
    /// are materialized and updated point by point. The operation performs an
    /// overflow preflight before mutating the timeline.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] when any resulting slack is not
    /// representable.
    pub fn apply_delta(&mut self, interval: Interval, delta: Slack) -> Result<(), IndexError> {
        if delta == 0 {
            return Ok(());
        }

        self.minimum_slack(interval)?
            .checked_add(delta)
            .ok_or(IndexError::SlackOverflow)?;
        self.maximum_slack(interval)?
            .checked_add(delta)
            .ok_or(IndexError::SlackOverflow)?;

        self.ensure_boundary(interval.end())?;
        self.ensure_boundary(interval.start())?;

        let (start_block, start_point) = self
            .find_exact_point(interval.start())
            .ok_or(IndexError::InvalidPointRange)?;
        let (end_block, end_point) = self
            .find_exact_point(interval.end())
            .ok_or(IndexError::InvalidPointRange)?;

        if start_block == end_block {
            self.blocks[start_block].apply_delta_to_point_range(start_point..end_point, delta)?;
        } else {
            let start_len = self.blocks[start_block].points.len();
            if start_point == 0 {
                self.blocks[start_block].apply_delta(delta)?;
            } else {
                self.blocks[start_block]
                    .apply_delta_to_point_range(start_point..start_len, delta)?;
            }

            for block in &mut self.blocks[start_block + 1..end_block] {
                block.apply_delta(delta)?;
            }

            if end_point > 0 {
                self.blocks[end_block].apply_delta_to_point_range(0..end_point, delta)?;
            }
        }

        self.normalize_and_rebalance()?;
        Ok(())
    }

    fn maximum_slack(&self, interval: Interval) -> Result<Slack, IndexError> {
        self.slack_bound(interval, SlackBound::Maximum)
    }

    fn slack_bound(&self, interval: Interval, bound: SlackBound) -> Result<Slack, IndexError> {
        let mut bound_slack = self.slack_at(interval.start())?;

        for block in &self.blocks {
            let first_timestamp = block.points[0].timestamp();
            let last_timestamp = block.points[block.points.len() - 1].timestamp();

            if last_timestamp <= interval.start() || first_timestamp >= interval.end() {
                continue;
            }

            if first_timestamp >= interval.start() && last_timestamp < interval.end() {
                let block_bound = match bound {
                    SlackBound::Minimum => block.minimum_slack,
                    SlackBound::Maximum => block.maximum_slack,
                }
                .ok_or(IndexError::InvalidPointRange)?;
                bound_slack = match bound {
                    SlackBound::Minimum => bound_slack.min(block_bound),
                    SlackBound::Maximum => bound_slack.max(block_bound),
                };
                continue;
            }

            for point in &block.points {
                if point.timestamp() <= interval.start() || point.timestamp() >= interval.end() {
                    continue;
                }
                let slack = point
                    .slack()
                    .checked_add(block.slack_delta)
                    .ok_or(IndexError::SlackOverflow)?;
                bound_slack = match bound {
                    SlackBound::Minimum => bound_slack.min(slack),
                    SlackBound::Maximum => bound_slack.max(slack),
                };
            }
        }

        Ok(bound_slack)
    }

    fn ensure_boundary(&mut self, timestamp: Timestamp) -> Result<(), IndexError> {
        if self.find_exact_point(timestamp).is_some() {
            return Ok(());
        }

        let slack = self.slack_at(timestamp)?;
        if self.blocks.is_empty() {
            self.blocks
                .push(SlackBlock::from_sorted_points_preserving_boundaries(vec![
                    SlackPoint::new(timestamp, slack),
                ])?);
            return Ok(());
        }

        let insertion = self
            .blocks
            .partition_point(|block| block.points[0].timestamp() <= timestamp);
        let block_index = insertion.saturating_sub(1).min(self.blocks.len() - 1);
        let block = &mut self.blocks[block_index];
        block.materialize_delta()?;
        let point_index = block
            .points
            .binary_search_by_key(&timestamp, |point| point.timestamp())
            .unwrap_or_else(|index| index);
        block
            .points
            .insert(point_index, SlackPoint::new(timestamp, slack));
        block.recompute_aggregates()?;
        self.split_oversized_block(block_index)?;
        Ok(())
    }

    fn find_exact_point(&self, timestamp: Timestamp) -> Option<(usize, usize)> {
        let insertion = self
            .blocks
            .partition_point(|block| block.points[0].timestamp() <= timestamp);
        if insertion == 0 {
            return None;
        }
        let block_index = insertion - 1;
        self.blocks[block_index]
            .points
            .binary_search_by_key(&timestamp, |point| point.timestamp())
            .ok()
            .map(|point_index| (block_index, point_index))
    }

    fn split_oversized_block(&mut self, block_index: usize) -> Result<(), IndexError> {
        if self.blocks[block_index].points.len() <= MAX_POINTS_PER_BLOCK {
            return Ok(());
        }

        self.blocks[block_index].materialize_delta()?;
        let split_index = self.blocks[block_index].points.len() / 2;
        let right_points = self.blocks[block_index].points.split_off(split_index);
        self.blocks[block_index].recompute_aggregates()?;
        let right = SlackBlock::from_sorted_points_preserving_boundaries(right_points)?;
        self.blocks.insert(block_index + 1, right);
        Ok(())
    }

    fn normalize_and_rebalance(&mut self) -> Result<(), IndexError> {
        self.blocks.retain(|block| !block.points.is_empty());

        if let Some(first) = self.blocks.first_mut() {
            first.materialize_delta()?;
            if first.points.first().is_some_and(|point| point.slack() == 0) {
                first.points.remove(0);
                first.recompute_aggregates()?;
            }
        }
        self.blocks.retain(|block| !block.points.is_empty());

        let mut index = 0;
        while index + 1 < self.blocks.len() {
            let combined_len =
                self.blocks[index].points.len() + self.blocks[index + 1].points.len();
            if combined_len <= MAX_POINTS_PER_BLOCK {
                let mut right = self.blocks.remove(index + 1);
                self.blocks[index].materialize_delta()?;
                right.materialize_delta()?;
                self.blocks[index].points.append(&mut right.points);
                let points = std::mem::take(&mut self.blocks[index].points);
                self.blocks[index] = SlackBlock::from_sorted_points(points)?;
                continue;
            }

            let left_slack = self.blocks[index].points[self.blocks[index].points.len() - 1]
                .slack()
                .checked_add(self.blocks[index].slack_delta)
                .ok_or(IndexError::SlackOverflow)?;
            let right_slack = self.blocks[index + 1].points[0]
                .slack()
                .checked_add(self.blocks[index + 1].slack_delta)
                .ok_or(IndexError::SlackOverflow)?;
            if left_slack == right_slack {
                self.blocks[index + 1].materialize_delta()?;
                self.blocks[index + 1].points.remove(0);
                self.blocks[index + 1].recompute_aggregates()?;
                if self.blocks[index + 1].points.is_empty() {
                    self.blocks.remove(index + 1);
                }
                continue;
            }
            index += 1;
        }

        Ok(())
    }

    /// Returns all effective points in canonical chronological order.
    ///
    /// This inspection helper materializes block deltas into the returned copy;
    /// it does not mutate the timeline.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::SlackOverflow`] if a block contains an invalid delta.
    pub fn effective_points(&self) -> Result<Vec<SlackPoint>, IndexError> {
        let point_count = self.blocks.iter().map(|block| block.points.len()).sum();
        let mut points = Vec::with_capacity(point_count);

        for block in &self.blocks {
            for point in &block.points {
                let slack = point
                    .slack()
                    .checked_add(block.slack_delta)
                    .ok_or(IndexError::SlackOverflow)?;
                if points
                    .last()
                    .is_none_or(|previous: &SlackPoint| previous.slack() != slack)
                {
                    points.push(SlackPoint::new(point.timestamp(), slack));
                }
            }
        }
        if points.first().is_some_and(|point| point.slack() == 0) {
            points.remove(0);
        }
        Ok(points)
    }

    fn blocks_from_sorted_points(points: Vec<SlackPoint>) -> Result<Vec<SlackBlock>, IndexError> {
        let normalized = SlackBlock::from_sorted_points(points)?;
        let mut points = normalized.points;
        if points.first().is_some_and(|point| point.slack() == 0) {
            points.remove(0);
        }
        let mut blocks = Vec::with_capacity(points.len().div_ceil(MAX_POINTS_PER_BLOCK));

        for chunk in points.chunks(MAX_POINTS_PER_BLOCK) {
            blocks.push(SlackBlock::from_sorted_points(chunk.to_vec())?);
        }

        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Bundle, CapacitySegment, MAX_QUANTITY, PromiseId, SequenceNumber};

    fn point(timestamp: Timestamp, slack: Slack) -> SlackPoint {
        SlackPoint::new(timestamp, slack)
    }

    fn capacity_curve(segments: &[(Timestamp, Timestamp, u64)]) -> CapacityCurve {
        CapacityCurve::from_sorted(
            segments
                .iter()
                .map(|(start, end, capacity)| {
                    CapacitySegment::new(
                        Interval::new(*start, *end).expect("the interval should be valid"),
                        *capacity,
                    )
                })
                .collect(),
        )
        .expect("the capacity curve should be valid")
    }

    fn claim(pool_id: ResourcePoolId, start: Timestamp, end: Timestamp, quantity: u64) -> Claim {
        Claim::new(
            pool_id,
            Interval::new(start, end).expect("the interval should be valid"),
            quantity,
        )
        .expect("the claim should be valid")
    }

    fn assert_timeline_invariants(timeline: &SlackTimeline) {
        let points = timeline
            .effective_points()
            .expect("the timeline should be valid");
        assert!(
            points
                .windows(2)
                .all(|pair| pair[0].timestamp() < pair[1].timestamp())
        );
        assert!(
            points
                .windows(2)
                .all(|pair| pair[0].slack() != pair[1].slack())
        );
        assert!(points.first().is_none_or(|point| point.slack() != 0));

        for block in timeline.blocks() {
            assert!(!block.points().is_empty());
            assert!(block.points().len() <= MAX_POINTS_PER_BLOCK);
            let effective: Vec<Slack> = block
                .points()
                .iter()
                .map(|point| point.slack() + block.slack_delta())
                .collect();
            assert_eq!(block.minimum_slack(), effective.iter().copied().min());
            assert_eq!(block.maximum_slack(), effective.iter().copied().max());
        }
    }

    #[test]
    fn extracts_normalized_deficit_intervals() {
        let timeline = SlackTimeline {
            blocks: vec![
                SlackBlock::from_sorted_points(vec![
                    point(0, 4),
                    point(10, -2),
                    point(30, -3),
                    point(40, 1),
                ])
                .expect("the block should be valid"),
            ],
        };

        assert_eq!(
            timeline
                .deficit_intervals()
                .expect("deficits should be extracted"),
            vec![
                SlackDeficit::new(Interval::new(10, 30).unwrap(), 2),
                SlackDeficit::new(Interval::new(30, 40).unwrap(), 3),
            ]
        );
    }

    #[test]
    fn a_non_negative_timeline_has_no_deficits() {
        let timeline = SlackTimeline {
            blocks: vec![
                SlackBlock::from_sorted_points(vec![point(0, 4), point(10, 0)])
                    .expect("the block should be valid"),
            ],
        };

        assert_eq!(timeline.deficit_intervals(), Ok(Vec::new()));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn compact_types_have_the_expected_64_bit_layout() {
        assert_eq!(std::mem::size_of::<SlackPoint>(), 16);
        assert_eq!(std::mem::size_of::<SlackBlock>(), 64);
    }

    #[test]
    fn creates_an_empty_block() {
        let block = SlackBlock::empty();

        assert!(block.points().is_empty());
        assert_eq!(block.minimum_slack(), None);
        assert_eq!(block.slack_delta(), 0);
    }

    #[test]
    fn normalizes_sorted_points_and_calculates_the_minimum() {
        let block = SlackBlock::from_sorted_points(vec![
            point(0, 10),
            point(50, 10),
            point(100, 4),
            point(150, 8),
        ])
        .expect("the sorted points should be valid");

        assert_eq!(
            block.points(),
            &[point(0, 10), point(100, 4), point(150, 8)]
        );
        assert_eq!(block.minimum_slack(), Some(4));
        assert_eq!(block.slack_delta(), 0);
    }

    #[test]
    fn rejects_unsorted_points() {
        let result = SlackBlock::from_sorted_points(vec![point(100, 4), point(0, 10)]);

        assert_eq!(result, Err(IndexError::UnsortedSlackPoints));
    }

    #[test]
    fn rejects_duplicate_timestamps() {
        let result = SlackBlock::from_sorted_points(vec![point(0, 10), point(0, 4)]);

        assert_eq!(result, Err(IndexError::DuplicateSlackTimestamp));
    }

    #[test]
    fn sorts_unsorted_points_before_normalizing() {
        let block =
            SlackBlock::from_unsorted_points(vec![point(100, 4), point(0, 10), point(50, 10)])
                .expect("the points should become valid after sorting");

        assert_eq!(block.points(), &[point(0, 10), point(100, 4)]);
        assert_eq!(block.minimum_slack(), Some(4));
    }

    #[test]
    fn applies_a_delta_to_only_the_selected_points() {
        let mut block = SlackBlock::from_sorted_points(vec![
            point(0, 10),
            point(10, 8),
            point(20, 12),
            point(30, 7),
        ])
        .expect("the block should be valid");
        block
            .apply_delta(-2)
            .expect("the block-wide delta should apply");

        block
            .apply_delta_to_point_range(1..3, -3)
            .expect("the partial delta should apply");

        assert_eq!(
            block.points(),
            &[point(0, 8), point(10, 3), point(20, 7), point(30, 5)]
        );
        assert_eq!(block.minimum_slack(), Some(3));
        assert_eq!(block.slack_delta(), 0);
    }

    #[test]
    fn partial_delta_normalizes_equal_adjacent_points() {
        let mut block =
            SlackBlock::from_sorted_points(vec![point(0, 10), point(10, 8), point(20, 10)])
                .expect("the block should be valid");

        block
            .apply_delta_to_point_range(1..2, 2)
            .expect("the partial delta should apply");

        assert_eq!(block.points(), &[point(0, 10)]);
        assert_eq!(block.minimum_slack(), Some(10));
    }

    #[test]
    fn rejects_an_invalid_point_range_without_mutation() {
        let mut block = SlackBlock::from_sorted_points(vec![point(0, 10), point(10, 8)])
            .expect("the block should be valid");
        let original = block.clone();

        let result = block.apply_delta_to_point_range(1..3, -2);

        assert_eq!(result, Err(IndexError::InvalidPointRange));
        assert_eq!(block, original);
    }

    #[test]
    fn partial_delta_overflow_does_not_mutate_the_block() {
        let mut block = SlackBlock::from_sorted_points(vec![point(0, Slack::MAX), point(10, 0)])
            .expect("the block should be valid");
        let original = block.clone();

        let result = block.apply_delta_to_point_range(0..1, 1);

        assert_eq!(result, Err(IndexError::SlackOverflow));
        assert_eq!(block, original);
    }

    #[test]
    fn timeline_delta_creates_boundaries_in_an_empty_timeline() {
        let mut timeline = SlackTimeline::empty();
        let interval = Interval::new(10, 20).expect("the interval should be valid");

        timeline
            .apply_delta(interval, -3)
            .expect("the timeline delta should apply");

        assert_eq!(
            timeline
                .effective_points()
                .expect("the timeline should remain valid"),
            vec![point(10, -3), point(20, 0)]
        );
    }

    #[test]
    fn timeline_delta_updates_partial_and_complete_blocks() {
        let mut first = SlackBlock::from_sorted_points(vec![point(0, 10), point(10, 8)])
            .expect("the first block should be valid");
        first
            .apply_delta(-2)
            .expect("the first block delta should apply");
        let second = SlackBlock::from_sorted_points(vec![point(20, 12), point(30, 0)])
            .expect("the second block should be valid");
        let mut timeline = SlackTimeline {
            blocks: vec![first, second],
        };
        let interval = Interval::new(5, 25).expect("the interval should be valid");

        timeline
            .apply_delta(interval, -3)
            .expect("the timeline delta should apply");

        assert_eq!(
            timeline
                .effective_points()
                .expect("the timeline should remain valid"),
            vec![
                point(0, 8),
                point(5, 5),
                point(10, 3),
                point(20, 9),
                point(25, 12),
                point(30, 0),
            ]
        );
    }

    #[test]
    fn timeline_delta_overflow_does_not_mutate_the_timeline() {
        let block = SlackBlock::from_sorted_points(vec![point(0, Slack::MAX), point(10, 0)])
            .expect("the block should be valid");
        let mut timeline = SlackTimeline {
            blocks: vec![block],
        };
        let original = timeline.clone();
        let interval = Interval::new(0, 5).expect("the interval should be valid");

        let result = timeline.apply_delta(interval, 1);

        assert_eq!(result, Err(IndexError::SlackOverflow));
        assert_eq!(timeline, original);
    }

    #[test]
    fn builds_slack_from_a_capacity_curve_with_a_gap() {
        let curve = capacity_curve(&[(0, 10, 10), (20, 30, 8)]);

        let timeline = SlackTimeline::from_capacity_curve(&curve)
            .expect("the capacity curve should produce slack");

        assert_eq!(
            timeline
                .effective_points()
                .expect("the timeline should be valid"),
            vec![point(0, 10), point(10, 0), point(20, 8), point(30, 0)]
        );
        assert_timeline_invariants(&timeline);
    }

    #[test]
    fn slack_at_observes_points_and_implicit_zero() {
        let curve = capacity_curve(&[(0, 10, 10), (20, 30, 8)]);
        let timeline = SlackTimeline::from_capacity_curve(&curve)
            .expect("the capacity curve should produce slack");

        assert_eq!(timeline.slack_at(-1), Ok(0));
        assert_eq!(timeline.slack_at(0), Ok(10));
        assert_eq!(timeline.slack_at(9), Ok(10));
        assert_eq!(timeline.slack_at(10), Ok(0));
        assert_eq!(timeline.slack_at(20), Ok(8));
        assert_eq!(timeline.slack_at(30), Ok(0));
    }

    #[test]
    fn minimum_slack_respects_half_open_interval_boundaries() {
        let curve = capacity_curve(&[(0, 10, 10), (10, 20, 3), (20, 30, 8)]);
        let timeline = SlackTimeline::from_capacity_curve(&curve)
            .expect("the capacity curve should produce slack");

        assert_eq!(
            timeline.minimum_slack(Interval::new(0, 10).expect("valid interval")),
            Ok(10)
        );
        assert_eq!(
            timeline.minimum_slack(Interval::new(10, 20).expect("valid interval")),
            Ok(3)
        );
        assert_eq!(
            timeline.minimum_slack(Interval::new(5, 25).expect("valid interval")),
            Ok(3)
        );
    }

    #[test]
    fn stores_the_maximum_capacity_as_slack() {
        let curve = capacity_curve(&[(0, 10, MAX_QUANTITY)]);
        let timeline = SlackTimeline::from_capacity_curve(&curve)
            .expect("maximum capacity should produce representable slack");

        assert_eq!(timeline.slack_at(5), Ok(Slack::MAX));
    }

    #[test]
    fn wide_rebuild_scratch_can_produce_a_representable_forced_deficit() {
        let pool_id = ResourcePoolId::generate();
        let curve = capacity_curve(&[(0, 10, MAX_QUANTITY)]);
        let first = claim(pool_id, 0, 10, MAX_QUANTITY);
        let second = claim(pool_id, 0, 10, MAX_QUANTITY);

        let timeline = SlackTimeline::from_capacity_and_claims(&curve, &[&first, &second])
            .expect("wide usage aggregation should narrow to a representable deficit");

        assert_eq!(timeline.slack_at(5), Ok(-Slack::MAX));
        assert_eq!(
            timeline.deficit_intervals(),
            Ok(vec![SlackDeficit::new(
                Interval::new(0, 10).unwrap(),
                MAX_QUANTITY,
            )])
        );
    }

    #[test]
    fn rebuilds_slack_from_capacity_and_active_claims() {
        let pool_id = ResourcePoolId::generate();
        let curve = capacity_curve(&[(0, 30, 10)]);
        let active_claim = claim(pool_id, 5, 25, 4);

        let timeline = SlackTimeline::from_capacity_and_claims(&curve, &[&active_claim])
            .expect("capacity and claims should rebuild slack");

        assert_eq!(
            timeline
                .effective_points()
                .expect("the timeline should be valid"),
            vec![point(0, 10), point(5, 6), point(25, 10), point(30, 0)]
        );
        assert_timeline_invariants(&timeline);
    }

    #[test]
    fn rebuilds_only_matching_held_and_committed_promises() {
        let pool_id = ResourcePoolId::generate();
        let other_pool_id = ResourcePoolId::generate();
        let curve = capacity_curve(&[(0, 30, 10)]);
        let make_promise = |claim: Claim| {
            Promise::with_id(
                PromiseId::generate(),
                Bundle::new(vec![claim]).expect("the bundle should be valid"),
                100,
                0,
                SequenceNumber::new(1),
            )
            .expect("the promise should be valid")
        };
        let held = make_promise(claim(pool_id, 0, 10, 2));
        let mut committed = make_promise(claim(pool_id, 10, 20, 3));
        committed
            .commit(committed.version(), 0, SequenceNumber::new(2))
            .expect("the promise should commit");
        let mut released = make_promise(claim(pool_id, 20, 30, 4));
        released
            .release(released.version(), 0, SequenceNumber::new(2))
            .expect("the promise should release");
        let other_pool = make_promise(claim(other_pool_id, 0, 30, 9));

        let timeline = SlackTimeline::from_capacity_and_promises(
            &curve,
            pool_id,
            &[&held, &committed, &released, &other_pool],
        )
        .expect("the promises should rebuild slack");

        assert_eq!(
            timeline
                .effective_points()
                .expect("the timeline should be valid"),
            vec![point(0, 8), point(10, 7), point(20, 10), point(30, 0)]
        );
        assert_timeline_invariants(&timeline);
    }

    #[test]
    fn applying_and_reverting_a_delta_restores_the_canonical_timeline() {
        let mut timeline = SlackTimeline::empty();
        let original = timeline.clone();
        let interval = Interval::new(10, 20).expect("the interval should be valid");

        timeline
            .apply_delta(interval, -3)
            .expect("the delta should apply");
        timeline
            .apply_delta(interval, 3)
            .expect("the inverse delta should apply");

        assert_eq!(timeline, original);
        assert_timeline_invariants(&timeline);
    }

    #[test]
    fn complete_interior_block_keeps_a_lazy_delta() {
        let points: Vec<SlackPoint> = (0..600)
            .map(|timestamp| point(timestamp, 10 + Slack::from(timestamp % 2)))
            .collect();
        let mut timeline = SlackTimeline {
            blocks: SlackTimeline::blocks_from_sorted_points(points)
                .expect("the points should form blocks"),
        };
        let interval = Interval::new(256, 512).expect("the interval should be valid");

        timeline
            .apply_delta(interval, -2)
            .expect("the delta should apply");

        assert_eq!(timeline.blocks().len(), 3);
        assert_eq!(timeline.blocks()[1].slack_delta(), -2);
        assert_eq!(timeline.minimum_slack(interval), Ok(8));
        assert_timeline_invariants(&timeline);
    }

    #[test]
    fn random_updates_match_a_slow_reference_model() {
        let mut timeline = SlackTimeline::empty();
        let mut reference: Vec<(Interval, Slack)> = Vec::new();
        let mut random_state: u64 = 0x5eed_cafe;

        for _ in 0..200 {
            random_state = random_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let start = Timestamp::try_from(random_state % 40).expect("start should fit");
            random_state = random_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let end =
                start + Timestamp::try_from(random_state % 10 + 1).expect("duration should fit");
            random_state = random_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let mut delta = Slack::try_from(random_state % 11).expect("small delta should fit") - 5;
            if delta == 0 {
                delta = 1;
            }
            let interval = Interval::new(start, end).expect("the interval should be valid");

            timeline
                .apply_delta(interval, delta)
                .expect("the small random delta should apply");
            reference.push((interval, delta));

            for timestamp in 0..=50 {
                let expected: Slack = reference
                    .iter()
                    .filter(|(interval, _)| interval.contains(timestamp))
                    .map(|(_, delta)| *delta)
                    .sum();
                assert_eq!(timeline.slack_at(timestamp), Ok(expected));
            }

            let query_start = start.saturating_sub(2);
            let query_end = (end + 2).min(51);
            let query = Interval::new(query_start, query_end).expect("query should be valid");
            let expected_minimum = (query_start..query_end)
                .map(|timestamp| {
                    reference
                        .iter()
                        .filter(|(interval, _)| interval.contains(timestamp))
                        .map(|(_, delta)| *delta)
                        .sum::<Slack>()
                })
                .min()
                .expect("the query should not be empty");
            assert_eq!(timeline.minimum_slack(query), Ok(expected_minimum));
            assert_timeline_invariants(&timeline);
        }
    }
}
