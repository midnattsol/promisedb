use promisedb::domain::{CapacityCurve, CapacitySegment, Interval};
use promisedb::index::SlackTimeline;
use std::hint::black_box;
use std::time::Instant;

const POINT_COUNT: i64 = 768;
const QUERY_ITERATIONS: u32 = 200_000;
const UPDATE_ITERATIONS: u32 = 2_000;

fn timeline() -> SlackTimeline {
    let segments = (0..POINT_COUNT)
        .map(|timestamp| {
            CapacitySegment::new(
                Interval::new(timestamp, timestamp + 1)
                    .expect("benchmark interval should be valid"),
                if timestamp % 2 == 0 { 100 } else { 101 },
            )
        })
        .collect();
    let curve = CapacityCurve::from_sorted(segments).expect("benchmark curve should be valid");
    SlackTimeline::from_capacity_curve(&curve).expect("benchmark timeline should be valid")
}

fn measure(name: &str, iterations: u32, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    let elapsed = started.elapsed();
    let nanos_per_iteration = elapsed.as_nanos() / u128::from(iterations);
    println!("{name:24} {nanos_per_iteration:>10} ns/iter ({elapsed:?} total)");
}

fn main() {
    let timeline = timeline();
    let query_interval = Interval::new(128, 640).expect("query interval should be valid");

    measure("slack_at", QUERY_ITERATIONS, || {
        black_box(
            timeline
                .slack_at(black_box(511))
                .expect("slack query should succeed"),
        );
    });

    measure("minimum_slack", QUERY_ITERATIONS, || {
        black_box(
            timeline
                .minimum_slack(black_box(query_interval))
                .expect("minimum query should succeed"),
        );
    });

    let full_block = Interval::new(256, 512).expect("full-block interval should be valid");
    let mut full_block_timeline = timeline.clone();
    measure("full-block update pair", UPDATE_ITERATIONS, || {
        full_block_timeline
            .apply_delta(black_box(full_block), black_box(-1))
            .expect("full-block decrement should succeed");
        full_block_timeline
            .apply_delta(black_box(full_block), black_box(1))
            .expect("full-block restoration should succeed");
        black_box(&full_block_timeline);
    });

    let partial = Interval::new(257, 511).expect("partial interval should be valid");
    let mut partial_timeline = timeline;
    measure("partial update pair", UPDATE_ITERATIONS, || {
        partial_timeline
            .apply_delta(black_box(partial), black_box(-1))
            .expect("partial decrement should succeed");
        partial_timeline
            .apply_delta(black_box(partial), black_box(1))
            .expect("partial restoration should succeed");
        black_box(&partial_timeline);
    });
}
