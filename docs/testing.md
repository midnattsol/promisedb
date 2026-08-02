# Testing and benchmarks

## Validation

Run the complete local validation set from the repository root:

```sh
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items
```

Numeric boundary tests cover `MAX_QUANTITY`, rejected out-of-range claims and capacities, maximum stored slack, forced deficits, wide aggregate demand, and transactional overflow behavior. The index also checks the expected x86_64 layouts for `SlackPoint` and `SlackBlock`.

File-storage crash tests use RAII directories under `std::env::temp_dir()` with unique version-4 UUID names and no test-only filesystem dependency. They cover same-process lock exclusion and release, canonical manifest bytes and persisted-limit mismatch, segment UUID/naming validation, rotation, cross-segment recovery, continuation, retry prevention, final-tail repair, and fatal corruption/gaps. Snapshot coverage includes watermark-zero empty state, recognized temp cleanup, forced suffix rotation and covered-segment pruning, exact state/event/idempotency retry equivalence, suffix replay, highest-snapshot corruption without fallback, and property-style arbitrary-byte decoding under small limits with panic detection.

## Slack timeline microbenchmark

The dependency-free benchmark uses `std::hint::black_box` and `std::time::Instant`. Run it with:

```sh
cargo bench --bench slack_timeline
```

It reports bounded loops for `slack_at`, `minimum_slack`, a complete interior-block update pair, and a partial update pair. Results are intended for comparing changes on the same machine and toolchain, not as portable absolute thresholds.

The current implementation remains a blocked array-of-structs (AoS). Splitting block metadata or changing points to a structure-of-arrays (SoA) representation should be considered only when this benchmark, supplemented by representative workload measurements, demonstrates a repeatable benefit.
