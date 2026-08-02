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

File-storage crash tests use RAII directories under `std::env::temp_dir()` with unique version-4 UUID names and no test-only filesystem dependency. They cover same-process lock exclusion and release, canonical manifest bytes and checksum corruption, persisted record-limit mismatch, segment UUID and naming validation, whole-group rotation, oversized fresh segments, cross-segment streaming recovery, exact sequence continuation, duplicate-apply prevention, valid empty active segments, locked temp cleanup, repair and synchronization boundaries for a final partial tail, fatal sealed partial tails, fatal complete checksum corruption, and fatal sequence gaps without resynchronization. Fault tests drop the live coordinator before direct byte mutation so the lock contract remains explicit.

## Slack timeline microbenchmark

The dependency-free benchmark uses `std::hint::black_box` and `std::time::Instant`. Run it with:

```sh
cargo bench --bench slack_timeline
```

It reports bounded loops for `slack_at`, `minimum_slack`, a complete interior-block update pair, and a partial update pair. Results are intended for comparing changes on the same machine and toolchain, not as portable absolute thresholds.

The current implementation remains a blocked array-of-structs (AoS). Splitting block metadata or changing points to a structure-of-arrays (SoA) representation should be considered only when this benchmark, supplemented by representative workload measurements, demonstrates a repeatable benefit.
