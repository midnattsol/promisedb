# Indexes and invariants

## Sources of truth

Authoritative state:

```text
resource pools and capacity curves
promises, bundles, states, and versions
idempotency hashes and cached responses
global sequence
```

Derived state:

```text
SlackTimeline per resource pool
normalized deficit intervals
at-risk promise lists
availability explanations
```

Events are ordered audit output. Future storage will define their durable retention, but recovery must not depend on an in-memory index being the only copy of business state.

## Admission invariant

For normal admission:

```text
held usage + committed usage <= capacity
```

Equivalent slack rule:

```text
slack = capacity - active usage
slack >= 0
```

A forced capacity revision may create negative slack. New holds must not worsen it. Replace may retain negative slack only when the final value is no worse than the previous value.

## Capacity and usage boundaries

Capacity and claims use half-open intervals. Every algorithm must preserve:

```text
[a, b) does not overlap [b, c)
```

Capacity outside explicit curve segments is zero. Quantities are integer subunits under the pool's immutable `Unit`.

## Updating slack

- Hold: subtract accepted claim quantities.
- Commit: no slack change; held and committed both consume capacity.
- Release: add old claim quantities.
- Expiration: add old claim quantities.
- Replace: restore the old bundle on temporary timelines, then subtract the new bundle and publish only the final timelines.
- Capacity revision: rebuild from the replacement curve and active promises.

Always update every affected pool atomically.

## Rebuilding for comparison

`SlackTimeline::from_capacity_and_promises` is the direct reconstruction path. When debugging an index discrepancy:

1. process or account for due expirations;
2. collect active held and committed promises;
3. rebuild the timeline from the pool curve;
4. compare effective points with the maintained timeline;
5. identify the first transition where they differ.

The slow availability path under `#[cfg(test)]` is a correctness oracle. Do not remove it merely because the indexed path is faster.

## Blocked timeline cautions

Slack points mark value changes; a point applies until the next point. Blocks may hold a lazy delta. Use effective values or existing APIs rather than reading stored point slack as if every delta were materialized.

Range updates must preflight overflow and publish no partial mutation on error.
