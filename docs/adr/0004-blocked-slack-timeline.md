# ADR-0004: Blocked slack timeline

- Status: Accepted
- Date: 2026-07-29

## Context

Admission repeatedly asks for minimum slack over time ranges and applies additions or removals of usage. Scanning every promise for every temporal segment is correct but expensive. A node-per-point tree has significant memory and cache overhead.

## Decision

Represent derived slack as ordered change points grouped into blocks of at most 256 points.

Each block stores:

```text
ordered SlackPoints
minimum effective slack
maximum effective slack
lazy block-wide slack delta
```

A point's value applies from its timestamp until the next point. Complete blocks use aggregate queries and lazy deltas. Partial boundary blocks materialize their delta and update selected points. Adjacent equal values are normalized.

## Consequences

- Points inside a block are contiguous and cache-friendly.
- Local insertions move at most one bounded block before split/rebalance.
- Complete blocks can be checked or adjusted without visiting each point.
- Positive and negative overflow can be preflighted using maximum and minimum slack.
- Operations still inspect the affected block list; this is not a logarithmic augmented tree.
- The index remains reconstructible from authoritative state.

## Alternatives considered

- One flat vector: lower overhead but potentially moves the entire tail on insertion.
- `BTreeMap`: simpler insertion but more memory and no range-min aggregate.
- Dynamic segment tree or augmented B+tree: stronger asymptotics but substantially more implementation complexity.
- Recomputing from promises per hold: retained as the correctness oracle only.
