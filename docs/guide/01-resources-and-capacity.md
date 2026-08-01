# 2. Resources and capacity

A `ResourcePool` represents one kind of finite capacity.

Examples:

```text
workshop-a/machines
workshop-a/operators
workshop-a/inspection
```

Each pool has:

- an opaque ID used by the engine;
- a display name for humans;
- an immutable fixed-point unit;
- a capacity curve.

A unit contains a name and an integer scale:

```text
name: watts
subunits_per_unit: 1000
```

The engine stores quantities in subunits. With this unit:

```text
quantity 1000 → 1 watt
quantity 1500 → 1.5 watts
quantity 1    → 0.001 watts
```

An indivisible resource uses a scale of one. PromiseDB never uses floating-point capacity. The control API converts human inputs such as `1.5 watts` or `1500 milliwatts` to the same integer quantity before creating a command.

## Capacity changes over time

Capacity is not necessarily constant. A workshop may have two machines in the morning and one during maintenance:

```text
[08:00, 12:00) → capacity 2
[12:00, 14:00) → capacity 1
[14:00, 18:00) → capacity 2
```

This is a `CapacityCurve`: an ordered collection of constant-capacity segments.

Intervals are half-open:

```text
[start, end)
```

The start is included and the end is excluded. Therefore these intervals are adjacent, not overlapping:

```text
[08:00, 12:00)
[12:00, 14:00)
```

At exactly 12:00, the second interval applies.

## Gaps mean zero capacity

If no capacity segment covers a timestamp, capacity is zero:

```text
[08:00, 12:00) → 2
[14:00, 18:00) → 2
```

Between 12:00 and 14:00 the pool is unavailable.

## Why IDs and display names are separate

The control API generates stable IDs before a command reaches the engine. Display names may be easier for people, but IDs provide unambiguous identity during replay and future replication.

Next: [Claims, bundles, and promises](02-claims-bundles-promises.md).
