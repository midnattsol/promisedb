# ADR-0009: Explicit fixed-point resource units

- Status: Accepted
- Date: 2026-08-01

## Context

Resource capacity may be divisible: applications can require values such as 1.5 watts, machine equivalents, or labor units. Binary floating-point cannot exactly represent many decimal values and would make admission comparisons, replay, and canonical command hashes vulnerable to rounding differences. Keeping scale only in an informal unit string would make durable quantities ambiguous.

## Decision

Each resource pool owns an immutable `Unit` containing a non-empty major-unit name and a nonzero `subunits_per_unit` scale. `Quantity` remains `u64` and always stores integer subunits.

For `Unit { name: "watts", subunits_per_unit: 1_000 }`, quantity `1_500` represents 1.5 watts. The control API converts human decimal or prefixed-unit input to integer subunits before constructing claims or capacity curves. Claims do not repeat the unit because their resource-pool ID determines it.

The unit name and scale are part of `CreateResourcePool` canonical hashing and future durable formats. The engine does not perform display formatting or unit conversion.

## Consequences

- Capacity arithmetic and comparisons remain exact integer operations.
- Replay and replicated application do not depend on floating-point behavior.
- Pools can represent divisible or indivisible resources by choosing an appropriate scale.
- Changing a pool's unit or scale is not a capacity revision; the unit is immutable.
- SDKs and the control API must reject values that cannot be represented exactly at the pool's configured scale.
- Maximum displayed quantity decreases as scale increases because stored quantities remain `u64`.

## Alternatives considered

- `f64` quantities: rejected because decimal values such as 0.1 are not generally exact and special values such as NaN complicate ordering.
- An opaque unit string with implicit scale: rejected because quantities would be ambiguous across clients and durable records.
- Repeating unit metadata on every claim: rejected because the resource pool already defines the unit and mismatches would require redundant validation.
