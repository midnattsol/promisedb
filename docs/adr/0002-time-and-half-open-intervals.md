# ADR-0002: Integer UTC time and half-open intervals

- Status: Accepted
- Date: 2026-07-29

## Context

Capacity and usage require unambiguous boundaries. Timezone-aware values and inclusive interval ends introduce unnecessary ambiguity inside the engine.

## Decision

Use `Timestamp = i64` as Unix UTC time. Every interval is `[start,end)` and requires `start < end`.

## Consequences

- Adjacent intervals do not overlap.
- Start events apply at `start`; end events stop applying at `end`.
- Timezone conversion remains outside PromiseDB.
- The chosen timestamp unit must remain consistent across APIs and durable formats.

## Alternatives considered

- Inclusive end timestamps: rejected because adjacent reservations would share a boundary.
- Floating-point time: rejected because ordering and serialization must be exact.
- Timezone-aware domain values: rejected because the engine only needs an ordered UTC timeline.
