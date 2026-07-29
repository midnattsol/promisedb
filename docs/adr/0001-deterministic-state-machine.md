# ADR-0001: Deterministic state machine with an injected clock

- Status: Accepted
- Date: 2026-07-29

## Context

Expiration depends on time. Reading the system clock inside domain objects would make tests, replay, and replicated execution nondeterministic.

## Decision

The engine owns a `Clock` supplied at construction. Public operations read it once and delegate to deterministic `*_at` transitions that receive `now: Timestamp` explicitly.

`Engine` stores `Box<dyn Clock>` so its concrete type does not depend on the clock implementation.

## Consequences

- Tests can use fixed timestamps without sleeping.
- Replay can reuse the original command timestamp.
- A future leader can choose one timestamp for every replica.
- Clock failures remain explicit operation errors.
- Dynamic dispatch adds one insignificant call at the command boundary.

## Alternatives considered

- Calling `SystemTime::now()` inside domain types: rejected as nondeterministic.
- Making `Engine` generic over the clock: rejected because the generic type would spread through callers.
- A global clock: rejected because it hides a state-machine dependency.
