# ADR-0007: Control-plane preparation and deterministic commands

- Status: Accepted
- Date: 2026-08-01

## Context

Replay and future replicated nodes must apply identical inputs. Generating resource-pool or promise IDs inside the replicated state machine would make results depend on local randomness. External requests also need a stable idempotency identity without repeating those fields in every operation.

## Decision

A control API prepares internal commands before state-machine entry. It generates `ResourcePoolId` and `PromiseId`, resolves any external names, and supplies a `(ClientId, IdempotencyKey)` pair. The resulting `Command` contains a deterministic `CommandOperation` and is applied through `Engine::apply(command, now)`.

The authoritative timestamp is supplied separately from the command. Future replication or replay must reuse the timestamp chosen for the original application.

Commands are intended to become the WAL recovery input. Events are ordered audit facts derived from successful transitions, not the sole recovery source. Queries are pure current-state reads; deadline processing is an explicit command or occurs before another mutating command.

## Consequences

- Replicas and replay preserve exactly the same entity identities.
- The engine command path uses no randomness or system clock.
- Control-plane deployment can later move outside the database process without changing command semantics.
- Idempotency identity is present on every command; storage and duplicate detection remain a separate implementation step.
- One command can emit expiration events before the requested-operation event.
- Direct convenience methods remain available during the MVP, but the durable boundary is `Engine::apply`.

## Alternatives considered

- Generating IDs independently in each state-machine replica: rejected because it is nondeterministic.
- Letting external clients choose all internal IDs directly: rejected as the only interface because the control API should own identity preparation.
- Persisting only derived events: rejected because commands preserve the requested deterministic transition more directly for replay.
- Processing expirations inside read queries: rejected because reads should not mutate authoritative state.
