# ADR-0010: Prepared durable transitions

- Status: Accepted
- Date: 2026-08-02

## Context

Persisting commands and executing them again during recovery makes historical state depend on the current admission implementation. It also cannot safely represent commands whose requested operation failed after due expirations were applied, unavailable outcomes that must be cached exactly, or future changes to deterministic algorithms.

Durability must order persistence before in-memory publication without mutating visible engine state during validation.

## Decision

PromiseDB's durable path prepares each first-seen command against a cloned candidate `EngineState`. Preparation first computes the next publication revision with checked arithmetic, then returns the exact response, a versioned durable effect record, runtime-only candidate state, and that next revision. The published engine remains untouched until storage has accepted the effect. Publication verifies the base revision, then replaces state and assigns the already-prepared next revision without arithmetic, domain validation, or admission.

`Engine::apply` remains the fast in-memory path. It performs idempotency lookup and checked revision preflight directly, runs the existing transition against published state, caches the response, and advances publication revision without cloning `EngineState`.

The durable transition stores:

- the original command for audit;
- its client and idempotency identity;
- the persisted canonical command-hash bytes;
- the exact success, unavailable, or error response;
- changed or new authoritative resource-pool and promise after-values;
- exact newly emitted ordered events; and
- the final domain sequence.

It does not store `SlackTimeline`. Recovery decodes and validates transitions, installs their authoritative after-values and idempotency records, and rebuilds derived timelines. Recovery never executes the original command.

The version-1 transition payload uses explicit tags, little-endian fixed-width integers, stable UUID bytes, canonical map-derived ordering, and `u32` length prefixes. Runtime preparation may later move from a full state clone to sparse overlays without changing this durable representation.

## Consequences

- Exact retries and idempotency conflicts create no new durable transition.
- Every first-seen command creates one durable transition, including errors and unavailable outcomes and transitions containing only expirations or idempotency state.
- A stale prepared candidate is rejected using publication revision, independently of domain sequence.
- Recovery reproduces historical effects across admission-code changes and can install effects that replaying the audit command against current state would reject.
- Versioned manual codecs and restoration validation add implementation work, but avoid Rust-layout and serialization-library coupling.
- Single and batch durable preparation clone complete engine state. A nonempty batch clones it exactly once and executes commands sequentially against that candidate; sparse overlays remain an optimization that must preserve durable bytes. Direct `Engine::apply` does not clone the complete state.
- Version 1 batch items may contain cumulative authoritative after-values relative to the batch's original base. This avoids per-command state clones but can enlarge later WAL records in a group. Events, timestamps, responses, and idempotency identities remain exact per command; a future sparse-diff optimization must preserve recovery semantics and explicitly version any durable format change.
- Publication revision is a runtime `u128` generation. First-seen apply, durable preparation, and recovery installation reject revision exhaustion before command execution, candidate cloning, or state mutation. Exact retries and idempotency conflicts remain available at the maximum revision because they publish nothing.

## Alternatives considered

### Persist and replay commands

Rejected because recovery would re-run admission and could diverge after code changes. It also conflates requested operations with exact committed effects.

### Persist events only

Rejected because audit events intentionally omit complete authoritative after-values and cached command responses.

### Serialize the complete candidate state

Rejected because it would persist derived indexes, enlarge every record, and couple durability to runtime representation rather than stable authoritative effects.
