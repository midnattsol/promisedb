# PromiseDB maintainer guide

This guide is for engineers changing the PromiseDB codebase. It complements, but does not replace:

- [Semantics](../semantics.md): authoritative behavioral rules.
- [Architecture](../architecture.md): component boundaries and state flow.
- [ADRs](../adr/README.md): reasons behind durable decisions.
- [Learning guide](../guide/README.md): conceptual introduction.
- Rustdoc: concrete API contracts.

Recommended order:

1. [Code tour](code-tour.md)
2. [State transitions](state-transitions.md)
3. [Adding a command](adding-a-command.md)
4. [Indexes and invariants](indexes-and-invariants.md)
5. [Testing and debugging](testing-and-debugging.md)

## Before changing behavior

1. Find the rule in `docs/semantics.md`.
2. Find the owning module in `code-tour.md`.
3. Check the relevant ADR before changing a durable representation.
4. Add a failing test at the narrowest useful level.
5. Prepare all fallible state on copies before publishing.
6. Run the full validation commands in `testing-and-debugging.md`.

## Current maturity

PromiseDB is a single-process durable state machine. Commands, events, idempotency, capacity revisions, derived slack indexes, versioned codecs, a locked segmented WAL, effect recovery, atomic snapshots, and post-install compaction exist. Replication and cross-host coordination are not implemented; do not document or rely on them as current behavior.
