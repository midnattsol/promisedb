# ADR-0008: Canonical command hashing for idempotency

- Status: Accepted
- Date: 2026-08-01

## Context

Clients retry mutating requests when responses are delayed or lost. Reapplying a successful hold, commit, release, or replacement would create duplicate transitions or state-dependent errors. Retaining complete command payloads for comparison would consume memory proportional to bundles and duplicate data required elsewhere.

## Decision

Every command is identified by `(ClientId, IdempotencyKey)`. PromiseDB hashes the normalized `CommandOperation` with BLAKE3 and retains the 32-byte digest plus the complete original `CommandResponse`.

Canonical hashing uses a versioned domain prefix, explicit one-byte tags, little-endian fixed-width integers, length-prefixed UTF-8 strings and collections, stable UUID bytes, and normalized capacity curves. Bundle claims are sorted by resource-pool ID, interval start, interval end, and quantity before hashing, so input claim order is not semantically significant.

An exact retry returns the cached response before inspecting the supplied timestamp or current state. Reusing the identity with another operation returns `IdempotencyConflict`. Both successful and error responses are cached. Idempotency records are authoritative recovery state and must be included in snapshots or restored from persisted prepared transitions without replaying commands.

## Consequences

- Exact retries consume no sequence, emit no events, and do not process expirations.
- Duplicate commit and release return their original successful versions.
- A response remains stable even if later commands change relevant state.
- Memory retains a fixed-size command digest plus the original response, not the complete command payload.
- The canonical binary representation becomes a compatibility contract and is protected by golden hash tests. It is version-sensitive: any future change to integer byte order or other canonical bytes requires a new format/domain version rather than silently changing hashes under the existing version.
- BLAKE3 collisions are treated as equivalent operations; the cryptographic collision risk is accepted for the MVP.

## Alternatives considered

- Storing complete normalized commands: rejected because it duplicates potentially large bundles in memory and snapshots.
- Rust `DefaultHasher`: rejected because its output is not a stable cross-version serialization contract.
- Hashing raw Rust memory: rejected because layout, padding, architecture, and addresses are not deterministic formats.
- Caching only successful responses: rejected because exact retries must remain stable after state changes even when the original response was an error.
