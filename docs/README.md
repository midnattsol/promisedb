# PromiseDB documentation

- [Learning guide](guide/README.md): a beginner-friendly path through the core concepts.
- [Maintainer guide](maintainers/README.md): practical procedures for changing the code safely.
- [Architecture](architecture.md): current component boundaries and transition flow.
- [Semantics](semantics.md): authoritative behavioral rules.
- [Architecture Decision Records](adr/README.md): decisions, alternatives, and consequences.

API-level documentation is generated from Rustdoc:

```bash
cargo doc --no-deps --open
```
