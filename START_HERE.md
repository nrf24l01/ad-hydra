# Continue development on Hydra

Hydra already has a working Rust Core/Proxy workspace and an embedded Vue console. **Do not restart implementation at Phase 0.** Begin with the existing code and the next requested change.

Read in this order:

1. [README](README.md) — product, architecture, installation and maintenance.
2. [SPEC](SPEC.md), [AGENTS](AGENTS.md) and [repository structure](REPOSITORY_STRUCTURE.md) — product authority and engineering boundaries. Explicit amendments supersede older conflicting text.
3. [Current handoff](docs/HANDOFF.md) — implemented scope, known limits and last recorded verification.
4. [Developer guide](docs/DEVELOPMENT.md) — source map, local startup, contracts and change workflow.
5. [Implementation plan](IMPLEMENTATION_PLAN.md) — original delivery order; consult [progress](PROGRESS.md) and [decisions](DECISIONS.md) before treating an item as unfinished.
6. `contracts/` and the relevant feature guide from the [documentation index](docs/README.md).

Inspect `git status` and recent commits before editing. Preserve unrelated local files. Generated REST contracts and the canonical protobuf must match the implementation; historical draft contracts are reference material only.

For each change, implement working behavior, run checks appropriate to the affected paths, and update the relevant guide and progress record. Record architectural or compatibility changes in `DECISIONS.md`. Report actual verification results and unresolved limitations. Keep credentials, private infrastructure addresses, runtime state and captures outside the repository.
