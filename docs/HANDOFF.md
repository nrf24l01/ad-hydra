# Current development handoff

Documentation review: 2026-11-23. Code baseline: `7629be1` on `main`. This is a continuation snapshot, not a claim that every original acceptance target has been measured.

## Implemented scope

| Area | Working implementation and reference |
| --- | --- |
| Runtime | Independent Rust Core and Proxy, gRPC over UDS, SQLite owned by Core, Vue embedded into Core; [development](DEVELOPMENT.md) |
| Transport | TCP full duplex/half-close, bounded UDP sessions, shared relay context, CPU-aware Tokio pools; [performance](PERFORMANCE.md) |
| TLS | Per-service TCP termination and upstream TLS, live certificate reload, API private-key redaction; [configuration](CONFIGURATION.md) |
| Inspection | Native raw/UTF-8/HTTP, framing profiles, versioned WASM parsers with schemas; [decoder guide](CUSTOM_DECODERS.md) |
| Traffic | Live updates, saved ordered TCP and datagram history, terminal/hex/decoded HTTP views, shared stream previews, favourites; [traffic](traffic-view.md) |
| Filtering | Shared service scope, AND/OR/NOT and per-condition direction, schema-driven fields, regex shortcut, Python checker editor, desired/applied state; [rules](rules-integration.md) and [checkers](REQUEST_CHECKERS.md) |
| Farm | Service/team catalogs, uploaded or pasted Binary/Python/Bash versions, uv dependencies, manual/scheduled runs, effectiveness matrix, retained logs; [farm](farm.md) |
| Flags | Exact regex matches from authorized Farm/manual sources, persistent queue, pagination, manual retry/batches, ForcAD/custom Python submitter and submission I/O; [flags](flags-operations.md) |
| Identity | Equal full privileges, CLI bootstrap and authenticated UI account creation, 60-second access JWT, rotating refresh with 10-hour absolute lifetime, audit |
| Distribution | Static x86_64 musl binaries, installer with public GitHub/authenticated Actions/S3/offline archive sources, systemd units, rollback; [install](INSTALL.md) |

New byte histories and logs use zstd; legacy gzip remains readable. Captured-byte history defaults to 128 MiB per service with a configurable compression level. Decoded traffic has a separate 256 MiB compressed payload budget per service at fixed zstd level 3. Favourite admission and capture bounds still apply. See [traffic retention](traffic-view.md#decoded-history-storage-2026-10-23).

## Compatibility and limits to preserve

- Unknown traffic passes unchanged unless an operator enables a rule, checker or substitution policy. Ordinary reverse proxying changes the backend-visible peer IP.
- Raw is a hex view, not fabricated parsed messages. UTF-8 is a separate decoder/view. HTTP/2 and WebSocket semantic decoding are not implemented; service TLS offers HTTP/1.1 ALPN. Old encrypted sessions cannot be decrypted by adding a certificate later.
- History is bounded and can be partial, dropped or rotated. A flag marker does not guarantee corresponding decoded or byte content remains available. Preserve explicit loss and availability states instead of displaying empty data as a complete capture.
- Audit remains append-only; there is no artificial shared SQLite size ceiling. Per-feature retention does not bound total database/index/audit size. Operators must monitor disk and backups.
- Proxy-observed flags are value-free presence markers. They must not enter the flag submission vault. Explicit substitution mappings have their own protected store and never enter the submission queue.
- Python/WASI request checkers are distinct from capture decoder plugins and Farm jobs. Their `/state` files persist; Python checkers run in networkless Bubblewrap workers. Farm host mode is an explicit option; missing sandbox prerequisites never silently select it.
- Upgrade Proxy before Core for additive wire features, including direction scopes and shared service rules. A downgrade across decoded-storage migrations requires a matching pre-migration database backup. Capture formats also have downgrade limits; consult the decisions and traffic guides.
- Farm host mode retains a shared home and uv cache, but its per-run working directory is removed. A dedicated per-checker/per-exploit persistent mount is not established by the current host runner; do not confuse it with traffic checkers’ `/state`. See [Farm storage](farm.md#host-runner-files-and-persistence).
- Short two-CPU relay measurements exist, but sustained competition-load behavior on a physical 2 GiB host and nginx-level performance parity are not established.

## Last recorded verification

[Actions run 44873454868](https://github.com/nrf24l01/ad-hydra/actions/runs/44873454868), for `7629be1`, completed successfully on 2026-11-21. It passed frontend checks, Rust formatting/check/Clippy/workspace tests, contracts, local service end-to-end fixtures, embedded Chromium smoke, release and static musl builds, artifact upload, and public GitHub release publication. Optional S3 publication was skipped by its configuration gate. This is historical evidence, not a fresh result for later commits.

The fixes preceding that run were a TLS fixture using the shared configuration revision after a filter mutation, a larger scrollable decoder-field dropdown, and browser assertions updated for required names and compact filter rows. Both binaries were redeployed during that session; current remote state must be inspected separately before any new deployment.

## Resume checklist

1. Inspect branch, working tree, remote changes and the user's next requested scope. Do not overwrite unrelated local work or assume a new PR has been merged.
2. Read the relevant feature guide and source; use the generated API and canonical protobuf for current interfaces.
3. Implement a focused change, update contracts/migrations if needed, and run the corresponding checks from [verification](VERIFICATION.md).
4. Record new evidence and limitations here or in [progress](../PROGRESS.md), and document compatibility decisions. Recheck current Actions after a push; previous success is not evidence for a new commit.

No new feature priority is inferred here. Continue the user's requested work; use unresolved limits above as constraints and investigate a concrete regression before changing behavior.
