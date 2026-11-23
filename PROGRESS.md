# Hydra implementation ledger

Authority: SPEC.md. Delivery order: IMPLEMENTATION_PLAN.md. This is a historical milestone ledger. Older paragraphs describe behavior at the time and may be superseded. Use [the current handoff](docs/HANDOFF.md) for continuation and [the documentation index](docs/README.md) to find feature guides.

## Documentation handoff — 2026-11-23

Reviewed the existing guides against the current source at `7629be1`. Added a developer guide, current implementation/limitations handoff and documentation index; replaced the obsolete Phase 0 starter prompt. Updated verification build order, TLS/filter configuration, frontend history wording, release distribution and deployment storage guidance. Documented the host runner’s actual shared HOME/cache persistence and temporary run directory; a dedicated per-exploit state mount is a remaining implementation gap. This change documents existing behavior; it adds no runtime feature.

Last recorded successful CI: [44873454868](https://github.com/nrf24l01/ad-hydra/actions/runs/44873454868), 2026-11-21, commit `7629be1`; see the handoff for coverage and publication caveats. Documentation validation results are reported with this change; application tests are not rerun for prose edits.

## Latest milestone — 2026-10-22

## Milestone — 2026-10-20

## Phase 0 — working foundation

Implemented seven real workspace packages: independent `apps/hydra-core` and `apps/hydra-proxy`, pure `hydra-domain`, generated `hydra-wire`, `hydra-filter-engine`, independent `hydra-protocol-sdk`, and `hydra-protocol-runtime`. Thin mains; vertical Core features, explicit Proxy transport/inspection/gRPC/telemetry modules. Canonical protobuf generates only in Cargo OUT_DIR. Original OpenAPI draft preserved; active OpenAPI derives from handlers. Frontend helpers/types derive from that snapshot. Added build configuration, CI, architecture checker and deployment instructions.

Verification commands: `cargo check --locked --workspace --all-targets`, `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, `cargo test --locked --workspace`, `scripts/check-contracts.sh`, `python3 scripts/generate-frontend.py --check`. Final verification results are recorded below. Dependency-direction check passed: no app-to-app Cargo dependency, no Proxy SQLite/auth/API dependency, independent SDK, one canonical proto.

Changed areas: root Cargo files/lock/toolchain, `.cargo`, `.github/workflows`, `apps`, `crates`, `contracts`, `scripts`, `frontend`, `deploy`, documentation.

## Phase 1 — real relay and UDS control

TCP full duplex and half-close preserve binary bytes; bounded UDP mappings preserve replies, including empty datagrams. Connection/time/buffer/channel bounds are explicit. Local gRPC uses mode0600 UDS and peer UID checks. Service/rule snapshots validate, bind listeners before mutation, persist atomically and ACK; combined snapshots preserve cross-references. Proxy restarts from its last accepted state independently. Cancellation-admitted commits finish safely; graceful shutdown drains admitted changes. Normal flow metadata is parsed telemetry, never packet streaming.

`python3 tests/e2e/relay.py` passed against both real executables: binary/fragmented TCP, half-close, two UDP clients, zero-length datagram, mapping expiry, global capacity rejection, config ACK/idempotency/stale/conflicting-bind rollback, existing TCP during Core kill, Proxy/Core restart and saved config. State integration tests exercise cancellation and atomic services/rules.

Changed areas: `apps/hydra-proxy/src/{transport,grpc,state,config,security,telemetry}`, Core `infrastructure/proxy_client`, domain/wire, `tests/e2e/relay.py`, example config.

## Phase 2 — accounts, audit and filters

SQLite WAL migrations, private directories/files, bounded writer admission/database/traffic budgets. CLI-only account creation with hidden password prompt. Argon2id passwords; Ed25519 JWT access60seconds; rotating refresh with absolute10-hour lifetime, family reuse detection, immediate revocation and equal full account privileges. Secure HttpOnly/Strict refresh cookie, Origin+double-submit CSRF for cookie operations. Authentication/session/user/audit UI and API, native HTTPS, embedded Swagger.

Real filter AST/operators/typed values, priority/TTL/missing-field semantics, shadow tests against saved messages before enforcement, author/editor derived from JWT, creation date, counters, edits and live toggles. Userspace rules enforce transport-safe TCP/UDP and completely buffered supported HTTP requests. Fixed-window field-keyed rate limits have1024 total keys, bounded windows and explicit capacity failure. Filter/service revisions conflict with409; ACK status is pending/applied/error.

Rust account/session tests passed, including exact lifetimes, refresh reuse, immediate revocation, cookie/HTTP2 split headers, permissions and server attribution. Engine tests cover rule semantics, aggregate regex/node/value budgets and rate windows/cardinality. `tests/e2e/http_filters.py` passed real auth, shadow/enforce/toggle, HTTP edge cases, attribution, counters, revision conflict and traffic persistence. Final rerun includes saved-sample parser test.

Changed areas: Core `modules/{auth,users,audit,services,filters}`, `infrastructure/{crypto,db}`, migrations1/2/5, filter engine/domain/wire, frontend feature pages and shared auth helper.

## Phase 3 — parsing, WASM, Traffic and critical telemetry

Native bounded HTTP/1.1 request parser plus raw fallback. Import-free WASM interpreter with checksum, fuel, memory, output, state, session/version bounds; real Rust u16-length-framed sample decoder. Immutable upload, live activation, restart, version retention and explicit parser testing against captured samples. Typed protobuf values, traffic SQLite index, flow/message views, pagination/search and authenticated bounded SSE. On-demand next-chunk sample ring:16KiB/sample,4MiB payload storage,16 pending requests,30-second expiry, UUID generations, audited capture/read and410 expiry handling. Configured-service diagnostics are bounded and accept no arbitrary destination.

Flag scanner handles per-direction TCP chunk boundaries and decoded fields; observed findings are own_exposed. Critical observations use finite durable replay/ACK, separate persisted epoch, explicit losses, Core durable flag/loss writes and epoch/sequence receipts before ACK. Normal rule counters reconcile separately across lossy telemetry.

Verified: real Rust WASM upload/activation/typed fragmented decode/malformed isolation/restart; scanner unit tests; real disk sample quota/eviction/expiry tests; seven critical-spool disk restart/ACK/corruption/quota tests; Core durable receipt/dedup/loss tests. `tests/e2e/diagnostics.py` passed sample capture,410, configured local echo probe, defensive flag cross-chunk detection, Core-down spool persistence and two-process restart replay. `tests/e2e/browser.py` passed native HTTPS login, cookie refresh after reload, actual feature navigation, authenticated API calls and logout.

Changed areas: SDK/runtime, sample plugin, Proxy inspection/samples/critical/diagnostics, Core traffic/protocols/critical receipts, migrations3/4/6/9, frontend Traffic/Protocols, integration tests/docs.

Limitations: no exposed generic declarative framing registry beyond raw/HTTP; no response HTTP parsing, chunked/HTTP2/WebSocket/TLS decryption. Unsupported framing passes unchanged with fallback metadata. Samples capture one requested next chunk, not a whole conversation. Critical producer admission has a finite crash window before its background fsync, with bounded loss rather than unlimited guarantees.

## Phase 4 — Farm, Flag Manager and Submitter

Target allowlist, immutable exploit uploads/version history, persisted manual/interval scheduling, enable/disable/edit/delete, optimistic revisions, run idempotency, cancellation/status/logs. One transient worker; Linux user/mount/PID/IPC/UTS/network namespaces, capabilities removed from worker, fixed-destination bridge and delegated cgroup CPU/memory/PID limits. Missing setup refuses execution. Scheduled lifecycle audit uses system actor with initiating user retained. Flags have captured/own_exposed/unknown categories, regex validation, hash/category dedup, provenance and redacted UI; only captured values are eligible for submission. Durable queue, stable external IDs, retries/backoff cap10 and real HTTP/TCP checker adapters. Checker credentials remain in Core configuration.

Actual SQLite/API tests passed for allowlist/revision, immutable upload, queue/cancel/restart, attribution, flag category/dedup/redaction and own-exposed submission refusal. `tests/e2e/operations.py` passed real APIs and local checker acceptance/persistence; unconfigured sandbox produces truthful failed runs. Earlier in the session, the opt-in real kernel suite passed within its own delegated scope (authorized local echo, filesystem/environment isolation, timeout/cancel, CPU/PID/memory limits). After the user's instruction to avoid cyber-risk tests, no further exploit-execution tests are run. New full Farm-to-sandbox-to-checker fixture is opt-in and has not been executed as an integrated acceptance claim.

Changed areas: Core `modules/{farm,flags}`, `infrastructure/{sandbox,operations}`, migrations7/8, Farm/Flags frontend, `tests/sandbox`, `tests/e2e/{operations,farm_execution}.py`, integration documentation.

Historical milestone limitations (superseded by the completion milestone below): sandbox adapter supports IPv4 TCP per-target jobs; multi-target batch and UDP execution fail explicitly. Checker adapters use plaintext HTTP JSON or TCP JSON-lines; no native HTTPS/custom/batch adapter. Redacted outcome summaries replace arbitrary raw checker replies. External exactly-once effects require checker idempotency support. Flag lists currently show newest100. Target/exploit list/run pagination is bounded. Full product-specific sandbox deployment still needs operator cgroup delegation.

## Phase 5 — deployment and resource validation

Real compact console pages for Services, Filters, Traffic, Protocols, Farm, Flags, Users/Audit. Memory-only browser access token and refresh coordination, secure rendering, error visibility. Build/run/backup docs, example operations config and two systemd units supplied. Units are examples requiring installation, certificate and operator configuration; no host services were installed or started.

Measured local echo acceptance under `systemd-run --user --scope -p MemoryMax=2G -p CPUQuota=200%`, with both app processes constrained to two CPU affinity slots: Proxy debug RSS19716KiB (19.3MiB), Core31036KiB (30.3MiB), combined49.6MiB. One-second idle samples after the short relay test: Proxy1.00% and Core0.00% of one core. Test elapsed2.59seconds. This is a constrained smoke measurement on the implementation host, not a sustained throughput/load benchmark or a physically2GiB machine. Runtime bounds are implemented; competition-scale latency, packet rate and sustained RSS remain to be measured.

Historical remaining scope (implemented in the completion milestone below): generic frame-profile editor, richer protocol-field/rule search, multi-target batch/UDP sandbox adapter, custom checker adapters and comprehensive resource/load benchmarks. These are not represented by placeholder endpoints.

## Earlier integration verification record

Current commands and logs are recorded during final integration. Build artifacts stay in ignored `target/`; temporary logs and matching official WASM sample toolchain are under `/tmp`. Reproducible ordinary commands are in README and CI. No publish/deploy/commit was performed.

Final integration verification (2026-10-16): workspace tests **85 passed, 0 failed, 1 ignored**; fmt and strict all-target Clippy passed; debug and optimized release builds passed. Release executables are approximately25MiB Core and7.4MiB Proxy. Release-generated OpenAPI exactly matches the active snapshot; protoc validation, architecture checker, frontend generation/diff and JavaScript/Python syntax checks passed. Saved-sample Rust WASM HTTP integration and native HTTPS Chromium browser checks passed after the final integration changes. Systemd syntax verification passed using temporary copies pointing to the built workspace binaries; units were not installed. No further exploit-execution checks were run after the user's restriction.

Optimized release resource smoke also passed under the same2GiB/two-CPU scope: Proxy8740KiB RSS (8.5MiB), Core14384KiB (14.0MiB), combined22.6MiB. One-second idle samples were Proxy0% and Core1% of one core; these coarse samples are not a CPU performance guarantee. The short relay acceptance took2.59seconds.

Final audit hardening: offline `hydra-core apply` now requires the configured private Core database and writes local_cli request/result audit records with the same request ID sent to Proxy. A local echo regression checks successful/idempotent/rejected maintenance changes retain audit provenance. Core was stopped for those maintenance operations; status remains independent of SQLite.

Post-hardening verification: strict Clippy and all85Rust tests passed again; both final release executables rebuilt successfully; active/release OpenAPI diff, architecture and generated frontend checks passed. The final release two-process local relay/offline-audit smoke also passed. All deployment changes remain reviewable files in the workspace.

## Completion milestone — remaining product surfaces

Implemented immutable native framing profiles (raw/delimiter/fixed-size/length-prefix), bounded typed field schemas and optional WASM metadata, live activation/deactivation with persisted manifests and idempotent ACK handling. Existing sessions retain decoder versions. Traffic Explorer adds service/time/transport/address/direction/exact-field/rule/flag filters, conversation details, message comparison and saved-field shadow rule creation. Exact detail lookup traverses bounded search pages. Bulk filter toggles preserve atomic revisions and per-rule attribution; the UI explicitly shows desired/applied enabled state.

Farm adds bounded fixed-endpoint IPv4 UDP, explicit multi-target batch manifests/provenance, aggregate deadlines, cancellation cleanup and immutable retry attempts. Flags adds paginated search, unknown triage, defensive-digest quarantine, verified HTTPS and declarative atomic CustomTcp batches, stable retry IDs and redacted response evidence. Users adds revision-checked editing, password reset, activation/deactivation and immediate session revocation. Audited settings/export and consistent request/error IDs complete the management surface. Production units separate Core and Proxy users while preserving exact UDS peer-UID authentication.

Changed files/areas: `crates/hydra-domain/src/{decimal,protocols}.rs`, protocol SDK/runtime framing/metadata, canonical `contracts/hydra/proxy/v1/proxy.proto`, Proxy protocol/gRPC/config/security modules; Core `modules/{users,auth,audit,services,filters,traffic,protocols,farm,flags,settings}`, API middleware, SQLite startup and sandbox/operations adapters; migrations0010–0014; frontend feature pages/generated contracts; deployment units/README; CI; safe integration/resource tests and related docs. Existing phase files remain organized by the repository dependency contract.

Commands: `cargo build --workspace --offline --locked`, `cargo test --workspace --offline --locked`, `cargo clippy --workspace --all-targets --offline --locked -- -D warnings`, `cargo fmt --all -- --check`, `cargo build --release --workspace --offline --locked`; generated `hydra-core openapi`, `scripts/check-contracts.sh`, `python3 scripts/generate-frontend.py --check`, JavaScript/Python syntax checks. The Rust WASM example is rebuilt with a matching isolated upstream compiler/target and exercised only as a parser. Local fixtures: `tests/e2e/{relay,http_filters,operations,diagnostics,browser}.py`. Operations checks configure no sandbox and verify refusal; they execute no uploaded code. Read-only systemd syntax checks use temporary unit copies; no services/accounts were installed.

Safe checks passed for Rust compilation, SQLite transitions, framing, schema quotas, account/session revocation, maximal bulk transactions, target validation, checker transports/TLS hostname verification, triage/quarantine and HTTP request audit. The ordinary suite deliberately ignores actual kernel exploit execution. The full live Farm-to-worker-to-checker acceptance path and new batch/UDP worker behavior remain unverified after the user restricted execution tests. Earlier TCP kernel results above do not validate these new paths. Operators must provision cgroup delegation and sandbox runtimes before use; absent prerequisites fail closed.

Other limits: HTTP inspection/enforcement covers supported HTTP/1.1 requests only; encrypted/unsupported protocols pass through. Checker atomic-batch acknowledgement and external idempotency depend on the configured checker. Configuration export covers services/rules, not a full live database backup. Short constrained-host resource runs do not demonstrate physical 2GiB hardware or long-term memory stability. Normal telemetry is explicitly lossy under load; critical delivery also has finite admission/storage bounds. No Docker daemon, Redis/PostgreSQL, eBPF, rounds or service monitor was introduced.

Final safe verification: **107 Rust tests passed, 0 failed, 1 kernel execution test ignored**. Strict all-target Clippy, fmt, architecture boundaries, protoc/handler-generated OpenAPI diff, generated frontend checks and JavaScript/Python syntax passed. All49 OpenAPI paths parse and local schema references resolve. Actual local HTTP/WASM/native-profile, relay/restart, diagnostics, fail-closed management/checker and HTTPS Chromium suites passed. Final test logs are `/tmp/hydra-final-{tests,clippy,http,relay,diagnostics,operations,browser,contracts}.log`; build logs use `/tmp/hydra-final-{build,release,wasm}.log`. The Rust WASM sample is an independent guest workspace artifact; no binaries/keys/database files are checked into source.

Final optimized build and release/OpenAPI diff passed. Binaries: Core27060664bytes (25.81MiB), Proxy7942024bytes (7.57MiB). The final 30-second owned-loopback benchmark passed under verified2GiB/200%-CPU scope limits and two-CPU affinity: 221030 successful requests, 0 payload errors, idleRSS24.66MiB, peakRSS28.74MiB, window growth+0.86MiB; Core received8469events with0indexing losses and Proxy dropped0normal events in this workload. See `docs/PERFORMANCE.md` and checked-in raw measurement JSON for CPU and per-second results. No full sandbox/exploit tests were rerun. No commit, publication or system installation was performed.

## Vue console migration — build and runtime integration
Core now embeds the Vite build output through `apps/hydra-core/build.rs` and serves exact asset URLs from memory. Build-time checks reject missing/stale bundles, symlinks/nonregular files, unsupported types and oversized output. Router tests cover the compiled module reference, embedded bytes, content types, cache/nosniff headers, unknown assets and encoded traversal. CI installs pinned Bun, typechecks/tests/builds Vue before Cargo, and includes the native HTTPS browser smoke. Build/runtime instructions are in `README.md` and `docs/vue-frontend.md`.

The browser harness checks actual Vue mounting and compiled hashed JavaScript retrieval, dispatches input events for Vue bindings, and can temporarily hide dist with `HYDRA_VERIFY_EMBEDDED=1` before Core starts. Python syntax compilation passed. Full Rust/frontend/browser validation is recorded by the enclosing migration verification after all feature sources and the final bundle are ready. This migration does not rerun kernel or exploit execution tests. Earlier resource figures describe the previous console build; new measurements follow below.

Native Vue pages now cover Services, Filters, Traffic, Protocols, Farm, Flag Vault, Users/Audit and Settings, preserving real operations, revisions/ACK status, schemas/captures, triage/retries and export. The shared TypeScript client retains memory-only access tokens, serialized refresh rotation, CSRF, bounded SSE and request references. Legacy DOM-rendered JavaScript was removed. Small feature components use generated OpenAPI DTOs and official local shadcn-vue primitives. App.vue is a thin shell.

Review corrections: stale editor-save completions cannot alter newer dialogs; closing/logging out clears editor data/callbacks; keepalive comments do not announce traffic; Core checks `.env*` and root-directory deletion/rename timestamps before embedding. An independent follow-up review approved the fixes. TypeScript5.9.3 is pinned for vue-tsc compatibility. Bun1.4.3/Node22.23.3 are build-only tools; deployment still runs only Core and Proxy.

Changed areas: `frontend/` Vue/TypeScript/Vite config, Bun lockfile, features/auth/shared components and generated operations; `apps/hydra-core/build.rs` and API static serving; frontend generator, CI, browser/freshness tests, README/decisions and Vue/performance documentation. Public REST/OpenAPI and protobuf contracts are unchanged. No unrelated Rust feature modules changed.

Verified: frozen Bun install; `bun run typecheck`, `bun run test` (**9 passed/29 assertions**), `bun run build`; `cargo test --workspace --offline --locked` (**109 passed,0failed,1kernelexecutiontestignored**), strict all-target Clippy, fmt, architecture and generated OpenAPI/frontend diff checks; `python3 tests/compatibility/frontend_build.py` (**15 isolated edit/delete/rename cases**); debug/release builds. `HYDRA_BUILD_PROFILE=release HYDRA_VERIFY_EMBEDDED=1 python3 tests/e2e/browser.py` passed actual Vue mount/hashed assets, login/reload restoration, loopback service creation, every page and logout with dist absent, then restored files. Test fixtures wait for Vue input/loading updates before assertions. No actual exploit, worker or outbound target execution ran.

Final bundle: **20files/395553bytes** (about386KiB), embedded at compile time; no CDN or asset directory needed at runtime. Final release Core27415224bytes, Proxy7942024bytes. A15-second resource check under2GiB/200%-CPU quota passed **100664requests/0errors**, idleRSS24.36MiB and peakRSS28.06MiB; CoreCPU23.70%/ProxyCPU35.97% ofoneCPU. Coreindexed4020events with0indexinglosses; Proxyreported0normaldrops inthisworkload. ComparedRSSwindowsgrew+0.38MiB. Scope-on-larger-host and short-run limitations remain; see `docs/PERFORMANCE.md`. Logs are `/tmp/hydra-vue-*.log`; raw resource samples are checked into `docs/benchmarks/`. Runtime deployment units need no changes.

## Completed TCP stream history — 2026-10-17

Implemented default-on per-service retention for completed TCP flows, a bounded 64 KiB-per-direction tail with gzip-at-rest persistence, 24-hour expiry and 100 MiB compressed quota, active enable/disable, the audited Core history endpoint, and closed-session directional rendering (hex default, UTF-8/ASCII selectable). Continuous gRPC telemetry still contains parsed events only; raw history is fetched only by known flow ID. Updated `SPEC.md`, `DECISIONS.md`, decoder/deployment documentation, and `tests/e2e/relay.py`.

## UTF-8 conversation repair — 2026-10-17

The reported Ark flow `09c1e3a4-a4b6-414e-aa2b-97f9742c2e71` used the UTF-8 profile and retained all 205 client bytes and 671 server bytes in gzip history, but had no parsed messages. TCP and UDP inspection selected only WASM/framing decoders for non-HTTP streams, so the built-in UTF-8 decoder was never fed. Inspection now includes UTF-8, with independent incomplete-code-point buffers for each direction. Existing closed flows without parsed messages use their saved history as a directional UTF-8 display regardless of current service profile; raw still opens as hex. Saved history is grouped by direction and does not reconstruct packet timing or protocol fields.

Changed files: `apps/hydra-proxy/src/inspection/protocols.rs`, `apps/hydra-proxy/src/transport/{tcp.rs,tcp_tests.rs,udp.rs}`, `crates/hydra-protocol-runtime/src/utf8.rs`, `crates/hydra-protocol-runtime/tests/decoders.rs`, `frontend/src/features/traffic/{FlowConversation.vue,traffic.ts}`, `frontend/tests/traffic-history.test.ts`, and `tests/e2e/relay.py`. Public contracts and database schema are unchanged.

Verification: the TCP inspection regression first reproduced zero events, then passed with both directional text events. The direction-isolation regression first reproduced a corrupted server prefix, then passed. Commands: `cargo test -p hydra-proxy --lib` (23 passed), `cargo test -p hydra-protocol-runtime` (24 passed), `cargo clippy -p hydra-proxy -p hydra-protocol-runtime --all-targets -- -D warnings`, `cargo fmt --all -- --check`, frontend `bun test` (29 passed), `bun run build`, `cargo build --locked --release -p hydra-core -p hydra-proxy`, and `HYDRA_BUILD_PROFILE=release python3 tests/e2e/relay.py` (passed). The Proxy tests initially hit sandbox EPERM for two localhost binds; rerunning with localhost binds permitted passed all 23. The isolated two-process test verifies byte integrity, UTF-8 messages through gRPC into the Core traffic API, and saved history using only temporary loopback services and a temporary database. Loopback RSS: Proxy 9,288 KiB, Core 15,348 KiB, two-CPU affinity; a one-second idle sample was 0% of one CPU for both. This is a smoke measurement, not a throughput or 2 GiB memory-cap benchmark.

## Farm workflow and gzip logs — 2026-10-17

Stdout/stderr are displayed separately, updated live, and stored as gzip BLOBs. The worker keeps draining output beyond the1MiB/stream retained limit and marks loss. Cancellation, interruption and late runner failures preserve previously persisted gzip output. Old completed plaintext logs convert incrementally in batches of64. Rotation excludes active work and limits completed history to7days,1,000records and64MiB of compressed output. Logs, runtime status, exit codes, target labels, cancellation and retries are usable from the UI. Every catalog mutation, run action, diagnostic log read and rotation is audited. The existing sandbox remains an explicit alternative; the user's host-execution amendment is recorded in SPEC.md/DECISIONS.md.

Changed files/areas: Core `modules/farm/{catalog,catalog_http,catalog_tests,completion,packages,retention,runtimes,http,service,scheduler,tests}.rs` and `mod.rs`; `infrastructure/{host_process,operations,mod}.rs`, Core bootstrap runner wiring and Cargo dependencies/lock; Flags catalog-team provenance validation; migration0016; generated OpenAPI/frontend contracts; Vue Farm catalog, exploit form, DTO projections, run actions/log display/pagination/polling and tests; `tests/e2e/farm_host.py`; deployment scripts/README and `docs/farm.md`. Earlier traffic-decoder fixes in this worktree were preserved. Untracked `ad-test/` was not changed.

Operational limits: host mode shares Core's OS privileges/network/filesystem permissions and service-wide limits; manifest per-job CPU/memory controls apply only to the sandbox runner. Host cancellation kills the child process group. Source uploads remain bounded/immutable. Initial uv package resolution needs registry access; unpinned dependencies follow uv resolution. Historical logs removed by retention cannot be retried or recovered through Farm. Reload the browser to obtain the new embedded Vue bundle. Full usage, API additions and compatibility notes are in `docs/farm.md`.

Implemented Farm-output regex extraction into a durable deduplicated submission queue, independent of the legacy auto-submit switch. Added editable/persisted Python source (paste/file), optional uv dependencies, revision checks, live enable/pause, a durable 30-second collection clock, bounded 240-flag/60KiB calls, whole-batch exit-code acknowledgement, retries (at least 30 seconds, max 10 attempts), and interrupted-claim recovery. Configuration edits prevent subsequent chunks using an old revision. Own-exposed digest quarantine remains enforced. Submitter output is redacted and gzip compressed (64KiB per stream), with 7-day/500-record rotation, including periodic cleanup when paused. The UI shows queue count, next window, last batch result, stdout/stderr and exit code; polling preserves unsaved code.

Changed/new implementation files: `modules/flags/{mod,http,repository,submitter,script_config,script_http,script_dispatch,script_tests}.rs`; `infrastructure/{operations,submitter_process}.rs`; Farm package-validator export; migration 0017; Vue `features/flags/{FlagsPage,SubmitterForm,submitterDto}` plus DTO tests and the Farm link; generated OpenAPI/frontend contracts; `tests/e2e/python_submitter.py`. Documentation: `SPEC.md`, `DECISIONS.md`, `docs/farm.md`, deployment report. Existing earlier Farm/traffic work remains intact; `ad-test/` was not modified.

### 2026-10-17 — Reinstall after the operator reset the A/D environment

Post-smoke 2-second sample: Core 36,524 KiB RSS/Proxy 7,240 KiB RSS, combined 42.7 MiB; 0% CPU registered at this sampling resolution. Guest 2 vCPU/3915 MiB RAM, 996 MiB used/2919 MiB available with games. Detailed commands, files, measurements and limitations are in the reinstall report. The operator credential is generated/stored only root-private on the VM and handed directly to the user. Old erased state cannot be restored from this fresh install; submission remains paused pending the actual user-supplied checker script.

### 2026-10-17 — Group removal and Docs filter enforcement

### 2026-10-17 — Usable saved-traffic test and enforcement flow

### 2026-10-17 — Immediate filter activation (explicit user policy change)

### 2026-10-17 — Blocked traffic visibility, flag highlights and service colors

### 2026-10-17 — Exact flag submission, manual controls and browsing

### 2026-10-17 — Readable flags and explicit failed resubmission

### 2026-10-17 — Submission I/O, streamed Farm logs and process flag counts

### 2026-10-18 — Run summary columns and modal logs

## Accounts, username labels and editable file-backed settings — 2026-10-18

Explicit user amendment enables authenticated same-origin account creation in UI/API with equal full privileges and preserved CLI bootstrap/recovery. Shared bounded validation/hashing and actor-derived audits back the real endpoint. A reactive authenticated directory displays usernames for authors, editors, initiators and audit actors, including nested provenance; captured values and technical resource/request IDs are unchanged. Farm runs use one New / duplicate column.

Verification so far: `bun run build` and `bun test` (48/48); `cargo test --locked -p hydra-core --test identity` (13/13); Settings module tests (6/6); Core library Clippy with `-D warnings`; `cargo fmt --all -- --check`; `scripts/check-contracts.sh`; `python3 scripts/generate-frontend.py --check`; `python3 scripts/check-architecture.py`; `python3 tests/e2e/browser.py` (real HTTPS account creation and persisted settings); `python3 tests/e2e/relay.py` (TCP/UDP integrity and independent restart), deployment Bash/Python syntax checks. Focused review found no outstanding material issue. Local relay smoke measured Core38,308KiB / Proxy22,148KiB RSS and1%/0% idle CPU with affinity2; this brief fixture measurement is not a production throughput benchmark.

Final release `CARGO_BUILD_JOBS=2 cargo build --locked --release -p hydra-core -p hydra-proxy` passed (2m38s). `HYDRA_BUILD_PROFILE=release HYDRA_VERIFY_EMBEDDED=1 python3 tests/e2e/browser.py` passed with frontend/dist removed during runtime, including username author/editor labels. Release OpenAPI equals checked-in snapshot. Core SHA256 f932f0e8f655fefba7bd364d6b9c0c97025fc1818c2425380e230dda935cf382; Proxy42419cd3aedb98e7470578c752c7cadbed27aeeb8bf00972744a732781e237d3.

Remaining behavior: saved operations changes require Core restart; fixed JWT lifetimes and worker concurrency are read-only. Account names resolve current names, and unavailable identities show Unknown user. CLI remains required for the first account/recovery; every subsequently created UI account has full equal rights.

## Uploaded WASM selection from Services — 2026-10-18

Services now loads the installed protocol catalog and current per-service activations. The searchable Capture decoder field lists built-ins and installed immutable WASM/framing versions; saving a new or existing service applies the selection. The table/editor display the actual override. Returning to a built-in removes that override. Bounded ACK waiting handles new listener creation before activation; partial activation failures retain service ID/revision for retry without duplicate creation. Failed catalog loads prevent unsafe saves, and rejected modules are excluded. Existing Protocols upload/activation lifecycle, audit and stream version pinning remain unchanged. Service creation adds the created ID to its compatible snapshot response so shared TCP/UDP listener addresses cannot misdirect activation; retries refresh activation state after partial failures.

Changed: Vue Services page/form plus focused decoder mapping/save helper and tests; real-browser upload/select/switch/create coverage; decoder guide and decisions. Verification/deployment evidence follows.

Verification: `bun run build` and `bun test` passed50/50; `cargo test --locked -p hydra-core --lib modules::services::tests` passed6/6; debug build of both binaries passed; expanded `python3 tests/e2e/browser.py` passed against real Core/Proxy and a real ABI WASM upload. It checks installed option visibility, editor/table selection, activation on an existing service, built-in removal, new service activation, shared TCP/UDP listener address targeting, and response-loss recovery after actual Proxy activation. One test timing race was corrected by waiting for the existing Services reload to enable Add service. Core library Clippy with `-D warnings`, generated contracts/frontend checks, architecture, formatting and diff checks passed. Focused review findings on exact service identity and stale retry state were fixed and re-reviewed. Release/deployment evidence follows.

Release `CARGO_BUILD_JOBS=2 cargo build --locked --release -p hydra-core -p hydra-proxy` passed(2m37s), followed by `HYDRA_BUILD_PROFILE=release HYDRA_VERIFY_EMBEDDED=1 python3 tests/e2e/browser.py` with the complete real-WASM workflow and dist hidden. Release OpenAPI matches checked-in snapshot. Core SHA256 ae6fbc3c1f5d011326d8ee799fad0e0f147c7c687c87c0410d861353ccf5a005; unchanged Proxy42419cd3aedb98e7470578c752c7cadbed27aeeb8bf00972744a732781e237d3.

Changed plugin build/schema files/Cargo lock, bounded session decoder and real-WASM tests, operator/plugin documentation, real browser typed-response/CLI-hotload test, and `scripts/deploy-protocol-remote.py`. The module deploy helper keeps credentials on the VM, checks installation/activation, and restores the prior confirmed activation on failure. Verification and live deployment evidence follows.

## GitHub Actions diagnostics regression — 2026-10-18

Run39100264600 failed in tests/e2e/diagnostics.py before static binary packaging: the test still required a stored own_exposed flag, despite defensive value storage having been removed by the product requirement. Reproduced the obsolete assertion locally. Updated the real two-process crash/spool/restart test to require retained traffic flag-presence metadata, NULL flag references and empty flags/submissions instead. No production code or deployment artifact changed.

Verification: HYDRA_BUILD_PROFILE=release python3 tests/e2e/diagnostics.py passes (0.518s); git diff --check passes. The test needs localhost sockets and was run outside the restricted sandbox. CI rerun result will be recorded separately.

Follow-up run39102673441 passed diagnostics and exposed a browser assertion race at browser.py:446. The length-prefixed fixture emits protocol.payload for both directions, so waiting for that field anywhere could complete on the request before the server event rendered. Scope the bounded wait to a server-direction article containing the expected decoded field; retain the decoded-view and response assertions. No application/deployment artifact changed.

## Proxy CPU: indexed gzip history writer — 2026-10-18

Implemented a single-owner ordered index rebuilt once at startup, incremental compressed-byte accounting and direct oldest/expired eviction. Capture/filtering/WASM/forwarding remain enabled; disk format and API/protobuf unchanged. Index memory and small-file growth now have a65,536-record cap alongside24-hour TTL/100MiB compressed payload quota. Extracted disk persistence/recovery into apps/hydra-proxy/src/history/storage.rs; mod.rs retains active capture/queue orchestration. Added five targeted regressions, a real-Proxy synthetic history benchmark, before/after JSON, operator/resource docs and scripts/redeploy-proxy-remote.sh with hash verification/readiness/backup/rollback.

Red/green: appending_history_does_not_reopen_unrelated_metadata failed on the old writer and passes with the index. `cargo test --locked -p hydra-proxy` passes34unit+7integration tests; socket tests need execution outside the restricted sandbox(two initial sandbox socket denials were rerun successfully). `cargo clippy --locked -p hydra-proxy --all-targets -- -D warnings`, `cargo fmt --all -- --check`, canonical protoc generation, unchanged release OpenAPI diff, architecture checks, Bash/Python syntax and diff checks pass. Release build: `CARGO_BUILD_JOBS=2 cargo build --locked --release -p hydra-proxy`(1m28s). Release relay TCP/UDP/history recovery, HTTP/filter/WASM lifecycle and diagnostics crash-replay tests all pass. Embedded real Chromium smoke passes(5.671s) on a diagnostic rerun; an initial run encountered transient Failed to fetch during service creation, not reproduced on rerun.

Controlled synthetic `python3 tests/performance/history.py --records 10000 --flows 20` on two-CPU affinity: Proxy CPU2.04s→0.02s(102→1ms/stream); average96.16%→1.88%; elapsed2.12→1.06s including50ms pacing and full writer drain. RSS9,515,008→10,108,928bytes. All binary echoes and all20gzip histories verified both times. Fixture excludes seeding/startup/Core/generator from process measurements and imposes no RAM cgroup limit; this is not maximum throughput or nginx parity. Raw measurements: docs/benchmarks/history-index-release-2026-10-18.json.

Post-deploy15-second live sample:0.18CPU seconds(1.20% of one core),16,580,608bytes RSS,0read syscalls in that window. Traffic is uncontrolled and varies, so the synthetic result is the comparable CPU-cost evidence. VM currently reports2CPUs/3915MiB total memory; this live measurement is not a physical2GiB benchmark. Remaining limits: one startup scan, gzip/decoder costs under heavy traffic, loss under bounded queue overload, expiry/count/quota evictions and no retrospective parser replay.

## CPU-aware shared Proxy pools — 2026-10-18

User requested scaling across available CPUs without per-task threads. Proxy previously hardcoded two Tokio workers; bootstrap now uses Rust available_parallelism and explicitly limits the separate blocking pool to min(CPUs,4). Startup reports actual async/blocking pool sizes. Connections remain async tasks, per-flow parsing remains ordered, and the bounded history catalog retains a single owner. No API/protobuf/storage/settings changes.

Changed apps/hydra-proxy/src/main.rs, added tests/e2e/proxy_runtime.py to CI, and documented pool/housekeeping/systemd limits in PERFORMANCE.md and DECISIONS.md. Test first failed against the old binary's absent CPU-aware runtime diagnostics. Both debug and release tests now verify128 real simultaneous TCP sessions,8,192 binary exchanges per affinity and a total OS-thread budget independent of connection count. Release results: affinity[0]→1async worker/4total threads; affinity[0,1,2]→3async workers/6total threads. This concurrency check is not a throughput benchmark.

Validation: Proxy Clippy all-targets with -D warnings, formatting, dependency boundaries, Python syntax and diff checks pass. `cargo build --locked -p hydra-proxy` and `CARGO_BUILD_JOBS=2 cargo build --locked --release -p hydra-proxy` pass(release46.75s). Release CPU-aware runtime, TCP/UDP/retained-history restart, diagnostics crash replay and HTTP/filter/WASM lifecycle scripts all pass. Socket integration runs require execution outside the restricted sandbox.

## 2026-10-19 — effectiveness matrix, UDP retained history, filter cards

## 2026-10-22 — nc-style terminal stream playback

## 2026-10-22 — terminal direction colors

## 2026-10-22 — decoder filter field rejection

## 2026-10-22 — filter match direction

## 2026-10-23 — per-service compressed decoded retention

## 2026-10-23 — direction for every condition row

## 2026-10-23 — HTTP viewer and Services UX

