# Decisions and contract audit

## 001 — Implement in the supplied workspace
The handoff files are all untracked and there is no existing executable code or commit to isolate. Work stays in the supplied directory so the original specification remains available. No existing documents are removed or committed without instruction.

## 002 — Contracts describe implemented features
The original REST draft advertises the complete future product. Preserve it verbatim as `contracts/openapi.future-draft.yaml`; `contracts/openapi.yaml` is now generated from Core's actual annotated handlers. This intentionally removes unimplemented paths from the active snapshot before public adoption. The draft's login/logout, filter and farm schemas remain reference material, not functionality claims. The legacy notes incorrectly mention roles in JWT validation; SPEC.md's equal account rights wins. REST arbitrary JSON values and protobuf typed values will need explicit numeric/bytes mapping in Phase 2/3.

The canonical protobuf compiles without changes to existing field numbers. Additive status fields 9/10 report rejected flows and dropped UDP datagrams. Diagnostics is not registered; rule/plugin/critical-stream RPCs in the draft return explicit UNIMPLEMENTED until their phases. They are not completed features. Keep the package v1 and preserve existing fields for subsequent phases.

## 003 — Bounded raw relay first
Phase 1 accepts numeric IP:port endpoints and the `raw` profile only. Unknown bytes (including encrypted streams) pass unchanged; no false parsed-message claims. Domain validation rejects duplicate IDs/listeners, zero ports, unspecified/multicast upstreams, and direct relay loops. Bootstrap config is operator-controlled; the authenticated local Core is the service configuration authority. There is no probe or arbitrary packet injection API.

Services <=128, global TCP+UDP flows 1..8192 (default 1024), config/state/gRPC control payload <=256 KiB, TCP buffers 16 KiB per direction, UDP datagrams <=65535 bytes with no per-client send queue, telemetry ring 1024 events, four subscribers with 16 queued batches each, 32 local gRPC connections and 16 streams per connection. UDP overload is lossy and counted; mappings expire on bidirectional inactivity. TCP read/write idle timeout is per direction, including after half-close. Connection rejection and all telemetry/datagram losses are observable through status. Normal flow events are lossy metadata, with no payload capture.

## 004 — Last-good snapshots and local authorization
Prepare every added listener before persistence or live mutation. Reuse existing bound endpoints when upstream changes; accepted TCP connections retain their old upstream. UDP mappings reset when that listener's config changes. Persist a bounded snapshot plus actor/request provenance via 0600 create-new temporary file, fsync, rename, and directory fsync before ACK. Exact same-revision replay is idempotent; older or conflicting same-revision snapshots fail. A restart prefers persisted state over bootstrap configuration. Corrupt state fails startup, never silently resets. Local config directories must be provisioned by the operator.

Unix socket mode is 0600 and peer UID is checked (default Proxy effective UID). Same service UID is supported initially; separate service UIDs require an explicit operator-managed socket ACL plus `allowed_core_uid`. Only an owned stale socket is replaced; an active socket, non-socket or foreign socket is refused. Phase 1 provenance is stored for the last snapshot and logged; it is not the append-only SQLite audit trail required in Phase 2. Management REST is loopback-only until authentication exists.

## 005 — Build and resource evidence
Use tested Rust 1.99.0, edition 2024; Cargo.lock pins dependency resolution. Two build jobs and two runtime worker threads per binary. Swagger assets are embedded from the vendored crate, without a deployment-time Node runtime or CDN. Host measurements are reported with conditions; they are not validation of a 2-vCPU/2-GiB target unless run on that target. SQLite/WASM/filter/frontend crates are added only with their real phase implementations.

## 006 — Implemented control plane supersedes initial phase notes
The Phase 1-only statements above describe that milestone. Core now owns Argon2id/Ed25519 authentication, 60-second access tokens and rotating absolute10-hour refresh sessions; append-only SQLite audit; services/filter CRUD; traffic indexing/SSE; protocol upload/activation; Farm and Flag Manager APIs. The SPA is small native ES modules embedded in Core, with generated OpenAPI endpoint helpers and TypeScript declarations. No deployment Node runtime is needed. Native rustls HTTPS avoids requiring a third always-running process. REST numeric parsed fields represent int64/uint64 as strings to preserve browser precision.

Only Core owns SQLite. The service/filter desired snapshot is one transactional aggregate in the service repository to preserve cross-feature references. Filter HTTP handling remains a separate vertical module. Core serves only loopback, with explicit HTTPS origin and secure refresh cookies. UDS actor metadata is attribution; Unix peer UID is the credential.

## 007 — Contract compatibility and bounded payloads
Active OpenAPI is generated from annotated handlers and diff-checked; the original future draft stays preserved. Canonical protobuf adds atomic `ApplyConfigSnapshot` (service and rule revision/provenance agree), status counters9..13 (connection/datagram/rule/critical/sample losses), explicit `CaptureSample`, and saved-sample `TestProtocol`. Existing field numbers and methods remain. ProxyDiagnostics, plugins and critical streams are now implemented. No generated source is checked in. Saved service/rule state is capped2MiB; bootstrap JSON256KiB; module uploads4MiB, decoded messages64KiB, admission limits and per-method message bounds apply separately.

## 008 — Parsing and enforcement
Raw remains transparent. Native HTTP supports complete strict HTTP/1.1 requests, not responses, chunked, upgrade, HTTP2 or TLS decryption. Buffers cap64KiB, headers16KiB, bodies49152bytes, pending request deadline5seconds. Complete valid prefixes are evaluated before fallback for malformed suffixes. Unsupported framing falls back unchanged, with diagnostic metadata; Expect is passed through to avoid100-continue deadlock. Blocking a later pipelined request closes the connection rather than injecting an out-of-order response.

WASM is import-free, interpreter-hosted, checksum-validated, fuel/memory/output bounded. Existing sessions retain versions; eight guest sessions and sixteen installed versions cap memory. SDK is independent of binaries. ABI1 exchanges bounded JSON in guest memory; internal gRPC transfers typed parsed events. Unknown original message size is0. Native generic declarative framing profiles beyond raw/HTTP are not yet exposed; the real Rust WASM example implements u16 big-endian length framing.

## 009 — Critical replay and explicit samples
Normal metadata is lossy; cumulative rule hits reconcile separately. Defensive flags use a2MiB live durable spool with another2MiB atomic-write budget and bounded64-command admission, separate from normal events. The spool keeps its own persisted epoch across process restarts; status process epoch still identifies restarts. ACK follows durable flag/loss persistence plus Core epoch/sequence highwater. Sixteen historical spool epochs are bounded and fail closed on exhaustion. Admission is not fsync: a crash can lose unflushed queued observations. Exhaustion/loss is visible, never unlimited guaranteed delivery.

Raw samples record the next chunk only after an authenticated audited request for a live flow. Sixteen pending captures, sixteen queued writes,256 disk slots of16KiB and30-second expiry cap storage4MiB. UUID generations expire across restart. REST410 represents pending, expired, evicted or absent samples. Samples are separate on-demand diagnostics, never a raw stream. Configured-service probes cap request/reply4096bytes, concurrency1 and5seconds; no destination address is accepted from HTTP.

## 010 — Farm and submission adapters
Immutable exploit uploads are bounded2MiB; one transient worker executes numeric allowlisted targets. Linux bubblewrap namespaces plus delegated cgroup v2 CPU/memory/PID controls are required; missing setup fails without unsandboxed fallback. Current networking adapter supports IPv4 TCP and per-target execution; multi-target batch and UDP jobs are explicitly rejected. The supervisor owns a fixed-destination Unix bridge; workers have no host network route or submission credentials. Scheduled transitions use system actor with the initiating user preserved.

Flag matching is operator-configured, at most256bytes per flag. Defensive findings are own_exposed and never eligible for submission. Captured/manual findings require an enabled explicit target. Hash/category deduplication, durable queue, stable external idempotency IDs, at most 10 attempts with backoff are real. HTTP JSON and TCP JSON-lines adapters are bounded plaintext protocols for trusted local endpoints/tunnels. Custom/batch/native HTTPS checkers remain unimplemented. Raw checker replies are replaced with redacted outcome summaries to avoid echoing flags/credentials; this is an intentional privacy tradeoff from preserving arbitrary raw response bodies.

## 011 — Fixed-window rate limits
`rate_limit` is implemented with key, requests, window_ms and transport-valid on_exceed. Snapshot-wide cardinality is1024, key values scalar and<=256bytes, window50ms..1hour. Missing keys do not match. Capacity exhaustion follows on_exceed and emits explicit loss metadata. Shadow evaluations update counters without enforcement. A rule snapshot replacement resets rate windows. Units are inspection decisions: complete supported HTTP messages, UDP datagrams and raw TCP connection/read decisions; this is not byte shaping or a bandwidth guarantee. Prefer safe HTTP message predicates for request-rate limits.

## 012 — Offline maintenance audit
The `hydra-core apply` recovery command is a Core-owned CLI mutation, so it opens the configured private Core SQLite database and writes intent before the Proxy call plus result afterward, with matching local_cli UID/request identity. It cannot bypass audit merely because the HTTP daemon is stopped. It remains an offline recovery tool: a direct Proxy snapshot can diverge from Core's desired revision and needs an operator reconciliation through Core; ordinary service changes belong to REST/UI. Read-only CLI status does not require SQLite.

## 013 — Completion of previously deferred surfaces
The earlier phase limitations record historical delivery boundaries. Generic raw/delimiter/fixed-size/length-prefix profiles are now immutable uploaded versions using the same tested activation mechanism as WASM. Additive protobuf InstallProtocol fields7/6 carry framing JSON/schema metadata; optional WASM hydra_metadata exports preserve ABI1 compatibility. Additive DeactivateProtocol restores the configured service parser for new flows while existing sessions retain their decoder. Completed idempotency keys cannot remove a later activation. Native and WASM schema metadata stays bounded and is validated once at install.

Farm now implements per-target/batch IPv4 TCP/UDP fixed endpoints, aggregate deadlines, explicit batch target provenance and immutable retry attempts. Duplicate endpoint aliases in batch are rejected. Runtime directory guards clean up on cancellation after worker group termination. These new worker paths were not executed after the user's restriction on exploit-execution tests; pure framing/validation and SQLite checks are separate evidence from kernel enforcement.

Checker adapters now include verified HTTPS with an operator-pinned DER CA and declarative CustomTcp atomic batches. Endpoint and credentials remain operator-only; no adapter executes code. Bounded redacted response bodies/status preserve outcome evidence. Unknown triage requires explicit authorized target provenance. A defensive observation quarantines pending matching-digest submissions; an already-sent external request cannot be recalled. External idempotency remains a checker responsibility.

## 014 — HTTP provenance, typed values and operator controls
All mutating API dispatches retain a correlated durable intent/result audit, including rejected operations, in addition to transactional feature audit. Unauthenticated attempts use a service observer identity. Timeout/overload errors retain JSON error shape and request IDs. Results cannot turn multi-process operations into atomic transactions; callers must check persisted operation state after timeout. Sensitive settings/configuration export reads are audited and contain no signing/checker secrets.

Users can edit/reset/deactivate/reactivate any account with optimistic revisions; creation initially remained CLI-only (superseded by the authenticated creation amendment below). Password changes and deactivation revoke sessions. Bulk filter changes validate the entire bounded selection and commit one snapshot revision; each selected rule retains before/after audit. Saved-field creation reads the exact typed value from Core storage, creates a shadow rule, and still requires testing before enforcement.

Parsed int64/uint64 REST values now serialize as decimal strings, including rule snapshots; readers accept previous numeric JSON for stored-state compatibility. Protobuf still uses native signed/unsigned64. Existing rule edits may require repeating the saved-traffic test because canonical fingerprint representation changed. No protobuf fields were removed or renumbered. Migrations10–14 are ordered and checksum-verified by Core only.

## 015 — Separate deployment identities and measurements
Production units now use separate hydra-core/hydra-proxy identities with a shared control group, explicit mode0660 UDS and exact configured Core peer UID. Core state remains private to its UID; Proxy does not gain database/key access. Development retains mode0600/same-UID defaults. No host account or systemd installation was performed.

A reproducible loopback benchmark records actual cgroup limits, app CPU affinity, RSS/CPU, verified payloads and telemetry loss. A two-CPU/2GiB scope on a larger machine is documented as constrained-host evidence, not a physical target-machine claim. Short runs cannot establish long-term memory growth guarantees. See docs/PERFORMANCE.md.

## 016 — Vue bundle embedded by Core
The requested Vue 3/TypeScript console replaces the hand-maintained browser JavaScript while retaining the existing HTTP contracts and two-process deployment. Bun/Vite are build tools only. A frontend build produces ignored `frontend/dist`; Core's Cargo build embeds its document and hashed assets through a generated `OUT_DIR` map. Cargo deliberately does not install dependencies or run Vite. Missing or older bundles fail with an explicit frontend build command. Freshness compares input/output modification times rather than introducing an additional hashing dependency; preserved timestamps require the documented clean frontend rebuild discipline.

The asset tree rejects symlinks/nonregular files and enforces 256-file/32-MiB build limits. Runtime serving performs exact embedded URL lookup with MIME/nosniff headers, immutable asset caching and a revalidated root document. Hash routing preserves refresh/navigation without a catch-all HTML response for unknown paths. No Rust application dependency direction, public API field or protobuf method changes for this migration. The opt-in browser smoke hides the dist directory while exercising the actual binary, proving frontend deployment files are unnecessary.

## 018 — Session-first traffic workspace
The requested primary view is now a service dropdown above a left-hand TCP session list, with selection opening a Wireshark-style stream pane on the right. Decoded directional messages and explicit ASCII/hex byte samples use authenticated endpoints. No continuous raw packet telemetry or fabricated wire reconstruction was added. The old table/JSON-first presentation is replaced while field inspection, comparison, capture and saved-field filter creation remain available.

The existing traffic query gains optional `newest_first`, default false. True sorts by descending ingestion sequence and treats `after` as an exclusive older-than sequence. Existing default ordering/cursors remain compatible; callers must preserve query order between pages. The sidebar opts in so current sessions appear first and older pages remain stable when new connections arrive. This is an additive OpenAPI change, with no protobuf or database migration. UI request generations, bounded previews and cleanup prevent stale service responses or polling tasks from crossing session/navigation boundaries.

## 019 — Passive native HTTP responses in the conversation

Native HTTP request filtering previously bypassed server-direction decoding entirely. Add independent bounded response state to the native decoder and explicitly feed it from the TCP return path, including EOF for close-delimited replies. Failure disables only response inspection, preserving pending client bytes and request enforcement. Support HTTP/1.0 and 1.1 response status, Content-Length, chunked transfer, HEAD, informational/no-body statuses and connection-close framing. Response inspection holds at most 64 KiB per flow, with at most 128 outstanding request methods; unsupported/oversize responses keep forwarding unchanged and emit a decoder error. Plain UTF-8 text/JSON/XML bodies expose at most 4096 bytes as a decoded `http.body_text` field, with body length, content type and explicit preview truncation; compressed/binary entities are not rendered as text. No arbitrary headers (especially cookies/auth) or raw packet streams are exported. This is passive inspection, not safe response-block enforcement. Existing parsed-event wire/API contracts need no schema change; the native field registry gains additive typed response fields. Historical unindexed responses cannot be reconstructed.

## 020 — Live parsed traffic, complete bounded HTTP headers, and app port mapping

The Traffic page consumes authenticated Server-Sent Events from Core for committed parsed flow/message/loss events. Core publishes only after durable SQLite commit; the stream carries parsed events, never raw packet bytes. On reconnect, the UI reconciles from the REST message API and keeps the newest 500 conversation messages; events remain bounded by existing parser, queue, and SQLite limits. A `ready` frame flushes the HTTP response immediately. Live flow arrivals appear in the session list without polling.

For supported HTTP/1.x messages, initial headers and chunked trailers are now retained as ordered typed name/value pairs, preserving duplicate fields, original casing, and whitespace, and displayed in Traffic. Each initial header/trailer section remains capped at 16 KiB/128 fields, text preview at 4096 bytes, and a stored event row at 64 KiB to match existing SQLite constraints. This supersedes ADR019's decision to omit headers: full parsed headers are available to every authenticated user because Hydra grants equal full privileges. Headers can contain credentials/cookies and inherit that access policy. Unsupported/malformed messages and HTTP/2/TLS remain outside this parser.

Service forms separate the client-facing proxy listener (default `0.0.0.0:9000`) from the application backend (default `127.0.0.1:19000`); operators move the application first, and Hydra does not reconfigure it. Python exploit manifests may contain up to 32 exact `name==version` package pins. `uv` prepares wheels-only, bounded, cached offline environments before the worker is started; package preparation and exploit execution both remain behind the configured fail-closed Linux sandbox. No exploit was run as part of implementation verification.

## 021 — Bounded gzip history for completed TCP flows

The operator requested inspection after a TCP stream has closed. Extend the previous on-demand-only byte sample into per-service automatic retention, enabled by default and switchable live. Proxy keeps the latest 64 KiB per direction for at most 256 active histories, submits completed records through a 64-entry nonblocking queue, and stores gzip payloads in its private state directory. Records expire after 24 hours; oldest records are evicted above a 100 MiB compressed-payload quota. Decompression is capped at 128 KiB. Queue or storage loss does not delay forwarding and increments the history loss counter; tail truncation and byte totals are returned with each completed history.

Core continues to receive parsed telemetry only. It fetches bytes on demand through `ProxyDiagnostics.GetFlowHistory` and exposes an authenticated, audited flow-ID-only REST route; the response contains two directional segments and cannot select a network destination. Existing saved parser messages provide decoded conversations after closure. Raw views default to hex, with explicit UTF-8/ASCII choices. Existing one-shot next-chunk samples remain for live sessions. Service snapshots are JSON, so the default-on field is serde-compatible without a SQL migration. History and parsed-message retention are separate: Proxy history lasts at most 24 hours while Core's existing message retention policy remains authoritative.

## 2026-10-17 — Farm service/team catalog, explicit host runtime and gzip logs

The user explicitly changed the Farm requirement to ordinary host child processes, with Binary/Python/Bash uploads or pasted code, Python dependencies via uv, separately configured service ports and team IPs, direct-team selection, and gzip logs. This supersedes the earlier requirement that every exploit run must be isolated. Host execution is operator-selected in private operations configuration; the prior sandbox remains the default for other deployments and never silently falls back to host execution. Both modes retain configured-target selection, attribution, timeout/cancellation and bounded output. Host jobs inherit Core's OS privileges, not per-job sandbox resource or network restrictions. See `docs/farm.md` for interfaces and deployment semantics.

Farm catalog/run additions are additive migration0016. Legacy target/exploit/run records remain available, with old plaintext logs converted incrementally. Run pagination changes to newest-first row cursors; callers should restart pagination after upgrading. Gzip streams retain at most1MiB each; retention is7days/1,000completed runs/64MiB compressed output. No new permanent process, external database or public arbitrary-host run endpoint is introduced.

## 2026-10-17 — Durable Farm flag queue and editable Python batch submission

The user requested automatic regex extraction from Farm output, a persistent flag queue, and a Python submit script configured like Python exploits, with flags supplied as command-line arguments every 30 seconds. Migration 0017 adds a singleton revisioned config, durable collection clock and bounded gzip batch log history. Farm captures enqueue regardless of the legacy auto-submit switch; own-exposed digest quarantine and selected-run target provenance remain enforced. The editable Farm regex is independent of Proxy's defensive scanner pattern. No raw flags enter audit or submitter diagnostic output.

A configured uv host child receives up to 240 flags/60 KiB payload, no target arguments, and runs at most 120 seconds. One child runs per submitter, and one Farm worker may execute concurrently. Exit 0 acknowledges the whole batch; failures retry no sooner than 30 seconds, at most 10 attempts. Larger windows split into bounded calls. Config edits/pauses stop later chunks from claiming the old revision; active claims complete. Restart/persistence-error recovery preserves pending attempts. External success before a crash is necessarily at-least-once; script/checker idempotency handles duplicates. Existing operator checker adapters remain compatible when no Python config exists. No daemon, external database, protobuf change, or arbitrary-host run API was added. Details and reproducible acceptance commands are in docs/farm.md.

## 2026-10-17 — Live byte limits independent of decoding

The operator clarified that byte rules must stop a transfer while it is still arriving. Add host-owned `body.size` and `transfer.bytes` unsigned counters and evaluate size-dependent rules before forwarding each read. HTTP/1.x body counts reset per request and exclude headers/chunk framing; an independent bounded framing counter never stores body content and survives native content-decoder fallback. Other TCP profiles count received stream bytes per direction because arbitrary protocol request boundaries are unknown; UDP body size is each datagram and transfer size is cumulative per direction. Malformed HTTP framing becomes opaque stream accounting. A matching TCP limit closes the connection before forwarding the offending read; HTTP status injection remains limited to completely buffered HTTP requests.

Size metadata is carried in existing typed event fields, with closure-only metadata summaries for flows without decoded messages so raw rules can be tested against retained evidence. There is no protobuf/REST shape change, SQLite migration, raw packet stream or extra process. New filters default to transport blocking so selecting a live size field does not accidentally require complete HTTP buffering. Byte input uses exact unsigned 64-bit decimal/unit conversion, independent of text decoding and preview limits. See docs/rules-integration.md for field semantics and limitations.

### Nested group removal and explicit enforcement state

### Captured-traffic picker for filter rehearsal

The required exact-rule saved-traffic test was exposed as a JSON message-ID
prompt; enforcement shortcuts consequently failed for new or edited rules with
an opaque policy error. Keep SPEC's rehearsal requirement, but provide a session
and parsed-message picker scoped to the rule's service. Test and enforce reads
the current saved revision, tests that message, and submits a writable-fields-only
Enforce update when it matches. Revision conflicts and test failures prevent the
update; a no-match leaves the existing mode unchanged. Saving in Enforce mode
first saves Shadow and opens this dialog, with an explicit warning that the rule
will observe without blocking until enforcement is applied. Session choices are
bounded (25/page, 100 retained) and messages are bounded to 100 per session.
Existing REST handlers/contracts and the proxy data path are unchanged.

### Immediate filter activation — user product amendment, 2026-10-17

The user explicitly superseded the original shadow-first/test-before-enforce
policy: adding a rule should make it work directly. SPEC now reflects this.
Remove rehearsal prerequisites from create, update, single enable and bulk enable;
keep validation, optimistic revisions, audit attribution and Proxy ACK unchanged.
New UI rules default to enabled/Enforce; saving never demotes an enforced rule or
opens a required test dialog. Shadow and saved-message evaluation remain optional.
Existing modes are preserved; no mass promotion of existing Shadow rules. Keep
historical rule_tests records and the optional test API without a DB migration.
This supersedes earlier decisions making captured-traffic rehearsal mandatory.
The from-message creation endpoint also defaults to Enforce instead of Shadow;
this changes activation behavior while preserving request/response fields and
status codes. Its Explorer button and confirmation text now reflect activation.

### Traffic visibility, flag markers and service colors

The user requested blocked traffic hidden by default, flagged connections
highlighted, and a service-color line on stream cards. Console Show blocked is
opt-in; API include_blocked defaults to previous include-all behavior and filters
before pagination when false. A connection with any actual enforced block/drop is
classified as blocked, not a Shadow hit or an ordinary backend error. Metadata is
computed from retained events/indexes; no new unbounded state or storage migration.
Add compatible RuleHit.blocked field 5 and a domain hit indicator to distinguish
actual exceeded-rate-limit enforcement from allowed rate-limit matches. Legacy
normal block/drop hits remain recognizable; older rate-limit hits are ambiguous.
After durable critical commit, publish only redacted flag_detected SSE metadata,
never flag values. Lifecycle events hydrate indexes to cover cross-stream ordering.
Service stripes use a stable hashed HSL color, with text labels for service, Flag
and Blocked so color is supplementary. See docs/traffic-view.md for limits.

### Exact captured flags, operator batches and defensive vault removal

The user's flag workflow amendment supersedes the former mandatory 30-second
collection window and own_exposed vault retention. Preserve full UTF-8 regex
matches byte for byte and queue captured values while paused. Dispatch due
captures within about one second when enabled and idle, with configurable
1–240 flags per Python call. Manual durable requests select 1–5,000 queued/retry
flags while paused; the existing single worker splits them into bounded calls.
Retain retry backoff, redacted gzip logs, revision checks and actor auditing.

Migration 18 purges legacy defensive vault/triage/submission records, preserves
value-free detection metadata for Traffic highlights, and prevents reinsertion of
own_exposed vault rows. Move metadata incrementally to avoid duplicating its whole
SQLite table. Proxy omits defensive flag values in new critical spool records;
traffic bytes/history and rollback backups remain governed by their own retention.
Captured values and existing submitter configuration are preserved. Add optional
batch_size with default240, paginated /flags/page alongside legacy /flags, atomic
regex text import, and /submitter/run. Generated OpenAPI/frontend types reflect
these compatible additions. See docs/flags.md for limits and restart behavior.

### Flag text in the authenticated manager

The user explicitly requested original flag text instead of Digest in the table.
Expose optional value on the audited authenticated /flags/page response and render
it as escaped text with preserved whitespace in Vue. Keep SHA-256 digest for
deduplication/API compatibility, legacy /flags arrays redacted, and submission
logs redacted. This supersedes the earlier redacted-manager UI decision.

Explicit Submit on a failed captured flag requeues the same submission with
attempts0, a due-now timestamp and the requesting user. Only explicit actions reset
failed attempts; automatic duplicate captures do not. Accepted/inflight/queued
entries remain idempotent; a full pending queue rejects the retry atomically.

### Original submission diagnostics and streaming Farm logs

The user requested removal of [redacted flag] from Python submission diagnostics,
exact per-attempt input/output, Farm output beyond1 MiB, and new/existing flag
counts. New attempts persist the exact argv array and original stdout/stderr as
gzip; historical redaction cannot be reversed. Authenticated batch list returns
metadata summaries, detail returns bounded diagnostics; audit never copies raw
flags/output. Retention counts inputs as well as outputs (1,000/7days/64MiB).

Host Farm logs use32KiB gzip chunks and absolute byte offsets,64KiB compatibility
previews, and64KiB UI pages. Shared64MiB compressed/512MiB raw chunk quotas rotate
oldest history explicitly, avoiding a per-process1MiB cutoff or unbounded RAM.
Sandbox worker bounds remain, with all captured target output archived. Distinct
run/target/digest observations classify first-committed new flags versus already
captured values; repeats in one process do not inflate counts. Legacy counts are
partial and labeled. Observations contain no extra plaintext, cap at1million,
and cascade with run deletion. Migrations19–21 preserve existing configuration.

### Farm run table and modal logs

The user requested run time, actual teams, exploit/service names, duplicate/new
counts, state and actions in that order, with Logs opening a modal. Show the
start timestamp (creation time while queued) plus duration from start to finish;
running durations advance without counting queue time. Resolve catalog names in
Core so run history is independent of the currently loaded exploit editor page.
Team IPs come from the persisted run target snapshot, retaining the address used
even if its catalog IP changes; legacy targets/catalog removals fall back to
available names or IDs. Existing run list/detail responses gain optional display
metadata without a migration or removed fields. The shadcn dialog keeps existing
live paged logs/counters and restores keyboard focus to the invoking button.

### User identity, account creation and persisted settings (2026-10-18)

The user explicitly requested account creation in the console, superseding the original CLI-only rule. Authenticated same-origin creation shares CLI validation/hashing and quotas, audits the initiating account, and gives the new account identical full privileges. CLI bootstrap/recovery remains. Human provenance labels resolve UUIDs against the authenticated user directory; technical resource/request identifiers and captured values remain unchanged. Deleted/unavailable user references display Unknown user.

The Settings form validates and atomically replaces the actual bounded private operations file, with revision conflict detection, fsync, redacted intent/result audit and write-only credential keep/replace/clear semantics. Existing runtime state stays immutable until Core restarts; saved and running settings are shown separately. A dedicated Core-owned configuration directory allows rename without granting broad `/etc/hydra` writes. API users cannot select a file path. Farm table counters now share one New / duplicate column.

### Uploaded decoders in service configuration (2026-10-18)

WASM upload on Protocols registers an installed immutable version, which appears alongside built-ins and framing profiles in the service's searchable decoder field. Saving uses the existing service and protocol activation APIs; service snapshots retain a raw native fallback while the explicit versioned override determines parsing. The service table/editor resolve that override rather than falsely displaying raw. Rejected or not-yet-installed versions are excluded. Switching to a built-in removes the override after the listener snapshot applies. These API calls are individually audited and are not an atomic combined transaction: a partial failure says the listener was saved, preserves its assigned ID/revision, and permits retry without creating a duplicate service. New flows use the selected version; established streams retain their parser. Service creation adds an explicit `id` to its existing snapshot response, so transport-sharing listeners and canonicalized addresses cannot select an older service. Existing fields remain compatible. Activation state is refreshed before retry decisions to handle a lost acknowledgement.

### Normal gRPC response decoding budget (2026-10-18)

### Indexed gzip history eviction (2026-10-18)

Live Proxy profiling identified repeated whole-directory metadata scans on every completed TCP history as the CPU hot spot (about23,600 retained streams). The single history worker now rebuilds an ordered index once at startup, updates compressed-byte accounting on successful writes, and touches only expired/evicted records thereafter. No relay/filter/parser behavior or disk format changes; gzip,24-hour TTL,100MiB compressed-payload quota, bounded64-entry queue and64KiB tails per direction remain. A65,536-record oldest-first ceiling bounds index memory and small-file growth independently of compression ratio. Sidecars/filesystem allocation are additional to the compressed-payload quota. The private directory is worker-owned; external edits require restart to rebuild accounting. Startup still validates/reconciles files, while reads independently validate/decompress only the requested record.

Proxy-only redeployment uses a checksum-verified atomic binary replacement, binary backup, real gRPC readiness check and rollback. Core/SQLite/configuration/modules are preserved; existing TCP sessions reconnect across the Proxy restart.

### Shared relay and decoder contracts; retained-stream filter preview (2026-10-18)

TCP and UDP relay contexts compose the same `RelayBase` and implement `RelayTransport`, which owns common configured service, client endpoint, telemetry and decoder state while leaving async socket behavior transport-specific. Rust uses a trait plus composition for a shared parent contract. Built-in raw, UTF-8, HTTP, framing and WASM decoders implement the protocol runtime's common `ProtocolDecoder` interface and expose the same typed field-schema shape. Raw byte rules are evaluated inside Proxy; raw payload samples are never sent in parsed telemetry.

Traffic selection is bounded to 50 streams from one service. The visual filter editor loads fields from the selected service's applied decoder and sends an unsaved candidate plus flow IDs to Core. Core forwards only candidate/context metadata over the existing Unix gRPC channel. Proxy replays locally retained closed histories through its decoder and filter engine, returning match/block metadata only. Active, unavailable, decoder-error and partial-history states are explicit. This is a diagnostic preview, not enforcement; saving still applies the rule through the existing snapshot path. Each preview attempt is audited. OpenAPI and generated frontend types are checked against the handler.

### CPU-aware Proxy runtime pools (2026-10-18)

Replace the hardcoded two-thread Tokio runtime with one async worker per CPU reported by Rust available_parallelism at startup. Bound blocking pool growth explicitly to min(available CPUs,4). Connections stay async tasks; no thread is created per connection/task. Record actual pool sizes in the startup log. Shared catalog serialization and per-connection parser order remain; independent connections can execute across workers. Existing two-vCPU deployment keeps two async workers and at most two blocking workers. No API, wire, settings or history-format changes.

### Service appearance and traffic workspace (2026-10-18)

Service colors are Core-owned presentation data in SQLite migration 0022, keyed to service attribution with cascading deletion. Authenticated `PUT /api/v1/services/{id}/color` accepts a hex color or null to reset and records actor/before/after audit metadata. This changes neither relay revisions nor the internal Protobuf/service snapshot. Existing clients remain compatible; service list items gain an optional color. Database backups preserve colors; the relay configuration export remains concerned with networking/filter settings.

Twelve named presets plus a custom picker identify services. Unconfigured services receive deterministic, distinct presets within the loaded catalog until all twelve are used; configured duplicate/custom colors are allowed. A shared resolver keeps Services and Traffic consistent. The desktop workspace uses Reka/shadcn resizable panels with a pixel-sized stream column: expanding to the viewport grows the conversation while retaining that column width. Below 768px the panes stack. Conversation blocks align client-left/server-right when the viewer itself is at least768px wide, maintaining session order rather than regrouping events. Flag records, submissions, submission attempts and Farm run history start collapsed; closed histories suspend their polling. Initiating a Farm run opens its history for immediate feedback.

### Per-service traffic history and connection status (2026-10-18)

Replace the shared 100 MiB gzip budget with a default 128 MiB budget for each service, accounting for payload and sidecar bytes. Settings exposes `traffic_history_per_service_mib` (integer 1..1024); API saves persist to the private Core operations file and publish a bounded watch update. Additive `ProxyControl.ConfigureHistory` and `ProxyStatus.history_per_service_bytes` report/apply the policy over UDS with provenance. Upgrade Core and Proxy together; older Core remains compatible with the new Proxy default. Other operator settings retain their restart requirement; restart detection ignores history-only edits. Saved-versus-applied state remains explicit while disconnected, and reconnection replays the latest desired limit.

The history worker owns quota persistence, gzip writes and both time/service indexes. It imports existing flat gzip/JSON files once at startup, retaining their disk format. Append/rotation does not enumerate directories. Rotation for byte/index pressure touches only the writing service; TTL expiration still affects all expired records. Keep the global 65,536-record bound for memory/inode safety; reject/report a new service's capture when the index is full and that service has no record to rotate. The private history policy file preserves a custom limit if Proxy restarts while Core is absent. Existing 64 KiB directional tails, 24-hour expiry and bounded 64-record write queue remain. Filesystem block allocation is additional to the accounted file bytes.

The console header uses an explicit Proxy connection boolean, turns red on disconnect/status errors, and clears on recovery. Remove the SSE callback that replaced status with a generic connected label; event delivery from Core is not proof of Proxy connectivity. Poll the cached status every five seconds.

### UDP visibility and standalone installation (2026-10-19)

Remove the console's implicit TCP-only flow query and live-event guard; display transport on cards and label UDP conversations explicitly. The existing shared rule/decoder implementation is unchanged. UDP supports datagram dropping, per-datagram byte limits, cumulative directional transfer limits, parsed messages, live samples and flag classification. TCP-only gzip byte history and its replay preview are not extended in this change; show that limitation rather than offering a misleading closed-UDP history request. No wire, API or database change is required.

Provide a standalone Bash/Python installer with an editable download URL at its top, supporting verified CI tarballs and Actions ZIP/index downloads. Default to the latest unexpired main artifact, preserve existing users/configuration/units and use a consistent stopped-Core SQLite backup for rollback. Generate initial credentials through the real hidden CLI prompt only when no users exist. Pin optional uv installation; keep execution mode sandbox until an operator explicitly configures execution. Target x86_64 Linux/systemd, leaving competition applications separate. Downloads are bounded HTTPS, bearer tokens are sent only to GitHub's API and stripped across redirects, archive members are allowlisted and binary checksums are mandatory. Existing installations with custom database/key/operations paths are rejected before mutation instead of backing up the wrong database.

### Latest-only S3 publication (2026-10-19)

Add a final main-only CI job after verification, consuming the exact run's existing static artifact. Serialize publication without canceling active uploads and skip superseded main SHAs. Write only fixed `hydra-core`/`hydra-proxy` keys in the configured Garage S3 bucket, replacing previous objects and retaining commit/SHA-256 metadata. Credentials are repository Actions secrets scoped to the upload step. The supplied key allows writes but denies HEAD/GET/list; verify each single-part PutObject through Content-MD5 and its matching ETag instead of requiring additional read privileges. Preserve downloadable Actions artifacts and avoid changing bucket policy or unrelated keys. Two-object replacement is not atomic; a partial failure fails CI and is recovered by rerunning publication.

### CI dependency and toolchain caches (2026-10-19)

Cache Cargo archives/git dependencies and dependency build artifacts for the root workspace and standalone WASM sample, including native debug/release and musl targets. Use the maintained Rust cache action's dependency cleanup and environment/manifest keys rather than archiving all target files or duplicating them in sccache. Disable incremental output through that action and permit main-only cache saves after later check failures. Cache pinned Rust toolchain/target libraries separately from dependency keys, and cache Bun/pip package downloads by lock/requirements inputs. Keep the runner on Ubuntu24.04 and install protobuf/musl tools together. Rebuild embedded Vue and execute all checks every run. Runtime state/private uv caches stay isolated; a global setup-uv package cache would not receive those downloads. First-run cache population and later cache-hit measurements must be distinguished from measured speedup.

### Standalone stdin-configured S3 installer (2026-10-19)

Default installer downloads now use curl's built-in Signature V4 rather than requiring an Actions artifact, Git checkout or AWS CLI. Read endpoint, region, bucket, access ID, secret and optional session token only from the original stdin, preserved on a separate descriptor while Python reads the embedded installer code. Hide credential prompts on terminals and pass curl configuration through its own stdin; never put S3 credentials in arguments/files/logs or propagate that descriptor to bootstrap children. Install missing curl/Python/system tools using the supported package manager. Keep offline archive and explicitly configured legacy HTTPS modes.

Validate each latest object against SHA-256/commit/target metadata, reject mismatched release pairs and redirects, and verify ELF architecture before touching installed state. This consumes the two existing S3 keys without adding a manifest object or requiring bucket listing. Bound downloads with curl time/size limits plus a child file-size limit, including curl versions predating streamed size enforcement. Existing installation backup/rollback and CLI bootstrap remain. A read-capable key is required; publisher write-only access is not broadened.

### UDP saved-history parity, farm effectiveness and filter controls (2026-10-19)

Extend the existing private Proxy gzip history store to UDP. Retain complete datagrams in capture order (direction, millisecond capture time, monotonic flow sequence), including empty datagrams. Preserve the shared 64KiB per-direction bound, 256 active captures, 64 queued writes, 24-hour TTL, configurable per-service quota and global 65,536-record index. Add a 1,024-datagram per-flow ceiling; evict whole oldest datagrams and report partial history. Capture before inspection as TCP does; captured counts include blocked datagrams while forwarded byte totals exclude them. Replay uses the same datagram decoder/body-size path as live UDP and never joins raw bytes across datagrams. HTTP stream parsing remains TCP-specific.

Protobuf and HTTP history responses gain additive transport/datagram fields; Proxy status gains history_dropped for admission, queue and persistence failures, separate from tail truncation. Old TCP sidecars default to TCP and their gzip layout is unchanged. UDP gzip adds bounded binary framing; upgrade both binaries together. Rolling back to an older reader requires removing or restoring its pre-upgrade UDP history directory, since it cannot decode new UDP framing. History is diagnostic data; failures never block forwarding. The console exposes writer loss independently from telemetry loss.

Add an authenticated/audited paged farm effectiveness projection: up to20 exploit/service rows by10 team columns, service and run-creation-window filters, latest run/log access, distinct new/duplicate flags and accepted counts credited only to the original capture run and target. Migration0023 records actual child attempt boundaries and exit states independently from aggregate run state. Host attempts record spawn-to-exit timing; sandbox timing includes setup. Shared batch processes and old runs have unknown individual outcomes; do not copy an aggregate success/failure into each team cell. Existing run/log retention bounds the projection; partial legacy flag statistics are marked. No process, external database or network dependency is added.

Replace the filter table with service-striped cards sharing Traffic's color resolver. Each rule has a controlled accessible switch sending the explicit desired boolean through the existing revision-checked audited update path. Desired enablement is green/red, independently labeled from Proxy application state. Pending/error states cannot masquerade as an applied rule. Reconcile pending snapshots with bounded non-overlapping reads, stale-response invalidation, backoff and cleanup; editing/selection is preserved. Switching requires no optional preview or shadow test. Public mutation contracts remain unchanged.

## 024 — Configurable zstd for retained traffic and logs

New traffic archives, Farm output chunks/previews, and submitter input/output diagnostics use checksummed zstd through the shared `hydra-compression` crate. Settings exposes level 1–19, default 1, and Core applies it to new Core writes while Proxy receives it through the provenance-bearing history policy RPC. Proxy persists the applied level alongside its history quota so it remains effective across restart without Core. Old gzip traffic files and gzip blobs in existing SQLite columns continue to decode; new traffic files use `.zst`. Existing database column names are retained to avoid a data migration; the blob codec is detected from its magic bytes. Lower levels favor speed, higher levels can trade CPU for smaller files. Level changes do not recompress old records.

## 025 — Aggregate service overload budgets and live metrics

Traffic can share one gateway IP, so bound work by configured service rather than banning that source. Extend Service with optional protection (legacy default disabled): active sessions, new TCP connections, UDP datagrams, aggregate bytes and inactivity. Share monotonic token buckets between live sessions and retain them across configuration changes. Reject before decoding/capture/upstream creation, close complete TCP connections on excess reads, and drop complete UDP datagrams. No packet wait queue or per-denial persisted event. Retain the global flow semaphore and bounded service registry. Service metrics are additive protobuf/status fields, and Vue computes recent rates from bounded visible-page polls with epoch/gap handling. No externally deployed monitoring system or unmeasured throughput promise.

## 026 — Explicitly saved traffic survives ordinary rotation

Favourites protect retained Core flow/message metadata and a separate checksummed zstd archive on Proxy. A fixed 128-slot archive bounds retained state without deleting another favourite automatically. Core excludes saved records from normal retention and separately accounts their finite decoded budgets. Reject oversized snapshots on admission; surface incompleteness if later live decoded capture reaches its bound. Pin active flow snapshots before completion, update them on completion, preserve interrupted snapshots as partial, and reconcile durable pending/unpin intent after reconnect. All users share these archives under existing authentication/audit; no raw packet streaming is added to gRPC. Upgrade the binary pair together; old services remain valid and existing gzip history stays readable.

## 027 — Remove the shared SQLite file ceiling; audit actual acknowledgement changes

Explicit user amendment 2026-10-22: remove the 256 MiB database limit. The live database reached 65,536 pages and SQLite rejected session/audit writes, causing valid logins to return the generic authentication failure. Clear the old ceiling to SQLite's native maximum on connection startup; retain existing per-feature traffic/log/queue quotas and memory bounds. Do not erase audit history or favourites to recover login. Acknowledging an unchanged service/rule revision or identical error is not a state change: skip its SQL update and duplicate `control.apply` audit row. Ignore stale/future acknowledgements rather than auditing a state transition that never happened. Audit actual applied-revision/error transitions and recovery as before.

## 028 — Explicit framed forwarding with reversible flag substitutions (2026-10-22)

The user approved forwarding to a configured competition team, generated matching substitutes, persistence across connections/restarts and restoration of original flags in replies. This is an explicit opt-in exception to byte-transparent relay and value-free defensive flag detection: a separate protected Proxy mapping journal necessarily retains both values, but never inserts them into Core's Flag Manager/submission queue or parsed gRPC telemetry. Equal-length substitutions avoid rewriting protocol lengths; unknown, encrypted, signed or unsupported framing does not acquire an unsupported transparency promise.

Add the justified pure `hydra-rewrite` crate (depends only on domain and lightweight parsing/generation libraries), shared by Core for configuration validation and Proxy for framing/rewriting. Core does not import the WASM host or Proxy; Proxy does not import SQLite. Typed optional protobuf fields preserve legacy disabled behavior. Proxy resolves no client-supplied per-request addresses: Core resolves an enabled Farm team to a fixed endpoint when saving service configuration. Proxy detects local-interface listener loops, including IPv4-mapped addresses.

A bounded private mapping journal is durable before substitution is forwarded, exclusively locked and never automatically evicted. Limits8192 entries/16MiB are global; exhaustion rejects new mappings. A fixed-width printable ASCII generator avoids pretending arbitrary regexes are generatable. HTTP chunk entity spans preserve original chunk framing and headers; generic TCP requires explicit delimiter/length framing. Existing UDP bindings reset on configuration changes as before; TCP connections retain their chosen target/policy. This is a documented refinement to the approved design's general existing-session statement.

See docs/flag-forwarding.md and docs/superpowers/specs/2026-10-22-flag-substitution-proxy-design.md. No new permanent process or per-session thread is introduced.

## 029 — Read-only terminal playback and ordered TCP history (2026-10-22)

The user requested a normal nc-style view for ports7117/13345 alongside captured bytes and decoded fields. Both services already use UTF-8. Add a lazy-loaded xterm.js renderer to the existing shared conversation component and make it the default for UTF-8 profiles; keep Raw's hexadecimal default. Capture replay has no interactive backend or clipboard/link-opening integrations.

A terminal requires direction interleaving, which the previous TCP archive's two direction buffers could not reconstruct. Reuse the existing bounded frame codec for new ordered TCP chunks, with backward-readable `ordered_tcp` metadata. Preserve64KiB per direction and1024 chunks globally, keeping bytes/timestamps/sequence together; UDP remains datagram-based. A message-table insertion sequence makes saved queries and live parsed events consistent when timestamps tie. Older captures without interleaving use saved parsed text or single-direction playback. No new public route, migration or protobuf field is needed; existing JSON message objects gain `stream_sequence` and existing history segment metadata carries order. A Proxy downgrade has a documented ordered-history compatibility limit.

## 030 — Terminal direction colors and invalid-byte annotations (2026-10-22)

Use fixed red/client, blue/server and yellow/invalid-byte terminal colors, overriding payload SGR foreground requests. Consume payload SGR and string controls while retaining complete cursor escape sequences across read boundaries. Preserve invalid bytes as typed parsed `utf8.spans` annotations alongside compatible `utf8.text`; bound each text event to4096 bytes and64 spans. This avoids trying to reconstruct invalid values from U+FFFD, which can also be genuine valid text. Saved byte playback uses independent streaming UTF-8 validators per direction. No raw live packet gRPC API or schema migration is introduced.

## 031 — Decoder field paths are not a static engine whitelist (2026-10-22)

The pure rule engine accepts bounded nonempty dot-separated field paths, including native UTF-8, framing and hot-loaded WASM fields. Decoder catalogs populate UI choices; they cannot be represented by a compile-time engine allowlist. Removed the obsolete whitelist that rejected fields already offered by the configurator. Existing path/value/expression limits and three-valued missing-field semantics remain; absent fields never cause a match, including under negation. Unknown-but-well-formed paths now compile and remain nonmatching until supplied by parsed events. No contract or storage migration.

## 032 — Whole-filter direction selector (2026-10-22)

Represent client/server selection as an outer `all` containing the existing typed direction predicate and the user's original expression. Both directions leaves the original expression unchanged. Editor recognizes only that canonical wrapper, never extracts a predicate from OR/NOT, and uses the same serialization for live rules and saved-stream previews. Backend already supplies direction for TCP/UDP; contracts, migrations and Proxy are unchanged. Filter cards show direction explicitly; TCP close semantics are explained in the editor.

## 033 — Compressed per-service decoded traffic (2026-10-23)

User requires256MiB per service and zstd3 for decoded retention. Store complete bounded JSON records as checksummed zstd frames in SQLite backing tables; expose bounded decoded JSON views to existing feature queries and indexes. Register deterministic/innocuous compression functions on Core's sole database connection. Fixed decoded level3 is independent of configurable farm/raw history compression. Budget triggers count actual compressed payload bytes per service and globally for diagnostics; SQLite index/page overhead is excluded. Direct backing-table ingestion preserves affected-row dedup and insertion sequence.

Replace destructive independent-table/global cleanup with coherent service-local session rotation. Active tails are trimmed only as needed, including raw rule-hit-only sessions, and marked partial; favourites stay protected under existing finite admission limits. SQL migration compresses all retained rows, restores message indexes from complete retained events in original event order, and reconstructs orphaned session cards where opening and message data survive. Core-only rolling upgrade keeps Proxy forwarding. Old binaries require a pre-migration database restore. Public API/protobuf remain unchanged.

## 034 — Per-condition direction scopes (2026-10-23)

User clarified that direction belongs on each condition row. Add typed `Expression::Scoped {direction, scoped}` and Protobuf `RuleExpression.direction_scoped` field5. The engine returns Unknown outside the chosen side; ordinary boolean direction guards would incorrectly turn true under group negation. Scopes participate in existing depth/node budgets and streaming-byte reference detection; evaluation adds no allocation or session state.

Visual rows serialize independent scopes, including condition negation inside each scope. Both means no wrapper. Existing whole-filter canonical guards migrate to equivalent per-condition selections on edit; unsupported contradictory nested scopes remain protected from lossy visual editing. Cards summarize single versus varying row directions. API expressions remain Object-shaped with an additive node documented in generated OpenAPI. Upgrade Proxy before Core; old readers reject new scoped nodes, so downgrades require removing scopes or restoring configuration. No SQLite migration is required.

## 035 — HTTP inspection layout and service metadata (2026-10-23)

User clarified that HTTP work is the viewer layout, not new decoder support. Render existing parsed HTTP fields as structured messages with headers, query parameters and body formatting, preserving chronology/direction layout and generic field inspection. Format valid JSON by changing whitespace in original tokens; parse-and-stringify would round large integers and drop duplicate keys. Bound input/depth/output/header/query previews and render captured text only through escaped Vue interpolation.

Service names live in Core-only `service_names` metadata (migration26), with optional `ServiceInput.name` for API compatibility. Write name and service snapshot in one audited transaction; omit name on update to preserve it. Stable IDs remain transport/history keys. The Services table exposes the exact requested columns; enable/application status stays in the Name cell. A visible TCP mirroring button opens the existing framing/flag-substitution configuration. Copy framing into its draft to preserve Cancel semantics. No gRPC or protocol decoder changes; deploy Core only.

### Shared filter service scope

A filter keeps its UUID, author, switch, priority, and aggregate hit count when it is attached to more than one service. Its existing `service_id` is the first service. The optional OpenAPI `additional_service_ids` and repeated Protobuf field 13 add the other services to the same rule. Old records and single-service clients can omit the new field. No database migration is needed because rules are stored as JSON.

The editor intersects field paths and types across its selected decoders. The compiler rejects duplicate or unknown services and validates transport-specific actions against every selected service. Rate windows stay separate by service. A saved-message test uses the actual service associated with that message. Upgrade Core and Proxy together before applying shared rules because an older Proxy does not recognize field 13.


### Networkless Python and WASI request checkers

Proxy can optionally launch a persistent Python or WASI sandbox for configured services. It evaluates client input before forwarding and uses a configured fallback on errors. One worker processes one framed input at a time, and concurrent requests use the fallback instead of entering a queue. Each worker gets a private writable directory mounted at `/state`. The host directory is keyed by checker ID under the Proxy state directory. Stateful scripts retain files independently of worker or Proxy restarts. The protobuf status additions expose an error count and the latest safe diagnostic string. Proxy persists console policy edits atomically with a separate revision. The host administrator registers the actual Python or WASI program in Proxy JSON. See [Request checkers](docs/REQUEST_CHECKERS.md).

### Live rule evaluation of large decoder fields

Rule input stays bounded at 16 KiB per value, but live evaluation now uses a separate 68 KiB cap (`MAX_EVALUATED_FIELD_BYTES`). A 16 KiB TCP read plus the raw decoder's 1023-byte overlap tail, or a 64 KiB UDP datagram, legitimately exceeds the input cap; treating those values as unknown silently disabled the rule, so `raw.bytes contains` forwarded a 20 KiB request unchanged while the same 2-byte marker was blocked in a small request. Values above the runtime cap remain unknown and are never matched on a truncated prefix, which preserves the existing guarantee that a truncated window cannot produce a match. Built-in decoders cannot reach the runtime cap, so no rule sees different bytes than before.

### Host network metadata wins over decoder fields

Field merging in the TCP path now inserts decoder output first and host-owned `network.src_ip`, `network.src_port`, `network.dst_ip`, `network.dst_port`, `transport.kind` and `packet.size` last. A native or WASM decoder could previously return those keys and shadow the real connection, so a source-based rule or a size limit could be bypassed by the plugin that is supposed to describe the protocol. `body.size`, `transfer.bytes` and `direction` were already protected by host-side annotation.

### TLS termination and upstream TLS per service

The operator asked for HTTPS support next to the HTTP decoder. A decoder cannot
decrypt TLS, so this is a transport feature: an optional `tls` block on a TCP
service lets the Proxy terminate client TLS with an operator-supplied PEM chain
and key, connect to the backend over TLS, or do both. Termination happens inside
the connection task, so a slow or hostile handshake cannot stall the accept loop,
and the handshake shares the existing connect timeout. Upstream TLS is negotiated
before flag substitution so the forwarding pump still sees plaintext, and
`insecure` accepts self-signed competition backends while a configured
`server_name` supplies SNI and verification. ALPN offers HTTP/1.1 only: the native
decoder cannot parse HTTP/2, and a mis-decoded h2 stream would be worse than an
unparsed one. The private key travels to the Proxy over the local socket and is
never returned by the API; reads report `private_key_configured` instead, and an
update that omits the key keeps the stored one. Nothing is decrypted passively:
traffic without a terminating service stays opaque, as before.

### Certificate prompt for undecoded TLS streams

Opening a stream that starts with a TLS record header used to show hex with no
hint that a certificate would make it readable. The traffic view now detects the
handshake from the first client chunk (a record content type 0x14-0x17 with major
version 0x03 and a minor version up to 0x04, in text or hex spans) and, when the
service has no TLS configuration, shows a prompt with a button that opens that
service's TLS section. A certificate cannot decrypt a session that was already
negotiated, so the prompt states that only connections opened after saving are
read as HTTP; captured history stays opaque and is not rewritten. Detection is
client-side on bytes the console already receives, so no new API field, protobuf
message or migration is added.

## 036 — Normal telemetry batches only already-ready events (2026-11-23)

Under sustained UTF-8 traffic the Proxy forwarded one `EventBatch` per event, so
Core indexing and the telemetry stream paid per-event framing even though
`StreamTelemetryRequest.max_events_per_batch` already existed. The stream now
awaits exactly one broadcast event as before, then drains only events already
queued with `try_recv`, never with a timer or delay. Batching therefore adds no
latency at low rates, and the data path, broadcast ring and `mpsc(16)` queue are
unchanged; only the payload of each already-queued message grows.

Bounds are `max_events_per_batch` (validated `> 128` before subscribing) and
256 KiB of encoded batch bytes, measured with prost `encoded_len`. `0` keeps the
legacy one-event-per-batch behaviour rather than meaning "server default", so a
client that omits the field sees no behaviour change. A candidate that would
cross the byte cap is carried into the next batch as its first event, preserving
FIFO order without splitting or dropping it; a single event larger than the cap
is still sent alone. `Lagged(n)` increments the dropped counter exactly once at
the point the gap is observed and contributes one `subscriber_lag` loss marker,
including while draining or when that marker has to be carried. A closed
broadcast still flushes the batch already assembled.

One event whose protobuf encoding exceeds Core's 49,152-byte field budget is
forced to travel alone. That constant mirrors Core's *field budget* only
(`traffic::repository::field`); it is not Core's 64 KiB JSON-row limit. Core
converts a whole batch before its transaction and rejects the entire batch on
one error, so a single event over the field budget would otherwise discard up to
127 healthy neighbours: for that class, keeping the existing one-event blast
radius is the compatibility requirement, not a new Core limit. Core's separate
`data.len() > 65536` JSON-row rejection is **not** bounded by `encoded_len`
(bytes fields become JSON number arrays, control characters escape to `\u00XX`,
and many small bytes fields can compound), so an event under the guard can still
fail the whole mixed batch. That residual per-event isolation gap is documented
here and needs a separate follow-up (Core per-event skip/reject accounting or a
tested proxy guard); this change does not claim batch poisoning is eliminated
for all cases. If Core's acceptance policy changes, this constant must follow.

Queue memory is bounded in *encoded* bytes, not heap bytes: the retained
`mpsc(16)` holds up to 256 KiB per multi-event batch (≈4 MiB per subscriber,
≈16 MiB across the four permits) plus one carried event, and an oversized solo
event adds its own size as an exception. Those are upper bounds on encoded
payload, not measured RSS; prost container overhead can be a multiple of them.

The batching loop lives in `grpc/telemetry.rs` because the pre-existing
`ProxyTelemetry` handler already built and sent one `EventBatch` there; this
change extends that delivery loop in place rather than moving ownership to
`telemetry/`. No new crate, process or Core dependency is introduced, and a
future extraction into `telemetry/` is optional, not required by this fix.
`first_sequence`, `critical`, the epoch format, the critical spool and
proto/OpenAPI are untouched.

## 037 — Page-scoped WebSocket for the Streams workspace (2026-11-23)

User amendment (explicit, 2026-11-23): live Streams-page updates, initial snapshot, messages, global
retained search and byte history (active and closed) travel over one authenticated page-scoped
WebSocket; flag highlight spans are computed for selected messages and search results. This amends
SPEC line 84 ("frontend live traffic via SSE") for `TrafficPage` only. Design:
`docs/superpowers/specs/2026-11-23-streams-workspace-design.md`.

- SSE endpoint `/api/v1/traffic/events/stream` is unchanged and remains the transport for
  `ConsoleLayout`, `FilterStreamWorkspace` and external consumers; no OpenAPI change (a WS upgrade is
  not expressible in OpenAPI 3.1 and is not annotated).
- One socket per page, in-band `auth` frame; token never in URL, query string or logs. Access JWT
  remains 60 s: client re-auth ≤50 s, server re-verify every 5 s, close 4401 on expiry/revocation.
- Caps unchanged in count: 2 sockets/actor, 128 actors, 16 total shared with SSE, one search and one
  history stream per socket. Inbound frames are capped at 32 KiB; outbound frames may exceed it with
  bytes and are bounded by 1 MiB per frame / 4 MiB queued per socket.
- Byte history is an **on-demand** Proxy diagnostic read: `GetActiveFlowHistory` (additive proto,
  fields 1–16 frozen, reusing the existing `HistorySegment` message) for the active 64 KiB/direction
  tail, and the existing `GetFlowHistory` for closed flows. Both arrive as `active_bytes` frames with
  `snapshot.active` reflecting the source. No continuous raw packet broadcast, no data-plane Core
  dependency, no raw history files searched, no HTTP fallback on the Streams page. Legacy HTTP
  `GET .../history` keeps its 409 for active flows for non-Streams consumers.
- The new RPC validates actor (from the verified JWT, never a WS frame) and request id, writes one
  audit row, and never broadcasts; a head drop forces a snapshot `reset`, never a splice.
- Global search is a resumable bounded Rust-side scan over decoded zstd backing rows: no FTS, no
  migration, no backfill, no 4 KiB projection. Literal case-insensitive input (not user regex), full
  retained decoded text, per request ≤5 000 rows / 750 ms / 32 MiB, cursor = last scanned position
  (globally stable `seq`). Responses carry `coverage {scanned,truncated,continuation,undecodable,
  decoded_only,raw_history_searched:false}`; the client auto-continues up to 3 units then shows an
  explicit partial/More state. Proxy raw capture and rotated-out bytes are not searched.
- Flag highlight spans are server-computed from the visible configured Core `flag_pattern`, returned
  as **UTF-16 code units**, and rendered as text nodes (no `v-html`, no `new RegExp` on untrusted
  text). `has_flag` remains the canonical flow-level finding predicate; highlighting never changes
  finding truth.
- No new process, crate, or data-plane path; `axum` gains the `ws` feature and `tokio-tungstenite` is a
  dev-dependency only.
