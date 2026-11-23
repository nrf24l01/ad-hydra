# Streams workspace over WebSocket — implementation plan (2026-11-23)

**Status: USER APPROVED (explicit, 2026-11-23).** Do not re-ask for approval. This plan does not
authorize commits or pushes; those happen only after the runner verifies every gate.

## Goal

Replace the Streams page's HTTP/SSE data path with one authenticated, page-scoped WebSocket that
carries the flow snapshot, pagination, selected-flow messages, live events, global retained search,
and byte history for both active and closed flows — with server-computed flag highlight spans that
never alter finding truth.

## Architecture

- `TrafficPage.vue` owns exactly one socket to `GET /api/v1/traffic/events/socket` (subprotocol
  `hydra.traffic.v1`); SSE remains for every other consumer.
- Core `Traffic` owns `LiveSockets` (permits, per-actor map, counters), the WS state machine in
  `modules/traffic/ws.rs`, and a bounded Rust-side `search.rs`. No new process, crate or daemon.
- Search scans existing decoded zstd backing rows directly: **no FTS, no migration, no backfill, no
  4 KiB projection**. Each request is bounded to ≤5 000 rows, ≤750 ms, ≤32 MiB decoded, resumable by
  a last-scanned cursor; full retained decoded text is matched.
- History is on-demand from Proxy: new additive `GetActiveFlowHistory` RPC for the active tail and
  the existing `GetFlowHistory` RPC for closed flows; both reach the client as `active_bytes` frames.
  Proxy never pushes bytes and Core has no data-plane dependency.
- Flag spans are computed server-side from the visible configured Core `flag_pattern`, returned as
  UTF-16 code units, and rendered as text nodes. Findings (`has_flag`) remain canonical.

## Global constraints

- Preserve all pre-existing dirty/untracked work listed in the spec §0; never `git checkout`,
  `git clean` or revert it. No `.env` or secrets read/written/logged.
- Token only in the `auth` frame; never URL, query string, header echo or logs. Access JWT stays 60 s:
  client re-auth ≤50 s, server re-verify every 5 s, close 4401 on expiry/revocation.
- Bounded everything: 2 sockets/actor, 128 tracked actors, 16 sockets total shared with SSE, 32 KiB
  **inbound** frames, 1 MiB per outbound frame / 4 MiB queued per socket, one search and one history
  stream per socket, 2 concurrent searches.
- No HTTP data fallback on the Streams page for list/messages/search/history. Legacy HTTP history and
  its 409 stay for non-Streams consumers only.
- Read-only DB connection for search when possible; otherwise chunked `Database::run` so no shared
  mutex hold exceeds 750 ms and auth/ingest are not starved.
- All caps and loss are explicit (`loss`, `truncated`, `continuation`, `undecodable`); never silently
  present partial results as complete.

## Review focus

1. Contract correctness: proto field numbers 1–16 frozen; existing `HistorySegment` reused verbatim;
   search `coverage` fields and cursor semantics exactly as spec §5.
2. Boundedness: row/time/byte caps, outbound byte queue, 16/2 socket caps, no unbounded channel or
   global mutex hold.
3. Auth/audit: actor and request id validated server-side; one audit row per history subscription;
   no payload/token/flag text in logs or audit.
4. No schema/migration/FTS remnants anywhere; no HTTP fallback calls from the Streams page.
5. UTF-16 span correctness and no `v-html`/`new RegExp` on untrusted text.

Spec: `docs/superpowers/specs/2026-11-23-streams-workspace-design.md`.
Repo `/home/Technik12345/vscodeprojects/ad-hydra`, branch `main`, baseline
`fe46766d94b8e32f0950989ba7383300107336f8`.

## Sequencing

```
T1 Proxy proto+snapshot ──► T2 Core WS/search ──► T3 Frontend
        │                        │                   │
        └────────► T4 tests authored from the frozen contract (parallel, disjoint files)
                                   └────► T5 runner (last, no edits)
```

T4 can write tests as soon as T1/T2 interfaces exist as stubs, but must not edit T1–T3 source. T1
adds the `#[cfg(test)] mod active_tests;` line so T4's file compiles. Nothing is "done" until T5
runs the gates.

---

## T1 — Proxy: on-demand active byte snapshot

**Files (only these):**
- `contracts/hydra/proxy/v1/proxy.proto`
- `apps/hydra-proxy/src/history/mod.rs`
- `apps/hydra-proxy/src/history/tcp.rs`
- `apps/hydra-proxy/src/grpc/diagnostics.rs`

**Steps**
1. Add `rpc GetActiveFlowHistory(GetActiveFlowHistoryRequest) returns (GetActiveFlowHistoryReply);`
   to `ProxyDiagnostics` and the messages from spec §4.2, verbatim field numbers 1–16. Additive only:
   do not touch `GetFlowHistoryRequest/Reply`. Reuse the **existing** `HistorySegment` message
   (`proxy.proto:353-360`, fields 1–5) — do not define a new segment type and do not reference a
   generated type that does not exist.
2. Validate the diagnostic request before reading: actor must match the authenticated Core actor
   (built from the verified JWT, never a WS frame), `request_id` non-empty and ≤64 chars, `flow_id`
   like `get()`. No broadcast, no subscription, no push.
3. `Buffer`: add `dropped: [u64; 2]`. In `record_tcp` increment the direction's `dropped` for every
   byte removed by partial `drain` and whole-chunk eviction (the same places that call
   `mark_truncated`). UDP path unchanged.
4. `HistoryStore::active_snapshot(&self, flow_id, cursor: Option<ActiveCursor>) ->
   Result<Option<ActiveSnapshot>, HistoryReadError>`:
   - `None` when the flow is not in `active` (closed/unknown) — do **not** fall back to disk.
   - Lock the buffer, clone at most the retained chunks (≤1024, ≤128 KiB), compute
     `retained_*_bytes`, `truncated_*`, `dropped`, last sequences.
   - `reset = cursor.is_none() || cursor.last_seq != current_last_seq || cursor.head_dropped !=
     current_dropped` per direction. When `reset == false`, return only chunks with sequence greater
     than the cursor's last sequence. Any head drop forces `reset=true` (snapshot, never a splice).
   - No persistence, no blocking file IO, no new task.
5. gRPC handler: map `Some(snapshot)` → reply with `active=true`; `None` → `active=false` (OK, not an
   error). `Storage` → `internal("flow history is unavailable")`. Keep `history_status()` unchanged for
   `GetFlowHistory`.
6. Do not change `get()`: active flows still return `Active` (legacy HTTP 409 path preserved).
7. Add `#[cfg(test)] mod active_tests;` at the bottom of `history/mod.rs` (T4 owns the file content).

**Acceptance:** `cargo build -p hydra-proxy`, `cargo test -p hydra-proxy history`, `buf lint` if
configured. No new crate, no new task, no change to `finish()` semantics.

---

## T2 — Core: WS transport, live sockets, global search, audit fix

**Files (only these):**
- `apps/hydra-core/src/modules/traffic/ws.rs` (new)
- `apps/hydra-core/src/modules/traffic/mod.rs`
- `apps/hydra-core/src/modules/traffic/repository.rs`
- `apps/hydra-core/src/modules/traffic/search.rs` (new)
- `apps/hydra-core/src/modules/traffic/diagnostics.rs`
- workspace `Cargo.toml`, `apps/hydra-core/Cargo.toml`
- `DECISIONS.md` (append 037)

**Steps**
1. Cargo: workspace `axum = { version = "0.8", features = ["ws"] }`; `hydra-core` dev-dependency
   `tokio-tungstenite = "0.28"`. Commit the lock delta with the change.
2. `mod.rs`: `mod ws; mod search;` and one route
   `.route("/api/v1/traffic/events/socket", get(ws::socket))`. No `#[utoipa::path]` on it.
3. `LiveSockets` in `repository.rs`: raise `streams` to 16 permits shared by SSE+WS; per-actor map
   (cap 128 actors × 2 sockets, zero-count eviction) guarded by a `Mutex`; counters for the `counters`
   frame. Owned by `Traffic`, never a `static`. No SQL in this struct.
4. `ws.rs` (~300 lines, private state machine):
   - Origin check against `AuthState.origin` pre-upgrade; permit via `try_acquire_owned`; `on_upgrade`.
   - 5 s first-frame deadline; `auth.authenticate(token)` + `decode_at(token,"access",now)`; pin
     `sid`, reject a different `sid`; re-verify every 5 s; client re-auth ≤50 s; on expiry/revocation
     send `reauth_required` then close 4401.
   - Frame size 32 KiB, binary rejected; unknown `type` → `error` + 4400.
   - `subscribe`: `let rx = t.events.subscribe()` **before** `t.flows(query)`; send `snapshot`
     (generation 1); forward from `rx`. `page` uses `t.flows(Query{after,limit})`. `messages` uses
     `t.messages`. `select` filters messages and manages history. `search` calls `t.search`.
     `resync` re-snapshots with generation+1. `pause`/`resume` per spec §3.2.
   - Outbound `mpsc` byte-bounded at 4 MiB queued / 1 MiB per frame with `try_send`; full →
     best-effort `loss`, close 1013. `Lagged(n)` → `loss` and continue. Never log tokens or payloads;
     log `connection_id`, `request_id`, actor id. The 32 KiB cap is inbound only.
   - `counters`: derive from forwarded events, emit ≤ every 2 s.
   - History: on `select`, write **one** audit row `traffic.history.subscribe` (actor, flow_id,
     connection_id, request_id, outcome) through the existing audit module. Active flow → poll
     `t.history(flow_id, cursor)` at ≤1/s via `GetActiveFlowHistory`; closed flow → one
     `GetFlowHistory` read. Both forward one `active_bytes` frame per reply with `snapshot.active`
     reflecting the source; stop on deselect/close; final frame `active:false`. No HTTP fallback.
5. `search.rs`: **no migration, no FTS, no projection.** Implement the resumable bounded scan from
   spec §5:
   - Read `traffic_messages_zstd` / `traffic_flows_zstd` rows by descending `seq`, decode each row in
     Rust (`hydra_compression::decode(.., 65_536)`), skip invalid rows and count `undecodable`.
   - Match literal case-insensitive escaped input (`regex::escape` + `case_insensitive(true)`, 1 MiB
     limits), never user regex; search full decoded text.
   - Per request ≤5 000 rows, ≤750 ms, ≤32 MiB decoded; `next_cursor` = last **scanned**
     `(kind,seq,occurred_ms)`; coverage `{scanned,truncated,continuation,undecodable,decoded_only:true,
     raw_history_searched:false}`. Client auto-continues ≤3 units then shows explicit partial/More.
   - Prefer a read-only connection; otherwise chunk via `Database::run` so no mutex hold >750 ms.
   - Server-compute `flag_spans` from Core's visible configured `flag_pattern` as UTF-16 code units
     over the snippet and over selected `messages_page` items; `has_flag` stays the canonical
     flow-level finding predicate and spans never change it.
6. Do **not** add any migration, FTS table, trigger, backfill, `hooks` feature, progress handler or
   4 KiB projection. Existing zstd retention is the only lifecycle.
7. `diagnostics.rs`: add the `history` client (active → `GetActiveFlowHistory`, closed →
   `GetFlowHistory`); replace the handler-minted `Uuid::new_v4()` at `:228` with
   `crate::modules::audit::request_id()` and add one bounded WARN on history failure with
   `request_id`, flow id and mapped status (no payloads). This closes the traceability gap from
   `scratch/hydra-409-investigation.md`.
8. Append DECISIONS 037 (text below). Do not edit 036.

**Acceptance:** `cargo test -p hydra-core modules::traffic`, `scripts/check-contracts.sh` (byte-identical
OpenAPI), `python3 scripts/generate-frontend.py --check`.

---

## T3 — Frontend: page-scoped socket, search panel, active bytes, highlight

**Files (only these):**
- `frontend/src/shared/api/client.ts` (export `accessToken()`)
- `frontend/src/features/traffic/liveSocket.ts` (new)
- `frontend/src/features/traffic/live.ts` (add `liveEvent(value)`; keep `trafficEvent(frame)` for SSE)
- `frontend/src/features/traffic/highlight.ts` (new)
- `frontend/src/features/traffic/GlobalSearch.vue` (new)
- `frontend/src/features/traffic/TrafficPage.vue`, `TrafficWorkspace.vue`, `FlowConversation.vue`,
  `SessionList.vue` (badge only)
- `frontend/vite.config.ts` (`ws: true` on `/api`)
- `docs/traffic-view.md`, `docs/traffic-workspace.md`

**Steps**
1. `liveSocket.ts`: URL from `endpoint('/traffic/events/socket')` resolved against `location.origin`,
   `http→ws`/`https→wss`; subprotocol `['hydra.traffic.v1']`; token only in the `auth` frame; 50 s
   re-auth; backoff 1 s→5 s; generation discard; `loss→resync`; close-code map; state strings stay
   `connecting|live|retrying` so `#traffic-live-status` assertions keep passing.
2. `TrafficPage.vue`: replace `events(...)` with `openLive`; `snapshot`/`flows_page` → `flowRows` +
   `mergeFlows` (500 cap); "More" → `page(cursor)`; filters/service changes → `subscribe`. No HTTP
   data fallback for list/messages/search/history. On unmount: `close()`; on `document.hidden`:
   `pause()`/`resume()`.
3. `FlowConversation.vue`: `applySnapshot(items)` on `messages_page`; `select(flow_id, active)` on
   selection; render `active_bytes` with the reset/append/gap rules from spec §4.4 for both active
   (`active=true`, polling) and closed (`active=false`, `GetFlowHistory` result) flows. Show
   "Live capture (partial, bounded)" only while active. Do not call the HTTP history route and do not
   surface its 409 in the Streams UI; keep `historyState='active'` only for legacy/non-Streams use.
4. `GlobalSearch.vue`: query input (≥2 chars), scope selector, results list with service/time/flag
   badges, automatic ≤3 continuations then explicit "Partial results — more available" + `More`;
   coverage note from `coverage` ("decoded retained traffic only; raw capture not searched"),
   empty-state wording that does not claim absence. Selecting a result selects that flow and loads its
   messages.
5. `highlight.ts`: `segments(text, spans)` slices UTF-16 code units into plain/marked parts; used for
   search `snippet` and `messages_page` text with `flag_spans`; never `v-html`, never `new RegExp`
   over untrusted text. Keep `flagPattern.ts` only for any remaining local live-list preview.
6. `SessionList.vue`: flag badge only (reuse existing `has_flag` styling); no layout rewrite.
7. Docs: frame types, close codes, auth-frame rule, "token never in URL", search coverage/continuation
   limits, active/closed history semantics, UTF-16 spans.

**Acceptance:** `bun run typecheck && bun run test && bun run build` in `frontend/`.

---

## T4 — Tests (authored in parallel, disjoint files)

**Files (only these):**
- `apps/hydra-core/src/modules/traffic/ws_tests.rs`
- `apps/hydra-core/src/modules/traffic/search_tests.rs`
- `apps/hydra-proxy/src/history/active_tests.rs`
- `frontend/tests/live-socket.test.ts`
- `frontend/tests/highlight.test.ts`
- `tests/e2e/streams_workspace.py`

Use the real `axum::serve` on `127.0.0.1:0` + `tokio-tungstenite` client for WS tests; reuse
`traffic::tests::setup()` (`pub(super)`). Cover exactly the matrix in spec §8, including: origin 403,
auth deadline, revoked/expired 4401, sid switch, snapshot/live dedupe, per-actor cap, lag `loss`,
slow-consumer 1013, pause, closed-flow history over WS, search across an older page, literal-match
semantics, cursor-last-scanned, partial/continuation coverage, UTF-16 flag spans, invalid spans
dropped, `<`/`&` escaping, active-byte reset vs append, `get()` still `Active`. E2E must not depend on
a commit or on remote state; run against the local two-process smoke setup.

**Acceptance:** each file compiles and fails for the right reason before T1–T3 land, passes after.

---

## T5 — Runner (no edits)

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p hydra-proxy history
cargo test --locked -p hydra-core modules::traffic
scripts/check-contracts.sh
python3 scripts/generate-frontend.py --check
(cd frontend && bun run typecheck && bun run test && bun run build)
cargo build --locked -p hydra-core
python3 tests/e2e/streams_workspace.py
python3 tests/e2e/utf8_telemetry.py
```

Report: pass/fail per command, files changed, any skipped test with the reason. Do not commit or push.
If the full-suite `TMPDIR` failure appears, report it as a known sandbox limitation and use the
targeted commands; no workaround outside the scratch directory.

---

## DECISIONS 037 — append text

```markdown
## 037 — Page-scoped WebSocket for the Streams workspace (2026-11-23)

User amendment (explicit, 2026-11-23): live Streams-page updates, initial snapshot, messages, global
retained search and byte history (active and closed) travel over one authenticated page-scoped
WebSocket; flag highlight spans are computed for selected messages and search results. This amends
SPEC line 84 ("frontend live traffic via SSE") for `TrafficPage` only.

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
```
