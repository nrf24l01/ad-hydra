# Streams workspace over WebSocket — design (approved 2026-11-23)

Repo: `/home/Technik12345/vscodeprojects/ad-hydra`, branch `main`, baseline `fe46766d94b8e32f0950989ba7383300107336f8`
(`## main...origin/main`). This document is design only: no code, build, test, commit or push was performed.

## 0. Baseline and preserved work

Working tree already contained unrelated changes that this work MUST NOT touch or revert:

- modified: `DECISIONS.md`, `apps/hydra-proxy/src/grpc/mod.rs`, `apps/hydra-proxy/src/grpc/telemetry.rs`
- untracked: `apps/hydra-proxy/src/grpc/telemetry_batching_tests.rs`, `docs/team1-capture-verification.md`,
  `IDEA.md`, `.VSCodeCounter/`, `ad-test/`, `plugins/examples/crypter-grpc/`, `scratch/`,
  `tests/e2e/tcp_framing_capture.py`, `tests/e2e/utf8_telemetry.py`

`DECISIONS.md` already ends at 036 (normal telemetry batching). This design adds 037 by **appending**;
it does not renumber or edit 036. No `.env` or secret file is read or written. Full-suite `cargo test`
is known to fail in the sandbox on `TMPDIR` path length; only the targeted commands in §9 are gates.

## 1. Verified current state (source, not the previous draft)

| Fact | Evidence |
|---|---|
| Page is `/traffic` → `TrafficPage.vue` + `TrafficWorkspace.vue` + `SessionList.vue` + `FlowConversation.vue` | `frontend/src/app/router/index.ts`, `frontend/src/features/traffic/TrafficPage.vue` |
| Live updates are SSE only | `apps/hydra-core/src/modules/traffic/http.rs:103-140`, route `:153`; consumers `TrafficPage.vue:116`, `ConsoleLayout.vue:24`, `FilterStreamWorkspace.vue:48` |
| Fan-out is one global `broadcast::Sender<Value>` cap 128 | `repository.rs:76`, publish `:139`, `flag_detected` `:84` |
| Flow/message reads are `Traffic::flows/messages` over SQLite views, cursor `seq` | `repository.rs:143-247` |
| No free-text search exists; only exact `field_path`/`field_value` predicates | `repository.rs:225-246` |
| Traffic tables are zstd views (`traffic_flows`, `traffic_messages`, `traffic_events` → `*_zstd`) | `migrations/0025_decoded_service_zstd.sql` |
| Active-flow bytes ARE retained in Proxy memory, bounded 64 KiB per direction, 1024 chunks | `apps/hydra-proxy/src/history/mod.rs:20-35,92-103`; `history/tcp.rs:5-58` |
| Active flow read returns `HistoryReadError::Active` → gRPC `FailedPrecondition` → HTTP 409 | `history/mod.rs:479-495`, `grpc/diagnostics.rs:244-247`, `traffic/diagnostics.rs:285-293` |
| HTTP history 409 is already mapped to a benign `historyState='active'` alert | `FlowConversation.vue:85-86,137` |
| Flag pattern is a Core setting, Rust `regex` | `infrastructure/operations.rs:22-60`, `modules/flags/script_config.rs:20` |
| Client flag highlight is a deliberately tiny JS regex subset | `frontend/src/features/traffic/flagPattern.ts` |
| `axum` is 0.8 with default features (no `ws`); Vite proxy has no `ws: true` | workspace `Cargo.toml`, `frontend/vite.config.ts` |
| Prior 409 claim was not attributable: GET history mints its own audit UUID | `scratch/hydra-409-investigation.md:12-21` (in-repo copy), `diagnostics.rs:228` |

Correcting the earlier overclaim: `stream_sequence` is a per-message rowid and the proxy history
`sequence` is a **per-direction chunk counter**, not a byte offset and not a global cursor. Neither is
used as a resume cursor in this design.

## 2. Scope, and SPEC conflict resolution

In scope: one authenticated page-scoped WebSocket for the Streams page carrying initial flow snapshot,
pagination, selected-flow messages, live flow/message/flag events, live counters, global retained
search, and byte history (active and closed) for the selected stream; flag highlighting in both the
live list and search results.

Out of scope: deleting SSE, moving auth/static/downloads/bootstrap catalogs to WS, raw continuous packet
broadcast, browsing Proxy history files for search, new process/crate/daemon, any data-plane dependency
on Core.

**SPEC conflict (explicit user amendment).** `SPEC.md:84` says "Frontend live traffic via SSE; 60s access
token expiry requires renewal and reconnect." The user explicitly approved a page-scoped WebSocket on
2026-11-23 ("all should be sent via WebSockets on the streams view page", plus "search for all streams,
and flag highlight in answer"). Resolution recorded as DECISIONS 037:

- SSE remains implemented and supported for `ConsoleLayout`, `FilterStreamWorkspace`, and external
  consumers; only `TrafficPage` switches transport. SPEC line 84's requirement is therefore still met
  for every existing consumer.
- The 60 s access-token rule is preserved exactly: in-band re-auth every ≤50 s, server re-verifies every
  5 s, close on expiry/revocation.
- No OpenAPI change: WebSocket upgrade is not expressible in OpenAPI 3.1 and must not be annotated with
  `utoipa::path`.

## 3. Architectural contract

### 3.1 One connection, one page

Route `GET /api/v1/traffic/events/socket`, subprotocol `hydra.traffic.v1`, registered in
`traffic::router` next to SSE. Exactly **one** socket per Streams page instance, owned by
`TrafficPage.vue` and closed on unmount. A socket is bound to `(actor sub, session sid)`.

Upgrade path:
1. `Origin` must equal `AuthState.origin` (else 403 before upgrade).
2. Connection permit from the shared `Traffic::streams` semaphore (raised 8 → 16, shared with SSE).
3. `on_upgrade` only — never await the socket inside the request future (the `request_audit`
   middleware's 30 s timeout would kill it).
4. First frame must be `auth` within 5 s, else close 4401.
5. Token travels only inside the frame: never query string, never header echo, never logged.

Per-actor and global caps (all bounded, no global static):

| Bound | Value |
|---|---|
| Sockets per actor | 2 |
| Tracked actors | 128, zero-count entries evicted |
| Sockets total (WS + SSE) | 16 (unchanged; 2 per actor) |
| Inbound text frame (client→server) | 32 KiB, binary rejected (4400) |
| Outbound frame size | up to 1 MiB per frame (history/search pages carry `bytes`) |
| Outbound queue per socket | 4 MiB queued bytes / ≤64 frames, `try_send`; full → `loss` + close 1013 |
| Concurrent `search` per socket | 1 |
| `active_bytes`/history streams per socket | 1 (the selected flow only) |
| `active_bytes` push rate | ≤1/s, coalesced |
| Auth re-check | every 5 s; client re-auth every ≤50 s |

The 32 KiB limit is **inbound only**. Server→client frames may legitimately exceed it because history
segments and search snippets carry `bytes`/text; the outbound queue is therefore byte-bounded
(1 MiB per frame, 4 MiB queued per socket), not frame-count-bounded alone. Socket counts are unchanged:
16 total shared with SSE and 2 per actor.

Live state accounting (`LiveSockets`, owned by `Traffic`, not a `static`): permit holder, actor map,
and counters (`open`, `peak`, `dropped_frames`) exposed in the `counters` frame.

### 3.2 Frames (JSON text; all server frames carry `type`)

Client → server:

```json
{"type":"auth","request_id":"<uuid>","token":"<access JWT>"}
{"type":"subscribe","request_id":"<uuid>","query":{"service_id":null,"favorite":null,"include_blocked":false,"newest_first":true,"limit":100}}
{"type":"page","request_id":"<uuid>","after":12345,"limit":100}
{"type":"select","request_id":"<uuid>","flow_id":"<id>","active_bytes":true}
{"type":"select","request_id":"<uuid>","flow_id":null}
{"type":"messages","request_id":"<uuid>","flow_id":"<id>","after":987,"limit":100}
{"type":"search","request_id":"<uuid>","q":"flag{","scope":"both","service_id":null,"after":null,"limit":50}
{"type":"resync","request_id":"<uuid>","reason":"loss"}
{"type":"pause"}
{"type":"resume","request_id":"<uuid>"}
```

Server → client:

```json
{"type":"ready","protocol":1,"connection_id":"<uuid>","actor":"<sub>","expires_in_ms":41200,"server_time_ms":0}
{"type":"snapshot","request_id":"<uuid>","generation":1,"items":[],"next_cursor":null,"query":{}}
{"type":"flows_page","request_id":"<uuid>","generation":1,"items":[],"next_cursor":null}
{"type":"messages_page","request_id":"<uuid>","generation":1,"flow_id":"<id>","items":[],"next_cursor":null}
{"type":"flow","generation":1,"event":{}}
{"type":"message","generation":1,"flow_id":"<id>","event":{}}
{"type":"counters","generation":1,"flows_open":3,"flows_seen":812,"messages_seen":9401,"bytes_c2s":0,"bytes_s2c":0,"sockets":1,"at_ms":0}
{"type":"search_page","request_id":"<uuid>","generation":1,"items":[],"next_cursor":null,"coverage":{"scanned":0,"truncated":false,"continuation":false,"undecodable":0,"decoded_only":true,"raw_history_searched":false},"took_ms":12}
{"type":"active_bytes","request_id":"<uuid>","generation":1,"flow_id":"<id>","snapshot":{}}
{"type":"loss","generation":1,"dropped":7,"reason":"broadcast_lag"}
{"type":"error","request_id":"<uuid>","code":"invalid_frame","message":"...","retryable":false}
{"type":"reauth_required","reason":"expired"}
```

Rules:
- `request_id` is client-minted (UUIDv4, ≤64 chars) and echoed on every response to that request;
  server-side errors not tied to a request mint their own and include it. Tracing logs
  `connection_id` + `request_id` + actor, never token or payload bytes.
- `generation` increments on every `snapshot`/`resync`; the client discards frames with a generation
  lower than its current one. There is **no resume-by-sequence**; reconnect and `resync` always
  re-snapshot.
- `event` payloads for `flow`/`message` are byte-identical to today's SSE `traffic` data, so
  `live.ts` merge logic is reused.
- `select` always triggers a history read: an active flow is polled via `GetActiveFlowHistory`; a
  closed flow is read once via the existing `GetFlowHistory`. Both are delivered as `active_bytes`
  frames with `snapshot.active` reflecting the source, so the Streams page never calls HTTP history.
- Close codes: 4400 bad frame, 4401 auth/expired/revoked, 4403 origin, 4429 cap, 1013 slow consumer.
- `pause` (sent on `document.hidden`) stops server pushes and the active-bytes timer but keeps the
  socket and permit; `resume` re-snapshots. Unmount closes the socket, cancels the timer, and drops
  the selection server-side.

### 3.3 Snapshot handoff, dedupe, resync, loss

Handler order: `let mut rx = traffic.events.subscribe()` **before** querying; run
`Traffic::flows(query)`; send `snapshot`; then forward `rx`. Events that landed during the query are
queued and arrive after the snapshot; duplicates collapse client-side by `flow_id` / `message_id`
(`mergeFlows`, `mergeMessages`). A `flow_opened` for a flow already in the snapshot replaces it.

`RecvError::Lagged(n)` → `loss` frame, socket stays open, client sends `resync`; server emits a fresh
`snapshot` with generation+1 and the client replaces list state (bounded 500 rows) without forcing
scroll. Outbound queue full → best-effort `loss` then close 1013; the client reconnects with backoff
1 s → 5 s cap and re-subscribes with the current query.

Server-side filtering: `kind=="message"` forwarded only for the selected `flow_id`; flow events
filtered by the active `service_id`/`favorite`/`include_blocked` query. Flag events update `has_flag`
in place, they are not inserted as flows.

Counters are derived from the same broadcast (opened/closed/message/byte totals) and sent at most
every 2 s, plus once after each snapshot; they are display-only and never authoritative.

### 3.4 HTTP remains (compat and non-live concerns)

HTTP keeps: auth/login/refresh, static SPA, downloads, bootstrap catalogs (`/services`, `/protocols`,
`/system/settings`), favorites/capture/checker mutations, and the SSE endpoint unchanged for
`ConsoleLayout`, `FilterStreamWorkspace` and external consumers.

The Streams page has **no HTTP data fallback** for list, messages, search or history. Those four are
served only by the page-scoped WS. This includes closed-flow history: the WS serves it through the
existing `GetFlowHistory` RPC and delivers it as an `active_bytes` frame with `snapshot.active=false`.
`GET /flows/{id}/history` (including its 409 for active flows) remains implemented and unchanged for
legacy/external consumers; it is not called by `TrafficPage`/`FlowConversation`. `GET /flows/{id}`
metadata may remain for non-Streams callers, but the Streams page resolves selection over WS.

## 4. Byte history for the selected stream (active and closed)

### 4.1 Existing model (verified)

`HistoryStore` keeps `active: HashMap<String, Arc<Mutex<Buffer>>>` capped at 256 flows
(`history/mod.rs:124-136,325-358`). `Buffer` holds ordered `Datagram` chunks with a per-direction
sequence counter, capped at 64 KiB **per direction** and 1024 chunks (`history/tcp.rs:5-58`). Old data
is evicted from the head, and a partial drain can shorten the oldest chunk — so a chunk sequence is
not a byte offset. `get()` deliberately refuses active flows (`Active` → `FailedPrecondition` → 409).

### 4.2 Minimal backward-compatible proto

Add to `ProxyDiagnostics` (additive; no existing field/rpc changes, no generated code hand-edited):

```proto
rpc GetActiveFlowHistory(GetActiveFlowHistoryRequest) returns (GetActiveFlowHistoryReply);

message GetActiveFlowHistoryRequest {
  string flow_id = 1;
  Actor actor = 2;          // validated from Core auth context, never from user input
  string request_id = 3;    // Core request id, non-empty, ≤64 chars, for audit correlation
  ActiveBytesCursor cursor = 4; // optional; omit for a full tail snapshot
}
message ActiveBytesCursor {
  uint64 client_sequence = 1;
  uint64 server_sequence = 2;
  uint64 client_head_dropped = 3;
  uint64 server_head_dropped = 4;
}
message GetActiveFlowHistoryReply {
  string flow_id = 1;
  string service_id = 2;
  string protocol_id = 3;
  int64 opened_at_ms = 4;
  bool active = 5;
  repeated HistorySegment segments = 6;   // EXISTING message, see below
  bool truncated_client_to_server = 7;
  bool truncated_server_to_client = 8;
  uint64 retained_client_to_server_bytes = 9;
  uint64 retained_server_to_client_bytes = 10;
  bool reset = 11;                        // true => replace, do not append
  uint64 client_sequence = 12;
  uint64 server_sequence = 13;
  uint64 client_head_dropped = 14;
  uint64 server_head_dropped = 15;
  int64 generated_at_ms = 16;
}
```

`HistorySegment` is **not** a new type. The existing message at `proxy.proto:353-360` is reused
verbatim: `Direction direction = 1`, `bytes data = 2`, `bool datagram = 3`,
`int64 occurred_at_ms = 4`, `uint64 sequence = 5` — the same message used by
`GetFlowHistoryReply.segments = 8` and the existing Core `HistorySegment` DTO
(`apps/hydra-core/src/modules/traffic/diagnostics.rs:33-39`). Do not invent an
`ActiveHistorySegment` or reference a generated type that does not exist. Field numbers 1–16 above are
frozen as drafted; only the segment *type reference* is corrected to the existing message.

This is a diagnostic read, not a broadcast: the handler validates the caller-supplied `Actor`
against Core's authenticated actor (Core builds it from the verified JWT, never from a WS frame) and
requires a non-empty `request_id` before reading. Each call writes exactly one audit row; there is no
subscription, no push, and no raw packet broadcast.

`Buffer` gains `dropped: [u64; 2]` incremented on every head drain/eviction in `history/tcp.rs`
(currently only boolean truncation flags exist). `active_snapshot(flow_id, cursor)` locks the buffer,
copies at most the retained tail, and returns `reset=false` only when the caller's cursor matches both
last sequences and both head-dropped counters; otherwise `reset=true` with the full tail. A head drop
therefore forces a snapshot reset instead of a silent splice. It never persists, never allocates
beyond 128 KiB, and returns `active=false` for a flow that has closed (the WS then reads closed
history through the existing `GetFlowHistory` RPC).

`GetFlowHistory` and `get()` are untouched: a completed HTTP read of an active flow still yields
`Active`/409, which preserves the documented legacy behavior and existing tests.

### 4.3 Core and WS

`traffic/diagnostics.rs` gains `history(flow_id, cursor)` that calls `GetActiveFlowHistory` while the
flow is active and the existing `GetFlowHistory` once it is closed, reusing the existing
`HistoryReadError` mapping. The WS `select`:

- writes exactly one audit row `traffic.history.subscribe` (actor, flow_id, connection_id,
  request_id, outcome) — an explicit sensitive read, not per poll;
- for an active flow: starts a 1 s coalesced `GetActiveFlowHistory` poll while the socket is live,
  the flow is selected, and the tab is not paused; each reply is forwarded as one `active_bytes`
  frame; on `flow_closed` it stops polling, sends a final `active_bytes` with `active:false`, then
  reads the now-persisted history through `GetFlowHistory` and sends one more `active_bytes` frame
  with `snapshot.active=false`;
- for a flow already closed when selected: skips polling and sends the `GetFlowHistory` result as an
  `active_bytes` frame with `snapshot.active=false`;
- never calls HTTP for Streams history and never exposes the legacy 409 to the Streams UI.

UI state: an active flow with `active_bytes` enabled shows "Live capture (partial, bounded)"; a closed
flow shows the persisted history from the same frame type. The HTTP 409 alert and
`historyState='active'` remain only for the legacy HTTP route used by non-Streams consumers. No
continuous raw packet broadcast and no Core data-plane involvement: Core asks Proxy on demand; Proxy
never pushes bytes.

### 4.4 Stable offsets, truncation, gaps, no duplicate render

The client keeps two per-direction ring buffers keyed by chunk `sequence`:

- `reset:true` → replace the whole direction buffer, show `truncated_*` and any head-drop as an
  explicit "earlier bytes evicted" marker (never a silent splice).
- `reset:false` → append only segments whose `sequence` is greater than the last rendered sequence;
  an identical sequence with shorter data is a superseded head chunk and replaces in place.
- `client_head_dropped`/`server_head_dropped` growth produces a visible `gap` marker with the exact
  dropped byte count; byte totals always come from `retained_*_bytes`, never from summing rendered
  chunks after eviction.
- Rendering is bounded: at most 128 KiB per direction and 1024 chunks; no forced scroll-to-bottom.

## 5. Global retained-stream search

Search spans **all retained flows and messages stored in Core**, not the current page and not the
Proxy raw capture ring. It is a WS request (`search` frame) served by `Traffic::search(SearchQuery)`.
There is **no FTS table, no migration, no backfill and no projection**. Search is a resumable,
bounded Rust-side scan over the decoded zstd backing rows, with explicit continuation.

### 5.1 Source and scan

- Read `traffic_messages_zstd` / `traffic_flows_zstd` backing rows directly (`seq, id, flow_id,
  service_id, occurred_ms, data`), decode each `data` BLOB per row with
  `hydra_compression::decode(.., 65_536)`. **Do not** `SELECT hydra_text(data)`: it returns a SQLite
  error on invalid UTF-8 and would abort the whole query. A row that fails to decode is skipped and
  counted as `coverage.undecodable`; it never fails the request.
- Search the **full retained decoded text** (up to the existing 65 536-byte CHECK), not a 4 KiB
  projection or prefix. There is no projection column and no `truncated_rows` concept. The snippet is
  only a display window around the first match; matching itself sees the whole decoded row.
- Prefer a dedicated **read-only** SQLite connection (WAL permits concurrent readers) for search. If
  one cannot be opened, run each bounded chunk through the existing `Database::run` so no single
  shared-mutex hold exceeds the 750 ms budget; auth, ingest and SSE hydration can proceed between
  chunks. Never hold the global connection mutex for a full multi-chunk scan.
- At most 2 searches run concurrently (semaphore on `Traffic`, not a `static`); saturation returns a
  retryable `search_busy` error frame, never queues unboundedly.

Per-request budget (hard caps, all enforced per chunk): **≤5 000 source rows visited**, **≤750 ms**
wall time in the scan, **≤32 MiB decoded text**. A request that hits any cap returns its partial page
with `coverage.truncated=true`, `coverage.continuation=true` and `next_cursor` = the last row actually
scanned (not the last hit).

### 5.2 Query contract

`SearchQuery { q: String (2..=256 bytes, no NUL/C0), scope: messages|flows|both, service_id?,
since_ms?, until_ms?, has_flag?, after: Option<SearchCursor>, limit: 1..=50 }`.

Matching is **literal, case-insensitive substring**, never user regex:
`RegexBuilder::new(&regex::escape(q)).case_insensitive(true)` with 1 MiB `size_limit` /
`dfa_size_limit` and an empty-match guard. `\` and `.` are literals. Input is rejected, not
interpreted, if it contains control bytes.

Ordering is `occurred_ms DESC, kind, seq DESC`; `seq` is the table `INTEGER PRIMARY KEY AUTOINCREMENT`
(globally monotonic and never reused), so pagination is globally stable: inserting newer rows between
pages cannot duplicate or skip an older row. The cursor is opaque, encodes the **last scanned**
`(kind, seq, occurred_ms)` — `m:<seq>:<ms>` or `f:<seq>:<ms>` — and resumes strictly after it.
`next_cursor` is present whenever candidates remain or a cap was hit, even if the page has zero hits.
A page that finds `limit` hits before exhausting the budget also returns the last scanned position.

Response:

```json
{"type":"search_page","request_id":"…","generation":1,"items":[{
  "kind":"message","flow_id":"…","message_id":"…","service_id":"…","occurred_ms":0,
  "direction":2,"protocol_id":"utf8","snippet":"…","snippet_truncated":false,
  "spans":[[0,6]],"flag_spans":[[12,43]],"has_flag":true
}],"next_cursor":"m:98123:1700000000000","took_ms":12,
 "coverage":{"scanned":480,"truncated":false,"continuation":false,"undecodable":0,
             "decoded_only":true,"raw_history_searched":false}}
```

`coverage.scanned` is rows visited this request. `truncated` means a row/time/byte cap stopped the
scan; `continuation` means more candidates exist and `next_cursor` must be followed.
`decoded_only` is always `true` and `raw_history_searched` always `false` for this implementation —
they are explicit contract constants, not guesses.

**Continuation UX.** One `search` frame = one bounded unit. The client automatically issues up to 3
continuation requests (each a fresh bounded unit, still ≤5 000 rows / 750 ms / 32 MiB) to fill the
visible page. If the scan is still partial, the UI shows an explicit **"Partial results — more
available"** notice with a `More` button; it never silently presents a partial scan as complete.
Cancelling or closing the panel stops continuation immediately.

Honesty requirements (rendered in the UI): search covers decoded traffic still retained by Core;
it does **not** search Proxy raw history files, bytes rotated out by retention, or live tail not yet
committed to Core. Empty results with `coverage.decoded_only=true` must not be described as "no such
traffic anywhere". `raw_history_searched=false` is shown as such.

### 5.3 Flag highlight

One truth per context, and highlight never changes finding truth:

- **Selected messages (WS `messages_page`)**: the server annotates every item with `flag_spans`,
  computed from the **visible configured highlight pattern** (Core `operations.json` `flag_pattern`,
  the same value exposed by `GET /api/v1/system/settings`) over that item's full decoded text.
  Spans are **UTF-16 code-unit offsets** relative to the item's returned `text`/`snippet` string.
  This is a display annotation only: it does not create, suppress or reclassify findings.
- **Search results**: same server-side annotation over the snippet, same pattern source and same
  UTF-16 units.
- `has_flag` remains the canonical flow-level finding predicate
  (`flag_events` OR `traffic_favorites.has_flag`); a pattern match with no finding shows a highlight
  but not a `Flag detected` badge. The pattern source is labelled `core_settings (highlight pattern)`
  and never presented as the Proxy finding regex.
- If the pattern is absent or invalid, `flag_spans` is empty; the search `coverage` marks highlight
  unavailable and the messages response sets `highlight_status:"unavailable"`. The UI says
  highlighting is unavailable rather than implying no matches.
- Spans are capped at ≤64, sorted, non-overlapping, validated (`start < end <= length`); invalid spans
  are dropped. A flag outside the snippet yields `has_flag=true` with no `flag_spans` (documented).
- Rendering escapes by construction: Vue text interpolation plus slicing into `<mark>` segments — no
  `v-html`, no server-provided HTML, no `new RegExp` over untrusted text, no regex evaluation in the
  browser for server spans.

Existing flag UI is reused: `FlagRecords.vue` / `FlagsPage.vue` remain the flag manager;
`SessionList`/`FlowConversation` keep `has_flag` styling; search adds a `Flag` badge that links to the
flow, not a second flag store.

## 6. Interfaces to implement (frozen for parallel work)

Rust (Core):

```rust
// repository.rs
pub struct LiveSockets { /* permits, per-actor map, counters */ }
pub enum SearchScope { Messages, Flows, Both }
pub struct SearchQuery { pub q: String, pub scope: SearchScope, pub service_id: Option<String>,
  pub since_ms: Option<i64>, pub until_ms: Option<i64>, pub has_flag: Option<bool>,
  pub after: Option<SearchCursor>, pub limit: Option<usize> }
pub struct SearchCursor { pub kind: SearchKind, pub seq: i64, pub occurred_ms: i64 }
pub struct SearchCoverage { pub scanned: u64, pub truncated: bool, pub continuation: bool,
  pub undecodable: u64, pub decoded_only: bool, pub raw_history_searched: bool }
pub struct SearchPage { pub items: Vec<Value>, pub next_cursor: Option<String>,
  pub coverage: SearchCoverage, pub took_ms: u64 }
impl Traffic {
  pub async fn search(&self, q: SearchQuery) -> anyhow::Result<SearchPage>;
  // active => GetActiveFlowHistory; closed => existing GetFlowHistory; both delivered as one
  // active_bytes frame with snapshot.active reflecting the source.
  pub async fn history(&self, flow_id: String, cursor: Option<ActiveCursor>)
      -> anyhow::Result<Option<ActiveHistory>>;
}
```

WS module `ws.rs` exposes only `pub async fn socket(State<Traffic>, HeaderMap, WebSocketUpgrade) ->
Response` plus private per-connection state; no catch-all handler file, no new crate, no migration.

Frontend:

```ts
// shared/api/client.ts
export function accessToken(): string
// features/traffic/liveSocket.ts
export function openLive(opts: {query: LiveQuery; onFrame: (f: ServerFrame) => void;
  onState: (s: LiveState) => void}): {subscribe(q?): void; select(id: string|null, active: boolean): void;
  page(after: number): void; messages(flowId: string, after?: number): void; search(q: SearchQuery): void;
  resync(): void; pause(): void; resume(): void; close(): void}
// features/traffic/highlight.ts — spans are UTF-16 code units, sliced with String#slice
export function segments(text: string, spans: Array<[number, number]>): Array<{text: string; flag: boolean}>
```

## 7. File ownership (disjoint)

| Task | Owns (only these) |
|---|---|
| T1 Proxy backend | `contracts/hydra/proxy/v1/proxy.proto`, `apps/hydra-proxy/src/history/mod.rs`, `apps/hydra-proxy/src/history/tcp.rs`, `apps/hydra-proxy/src/grpc/diagnostics.rs` (active-history handler only) |
| T2 Core backend | `apps/hydra-core/src/modules/traffic/ws.rs` (new), `mod.rs`, `repository.rs`, `search.rs` (new), `diagnostics.rs` (history client + request-id fix), workspace `Cargo.toml` + `apps/hydra-core/Cargo.toml` (`ws` feature, `tokio-tungstenite` dev-dep), `DECISIONS.md` (append 037) |
| T3 Frontend | `frontend/src/shared/api/client.ts`, `features/traffic/liveSocket.ts` (new), `live.ts`, `highlight.ts` (new), `GlobalSearch.vue` (new), `TrafficPage.vue`, `TrafficWorkspace.vue`, `FlowConversation.vue`, `SessionList.vue` (badge only), `frontend/vite.config.ts`, `docs/traffic-view.md`, `docs/traffic-workspace.md` |
| T4 Test authors | `apps/hydra-core/src/modules/traffic/ws_tests.rs`, `search_tests.rs`, `apps/hydra-proxy/src/history/active_tests.rs` (new, included by `history/mod.rs` under `#[cfg(test)]` — coordinate: T1 adds the `mod` line), `frontend/tests/live-socket.test.ts`, `frontend/tests/highlight.test.ts`, `tests/e2e/streams_workspace.py` |
| T5 Runner | no source edits; runs gates, reports |

T4 may start from this contract immediately; it must not edit T1–T3 files. `ws.rs`/`search.rs` are new
files so T2 and T4 never collide. T1 adds the single `#[cfg(test)] mod active_tests;` line before T4
lands its file.

## 8. Tests required

| Layer | File | Must prove |
|---|---|---|
| Proxy unit | `history/active_tests.rs` | active snapshot returns bounded tail; `reset` on head drain; cursor append when unchanged; `active=false` after finish; `get()` still `Active`; actor/request-id validation rejects missing values |
| Core WS integration | `traffic/ws_tests.rs` | origin 403; 5 s auth deadline; expired/revoked → 4401; sid switch rejected; snapshot-then-live dedupe; service filter; select filtering; per-actor 2-socket cap; lag → `loss`; slow consumer 1013; `pause` stops pushes; unmount releases permit; closed-flow select returns `active_bytes` with `active=false`; no HTTP fallback call |
| Core search | `traffic/search_tests.rs` | cross-page (all retained) results; literal backslash/dot; case-insensitive; control bytes rejected; invalid-UTF-8 row skipped and counted `undecodable`; caps set `truncated`+`continuation`; cursor is last **scanned** row; pagination stable with newer inserts; scope filtering; UTF-16 spans with astral char; invalid pattern → empty spans; no FTS/migration assumption |
| Core history | extend `diagnostics.rs` test at `:519` | history subscribe audit has one row with `request_id == connection request id`; legacy HTTP 409 still 409 |
| Frontend unit | `frontend/tests/live-socket.test.ts` | frame order, re-auth ≤50 s, backoff, `loss→resync`, snapshot merge bounded 500, no token in URL, close-code mapping, generation discard |
| Frontend unit | `frontend/tests/highlight.test.ts` | span slicing escapes `<`, `&`, emoji offsets, overlapping/invalid spans dropped |
| E2E | `tests/e2e/streams_workspace.py` | page live over WS (`#traffic-live-status`), global search finds an old flow outside the visible page, flag `<mark>` appears, closed history loads over WS without HTTP, active stream shows partial live bytes without a 409 toast |

## 9. Acceptance commands

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p hydra-proxy history::active
cargo test --locked -p hydra-core modules::traffic
scripts/check-contracts.sh            # OpenAPI byte-identical (no new HTTP route)
python3 scripts/generate-frontend.py --check
cd frontend && bun run typecheck && bun run test && bun run build
cargo build --locked -p hydra-core
python3 tests/e2e/streams_workspace.py
python3 tests/e2e/utf8_telemetry.py   # SSE compatibility
```

Protobuf compatibility: `cargo build` regenerates in `OUT_DIR`; run `buf lint`/`buf breaking` against
the previous descriptor if available. Never hand-edit generated code.

## 10. Compatibility and risks

- Proto change is additive only; old Core↔new Proxy and new Core↔old Proxy both remain wire-compatible
  (new RPC simply unavailable → Core returns `error.code="history_unavailable"`).
- No SQLite schema change at all: no new table, no FTS index, no migration, no backfill. Search reads
  existing zstd backing rows and therefore inherits retention exactly (nothing stale to clean up).
- Search is a bounded scan, not an index lookup. Cost is explicit: ≤5 000 rows / 750 ms / 32 MiB per
  unit, at most 2 concurrent units, with automatic continuation capped at 3 extra units per user
  request. A deep search over a full 24 h retention window may require explicit `More` continuation;
  the UI says "Partial results" rather than pretending the scan is complete.
- Decoding happens per row in Rust, so binary/encrypted/unknown protocols contribute few or no hits
  and invalid-UTF-8 rows are skipped (`coverage.undecodable`) instead of aborting the query. The full
  retained decoded text is searched, not a 4 KiB preview.
- A dedicated read-only connection is preferred; if unavailable, chunked `Database::run` keeps each
  shared-mutex hold ≤750 ms so auth/ingest are not starved. This is a latency risk to monitor, not a
  correctness promise of zero contention.
- 409 behavior: HTTP history 409 preserved for legacy/external consumers; the Streams page never calls
  it and never surfaces a reference-bearing toast. The request-id mismatch for GET history
  (`diagnostics.rs:228` mints a fresh UUID) is fixed in T2 so future 409s are traceable.
- Outbound frames can exceed the 32 KiB inbound cap; the outbound queue is byte-bounded (1 MiB/frame,
  4 MiB/socket) so a large history snapshot cannot exhaust memory. Socket counts stay 16 total / 2 per
  actor.
- `axum` `ws` feature and `tokio-tungstenite` dev-dep add lock entries; no new runtime crate.
- Full workspace `cargo test` still hits the sandbox `TMPDIR` length limitation; use the targeted
  commands above and do not add a workaround outside the scratch directory.
