# Streams WebSocket frame contract (Core backend)

Route: `GET /api/v1/traffic/events/socket`, subprotocol `hydra.traffic.v1`.
Origin must equal the configured Core origin (403 before upgrade). The access token is sent only in
the first `auth` frame (never in the URL or a header), and the server closes 4401 if it is missing
within 5 s, expires, is revoked, or the session changes.

Frames are JSON text. Client frames are capped at 32 KiB; the server rejects binary frames and
oversized frames with 4400. Outbound frames may be larger (history/search carry bytes) but each frame
is ≤1 MiB and each socket queues ≤4 MiB / 64 frames. A frame that cannot be delivered is never
dropped silently: oversize frames produce a `loss` frame (`reason="frame_too_large"`) and a full
queue closes 1013 (`slow consumer`).

## Client → server

`auth`, `subscribe`, `page`, `select`, `messages`, `search`, `resync`, `pause`, `resume`.

`subscribe.query` carries the same filters as `GET /api/v1/traffic/flows` (`repository::Query`) and is
validated by the same function:

```
include_blocked, favorite, after, limit, newest_first, service_id, client_ip, since_ms, until_ms,
transport (1=tcp,2=udp), source_ip, destination_ip, direction (1=c2s,2=s2c), field_path,
field_value, matched_rule, has_flag
```

`limit` defaults to 100 and is clamped to 1..=100. Invalid filters produce an `error`
(`code="invalid_frame"`, non-retryable) and no snapshot. The snapshot and every live event use the
same filter; events that lack a field the filter needs (favorite/blocked/has_flag/advanced filters)
are reconciled against the committed flow record before forwarding, so `favorite=true` does not
silently lose lifecycle events and filtered sockets do not receive out-of-scope flows.

`select` accepts `{request_id, flow_id, active_bytes}`; `flow_id:null` clears the selection.

## Server → client

`ready`, `snapshot`, `flows_page`, `messages_page`, `selected`, `flow`, `message`, `counters`,
`search_page`, `active_bytes`, `loss`, `error`, `reauth_required`. Every response echoes the client
`request_id`; frames not tied to a request mint their own.

- `generation` increments on every `snapshot`/`resync`; discard lower generations.
- `snapshot`, `flows_page` and `messages_page` are byte-bounded below the 1 MiB frame cap: when the
  full page would not fit, trailing items are trimmed and `next_cursor` is the last returned item's
  global `seq`, so no row is skipped. A single item too large for the budget fails the request with
  `error code="frame_too_large"` (non-retryable); it is never a `loss`/resync or an empty page.
- `selected` is the authoritative selection confirmation and echoes
  `{type:"selected", request_id, generation, flow_id, flow}` where `flow` is the **real**
  `Traffic::flow` object (same shape as `GET /api/v1/traffic/flows/{id}`, including `favorite`,
  `blocked`, `has_flag`, `closed`, endpoints and `rule_hits`). It is emitted for every selection,
  including search hits outside the loaded page. An unknown or unreadable id is never a `selected`
  frame with `flow:null`: the socket emits `error code="flow_unavailable"` instead, never a
  fabricated placeholder.
- `flow`/`message` events are byte-identical to SSE `traffic` data plus, for messages,
  `display_text` and `flag_spans` (UTF-16 offsets into `display_text`). Lifecycle events keep the
  legacy `blocked`/`has_flag` decoration; message events remain the canonical SSE payload, with no
  derived `blocked`/`favorite`/`has_flag` injection. Streams reconciles missing visibility against
  the committed flow for subscription filtering instead.
- `search_page` items carry `snippet`/`spans`/`flag_spans`/`has_flag`; `coverage` always reports
  `decoded_only=true` and `raw_history_searched=false`. Matching scans the **full decoded row**
  (all decoded scalar values and field names, up to the retained 64 KiB row) — not
  `display_text`/`MAX_DISPLAY_CHARS` and not a 4 KiB prefix. String contents are matched literally
  (unescaped), so a query containing a backslash matches the decoded value. `snippet` remains a
  bounded display window around the first match. `truncated`/`continuation` say whether the bounded
  unit stopped and whether `next_cursor` must be followed; the cursor is the last row actually
  **scanned** (hit or not) and is only valid for the scope that produced it.
- `active_bytes.snapshot.active=true` means the bounded live tail; `active=false` means persisted
  history. `pending=true` means the flow closed but history is still being written — retried a
  bounded number of times, never surfaced as an HTTP 409. Consecutive unanswered polls (Proxy
  timeout) are bounded by the same counter: after 10 the socket emits
  `error code="history_unavailable"` with the selection `request_id` and stops polling.
- Highlight spans are display annotations from the configured Core highlight pattern; `has_flag`
  remains the finding truth and is never derived from spans. The pattern is recompiled when
  `/api/v1/system/settings` saves a new `flag_pattern`, so Streams highlighting is live.

## Audit

`select` with `active_bytes=true` writes one `traffic.history.subscribe` audit row before any read;
if the audit write fails the socket emits `error code="audit_unavailable"` and reads no bytes.
Every `search` attempt/result is audited with `{source, query_bytes, status, scanned, code}` — the
query text is never logged, stored or returned. Search runs in a spawned task with a hard 5 s
timeout, so ticks (token expiry, revocation) stay responsive while a search is in flight.

Close codes: 4400 invalid frame, 4401 auth/expired/revoked/sid switch, 4429 per-actor socket cap,
1013 slow consumer.
