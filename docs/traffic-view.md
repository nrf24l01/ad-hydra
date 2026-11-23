# Traffic view

Select a service above the session list, then select a TCP stream or UDP conversation to follow its
conversation. Each card has a left stripe with its configured service color, with automatic
palette colors for services without an explicit choice. Set colors in Services.
The service dropdown uses the same colors. Refreshes and pagination
keep those colors unchanged.

**Show blocked** is unchecked by default. Streams with an actual enforced HTTP
block, TCP close, UDP drop or exceeded blocking rate limit are hidden. Checking
it includes those streams and labels them **Blocked**. A TCP stream is classified
as blocked when any such action occurred in the connection, including a later
request in an existing connection. Shadow hits, alert/log matches, and ordinary
backend HTTP errors do not classify traffic as blocked. A live connection can
appear before a blocking decision arrives; it disappears when that decision is
received if Show blocked is unchecked.

Connections with a retained flag finding have an amber highlight and **Flag**
label; their conversation header shows **Flag detected**. Findings follow the
Proxy's flag regex. If `flag_pattern` is omitted from Proxy's JSON configuration,
the scanner uses the ForcAD format `[A-Z0-9]{31}=`. Set a custom regex for a
different format or set `"flag_pattern": null` to disable these markers.
Proxy reads this setting at startup. This classification does not imply submission or
expose flag values in the new notification. Blocked streams can also contain
findings: check Show blocked to inspect them. Defensive findings retain value-free flow/service metadata only; own_exposed flag values and digests are removed from the flag vault. Captured conversation bytes, traffic history and pre-migration backups can still contain observed traffic bytes.

## API and compatibility

Flow list/detail return boolean `blocked` and `has_flag` metadata. Message lists
also return these fields; message blocking uses matching message IDs or flow-wide
hits. `include_blocked=false` filters before pagination on flow/message lists.
Omitting it retains the API's previous behavior (include all). The console sends
it explicitly according to the checkbox. An existing `has_flag` query still works.

A committed critical finding emits redacted SSE `flag_detected` metadata containing
only kind, flow ID, service ID and `has_flag=true`. Flow lifecycle notifications
include retained classification, including findings arriving before normal flow
telemetry. Existing SSE loss/catch-up behavior remains in place.

Protobuf `RuleHit.blocked` (field 5, additive) indicates an actual blocking action,
including an exceeded rate limit. Legacy enforced block/drop hits are recognized
from their action codes. Older rate-limit hits cannot distinguish an exhausted
quota from a permitted request and remain unclassified. Upgrade both binaries to
classify new rate-limit events accurately. No storage migration is needed;
classification reads retained rule events and indexed flag provenance. Indicators
last as long as their underlying bounded event retention.

## UDP support

The default list includes TCP and UDP; cards identify their transport. UDP groups datagrams by client peer until the idle timeout (30 seconds by default). Incoming and outgoing decoded messages arrive through the same live events as TCP. The stream viewer, service selection, flag indicators and blocked checkbox work with these conversations. Closed UDP conversations support the same authenticated history API, raw views and saved-traffic filter preview as TCP. Replay feeds each retained datagram separately, preserving empty datagrams and arrival order; raw-byte matches never span datagram boundaries. A truncated capture reports a partial prediction.

Both transports use the same decoder and rule abstractions, including raw bytes, UTF-8 and compatible WASM fields. UDP `body.size`/`packet.size` count each datagram's bytes; `transfer.bytes` accumulates separately per direction within the UDP session. Enforced UDP blocking drops a datagram. TCP connection closing and HTTP-specific response blocking require their respective transports/protocols; unsupported combinations fail validation.

Completed UDP conversations retain zstd-compressed byte history after idle closure, service replacement or orderly shutdown. The viewer shows each complete retained datagram, direction, capture timestamp and monotonic capture sequence, including empty datagrams. Hex is the default; UTF-8 and ASCII are explicit views. Datagram text is never concatenated across boundaries. Retained parsed messages remain in Explorer under its event-retention policy; live capture still samples the next datagram. Raw UDP has no decoded protocol text by definition: choose UTF-8 or a suitable WASM decoder for parsed fields.

## Streams WebSocket

The Traffic page now opens one page-scoped WebSocket at `GET /api/v1/traffic/events/socket` with subprotocol `hydra.traffic.v1`. Flow snapshots, pagination, selected-flow messages, live flow/message/counter events, retained-text search and selected-stream byte history all arrive on that socket. Auth, service/protocol/settings bootstrap, favorites and other mutations remain HTTP. Other SSE consumers are unchanged.

The access token is sent only in the first in-band `auth` frame (within 5 seconds); it is never placed in the URL. The client re-authenticates every 45 seconds, before the 60-second access token expires. If the socket drops it reconnects with bounded 1–5 second backoff, re-subscribes the current filters and re-selects the open stream. Live status is shown only after the server's `ready` frame and a successful Core reachability check. Hiding the tab pauses server pushes; showing it resumes and re-snapshots.

Search covers decoded flows and messages still retained by Core. It is a bounded server-side scan with literal, case-insensitive matching, not a regex and not the current visible page. Proxy raw capture files, rotated bytes and uncommitted live tail are not searched. When a scan stops at its row/time/byte cap, the panel says **Partial results — more available** and offers **More**; it never presents a partial scan as complete. Empty results are described as no matches in the scanned retained range, not as proof the text never existed.

Selected live streams show a **Live capture (partial, bounded)** label and the retained tail per direction. Each direction keeps at most 64 KiB and 1,024 chunks; earlier bytes are reported as an explicit eviction/gap marker, never silently spliced. Closed flows use the same frame type with `active=false`. Server flag spans are UTF-16 offsets over `display_text`/snippet text and are rendered as escaped text plus `<mark>` segments; they are display annotations, while the **Flag detected** badge still comes from the separate finding predicate.

## Saved TCP and UDP history limits

Services with retention enabled retain a compressed tail of up to 64 KiB per direction after a flow closes. UDP evicts complete oldest datagrams to fit this bound, with a separate 1,024-datagram limit per flow (empty datagrams also consume a record). Retained sequence gaps and per-direction truncation flags identify capture loss; observed datagram counts include evicted and blocked datagrams. Datagram timestamps are wall-clock capture times, while sequence numbers define receive order even if the clock changes. TCP retains ordered read chunks for new captures; these are application read boundaries, not original packet boundaries or packet timing. Both transports capture before rule enforcement, so history may include blocked traffic; forwarded byte totals exclude it. Each service has its own disk budget, default **128 MiB**, covering compressed payloads and JSON metadata. Change **Settings → Traffic history → Saved traffic per service (MiB)** to any whole value from 1 to 1024. Saving applies live and persists in the Core configuration file; disconnected Proxy receives the latest value when it reconnects. Lowering the limit rotates each service's oldest records immediately. A busy service never evicts another service's history to meet its quota.

History also expires after 24 hours. A global 65,536-record index ceiling bounds memory/inode growth: when full, only the writing service may rotate its own records; if it has none, the new capture is rejected and loss is reported. Filesystem allocation adds overhead beyond the accounted payload and metadata bytes. The nonblocking writer queue holds 64 completed streams; overload reports capture loss instead of delaying forwarding. Active capture admission is capped at 256 flows; overflow and asynchronous persistence failure increment Proxy `history_dropped`, while closure metadata reports known incomplete captures. A successful closure event cannot guarantee a later disk write will succeed. Existing legacy TCP gzip archives are indexed by their saved service IDs on startup without conversion; missing transport metadata means TCP. UDP payloads carry bounded binary datagram framing and additive sidecar fields. New history responses add transport and datagram metadata without changing existing field numbers. Upgrade both binaries together for UDP history; older readers do not understand the new zstd framing.

The HYDRA header turns red when Core reports Proxy disconnected or the status request fails. It returns to its normal color after a successful reconnect. Connection status and telemetry counters refresh every five seconds; live traffic events do not override the connection state.

The writer indexes completed files once at startup; appending and expiry do not rescan the history directory. Its private directory must not be edited while Proxy runs. Restart rebuilds the index; on-demand reads still validate permissions and bounded decompression. See [performance measurements](PERFORMANCE.md).

## Terminal stream view (2026-10-22)

The Stream view selector has three modes: **Captured bytes**, **Decoded conversation**, and **Stream · terminal**. UTF-8 services open in the terminal by default; Raw still opens as hexadecimal bytes. Services listening on7117 (aquarius) and13345 (ark) already use UTF-8, so no backend port/profile change is necessary.

The terminal uses [xterm.js](https://xtermjs.org/docs/api/terminal/classes/terminal/) to render captured prompts, input and output, ANSI colors, carriage returns, backspaces and cursor controls. It is read-only: it has no shell, PTY or network input connection. No outgoing-data, clipboard or link-opening addon is attached; OSC links have a no-op handler. Bare LF is displayed as a line break from a non-PTY source. Both directions share one capture-order transcript; the direction selector can show either side separately. Decoded fields and raw hex remain available separately.

Live playback consumes parsed `utf8.text` events, never a raw packet gRPC stream. Indexed messages and SSE expose `stream_sequence`, the committed message insertion order, so packets sharing a millisecond retain their order. Duplicate events/messages do not duplicate displayed text. Xterm writes are serialized with one coalesced pending transcript, so direction changes and live-to-saved replacement cannot leave queued text from the previous view.

New TCP histories retain up to1024 ordered read chunks and the existing64KiB per direction. They use the same bounded framed compressed payload as UDP with an `ordered_tcp` sidecar flag; API segments have sequence/timestamp metadata and `datagram=false`. UDP keeps its datagram semantics. Raw transfer-summary messages remain metadata when a service's decoder changes; saved byte history supplies the readable fallback when available. Traffic Explorer also checks for a saved Proxy capture when Core still lists a flow as open, because its close event may have been lost. Core's state remains Open; forwarded totals come from the archive, and no close reason is inferred.

Proxy retains at most64KiB per direction, so an available archive may be partial even when its write completed. A decoder warning identifies known response parsing failures and links to Captured bytes; it does not promise that the full response was parsed. A closed stream shows a concise label for recognized socket failures, such as an upstream connection refusal. Unknown close details are not exposed, and previously unavailable or expired bytes cannot be reconstructed. Old gzip/zstd TCP files remain readable; without recorded interleaving, the terminal uses ordered saved UTF-8 messages when available, otherwise asks for a single direction rather than inventing conversation order. Downgrading Proxy to an older binary cannot interpret the new ordered TCP payloads; back up histories before a downgrade.

Terminal playback retains at most256K text code units and2000 scrollback lines (with bounded viewport dimensions). Capture loss and partial history stay visible. Scrolling away pauses following; **Follow output** returns to the bottom. The same component runs in Traffic, expanded mode, and the filter stream preview. The renderer and resize addon are lazy-loaded from static assets embedded in Core; no deployment Node process is added.


### Direction colors and invalid UTF-8

Terminal text uses red for client input and blue for server output. Invalid UTF-8 is displayed as yellow bracketed hex, for example `[ff]`; a valid UTF-8 replacement character remains ordinary text. Direction colors take precedence over payload SGR colors. Cursor controls remain supported; payload SGR and terminal string commands (including OSC) are consumed. The on-screen legend identifies the colors.

New live UTF-8 events optionally include `utf8.spans`: an ordered list of text values and exact invalid-byte values. `utf8.text` stays compatible with replacement-character decoding for existing consumers and rules. Each message is bounded to4096 text bytes and64 spans. Saved history validates the retained bytes independently, including incomplete final sequences, and respects per-direction UTF-8 state. Old parsed-only events cannot recover bytes already replaced by the older decoder; their saved byte history can. Incomplete final code points appear as hex when closed history is loaded.

### Filter match direction

Every condition row has its own **Direction** selector: Both directions (default), Client → server, or Server → client. Each choice persists on edit and applies to TCP/UDP live enforcement and saved-stream previews. Different rows can use different sides. Outside its selected side, a condition is inapplicable even when inverted or inside a NOT group. Conditions evaluate one message at a time: use OR for a client-only condition or a server-only condition; AND between opposite exclusive directions cannot match one message. There is no request/response correlation state. Existing whole-filter direction guards restore as matching direction values on every condition row when edited. On TCP, Block transfer closes the full connection; Block HTTP request remains a client-request action.


## Decoded history storage (2026-10-23)

Decoded retention is **256MiB per service**, compressed using **zstd level3**. This replaces the former shared64MiB decoded JSON budget and independent global row sweeps. Session metadata, events and parsed messages use compressed SQLite backing tables with bounded JSON views. Compression is fixed for decoded records; Settings distinguishes it from the configurable farm-log and captured-byte compression levels. Payload byte accounting excludes SQLite page/index overhead.

Rotation affects only the service over its quota and stops after freeing enough space. Old non-favourite closed sessions and their messages/events are removed together. If only active sessions remain, their oldest messages/events are trimmed while keeping cards and marking decoded history partial. Favourite admission limits remain128sessions,128messages and900,000decoded bytes per favourite; they are protected from rotation. Captured byte archives retain their existing separate per-service disk budget/TTL and may have different history coverage.

Migration25 compresses existing rows, restores missing parsed-message indexes from complete retained message events in event order, and recovers session cards when retained opening telemetry supports them. It cannot recover data deleted from every retained copy. Recovered old cards are newly indexed during migration; their displayed timestamps retain the original capture time. Message fields, pagination, saved-field filtering and stream previews use the same public API. Administrative SQLite tools can inspect `traffic_*_zstd` backing tables directly for counts/compressed sizes; logical `traffic_flows`, `traffic_messages`, and `traffic_events` views need Core's bounded `hydra_text` SQLite function. Downgrades require restoring the pre-migration SQLite backup.


Per-condition direction serializes as `{ "direction": "client_to_server", "scoped": { "field": "utf8.text", "op": "contains", "value": { "type": "string", "value": "hello" } } }`. Server direction uses `server_to_client`; Both directions omits the wrapper. Scope wraps the condition including any condition negation. Core and Proxy must both support the additive rule-expression wire node. Upgrade Proxy first, then Core. Older binaries cannot load rules containing the new scoped expression; remove those scopes or restore the pre-upgrade configuration before downgrading.


## HTTP inspection layout

HTTP request/response messages in Decoded conversation show a method/target or status/reason headline, ordered header rows, repeated decoded query parameters, optional trailers and the retained body. Headers are visible by default and retain original spelling, duplicates and value whitespace. The request target is shown unchanged above its decoded query parameters. JSON bodies offer Formatted JSON and Text: formatting preserves original numeric tokens, duplicate keys, escape sequences and string content; Text is the exact retained preview. Captured HTML is displayed as text. Truncation and absent binary/encoded text previews are explicit; Captured bytes opens the existing byte viewer. Original protocol fields remain under Inspect fields. This same component appears in filter stream previews.

This is presentation of existing decoder output: HTTP framing support, body preview limits, retained byte tails and opaque/encrypted passthrough are unchanged. Pretty JSON accepts at most16,384 UTF-16 code units of input,32 nesting levels and65,536 UTF-16 code units of output; header and query tables inspect at most128entries, with explicit truncation notices.

## Services and TCP mirroring

Services shows Name, a square-only Color control, Transport, Listener, Destination, Decoder and Buttons. Edit can set a friendly name; existing IDs and historical records remain stable. Names also appear in service selectors and metric headings. Click the colored square for the existing palette/custom color dialog. Status is shown with the name.

Click **TCP mirroring** on the service row to open its existing forwarding settings. Enable it explicitly, choose an enabled Farm team and target port, then configure the flag pattern/framing and Save service. Requests go to that configured team with matching flag substitutes; known substitutes are restored in replies. Opening the panel leaves mirroring disabled when it was disabled. Available TCP framing remains HTTP/1.1, delimited or length-prefixed; unsupported TLS/compressed/signed payload rewrites still need an adapter. UDP uses the Forwarding action and datagram framing. Destination displays the effective configured endpoint.
