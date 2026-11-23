# Traffic workspace

In **Services**, click a service's **Color** button. Choose one of twelve named presets, enter a custom `#RRGGBB` color, or reset to the automatic palette. Save applies to all users and survives restarts. It does not reconnect clients or change relay configuration. The service dropdown and stream-card stripe use the same color. Unconfigured services use distinct presets until all twelve have been used; you can deliberately reuse a configured color.

In **Traffic**, the checkbox inside each stream card selects that stream for filter creation. Clicking the rest of the card opens its conversation. Selection remains limited to fifty streams from one service.

On desktop, drag the divider between cards and the conversation. You can also focus the divider and use the arrow keys. **Expand traffic** fills the browser window and preserves the current card-column width. **Exit expanded view** or Escape returns to the regular layout. Live delivery, the selected session and filter selections continue across expansion.

Below 768px window width, the list sits above the conversation. When the conversation pane itself is at least 768px wide, client blocks align left and server blocks align right; narrower panes use the full available width. Blocks retain their chronological order and direction labels in decoded, saved UTF-8 and saved hex views.

**Flags**, **Submissions**, **Submission attempts** and Farm **Run history** start collapsed. Click a heading to open its list, pagination and controls. Closing a history stops its background list polling. Starting a Farm run opens its history automatically.

Service colors are saved in Core's SQLite database and are included in database backups. The networking configuration export does not contain this presentation setting. The public color API is documented in Swagger: `PUT /api/v1/services/{id}/color`, with `{"color":"#66d9e8"}` or `{"color":null}` to reset. No internal Proxy protocol changes are required.

## Search and live byte workspace

**Search all traffic** opens the retained-text panel. Enter at least two characters; input is debounced, literal and case-insensitive. Scope selects messages, flows or both, and the service selector defaults to all services. Results show service, time, direction, protocol, snippet and a flag badge when the flow has a retained finding. A result can open a flow that is outside the current list page. The panel states when coverage is partial and does not claim that a partial scan is complete.

The selected stream's **Captured bytes** view uses the same WebSocket. For an open stream it shows a bounded live tail per direction (64 KiB / 1,024 chunks) and labels it as partial; evicted head bytes produce a visible gap marker. For a closed stream the server sends the persisted history with `active=false`, so no HTTP poll and no 409 alert is involved. The list keeps at most 500 sessions and merges snapshots/events by flow ID without forcing scroll.

### Partial history failures

Core emits one generic `message` on every WebSocket `error` frame, so the `code` field, not the text, names the failing phase. `frontend/src/features/traffic/historyErrors.ts` maps exactly four retained-capture codes to the **Captured bytes** state; every other code, including `flow_unavailable`, stays a real error:

| `code` | Meaning | UI |
|---|---|---|
| `history_pending` | Core's bounded poll budget is spent; the Proxy is still saving the flow's history and Core stops polling | Terminal **Captured bytes not yet persisted** notice with manual **Refresh to try again** |
| `history_unavailable` | Active or closed history read failed, timed out or is unsupported | Terminal **Captured bytes unavailable** notice |
| `active_bytes_unavailable` | Active retained tail not served (no selection/socket, or Proxy reports the read unsupported) | Terminal **Captured bytes unavailable for this session** notice |
| `flow_not_found` | Closed history read returned NotFound (emitted only by the history path; flow metadata uses `flow_unavailable`) | Terminal **No retained capture is stored for this session** notice |
| `flow_unavailable` | Flow *metadata* read failed | Metadata-unavailable notice with retry |

`history_pending` is terminal, not a retry loop: Core emits it only after `HISTORY_RETRIES` and sets `history_done`, so the workspace marks the capture unavailable and offers **Refresh to try again** instead of staying in a loading state. An interim `active_bytes` snapshot with `pending=true` still counts as loading, because Core really is polling then.

These notices never claim packet, capture or byte loss. A failed history read is not evidence that bytes were dropped, and eviction is only ever reported by an actual snapshot gap marker. The notice carries the frame's `request_id` so the failure can be traced in Core logs; flow metadata and already loaded decoded messages stay visible, and the flow is never marked missing.
