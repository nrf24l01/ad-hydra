//! Live event forwarding with subscription-scope filtering and counters.
use super::super::Traffic;
use super::{
    POLL_EVERY,
    outbound::Outbound,
    session::{Session, annotate_message, event_matches},
};
use serde_json::{Value, json};
use std::time::Instant;

pub(super) async fn handle_event(t: &Traffic, session: &mut Session, out: &Outbound, event: Value) {
    if session.paused {
        t.live.record_dropped();
        return;
    }
    let flow_id = event
        .get("flow_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let generation = session.generation;
    match event
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "message" => {
            if session.selected.as_deref() != Some(flow_id.as_str()) {
                return;
            }
            if !event_matches(t, session.query.as_ref(), &event).await {
                return;
            }
            session.counters.messages_seen += 1;
            out.emit(
                generation,
                json!({
                    "type": "message",
                    "generation": generation,
                    "flow_id": flow_id,
                    "event": annotate_message(t, event),
                }),
            );
        }
        "flag_detected" => {
            // Updates `has_flag` in place; never inserted as a flow. Filtered by
            // the same subscription scope as every other event.
            if !event_matches(t, session.query.as_ref(), &event).await {
                return;
            }
            out.emit(
                generation,
                json!({
                    "type": "flow",
                    "generation": generation,
                    "event": event,
                }),
            );
        }
        kind @ ("flow_opened" | "flow_closed" | "favorite_changed") => {
            if !event_matches(t, session.query.as_ref(), &event).await {
                return;
            }
            match kind {
                "flow_opened" => {
                    session.counters.flows_open += 1;
                    session.counters.flows_seen += 1;
                }
                "flow_closed" => {
                    session.counters.flows_open = session.counters.flows_open.saturating_sub(1);
                    session.counters.flows_seen += 1;
                    if let Some(bytes) = event
                        .get("bytes_client_to_server")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        session.counters.bytes_c2s =
                            session.counters.bytes_c2s.saturating_add(bytes);
                    }
                    if let Some(bytes) = event
                        .get("bytes_server_to_client")
                        .and_then(Value::as_str)
                        .and_then(|v| v.parse::<u64>().ok())
                    {
                        session.counters.bytes_s2c =
                            session.counters.bytes_s2c.saturating_add(bytes);
                    }
                    if session.selected.as_deref() == Some(flow_id.as_str()) && session.active_bytes
                    {
                        // Stop live polling; the next read returns persisted history.
                        session.cursor = None;
                        session.closed_poll = true;
                        session.history_done = false;
                        session.history_retries = 0;
                        session.last_poll = Instant::now()
                            .checked_sub(POLL_EVERY)
                            .unwrap_or_else(Instant::now);
                    }
                }
                _ => {}
            }
            out.emit(
                generation,
                json!({
                    "type": "flow",
                    "generation": generation,
                    "event": event,
                }),
            );
        }
        _ => {}
    }
}
