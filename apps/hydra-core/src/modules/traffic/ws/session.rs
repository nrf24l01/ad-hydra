//! Per-connection session state and live-event filter evaluation.
use super::super::{LiveCounters, Query, diagnostics::ActiveCursor, repository::LiveLease, text};
use super::{frames::LiveQuery, searching::SearchJob};
use crate::modules::{audit, auth::now};
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::Receiver;

/// Upper bound for one live-event reconciliation query.
const MATCH_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Default)]
pub(super) struct Counters {
    pub(super) flows_open: i64,
    pub(super) flows_seen: u64,
    pub(super) messages_seen: u64,
    pub(super) bytes_c2s: u64,
    pub(super) bytes_s2c: u64,
}

pub(super) struct Session {
    pub(super) connection_id: String,
    pub(super) actor: Option<audit::Actor>,
    pub(super) lease: Option<LiveLease>,
    pub(super) token: String,
    pub(super) sub: String,
    pub(super) sid: String,
    pub(super) expires_at: i64,
    pub(super) generation: u64,
    pub(super) query: Option<LiveQuery>,
    pub(super) rx: Option<Receiver<Value>>,
    pub(super) selected: Option<String>,
    pub(super) active_bytes: bool,
    pub(super) paused: bool,
    pub(super) selection_request: Option<String>,
    pub(super) cursor: Option<ActiveCursor>,
    pub(super) history_done: bool,
    pub(super) history_retries: u8,
    pub(super) closed_poll: bool,
    pub(super) search: Option<SearchJob>,
    pub(super) counters: Counters,
    pub(super) last_counters: Instant,
    pub(super) last_poll: Instant,
    pub(super) last_heartbeat: Instant,
}

impl Session {
    pub(super) fn new() -> Self {
        Self {
            connection_id: uuid::Uuid::new_v4().to_string(),
            actor: None,
            lease: None,
            token: String::new(),
            sub: String::new(),
            sid: String::new(),
            expires_at: 0,
            generation: 0,
            query: None,
            rx: None,
            selected: None,
            active_bytes: false,
            paused: false,
            selection_request: None,
            cursor: None,
            history_done: true,
            history_retries: 0,
            closed_poll: false,
            search: None,
            counters: Counters::default(),
            last_counters: Instant::now(),
            last_poll: Instant::now(),
            last_heartbeat: Instant::now(),
        }
    }

    pub(super) fn trace(&self, request_id: &str, action: &str) {
        tracing::debug!(
            connection_id = %self.connection_id,
            actor = %self.sub,
            request_id,
            action,
            "traffic socket"
        );
    }

    /// Clear the selection and any active-history polling state.
    pub(super) fn clear_selection(&mut self) {
        self.selected = None;
        self.active_bytes = false;
        self.history_done = true;
        self.history_retries = 0;
        self.cursor = None;
        self.closed_poll = false;
        self.selection_request = None;
    }
}

/// Evaluate a published event against the subscription filter.
///
/// Cheap checks (service, time range) use the event itself. Fields that an
/// unhydrated event does not carry are reconciled against the committed flow
/// record, bounded by `MATCH_TIMEOUT`; a failed reconciliation fails closed
/// (the event is not forwarded) instead of leaking out-of-scope flows.
pub(super) async fn event_matches(
    t: &super::super::Traffic,
    query: Option<&LiveQuery>,
    event: &Value,
) -> bool {
    let Some(live) = query else {
        return false;
    };
    let Ok(filter) = live.validated() else {
        return false;
    };
    if let Some(service) = filter.service_id.as_deref()
        && event.get("service_id").and_then(Value::as_str) != Some(service)
    {
        return false;
    }
    if let Some(occurred_ms) = event.get("occurred_ms").and_then(Value::as_i64)
        && (filter.since_ms.is_some_and(|since| occurred_ms < since)
            || filter.until_ms.is_some_and(|until| occurred_ms > until))
    {
        return false;
    }
    if filter.include_blocked == Some(false)
        && event.get("blocked").and_then(Value::as_bool) == Some(true)
    {
        return false;
    }
    if let Some(favorite) = filter.favorite
        && let Some(value) = event.get("favorite").and_then(Value::as_bool)
        && value != favorite
    {
        return false;
    }
    if let Some(has_flag) = filter.has_flag
        && let Some(value) = event.get("has_flag").and_then(Value::as_bool)
        && value != has_flag
    {
        return false;
    }
    if !needs_reconcile(&filter, event) {
        return true;
    }
    let flow_id = event
        .get("flow_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if flow_id.is_empty() {
        return false;
    }
    match tokio::time::timeout(MATCH_TIMEOUT, t.flow_matches(filter, flow_id)).await {
        Ok(Ok(matches)) => matches,
        Ok(Err(error)) => {
            tracing::debug!(error = %error, "live event filter reconciliation failed");
            false
        }
        Err(_) => {
            tracing::debug!("live event filter reconciliation timed out");
            false
        }
    }
}

/// True when the live filter needs committed flow state that a published
/// event does not carry (favorite, blocked, has_flag, or an advanced filter).
fn needs_reconcile(filter: &Query, event: &Value) -> bool {
    if filter.favorite.is_some() && event.get("favorite").is_none() {
        return true;
    }
    if filter.include_blocked == Some(false) && event.get("blocked").is_none() {
        return true;
    }
    if filter.has_flag.is_some() && event.get("has_flag").is_none() {
        return true;
    }
    filter.transport.is_some()
        || filter.client_ip.is_some()
        || filter.source_ip.is_some()
        || filter.destination_ip.is_some()
        || filter.matched_rule.is_some()
        || filter.direction.is_some()
        || filter.field_path.is_some()
}

/// Shared display annotation for one message event.
pub(super) fn annotate_message(t: &super::super::Traffic, mut item: Value) -> Value {
    let text = text::display_text("message", &item);
    let highlight = t.highlight();
    let spans = text::flag_spans(&text, highlight.as_ref());
    if let Some(object) = item.as_object_mut() {
        object.insert("display_text".into(), text.into());
        object.insert("flag_spans".into(), serde_json::json!(spans));
    }
    item
}

/// Counters frame; `sockets` is process-wide open sockets.
pub(super) fn counters_frame(session: &Session, sockets: LiveCounters) -> Value {
    serde_json::json!({
        "type": "counters",
        "generation": session.generation,
        "flows_open": session.counters.flows_open.max(0),
        "flows_seen": session.counters.flows_seen,
        "messages_seen": session.counters.messages_seen,
        "bytes_c2s": session.counters.bytes_c2s,
        "bytes_s2c": session.counters.bytes_s2c,
        "sockets": sockets.open,
        "at_ms": now().saturating_mul(1000),
    })
}
