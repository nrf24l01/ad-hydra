//! Client frame dispatch: auth, snapshot/paging, selection, search, resume.
use super::super::{
    Query,
    search::{SearchCursor, SearchQuery, SearchScope},
};
use super::{
    frames::{
        AuthFrame, MessagesFrame, PageFrame, ResumeFrame, ResyncFrame, SearchFrame, SelectFrame,
        SubscribeFrame,
    },
    history::{audit_selection, poll_history},
    outbound::{Outbound, emit_page, error_frame},
    searching,
    session::Session,
    subscribe::{authenticate, emit_selected, snapshot},
};
use crate::modules::traffic::Traffic;
use serde_json::{Value, json};
use std::time::Instant;

/// Returns a close instruction when the connection must end.
pub(super) async fn handle_frame(
    t: &Traffic,
    session: &mut Session,
    out: &Outbound,
    text: &str,
) -> Option<(u16, &'static str)> {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        error_frame(out, session.generation, "", "invalid_frame", false);
        return Some((4400, "invalid json"));
    };
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let request_id = value
        .get("request_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if request_id.len() > 64 {
        error_frame(out, session.generation, "", "invalid_frame", false);
        return Some((4400, "invalid request id"));
    }
    if session.actor.is_none() && kind != "auth" {
        error_frame(out, session.generation, &request_id, "unauthorized", false);
        return Some((4401, "auth required"));
    }
    session.trace(&request_id, &kind);
    match kind.as_str() {
        "auth" => {
            let Ok(frame) = serde_json::from_value::<AuthFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return Some((4400, "invalid auth frame"));
            };
            return authenticate(t, session, out, &frame.request_id, &frame.token).await;
        }
        "subscribe" => {
            let Ok(frame) = serde_json::from_value::<SubscribeFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            // Same validator as the HTTP list route; the frame-level error code
            // stays `invalid_frame` (established client contract).
            if frame.query.validated().is_err() {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_frame",
                    false,
                );
                return None;
            }
            if let Err(error) = snapshot(t, session, out, &frame.request_id, frame.query).await {
                error_frame(out, session.generation, &frame.request_id, error, true);
            }
        }
        "page" => {
            let Ok(frame) = serde_json::from_value::<PageFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            if frame.after.is_some_and(|after| after < 0) {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_query",
                    false,
                );
                return None;
            }
            let Some(query) = session.query.as_ref() else {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "not_subscribed",
                    true,
                );
                return None;
            };
            let mut page_query = query.query();
            page_query.after = frame.after;
            page_query.limit = Some(frame.limit.unwrap_or(100).clamp(1, 100));
            match t.flows(page_query).await {
                Ok(page) => emit_page(
                    out,
                    session.generation,
                    json!({
                        "type": "flows_page",
                        "request_id": frame.request_id,
                        "generation": session.generation,
                    }),
                    page.items,
                    page.next_cursor,
                    |item| item.get("seq").and_then(Value::as_i64),
                ),
                Err(_) => error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "query_failed",
                    true,
                ),
            }
        }
        "messages" => {
            let Ok(frame) = serde_json::from_value::<MessagesFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            if frame.flow_id.is_empty() || frame.flow_id.len() > 128 {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_frame",
                    false,
                );
                return None;
            }
            if frame.after.is_some_and(|after| after < 0) {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_query",
                    false,
                );
                return None;
            }
            let query = Query {
                after: frame.after,
                limit: Some(frame.limit.unwrap_or(100).clamp(1, 100)),
                ..Query::default()
            };
            match t.messages(frame.flow_id.clone(), query).await {
                Ok(page) => {
                    let items = page
                        .items
                        .into_iter()
                        .map(|item| super::session::annotate_message(t, item))
                        .collect::<Vec<_>>();
                    emit_page(
                        out,
                        session.generation,
                        json!({
                            "type": "messages_page",
                            "request_id": frame.request_id,
                            "generation": session.generation,
                            "flow_id": frame.flow_id,
                        }),
                        items,
                        page.next_cursor,
                        |item| item.get("stream_sequence").and_then(Value::as_i64),
                    );
                }
                Err(_) => error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "query_failed",
                    true,
                ),
            }
        }
        "select" => {
            let Ok(frame) = serde_json::from_value::<SelectFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            let Some(flow_id) = frame.flow_id else {
                session.clear_selection();
                return None;
            };
            if flow_id.is_empty() || flow_id.len() > 128 {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_frame",
                    false,
                );
                return None;
            }
            session.selected = Some(flow_id.clone());
            session.active_bytes = frame.active_bytes;
            session.cursor = None;
            session.history_done = !frame.active_bytes;
            session.history_retries = 0;
            session.closed_poll = false;
            session.selection_request = Some(frame.request_id.clone());
            if frame.active_bytes {
                // Audited sensitive read first: an audit failure stops the read.
                if !audit_selection(t, session, &flow_id, &frame.request_id).await {
                    error_frame(
                        out,
                        session.generation,
                        &frame.request_id,
                        "audit_unavailable",
                        false,
                    );
                    session.clear_selection();
                    return None;
                }
                session.last_poll = Instant::now();
                emit_selected(t, session, out, &frame.request_id, &flow_id).await;
                poll_history(t, session, out).await;
            } else {
                // Out-of-page search hits get the real flow record, never a
                // placeholder transport/closure state.
                emit_selected(t, session, out, &frame.request_id, &flow_id).await;
            }
        }
        "search" => {
            let Ok(frame) = serde_json::from_value::<SearchFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            if session.search.is_some() {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "search_busy",
                    true,
                );
                return None;
            }
            let Some(scope) = SearchScope::parse(frame.scope.as_deref().unwrap_or("both")) else {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "invalid_frame",
                    false,
                );
                return None;
            };
            let after = match frame.after.as_deref() {
                None => None,
                Some(raw) => match SearchCursor::parse(raw) {
                    Some(cursor) => Some(cursor),
                    None => {
                        error_frame(
                            out,
                            session.generation,
                            &frame.request_id,
                            "invalid_frame",
                            false,
                        );
                        return None;
                    }
                },
            };
            let query = SearchQuery {
                q: frame.q,
                scope,
                service_id: frame.service_id,
                since_ms: frame.since_ms,
                until_ms: frame.until_ms,
                has_flag: frame.has_flag,
                after,
                limit: frame.limit,
            };
            session.search = Some(searching::start(t, frame.request_id, query));
        }
        "resync" => {
            let Ok(frame) = serde_json::from_value::<ResyncFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            let Some(query) = session.query.clone() else {
                error_frame(
                    out,
                    session.generation,
                    frame.request_id.as_deref().unwrap_or_default(),
                    "not_subscribed",
                    true,
                );
                return None;
            };
            let id = frame
                .request_id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let _ = frame.reason;
            if let Err(error) = snapshot(t, session, out, &id, query).await {
                error_frame(out, session.generation, &id, error, true);
            }
        }
        "pause" => session.paused = true,
        "resume" => {
            let Ok(frame) = serde_json::from_value::<ResumeFrame>(value) else {
                error_frame(out, session.generation, &request_id, "invalid_frame", false);
                return None;
            };
            session.paused = false;
            let Some(query) = session.query.clone() else {
                error_frame(
                    out,
                    session.generation,
                    &frame.request_id,
                    "not_subscribed",
                    true,
                );
                return None;
            };
            if let Err(error) = snapshot(t, session, out, &frame.request_id, query).await {
                error_frame(out, session.generation, &frame.request_id, error, true);
            }
        }
        _ => {
            error_frame(out, session.generation, &request_id, "invalid_frame", false);
            return Some((4400, "unknown frame"));
        }
    }
    None
}
