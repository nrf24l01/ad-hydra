//! Selected-flow active-byte history: audited subscribe and bounded polling.
use super::super::{
    Traffic,
    diagnostics::{ActiveCursor, HistoryFailure, read_active},
};
use super::{
    outbound::{Outbound, error_frame},
    session::Session,
};
use crate::modules::{audit, auth::now};
use serde_json::json;
use std::time::Duration;

pub(super) const HISTORY_RETRIES: u8 = 10;
pub(super) const POLL_TIMEOUT: Duration = Duration::from_millis(1500);

/// One explicit audited sensitive read per selected subscribe. A failure must
/// stop the read: no bytes are polled without a `traffic.history.subscribe` row.
pub(super) async fn audit_selection(
    t: &Traffic,
    session: &Session,
    flow_id: &str,
    request_id: &str,
) -> bool {
    let Some(actor) = session.actor.clone() else {
        return false;
    };
    let flow_id = flow_id.to_owned();
    let request_id = request_id.to_owned();
    t.auth
        .db
        .run(move |db| {
            let tx = db.transaction()?;
            audit::append(
                &tx,
                &actor,
                "traffic.history.subscribe",
                &flow_id,
                &request_id,
                true,
                now(),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .is_ok()
}

pub(super) async fn poll_history(t: &Traffic, session: &mut Session, out: &Outbound) {
    let (Some(flow_id), Some(socket)) = (
        session.selected.clone(),
        t.proxy_socket().map(std::path::Path::to_path_buf),
    ) else {
        error_frame(
            out,
            session.generation,
            session.selection_request.as_deref().unwrap_or_default(),
            "active_bytes_unavailable",
            false,
        );
        session.history_done = true;
        return;
    };
    let Some(actor) = session.actor.clone() else {
        return;
    };
    let request_id = session
        .selection_request
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let cursor = if session.closed_poll {
        None
    } else {
        session.cursor
    };
    let result = tokio::time::timeout(
        POLL_TIMEOUT,
        read_active(&socket, flow_id.clone(), actor, request_id.clone(), cursor),
    )
    .await;
    let generation = session.generation;
    match result {
        // A slow Proxy is bounded like a pending history write: after
        // `HISTORY_RETRIES` consecutive unanswered polls, surface an
        // actionable error and stop the selection timer instead of polling
        // forever. Any success or new selection resets the counter.
        Err(_) => {
            session.history_retries += 1;
            if session.history_retries >= HISTORY_RETRIES {
                error_frame(out, generation, &request_id, "history_unavailable", false);
                session.history_done = true;
            }
        }
        Ok(Ok(history)) => {
            session.cursor = Some(ActiveCursor {
                client_sequence: history.client_sequence,
                server_sequence: history.server_sequence,
                client_head_dropped: history.client_head_dropped,
                server_head_dropped: history.server_head_dropped,
            });
            session.history_retries = 0;
            session.closed_poll = false;
            if !history.active {
                session.history_done = true;
            }
            out.emit(
                generation,
                json!({
                    "type": "active_bytes",
                    "request_id": request_id,
                    "generation": generation,
                    "flow_id": flow_id,
                    "snapshot": history,
                }),
            );
        }
        Ok(Err(HistoryFailure::Pending)) => {
            session.history_retries += 1;
            out.emit(
                generation,
                json!({
                    "type": "active_bytes",
                    "request_id": request_id,
                    "generation": generation,
                    "flow_id": flow_id,
                    "snapshot": {
                        "flow_id": flow_id,
                        "active": false,
                        "pending": true,
                        "segments": [],
                        "reset": true,
                    },
                }),
            );
            if session.history_retries >= HISTORY_RETRIES {
                error_frame(out, generation, &request_id, "history_pending", true);
                session.history_done = true;
            }
        }
        Ok(Err(HistoryFailure::NotFound)) => {
            error_frame(out, generation, &request_id, "flow_not_found", false);
            session.history_done = true;
        }
        Ok(Err(HistoryFailure::Unavailable(code))) => {
            error_frame(out, generation, &request_id, code, true);
            session.history_done = true;
        }
    }
}
