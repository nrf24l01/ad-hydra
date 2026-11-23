//! Authentication handshake and subscription snapshot.
use super::super::Traffic;
use super::{
    frames::LiveQuery,
    outbound::{self, Outbound, error_frame},
    session::{Session, counters_frame},
};
use crate::modules::auth::now;
use serde_json::json;
use std::time::Instant;

pub(super) async fn authenticate(
    t: &Traffic,
    session: &mut Session,
    out: &Outbound,
    request_id: &str,
    token: &str,
) -> Option<(u16, &'static str)> {
    if token.is_empty() || token.len() > 4096 {
        error_frame(out, session.generation, request_id, "unauthorized", false);
        return Some((4401, "invalid token"));
    }
    let actor = match t.auth.authenticate(token).await {
        Ok(actor) => actor,
        Err(_) => {
            error_frame(out, session.generation, request_id, "unauthorized", false);
            return Some((4401, "unauthorized"));
        }
    };
    let claims = match t.auth.decode_at(token, "access", now()) {
        Ok(claims) => claims,
        Err(_) => {
            error_frame(out, session.generation, request_id, "unauthorized", false);
            return Some((4401, "unauthorized"));
        }
    };
    if session.actor.is_some() && (session.sub != claims.sub || session.sid != claims.sid) {
        error_frame(
            out,
            session.generation,
            request_id,
            "session_changed",
            false,
        );
        return Some((4401, "session changed"));
    }
    if session.actor.is_none() {
        let Some(lease) = t.live.acquire(&claims.sub) else {
            error_frame(out, session.generation, request_id, "capacity", false);
            return Some((4429, "socket cap"));
        };
        session.lease = Some(lease);
    }
    session.actor = Some(actor);
    session.token = token.to_owned();
    session.sub = claims.sub.clone();
    session.sid = claims.sid.clone();
    session.expires_at = claims.exp;
    out.emit(
        session.generation,
        json!({
            "type": "ready",
            "protocol": 1,
            "connection_id": session.connection_id,
            "actor": claims.sub,
            "expires_in_ms": (claims.exp - now()).max(0).saturating_mul(1000),
            "server_time_ms": now().saturating_mul(1000),
        }),
    );
    None
}

/// Subscribe the broadcast **before** querying, then snapshot, then forward.
pub(super) async fn snapshot(
    t: &Traffic,
    session: &mut Session,
    out: &Outbound,
    request_id: &str,
    query: LiveQuery,
) -> Result<(), &'static str> {
    let filter = query.validated().map_err(|_| "invalid_query")?;
    let receiver = t.events.subscribe();
    let page = t.flows(filter).await.map_err(|_| "query_failed")?;
    session.generation = session.generation.saturating_add(1);
    session.rx = Some(receiver);
    session.query = Some(query.clone());
    outbound::emit_page(
        out,
        session.generation,
        json!({
            "type": "snapshot",
            "request_id": request_id,
            "generation": session.generation,
            "query": query.to_json(),
        }),
        page.items,
        page.next_cursor,
        |item| item.get("seq").and_then(serde_json::Value::as_i64),
    );
    session.last_counters = Instant::now();
    let sockets = t.live.counters();
    out.emit(session.generation, counters_frame(session, sockets));
    Ok(())
}

/// Send the real flow record for a selection (`selected` frame). A missing or
/// unreadable flow is reported, never replaced by fabricated metadata.
pub(super) async fn emit_selected(
    t: &Traffic,
    session: &Session,
    out: &Outbound,
    request_id: &str,
    flow_id: &str,
) {
    match t.flow(flow_id.to_owned()).await {
        Ok(flow) => out.emit(
            session.generation,
            json!({
                "type": "selected",
                "request_id": request_id,
                "generation": session.generation,
                "flow": flow,
                "flow_id": flow_id,
            }),
        ),
        Err(_) => error_frame(
            out,
            session.generation,
            request_id,
            "flow_unavailable",
            true,
        ),
    }
}
