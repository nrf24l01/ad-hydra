//! Page-scoped Streams WebSocket: one authenticated connection per page.
//!
//! Frames are JSON text. The token travels only in the first `auth` frame and
//! is never echoed, logged or placed in a URL. All reads (snapshot, pages,
//! messages, search, active bytes) are served here; the Streams page has no
//! HTTP data fallback.
//!
//! Layout: `frames` (client frame shapes + live query), `session` (connection
//! state + live-event filtering), `outbound` (bounded queue with explicit
//! loss), `commands` (frame dispatch), `history` (audited active bytes) and
//! `searching` (off-loop bounded search).
mod commands;
mod events;
mod frames;
mod history;
mod outbound;
mod searching;
mod session;
mod subscribe;

use super::Traffic;
use crate::modules::auth::now;
use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use frames::{MAX_INBOUND, PROTOCOL};
use outbound::Outbound;
use serde_json::{Value, json};
use session::{Session, counters_frame};
use std::time::{Duration, Instant};
use tokio::sync::{
    OwnedSemaphorePermit,
    broadcast::{Receiver, error::RecvError},
};

const AUTH_DEADLINE: Duration = Duration::from_secs(5);
const AUTH_RECHECK: Duration = Duration::from_secs(5);
const HEARTBEAT: Duration = Duration::from_secs(20);
const COUNTERS_EVERY: Duration = Duration::from_secs(2);
const POLL_EVERY: Duration = Duration::from_secs(1);

/// Upgrade entry point. Origin and subprotocol are checked before upgrading;
/// the socket itself is never awaited inside the request future.
pub async fn socket(
    State(t): State<Traffic>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) != Some(t.auth.origin.as_str()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let subprotocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|candidate| candidate.trim() == PROTOCOL)
        });
    if !subprotocol {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(permit) = t.streams.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    upgrade
        .protocols([PROTOCOL])
        // Read a little above the protocol cap so an oversized inbound frame is
        // rejected by us with 4400 instead of a transport-level abort.
        .max_message_size(MAX_INBOUND * 2)
        .max_frame_size(MAX_INBOUND * 2)
        .on_upgrade(move |socket| run(t, socket, permit))
        .into_response()
}

enum Step {
    Incoming(Option<Result<Message, axum::Error>>),
    Outgoing(Option<(Message, usize)>),
    Tick,
    Auth,
    Event(Result<Value, RecvError>),
    Search(Option<searching::SearchOutcome>),
}

async fn run(t: Traffic, mut socket: WebSocket, permit: OwnedSemaphorePermit) {
    let (out, mut out_rx) = Outbound::new();
    let mut session = Session::new();
    let mut tick = tokio::time::interval(POLL_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut auth_tick = tokio::time::interval(AUTH_RECHECK);
    auth_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let auth_deadline = Instant::now() + AUTH_DEADLINE;
    let mut closing: Option<(u16, &'static str)> = None;
    loop {
        let step = tokio::select! {
            incoming = socket.recv() => Step::Incoming(incoming),
            outgoing = out_rx.recv() => Step::Outgoing(outgoing),
            _ = tick.tick() => Step::Tick,
            _ = auth_tick.tick() => Step::Auth,
            event = next_event(&mut session.rx) => Step::Event(event),
            outcome = next_search(&mut session.search) => Step::Search(outcome),
        };
        match step {
            Step::Incoming(Some(Ok(Message::Text(text)))) => {
                let text = text.as_str();
                if text.len() > MAX_INBOUND {
                    outbound::error_frame(&out, session.generation, "", "invalid_frame", false);
                    closing = Some((4400, "frame too large"));
                } else if let Some(result) =
                    commands::handle_frame(&t, &mut session, &out, text).await
                {
                    closing = Some(result);
                }
            }
            Step::Incoming(Some(Ok(Message::Binary(_)))) => {
                outbound::error_frame(&out, session.generation, "", "invalid_frame", false);
                closing = Some((4400, "binary frames are rejected"));
            }
            Step::Incoming(Some(Ok(Message::Close(_)))) | Step::Incoming(None) => break,
            Step::Incoming(Some(Ok(_))) => {}
            Step::Incoming(Some(Err(_))) => {
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: 4400,
                        reason: "invalid frame".into(),
                    })))
                    .await;
                break;
            }
            Step::Outgoing(Some((message, bytes))) => {
                out.complete(bytes);
                if socket.send(message).await.is_err() {
                    break;
                }
            }
            Step::Outgoing(None) => break,
            Step::Tick => {
                // The auth deadline applies until the first successful auth,
                // regardless of `expires_at` (which defaults to 0).
                if session.actor.is_none() {
                    if Instant::now() >= auth_deadline {
                        outbound::error_frame(&out, session.generation, "", "auth_timeout", false);
                        closing = Some((4401, "auth timeout"));
                    }
                } else if now() >= session.expires_at {
                    out.emit(
                        session.generation,
                        json!({"type":"reauth_required","reason":"expired"}),
                    );
                    closing = Some((4401, "access token expired"));
                } else {
                    if session.last_counters.elapsed() >= COUNTERS_EVERY {
                        session.last_counters = Instant::now();
                        let sockets = t.live.counters();
                        out.emit(session.generation, counters_frame(&session, sockets));
                    }
                    if !session.paused
                        && session.active_bytes
                        && !session.history_done
                        && session.last_poll.elapsed() >= POLL_EVERY
                    {
                        session.last_poll = Instant::now();
                        history::poll_history(&t, &mut session, &out).await;
                    }
                    if session.last_heartbeat.elapsed() >= HEARTBEAT {
                        session.last_heartbeat = Instant::now();
                        if out.raw(Message::Ping(Vec::new().into()), 0).is_err() {
                            closing = Some((1013, "slow consumer"));
                        }
                    }
                }
            }
            Step::Auth => {
                if let Some(actor) = session.actor.as_ref()
                    && t.auth.authenticate(&session.token).await.is_err()
                {
                    out.emit(
                        session.generation,
                        json!({"type":"reauth_required","reason":"revoked"}),
                    );
                    tracing::info!(
                        connection_id = %session.connection_id,
                        actor = %actor.id,
                        "traffic socket session revoked"
                    );
                    closing = Some((4401, "session revoked"));
                }
            }
            Step::Event(Ok(event)) => {
                events::handle_event(&t, &mut session, &out, event).await;
            }
            Step::Event(Err(RecvError::Lagged(dropped))) => {
                out.emit(
                    session.generation,
                    json!({
                        "type": "loss",
                        "generation": session.generation,
                        "dropped": dropped,
                        "reason": "broadcast_lag",
                    }),
                );
            }
            Step::Event(Err(RecvError::Closed)) => session.rx = None,
            Step::Search(Some(outcome)) => {
                if let Some(job) = session.search.take() {
                    searching::finish(&t, &session, &out, job, outcome).await;
                }
            }
            Step::Search(None) => session.search = None,
        }
        // A frame that could not be queued is an explicit slow-consumer close,
        // never a silent drop.
        if out.take_fatal() && closing.is_none() {
            closing = Some((1013, "slow consumer"));
        }
        if let Some((code, reason)) = closing.take() {
            // Flush already-queued frames (errors, loss) before the close frame so
            // the client sees the reason; bounded by the queue capacity.
            while let Ok((message, bytes)) = out_rx.try_recv() {
                out.complete(bytes);
                if socket.send(message).await.is_err() {
                    break;
                }
            }
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code,
                    reason: reason.into(),
                })))
                .await;
            break;
        }
    }
    if let Some(job) = session.search.take() {
        job.abort();
    }
    drop(session.lease.take());
    drop(permit);
}

async fn next_event(rx: &mut Option<Receiver<Value>>) -> Result<Value, RecvError> {
    match rx {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

async fn next_search(slot: &mut Option<searching::SearchJob>) -> Option<searching::SearchOutcome> {
    match slot {
        Some(job) => (&mut job.rx).await.ok(),
        None => std::future::pending().await,
    }
}
