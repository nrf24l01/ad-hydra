//! End-to-end WebSocket contract tests for `/api/v1/traffic/events/socket`.
//!
//! These tests drive the real axum router over a real TCP socket with a real
//! `tokio-tungstenite` client (dev-dependency added alongside the `ws` feature).
//! Only public behaviour is asserted: upgrade/origin rules, close codes, frame
//! shape and `request_id` echoing. No private per-connection state is touched.
use super::*;
use crate::{
    infrastructure::{crypto::Crypto, db::Database},
    modules::{
        auth::{AuthState, Claims, Tokens},
        users,
    },
};
use futures_util::{SinkExt, StreamExt};
use hydra_wire::v1::{self as proto, event::Payload};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message, client::IntoClientRequest, http::HeaderValue},
};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const ORIGIN: &str = "https://hydra.test";
const PROTOCOL: &str = "hydra.traffic.v1";

async fn setup() -> (Traffic, AuthState, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("hydra-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let db = Database::open(path.join("db")).await.unwrap();
    let crypto = Crypto::load(&path.join("key")).unwrap();
    let auth = AuthState::new(db, crypto, ORIGIN.into());
    let traffic = Traffic::open(auth.clone()).await.unwrap();
    (traffic, auth, path)
}

async fn user_and_token(auth: &AuthState) -> (String, Tokens) {
    let username = format!("u{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    let id = users::create(
        &auth.db,
        &auth.crypto,
        username.clone(),
        "correct horse battery".into(),
    )
    .await
    .unwrap();
    let tokens = auth
        .login(username, "correct horse battery".into(), "test".into())
        .await
        .unwrap();
    (id, tokens)
}

fn expired_token(auth: &AuthState, sub: &str, sid: &str) -> String {
    let at = crate::modules::auth::now();
    let claims = Claims {
        sub: sub.into(),
        sid: sid.into(),
        jti: uuid::Uuid::new_v4().to_string(),
        kind: "access".into(),
        iat: at - 120,
        exp: at - 60,
        nbf: at - 120,
        iss: "hydra-core".into(),
        aud: "hydra-web".into(),
    };
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA),
        &claims,
        &auth.crypto.encoding,
    )
    .unwrap()
}

async fn spawn(t: Traffic) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = router(t);
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("ws://{addr}/api/v1/traffic/events/socket"), handle)
}

async fn connect(url: &str, origin: &str) -> Result<(Ws, Value), tungstenite::Error> {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", HeaderValue::from_str(origin).unwrap());
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(PROTOCOL));
    let (ws, response) = tokio_tungstenite::connect_async(request).await?;
    let status = response.status().as_u16();
    Ok((ws, json!({ "status": status })))
}

async fn connect_ok(url: &str) -> Ws {
    connect(url, ORIGIN).await.expect("upgrade accepted").0
}

async fn next(ws: &mut Ws, within: Duration) -> Option<Message> {
    match tokio::time::timeout(within, ws.next()).await {
        Ok(Some(Ok(message))) => Some(message),
        _ => None,
    }
}

async fn next_json(ws: &mut Ws, within: Duration) -> Value {
    match next(ws, within).await {
        Some(Message::Text(text)) => serde_json::from_str(&text).expect("JSON text frame"),
        other => panic!("expected JSON text frame, got {other:?}"),
    }
}

async fn next_json_type(ws: &mut Ws, kind: &str, within: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = next_json(ws, left).await;
        if frame["type"] == kind {
            return frame;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no {kind} frame, last {frame}"
        );
    }
}

/// Waits for any frame echoing `request_id`. A `select` is acknowledged with a
/// `selected` frame (or a scoped `error`), so this is the barrier that proves a
/// command was applied before the test pushes events at the session.
async fn next_json_request(ws: &mut Ws, request_id: &str, within: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = next_json(ws, left).await;
        if frame["request_id"] == request_id {
            return frame;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no frame echoing {request_id}, last {frame}"
        );
    }
}

async fn send(ws: &mut Ws, frame: Value) {
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
}

async fn auth_frame(ws: &mut Ws, token: &str) -> Value {
    send(
        ws,
        json!({"type":"auth","request_id":uuid::Uuid::new_v4().to_string(),"token":token}),
    )
    .await;
    next_json_type(ws, "ready", Duration::from_secs(2)).await
}

async fn close_code(ws: &mut Ws, within: Duration) -> u16 {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match next(ws, left).await {
            Some(Message::Close(Some(frame))) => return u16::from(frame.code),
            Some(_) => continue,
            None => panic!("expected close frame before deadline"),
        }
    }
}

fn opened(event_id: &str, flow: &str, service: &str) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        payload: Some(Payload::FlowOpened(proto::FlowOpened {
            transport: 1,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn message(event_id: &str, flow: &str, service: &str) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        payload: Some(Payload::Message(proto::ParsedMessage {
            message_id: event_id.into(),
            protocol_id: "utf8".into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[tokio::test]
async fn origin_mismatch_is_rejected_before_upgrade() {
    let (t, _auth, path) = setup().await;
    let (url, server) = spawn(t.clone()).await;
    let error = connect(&url, "https://evil.test")
        .await
        .expect_err("must fail");
    match error {
        tungstenite::Error::Http(response) => assert_eq!(response.status().as_u16(), 403),
        other => panic!("expected HTTP 403, got {other:?}"),
    }
    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn missing_auth_frame_closes_with_4401_within_deadline() {
    let (t, _auth, path) = setup().await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    assert_eq!(close_code(&mut ws, Duration::from_secs(7)).await, 4401);
    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn valid_auth_negotiates_subprotocol_and_reports_ready() {
    let (t, auth, path) = setup().await;
    let (id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;

    let mut request = url.as_str().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", HeaderValue::from_static(ORIGIN));
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(PROTOCOL));
    let (mut ws, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert_eq!(
        response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok()),
        Some(PROTOCOL)
    );

    let ready = auth_frame(&mut ws, &tokens.response.access_token).await;
    assert_eq!(ready["protocol"], 1);
    assert_eq!(ready["actor"], id);
    assert!(
        ready["connection_id"]
            .as_str()
            .is_some_and(|v| !v.is_empty())
    );
    assert!(ready["expires_in_ms"].as_u64().unwrap() <= 60_000);

    ws.close(None).await.ok();
    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn expired_and_revoked_tokens_close_with_4401() {
    let (t, auth, path) = setup().await;
    let (id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;

    let mut expired = connect_ok(&url).await;
    let stale = expired_token(
        &auth,
        &id,
        &auth
            .decode_at(
                &tokens.response.access_token,
                "access",
                crate::modules::auth::now(),
            )
            .unwrap()
            .sid,
    );
    send(
        &mut expired,
        json!({"type":"auth","request_id":"r","token":stale}),
    )
    .await;
    assert_eq!(close_code(&mut expired, Duration::from_secs(2)).await, 4401);

    auth.logout(tokens.refresh.clone(), "test".into())
        .await
        .unwrap();
    let mut revoked = connect_ok(&url).await;
    send(
        &mut revoked,
        json!({"type":"auth","request_id":"r","token":tokens.response.access_token}),
    )
    .await;
    assert_eq!(close_code(&mut revoked, Duration::from_secs(2)).await, 4401);

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn socket_is_bound_to_one_session_and_rejects_sid_switch() {
    let (t, auth, path) = setup().await;
    let (_id, first) = user_and_token(&auth).await;
    let username: String = auth
        .db
        .run(move |db| {
            Ok(db.query_row(
                "SELECT username FROM users WHERE id=(SELECT id FROM users ORDER BY rowid LIMIT 1)",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    let second = auth
        .login(username, "correct horse battery".into(), "test".into())
        .await
        .unwrap();
    let (url, server) = spawn(t.clone()).await;

    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &first.response.access_token).await;
    send(
        &mut ws,
        json!({"type":"auth","request_id":"switch","token":second.response.access_token}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match next(&mut ws, left).await {
            Some(Message::Close(Some(frame))) => {
                assert_eq!(u16::from(frame.code), 4401);
                break;
            }
            Some(Message::Text(text)) => {
                let frame: Value = serde_json::from_str(&text).unwrap();
                if frame["type"] == "error" {
                    assert_eq!(frame["request_id"], "switch");
                    assert_eq!(frame["code"], "session_changed");
                    break;
                }
            }
            Some(_) => {}
            None => panic!("sid switch was not rejected"),
        }
    }

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn two_sockets_per_actor_and_permit_released_on_close() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;

    let mut first = connect_ok(&url).await;
    auth_frame(&mut first, &tokens.response.access_token).await;
    let mut second = connect_ok(&url).await;
    auth_frame(&mut second, &tokens.response.access_token).await;

    // Third socket is refused while both permits are held: HTTP rejection or 4429.
    match connect(&url, ORIGIN).await {
        Err(tungstenite::Error::Http(response)) => {
            assert!(matches!(response.status().as_u16(), 403 | 429 | 503));
        }
        Ok((mut ws, _)) => {
            send(
                &mut ws,
                json!({"type":"auth","request_id":"r","token":tokens.response.access_token}),
            )
            .await;
            assert_eq!(close_code(&mut ws, Duration::from_secs(2)).await, 4429);
        }
        Err(other) => panic!("unexpected connect error {other:?}"),
    }

    first.close(None).await.ok();
    second.close(None).await.ok();

    // Bounded wait for the server to release the permits, then a new socket succeeds.
    let mut recovered = None;
    for _ in 0..20 {
        match connect(&url, ORIGIN).await {
            Ok((mut ws, _)) => {
                send(
                    &mut ws,
                    json!({"type":"auth","request_id":"retry","token":tokens.response.access_token}),
                )
                .await;
                match next(&mut ws, Duration::from_secs(2)).await {
                    Some(Message::Text(text))
                        if serde_json::from_str::<Value>(&text)
                            .map(|v| v["type"] == "ready")
                            .unwrap_or(false) =>
                    {
                        recovered = Some(ws);
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
            Err(tungstenite::Error::Http(_)) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(other) => panic!("unexpected connect error {other:?}"),
        }
    }
    assert!(
        recovered.is_some(),
        "closing sockets must release their permits"
    );
    if let Some(mut ws) = recovered {
        ws.close(None).await.ok();
    }

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn subscribe_sends_initial_snapshot_then_live_events_and_filters_by_service() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    t.ingest(proto::EventBatch {
        events: vec![opened("old-open", "flow-old", "svc-a")],
        ..Default::default()
    })
    .await
    .unwrap();
    let (url, server) = spawn(t.clone()).await;

    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;
    send(
        &mut ws,
        json!({"type":"subscribe","request_id":"sub-1","query":{"service_id":"svc-a","favorite":null,"include_blocked":false,"newest_first":true,"limit":100}}),
    )
    .await;
    let snapshot = next_json_type(&mut ws, "snapshot", Duration::from_secs(2)).await;
    assert_eq!(snapshot["request_id"], "sub-1");
    assert!(snapshot["generation"].as_u64().unwrap() >= 1);
    assert!(
        snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["flow_id"] == "flow-old")
    );

    // A different service is filtered out server-side.
    t.ingest(proto::EventBatch {
        events: vec![opened("b-open", "flow-b", "svc-b")],
        ..Default::default()
    })
    .await
    .unwrap();
    let mut saw_b = false;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(700);
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Some(Message::Text(text)) = next(&mut ws, left).await {
            let frame: Value = serde_json::from_str(&text).unwrap();
            if frame["type"] == "flow" && frame["event"]["flow_id"] == "flow-b" {
                saw_b = true;
            }
        }
    }
    assert!(!saw_b, "service filter must not forward other services");

    t.ingest(proto::EventBatch {
        events: vec![opened("a-open", "flow-a", "svc-a")],
        ..Default::default()
    })
    .await
    .unwrap();
    let live = next_json_type(&mut ws, "flow", Duration::from_secs(2)).await;
    assert_eq!(live["event"]["flow_id"], "flow-a");
    assert_eq!(live["generation"], snapshot["generation"]);

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn select_filters_messages_to_the_selected_flow_only() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;
    send(
        &mut ws,
        json!({"type":"subscribe","request_id":"s","query":{"service_id":null,"favorite":null,"include_blocked":false,"newest_first":true,"limit":100}}),
    )
    .await;
    next_json_type(&mut ws, "snapshot", Duration::from_secs(2)).await;

    // A real flow record, so the select is acknowledged with `selected` rather
    // than `flow_unavailable`, and the wrong-flow event is genuinely delivered
    // to the session before being filtered.
    t.ingest(proto::EventBatch {
        events: vec![opened("a-open", "flow-a", "svc")],
        ..Default::default()
    })
    .await
    .unwrap();

    send(
        &mut ws,
        json!({"type":"select","request_id":"sel","flow_id":"flow-a","active_bytes":false}),
    )
    .await;
    // Commands and live events are handled by the same socket task: wait for the
    // frame echoing the select's request_id *before* ingesting, otherwise the
    // events race the select and `events.rs` drops messages for an unselected
    // flow (the original flake). The ack proves selection was applied.
    let ack = next_json_request(&mut ws, "sel", Duration::from_secs(2)).await;
    assert_eq!(ack["type"], "selected");
    assert_eq!(ack["flow_id"], "flow-a");

    t.ingest(proto::EventBatch {
        events: vec![
            message("m-b", "flow-b", "svc"),
            message("m-a", "flow-a", "svc"),
        ],
        ..Default::default()
    })
    .await
    .unwrap();

    // The unselected flow's message is first in the batch, so only the selected
    // flow can produce a `message` frame, and it must carry the payload
    // unchanged (same message_id, same generation).
    let frame = next_json_type(&mut ws, "message", Duration::from_secs(2)).await;
    assert_eq!(frame["flow_id"], "flow-a");
    assert_eq!(frame["event"]["message_id"], "m-a");
    assert_eq!(frame["generation"], ack["generation"]);

    // No later `message` frame may appear: `flow-b` was filtered, not deferred.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Some(Message::Text(text)) = next(&mut ws, left).await {
            let frame: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(
                frame["type"], "message",
                "unselected flow must be filtered, got {frame}"
            );
        }
    }

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn pause_stops_pushes_and_resume_resnapshots_with_new_generation() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;
    send(
        &mut ws,
        json!({"type":"subscribe","request_id":"s","query":{"service_id":null,"favorite":null,"include_blocked":false,"newest_first":true,"limit":100}}),
    )
    .await;
    let first = next_json_type(&mut ws, "snapshot", Duration::from_secs(2)).await;

    send(&mut ws, json!({"type":"pause"})).await;
    t.ingest(proto::EventBatch {
        events: vec![opened("p-open", "flow-paused", "svc")],
        ..Default::default()
    })
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Some(Message::Text(text)) = next(&mut ws, left).await {
            let frame: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(frame["type"], "flow", "pause must stop live pushes");
        }
    }

    send(&mut ws, json!({"type":"resume","request_id":"resume-1"})).await;
    let resumed = next_json_type(&mut ws, "snapshot", Duration::from_secs(2)).await;
    assert_eq!(resumed["request_id"], "resume-1");
    assert!(resumed["generation"].as_u64().unwrap() > first["generation"].as_u64().unwrap());
    assert!(
        resumed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["flow_id"] == "flow-paused")
    );

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

/// One message row whose decoded payload is large enough that 100 of them cannot
/// fit in a single 1 MiB outbound frame.
fn large_message(event_id: &str, flow: &str, service: &str, payload: usize) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        payload: Some(Payload::Message(proto::ParsedMessage {
            message_id: event_id.into(),
            protocol_id: "utf8".into(),
            fields: Some(proto::FieldObject {
                fields: std::collections::HashMap::from([(
                    "payload".into(),
                    proto::FieldValue {
                        value: Some(proto::field_value::Value::StringValue("a".repeat(payload))),
                    },
                )]),
            }),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// A page that would exceed the frame budget must be split, not dropped: every
/// message has to stay reachable through `next_cursor` + `after`, and no single
/// `messages_page` frame may exceed the documented 1 MiB cap.
#[tokio::test]
async fn messages_page_byte_budget_keeps_every_message_reachable() {
    const COUNT: usize = 100;
    // 100 × 28 KiB ≈ 2.8 MiB of item JSON: comfortably above MAX_FRAME (1 MiB)
    // while each row stays below the 64 KiB per-event row cap.
    const PAYLOAD: usize = 28 * 1024;
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    t.ingest(proto::EventBatch {
        events: vec![opened("big-open", "flow-big", "svc-big")],
        ..Default::default()
    })
    .await
    .unwrap();
    t.ingest(proto::EventBatch {
        events: (0..COUNT)
            .map(|i| large_message(&format!("big-{i:03}"), "flow-big", "svc-big", PAYLOAD))
            .collect(),
        ..Default::default()
    })
    .await
    .unwrap();
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;

    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<i64> = None;
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(
            pages <= COUNT,
            "paging did not terminate after {pages} pages"
        );
        let request_id = format!("page-{pages}");
        send(
            &mut ws,
            json!({"type":"messages","request_id":request_id,"flow_id":"flow-big","after":after,"limit":100}),
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let frame = loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let (text_len, frame) = match next(&mut ws, left).await {
                Some(Message::Text(text)) => (
                    text.len(),
                    serde_json::from_str::<Value>(&text).expect("JSON text frame"),
                ),
                Some(other) => panic!("unexpected frame while paging: {other:?}"),
                None => panic!("no messages_page for {request_id}"),
            };
            if frame["type"] == "loss" {
                // `frame_too_large` here means rows were dropped instead of paged.
                assert_ne!(
                    frame["reason"], "frame_too_large",
                    "page exceeded the frame budget; rows must stay reachable by paging"
                );
                continue;
            }
            if frame["type"] == "error" && frame["request_id"] == request_id.as_str() {
                panic!("messages request failed instead of paging: {frame}");
            }
            if frame["type"] == "messages_page" && frame["request_id"] == request_id.as_str() {
                assert!(
                    text_len <= 1024 * 1024,
                    "one messages_page must stay within the 1 MiB frame cap, got {text_len} bytes"
                );
                break frame;
            }
        };
        for item in frame["items"].as_array().expect("items array") {
            seen.push(item["message_id"].as_str().expect("message_id").to_owned());
        }
        match frame["next_cursor"].as_i64() {
            Some(cursor) => {
                assert!(
                    after.is_none_or(|previous| cursor > previous),
                    "next_cursor must advance: {cursor} after {after:?}"
                );
                after = Some(cursor);
            }
            None => break,
        }
    }

    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        COUNT,
        "every message must be reachable exactly once through paging"
    );
    assert_eq!(seen.first().map(String::as_str), Some("big-000"));
    assert_eq!(seen.last().map(String::as_str), Some("big-099"));

    ws.close(None).await.ok();
    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn invalid_frames_return_safe_errors_with_request_ids() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;

    // Unknown frame types are a safe protocol error followed by a clean 4400 close.
    send(
        &mut ws,
        json!({"type":"not_a_frame","request_id":"client-req-1"}),
    )
    .await;
    let error = next_json_type(&mut ws, "error", Duration::from_secs(2)).await;
    assert_eq!(error["request_id"], "client-req-1");
    assert_eq!(error["code"], "invalid_frame");
    assert!(!error["retryable"].as_bool().unwrap());
    assert_eq!(close_code(&mut ws, Duration::from_secs(2)).await, 4400);

    // A semantically invalid frame is reported without closing the socket.
    let mut second = connect_ok(&url).await;
    auth_frame(&mut second, &tokens.response.access_token).await;
    send(&mut second, json!({"type":"subscribe","request_id":"bad-sub","query":{"service_id":"x".repeat(129),"favorite":null,"include_blocked":false,"newest_first":true,"limit":100}})).await;
    let error = next_json_type(&mut second, "error", Duration::from_secs(2)).await;
    assert_eq!(error["request_id"], "bad-sub");
    assert_eq!(error["code"], "invalid_frame");
    send(&mut second, json!({"type":"subscribe","request_id":"after-error","query":{"service_id":null,"favorite":null,"include_blocked":false,"newest_first":true,"limit":100}})).await;
    let snapshot = next_json_type(&mut second, "snapshot", Duration::from_secs(2)).await;
    assert_eq!(snapshot["request_id"], "after-error");

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn malformed_json_is_a_safe_error_then_4400() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect_ok(&url).await;
    auth_frame(&mut ws, &tokens.response.access_token).await;

    ws.send(Message::Text("{".into())).await.unwrap();
    let error = next_json_type(&mut ws, "error", Duration::from_secs(2)).await;
    assert_eq!(error["code"], "invalid_frame");
    assert_eq!(close_code(&mut ws, Duration::from_secs(2)).await, 4400);

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn binary_and_oversized_inbound_frames_close_with_4400() {
    let (t, auth, path) = setup().await;
    let (_id, tokens) = user_and_token(&auth).await;
    let (url, server) = spawn(t.clone()).await;

    let mut binary = connect_ok(&url).await;
    auth_frame(&mut binary, &tokens.response.access_token).await;
    binary
        .send(Message::Binary(vec![0u8; 4].into()))
        .await
        .unwrap();
    assert_eq!(close_code(&mut binary, Duration::from_secs(2)).await, 4400);

    let mut huge = connect_ok(&url).await;
    auth_frame(&mut huge, &tokens.response.access_token).await;
    huge.send(Message::Text("x".repeat(32 * 1024 + 1).into()))
        .await
        .unwrap();
    assert_eq!(close_code(&mut huge, Duration::from_secs(2)).await, 4400);

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}
