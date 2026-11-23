//! Focused WS regressions for the reviewed gaps in `scratch/hydra-ws-review1.md`.
//!
//! Deliberately a *separate* module from `ws_tests`: that file's helpers are private
//! to its module, and its 12 tests already cover upgrade/origin, close codes, pause,
//! filtering and request-id echo. This file adds only the missing contracts:
//!
//!   * the 5 s auth grace survives the immediate first timer tick (C1),
//!   * `select` answers with real flow metadata for a closed UDP flow outside the page (I9),
//!   * `favorite`/advanced filters keep matching live lifecycle events (I2/I10),
//!   * advanced query fields are accepted and echoed in the snapshot (I10).
//!
//! Search coverage (20 K row, cursor budgets) already lives in `search_tests.rs` and is
//! intentionally not duplicated here.
//!
//! Registration: this module must be declared in `apps/hydra-core/src/modules/traffic/mod.rs`
//! as `#[cfg(test)] mod ws_regression_tests;` — that file is owned by the Core agent.
use super::*;
use crate::{
    infrastructure::{crypto::Crypto, db::Database},
    modules::{
        auth::{AuthState, Tokens},
        users,
    },
};
use futures_util::{SinkExt, StreamExt};
use hydra_wire::v1::{self as proto, event::Payload};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};

type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const ORIGIN: &str = "https://hydra.test";
const PROTOCOL: &str = "hydra.traffic.v1";
const WAIT: Duration = Duration::from_secs(2);

async fn setup() -> (Traffic, AuthState, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("hydra-ws-reg-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    let db = Database::open(path.join("db")).await.unwrap();
    let crypto = Crypto::load(&path.join("key")).unwrap();
    let auth = AuthState::new(db, crypto, ORIGIN.into());
    let traffic = Traffic::open(auth.clone()).await.unwrap();
    (traffic, auth, path)
}

async fn token(auth: &AuthState) -> Tokens {
    let username = format!("u{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
    users::create(
        &auth.db,
        &auth.crypto,
        username.clone(),
        "correct horse battery".into(),
    )
    .await
    .unwrap();
    auth.login(username, "correct horse battery".into(), "test".into())
        .await
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

async fn connect(url: &str) -> Ws {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", HeaderValue::from_str(ORIGIN).unwrap());
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(PROTOCOL));
    let (ws, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("upgrade accepted");
    assert_eq!(response.status().as_u16(), 101);
    ws
}

async fn send(ws: &mut Ws, frame: Value) {
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
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

/// Any of the three selection frame names the client tolerates.
async fn next_selection(ws: &mut Ws, within: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = next_json(ws, left).await;
        if matches!(
            frame["type"].as_str(),
            Some("selected" | "select" | "flow_info")
        ) {
            return frame;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no selection frame, last {frame}"
        );
    }
}

fn opened(event_id: &str, flow: &str, service: &str, transport: proto::Transport) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        payload: Some(Payload::FlowOpened(proto::FlowOpened {
            transport: transport as i32,
            client: Some(proto::Endpoint {
                ip: "10.0.0.7".into(),
                port: 40000,
            }),
            upstream: Some(proto::Endpoint {
                ip: "10.0.0.9".into(),
                port: 53,
            }),
        })),
        ..Default::default()
    }
}

fn closed(event_id: &str, flow: &str, service: &str) -> proto::Event {
    proto::Event {
        event_id: event_id.into(),
        flow_id: flow.into(),
        service_id: service.into(),
        payload: Some(Payload::FlowClosed(proto::FlowClosed {
            bytes_client_to_server: 5,
            bytes_server_to_client: 7,
            reason: "idle".into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

async fn subscribe(ws: &mut Ws, request_id: &str, query: Value) -> Value {
    send(
        ws,
        json!({"type":"subscribe","request_id":request_id,"query":query}),
    )
    .await;
    next_json_type(ws, "snapshot", WAIT).await
}

/// C1: the 5 s `AUTH_DEADLINE` must not be bypassed by the immediate first interval tick.
/// An unauthenticated socket that sends `auth` at ~1.2 s must still get `ready`, not 4401.
#[tokio::test]
async fn auth_grace_is_five_seconds_despite_the_immediate_first_tick() {
    let (t, auth, path) = setup().await;
    let tokens = token(&auth).await;
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect(&url).await;

    // Nothing may arrive (least of all a close) before the 5 s deadline elapses.
    let early = next(&mut ws, Duration::from_millis(1_200)).await;
    assert!(
        early.is_none(),
        "socket closed or spoke before the 5 s auth deadline: {early:?}"
    );

    send(
        &mut ws,
        json!({"type":"auth","request_id":"late-auth","token":tokens.response.access_token}),
    )
    .await;
    let ready = next_json_type(&mut ws, "ready", WAIT).await;
    assert_eq!(ready["protocol"], 1);

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

/// I9: selecting a flow that is not on the current page must answer with real flow
/// metadata (endpoints, transport, closure) correlated to the select `request_id`,
/// so the client never renders a fabricated `transport:0 / closed:false` placeholder.
#[tokio::test]
async fn select_reports_metadata_for_a_closed_udp_flow_outside_the_page() {
    let (t, auth, path) = setup().await;
    let tokens = token(&auth).await;
    t.ingest(proto::EventBatch {
        events: vec![
            opened("udp-open", "flow-udp", "svc-udp", proto::Transport::Udp),
            closed("udp-close", "flow-udp", "svc-udp"),
        ],
        ..Default::default()
    })
    .await
    .unwrap();
    let expected = t
        .flow("flow-udp".into())
        .await
        .unwrap()
        .expect("flow is retained");
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect(&url).await;
    send(
        &mut ws,
        json!({"type":"auth","request_id":"a","token":tokens.response.access_token}),
    )
    .await;
    next_json_type(&mut ws, "ready", WAIT).await;

    // A service filter that matches nothing keeps `flow-udp` out of the snapshot page.
    let snapshot = subscribe(
        &mut ws,
        "sub",
        json!({"service_id":"svc-other","favorite":null,"include_blocked":false,"newest_first":true,"limit":100}),
    )
    .await;
    assert!(
        snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["flow_id"] != "flow-udp"),
        "flow must be out of page"
    );

    send(
        &mut ws,
        json!({"type":"select","request_id":"sel-udp","flow_id":"flow-udp","active_bytes":false}),
    )
    .await;
    let frame = next_selection(&mut ws, WAIT).await;
    assert_eq!(frame["request_id"], "sel-udp");
    let flow = &frame["flow"];
    assert_eq!(flow["flow_id"], "flow-udp");
    assert_eq!(flow["transport"], 2, "UDP must not be reported as TCP");
    assert_eq!(flow["client"]["ip"], "10.0.0.7");
    assert_eq!(flow["upstream"]["ip"], "10.0.0.9");
    assert_eq!(flow["upstream"]["port"], 53);
    assert_eq!(
        flow["transport"], expected["transport"],
        "frame must carry the retained flow, not a placeholder"
    );
    assert!(
        !flow["closed"].is_null() && flow["closed"] != json!(false),
        "closed flow must be reported as closed: {flow}"
    );

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

/// I2: `favorite=true` must not freeze the list at the snapshot. A favorited flow's
/// later lifecycle events must still be delivered to that subscription.
#[tokio::test]
async fn favorite_subscription_keeps_live_lifecycle_events() {
    let (t, auth, path) = setup().await;
    let tokens = token(&auth).await;
    t.ingest(proto::EventBatch {
        events: vec![opened(
            "fav-open",
            "flow-fav",
            "svc-a",
            proto::Transport::Tcp,
        )],
        ..Default::default()
    })
    .await
    .unwrap();
    t.auth
        .db
        .run(|db| {
            db.execute(
                "INSERT OR REPLACE INTO traffic_favorites(flow_id,raw_history_state) VALUES('flow-fav','saved')",
                rusqlite::params![],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect(&url).await;
    send(
        &mut ws,
        json!({"type":"auth","request_id":"a","token":tokens.response.access_token}),
    )
    .await;
    next_json_type(&mut ws, "ready", WAIT).await;

    let snapshot = subscribe(
        &mut ws,
        "fav-sub",
        json!({"service_id":null,"favorite":true,"include_blocked":false,"newest_first":true,"limit":100}),
    )
    .await;
    assert!(
        snapshot["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["flow_id"] == "flow-fav"),
        "favorited flow must be in the snapshot: {snapshot}"
    );

    t.ingest(proto::EventBatch {
        events: vec![closed("fav-close", "flow-fav", "svc-a")],
        ..Default::default()
    })
    .await
    .unwrap();
    let live = next_json_type(&mut ws, "flow", WAIT).await;
    assert_eq!(live["event"]["flow_id"], "flow-fav");
    assert_eq!(live["event"]["kind"], "flow_closed");

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}

/// I10: advanced query fields must be accepted (not `invalid_frame`) and must shape
/// both the snapshot and the live stream, and be echoed back for the client.
#[tokio::test]
async fn advanced_filters_shape_snapshot_and_live_events() {
    let (t, auth, path) = setup().await;
    let tokens = token(&auth).await;
    t.ingest(proto::EventBatch {
        events: vec![
            opened("tcp-open", "flow-tcp", "svc-a", proto::Transport::Tcp),
            opened("udp-open", "flow-udp", "svc-a", proto::Transport::Udp),
        ],
        ..Default::default()
    })
    .await
    .unwrap();
    let (url, server) = spawn(t.clone()).await;
    let mut ws = connect(&url).await;
    send(
        &mut ws,
        json!({"type":"auth","request_id":"a","token":tokens.response.access_token}),
    )
    .await;
    next_json_type(&mut ws, "ready", WAIT).await;

    let snapshot = subscribe(
        &mut ws,
        "adv-sub",
        json!({
            "service_id": null,
            "favorite": null,
            "include_blocked": false,
            "newest_first": true,
            "limit": 100,
            "transport": 2,
            "client_ip": "10.0.0.7",
            "since_ms": 0
        }),
    )
    .await;
    assert_eq!(snapshot["query"]["transport"], 2, "echo the applied filter");
    let ids: Vec<_> = snapshot["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["flow_id"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(ids, vec!["flow-udp"], "only UDP may match: {snapshot}");

    // Live events obey the same transport filter.
    t.ingest(proto::EventBatch {
        events: vec![opened(
            "udp-live",
            "flow-udp-live",
            "svc-a",
            proto::Transport::Udp,
        )],
        ..Default::default()
    })
    .await
    .unwrap();
    let live = next_json_type(&mut ws, "flow", WAIT).await;
    assert_eq!(live["event"]["flow_id"], "flow-udp-live");

    t.ingest(proto::EventBatch {
        events: vec![opened(
            "tcp-live",
            "flow-tcp-live",
            "svc-a",
            proto::Transport::Tcp,
        )],
        ..Default::default()
    })
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while tokio::time::Instant::now() < deadline {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if let Some(Message::Text(text)) = next(&mut ws, left).await {
            let frame: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(
                frame["event"]["flow_id"], "flow-tcp-live",
                "TCP event leaked past a UDP filter"
            );
        }
    }

    server.abort();
    drop(t);
    std::fs::remove_dir_all(path).unwrap();
}
