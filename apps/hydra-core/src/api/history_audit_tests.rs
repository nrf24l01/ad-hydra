//! Legacy HTTP history route traceability.
//!
//! `diagnostics.rs` already correlates `traffic.history.read`/`.result` audit rows
//! with the scoped HTTP request id. This module exercises the real diagnostics
//! router behind the real `request_audit` middleware and asserts that the audit
//! request id, the `x-request-id` response header and the error body all agree.
//!
//! The 409 branch itself needs a Proxy `GetFlowHistory` returning FailedPrecondition;
//! that is covered by the independent e2e agent with a real two-process harness
//! rather than a hand-written tonic mock here.
use super::request_audit;
use crate::{
    infrastructure::{crypto::Crypto, db::Database},
    modules::{
        auth::AuthState,
        traffic::{Traffic, diagnostics::Diagnostics},
        users,
    },
};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use tower::ServiceExt;

#[tokio::test]
async fn legacy_history_error_request_id_matches_audit_rows() {
    let directory =
        std::env::temp_dir().join(format!("hydra-history-audit-{}", uuid::Uuid::new_v4()));
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let auth = AuthState::new(
        Database::open(directory.join("db")).await.unwrap(),
        Crypto::load(&directory.join("key")).unwrap(),
        "https://hydra.test".into(),
    );
    users::create(
        &auth.db,
        &auth.crypto,
        "operator".into(),
        "test-password-123".into(),
    )
    .await
    .unwrap();
    let token = auth
        .login(
            "operator".into(),
            "test-password-123".into(),
            "login".into(),
        )
        .await
        .unwrap()
        .response
        .access_token;

    // A missing Proxy socket makes the legacy route fail with 503, but the audit
    // correlation must already hold for every outcome (including a future 409).
    let diagnostics = Diagnostics {
        auth: auth.clone(),
        socket: directory.join("no-proxy.sock"),
        traffic: Some(Traffic::open(auth.clone()).await.unwrap()),
    };
    let app = crate::modules::traffic::diagnostics::router(diagnostics).layer(
        axum::middleware::from_fn_with_state(auth.clone(), request_audit::middleware),
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/v1/traffic/flows/flow-legacy/history")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let header_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(!header_id.is_empty());

    let body = to_bytes(response.into_body(), 65536).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["request_id"], header_id);

    let (read_id, read_ok, result_id, result_ok) = auth
        .db
        .run(move |db| {
            let read: (String, bool) = db.query_row(
                "SELECT request_id,success FROM audit WHERE action='traffic.history.read' AND target='flow-legacy' ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let result: (String, bool) = db.query_row(
                "SELECT request_id,success FROM audit WHERE action='traffic.history.result' AND target='flow-legacy' ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok((read.0, read.1, result.0, result.1))
        })
        .await
        .unwrap();
    assert_eq!(
        read_id, header_id,
        "read audit row carries the HTTP request id"
    );
    assert_eq!(
        result_id, header_id,
        "result audit row carries the same request id"
    );
    assert!(read_ok, "the read attempt itself succeeded");
    assert!(
        !result_ok,
        "proxy failure is recorded as an unsuccessful result"
    );

    drop(auth);
    std::fs::remove_dir_all(directory).unwrap();
}
