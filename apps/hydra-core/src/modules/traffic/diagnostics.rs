//! Audited, explicitly requested diagnostic bytes; separate from parsed telemetry.
use crate::{
    infrastructure::proxy_client,
    modules::{audit, auth::AuthState},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    routing::{get, post},
};
use hydra_wire::v1::{self, proxy_diagnostics_client::ProxyDiagnosticsClient};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use utoipa::{OpenApi, ToSchema};
#[derive(Clone)]
pub struct Diagnostics {
    pub auth: AuthState,
    pub socket: PathBuf,
    pub traffic: Option<super::Traffic>,
}
#[derive(Serialize, ToSchema)]
pub struct Capture {
    pub sample_id: String,
}
#[derive(Serialize, ToSchema)]
pub struct Sample {
    pub data: Vec<u8>,
    pub truncated: bool,
    pub expires_at: String,
}
#[derive(Serialize, ToSchema)]
pub struct HistorySegment {
    pub direction: i32,
    pub data: Vec<u8>,
    pub datagram: bool,
    pub occurred_at_ms: i64,
    pub sequence: u64,
}
#[derive(Serialize, ToSchema)]
pub struct FlowHistory {
    pub transport: i32,
    pub datagrams_client_to_server: u64,
    pub datagrams_server_to_client: u64,
    pub flow_id: String,
    pub service_id: String,
    pub protocol_id: String,
    pub opened_at_ms: i64,
    pub closed_at_ms: i64,
    pub bytes_client_to_server: u64,
    pub bytes_server_to_client: u64,
    pub segments: Vec<HistorySegment>,
    pub truncated_client_to_server: bool,
    pub truncated_server_to_client: bool,
    pub history_truncated: bool,
}
pub(super) async fn actor(
    state: &Diagnostics,
    headers: &HeaderMap,
    mutation: bool,
) -> Result<audit::Actor, StatusCode> {
    if mutation
        && headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())
            != Some(state.auth.origin.as_str())
    {
        return Err(StatusCode::FORBIDDEN);
    }
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .auth
        .authenticate(token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)
}
pub(super) async fn record(
    state: &Diagnostics,
    actor: audit::Actor,
    action: &'static str,
    target: String,
    request: String,
    success: bool,
) -> Result<(), StatusCode> {
    state
        .auth
        .db
        .run(move |db| {
            let tx = db.transaction()?;
            audit::append(
                &tx,
                &actor,
                action,
                &target,
                &request,
                success,
                crate::modules::auth::now(),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}
#[utoipa::path(post,path="/api/v1/traffic/flows/{id}/capture",params(("id"=String,Path)),responses((status=202,body=Capture),(status=404),(status=503)),tag="traffic")]
async fn capture(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Capture>), StatusCode> {
    let actor = actor(&state, &headers, true).await?;
    let request = uuid::Uuid::new_v4().to_string();
    record(
        &state,
        actor.clone(),
        "traffic.capture.request",
        id.clone(),
        request.clone(),
        true,
    )
    .await?;
    let channel = proxy_client::connect(&state.socket)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = ProxyDiagnosticsClient::new(channel)
        .capture_sample(v1::CaptureSampleRequest {
            flow_id: id.clone(),
            request_id: request.clone(),
            actor: Some(v1::Actor {
                r#type: 1,
                user_id: actor.id.clone(),
                ..Default::default()
            }),
        })
        .await;
    record(
        &state,
        actor,
        "traffic.capture.result",
        id,
        request,
        result.is_ok(),
    )
    .await?;
    let result = result.map_err(map_status)?.into_inner();
    Ok((
        StatusCode::ACCEPTED,
        Json(Capture {
            sample_id: result.sample_id,
        }),
    ))
}
#[utoipa::path(get,path="/api/v1/traffic/samples/{id}",params(("id"=String,Path)),responses((status=200,body=Sample),(status=410,description="Sample expired, not yet captured, or unavailable"),(status=503)),tag="traffic")]
async fn sample(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Sample>, StatusCode> {
    let actor = actor(&state, &headers, false).await?;
    let request = uuid::Uuid::new_v4().to_string();
    record(
        &state,
        actor.clone(),
        "traffic.sample.read",
        id.clone(),
        request.clone(),
        true,
    )
    .await?;
    let channel = proxy_client::connect(&state.socket)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = ProxyDiagnosticsClient::new(channel)
        .get_sample(v1::GetSampleRequest {
            sample_id: id.clone(),
        })
        .await;
    record(
        &state,
        actor,
        "traffic.sample.result",
        id,
        request,
        result.is_ok(),
    )
    .await?;
    let result = result
        .map_err(|e| {
            if e.code() == tonic::Code::NotFound {
                StatusCode::GONE
            } else {
                map_status(e)
            }
        })?
        .into_inner();
    let seconds = result.expires_at.map(|v| v.seconds).unwrap_or_default();
    let expires_at = state
        .auth
        .db
        .run(move |db| {
            Ok(db.query_row(
                "SELECT strftime('%Y-%m-%dT%H:%M:%SZ',?1,'unixepoch')",
                [seconds],
                |r| r.get::<_, String>(0),
            )?)
        })
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(Sample {
        data: result.data,
        truncated: result.truncated,
        expires_at,
    }))
}

#[utoipa::path(get,path="/api/v1/traffic/flows/{id}/history",params(("id"=String,Path)),responses((status=200,body=FlowHistory),(status=401),(status=404),(status=409,description="Flow is active or history is still being saved"),(status=503)),tag="traffic")]
async fn flow_history(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<FlowHistory>, StatusCode> {
    if id.len() > 128 {
        return Err(StatusCode::NOT_FOUND);
    }
    let actor = actor(&state, &headers, false).await?;
    // Correlate with the scoped HTTP request id so a future 409 is traceable.
    let request = crate::modules::audit::request_id();
    record(
        &state,
        actor.clone(),
        "traffic.history.read",
        id.clone(),
        request.clone(),
        true,
    )
    .await?;
    let result = match proxy_client::connect(&state.socket).await {
        Ok(channel) => ProxyDiagnosticsClient::new(channel)
            .get_flow_history(v1::GetFlowHistoryRequest {
                flow_id: id.clone(),
            })
            .await
            .map(|response| response.into_inner()),
        Err(_) => Err(tonic::Status::unavailable("proxy unavailable")),
    };
    record(
        &state,
        actor,
        "traffic.history.result",
        id.clone(),
        request.clone(),
        result.is_ok(),
    )
    .await?;
    let history = match result {
        Ok(history) => history,
        Err(error) => {
            let status = map_history_status(error);
            tracing::warn!(
                request_id = %request,
                flow_id = %id,
                status = status.as_u16(),
                "flow history read failed"
            );
            return Err(status);
        }
    };
    Ok(Json(FlowHistory {
        transport: history.transport,
        datagrams_client_to_server: history.datagrams_client_to_server,
        datagrams_server_to_client: history.datagrams_server_to_client,
        flow_id: history.flow_id,
        service_id: history.service_id,
        protocol_id: history.protocol_id,
        opened_at_ms: history.opened_at_ms,
        closed_at_ms: history.closed_at_ms,
        bytes_client_to_server: history.bytes_client_to_server,
        bytes_server_to_client: history.bytes_server_to_client,
        segments: history
            .segments
            .into_iter()
            .map(|segment| HistorySegment {
                direction: segment.direction,
                data: segment.data,
                datagram: segment.datagram,
                occurred_at_ms: segment.occurred_at_ms,
                sequence: segment.sequence,
            })
            .collect(),
        truncated_client_to_server: history.truncated_client_to_server,
        truncated_server_to_client: history.truncated_server_to_client,
        history_truncated: history.history_truncated,
    }))
}

fn map_history_status(error: tonic::Status) -> StatusCode {
    match error.code() {
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Cursor for the additive active-flow snapshot; sequences are per-direction
/// chunk counters plus retained-head byte counters, never global offsets.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ActiveCursor {
    pub client_sequence: u64,
    pub server_sequence: u64,
    pub client_head_dropped: u64,
    pub server_head_dropped: u64,
}

/// One history snapshot delivered over WS, from either Proxy source.
#[derive(serde::Serialize)]
pub(crate) struct ActiveHistory {
    pub flow_id: String,
    pub service_id: String,
    pub protocol_id: String,
    pub opened_at_ms: i64,
    pub active: bool,
    /// Closed flow whose history is still being persisted; retry is bounded.
    pub pending: bool,
    pub segments: Vec<HistorySegment>,
    pub truncated_client_to_server: bool,
    pub truncated_server_to_client: bool,
    pub retained_client_to_server_bytes: u64,
    pub retained_server_to_client_bytes: u64,
    pub reset: bool,
    pub client_sequence: u64,
    pub server_sequence: u64,
    pub client_head_dropped: u64,
    pub server_head_dropped: u64,
    pub generated_at_ms: i64,
}

pub(crate) enum HistoryFailure {
    /// Flow closed but its history is still being written (legacy 409 state).
    Pending,
    NotFound,
    Unavailable(&'static str),
}

fn convert_segment(segment: v1::HistorySegment) -> HistorySegment {
    HistorySegment {
        direction: segment.direction,
        data: segment.data,
        datagram: segment.datagram,
        occurred_at_ms: segment.occurred_at_ms,
        sequence: segment.sequence,
    }
}

/// On-demand history for the selected stream: active snapshot while live,
/// otherwise the persisted `GetFlowHistory` result in the same shape.
pub(crate) async fn read_active(
    socket: &std::path::Path,
    flow_id: String,
    actor: audit::Actor,
    request_id: String,
    cursor: Option<ActiveCursor>,
) -> Result<ActiveHistory, HistoryFailure> {
    let channel = proxy_client::connect(socket)
        .await
        .map_err(|_| HistoryFailure::Unavailable("proxy_unavailable"))?;
    let mut client = ProxyDiagnosticsClient::new(channel);
    let reply = client
        .get_active_flow_history(v1::GetActiveFlowHistoryRequest {
            flow_id: flow_id.clone(),
            actor: Some(v1::Actor {
                r#type: 1,
                user_id: actor.id.clone(),
                ..Default::default()
            }),
            request_id: request_id.clone(),
            cursor: cursor.map(|cursor| v1::ActiveBytesCursor {
                client_sequence: cursor.client_sequence,
                server_sequence: cursor.server_sequence,
                client_head_dropped: cursor.client_head_dropped,
                server_head_dropped: cursor.server_head_dropped,
            }),
        })
        .await;
    let reply = match reply {
        Ok(reply) => reply.into_inner(),
        Err(status) => {
            let code = if status.code() == tonic::Code::Unimplemented {
                "active_bytes_unavailable"
            } else {
                "history_unavailable"
            };
            tracing::warn!(
                request_id = %request_id,
                flow_id = %flow_id,
                grpc_code = status.code() as i32,
                "active history read failed"
            );
            return Err(HistoryFailure::Unavailable(code));
        }
    };
    if reply.active {
        return Ok(ActiveHistory {
            flow_id: reply.flow_id,
            service_id: reply.service_id,
            protocol_id: reply.protocol_id,
            opened_at_ms: reply.opened_at_ms,
            active: true,
            pending: false,
            segments: reply.segments.into_iter().map(convert_segment).collect(),
            truncated_client_to_server: reply.truncated_client_to_server,
            truncated_server_to_client: reply.truncated_server_to_client,
            retained_client_to_server_bytes: reply.retained_client_to_server_bytes,
            retained_server_to_client_bytes: reply.retained_server_to_client_bytes,
            reset: reply.reset,
            client_sequence: reply.client_sequence,
            server_sequence: reply.server_sequence,
            client_head_dropped: reply.client_head_dropped,
            server_head_dropped: reply.server_head_dropped,
            generated_at_ms: reply.generated_at_ms,
        });
    }
    // Closed/unknown to the active map: read persisted history through the
    // existing RPC and deliver it as the same WS snapshot shape.
    match client
        .get_flow_history(v1::GetFlowHistoryRequest {
            flow_id: flow_id.clone(),
        })
        .await
    {
        Ok(response) => {
            let history = response.into_inner();
            Ok(ActiveHistory {
                flow_id: history.flow_id,
                service_id: history.service_id,
                protocol_id: history.protocol_id,
                opened_at_ms: history.opened_at_ms,
                active: false,
                pending: false,
                segments: history.segments.into_iter().map(convert_segment).collect(),
                truncated_client_to_server: history.truncated_client_to_server,
                truncated_server_to_client: history.truncated_server_to_client,
                retained_client_to_server_bytes: history.bytes_client_to_server,
                retained_server_to_client_bytes: history.bytes_server_to_client,
                reset: true,
                client_sequence: 0,
                server_sequence: 0,
                client_head_dropped: 0,
                server_head_dropped: 0,
                generated_at_ms: crate::modules::auth::now().saturating_mul(1000),
            })
        }
        Err(status) => {
            tracing::warn!(
                request_id = %request_id,
                flow_id = %flow_id,
                grpc_code = status.code() as i32,
                "closed history read failed"
            );
            Err(match status.code() {
                tonic::Code::FailedPrecondition => HistoryFailure::Pending,
                tonic::Code::NotFound => HistoryFailure::NotFound,
                _ => HistoryFailure::Unavailable("history_unavailable"),
            })
        }
    }
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProbeInput {
    pub transport: String,
    pub payload: Vec<u8>,
    pub timeout_ms: u32,
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceProbeInput {
    pub service_id: String,
    pub transport: String,
    pub payload: Vec<u8>,
    pub timeout_ms: u32,
}
#[utoipa::path(post,path="/api/v1/diagnostics/probe",request_body=ServiceProbeInput,responses((status=200,body=ProbeOutput),(status=400),(status=503)),tag="services")]
async fn diagnostic_probe(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Json(input): Json<ServiceProbeInput>,
) -> Result<Json<ProbeOutput>, StatusCode> {
    probe(
        State(state),
        headers,
        Path(input.service_id),
        Json(ProbeInput {
            transport: input.transport,
            payload: input.payload,
            timeout_ms: input.timeout_ms,
        }),
    )
    .await
}
#[derive(Serialize, ToSchema)]
pub struct ProbeOutput {
    pub response: Vec<u8>,
    pub truncated: bool,
    pub elapsed_ms: u32,
    pub error_code: String,
}
#[utoipa::path(post,path="/api/v1/services/{id}/probe",params(("id"=String,Path)),request_body=ProbeInput,responses((status=200,body=ProbeOutput),(status=400),(status=503)),tag="services")]
async fn probe(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(input): Json<ProbeInput>,
) -> Result<Json<ProbeOutput>, StatusCode> {
    let actor = actor(&state, &headers, true).await?;
    if input.payload.len() > 4096 || !(50..=5000).contains(&input.timeout_ms) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let transport = match input.transport.as_str() {
        "tcp" => 1,
        "udp" => 2,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let request = uuid::Uuid::new_v4().to_string();
    record(
        &state,
        actor.clone(),
        "service.probe.request",
        id.clone(),
        request.clone(),
        true,
    )
    .await?;
    let channel = proxy_client::connect(&state.socket)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = ProxyDiagnosticsClient::new(channel)
        .probe_service(v1::ProbeServiceRequest {
            service_id: id.clone(),
            transport,
            payload: input.payload,
            timeout_ms: input.timeout_ms,
            request_id: request.clone(),
            actor: Some(v1::Actor {
                r#type: 1,
                user_id: actor.id.clone(),
                ..Default::default()
            }),
        })
        .await;
    record(
        &state,
        actor,
        "service.probe.result",
        id,
        request,
        result.is_ok(),
    )
    .await?;
    let result = result.map_err(map_status)?.into_inner();
    Ok(Json(ProbeOutput {
        response: result.response,
        truncated: result.truncated,
        elapsed_ms: result.elapsed_ms,
        error_code: result.error_code,
    }))
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProtocolTest {
    pub sample_id: String,
    pub direction: String,
}
#[utoipa::path(post,path="/api/v1/protocols/{id}/{version}/test",params(("id"=String,Path),("version"=String,Path)),request_body=ProtocolTest,responses((status=200,body=Object),(status=410),(status=400)),tag="protocols")]
async fn test_protocol(
    State(state): State<Diagnostics>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, String)>,
    Json(input): Json<ProtocolTest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let actor = actor(&state, &headers, true).await?;
    let direction = match input.direction.as_str() {
        "client_to_server" => 1,
        "server_to_client" => 2,
        _ => return Err(StatusCode::BAD_REQUEST),
    };
    let request = uuid::Uuid::new_v4().to_string();
    record(
        &state,
        actor.clone(),
        "protocol.sample.test",
        input.sample_id.clone(),
        request.clone(),
        true,
    )
    .await?;
    let channel = proxy_client::connect(&state.socket)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let result = ProxyDiagnosticsClient::new(channel)
        .test_protocol(v1::TestProtocolRequest {
            protocol_id: id.clone(),
            version: version.clone(),
            sample_id: input.sample_id,
            direction,
            request_id: request.clone(),
            actor: Some(v1::Actor {
                r#type: 1,
                user_id: actor.id.clone(),
                ..Default::default()
            }),
        })
        .await;
    record(
        &state,
        actor,
        "protocol.test.result",
        format!("{id}/{version}"),
        request,
        result.is_ok(),
    )
    .await?;
    let result = result
        .map_err(|e| {
            if e.code() == tonic::Code::NotFound {
                StatusCode::GONE
            } else {
                map_status(e)
            }
        })?
        .into_inner();
    let messages=result.messages.into_iter().map(|m|{
        let fields=m.fields.unwrap_or_default().fields.into_iter().map(|(key,value)|Ok((key,super::repository::field(value,0,&mut 0).map_err(|e|e.to_string())?))).collect::<Result<std::collections::BTreeMap<_,_>,String>>().map_err(|_|StatusCode::BAD_GATEWAY)?;
        Ok(serde_json::json!({"protocol_id":m.protocol_id,"message_type":m.message_type,"direction":m.direction,"fields":fields}))
    }).collect::<Result<Vec<_>,StatusCode>>()?;
    Ok(Json(serde_json::json!({"messages":messages})))
}
fn map_status(e: tonic::Status) -> StatusCode {
    match e.code() {
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    }
}
#[derive(OpenApi)]
#[openapi(
    paths(
        super::favorites::favorite,
        capture,
        sample,
        flow_history,
        probe,
        diagnostic_probe,
        test_protocol
    ),
    components(schemas(
        super::favorites::FavoriteInput,
        super::favorites::FavoriteResult,
        Capture,
        Sample,
        FlowHistory,
        HistorySegment,
        ProbeInput,
        ServiceProbeInput,
        ProbeOutput,
        ProtocolTest
    ))
)]
pub struct DiagnosticsApi;
pub fn router(state: Diagnostics) -> Router {
    Router::new()
        .route(
            "/api/v1/traffic/flows/{id}/favorite",
            axum::routing::put(super::favorites::favorite),
        )
        .route("/api/v1/traffic/flows/{id}/capture", post(capture))
        .route("/api/v1/traffic/flows/{id}/history", get(flow_history))
        .route("/api/v1/traffic/samples/{id}", get(sample))
        .route("/api/v1/services/{id}/probe", post(probe))
        .route("/api/v1/diagnostics/probe", post(diagnostic_probe))
        .route("/api/v1/protocols/{id}/{version}/test", post(test_protocol))
        .with_state(state)
}

#[cfg(test)]
mod history_tests {
    use super::*;
    use crate::infrastructure::{crypto::Crypto, db::Database};
    use axum::{body::Body, http::Request};

    #[tokio::test]
    async fn history_read_is_authenticated_audited_and_records_proxy_failure() {
        use std::os::unix::fs::PermissionsExt;
        use tower::ServiceExt;

        let directory =
            std::env::temp_dir().join(format!("hydra-history-api-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let db = Database::open(directory.join("db")).await.unwrap();
        let crypto = Crypto::load(&directory.join("key")).unwrap();
        let auth = AuthState::new(db.clone(), crypto.clone(), "https://hydra.test".into());
        crate::modules::users::create(
            &db,
            &crypto,
            "reader".into(),
            "correct horse battery".into(),
        )
        .await
        .unwrap();
        let token = auth
            .login(
                "reader".into(),
                "correct horse battery".into(),
                "test".into(),
            )
            .await
            .unwrap()
            .response
            .access_token;
        let flow_id = uuid::Uuid::new_v4().to_string();
        let app = router(Diagnostics {
            auth,
            socket: directory.join("missing.sock"),
            traffic: None,
        });
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/traffic/flows/{flow_id}/history"))
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let rows = db
            .run(move |connection| {
                let mut statement = connection.prepare(
                    "SELECT action,target,actor_id,success FROM audit WHERE target=?1 ORDER BY id",
                )?;
                let rows = statement
                    .query_map([flow_id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, bool>(3)?,
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "traffic.history.read");
        assert_eq!(rows[1].0, "traffic.history.result");
        assert_eq!(rows[0].1, rows[1].1);
        assert_eq!(rows[0].2, rows[1].2);
        assert!(rows[0].3);
        assert!(!rows[1].3);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
