pub mod acceptor;
mod request_audit;
mod static_files;
use crate::modules::{
    auth::{AuthState, IdentityApi},
    services::{Control, ControlApi},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode, header},
    routing::get,
};
use hydra_wire::v1;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use utoipa::{OpenApi, ToSchema};

#[derive(Clone, Debug, Default, Serialize, ToSchema)]
pub struct ServiceMetrics {
    pub service_id: String,
    pub active_flows: u64,
    pub accepted_flows: u64,
    pub rejected_flows: u64,
    pub bytes_client_to_server: u64,
    pub bytes_server_to_client: u64,
    pub dropped_datagrams: u64,
    pub rate_limited: u64,
}
#[derive(Clone, Debug, Default, Serialize, ToSchema)]
pub struct ForwardingStats {
    pub mappings: u64,
    pub reused: u64,
    pub restored: u64,
    pub errors: u64,
}
#[derive(Clone, Debug, Default, Serialize, ToSchema)]
pub struct ProxyView {
    pub request_checker_errors: u64,
    pub last_request_checker_error: String,
    pub forwarding_stats: Option<ForwardingStats>,
    pub service_metrics: Vec<ServiceMetrics>,
    pub connected: bool,
    pub history_per_service_bytes: u64,
    pub history_compression_level: Option<i32>,
    pub history_dropped: u64,
    pub proxy_epoch_id: String,
    pub active_flows: u32,
    pub applied_services_revision: u64,
    pub applied_rules_revision: u64,
    pub telemetry_dropped: u64,
    pub critical_spool_bytes: u64,
    pub critical_dropped: u64,
    pub sample_dropped: u64,
    pub rejected_flows: u64,
    pub dropped_udp_datagrams: u64,
    pub events_received: u64,
    pub indexing_dropped: u64,
}
impl ProxyView {
    pub fn from_status(s: v1::ProxyStatus) -> Self {
        Self {
            request_checker_errors: s.request_checker_errors,
            last_request_checker_error: s.last_request_checker_error,
            forwarding_stats: s.forwarding_stats.map(|m| ForwardingStats {
                mappings: m.mappings,
                reused: m.reused,
                restored: m.restored,
                errors: m.errors,
            }),
            service_metrics: s
                .service_metrics
                .into_iter()
                .map(|m| ServiceMetrics {
                    service_id: m.service_id,
                    active_flows: m.active_flows,
                    accepted_flows: m.accepted_flows,
                    rejected_flows: m.rejected_flows,
                    bytes_client_to_server: m.bytes_client_to_server,
                    bytes_server_to_client: m.bytes_server_to_client,
                    dropped_datagrams: m.dropped_datagrams,
                    rate_limited: m.rate_limited,
                })
                .collect(),
            connected: true,
            history_per_service_bytes: s.history_per_service_bytes,
            history_compression_level: (s.history_compression_level > 0)
                .then_some(s.history_compression_level),
            history_dropped: s.history_dropped,
            proxy_epoch_id: s.proxy_epoch_id,
            active_flows: s.active_flows,
            applied_services_revision: s.applied_services_revision,
            applied_rules_revision: s.applied_rules_revision,
            telemetry_dropped: s.dropped_telemetry_events,
            critical_spool_bytes: s.critical_spool_bytes,
            critical_dropped: s.critical_dropped,
            sample_dropped: s.sample_dropped,
            rejected_flows: s.rejected_flows,
            dropped_udp_datagrams: s.dropped_udp_datagrams,
            events_received: 0,
            indexing_dropped: 0,
        }
    }
}
pub type SharedView = Arc<RwLock<ProxyView>>;

#[derive(Clone)]
struct ApiState {
    view: SharedView,
    auth: AuthState,
}
#[utoipa::path(get, path = "/api/v1/system/proxy", responses((status = 200, description = "Cached Proxy status; disconnected is explicit", body = ProxyView)))]
async fn proxy(
    State(state): State<ApiState>,
    headers: HeaderMap,
) -> Result<Json<ProxyView>, StatusCode> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    state
        .auth
        .authenticate(bearer)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    Ok(Json(state.view.read().await.clone()))
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Hydra Core REST API",
        version = "0.1.0",
        description = "Authenticated Hydra control plane. Documentation is served only on trusted loopback management interface."
    ),
    paths(proxy),
    components(schemas(ProxyView))
)]
pub struct ApiDoc;

pub fn openapi_document() -> utoipa::openapi::OpenApi {
    let mut api = ApiDoc::openapi();
    api.info.license = None;
    api.merge(IdentityApi::openapi());
    api.merge(crate::modules::settings::SettingsApi::openapi());
    api.merge(ControlApi::openapi());
    api.merge(crate::modules::traffic::TrafficApi::openapi());
    api.merge(crate::modules::traffic::diagnostics::DiagnosticsApi::openapi());
    api.merge(crate::modules::protocols::ProtocolApi::openapi());
    api.merge(crate::modules::flags::FlagsApi::openapi());
    api.merge(crate::modules::farm::FarmApi::openapi());
    api.merge(crate::modules::filters::FilterApi::openapi());
    let components = api.components.get_or_insert_with(Default::default);
    components.add_security_scheme(
        "AccessJWT",
        utoipa::openapi::security::SecurityScheme::Http(utoipa::openapi::security::Http::new(
            utoipa::openapi::security::HttpAuthScheme::Bearer,
        )),
    );
    api.security = Some(vec![utoipa::openapi::security::SecurityRequirement::new(
        "AccessJWT",
        std::iter::empty::<String>(),
    )]);
    for path in ["/api/v1/auth/login", "/api/v1/auth/refresh"] {
        if let Some(operation) = api
            .paths
            .paths
            .get_mut(path)
            .and_then(|item| item.post.as_mut())
        {
            operation.security = Some(vec![]);
        }
    }
    api
}
pub fn openapi_yaml() -> anyhow::Result<String> {
    Ok(serde_yaml::to_string(&openapi_document())?)
}
pub struct Modules {
    pub auth: AuthState,
    pub control: Control,
    pub traffic: crate::modules::traffic::Traffic,
    pub protocols: crate::modules::protocols::Protocols,
    pub flags: crate::modules::flags::Flags,
    pub farm: crate::modules::farm::Farm,
    pub diagnostics: crate::modules::traffic::diagnostics::Diagnostics,
    pub settings: crate::modules::settings::Settings,
}
pub fn router(view: SharedView, modules: Modules) -> Router {
    let Modules {
        auth,
        control,
        traffic,
        protocols,
        flags,
        farm,
        diagnostics,
        settings,
    } = modules;
    let concurrency = Arc::new(tokio::sync::Semaphore::new(32));
    let preview_control = control.clone();
    let preview_socket = protocols.socket_path().to_path_buf();
    let audit_auth = auth.clone();
    Router::new()
        .route("/api/v1/system/proxy", get(proxy))
        .with_state(ApiState {
            view,
            auth: auth.clone(),
        })
        .merge(static_files::router())
        .merge(crate::modules::settings::router(settings))
        .merge(crate::modules::auth::router(auth))
        .merge(crate::modules::services::router(control))
        .merge(crate::modules::filters::preview_router(
            preview_control,
            preview_socket,
        ))
        .merge(crate::modules::traffic::router(traffic))
        .merge(crate::modules::traffic::diagnostics::router(diagnostics))
        .merge(crate::modules::protocols::router(protocols))
        .merge(crate::modules::flags::router(flags))
        .merge(crate::modules::farm::router(farm))
        .route(
            "/openapi.yaml",
            get(|| async {
                (
                    [("content-type", "application/yaml")],
                    openapi_yaml().expect("static schema"),
                )
            }),
        )
        .merge(
            utoipa_swagger_ui::SwaggerUi::new("/docs")
                .url("/api-docs/openapi.json", openapi_document()),
        )
        .layer(DefaultBodyLimit::max(256 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            audit_auth,
            request_audit::middleware,
        ))
        .layer(axum::middleware::from_fn(
            move |mut request: axum::extract::Request, next: axum::middleware::Next| {
                let concurrency = concurrency.clone();
                async move {
                    let id = uuid::Uuid::new_v4().to_string();
                    request
                        .extensions_mut()
                        .insert(request_audit::RequestId(id.clone()));
                    let Ok(_permit) = concurrency.try_acquire_owned() else {
                        return request_audit::failure(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "concurrency_limit",
                            "Too many concurrent requests",
                            &id,
                        );
                    };
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        next.run(request),
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(_) => request_audit::failure(
                            StatusCode::REQUEST_TIMEOUT,
                            "request_timeout",
                            "Request timed out; check operation state",
                            &id,
                        ),
                    }
                }
            },
        ))
}
#[cfg(test)]
mod history_audit_tests;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_contains_real_identity_and_control() {
        let yaml = openapi_yaml().unwrap();
        let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed["openapi"].as_str(), Some("3.1.0"));
        assert!(parsed["paths"]["/api/v1/auth/login"].is_mapping());
        assert!(parsed["paths"]["/api/v1/filters"].is_mapping());
        let mut ids = std::collections::BTreeSet::new();
        for path in parsed["paths"].as_mapping().unwrap().values() {
            for op in path.as_mapping().unwrap().values() {
                if let Some(id) = op["operationId"].as_str() {
                    assert!(ids.insert(id.to_owned()), "duplicate operation id {id}");
                }
            }
        }
    }
}
