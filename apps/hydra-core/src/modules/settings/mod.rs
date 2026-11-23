//! A redacted view of the actual operator configuration; credentials never leave Core.
use crate::{
    infrastructure::operations,
    modules::{
        audit,
        auth::{AuthState, Authenticated},
    },
};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
mod config;
mod file;
pub use config::{CheckerConfig, EditableConfig, SecretUpdate};
use utoipa::{OpenApi, ToSchema};
#[derive(Clone, Serialize, ToSchema)]
pub struct SettingsView {
    pub editable_config: EditableConfig,
    pub traffic_history_per_service_mib_applied: Option<u32>,
    pub history_apply_pending: bool,
    pub traffic_history_compression_level_applied: Option<i32>,
    pub compression_apply_pending: bool,
    pub core_compression_level_applied: i32,
    pub saved_revision: Option<String>,
    pub restart_required: bool,
    pub configuration_editable: bool,
    pub readonly_reason: Option<String>,
    pub flag_pattern: Option<String>,
    pub automatic_submission: bool,
    pub checker_adapter: Option<String>,
    pub checker_batch_size: usize,
    pub sandbox_configured: bool,
    pub worker_concurrency: usize,
    pub supported_worker_networks: Vec<String>,
    pub access_lifetime_seconds: u32,
    pub refresh_absolute_lifetime_seconds: u32,
}
impl SettingsView {
    pub fn from_settings(settings: &operations::Settings) -> Self {
        let (kind, batch) = match settings.checker.as_ref() {
            Some(operations::Checker::Http { .. }) => (Some("http"), 1),
            Some(operations::Checker::Https { .. }) => (Some("https"), 1),
            Some(operations::Checker::Tcp { .. }) => (Some("tcp"), 1),
            Some(operations::Checker::CustomTcp { batch_size, .. }) => {
                (Some("custom_tcp"), *batch_size)
            }
            None => (None, 1),
        };
        Self {
            editable_config: EditableConfig::redacted(settings)
                .expect("serializable operations config"),
            traffic_history_per_service_mib_applied: None,
            history_apply_pending: true,
            traffic_history_compression_level_applied: None,
            compression_apply_pending: true,
            core_compression_level_applied: settings.traffic_history_compression_level,
            saved_revision: None,
            restart_required: false,
            configuration_editable: false,
            readonly_reason: None,
            flag_pattern: settings.flag_pattern.clone(),
            automatic_submission: settings.auto_submit_captured,
            checker_adapter: kind.map(str::to_owned),
            checker_batch_size: batch,
            sandbox_configured: settings.sandbox.is_some(),
            worker_concurrency: 1,
            supported_worker_networks: vec!["IPv4 TCP".into(), "IPv4 UDP".into()],
            access_lifetime_seconds: 60,
            refresh_absolute_lifetime_seconds: 36000,
        }
    }
}
#[derive(Clone)]
pub struct Settings {
    pub auth: AuthState,
    pub view: SettingsView,
    pub file: Arc<Mutex<file::ConfigFile>>,
    history_policy: Option<tokio::sync::watch::Sender<(u64, i32)>>,
    proxy_view: Option<crate::api::SharedView>,
    /// Live traffic state that caches the highlight pattern; refreshed on save
    /// so a `flag_pattern` change applies without a Core restart.
    traffic: Option<crate::modules::traffic::Traffic>,
}
#[utoipa::path(get,path="/api/v1/system/settings",responses((status=200,body=SettingsView),(status=401)),tag="system")]
async fn settings(
    State(state): State<Settings>,
    Authenticated(actor): Authenticated,
) -> Result<Json<SettingsView>, StatusCode> {
    let request = audit::request_id();
    state
        .auth
        .db
        .run(move |db| {
            let tx = db.transaction()?;
            audit::append(
                &tx,
                &actor,
                "settings.read",
                "settings",
                &request,
                true,
                crate::modules::auth::now(),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    state.current_view().await.map(Json)
}
// Authenticated's extractor uses AuthState, so authenticate through a small state adapter.
impl axum::extract::FromRef<Settings> for AuthState {
    fn from_ref(state: &Settings) -> Self {
        state.auth.clone()
    }
}
#[derive(OpenApi)]
#[openapi(
    paths(settings, update_settings),
    components(schemas(
        SettingsView,
        UpdateSettingsRequest,
        EditableConfig,
        CheckerConfig,
        SecretUpdate
    ))
)]
pub struct SettingsApi;
pub fn router(state: Settings) -> Router {
    Router::new()
        .route(
            "/api/v1/system/settings",
            get(settings).put(update_settings),
        )
        .layer(axum::extract::DefaultBodyLimit::max(65536))
        .with_state(state)
}

impl Settings {
    pub fn new(
        auth: AuthState,
        view: SettingsView,
        path: Option<std::path::PathBuf>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            auth,
            view,
            file: Arc::new(Mutex::new(file::ConfigFile::new(path)?)),
            history_policy: None,
            proxy_view: None,
            traffic: None,
        })
    }
    pub fn with_history_policy(
        mut self,
        policy: tokio::sync::watch::Sender<(u64, i32)>,
        view: crate::api::SharedView,
    ) -> Self {
        self.history_policy = Some(policy);
        self.proxy_view = Some(view);
        self
    }
    /// Attach the traffic runtime so saved highlight patterns take effect live.
    pub fn with_traffic(mut self, traffic: crate::modules::traffic::Traffic) -> Self {
        self.traffic = Some(traffic);
        self
    }
    async fn current_view(&self) -> Result<SettingsView, StatusCode> {
        let file = self.file.lock().await.clone();
        let mut view = self.view.clone();
        view.core_compression_level_applied = hydra_compression::level();
        let (saved, reason) = tokio::task::spawn_blocking(move || {
            let reason = file.readonly_reason();
            file.read().map(|saved| {
                (
                    saved.map(|s| {
                        let restart = Some(s.revision.clone()) != file.running_revision
                            && Some(file::restart_revision(&s.settings))
                                != file.running_restart_revision;
                        (s.settings, s.revision.clone(), restart)
                    }),
                    reason,
                )
            })
        })
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if let Some((settings, revision, restart)) = saved {
            view.editable_config =
                EditableConfig::redacted(&settings).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            view.saved_revision = Some(revision);
            view.restart_required = restart;
        }
        if let Some(proxy) = &self.proxy_view {
            let proxy = proxy.read().await;
            view.traffic_history_per_service_mib_applied = (proxy.connected
                && proxy.history_per_service_bytes > 0)
                .then_some((proxy.history_per_service_bytes / hydra_domain::history::MIB) as u32);
            view.traffic_history_compression_level_applied = proxy
                .connected
                .then_some(proxy.history_compression_level)
                .flatten();
        }
        view.history_apply_pending = view.traffic_history_per_service_mib_applied
            != Some(view.editable_config.traffic_history_per_service_mib);
        view.compression_apply_pending = view.core_compression_level_applied
            != view.editable_config.traffic_history_compression_level
            || view.traffic_history_compression_level_applied
                != Some(view.editable_config.traffic_history_compression_level);
        view.configuration_editable = reason.is_none();
        view.readonly_reason = reason;
        Ok(view)
    }
}
#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateSettingsRequest {
    pub saved_revision: String,
    pub editable_config: EditableConfig,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct SettingsError {
    pub error: String,
}
type ErrorResponse = (StatusCode, Json<SettingsError>);
fn error(status: StatusCode, text: &str) -> ErrorResponse {
    (status, Json(SettingsError { error: text.into() }))
}
async fn record(
    state: &Settings,
    actor: audit::Actor,
    action: &'static str,
    success: bool,
    metadata: serde_json::Value,
) -> Result<(), ErrorResponse> {
    let request = audit::request_id();
    state
        .auth
        .db
        .run(move |db| {
            let tx = db.transaction()?;
            audit::append_change(
                &tx,
                &actor,
                action,
                "settings",
                &request,
                success,
                crate::modules::auth::now(),
                &metadata,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Settings audit unavailable",
            )
        })
}
#[utoipa::path(put,path="/api/v1/system/settings",request_body=UpdateSettingsRequest,responses((status=200,body=SettingsView),(status=400,body=SettingsError),(status=401),(status=403,body=SettingsError),(status=409,body=SettingsError),(status=503,body=SettingsError)),tag="system")]
async fn update_settings(
    State(state): State<Settings>,
    Authenticated(actor): Authenticated,
    headers: HeaderMap,
    Json(input): Json<UpdateSettingsRequest>,
) -> Result<Json<SettingsView>, ErrorResponse> {
    // Serialize API writers across revision check, audit intent, and atomic replacement.
    let file = state.file.lock().await;
    let history_mib = input.editable_config.traffic_history_per_service_mib;
    let compression_level = input.editable_config.traffic_history_compression_level;
    let metadata = serde_json::json!({"source":"operations_file","apply_on":"history_live_other_settings_core_restart","traffic_history_per_service_mib":history_mib,"traffic_history_compression_level":compression_level});
    record(
        &state,
        actor.clone(),
        "settings.update.intent",
        true,
        metadata.clone(),
    )
    .await?;
    let result = if headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())
        != Some(state.auth.origin.as_str())
    {
        Err(error(
            StatusCode::FORBIDDEN,
            "Settings changes require the configured Origin",
        ))
    } else if file.readonly_reason().is_some() {
        Err(error(
            StatusCode::FORBIDDEN,
            "Configuration file is read-only; see readonly_reason",
        ))
    } else {
        let file = file.clone();
        match tokio::task::spawn_blocking(move || {
            file.save(&input.saved_revision, &input.editable_config)
        })
        .await
        {
            Ok(Ok(saved)) => Ok((
                u64::from(saved.settings.traffic_history_per_service_mib)
                    * hydra_domain::history::MIB,
                saved.settings.traffic_history_compression_level,
                saved.settings.flag_pattern,
            )),
            Ok(Err(file::SaveError::Conflict)) => Err(error(
                StatusCode::CONFLICT,
                "Configuration changed; reload Settings before saving",
            )),
            Ok(Err(file::SaveError::Invalid(message))) => {
                Err(error(StatusCode::BAD_REQUEST, &message))
            }
            _ => Err(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Configuration could not be saved; reload Settings to check saved state",
            )),
        }
    };
    let outcome = serde_json::json!({"source":"operations_file","traffic_history_per_service_mib":history_mib,"traffic_history_compression_level":compression_level,"after":{"http_status":result.as_ref().map(|_|200).unwrap_or_else(|(code,_)|code.as_u16())}});
    record(
        &state,
        actor,
        "settings.update.result",
        result.is_ok(),
        outcome,
    )
    .await?;
    let (history_bytes, compression_level, flag_pattern) = result?;
    hydra_compression::set_level(compression_level)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid zstd compression level"))?;
    // Highlight pattern is owned by settings; recompile it for live Streams
    // frames instead of keeping the startup value until restart.
    if let Some(traffic) = &state.traffic {
        traffic.refresh_flag_pattern(flag_pattern.as_deref());
    }
    if let Some(policy) = &state.history_policy {
        policy.send_replace((history_bytes, compression_level));
    }
    drop(file);
    state.current_view().await.map(Json).map_err(|status| {
        error(
            status,
            "Configuration saved; reload Settings to read saved state",
        )
    })
}

#[cfg(test)]
mod tests;
