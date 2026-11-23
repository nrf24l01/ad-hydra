use crate::{
    api::{self, ProxyView},
    infrastructure::{crypto::Crypto, db::Database, proxy_client as proxy},
    modules::{auth::AuthState, services::Control, users},
};
use anyhow::{bail, ensure};
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
#[derive(Parser)]
#[command(version, about = "Hydra control plane; authenticated control plane")]
struct Args {
    #[arg(long, default_value = "/run/hydra/proxy.sock")]
    socket: PathBuf,
    #[arg(
        long,
        global = true,
        default_value = "/var/lib/hydra/core/hydra.sqlite3"
    )]
    database: PathBuf,
    #[arg(long, global = true, default_value = "/var/lib/hydra/core/jwt.pem")]
    key: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Status,
    Create {
        #[command(subcommand)]
        command: CreateCommand,
    },
    Apply {
        snapshot: PathBuf,
    },
    Serve {
        #[arg(long)]
        operations: Option<PathBuf>,
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
        #[arg(long)]
        snapshot: Option<PathBuf>,
        #[arg(long, default_value = "https://localhost:8080")]
        origin: String,
        #[arg(long, requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        #[arg(long, requires = "tls_cert")]
        tls_key: Option<PathBuf>,
    },
    #[command(hide = true)]
    SandboxChild {
        cgroup: PathBuf,
        bwrap: PathBuf,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<std::ffi::OsString>,
    },
    #[command(hide = true)]
    SandboxBridge {
        config: String,
    },
    Openapi,
    Events {
        #[arg(long, default_value_t = 2)]
        count: usize,
    },
}
#[derive(Subcommand)]
enum CreateCommand {
    User {
        username: String,
        password: Option<String>,
    },
}
pub async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::SandboxChild {
            cgroup,
            bwrap,
            args,
        } => crate::infrastructure::sandbox::worker_child(&cgroup, &bwrap, &args)?,
        Command::SandboxBridge { config } => {
            let code =
                crate::infrastructure::sandbox::bridge(serde_json::from_str(&config)?).await?;
            std::process::exit(code);
        }
        Command::Create {
            command: CreateCommand::User { username, password },
        } => {
            let db = Database::open(&args.database).await?;
            let crypto = Crypto::load(&args.key)?;
            let id = if let Some(password) = password {
                eprintln!(
                    "Warning: password argv can be exposed through process listings and shell history; omit it for a hidden prompt."
                );
                users::create(&db, &crypto, username, password).await?
            } else {
                users::create_prompt(&db, &crypto, username).await?
            };
            println!("{id}");
        }
        Command::Openapi => print!("{}", api::openapi_yaml()?),
        Command::Status => {
            let status = proxy::status(proxy::connect(&args.socket).await?).await?;
            println!(
                "{}",
                serde_json::to_string(&ProxyView::from_status(status))?
            );
        }
        Command::Apply { snapshot } => {
            let snapshot = proxy::load_snapshot(&snapshot).await?;
            let result =
                crate::infrastructure::maintenance::apply(&args.database, &args.socket, &snapshot)
                    .await?;
            println!(
                "{}",
                serde_json::json!({ "applied": result.applied, "applied_revision": result.applied_revision,
                "error_code": result.error_code, "error_message": result.error_message })
            );
            if !result.applied {
                bail!("snapshot rejected");
            }
        }
        Command::Events { count } => {
            ensure!((1..=10000).contains(&count), "count must be 1..10000");
            let mut stream = proxy::events(proxy::connect(&args.socket).await?).await?;
            for _ in 0..count {
                let batch = stream
                    .message()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("stream ended"))?;
                for event in batch.events {
                    println!(
                        "{}",
                        serde_json::json!({"event_id": event.event_id, "flow_id": event.flow_id,
                        "service_id": event.service_id, "payload": format!("{:?}", event.payload)})
                    );
                }
            }
        }
        Command::Serve {
            operations,
            listen,
            snapshot,
            origin,
            tls_cert,
            tls_key,
        } => {
            ensure!(
                listen.ip().is_loopback(),
                "management API with trusted documentation must bind to loopback; use an HTTPS reverse proxy"
            );
            let snapshot = match snapshot {
                Some(path) => Some(proxy::load_snapshot(&path).await?),
                None => None,
            };
            ensure!(
                origin.starts_with("https://") && !origin.ends_with('/'),
                "origin must be an HTTPS origin without trailing slash"
            );
            let db = Database::open(&args.database).await?;
            let crypto = Crypto::load(&args.key)?;
            let auth = AuthState::new(db, crypto, origin);
            let settings =
                crate::infrastructure::operations::Settings::load(operations.as_deref())?;
            hydra_compression::set_level(settings.traffic_history_compression_level)?;
            let view = Arc::new(RwLock::new(ProxyView::default()));
            let (history_policy, history_receiver) = tokio::sync::watch::channel((
                u64::from(settings.traffic_history_per_service_mib) * hydra_domain::history::MIB,
                settings.traffic_history_compression_level,
            ));
            let settings_view = crate::modules::settings::SettingsView::from_settings(&settings);
            let settings_state =
                crate::modules::settings::Settings::new(auth.clone(), settings_view, operations)?
                    .with_history_policy(history_policy, view.clone());
            // Migration ordering is a product invariant, not an SQL workaround:
            // `Flags::open` applies 0018, which rebuilds `flag_events`. The
            // favorites triggers created by `Traffic::open` (0024/0025) reference
            // `flag_events`, so opening Traffic first leaves a fresh database
            // with triggers pointing at the table 0018 is about to drop/rename.
            // Keep Flags before Traffic on every serve/bootstrap path.
            let flags = crate::modules::flags::Flags::open(auth.clone(), settings.flags()).await?;
            let farm = crate::modules::farm::Farm::open(
                auth.clone(),
                args.database
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join("exploits"),
                crate::infrastructure::operations::runner(
                    settings.sandbox,
                    settings.execution_mode,
                    settings.host_runtime,
                    flags.clone(),
                    auth.db.clone(),
                ),
            )
            .await?;
            let control = Control::open(auth.clone()).await?;
            let traffic = crate::modules::traffic::Traffic::open(auth.clone())
                .await?
                .with_runtime(args.socket.clone(), settings.flag_pattern.clone());
            // Attach live traffic after both modules have migrated so the
            // settings save path can refresh the highlight pattern in place.
            let settings_state = settings_state.with_traffic(traffic.clone());
            if let Some(snapshot) = snapshot.as_ref() {
                control.seed_snapshot(snapshot.clone()).await?;
            }
            let protocols = crate::modules::protocols::Protocols::open(
                auth.clone(),
                control.clone(),
                args.socket.clone(),
                args.database
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join("protocols"),
            )
            .await?;
            let listener = tokio::net::TcpListener::bind(listen).await?;
            let shutdown = CancellationToken::new();
            let submitter_flags = flags.clone();
            let submitter_shutdown = shutdown.child_token();
            let submitter_task =
                tokio::spawn(
                    async move { submitter_flags.run_submitter(submitter_shutdown).await },
                );
            let farm_task = tokio::spawn(farm.clone().background(shutdown.child_token()));
            let task = tokio::spawn(proxy::reconcile(
                args.socket.clone(),
                history_receiver,
                view.clone(),
                shutdown.clone(),
                control.clone(),
                traffic.clone(),
                flags.clone(),
            ));
            let signal = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                signal.cancel();
            });
            let term = shutdown.clone();
            tokio::spawn(async move {
                if let Ok(mut signal) =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                {
                    signal.recv().await;
                    term.cancel();
                }
            });
            tracing::info!(%listen, "core management API ready");
            let handle = axum_server::Handle::new();
            let stopping = shutdown.clone();
            let server_handle = handle.clone();
            tokio::spawn(async move {
                stopping.cancelled().await;
                server_handle.graceful_shutdown(Some(std::time::Duration::from_secs(3)));
            });
            let diagnostics = crate::modules::traffic::diagnostics::Diagnostics {
                auth: auth.clone(),
                socket: args.socket,
                traffic: Some(traffic.clone()),
            };
            diagnostics.resume_favorites(shutdown.clone());
            let router = api::router(
                view,
                api::Modules {
                    auth: auth.clone(),
                    control,
                    traffic,
                    protocols,
                    flags,
                    farm,
                    diagnostics,
                    settings: settings_state,
                },
            );
            let listener = listener.into_std()?;
            let result = if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
                let _ = rustls::crypto::ring::default_provider().install_default();
                let config =
                    axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
                let mut server =
                    axum_server::from_tcp_rustls(listener, config).map(api::acceptor::Limited::new);
                server
                    .http_builder()
                    .http1()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .max_headers(64)
                    .max_buf_size(32768)
                    .header_read_timeout(std::time::Duration::from_secs(5));
                server.http_builder().http2().max_concurrent_streams(16);
                server
                    .handle(handle)
                    .serve(router.into_make_service())
                    .await
            } else {
                let mut server = axum_server::from_tcp(listener).map(api::acceptor::Limited::new);
                server
                    .http_builder()
                    .http1()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .max_headers(64)
                    .max_buf_size(32768)
                    .header_read_timeout(std::time::Duration::from_secs(5));
                server.http_builder().http2().max_concurrent_streams(16);
                server
                    .handle(handle)
                    .serve(router.into_make_service())
                    .await
            };
            shutdown.cancel();
            task.await?;
            submitter_task.await??;
            farm_task.await?;
            result?;
        }
    }
    Ok(())
}
