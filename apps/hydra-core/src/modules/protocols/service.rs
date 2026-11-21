use crate::{
    infrastructure::proxy_client,
    modules::{
        audit::{self, Actor},
        auth::{AuthState, now},
        services::Control,
    },
};
use hydra_wire::v1::{self, proxy_control_client::ProxyControlClient};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
#[derive(Clone)]
pub struct Protocols {
    pub auth: AuthState,
    control: Control,
    pub(super) socket: PathBuf,
    directory: PathBuf,
    pub admission: Arc<Semaphore>,
    _lock: Arc<std::fs::File>,
}
pub(super) fn name(s: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !s.is_empty()
            && s.len() <= 64
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "invalid protocol identifier"
    );
    Ok(())
}
pub(super) fn actor(a: &Actor, request: &str) -> v1::Actor {
    v1::Actor {
        r#type: 1,
        user_id: a.id.clone(),
        request_id: request.into(),
        ..Default::default()
    }
}
pub(super) fn request<T>(value: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(value);
    r.set_timeout(Duration::from_secs(5));
    r
}
impl Protocols {
    /// Core feature modules use the same configured Unix socket for internal RPC.
    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket
    }

    pub async fn open(
        auth: AuthState,
        control: Control,
        socket: PathBuf,
        directory: PathBuf,
    ) -> anyhow::Result<Self> {
        let path = directory.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            if !path.exists() {
                std::fs::DirBuilder::new().mode(0o700).create(&path)?;
            }
            let m = std::fs::symlink_metadata(&path)?;
            anyhow::ensure!(
                m.is_dir() && m.permissions().mode() & 0o077 == 0,
                "protocol directory must be private"
            );
            Ok(())
        })
        .await??;
        auth.db
            .migrate(
                4,
                include_str!("../../../../../migrations/0004_protocols.sql"),
            )
            .await?;
        auth.db
            .migrate(
                10,
                include_str!("../../../../../migrations/0010_protocol_profiles.sql"),
            )
            .await?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join(".lock"))?;
        lock.try_lock()
            .map_err(|e| anyhow::anyhow!("protocol storage already in use: {e}"))?;
        let known = auth
            .db
            .run(|db| {
                Ok(db
                    .prepare("SELECT id,version FROM protocols")?
                    .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?)
            })
            .await?;
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().ends_with(".wasm")
                && !known.iter().any(|(id, v)| {
                    name == std::ffi::OsString::from(format!("{}-{id}-{v}.wasm", id.len()))
                })
            {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(Self {
            auth,
            control,
            socket,
            directory,
            admission: Arc::new(Semaphore::new(1)),
            _lock: Arc::new(lock),
        })
    }
    pub async fn list(&self) -> anyhow::Result<Value> {
        self.auth.db.run(|db|{
  let versions=db.prepare("SELECT id,version,sha256,state,created_by,updated_by,strftime('%Y-%m-%dT%H:%M:%SZ',created_at,'unixepoch'),strftime('%Y-%m-%dT%H:%M:%SZ',updated_at,'unixepoch'),error,profile_json,metadata_json FROM protocols ORDER BY id,version LIMIT 16")?.query_map([],|r|Ok(json!({"id":r.get::<_,String>(0)?,"version":r.get::<_,String>(1)?,"sha256":r.get::<_,String>(2)?,"state":r.get::<_,String>(3)?,"created_by":r.get::<_,String>(4)?,"updated_by":r.get::<_,String>(5)?,"created_at":r.get::<_,String>(6)?,"updated_at":r.get::<_,String>(7)?,"error":r.get::<_,Option<String>>(8)?,"profile":r.get::<_,Option<String>>(9)?.and_then(|s|serde_json::from_str::<Value>(&s).ok()),"metadata":r.get::<_,Option<String>>(10)?.and_then(|s|serde_json::from_str::<Value>(&s).ok())})))?.collect::<Result<Vec<_>,_>>()?;
  let active=db.prepare("SELECT service_id,protocol_id,version,applied,updated_by,error FROM protocol_activations ORDER BY service_id LIMIT 128")?.query_map([],|r|Ok(json!({"service_id":r.get::<_,String>(0)?,"id":r.get::<_,String>(1)?,"version":r.get::<_,String>(2)?,"applied":r.get::<_,bool>(3)?,"updated_by":r.get::<_,String>(4)?,"error":r.get::<_,Option<String>>(5)?})))?.collect::<Result<Vec<_>,_>>()?;Ok(json!({"items":versions,"activations":active,"native_schemas":[{"id":"raw","fields":[{"path":"raw.bytes","field_type":"bytes"}]},{"id":"utf8","fields":[{"path":"utf8.text","field_type":"string"}]},{"id":"grpc","fields":[{"path":"grpc.stream_id","field_type":"unsigned"},{"path":"grpc.frame_type","field_type":"string"},{"path":"grpc.frame_length","field_type":"unsigned"},{"path":"grpc.end_stream","field_type":"bool"},{"path":"grpc.compressed","field_type":"bool"},{"path":"grpc.message_length","field_type":"unsigned"},{"path":"grpc.message_text","field_type":"string"},{"path":"grpc.message","field_type":"bytes"},{"path":"grpc.truncated","field_type":"bool"}]},{"id":"http","fields":[{"path":"http.method","field_type":"string"},{"path":"http.path","field_type":"string"},{"path":"http.user_agent","field_type":"string"},{"path":"http.headers","field_type":"list"},{"path":"http.trailers","field_type":"list"},{"path":"http.body_length","field_type":"unsigned"},{"path":"http.status_code","field_type":"unsigned"},{"path":"http.reason","field_type":"string"},{"path":"http.content_type","field_type":"string"},{"path":"http.body_text","field_type":"string"},{"path":"http.body_preview_truncated","field_type":"bool"}]}]}))
 }).await
    }
    pub async fn profile(
        &self,
        a: Actor,
        id: String,
        version: String,
        profile: hydra_domain::protocols::FramingProfile,
    ) -> anyhow::Result<Value> {
        profile.validate().map_err(anyhow::Error::msg)?;
        let bytes = serde_json::to_vec(&profile)?;
        let hash = format!("{:x}", Sha256::digest(&bytes));
        self.upload_inner(a, id, version, hash, bytes, true).await
    }
    pub async fn upload(
        &self,
        a: Actor,
        id: String,
        version: String,
        expected: String,
        bytes: Vec<u8>,
    ) -> anyhow::Result<Value> {
        self.upload_inner(a, id, version, expected, bytes, false)
            .await
    }
    async fn upload_inner(
        &self,
        a: Actor,
        id: String,
        version: String,
        expected: String,
        bytes: Vec<u8>,
        framing: bool,
    ) -> anyhow::Result<Value> {
        name(&id)?;
        name(&version)?;
        anyhow::ensure!(bytes.len() <= 4 * 1024 * 1024, "module exceeds 4 MiB");
        let digest = format!("{:x}", Sha256::digest(&bytes));
        anyhow::ensure!(digest == expected.to_ascii_lowercase(), "checksum mismatch");
        let old = self
            .auth
            .db
            .run({
                let id = id.clone();
                let version = version.clone();
                move |db| {
                    Ok(db
                        .query_row(
                            "SELECT sha256 FROM protocols WHERE id=?1 AND version=?2",
                            params![id, version],
                            |r| r.get::<_, String>(0),
                        )
                        .optional()?)
                }
            })
            .await?;
        let fresh = old.is_none();
        let module_path = self
            .directory
            .join(format!("{}-{id}-{version}.wasm", id.len()));
        if let Some(old) = old {
            anyhow::ensure!(old == digest, "immutable version conflict");
        } else {
            let count = self
                .auth
                .db
                .run(|db| {
                    Ok(db.query_row("SELECT count(*) FROM protocols", [], |r| r.get::<_, i64>(0))?)
                })
                .await?;
            anyhow::ensure!(count < 16, "protocol version quota exhausted");
            if !framing {
                let path = self
                    .directory
                    .join(format!("{}-{id}-{version}.wasm", id.len()));
                let copy = bytes.clone();
                tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let mut f = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(path)?;
                    f.write_all(&copy)?;
                    f.sync_all()?;
                    Ok(())
                })
                .await??;
            }
        }
        let req = uuid::Uuid::new_v4().to_string();
        let profile_json = if framing {
            Some(String::from_utf8(bytes.clone())?)
        } else {
            None
        };
        let stored_profile = profile_json.clone();
        let recorded=self.auth.db.run({let id=id.clone();let version=version.clone();let digest=digest.clone();let a=a.clone();let req=req.clone();move|db|{let tx=db.transaction()?;tx.execute("INSERT INTO protocols(id,version,sha256,state,created_by,updated_by,created_at,updated_at,error,profile_json) VALUES(?1,?2,?3,'pending',?4,?4,?5,?5,NULL,?6) ON CONFLICT(id,version) DO UPDATE SET updated_by=excluded.updated_by,updated_at=excluded.updated_at,state='pending',error=NULL",params![id,version,digest,a.id,now(),stored_profile])?;audit::append_change(&tx,&a,"protocol.upload",&format!("{id}/{version}"),&req,true,now(),&json!({"sha256":digest,"source":"http"}))?;tx.commit()?;Ok(())}}).await;
        if let Err(error) = recorded {
            if fresh {
                let _ = tokio::fs::remove_file(module_path).await;
            }
            return Err(error);
        }
        let result = async {
            let channel = proxy_client::connect(&self.socket).await?;
            let r = ProxyControlClient::new(channel)
                .install_protocol(request(v1::InstallProtocolRequest {
                    protocol_id: id.clone(),
                    version: version.clone(),
                    sha256_hex: digest.clone(),
                    wasm: if framing { Vec::new() } else { bytes },
                    framing_profile_json: profile_json.unwrap_or_default(),
                    request_id: req.clone(),
                    actor: Some(actor(&a, &req)),
                }))
                .await?
                .into_inner();
            anyhow::ensure!(r.installed, "{}", r.error_message);
            self.auth
                .db
                .run({
                    let id = id.clone();
                    let version = version.clone();
                    move |db| {
                        db.execute(
                            "UPDATE protocols SET metadata_json=?3 WHERE id=?1 AND version=?2",
                            params![id, version, r.metadata_json],
                        )?;
                        Ok(())
                    }
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let error = result
            .err()
            .map(|e| e.to_string().chars().take(2048).collect::<String>());
        let installed = error.is_none();
        self.auth
            .db
            .run({
                let id = id.clone();
                let version = version.clone();
                let error = error.clone();
                let a = a.clone();
                move |db| {
                    let tx = db.transaction()?;
                    tx.execute(
                        "UPDATE protocols SET state=?3,error=?4 WHERE id=?1 AND version=?2",
                        params![
                            id,
                            version,
                            if installed { "installed" } else { "error" },
                            error
                        ],
                    )?;
                    audit::append(
                        &tx,
                        &a,
                        "protocol.install",
                        &format!("{id}/{version}"),
                        &req,
                        installed,
                        now(),
                    )?;
                    tx.commit()?;
                    Ok(())
                }
            })
            .await?;
        Ok(
            json!({"id":id,"version":version,"state":if installed{"installed"}else{"error"},"error":error}),
        )
    }
    pub async fn activate(
        &self,
        a: Actor,
        id: String,
        version: String,
        service_id: String,
        key: String,
    ) -> anyhow::Result<Value> {
        name(&id)?;
        name(&version)?;
        anyhow::ensure!(
            (16..=128).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_graphic()),
            "Idempotency-Key must have 16..128 printable bytes"
        );
        anyhow::ensure!(
            self.control
                .desired()
                .await?
                .services
                .services
                .iter()
                .any(|s| s.id == service_id),
            "unknown service"
        );
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(format!("{id}:{version}:{service_id}"))
        );
        let req = uuid::Uuid::new_v4().to_string();
        self.auth.db.run({let a=a.clone();let id=id.clone();let version=version.clone();let service=service_id.clone();let req=req.clone();move|db|{let tx=db.transaction()?;
   let state:String=tx.query_row("SELECT state FROM protocols WHERE id=?1 AND version=?2",params![id,version],|r|r.get(0))?;anyhow::ensure!(state=="installed","protocol not installed");
   let old=tx.query_row("SELECT fingerprint FROM protocol_operations WHERE actor=?1 AND key=?2",params![a.id,key],|r|r.get::<_,String>(0)).optional()?;
   if let Some(old)=old{anyhow::ensure!(old==fingerprint,"idempotency key conflict")}else{let n:i64=tx.query_row("SELECT count(*) FROM protocol_operations",[],|r|r.get(0))?;anyhow::ensure!(n<10000,"operation quota exhausted");tx.execute("INSERT INTO protocol_operations VALUES(?1,?2,?3)",params![a.id,key,fingerprint])?;}
   let count:i64=tx.query_row("SELECT count(*) FROM protocol_activations",[],|r|r.get(0))?;let existing:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM protocol_activations WHERE service_id=?1)",[&service],|r|r.get(0))?;anyhow::ensure!(existing||count<256,"activation quota exhausted");
   tx.execute("INSERT INTO protocol_activations VALUES(?1,?2,?3,0,?4,?5,NULL) ON CONFLICT(service_id) DO UPDATE SET protocol_id=excluded.protocol_id,version=excluded.version,applied=0,updated_by=excluded.updated_by,updated_at=excluded.updated_at,error=NULL",params![service,id,version,a.id,now()])?;
   audit::append(&tx,&a,"protocol.activate",&service,&req,true,now())?;tx.commit()?;Ok(())}}).await?;
        let result = async {
            let c = proxy_client::connect(&self.socket).await?;
            let result = ProxyControlClient::new(c)
                .activate_protocol(request(v1::ActivateProtocolRequest {
                    protocol_id: id.clone(),
                    version: version.clone(),
                    service_id: service_id.clone(),
                    request_id: req.clone(),
                    actor: Some(actor(&a, &req)),
                }))
                .await?
                .into_inner();
            anyhow::ensure!(result.applied, "{}", result.error_message);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let error = result
            .err()
            .map(|e| e.to_string().chars().take(2048).collect::<String>());
        let applied = error.is_none();
        self.auth
            .db
            .run({
                let service = service_id.clone();
                let error = error.clone();
                move |db| {
                    db.execute(
                        "UPDATE protocol_activations SET applied=?2,error=?3 WHERE service_id=?1",
                        params![service, applied, error],
                    )?;
                    Ok(())
                }
            })
            .await?;
        Ok(json!({"service_id":service_id,"applied":applied,"error":error}))
    }
}
use std::os::unix::fs::DirBuilderExt;
