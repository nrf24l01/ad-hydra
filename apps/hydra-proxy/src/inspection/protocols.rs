//! Durable plugin lifecycle. Compilation and filesystem work stay off Tokio workers.
use anyhow::{Context, Result, bail};
use hydra_protocol_runtime::{
    Direction, HttpDecoder, ParsedMessage, PluginVersion, SessionDecoder,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_VERSIONS: usize = 16;
const MAX_SERVICES: usize = 256;
#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    id: String,
    version: String,
    sha256: String,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct Manifest {
    versions: Vec<Entry>,
    #[serde(default)]
    profiles: BTreeMap<String, hydra_domain::protocols::FramingProfile>,
    active: BTreeMap<String, (String, String)>,
}
struct State {
    manifest: Manifest,
    versions: BTreeMap<(String, String), Arc<PluginVersion>>,
}
pub struct Protocols {
    directory: PathBuf,
    state: Arc<RwLock<State>>,
    mutation: Arc<Semaphore>,
    sessions: Arc<Semaphore>,
}
pub struct ProtocolSession {
    decoder: SessionDecoder,
    _permit: Option<OwnedSemaphorePermit>,
}
impl ProtocolSession {
    pub fn http_mut(&mut self) -> Option<&mut HttpDecoder> {
        match &mut self.decoder {
            SessionDecoder::Http(http) => Some(http),
            _ => None,
        }
    }
    pub fn is_stream_decoder(&self) -> bool {
        matches!(
            self.decoder,
            SessionDecoder::Raw(_)
                | SessionDecoder::Wasm(_)
                | SessionDecoder::Framing(_)
                | SessionDecoder::Grpc(_)
                | SessionDecoder::Utf8(_)
        )
    }
    pub fn feed(&mut self, direction: Direction, bytes: &[u8]) -> Result<Vec<ParsedMessage>> {
        Ok(self.decoder.feed(direction, bytes)?)
    }
    pub fn feed_datagram(
        &mut self,
        direction: Direction,
        bytes: &[u8],
    ) -> Result<Vec<ParsedMessage>> {
        Ok(self.decoder.feed_datagram(direction, bytes)?)
    }
}
impl Protocols {
    pub async fn open(directory: impl Into<PathBuf>) -> Result<Arc<Self>> {
        let directory = directory.into();
        let path = directory.clone();
        let state = tokio::task::spawn_blocking(move || load(&path)).await??;
        Ok(Arc::new(Self {
            directory,
            state: Arc::new(RwLock::new(state)),
            mutation: Arc::new(Semaphore::new(1)),
            sessions: Arc::new(Semaphore::new(8)),
        }))
    }
    pub async fn install(
        &self,
        id: &str,
        version: &str,
        sha256_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        valid_name(id)?;
        valid_name(version)?;
        if bytes.len() > 4 * 1024 * 1024 {
            bail!("module exceeds 4 MiB");
        }
        let hash = hash_bytes(sha256_hex)?;
        let guard = self
            .mutation
            .clone()
            .try_acquire_owned()
            .context("plugin mutation busy")?;
        let key = (id.to_owned(), version.to_owned());
        let mut manifest = {
            let state = self
                .state
                .read()
                .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?;
            anyhow::ensure!(
                !state
                    .manifest
                    .profiles
                    .contains_key(&format!("{id}/{version}")),
                "version already installed as framing profile"
            );
            if let Some(existing) = state.versions.get(&key) {
                if existing.sha256 == hash {
                    return Ok(());
                }
                bail!("plugin version already installed with a different checksum");
            }
            if state.versions.len() + state.manifest.profiles.len() >= MAX_VERSIONS {
                bail!("plugin version limit");
            }
            state.manifest.clone()
        };
        let entry = Entry {
            id: id.into(),
            version: version.into(),
            sha256: sha256_hex.to_ascii_lowercase(),
        };
        manifest.versions.push(entry.clone());
        let path = self.directory.clone();
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = guard;
            let loaded = PluginVersion::load(&bytes, hash)?;
            atomic_write(&path, &filename(&entry), &bytes)?;
            if let Err(error) =
                atomic_write(&path, "manifest.json", &serde_json::to_vec(&manifest)?)
            {
                let _ = fs::remove_file(path.join(filename(&entry)));
                return Err(error);
            }
            let mut state = state
                .write()
                .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?;
            state.versions.insert(key, loaded);
            state.manifest = manifest;
            Ok(())
        })
        .await??;
        Ok(())
    }
    pub async fn install_profile(
        &self,
        id: &str,
        version: &str,
        json: &str,
        digest: &str,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};
        valid_name(id)?;
        valid_name(version)?;
        anyhow::ensure!(json.len() <= 4096, "profile too large");
        anyhow::ensure!(
            format!("{:x}", Sha256::digest(json.as_bytes())) == digest,
            "profile checksum mismatch"
        );
        let profile: hydra_domain::protocols::FramingProfile = serde_json::from_str(json)?;
        profile.validate().map_err(anyhow::Error::msg)?;
        let _guard = self
            .mutation
            .clone()
            .try_acquire_owned()
            .context("protocol mutation busy")?;
        let key = format!("{id}/{version}");
        let mut manifest = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .manifest
            .clone();
        anyhow::ensure!(
            !manifest
                .versions
                .iter()
                .any(|v| v.id == id && v.version == version),
            "version already installed as WASM"
        );
        if let Some(old) = manifest.profiles.get(&key) {
            anyhow::ensure!(old == &profile, "immutable profile version conflict");
            return Ok(());
        }
        anyhow::ensure!(
            manifest.profiles.len() + manifest.versions.len() < MAX_VERSIONS,
            "protocol version quota"
        );
        manifest.profiles.insert(key, profile);
        let bytes = serde_json::to_vec(&manifest)?;
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || atomic_write(&directory, "manifest.json", &bytes))
            .await??;
        self.state
            .write()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .manifest = manifest;
        Ok(())
    }
    pub async fn activate(&self, service_id: &str, id: &str, version: &str) -> Result<()> {
        valid_name(service_id)?;
        valid_name(id)?;
        valid_name(version)?;
        let guard = self
            .mutation
            .clone()
            .try_acquire_owned()
            .context("plugin mutation busy")?;
        let mut manifest = {
            let state = self
                .state
                .read()
                .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?;
            if !state.versions.contains_key(&(id.into(), version.into()))
                && !state
                    .manifest
                    .profiles
                    .contains_key(&format!("{id}/{version}"))
            {
                bail!("plugin version not installed");
            }
            if !state.manifest.active.contains_key(service_id)
                && state.manifest.active.len() >= MAX_SERVICES
            {
                bail!("active service limit");
            }
            state.manifest.clone()
        };
        manifest
            .active
            .insert(service_id.into(), (id.into(), version.into()));
        let path = self.directory.clone();
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = guard;
            atomic_write(&path, "manifest.json", &serde_json::to_vec(&manifest)?)?;
            state
                .write()
                .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
                .manifest = manifest;
            Ok(())
        })
        .await??;
        Ok(())
    }
    /// Removes only the override; live sessions retain their existing decoder snapshot.
    pub async fn deactivate(&self, service_id: &str) -> Result<()> {
        valid_name(service_id)?;
        let _guard = self
            .mutation
            .clone()
            .try_acquire_owned()
            .context("protocol mutation busy")?;
        let mut manifest = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .manifest
            .clone();
        if manifest.active.remove(service_id).is_none() {
            return Ok(());
        }
        let directory = self.directory.clone();
        let bytes = serde_json::to_vec(&manifest)?;
        tokio::task::spawn_blocking(move || atomic_write(&directory, "manifest.json", &bytes))
            .await??;
        self.state
            .write()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .manifest = manifest;
        Ok(())
    }
    pub fn test(
        &self,
        id: &str,
        version: &str,
        direction: Direction,
        bytes: &[u8],
    ) -> Result<Vec<ParsedMessage>> {
        let _permit = self
            .sessions
            .clone()
            .try_acquire_owned()
            .context("WASM session limit")?;
        if let Some(profile) = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .manifest
            .profiles
            .get(&format!("{id}/{version}"))
            .cloned()
        {
            return Ok(
                hydra_protocol_runtime::FramingDecoder::new(id.into(), profile)?
                    .feed(direction, bytes)?,
            );
        }
        let version = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?
            .versions
            .get(&(id.into(), version.into()))
            .cloned()
            .context("plugin version not installed")?;
        let mut session = hydra_protocol_runtime::WasmSession::new(version)?;
        Ok(session.feed(direction, bytes)?)
    }
    pub fn metadata(&self, id: &str, version: &str) -> Result<String> {
        let state = self
            .state
            .read()
            .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?;
        if let Some(profile) = state.manifest.profiles.get(&format!("{id}/{version}")) {
            let fields = if matches!(profile.framing, hydra_domain::protocols::Framing::Raw) {
                serde_json::json!([])
            } else {
                serde_json::json!([{"path":"payload","field_type":"bytes"}])
            };
            return Ok(serde_json::json!({"name":id,"fields":fields}).to_string());
        }
        let version = state
            .versions
            .get(&(id.into(), version.into()))
            .cloned()
            .context("unknown protocol")?;
        drop(state);
        Ok(serde_json::to_string(&version.metadata)?)
    }
    pub fn version_for(&self, service_id: &str) -> Option<Arc<PluginVersion>> {
        let state = self.state.read().ok()?;
        state
            .manifest
            .active
            .get(service_id)
            .and_then(|key| state.versions.get(key))
            .cloned()
    }
    /// Allocates a WASM instance only at flow creation; never during packet feeds.
    pub fn session(&self, service_id: &str, protocol_id: &str) -> Result<ProtocolSession> {
        let profile = {
            let state = self
                .state
                .read()
                .map_err(|_| anyhow::anyhow!("protocol state poisoned"))?;
            state.manifest.active.get(service_id).and_then(|(id, v)| {
                state
                    .manifest
                    .profiles
                    .get(&format!("{id}/{v}"))
                    .map(|p| (id.clone(), p.clone()))
            })
        };
        if let Some((id, profile)) = profile {
            if matches!(profile.framing, hydra_domain::protocols::Framing::Raw) {
                return Ok(ProtocolSession {
                    decoder: SessionDecoder::Raw(hydra_protocol_runtime::RawDecoder::default()),
                    _permit: None,
                });
            }
            return Ok(ProtocolSession {
                decoder: SessionDecoder::Framing(hydra_protocol_runtime::FramingDecoder::new(
                    id, profile,
                )?),
                _permit: None,
            });
        }
        let (decoder, permit) = if let Some(version) = self.version_for(service_id) {
            let permit = self
                .sessions
                .clone()
                .try_acquire_owned()
                .context("WASM session limit")?;
            (
                SessionDecoder::Wasm(Box::new(hydra_protocol_runtime::WasmSession::new(version)?)),
                Some(permit),
            )
        } else {
            let decoder = match protocol_id {
                "http" | "http1" | "http1.1" => SessionDecoder::Http(HttpDecoder::default()),
                "grpc" | "h2" | "grpc-h2" => {
                    SessionDecoder::Grpc(hydra_protocol_runtime::GrpcDecoder::default())
                }
                "utf8" | "utf-8" | "text" => {
                    SessionDecoder::Utf8(hydra_protocol_runtime::Utf8Decoder::default())
                }
                _ => SessionDecoder::Raw(hydra_protocol_runtime::RawDecoder::default()),
            };
            (decoder, None)
        };
        Ok(ProtocolSession {
            decoder,
            _permit: permit,
        })
    }
}
fn valid_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        bail!("invalid protocol/service identifier");
    }
    Ok(())
}
fn hash_bytes(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("invalid SHA-256");
    }
    let mut result = [0; 32];
    for (i, item) in result.iter_mut().enumerate() {
        *item = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)?;
    }
    Ok(result)
}
fn filename(entry: &Entry) -> String {
    format!("{}-{}--{}.wasm", entry.id.len(), entry.id, entry.version)
}
fn atomic_write(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let temp = directory.join(".pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temp)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, directory.join(name))?;
    if let Err(error) = fs::File::open(directory).and_then(|file| file.sync_all()) {
        tracing::warn!(%error, path=%directory.display(), "plugin file committed; directory fsync failed, crash durability uncertain");
    }
    Ok(())
}
fn load(directory: &Path) -> Result<State> {
    fs::create_dir_all(directory)?;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    let manifest_path = directory.join("manifest.json");
    let manifest: Manifest = if manifest_path.exists() {
        if fs::metadata(&manifest_path)?.len() > 128 * 1024 {
            bail!("plugin manifest too large");
        }
        serde_json::from_slice(&fs::read(&manifest_path)?)?
    } else {
        Manifest::default()
    };
    for (key, profile) in &manifest.profiles {
        anyhow::ensure!(key.split_once('/').is_some(), "invalid profile key");
        profile.validate().map_err(anyhow::Error::msg)?;
    }
    if manifest.versions.len() + manifest.profiles.len() > MAX_VERSIONS
        || manifest.active.len() > MAX_SERVICES
    {
        bail!("plugin manifest limits");
    }
    let mut versions = BTreeMap::new();
    for entry in &manifest.versions {
        valid_name(&entry.id)?;
        valid_name(&entry.version)?;
        let path = directory.join(filename(entry));
        if fs::metadata(&path)?.len() > 4 * 1024 * 1024 {
            bail!("plugin module too large");
        }
        let module = PluginVersion::load(&fs::read(path)?, hash_bytes(&entry.sha256)?)?;
        if versions
            .insert((entry.id.clone(), entry.version.clone()), module)
            .is_some()
        {
            bail!("duplicate persisted plugin version");
        }
    }
    for (service, key) in &manifest.active {
        valid_name(service)?;
        if !versions.contains_key(key)
            && !manifest
                .profiles
                .contains_key(&format!("{}/{}", key.0, key.1))
        {
            bail!("activation references absent plugin");
        }
    }
    let expected: std::collections::BTreeSet<_> = manifest.versions.iter().map(filename).collect();
    for file in fs::read_dir(directory)? {
        let file = file?;
        let name = file.file_name();
        if name.to_str().is_some_and(|name| {
            (name.ends_with(".wasm") && !expected.contains(name)) || name == ".pending"
        }) {
            fs::remove_file(file.path())?;
        }
    }
    Ok(State { manifest, versions })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    fn module() -> (Vec<u8>, String) {
        let bytes = wat::parse_str(
            r#"(module (memory (export "memory") 1 64)
          (data (i32.const 1024) "{\22messages\22:[],\22state\22:[]}")
          (func (export "hydra_abi_version") (result i32) i32.const 1)
          (func (export "hydra_alloc") (param i32) (result i32) i32.const 2048)
          (func (export "hydra_dealloc") (param i32 i32 i32 i32))
          (func (export "hydra_decode") (param i32 i32) (result i64) i64.const 4398046511130))"#,
        )
        .unwrap();
        let hash = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        (bytes, hash)
    }
    #[tokio::test]
    async fn framing_profile_retains_snapshot_and_reloads() {
        let directory =
            std::env::temp_dir().join(format!("hydra-profile-test-{}", uuid::Uuid::new_v4()));
        let protocols = Protocols::open(&directory).await.unwrap();
        for (version, size) in [("1", 2), ("2", 3)] {
            let json =
                serde_json::json!({"framing":{"kind":"fixed_size","size":size},"max_frame":8})
                    .to_string();
            let digest = format!("{:x}", Sha256::digest(json.as_bytes()));
            protocols
                .install_profile("frames", version, &json, &digest)
                .await
                .unwrap();
        }
        protocols.activate("svc", "frames", "1").await.unwrap();
        let mut old = protocols.session("svc", "raw").unwrap();
        assert!(
            old.feed(Direction::ClientToServer, b"a")
                .unwrap()
                .is_empty()
        );
        protocols.activate("svc", "frames", "2").await.unwrap();
        assert_eq!(old.feed(Direction::ClientToServer, b"b").unwrap().len(), 1);
        let reopened = Protocols::open(&directory).await.unwrap();
        let mut new = reopened.session("svc", "raw").unwrap();
        assert!(
            new.feed(Direction::ClientToServer, b"ab")
                .unwrap()
                .is_empty()
        );
        assert_eq!(new.feed(Direction::ClientToServer, b"c").unwrap().len(), 1);
        let json = serde_json::json!({"framing":{"kind":"raw"},"max_frame":8}).to_string();
        let digest = format!("{:x}", Sha256::digest(json.as_bytes()));
        assert!(
            reopened
                .install_profile("frames", "1", &json, &digest)
                .await
                .is_err()
        );
        reopened.deactivate("svc").await.unwrap();
        reopened.deactivate("svc").await.unwrap();
        assert!(matches!(
            reopened.session("svc", "http").unwrap().decoder,
            SessionDecoder::Http(_)
        ));
        assert_eq!(
            new.feed(Direction::ClientToServer, b"def").unwrap().len(),
            1
        );
        assert!(matches!(
            Protocols::open(&directory)
                .await
                .unwrap()
                .session("svc", "raw")
                .unwrap()
                .decoder,
            SessionDecoder::Raw(_)
        ));
        fs::remove_dir_all(directory).unwrap();
    }
    #[tokio::test]
    async fn grpc_profile_resolves_to_the_native_stream_decoder() {
        let directory = std::env::temp_dir().join(format!("hydra-grpc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let protocols = Protocols::open(&directory).await.unwrap();
        for id in ["grpc", "h2", "grpc-h2"] {
            let mut session = protocols.session("svc", id).unwrap();
            assert!(session.is_stream_decoder(), "{id}");
            assert!(session.http_mut().is_none(), "{id}");
            // One DATA frame carrying one length-prefixed gRPC message.
            let mut frame = vec![0, 0, 8, 0, 0, 0, 0, 0, 1];
            frame.extend_from_slice(&[0, 0, 0, 0, 3]);
            frame.extend_from_slice(b"abc");
            let messages = session.feed(Direction::ClientToServer, &frame).unwrap();
            assert_eq!(messages.len(), 1, "{id}");
            assert_eq!(messages[0].protocol, "grpc", "{id}");
        }
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[tokio::test]
    async fn persistence_permissions_sessions_and_immutable_versions() {
        let directory =
            std::env::temp_dir().join(format!("hydra-protocol-test-{}", uuid::Uuid::new_v4()));
        let protocols = Protocols::open(&directory).await.unwrap();
        let (bytes, hash) = module();
        protocols
            .install("sample", "1.0", &hash, bytes.clone())
            .await
            .unwrap();
        protocols
            .activate("service", "sample", "1.0")
            .await
            .unwrap();
        let old = protocols.version_for("service").unwrap();
        let mut session = protocols.session("service", "raw").unwrap();
        assert!(
            session
                .feed(Direction::ClientToServer, b"abc")
                .unwrap()
                .is_empty()
        );
        protocols
            .install("sample", "2.0", &hash, bytes.clone())
            .await
            .unwrap();
        protocols
            .activate("service", "sample", "2.0")
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(
            &old,
            &protocols.version_for("service").unwrap()
        ));
        if let SessionDecoder::Wasm(decoder) = &mut session.decoder {
            assert!(Arc::ptr_eq(decoder.version(), &old));
        } else {
            panic!("WASM session expected")
        }
        let mut leases = vec![];
        for _ in 0..7 {
            leases.push(protocols.session("service", "raw").unwrap());
        }
        assert!(protocols.session("service", "raw").is_err());
        drop(leases);
        drop(session);
        assert!(protocols.session("service", "raw").is_ok());
        let reopened = Protocols::open(&directory).await.unwrap();
        assert!(reopened.version_for("service").is_some());
        assert!(matches!(
            &reopened.session("other", "http").unwrap().decoder,
            SessionDecoder::Http(_)
        ));
        assert!(matches!(
            &reopened.session("other", "unknown").unwrap().decoder,
            SessionDecoder::Raw(_)
        ));
        let mut utf8 = reopened.session("other", "utf8").unwrap();
        let decoded = utf8
            .feed(Direction::ClientToServer, "привет".as_bytes())
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].protocol, "utf8");
        assert_eq!(
            decoded[0].fields["utf8.text"],
            hydra_protocol_runtime::Value::String("привет".into())
        );
        for entry in fs::read_dir(&directory).unwrap() {
            assert_eq!(
                entry.unwrap().metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert!(
            reopened
                .install("bad/path", "1", &hash, bytes.clone())
                .await
                .is_err()
        );
        assert!(
            reopened
                .install("bad", "1", &"00".repeat(32), bytes)
                .await
                .is_err()
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
