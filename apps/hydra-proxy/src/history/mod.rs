//! Bounded, zstd-compressed tails of completed TCP and UDP flows. Capture never writes
//! to disk on the relay task; one bounded worker owns persistence and cleanup.
mod favorites;
mod storage;
mod tcp;
use hydra_domain::Service;
use hydra_protocol_sdk::Direction;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use storage::read_record;
use tokio::sync::{mpsc, oneshot};

pub const MAX_DIRECTION_BYTES: usize = 64 * 1024;
pub const MAX_HISTORY_BYTES: usize = MAX_DIRECTION_BYTES * 2;
pub const MAX_DATAGRAMS: usize = 1024;
pub const MAX_ENCODED_BYTES: usize = MAX_HISTORY_BYTES + MAX_DATAGRAMS * 21;
const MAX_ACTIVE: usize = 256;
fn tcp_transport() -> String {
    "tcp".into()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Datagram {
    pub direction: u8,
    pub occurred_at_ms: i64,
    pub sequence: u64,
    pub data: Vec<u8>,
}
const WRITE_QUEUE: usize = 64;
const TTL_SECONDS: i64 = 24 * 60 * 60;
const FILE_PREFIX: &str = "hydra-history-";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowHistory {
    #[serde(default = "tcp_transport")]
    pub transport: String,
    #[serde(default)]
    pub datagrams: Vec<Datagram>,
    #[serde(default)]
    pub datagrams_client_to_server: u64,
    #[serde(default)]
    pub datagrams_server_to_client: u64,
    pub flow_id: String,
    pub service_id: String,
    pub protocol_id: String,
    pub opened_at_ms: i64,
    pub closed_at_ms: i64,
    pub bytes_client_to_server: u64,
    pub bytes_server_to_client: u64,
    pub client_to_server: Vec<u8>,
    pub server_to_client: Vec<u8>,
    pub truncated_client_to_server: bool,
    pub truncated_server_to_client: bool,
    pub history_truncated: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Metadata {
    #[serde(default)]
    ordered_tcp: bool,
    #[serde(default = "tcp_transport")]
    transport: String,
    #[serde(default)]
    datagram_count: usize,
    #[serde(default)]
    datagrams_client_to_server: u64,
    #[serde(default)]
    datagrams_server_to_client: u64,
    flow_id: String,
    service_id: String,
    protocol_id: String,
    opened_at_ms: i64,
    closed_at_ms: i64,
    bytes_client_to_server: u64,
    bytes_server_to_client: u64,
    client_len: usize,
    server_len: usize,
    truncated_client_to_server: bool,
    truncated_server_to_client: bool,
    history_truncated: bool,
}

#[derive(Default)]
struct Buffer {
    service_id: String,
    protocol_id: String,
    opened_at_ms: i64,
    truncated_client: bool,
    truncated_server: bool,
    // Bytes removed from the head of each direction, by direction index.
    // Cursors carry these counters so a reader can distinguish an append-only
    // tail from one whose beginning was evicted.
    dropped: [u64; 2],
    discarded: bool,
    udp: bool,
    datagrams: VecDeque<Datagram>,
    datagram_bytes: [usize; 2],
    datagram_totals: [u64; 2],
}

enum WriteCommand {
    Favorite {
        id: String,
        favorite: bool,
        snapshot: Option<Box<FlowHistory>>,
        reply: oneshot::Sender<Result<String, String>>,
    },
    Persist(Box<FlowHistory>),
    SetQuota {
        bytes: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    SetPolicy {
        bytes: u64,
        compression_level: i32,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

struct Inner {
    directory: PathBuf,
    enabled_services: Mutex<HashSet<String>>,
    active: Mutex<HashMap<String, Arc<Mutex<Buffer>>>>,
    pending: Arc<Mutex<HashSet<String>>>,
    writer: mpsc::Sender<WriteCommand>,
    quota: Arc<AtomicU64>,
    compression_level: Arc<std::sync::atomic::AtomicI32>,
    dropped: Arc<AtomicU64>,
}

pub struct HistoryStore(Arc<Inner>);

pub struct HistoryWriter {
    flow_id: String,
    buffer: Arc<Mutex<Buffer>>,
    store: Weak<Inner>,
}

#[derive(Debug)]
pub enum HistoryReadError {
    Active,
    Storage(anyhow::Error),
}
impl std::fmt::Display for HistoryReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => formatter.write_str("flow history is still active"),
            Self::Storage(_) => formatter.write_str("history storage failed"),
        }
    }
}
impl std::error::Error for HistoryReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error.as_ref()),
            Self::Active => None,
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Client cursor for [`HistoryStore::active_snapshot`].
///
/// The sequence fields are the last chunk sequence observed in each direction,
/// not a global offset. `*_head_dropped` are the matching retained-head byte
/// counters. A cursor that no longer describes the retained head forces a full
/// snapshot (`reset = true`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ActiveCursor {
    pub(crate) client_sequence: u64,
    pub(crate) server_sequence: u64,
    pub(crate) client_head_dropped: u64,
    pub(crate) server_head_dropped: u64,
}

/// Bounded, in-memory snapshot of an active flow. Segments reuse [`Datagram`]
/// because its direction/sequence/occurred_at/data shape is identical to the
/// generated `HistorySegment` wire message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveSnapshot {
    pub(crate) flow_id: String,
    pub(crate) service_id: String,
    pub(crate) protocol_id: String,
    pub(crate) opened_at_ms: i64,
    // True for whole-datagram UDP records; the gRPC handler maps this to
    // `HistorySegment.datagram`.
    pub(crate) udp: bool,
    pub(crate) segments: Vec<Datagram>,
    pub(crate) truncated_client_to_server: bool,
    pub(crate) truncated_server_to_client: bool,
    pub(crate) retained_client_to_server_bytes: u64,
    pub(crate) retained_server_to_client_bytes: u64,
    pub(crate) reset: bool,
    pub(crate) client_sequence: u64,
    pub(crate) server_sequence: u64,
    pub(crate) client_head_dropped: u64,
    pub(crate) server_head_dropped: u64,
    pub(crate) generated_at_ms: i64,
}

fn last_sequences(datagrams: &VecDeque<Datagram>) -> (u64, u64) {
    let mut last = (0, 0);
    for chunk in datagrams {
        if chunk.direction == 1 {
            last.0 = chunk.sequence;
        } else {
            last.1 = chunk.sequence;
        }
    }
    last
}

fn retained_bytes(datagrams: &VecDeque<Datagram>) -> (u64, u64) {
    let mut bytes = [0u64; 2];
    for chunk in datagrams {
        let index = usize::from(chunk.direction == 2);
        bytes[index] = bytes[index].saturating_add(chunk.data.len() as u64);
    }
    (bytes[0], bytes[1])
}

fn cursor_sequence_retained(datagrams: &VecDeque<Datagram>, direction: u8, sequence: u64) -> bool {
    sequence == 0
        || datagrams
            .iter()
            .any(|chunk| chunk.direction == direction && chunk.sequence == sequence)
}

impl HistoryStore {
    /// Open a private history directory and resume completed, unexpired records.
    pub async fn open(directory: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let directory = directory.as_ref().to_owned();
        let checked = directory.clone();
        let mut disk =
            tokio::task::spawn_blocking(move || storage::Disk::open(checked, now_ms())).await??;
        let (writer, mut reader) = mpsc::channel::<WriteCommand>(WRITE_QUEUE);
        let quota = Arc::new(AtomicU64::new(disk.quota()));
        let worker_quota = quota.clone();
        let compression_level =
            Arc::new(std::sync::atomic::AtomicI32::new(disk.compression_level()));
        let worker_level = compression_level.clone();
        let dropped = Arc::new(AtomicU64::new(0));
        let worker_dropped = dropped.clone();
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let worker_pending = pending.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(60);
            let mut reap =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            reap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let item = tokio::select! {
                    item = reader.recv() => match item { Some(item) => Some(item), None => break },
                    _ = reap.tick() => None,
                };
                let flow_id = match &item {
                    Some(WriteCommand::Persist(history)) => Some(history.flow_id.clone()),
                    _ => None,
                };
                let applied_quota = worker_quota.clone();
                let applied_level = worker_level.clone();
                let completed = tokio::task::spawn_blocking(move || {
                    let result = match item {
                        Some(WriteCommand::Persist(history)) => {
                            let archive = favorites::update(&disk.directory_path(), &history);
                            let ordinary = disk.persist(*history);
                            archive.and(ordinary)
                        }
                        Some(WriteCommand::Favorite {
                            id,
                            favorite,
                            snapshot,
                            reply,
                        }) => {
                            let result = favorites::set(
                                &disk.directory_path(),
                                &id,
                                favorite,
                                snapshot.map(|v| *v),
                            );
                            let _ = reply.send(result.map_err(|e| e.to_string()));
                            Ok(())
                        }
                        Some(WriteCommand::SetQuota { bytes, reply }) => {
                            let result = disk.set_quota(bytes);
                            if result.is_ok() {
                                applied_quota.store(bytes, Ordering::Relaxed);
                            }
                            let _ = reply
                                .send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
                            result
                        }
                        Some(WriteCommand::SetPolicy {
                            bytes,
                            compression_level,
                            reply,
                        }) => {
                            let result = disk.set_policy(bytes, compression_level);
                            if result.is_ok() {
                                applied_quota.store(bytes, Ordering::Relaxed);
                                applied_level.store(compression_level, Ordering::Relaxed);
                            }
                            let _ = reply
                                .send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
                            result
                        }
                        None => disk.reap(now_ms()),
                    };
                    (disk, result)
                })
                .await;
                let result;
                match completed {
                    Ok((updated, outcome)) => {
                        disk = updated;
                        result = outcome;
                    }
                    Err(_) => {
                        worker_dropped.fetch_add(1, Ordering::Relaxed);
                        worker_pending
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clear();
                        tracing::warn!("flow history worker stopped");
                        break;
                    }
                }
                if result.is_err() {
                    if let Some(id) = &flow_id {
                        let dropped = worker_dropped.fetch_add(1, Ordering::Relaxed) + 1;
                        tracing::warn!(flow_id=%id,dropped,"completed stream history was dropped");
                    } else {
                        tracing::warn!("flow history policy or cleanup failed");
                    }
                }
                if let Some(flow_id) = flow_id {
                    worker_pending
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&flow_id);
                }
            }
        });
        Ok(Arc::new(Self(Arc::new(Inner {
            directory,
            enabled_services: Mutex::new(HashSet::new()),
            active: Mutex::new(HashMap::new()),
            pending,
            writer,
            quota,
            compression_level,
            dropped,
        }))))
    }

    /// Apply the current policy. Disabling retention clears active buffers for
    /// that service immediately; the current relay flow continues unchanged.
    pub fn apply_services(&self, services: &[Service]) {
        let enabled: HashSet<String> = services
            .iter()
            .filter(|service| service.retain_history)
            .map(|service| service.id.clone())
            .collect();
        *self
            .0
            .enabled_services
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = enabled.clone();
        let mut active = self.0.active.lock().unwrap_or_else(|e| e.into_inner());
        active.retain(|_, buffer| {
            let mut buffer = buffer.lock().unwrap_or_else(|e| e.into_inner());
            if !enabled.contains(&buffer.service_id) {
                buffer.discarded = true;
                false
            } else {
                true
            }
        });
    }

    pub fn begin(
        &self,
        flow_id: String,
        service_id: String,
        protocol_id: String,
    ) -> Option<HistoryWriter> {
        if flow_id.len() > 128
            || !self
                .0
                .enabled_services
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&service_id)
        {
            return None;
        }
        let mut active = self.0.active.lock().unwrap_or_else(|e| e.into_inner());
        if active.len() >= MAX_ACTIVE || active.contains_key(&flow_id) {
            self.0.dropped.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let buffer = Arc::new(Mutex::new(Buffer {
            service_id,
            protocol_id,
            opened_at_ms: now_ms(),
            ..Default::default()
        }));
        active.insert(flow_id.clone(), buffer.clone());
        Some(HistoryWriter {
            flow_id,
            buffer,
            store: Arc::downgrade(&self.0),
        })
    }

    pub fn begin_udp(
        &self,
        flow_id: String,
        service_id: String,
        protocol_id: String,
    ) -> Option<HistoryWriter> {
        let writer = self.begin(flow_id, service_id, protocol_id)?;
        writer.buffer.lock().unwrap_or_else(|e| e.into_inner()).udp = true;
        Some(writer)
    }

    pub fn quota(&self) -> u64 {
        self.0.quota.load(Ordering::Relaxed)
    }

    pub fn compression_level(&self) -> i32 {
        self.0.compression_level.load(Ordering::Relaxed)
    }

    pub async fn set_policy(&self, bytes: u64, compression_level: i32) -> anyhow::Result<()> {
        use hydra_domain::history::{MAX_PER_SERVICE_MIB, MIB, MIN_PER_SERVICE_MIB};
        anyhow::ensure!(
            (u64::from(MIN_PER_SERVICE_MIB) * MIB..=u64::from(MAX_PER_SERVICE_MIB) * MIB)
                .contains(&bytes),
            "history quota must be 1..1024 MiB per service"
        );
        anyhow::ensure!(
            (hydra_compression::MIN_LEVEL..=hydra_compression::MAX_LEVEL)
                .contains(&compression_level),
            "compression level must be 1..=19"
        );
        let (reply, completed) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::SetPolicy {
                bytes,
                compression_level,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?;
        completed
            .await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?
            .map_err(anyhow::Error::msg)
    }

    pub async fn set_quota(&self, bytes: u64) -> anyhow::Result<()> {
        use hydra_domain::history::{MAX_PER_SERVICE_MIB, MIB, MIN_PER_SERVICE_MIB};
        anyhow::ensure!(
            (u64::from(MIN_PER_SERVICE_MIB) * MIB..=u64::from(MAX_PER_SERVICE_MIB) * MIB)
                .contains(&bytes),
            "history quota must be 1..1024 MiB per service"
        );
        let (reply, completed) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::SetQuota { bytes, reply })
            .await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?;
        completed
            .await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?
            .map_err(anyhow::Error::msg)
    }

    pub fn dropped(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }

    pub async fn set_favorite(&self, flow_id: &str, favorite: bool) -> anyhow::Result<String> {
        anyhow::ensure!(uuid::Uuid::parse_str(flow_id).is_ok(), "invalid flow id");
        let snapshot = self
            .0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(flow_id)
            .map(|buffer| {
                let b = buffer.lock().unwrap_or_else(|e| e.into_inner());
                let (client, server) = b.tcp_payloads();
                Box::new(FlowHistory {
                    transport: if b.udp { "udp" } else { "tcp" }.into(),
                    datagrams: b.datagrams.iter().cloned().collect(),
                    datagrams_client_to_server: if b.udp { b.datagram_totals[0] } else { 0 },
                    datagrams_server_to_client: if b.udp { b.datagram_totals[1] } else { 0 },
                    flow_id: flow_id.into(),
                    service_id: b.service_id.clone(),
                    protocol_id: b.protocol_id.clone(),
                    opened_at_ms: b.opened_at_ms,
                    closed_at_ms: 0,
                    // Final forwarded counters are supplied only on completion.
                    // A capture tail includes blocked observations and cannot
                    // substitute for those totals in an interrupted snapshot.
                    bytes_client_to_server: 0,
                    bytes_server_to_client: 0,
                    client_to_server: client,
                    server_to_client: server,
                    truncated_client_to_server: true,
                    truncated_server_to_client: true,
                    history_truncated: true,
                })
            });
        let (reply, rx) = oneshot::channel();
        self.0
            .writer
            .send(WriteCommand::Favorite {
                id: flow_id.into(),
                favorite,
                snapshot,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("history worker unavailable"))?
            .map_err(anyhow::Error::msg)
    }

    pub async fn get(&self, flow_id: &str) -> Result<Option<FlowHistory>, HistoryReadError> {
        if flow_id.len() > 128 || uuid::Uuid::parse_str(flow_id).is_err() {
            return Ok(None);
        }
        if self
            .0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(flow_id)
        {
            return Err(HistoryReadError::Active);
        }
        for _ in 0..20 {
            if !self
                .0
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(flow_id)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if self
            .0
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(flow_id)
        {
            return Err(HistoryReadError::Active);
        }
        let directory = self.0.directory.clone();
        let id = flow_id.to_owned();
        tokio::task::spawn_blocking(move || {
            if let Some(record) = favorites::get(&directory, &id)? {
                return Ok(Some(record));
            }
            read_record(&directory, &id)
        })
        .await
        .map_err(|error| HistoryReadError::Storage(error.into()))?
        .map_err(HistoryReadError::Storage)
    }

    /// Copy the bounded in-memory tail of an active flow.
    ///
    /// This never touches disk and never waits for the persistence worker. A
    /// closed or unknown flow yields `Ok(None)`. A cursor that still describes
    /// the retained head yields only newer chunks; an unchanged cursor yields
    /// an empty delta. Eviction or an inconsistent cursor forces `reset = true`
    /// and returns the whole retained tail.
    pub(crate) fn active_snapshot(
        &self,
        flow_id: &str,
        cursor: Option<ActiveCursor>,
    ) -> Result<Option<ActiveSnapshot>, HistoryReadError> {
        if flow_id.is_empty() || flow_id.len() > 128 || uuid::Uuid::parse_str(flow_id).is_err() {
            return Ok(None);
        }
        let Some(buffer) = self
            .0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(flow_id)
            .cloned()
        else {
            return Ok(None);
        };
        let buffer = buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buffer.discarded {
            return Ok(None);
        }
        let (client_sequence, server_sequence) = last_sequences(&buffer.datagrams);
        let (retained_client, retained_server) = retained_bytes(&buffer.datagrams);
        let reset = match cursor {
            None => true,
            Some(cursor) => {
                cursor.client_head_dropped != buffer.dropped[0]
                    || cursor.server_head_dropped != buffer.dropped[1]
                    || cursor.client_sequence > client_sequence
                    || cursor.server_sequence > server_sequence
                    || !cursor_sequence_retained(&buffer.datagrams, 1, cursor.client_sequence)
                    || !cursor_sequence_retained(&buffer.datagrams, 2, cursor.server_sequence)
            }
        };
        let segments = match cursor {
            Some(cursor) if !reset => buffer
                .datagrams
                .iter()
                .filter(|chunk| {
                    if chunk.direction == 1 {
                        chunk.sequence > cursor.client_sequence
                    } else {
                        chunk.sequence > cursor.server_sequence
                    }
                })
                .cloned()
                .collect(),
            _ => buffer.datagrams.iter().cloned().collect(),
        };
        Ok(Some(ActiveSnapshot {
            flow_id: flow_id.to_owned(),
            service_id: buffer.service_id.clone(),
            protocol_id: buffer.protocol_id.clone(),
            opened_at_ms: buffer.opened_at_ms,
            udp: buffer.udp,
            segments,
            truncated_client_to_server: buffer.truncated_client,
            truncated_server_to_client: buffer.truncated_server,
            retained_client_to_server_bytes: retained_client,
            retained_server_to_client_bytes: retained_server,
            reset,
            client_sequence,
            server_sequence,
            client_head_dropped: buffer.dropped[0],
            server_head_dropped: buffer.dropped[1],
            generated_at_ms: now_ms(),
        }))
    }
}

impl HistoryWriter {
    pub fn record(&mut self, direction: Direction, bytes: &[u8]) {
        let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buffer.discarded {
            return;
        }
        if buffer.udp {
            let index = usize::from(direction == Direction::ServerToClient);
            buffer.datagram_totals[index] = buffer.datagram_totals[index].saturating_add(1);
            let sequence = buffer.datagram_totals[0].saturating_add(buffer.datagram_totals[1]);
            // Evict whole oldest datagrams only. Empty datagrams consume record
            // capacity, so zero-byte traffic cannot grow memory without bound.
            while buffer.datagrams.len() >= MAX_DATAGRAMS
                || buffer.datagram_bytes[index].saturating_add(bytes.len()) > MAX_DIRECTION_BYTES
            {
                let Some(old) = buffer.datagrams.pop_front() else {
                    break;
                };
                let old_index = usize::from(old.direction == 2);
                buffer.datagram_bytes[old_index] -= old.data.len();
                buffer.dropped[old_index] =
                    buffer.dropped[old_index].saturating_add(old.data.len() as u64);
                if old_index == 0 {
                    buffer.truncated_client = true;
                } else {
                    buffer.truncated_server = true;
                }
            }
            if bytes.len() > MAX_DIRECTION_BYTES {
                buffer.dropped[index] = buffer.dropped[index].saturating_add(bytes.len() as u64);
                if index == 0 {
                    buffer.truncated_client = true;
                } else {
                    buffer.truncated_server = true;
                }
                return;
            }
            buffer.datagram_bytes[index] += bytes.len();
            buffer.datagrams.push_back(Datagram {
                direction: if index == 0 { 1 } else { 2 },
                occurred_at_ms: now_ms(),
                sequence,
                data: bytes.to_vec(),
            });
            return;
        }
        buffer.record_tcp(direction, bytes);
    }

    /// Returns true if the retained history is incomplete or could not be queued.
    pub fn finish(
        self,
        closed_at_ms: i64,
        bytes_client_to_server: u64,
        bytes_server_to_client: u64,
    ) -> bool {
        let Some(store) = self.store.upgrade() else {
            return true;
        };
        store
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.flow_id);
        let buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buffer.discarded {
            return false;
        }
        let (client, server) = buffer.tcp_payloads();
        let client_len = client.len();
        let server_len = server.len();
        let history = FlowHistory {
            transport: if buffer.udp { "udp" } else { "tcp" }.into(),
            datagrams: buffer.datagrams.iter().cloned().collect(),
            datagrams_client_to_server: if buffer.udp {
                buffer.datagram_totals[0]
            } else {
                0
            },
            datagrams_server_to_client: if buffer.udp {
                buffer.datagram_totals[1]
            } else {
                0
            },
            flow_id: self.flow_id.clone(),
            service_id: buffer.service_id.clone(),
            protocol_id: buffer.protocol_id.clone(),
            opened_at_ms: buffer.opened_at_ms,
            closed_at_ms,
            bytes_client_to_server,
            bytes_server_to_client,
            client_to_server: client,
            server_to_client: server,
            truncated_client_to_server: buffer.truncated_client
                || (!buffer.udp && bytes_client_to_server > client_len as u64),
            truncated_server_to_client: buffer.truncated_server
                || (!buffer.udp && bytes_server_to_client > server_len as u64),
            history_truncated: buffer.truncated_client
                || buffer.truncated_server
                || (!buffer.udp && bytes_client_to_server > client_len as u64)
                || (!buffer.udp && bytes_server_to_client > server_len as u64),
        };
        store
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(self.flow_id.clone());
        let truncated = history.history_truncated;
        match store
            .writer
            .try_send(WriteCommand::Persist(Box::new(history)))
        {
            Ok(()) => truncated,
            Err(_) => {
                store
                    .pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&self.flow_id);
                store.dropped.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::storage::{cleanup, cleanup_for_quota, paths, persist, write_private};
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use hydra_domain::Transport;
    use std::{fs, io::Write};
    use std::{net::SocketAddr, path::PathBuf};

    pub(super) struct Directory(pub(super) PathBuf);
    impl Directory {
        pub(super) fn new() -> Self {
            let path = std::env::temp_dir().join(format!("hydra-history-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
            }
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn service(retain_history: bool) -> Service {
        Service {
            forwarding: None,
            id: "svc".into(),
            transport: Transport::Tcp,
            listen_address: "127.0.0.1:23001".parse::<SocketAddr>().unwrap(),
            upstream_address: "127.0.0.1:23002".parse().unwrap(),
            protocol_id: "raw".into(),
            enabled: true,
            retain_history,
            protection: None,
            tls: None,
        }
    }
    async fn stored(store: &HistoryStore, id: &str) -> FlowHistory {
        for _ in 0..100 {
            if let Some(history) = store.get(id).await.unwrap() {
                return history;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("history was not persisted")
    }

    #[tokio::test]
    async fn udp_history_retention_is_enabled_for_configured_udp_services() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        let mut udp = service(true);
        udp.transport = Transport::Udp;
        store.apply_services(&[udp]);
        assert!(
            store
                .begin(uuid::Uuid::new_v4().to_string(), "svc".into(), "raw".into())
                .is_some()
        );
    }

    #[tokio::test]
    async fn udp_history_roundtrips_empty_binary_direction_order_and_timestamps() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store
            .begin_udp(id.clone(), "svc".into(), "utf8".into())
            .unwrap();
        writer.record(Direction::ClientToServer, &[0, 255, 1]);
        writer.record(Direction::ServerToClient, &[]);
        writer.record(Direction::ClientToServer, &[]);
        writer.record(Direction::ServerToClient, &[255, 0]);
        assert!(!writer.finish(now_ms(), 3, 2));
        let saved = stored(&store, &id).await;
        assert_eq!(saved.transport, "udp");
        assert_eq!(
            saved
                .datagrams
                .iter()
                .map(|d| (d.direction, d.sequence, d.data.clone()))
                .collect::<Vec<_>>(),
            vec![
                (1, 1, vec![0, 255, 1]),
                (2, 2, vec![]),
                (1, 3, vec![]),
                (2, 4, vec![255, 0])
            ]
        );
        assert!(
            saved
                .datagrams
                .windows(2)
                .all(|d| d[0].occurred_at_ms <= d[1].occurred_at_ms)
        );
        assert!(
            saved
                .datagrams
                .iter()
                .all(|d| d.occurred_at_ms >= saved.opened_at_ms
                    && d.occurred_at_ms <= saved.closed_at_ms)
        );
        assert_eq!(
            (
                saved.datagrams_client_to_server,
                saved.datagrams_server_to_client
            ),
            (2, 2)
        );
        assert!(!saved.history_truncated);
        assert!(saved.client_to_server.is_empty() && saved.server_to_client.is_empty());
        drop(store);
        let reopened = HistoryStore::open(&dir.0).await.unwrap();
        assert_eq!(reopened.get(&id).await.unwrap().unwrap(), saved);
    }

    #[tokio::test]
    async fn udp_tail_evicts_whole_datagrams_and_bounds_empty_datagram_count() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store
            .begin_udp(id.clone(), "svc".into(), "raw".into())
            .unwrap();
        for byte in [1, 2, 3] {
            writer.record(Direction::ClientToServer, &vec![byte; 32_768]);
        }
        writer.record(Direction::ServerToClient, &[9]);
        assert!(writer.finish(now_ms(), 98_304, 1));
        let saved = stored(&store, &id).await;
        assert_eq!(saved.datagrams.len(), 3);
        assert_eq!(saved.datagrams[0].sequence, 2);
        assert_eq!(saved.datagrams[0].data, vec![2; 32_768]);
        assert_eq!(saved.datagrams[1].data, vec![3; 32_768]);
        assert!(saved.truncated_client_to_server);
        assert!(!saved.truncated_server_to_client);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store
            .begin_udp(id.clone(), "svc".into(), "raw".into())
            .unwrap();
        for _ in 0..MAX_DATAGRAMS + 9 {
            writer.record(Direction::ClientToServer, &[]);
        }
        assert!(writer.finish(now_ms(), 0, 0));
        let saved = stored(&store, &id).await;
        assert_eq!(saved.datagrams.len(), MAX_DATAGRAMS);
        assert_eq!(saved.datagrams[0].sequence, 10);
        assert_eq!(saved.datagrams_client_to_server, (MAX_DATAGRAMS + 9) as u64);
        assert!(saved.history_truncated && saved.truncated_client_to_server);
    }

    #[tokio::test]
    async fn asynchronous_persistence_failure_counts_writer_loss() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store
            .begin_udp(id.clone(), "svc".into(), "raw".into())
            .unwrap();
        writer.record(Direction::ClientToServer, b"observed");
        // The worker must refuse to overwrite an existing file and report loss.
        write_private(&paths(&dir.0, &id).0, b"existing file").unwrap();
        assert!(!writer.finish(now_ms(), 8, 0));
        for _ in 0..100 {
            if store.dropped() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(store.dropped(), 1);
        assert!(store.get(&id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn quota_is_applied_by_worker_and_persisted_without_restarting_relays() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        assert_eq!(store.quota(), 128 * 1024 * 1024);
        assert!(store.set_quota(0).await.is_err());
        assert!(store.set_quota(1025 * 1024 * 1024).await.is_err());
        assert!(store.set_policy(2 * 1024 * 1024, 0).await.is_err());
        store.set_policy(2 * 1024 * 1024, 9).await.unwrap();
        assert_eq!(store.quota(), 2 * 1024 * 1024);
        assert_eq!(store.compression_level(), 9);
        drop(store);
        let reopened = HistoryStore::open(&dir.0).await.unwrap();
        assert_eq!(reopened.quota(), 2 * 1024 * 1024);
        assert_eq!(reopened.compression_level(), 9);
    }

    #[tokio::test]
    async fn appending_history_does_not_reopen_unrelated_metadata() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let first = uuid::Uuid::new_v4().to_string();
        store
            .begin(first.clone(), "svc".into(), "raw".into())
            .unwrap()
            .finish(now_ms(), 0, 0);
        stored(&store, &first).await;
        // Corrupt an unrelated sidecar after indexing. Appending must touch only
        // the new record, not enumerate and reopen every retained history.
        let (_, metadata) = paths(&dir.0, &first);
        fs::write(metadata, b"unrelated unreadable metadata").unwrap();
        let second = uuid::Uuid::new_v4().to_string();
        store
            .begin(second.clone(), "svc".into(), "raw".into())
            .unwrap()
            .finish(now_ms(), 0, 0);
        stored(&store, &second).await;
        assert!(store.get(&first).await.is_err());
    }

    #[tokio::test]
    async fn zstd_history_preserves_direction_order_tail_and_survives_restart() {
        let dir = Directory::new();
        let flow_id = uuid::Uuid::new_v4().to_string();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let mut writer = store
            .begin(flow_id.clone(), "svc".into(), "raw".into())
            .unwrap();
        writer.record(Direction::ClientToServer, &vec![1; 70_000]);
        writer.record(Direction::ClientToServer, &[2, 3]);
        writer.record(Direction::ServerToClient, &[9, 8]);
        assert!(writer.finish(now_ms(), 70_002, 2));
        let first = store.get(&flow_id).await.unwrap().unwrap();
        assert_eq!(first.client_to_server.len(), 65_536);
        assert_eq!(
            &first.client_to_server[first.client_to_server.len() - 2..],
            &[2, 3]
        );
        assert_eq!(first.server_to_client, [9, 8]);
        assert!(first.truncated_client_to_server);
        assert!(!first.truncated_server_to_client);
        drop(store);
        let reopened = HistoryStore::open(&dir.0).await.unwrap();
        assert_eq!(reopened.get(&flow_id).await.unwrap().unwrap(), first);
        let gz = std::fs::read_dir(&dir.0)
            .unwrap()
            .map(Result::unwrap)
            .find(|e| e.path().extension().is_some_and(|ext| ext == "zst"))
            .unwrap();
        assert_eq!(
            &std::fs::read(gz.path()).unwrap()[..4],
            &[0x28, 0xb5, 0x2f, 0xfd]
        );
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(gz.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&dir.0).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[tokio::test]
    async fn disabling_service_discards_active_history_and_prevents_new_records() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store.begin(id.clone(), "svc".into(), "raw".into()).unwrap();
        writer.record(Direction::ClientToServer, b"secret");
        store.apply_services(&[service(false)]);
        writer.record(Direction::ClientToServer, b"discarded");
        assert!(!writer.finish(now_ms(), 15, 0));
        assert!(store.get(&id).await.unwrap().is_none());
        assert!(
            store
                .begin(uuid::Uuid::new_v4().to_string(), "svc".into(), "raw".into())
                .is_none()
        );
    }

    #[tokio::test]
    async fn active_history_limit_is_bounded() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let mut writers = Vec::new();
        for _ in 0..MAX_ACTIVE {
            writers.push(
                store
                    .begin(uuid::Uuid::new_v4().to_string(), "svc".into(), "raw".into())
                    .unwrap(),
            );
        }
        assert!(
            store
                .begin(uuid::Uuid::new_v4().to_string(), "svc".into(), "raw".into())
                .is_none()
        );
        drop(writers);
    }

    #[tokio::test]
    async fn full_writer_queue_reports_capture_loss() {
        let dir = Directory::new();
        let (writer, _reader) = mpsc::channel(1);
        let store = Arc::new(HistoryStore(Arc::new(Inner {
            directory: dir.0.clone(),
            enabled_services: Mutex::new(HashSet::from(["svc".into()])),
            active: Mutex::new(HashMap::new()),
            pending: Arc::new(Mutex::new(HashSet::new())),
            writer,
            quota: Arc::new(AtomicU64::new(128 * 1024 * 1024)),
            compression_level: Arc::new(std::sync::atomic::AtomicI32::new(
                hydra_compression::DEFAULT_LEVEL,
            )),
            dropped: Arc::new(AtomicU64::new(0)),
        })));
        let first_id = uuid::Uuid::new_v4().to_string();
        let second_id = uuid::Uuid::new_v4().to_string();
        let mut first = store.begin(first_id, "svc".into(), "raw".into()).unwrap();
        let mut second = store.begin(second_id, "svc".into(), "raw".into()).unwrap();
        first.record(Direction::ClientToServer, b"first");
        second.record(Direction::ClientToServer, b"second");
        assert!(!first.finish(now_ms(), 5, 0));
        assert!(second.finish(now_ms(), 6, 0));
        assert_eq!(store.dropped(), 1);
    }

    #[tokio::test]
    async fn corrupt_and_oversized_gzip_are_rejected() {
        let dir = Directory::new();
        let store = HistoryStore::open(&dir.0).await.unwrap();
        store.apply_services(&[service(true)]);
        let corrupt_id = uuid::Uuid::new_v4().to_string();
        let mut writer = store
            .begin(corrupt_id.clone(), "svc".into(), "raw".into())
            .unwrap();
        writer.record(Direction::ClientToServer, b"valid");
        writer.finish(now_ms(), 5, 0);
        let _ = stored(&store, &corrupt_id).await;
        let (payload_path, _) = paths(&dir.0, &corrupt_id);
        fs::write(&payload_path, b"not gzip").unwrap();
        assert!(store.get(&corrupt_id).await.is_err());

        let oversized_id = uuid::Uuid::new_v4().to_string();
        let details = Metadata {
            ordered_tcp: false,
            transport: "tcp".into(),
            datagram_count: 0,
            datagrams_client_to_server: 0,
            datagrams_server_to_client: 0,
            flow_id: oversized_id.clone(),
            service_id: "svc".into(),
            protocol_id: "raw".into(),
            opened_at_ms: now_ms(),
            closed_at_ms: now_ms(),
            bytes_client_to_server: MAX_HISTORY_BYTES as u64,
            bytes_server_to_client: 0,
            client_len: MAX_DIRECTION_BYTES,
            server_len: MAX_DIRECTION_BYTES,
            truncated_client_to_server: false,
            truncated_server_to_client: false,
            history_truncated: false,
        };
        let (payload_path, metadata_path) = paths(&dir.0, &oversized_id);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&vec![0; MAX_HISTORY_BYTES + 1]).unwrap();
        write_private(&payload_path, &encoder.finish().unwrap()).unwrap();
        write_private(&metadata_path, &serde_json::to_vec(&details).unwrap()).unwrap();
        assert!(store.get(&oversized_id).await.is_err());
    }

    #[test]
    fn compressed_quota_evicts_oldest_record() {
        let dir = Directory::new();
        let now = now_ms();
        for _ in 0..2 {
            let id = uuid::Uuid::new_v4().to_string();
            persist(
                &dir.0,
                FlowHistory {
                    transport: "tcp".into(),
                    datagrams: vec![],
                    datagrams_client_to_server: 0,
                    datagrams_server_to_client: 0,
                    flow_id: id,
                    service_id: "svc".into(),
                    protocol_id: "raw".into(),
                    opened_at_ms: now,
                    closed_at_ms: now,
                    bytes_client_to_server: 1,
                    bytes_server_to_client: 0,
                    client_to_server: vec![42],
                    server_to_client: vec![],
                    truncated_client_to_server: false,
                    truncated_server_to_client: false,
                    history_truncated: false,
                },
            )
            .unwrap();
        }
        cleanup_for_quota(&dir.0, now, 1, 1).unwrap();
        let remaining = fs::read_dir(&dir.0)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "json")
            })
            .count();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn expired_records_are_removed_during_cleanup() {
        let dir = Directory::new();
        let id = uuid::Uuid::new_v4().to_string();
        let closed_at = now_ms() - (TTL_SECONDS + 1) * 1000;
        persist(
            &dir.0,
            FlowHistory {
                transport: "tcp".into(),
                datagrams: vec![],
                datagrams_client_to_server: 0,
                datagrams_server_to_client: 0,
                flow_id: id.clone(),
                service_id: "svc".into(),
                protocol_id: "raw".into(),
                opened_at_ms: closed_at - 1000,
                closed_at_ms: closed_at,
                bytes_client_to_server: 1,
                bytes_server_to_client: 0,
                client_to_server: vec![42],
                server_to_client: vec![],
                truncated_client_to_server: false,
                truncated_server_to_client: false,
                history_truncated: false,
            },
        )
        .unwrap();
        cleanup(&dir.0, now_ms()).unwrap();
        let (payload, metadata) = paths(&dir.0, &id);
        assert!(!payload.exists());
        assert!(!metadata.exists());
    }
}

#[cfg(test)]
mod terminal_history_tests {
    use super::*;
    #[tokio::test]
    async fn tcp_history_keeps_client_server_interleaving_after_restart() {
        let dir =
            std::env::temp_dir().join(format!("hydra-terminal-history-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let store = HistoryStore::open(dir.clone()).await.unwrap();
        store.apply_services(&[serde_json::from_value(serde_json::json!({"id":"nc","transport":"tcp","listen_address":"127.0.0.1:7117","upstream_address":"127.0.0.1:17117","protocol_id":"utf8","enabled":true})).unwrap()]);
        let id = uuid::Uuid::new_v4().to_string();
        let mut writer = store.begin(id.clone(), "nc".into(), "utf8".into()).unwrap();
        writer.record(Direction::ServerToClient, b"Name: ");
        writer.record(Direction::ClientToServer, b"Alice\n");
        writer.record(Direction::ServerToClient, b"Hello Alice\n");
        assert!(!writer.finish(now_ms(), 6, 18));
        let mut found = None;
        for _ in 0..100 {
            found = store.get(&id).await.unwrap();
            if found.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let history = found.unwrap();
        assert_eq!(
            history
                .datagrams
                .iter()
                .map(|s| s.direction)
                .collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert_eq!(history.client_to_server, b"Alice\n");
        assert_eq!(history.server_to_client, b"Name: Hello Alice\n");
        drop(store);
        let restarted = HistoryStore::open(dir.clone()).await.unwrap();
        assert_eq!(
            restarted.get(&id).await.unwrap().unwrap().datagrams,
            history.datagrams
        );
        drop(restarted);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod active_tests;
