//! Regression tests for the on-demand active-byte snapshot (Streams workspace, spec §4.2).
//!
//! Authored against the frozen interface in
//! `docs/superpowers/plans/2026-11-23-streams-workspace.md` (T1 step 3). Production is expected to
//! land, in `history/mod.rs`:
//!
//! ```ignore
//! pub struct ActiveCursor {
//!     pub client_sequence: u64,
//!     pub server_sequence: u64,
//!     pub client_head_dropped: u64,
//!     pub server_head_dropped: u64,
//! }
//! pub struct ActiveSnapshot {
//!     pub flow_id: String,
//!     pub service_id: String,
//!     pub protocol_id: String,
//!     pub opened_at_ms: i64,
//!     pub segments: Vec<Datagram>,
//!     pub truncated_client_to_server: bool,
//!     pub truncated_server_to_client: bool,
//!     pub retained_client_to_server_bytes: u64,
//!     pub retained_server_to_client_bytes: u64,
//!     pub reset: bool,
//!     pub client_sequence: u64,
//!     pub server_sequence: u64,
//!     pub client_head_dropped: u64,
//!     pub server_head_dropped: u64,
//!     pub generated_at_ms: i64,
//! }
//! impl HistoryStore {
//!     pub fn active_snapshot(
//!         &self,
//!         flow_id: &str,
//!         cursor: Option<ActiveCursor>,
//!     ) -> Result<Option<ActiveSnapshot>, HistoryReadError>;
//! }
//! ```
//!
//! `None` means the flow is not active (closed/unknown); the store never falls back to disk.
//! This file is included by `history/mod.rs` as `#[cfg(test)] mod active_tests;` (T1 owns that
//! line) and must not be registered anywhere else.

use super::storage::paths;
use super::tests::Directory;
use super::*;
use hydra_domain::Transport;
use std::net::SocketAddr;

fn service() -> Service {
    Service {
        forwarding: None,
        id: "svc".into(),
        transport: Transport::Tcp,
        listen_address: "127.0.0.1:23001".parse::<SocketAddr>().unwrap(),
        upstream_address: "127.0.0.1:23002".parse().unwrap(),
        protocol_id: "raw".into(),
        enabled: true,
        retain_history: true,
        protection: None,
        tls: None,
    }
}

fn begin(store: &HistoryStore, id: &str) -> HistoryWriter {
    store
        .begin(id.to_owned(), "svc".into(), "raw".into())
        .expect("retained service must admit an active flow")
}

fn begin_udp(store: &HistoryStore, id: &str) -> HistoryWriter {
    store
        .begin_udp(id.to_owned(), "svc".into(), "raw".into())
        .expect("retained service must admit an active UDP flow")
}

fn snapshot(store: &HistoryStore, id: &str, cursor: Option<ActiveCursor>) -> ActiveSnapshot {
    store
        .active_snapshot(id, cursor)
        .expect("active snapshot read must not fail")
        .expect("flow is still active")
}

/// Single place that encodes the cursor field contract; adjust here if T1 renames fields.
fn cursor_of(snapshot: &ActiveSnapshot) -> ActiveCursor {
    ActiveCursor {
        client_sequence: snapshot.client_sequence,
        server_sequence: snapshot.server_sequence,
        client_head_dropped: snapshot.client_head_dropped,
        server_head_dropped: snapshot.server_head_dropped,
    }
}

fn direction_bytes(snapshot: &ActiveSnapshot, direction: u8) -> Vec<u8> {
    let mut out = Vec::new();
    for segment in &snapshot.segments {
        if segment.direction == direction {
            out.extend_from_slice(&segment.data);
        }
    }
    out
}

fn direction_sequences(snapshot: &ActiveSnapshot, direction: u8) -> Vec<u64> {
    snapshot
        .segments
        .iter()
        .filter(|segment| segment.direction == direction)
        .map(|segment| segment.sequence)
        .collect()
}

#[tokio::test]
async fn tcp_snapshot_keeps_a_bounded_exact_tail_per_direction() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, &vec![b'A'; 30_000]);
    writer.record(Direction::ServerToClient, &vec![b'S'; 20_000]);
    writer.record(Direction::ClientToServer, &vec![b'B'; 30_000]);

    let initial = snapshot(&store, &id, None);
    assert!(initial.reset, "cursor-less read must be a full replacement");
    assert_eq!(initial.flow_id, id);
    assert_eq!(initial.service_id, "svc");
    assert_eq!(initial.protocol_id, "raw");
    assert!(initial.opened_at_ms > 0);
    assert_eq!(initial.retained_client_to_server_bytes, 60_000);
    assert_eq!(initial.retained_server_to_client_bytes, 20_000);
    assert!(!initial.truncated_client_to_server);
    assert!(!initial.truncated_server_to_client);
    let expected_client = [vec![b'A'; 30_000], vec![b'B'; 30_000]].concat();
    let expected_server = vec![b'S'; 20_000];
    assert_eq!(
        direction_bytes(&initial, 1).as_slice(),
        expected_client.as_slice()
    );
    assert_eq!(
        direction_bytes(&initial, 2).as_slice(),
        expected_server.as_slice()
    );

    // Push the client tail past 64 KiB: the oldest chunk is drained from its head, never spliced.
    writer.record(Direction::ClientToServer, &vec![b'C'; 10_000]);
    let bounded = snapshot(&store, &id, None);
    assert_eq!(
        bounded.retained_client_to_server_bytes,
        MAX_DIRECTION_BYTES as u64
    );
    assert_eq!(bounded.retained_server_to_client_bytes, 20_000);
    assert!(bounded.truncated_client_to_server);
    assert!(!bounded.truncated_server_to_client);
    let client = direction_bytes(&bounded, 1);
    assert_eq!(client.len(), MAX_DIRECTION_BYTES);
    let drained_head = vec![b'A'; 30_000 - 4_464];
    let expected_tail = [vec![b'B'; 30_000], vec![b'C'; 10_000]].concat();
    assert_eq!(&client[..drained_head.len()], &drained_head[..]);
    assert_eq!(&client[drained_head.len()..], &expected_tail[..]);
    assert_eq!(
        direction_bytes(&bounded, 2).as_slice(),
        expected_server.as_slice()
    );
    assert_eq!(bounded.client_head_dropped, 4_464);
}

#[tokio::test]
async fn unchanged_cursor_returns_reset_false_and_an_empty_delta() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, b"hello");

    let initial = snapshot(&store, &id, None);
    assert!(initial.reset);
    assert_eq!(initial.segments.len(), 1);

    let delta = snapshot(&store, &id, Some(cursor_of(&initial)));
    assert!(!delta.reset, "matching cursor must append, not replace");
    assert!(delta.segments.is_empty(), "no new bytes, no new segments");
    assert_eq!(delta.client_sequence, initial.client_sequence);
    assert_eq!(delta.server_sequence, initial.server_sequence);
    assert_eq!(delta.client_head_dropped, 0);
    assert_eq!(delta.server_head_dropped, 0);
    assert_eq!(delta.retained_client_to_server_bytes, 5);
}

#[tokio::test]
async fn appended_chunks_are_returned_once_without_duplication() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, b"hello");

    let first = snapshot(&store, &id, None);

    writer.record(Direction::ClientToServer, b" world");
    writer.record(Direction::ServerToClient, b"reply");
    let second = snapshot(&store, &id, Some(cursor_of(&first)));
    assert!(!second.reset);
    assert_eq!(direction_bytes(&second, 1), b" world".to_vec());
    assert_eq!(direction_bytes(&second, 2), b"reply".to_vec());
    assert_eq!(
        direction_sequences(&second, 1),
        vec![second.client_sequence]
    );

    writer.record(Direction::ClientToServer, b"!");
    let third = snapshot(&store, &id, Some(cursor_of(&second)));
    assert!(!third.reset);
    assert_eq!(direction_bytes(&third, 1), b"!".to_vec());
    assert!(
        direction_bytes(&third, 2).is_empty(),
        "server delta must not repeat"
    );

    let mut whole_client = direction_bytes(&first, 1);
    whole_client.extend_from_slice(&direction_bytes(&second, 1));
    whole_client.extend_from_slice(&direction_bytes(&third, 1));
    assert_eq!(whole_client, b"hello world!");
    assert_eq!(third.retained_client_to_server_bytes, 12);
}

#[tokio::test]
async fn partial_head_drain_advances_dropped_and_forces_reset() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, &vec![b'x'; MAX_DIRECTION_BYTES]);
    let first = snapshot(&store, &id, None);
    assert_eq!(first.client_head_dropped, 0);
    assert_eq!(first.segments.len(), 1);

    writer.record(Direction::ClientToServer, b"tail");
    let after = snapshot(&store, &id, Some(cursor_of(&first)));
    assert!(after.reset, "head drain invalidates the caller's cursor");
    assert_eq!(after.client_head_dropped, 4);
    assert_eq!(
        after.retained_client_to_server_bytes,
        MAX_DIRECTION_BYTES as u64
    );
    assert!(after.truncated_client_to_server);
    assert_eq!(after.segments.len(), 2);
    assert_eq!(after.segments[0].sequence, 1);
    assert_eq!(after.segments[0].data.len(), MAX_DIRECTION_BYTES - 4);
    let client = direction_bytes(&after, 1);
    assert_eq!(client.len(), MAX_DIRECTION_BYTES);
    assert!(client.ends_with(b"tail"));
    assert!(
        client[..MAX_DIRECTION_BYTES - 4]
            .iter()
            .all(|byte| *byte == b'x')
    );

    // A cursor taken from the reset snapshot is stable again.
    let stable = snapshot(&store, &id, Some(cursor_of(&after)));
    assert!(!stable.reset);
    assert!(stable.segments.is_empty());
}

#[tokio::test]
async fn whole_chunk_eviction_at_datagram_cap_advances_dropped_and_resets() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    for _ in 0..MAX_DATAGRAMS {
        writer.record(Direction::ClientToServer, b"a");
    }
    let first = snapshot(&store, &id, None);
    assert_eq!(first.segments.len(), MAX_DATAGRAMS);
    assert_eq!(first.client_head_dropped, 0);

    for _ in 0..3 {
        writer.record(Direction::ClientToServer, b"b");
    }
    let after = snapshot(&store, &id, Some(cursor_of(&first)));
    assert!(after.reset);
    assert_eq!(
        after.client_head_dropped, 3,
        "each whole evicted byte is counted"
    );
    assert_eq!(after.segments.len(), MAX_DATAGRAMS);
    assert_eq!(after.retained_client_to_server_bytes, MAX_DATAGRAMS as u64);
    assert_eq!(direction_sequences(&after, 1).first().copied(), Some(4));
    assert_eq!(after.client_sequence, MAX_DATAGRAMS as u64 + 3);
    assert!(after.truncated_client_to_server);
}

#[tokio::test]
async fn invalid_and_unknown_flow_ids_return_none() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let long = "a".repeat(129);
    for flow_id in ["", "not-a-uuid", long.as_str(), "svc"] {
        assert!(
            store
                .active_snapshot(flow_id, None)
                .expect("invalid ids are a clean miss, not an error")
                .is_none(),
            "expected None for {flow_id:?}"
        );
    }
    assert!(
        store
            .active_snapshot(&uuid::Uuid::new_v4().to_string(), None)
            .unwrap()
            .is_none(),
        "unknown but well-formed ids are not active"
    );
}

#[tokio::test]
async fn finish_transitions_snapshot_to_inactive_while_legacy_get_stays_active() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, b"live");

    assert!(
        matches!(store.get(&id).await, Err(HistoryReadError::Active)),
        "legacy history read must still refuse an active flow"
    );
    assert!(store.active_snapshot(&id, None).unwrap().is_some());

    assert!(!writer.finish(now_ms(), 4, 0));
    assert!(
        store.active_snapshot(&id, None).unwrap().is_none(),
        "closed flow must report inactive instead of a stale tail"
    );

    let mut saved = None;
    for _ in 0..100 {
        if let Some(history) = store.get(&id).await.unwrap() {
            saved = Some(history);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let saved = saved.expect("finished history must persist through the legacy path");
    assert_eq!(saved.client_to_server, b"live".to_vec());
    assert_eq!(saved.bytes_client_to_server, 4);
}

#[tokio::test]
async fn udp_snapshot_evicts_whole_datagrams_and_bounds_each_direction() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    let mut udp = service();
    udp.transport = Transport::Udp;
    store.apply_services(&[udp]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin_udp(&store, &id);
    for byte in [1, 2, 3] {
        writer.record(Direction::ClientToServer, &vec![byte; 32_768]);
    }

    let first = snapshot(&store, &id, None);
    assert!(first.reset);
    assert_eq!(first.retained_client_to_server_bytes, 2 * 32_768);
    assert_eq!(first.retained_server_to_client_bytes, 0);
    assert!(first.truncated_client_to_server);
    assert!(!first.truncated_server_to_client);
    assert_eq!(
        first.client_head_dropped, 32_768,
        "UDP evicts whole datagrams only"
    );
    assert_eq!(first.segments.len(), 2);
    assert_eq!(first.segments[0].sequence, 2);
    assert_eq!(first.segments[0].data, vec![2; 32_768]);
    assert_eq!(first.segments[1].sequence, 3);
    assert_eq!(first.segments[1].data, vec![3; 32_768]);
    assert_eq!(
        direction_bytes(&first, 1),
        [vec![2; 32_768], vec![3; 32_768]].concat()
    );

    let unchanged = snapshot(&store, &id, Some(cursor_of(&first)));
    assert!(!unchanged.reset);
    assert!(unchanged.segments.is_empty());

    writer.record(Direction::ServerToClient, &[9]);
    let appended = snapshot(&store, &id, Some(cursor_of(&unchanged)));
    assert!(!appended.reset);
    assert_eq!(appended.segments.len(), 1);
    assert_eq!(appended.segments[0].direction, 2);
    assert_eq!(appended.segments[0].data, vec![9]);
    assert_eq!(appended.client_sequence, first.client_sequence);
    assert_eq!(appended.server_sequence, 4);
}

#[tokio::test]
async fn recording_and_snapshotting_never_persist_active_history() {
    let dir = Directory::new();
    let store = HistoryStore::open(&dir.0).await.unwrap();
    store.apply_services(&[service()]);
    let id = uuid::Uuid::new_v4().to_string();
    let mut writer = begin(&store, &id);
    writer.record(Direction::ClientToServer, b"live-only");

    let live = snapshot(&store, &id, None);
    assert_eq!(direction_bytes(&live, 1), b"live-only".to_vec());

    let (payload, sidecar) = paths(&dir.0, &id);
    assert!(
        !payload.exists(),
        "fast-path capture must not write the payload"
    );
    assert!(
        !sidecar.exists(),
        "fast-path capture must not write the sidecar"
    );
    assert!(
        matches!(store.get(&id).await, Err(HistoryReadError::Active)),
        "active-only bytes are not readable through the legacy history path"
    );
    assert!(!payload.exists() && !sidecar.exists());
}
