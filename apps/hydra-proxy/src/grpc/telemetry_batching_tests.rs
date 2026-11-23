//! Deterministic contract tests for `ProxyTelemetry::stream_telemetry`
//! (`apps/hydra-proxy/src/grpc/telemetry.rs`).
//!
//! Contract under test (proto `StreamTelemetryRequest.max_events_per_batch`,
//! `contracts/hydra/proxy/v1/proxy.proto:310-312`):
//!   * `> 128` is rejected with `INVALID_ARGUMENT` before any subscriber slot
//!     is taken (`telemetry.rs:15-17`, `:19-23`).
//!   * `1` keeps the one-event-per-batch behaviour.
//!   * `0` (proto3 default; Core never sends it) keeps today's behaviour:
//!     accepted, one event per batch, no loss.
//!   * A queued backlog must be coalesced into batches of at most the
//!     requested cap, with event order and `event_id`s preserved.
//!   * Batches are also bounded by encoded size (the documented 256 KiB cap),
//!     never by count alone: a byte-full batch carries the next event into the
//!     following batch without dropping or reordering it.
//!   * An event whose `encoded_len()` exceeds Core's 49,152-byte per-event
//!     budget travels alone, so one oversized message cannot poison its
//!     healthy neighbours.
//!   * Subscriber lag survives as a `Payload::Loss{reason:"subscriber_lag"}`
//!     marker and is counted in `Telemetry::dropped`, including when the lag
//!     is observed at a batch boundary.
//!   * Shutdown cancels the stream without dropping the connection into a
//!     panic.
//!
//! Determinism premise (no sleeps, no timing guesses):
//! `#[tokio::test]` uses the single-threaded runtime. `Telemetry::emit` is
//! synchronous and never awaits, so a burst of emits issued before the first
//! `next()` on the stream cannot be observed by the spawned sender task:
//! `subscribe()` happens in the handler before the task is spawned
//! (`telemetry.rs:24-27`), so the whole burst is sitting in the broadcast ring
//! (capacity 1024, `telemetry/mod.rs:52`) when the task is first polled.
//! Pre-fix, `events: vec![event]` (`telemetry.rs:51-55`) yields one event per
//! batch, so the backlog test fails on `max batch > 1`; post-fix it must
//! coalesce. Only bounded `tokio::time::timeout` guards are used, never a
//! sleep that a fix could win a race against.

use crate::grpc::Rpc;
use crate::grpc::telemetry::{CORE_EVENT_BUDGET, MAX_BATCH_BYTES};
use hydra_wire::v1::{self, event::Payload, proxy_telemetry_server::ProxyTelemetry};
use prost::Message;
use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_stream::StreamExt;
use tonic::{Request, Status};

type Stream = <Rpc as ProxyTelemetry>::StreamTelemetryStream;

const READ_TIMEOUT: Duration = Duration::from_secs(5);

struct Harness {
    rpc: Rpc,
    telemetry: Arc<crate::telemetry::Telemetry>,
    dir: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn harness() -> Harness {
    use hydra_domain::ServiceSnapshot;
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("hydra-batching-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let telemetry = crate::telemetry::Telemetry::new(16);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let manager = crate::transport::Manager::new(
        crate::config::Limits::default(),
        telemetry.clone(),
        shutdown.clone(),
    );
    let state = crate::state::State::start(
        dir.join("state"),
        ServiceSnapshot::default(),
        manager,
        telemetry.clone(),
    )
    .await
    .unwrap();
    let protocols = crate::inspection::protocols::Protocols::open(dir.join("plugins"))
        .await
        .unwrap();
    Harness {
        rpc: Rpc {
            diagnostics: Arc::new(tokio::sync::Semaphore::new(1)),
            protocols,
            state,
            shutdown,
        },
        telemetry,
        dir,
    }
}

/// Emits a decoded-message event, which is exactly the shape the built-in
/// `utf8` decoder produces per text chunk (`protocol_id: "utf8"`), without
/// requiring a live flow. `message_id` is the identity used for order checks.
fn emit_message(telemetry: &crate::telemetry::Telemetry, index: usize) {
    telemetry.emit(
        "svc",
        "flow",
        Payload::Message(v1::ParsedMessage {
            message_id: format!("m{index:05}"),
            protocol_id: "utf8".into(),
            message_type: "text".into(),
            direction: 2,
            fields: None,
            original_size: 4096,
            sample_id: String::new(),
            truncated: false,
        }),
    );
}

#[allow(clippy::result_large_err)] // tonic trait boundary uses Status by value.
async fn open(rpc: &Rpc, max_events_per_batch: u32) -> Result<Stream, Status> {
    let request = Request::new(v1::StreamTelemetryRequest {
        max_events_per_batch,
    });
    rpc.stream_telemetry(request)
        .await
        .map(tonic::Response::into_inner)
}

/// Drains until `want` decoded-message events have been observed (loss markers
/// do not count). Bounded by `READ_TIMEOUT` per frame, so a stream that stops
/// producing fails instead of hanging.
async fn drain_events(stream: &mut Stream, want: usize) -> Vec<v1::EventBatch> {
    let mut batches = Vec::new();
    let mut seen = 0usize;
    while seen < want {
        match tokio::time::timeout(READ_TIMEOUT, stream.next())
            .await
            .expect("timed out waiting for a telemetry batch")
        {
            Some(Ok(batch)) => {
                seen += batch
                    .events
                    .iter()
                    .filter(|event| matches!(event.payload, Some(Payload::Message(_))))
                    .count();
                batches.push(batch);
            }
            Some(Err(status)) => panic!("telemetry stream errored: {status}"),
            None => panic!("telemetry stream ended after {seen}/{want} events"),
        }
    }
    batches
}

fn message_ids(batch: &v1::EventBatch) -> Vec<String> {
    batch
        .events
        .iter()
        .filter_map(|event| match &event.payload {
            Some(Payload::Message(message)) => Some(message.message_id.clone()),
            _ => None,
        })
        .collect()
}

fn flat_message_ids(batches: &[v1::EventBatch]) -> Vec<String> {
    batches.iter().flat_map(message_ids).collect()
}

fn expected_ids(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("m{i:05}")).collect()
}

/// A decoded message carrying one `utf8.text` string field of `text_bytes`.
/// Used to construct events of a known encoded size without a live flow.
fn sized_payload(message_id: String, text_bytes: usize) -> Payload {
    let field = v1::FieldValue {
        value: Some(v1::field_value::Value::StringValue("x".repeat(text_bytes))),
    };
    let fields: std::collections::HashMap<String, v1::FieldValue> =
        std::iter::once(("utf8.text".to_string(), field)).collect();
    Payload::Message(v1::ParsedMessage {
        message_id,
        protocol_id: "utf8".into(),
        message_type: "text".into(),
        direction: 2,
        fields: Some(v1::FieldObject { fields }),
        original_size: text_bytes.min(u32::MAX as usize) as u32,
        sample_id: String::new(),
        truncated: false,
    })
}

fn emit_sized(telemetry: &crate::telemetry::Telemetry, message_id: String, text_bytes: usize) {
    telemetry.emit("svc", "flow", sized_payload(message_id, text_bytes));
}

/// Encoded size of the event the proxy will actually send (uuid `event_id`,
/// timestamp and service/flow ids included), so byte-cap expectations are
/// derived from real `prost::Message::encoded_len()` values, not guesses.
fn emitted_event_len(payload: &Payload) -> usize {
    v1::Event {
        event_id: "00000000-0000-0000-0000-000000000000".into(),
        occurred_at: Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        }),
        service_id: "svc".into(),
        flow_id: "flow".into(),
        payload: Some(payload.clone()),
    }
    .encoded_len()
}

/// Acceptance bound for one batch: a multi-event batch must respect the byte
/// cap. A single-event batch is only allowed past the cap when that lone event
/// itself needs the room (oversized isolation).
fn assert_batch_byte_cap(batches: &[v1::EventBatch], context: &str) {
    for (index, batch) in batches.iter().enumerate() {
        let encoded = batch.encoded_len();
        assert!(
            encoded <= MAX_BATCH_BYTES || batch.events.len() == 1,
            "{context}: batch {index} encodes {encoded} bytes with {} events, \
             above the documented {MAX_BATCH_BYTES}-byte batch cap",
            batch.events.len()
        );
    }
}

#[tokio::test]
async fn queued_backlog_is_coalesced_within_requested_cap_and_keeps_order() {
    const QUEUED: usize = 200;
    const CAP: u32 = 64;
    let h = harness().await;
    let mut stream = open(&h.rpc, CAP).await.unwrap();
    assert_eq!(h.telemetry.events.receiver_count(), 1);

    // No await between emits: the sender task cannot run, so all 200 events
    // are queued in the broadcast ring when it is first polled.
    for index in 0..QUEUED {
        emit_message(&h.telemetry, index);
    }

    let batches = drain_events(&mut stream, QUEUED).await;
    let ids = flat_message_ids(&batches);

    assert_eq!(
        ids,
        expected_ids(0..QUEUED),
        "event order or event identity changed while batching"
    );
    assert_eq!(
        h.telemetry.dropped.load(Ordering::Relaxed),
        0,
        "a 200-event backlog must not be reported as loss"
    );
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.events.len())
            .sum::<usize>(),
        QUEUED,
        "batched delivery must not duplicate or drop events"
    );

    let largest = batches
        .iter()
        .map(|batch| batch.events.len())
        .max()
        .unwrap_or(0);
    assert!(
        largest > 1,
        "queued events were not combined: {QUEUED} events arrived in {} batches \
         (largest {largest}); stream_telemetry still sends one event per batch",
        batches.len()
    );
    assert!(
        largest <= CAP as usize,
        "batch of {largest} events exceeds requested max_events_per_batch={CAP}"
    );
    assert!(
        batches.len() <= 8,
        "a {QUEUED}-event backlog at cap {CAP} needed {} batches; the sender is \
         not draining the queue into the requested cap",
        batches.len()
    );
    for batch in &batches {
        assert_eq!(batch.proxy_epoch_id, h.telemetry.epoch);
        assert!(!batch.critical);
        assert!(!batch.events.is_empty(), "empty batch emitted");
    }

    let mut event_ids: Vec<&str> = batches
        .iter()
        .flat_map(|batch| batch.events.iter())
        .map(|event| event.event_id.as_str())
        .collect();
    let total = event_ids.len();
    event_ids.sort_unstable();
    event_ids.dedup();
    assert_eq!(
        event_ids.len(),
        total,
        "duplicate event_id in batched stream"
    );
}

#[tokio::test]
async fn cap_of_one_keeps_single_event_batches() {
    const EVENTS: usize = 40;
    let h = harness().await;
    let mut stream = open(&h.rpc, 1).await.unwrap();

    for index in 0..EVENTS {
        emit_message(&h.telemetry, index);
    }

    let batches = drain_events(&mut stream, EVENTS).await;
    assert_eq!(batches.len(), EVENTS, "cap 1 must not coalesce");
    for batch in &batches {
        assert_eq!(batch.events.len(), 1, "cap 1 emitted a multi-event batch");
    }
    assert_eq!(flat_message_ids(&batches), expected_ids(0..EVENTS));
    assert_eq!(h.telemetry.dropped.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn zero_cap_is_accepted_and_preserves_current_single_event_batches() {
    // Proto3 default. The only rejected value is `> 128`, so 0 must be
    // accepted, and today it behaves like 1 (the value is unused). Keeping
    // that is the compatibility baseline: Core sends 64, nobody sends 0, so
    // 0 must not silently change delivery shape or lose events.
    const EVENTS: usize = 20;
    let h = harness().await;
    let mut stream = open(&h.rpc, 0).await.unwrap();

    for index in 0..EVENTS {
        emit_message(&h.telemetry, index);
    }

    let batches = drain_events(&mut stream, EVENTS).await;
    assert_eq!(
        flat_message_ids(&batches),
        expected_ids(0..EVENTS),
        "cap 0 lost or reordered events"
    );
    assert_eq!(
        batches.len(),
        EVENTS,
        "cap 0 delivery shape changed from one event per batch"
    );
    assert!(batches.iter().all(|batch| batch.events.len() == 1));
    assert_eq!(h.telemetry.dropped.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn batch_cap_above_128_is_rejected_without_taking_a_subscriber_slot() {
    let h = harness().await;
    for rejected in [129u32, 4096, u32::MAX] {
        let error = open(&h.rpc, rejected)
            .await
            .expect_err("max_events_per_batch above 128 must be rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "{rejected}");
        assert!(
            error.message().contains("128"),
            "rejection should name the cap: {error}"
        );
        assert_eq!(
            h.telemetry.subscribers.available_permits(),
            4,
            "a rejected request leaked a telemetry subscriber permit"
        );
    }
    // The boundary value itself is still accepted (cap is `> 128`).
    let stream = open(&h.rpc, 128).await.expect("128 is within the cap");
    drop(stream);
    // The permit lives in the spawned sender task and is released only when
    // that task next polls `sender.closed()`. Dropping the response closes the
    // mpsc receiver synchronously, so give the task bounded scheduler turns
    // instead of reading the semaphore before it can run (no sleeps).
    for _ in 0..256 {
        if h.telemetry.subscribers.available_permits() == 4 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        h.telemetry.subscribers.available_permits(),
        4,
        "subscriber permit was not released after the stream was dropped"
    );
}

#[tokio::test]
async fn subscriber_lag_survives_as_loss_marker_and_is_counted() {
    const BURST: usize = 1500;
    // broadcast::channel(1024) => the ring keeps the newest 1024 and the
    // subscriber is told exactly how many were overwritten.
    const RING: usize = 1024;
    const LOST: usize = BURST - RING;
    let h = harness().await;
    let mut stream = open(&h.rpc, 64).await.unwrap();

    for index in 0..BURST {
        emit_message(&h.telemetry, index);
    }

    let batches = drain_events(&mut stream, RING).await;
    let losses: Vec<&v1::EventLoss> = batches
        .iter()
        .flat_map(|batch| batch.events.iter())
        .filter_map(|event| match &event.payload {
            Some(Payload::Loss(loss)) => Some(loss),
            _ => None,
        })
        .collect();
    assert_eq!(losses.len(), 1, "lag must surface as exactly one marker");
    assert_eq!(losses[0].reason, "subscriber_lag");
    assert_eq!(losses[0].count as usize, LOST);
    assert_eq!(
        h.telemetry.dropped.load(Ordering::Relaxed) as usize,
        LOST,
        "dropped counter must match the lag marker"
    );
    assert_eq!(
        flat_message_ids(&batches),
        expected_ids(LOST..BURST),
        "post-lag events must still arrive complete and in order"
    );
    let largest = batches
        .iter()
        .map(|batch| batch.events.len())
        .max()
        .unwrap_or(0);
    assert!(
        largest <= 64,
        "batch of {largest} exceeds requested cap after a lag marker"
    );
}

#[tokio::test]
async fn shutdown_terminates_the_stream_without_losing_queued_prefix_order() {
    const EVENTS: usize = 5;
    let h = harness().await;
    let mut stream = open(&h.rpc, 8).await.unwrap();
    for index in 0..EVENTS {
        emit_message(&h.telemetry, index);
    }
    h.rpc.shutdown.cancel();

    let mut delivered = Vec::new();
    loop {
        match tokio::time::timeout(READ_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(batch))) => delivered.extend(message_ids(&batch)),
            Ok(Some(Err(status))) => panic!("stream errored during shutdown: {status}"),
            Ok(None) => break,
            Err(_) => panic!("stream did not terminate after shutdown"),
        }
    }
    let expected = expected_ids(0..EVENTS);
    assert_eq!(
        delivered,
        expected[..delivered.len()].to_vec(),
        "events delivered around shutdown must be an ordered prefix"
    );
}

/// Count alone is not a sufficient bound: a queued backlog of events whose
/// combined encoded size exceeds 256 KiB must split on the byte cap, keep FIFO
/// order, and still fill batches close to the cap.
#[tokio::test]
async fn byte_full_batches_carry_the_next_event_without_loss_or_reorder() {
    const CAP: u32 = 64;
    const FIELD_BYTES: usize = 20_000;
    const EVENTS: usize = 30;
    let h = harness().await;
    let mut stream = open(&h.rpc, CAP).await.unwrap();

    let event_bytes = emitted_event_len(&sized_payload(String::new(), FIELD_BYTES));
    assert!(
        event_bytes < CORE_EVENT_BUDGET,
        "fixture event of {event_bytes} bytes must stay below the solo threshold"
    );
    assert!(
        EVENTS * event_bytes > 2 * MAX_BATCH_BYTES,
        "fixture needs more than two byte-capped batches: {EVENTS} x {event_bytes} bytes"
    );
    assert!(
        event_bytes > MAX_BATCH_BYTES / 32,
        "fixture event of {event_bytes} bytes is too small to exercise the byte cap"
    );

    for index in 0..EVENTS {
        emit_sized(&h.telemetry, format!("m{index:05}"), FIELD_BYTES);
    }
    let batches = drain_events(&mut stream, EVENTS).await;

    assert_eq!(
        flat_message_ids(&batches),
        expected_ids(0..EVENTS),
        "byte-capped batching lost or reordered events"
    );
    assert_eq!(
        h.telemetry.dropped.load(Ordering::Relaxed),
        0,
        "byte-cap splitting must carry, not drop"
    );
    assert!(
        batches.iter().any(|batch| batch.events.len() > 1),
        "events were not coalesced at all"
    );
    for batch in &batches {
        assert!(
            batch.events.len() <= CAP as usize,
            "batch of {} events exceeds requested cap {CAP}",
            batch.events.len()
        );
    }
    assert_batch_byte_cap(&batches, "byte-cap splitting");

    let fullest = batches
        .iter()
        .map(|batch| batch.encoded_len())
        .max()
        .unwrap_or(0);
    assert!(
        fullest > MAX_BATCH_BYTES - 2 * event_bytes,
        "largest batch encodes only {fullest} bytes; the sender is splitting well \
         below the documented {MAX_BATCH_BYTES}-byte cap"
    );
}

/// Core converts a batch all-or-nothing, so an event above its 49,152-byte
/// per-event budget must be the only event of its batch; healthy events on
/// both sides still coalesce.
#[tokio::test]
async fn oversized_event_travels_alone_and_healthy_events_still_batch() {
    const CAP: u32 = 64;
    const SMALL_FIELD: usize = 100;
    const BIG_FIELD: usize = 50_000;
    let h = harness().await;
    let mut stream = open(&h.rpc, CAP).await.unwrap();

    let big_len = emitted_event_len(&sized_payload("big".into(), BIG_FIELD));
    let small_len = emitted_event_len(&sized_payload("s0".into(), SMALL_FIELD));
    assert!(
        big_len > CORE_EVENT_BUDGET,
        "fixture oversized event is only {big_len} bytes"
    );
    assert!(small_len <= CORE_EVENT_BUDGET);

    for index in 0..3usize {
        emit_sized(&h.telemetry, format!("s{index}"), SMALL_FIELD);
    }
    emit_sized(&h.telemetry, "big".into(), BIG_FIELD);
    for index in 3..6usize {
        emit_sized(&h.telemetry, format!("s{index}"), SMALL_FIELD);
    }

    let batches = drain_events(&mut stream, 7).await;
    let mut expected: Vec<String> = (0..3).map(|i| format!("s{i}")).collect();
    expected.push("big".into());
    expected.extend((3..6).map(|i| format!("s{i}")));
    assert_eq!(
        flat_message_ids(&batches),
        expected,
        "oversized-event handling lost or reordered events"
    );

    let carrying: Vec<&v1::EventBatch> = batches
        .iter()
        .filter(|batch| message_ids(batch).iter().any(|id| id == "big"))
        .collect();
    assert_eq!(
        carrying.len(),
        1,
        "oversized message id appeared in {} batches",
        carrying.len()
    );
    assert_eq!(
        carrying[0].events.len(),
        1,
        "event encoded at {big_len} bytes shared a batch with healthy neighbours"
    );
    assert!(
        batches.iter().any(|batch| {
            batch.events.len() > 1 && !message_ids(batch).iter().any(|id| id == "big")
        }),
        "healthy events around the oversized one were not coalesced"
    );
    assert_batch_byte_cap(&batches, "oversized isolation");
    assert_eq!(h.telemetry.dropped.load(Ordering::Relaxed), 0);
}

/// Lag observed on the very first receive at the count boundary: with cap 1
/// the marker must be its own first batch, and it must stay global (no
/// service/flow attribution).
#[tokio::test]
async fn lag_marker_at_count_boundary_is_emitted_as_its_own_batch() {
    const BURST: usize = 1500;
    const RING: usize = 1024;
    const LOST: usize = BURST - RING;
    let h = harness().await;
    let mut stream = open(&h.rpc, 1).await.unwrap();
    for index in 0..BURST {
        emit_message(&h.telemetry, index);
    }

    let batches = drain_events(&mut stream, RING).await;
    assert!(!batches.is_empty(), "no batch arrived");
    let first = &batches[0];
    assert_eq!(
        first.events.len(),
        1,
        "cap 1 emitted a multi-event first batch"
    );
    match &first.events[0].payload {
        Some(Payload::Loss(loss)) => {
            assert_eq!(loss.reason, "subscriber_lag");
            assert_eq!(loss.count as usize, LOST);
        }
        _ => panic!("first batch is not the lag marker"),
    }
    assert!(
        first.events[0].flow_id.is_empty() && first.events[0].service_id.is_empty(),
        "lag marker must stay global, not be attributed to a flow/service"
    );

    let markers = batches
        .iter()
        .flat_map(|batch| batch.events.iter())
        .filter(|event| matches!(event.payload, Some(Payload::Loss(_))))
        .count();
    assert_eq!(markers, 1, "lag must surface as exactly one marker");
    assert_eq!(
        h.telemetry.dropped.load(Ordering::Relaxed) as usize,
        LOST,
        "dropped counter must match the lag marker"
    );
    assert_eq!(
        flat_message_ids(&batches),
        expected_ids(LOST..BURST),
        "retained events after a boundary lag must arrive complete and in order"
    );
    assert!(
        batches.iter().all(|batch| batch.events.len() == 1),
        "cap 1 must keep every batch at one event, marker included"
    );
}

/// Lag plus the byte cap: the marker joins the first batch, which fills to the
/// 256 KiB byte bound long before the 64-event count bound, so the count cap
/// cannot be what limits the batch.
#[tokio::test]
async fn lag_marker_shares_a_byte_capped_batch_and_count_cap_is_not_the_limiter() {
    const BURST: usize = 1200;
    const RING: usize = 1024;
    const LOST: usize = BURST - RING;
    const FIELD_BYTES: usize = 8_000;
    const CAP: u32 = 64;
    let h = harness().await;
    let mut stream = open(&h.rpc, CAP).await.unwrap();

    let event_bytes = emitted_event_len(&sized_payload(String::new(), FIELD_BYTES));
    assert!(
        event_bytes < CORE_EVENT_BUDGET,
        "fixture event of {event_bytes} bytes would trigger solo isolation"
    );

    for index in 0..BURST {
        emit_sized(&h.telemetry, format!("m{index:05}"), FIELD_BYTES);
    }
    let batches = drain_events(&mut stream, RING).await;

    match &batches[0].events[0].payload {
        Some(Payload::Loss(loss)) => {
            assert_eq!(loss.reason, "subscriber_lag");
            assert_eq!(loss.count as usize, LOST);
        }
        _ => panic!("first batch does not start with the lag marker"),
    }
    let markers = batches
        .iter()
        .flat_map(|batch| batch.events.iter())
        .filter(|event| matches!(event.payload, Some(Payload::Loss(_))))
        .count();
    assert_eq!(markers, 1, "lag must surface as exactly one marker");
    assert_eq!(h.telemetry.dropped.load(Ordering::Relaxed) as usize, LOST);
    assert_eq!(
        flat_message_ids(&batches),
        expected_ids(LOST..BURST),
        "events around a byte-capped lag batch were lost or reordered"
    );

    let largest = batches
        .iter()
        .map(|batch| batch.events.len())
        .max()
        .unwrap_or(0);
    assert!(
        largest > 1,
        "events were not coalesced after the lag marker"
    );
    assert!(
        largest <= CAP as usize,
        "batch of {largest} events exceeds requested cap {CAP}"
    );
    assert!(
        largest < CAP as usize,
        "batch reached the {CAP}-event count cap; the byte cap was not the limiter"
    );
    assert_batch_byte_cap(&batches, "lag + byte cap");

    let fullest = batches
        .iter()
        .map(|batch| batch.encoded_len())
        .max()
        .unwrap_or(0);
    assert!(
        fullest > MAX_BATCH_BYTES - 2 * event_bytes,
        "largest lag batch encodes only {fullest} bytes, well below the \
         {MAX_BATCH_BYTES}-byte cap"
    );
}
