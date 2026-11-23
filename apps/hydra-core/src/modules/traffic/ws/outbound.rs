//! Bounded outbound queue with explicit loss reporting.
//!
//! A frame that is too large, or that cannot be queued because the socket is
//! slow, is never silently discarded: the socket emits a `loss` frame (so the
//! client resyncs) or is marked fatal and closed 1013 by the socket loop.
use axum::extract::ws::Message;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::mpsc;

/// One outbound frame cap; history/search pages carry bytes.
pub(super) const MAX_FRAME: usize = 1024 * 1024;
/// Per-socket queued bytes cap.
pub(super) const MAX_QUEUED: usize = 4 * 1024 * 1024;
pub(super) const QUEUE_FRAMES: usize = 64;
/// Serialized-size budget for one page frame, measured on the real envelope so
/// `request_id`/`generation`/`query` overhead is included. Kept below
/// `MAX_FRAME` so a page never turns into a `loss` + resync.
pub(super) const PAGE_BUDGET: usize = MAX_FRAME - 16 * 1024;

/// A single page item does not fit the frame budget; the request must fail
/// with a typed error instead of a silent truncation.
#[derive(Clone, Copy, Debug)]
pub(super) struct PageTooLarge;

/// Fit `items` into one page frame under `PAGE_BUDGET`.
///
/// `envelope` is the frame without `items`/`next_cursor`; its serialized size
/// (with an empty item list and a null cursor) is the overhead baseline. When
/// only a prefix fits, `next_cursor` becomes the last kept item's global `seq`
/// so the remaining rows stay reachable; an item without a cursor is reported
/// as `PageTooLarge` rather than silently skipped. An empty input is a genuine
/// empty page, never a trimmed one.
pub(super) fn fit_page(
    envelope: &Value,
    items: Vec<Value>,
    next_cursor: Option<i64>,
    seq: impl Fn(&Value) -> Option<i64>,
) -> Result<(Vec<Value>, Option<i64>), PageTooLarge> {
    let base = {
        let mut probe = envelope.clone();
        if let Some(object) = probe.as_object_mut() {
            object.insert("items".into(), Value::Array(Vec::new()));
            object.insert("next_cursor".into(), Value::Null);
        }
        serde_json::to_string(&probe).map_or(usize::MAX, |text| text.len())
    };
    let mut used = base;
    let mut kept = 0usize;
    for item in &items {
        let size = serde_json::to_string(item).map_or(usize::MAX, |text| text.len());
        let cost = size.saturating_add(1);
        if used.saturating_add(cost) > PAGE_BUDGET {
            break;
        }
        used = used.saturating_add(cost);
        kept += 1;
    }
    if kept == items.len() {
        return Ok((items, next_cursor));
    }
    if kept == 0 {
        return Err(PageTooLarge);
    }
    match seq(&items[kept - 1]) {
        Some(cursor) => Ok((items.into_iter().take(kept).collect(), Some(cursor))),
        None => Err(PageTooLarge),
    }
}

/// Emit one byte-bounded page frame. A single item that cannot fit becomes a
/// typed `frame_too_large` error for this request, never a `loss` + resync and
/// never a fabricated empty page.
pub(super) fn emit_page(
    out: &Outbound,
    generation: u64,
    mut envelope: Value,
    items: Vec<Value>,
    next_cursor: Option<i64>,
    seq: impl Fn(&Value) -> Option<i64>,
) {
    match fit_page(&envelope, items, next_cursor, seq) {
        Ok((items, next_cursor)) => {
            if let Some(object) = envelope.as_object_mut() {
                object.insert("items".into(), Value::Array(items));
                object.insert("next_cursor".into(), json!(next_cursor));
            }
            out.emit(generation, envelope);
        }
        Err(PageTooLarge) => {
            let request_id = envelope
                .get("request_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            error_frame(out, generation, &request_id, "frame_too_large", false);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SendError {
    /// Serialized frame exceeds `MAX_FRAME`.
    TooLarge,
    /// The socket queue is full or the receiver is gone.
    Full,
}

pub(super) struct Outbound {
    tx: mpsc::Sender<(Message, usize)>,
    queued: Arc<AtomicUsize>,
    fatal: AtomicBool,
    dropped: AtomicU64,
}

impl Outbound {
    pub(super) fn new() -> (Self, mpsc::Receiver<(Message, usize)>) {
        let (tx, rx) = mpsc::channel::<(Message, usize)>(QUEUE_FRAMES);
        (
            Self {
                tx,
                queued: Arc::new(AtomicUsize::new(0)),
                fatal: AtomicBool::new(false),
                dropped: AtomicU64::new(0),
            },
            rx,
        )
    }

    /// Emit one frame. Oversize frames become an explicit `loss`; a full queue
    /// marks the socket fatal so the loop closes 1013 instead of pretending the
    /// frame was delivered.
    pub(super) fn emit(&self, generation: u64, value: Value) {
        match self.send(value) {
            Ok(()) => {}
            Err(SendError::TooLarge) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                let loss = json!({
                    "type": "loss",
                    "generation": generation,
                    "dropped": 1,
                    "total_dropped": dropped,
                    "reason": "frame_too_large",
                });
                if self.send(loss).is_err() {
                    self.fatal.store(true, Ordering::Relaxed);
                }
            }
            Err(SendError::Full) => self.fatal.store(true, Ordering::Relaxed),
        }
    }

    pub(super) fn send(&self, value: Value) -> Result<(), SendError> {
        let text = serde_json::to_string(&value).map_err(|_| SendError::TooLarge)?;
        if text.len() > MAX_FRAME {
            return Err(SendError::TooLarge);
        }
        let bytes = text.len();
        self.raw(Message::Text(text.into()), bytes)
    }

    pub(super) fn raw(&self, message: Message, bytes: usize) -> Result<(), SendError> {
        let queued = self.queued.load(Ordering::Relaxed);
        if queued.saturating_add(bytes) > MAX_QUEUED {
            return Err(SendError::Full);
        }
        self.queued.fetch_add(bytes, Ordering::Relaxed);
        match self.tx.try_send((message, bytes)) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.queued.fetch_sub(bytes, Ordering::Relaxed);
                Err(SendError::Full)
            }
        }
    }

    /// Release the accounting for a frame that was written to the socket.
    pub(super) fn complete(&self, bytes: usize) {
        self.queued.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// True once a frame could not be queued; the loop must close 1013.
    pub(super) fn take_fatal(&self) -> bool {
        self.fatal.swap(false, Ordering::Relaxed)
    }
}

/// Error frame. Failures are surfaced through the same loss/fatal path as any
/// other frame, so an error is never lost silently.
pub(super) fn error_frame(
    out: &Outbound,
    generation: u64,
    request_id: &str,
    code: &str,
    retryable: bool,
) {
    let request_id = if request_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        request_id.to_owned()
    };
    out.emit(
        generation,
        json!({
            "type": "error",
            "request_id": request_id,
            "code": code,
            "message": "Streams request failed",
            "retryable": retryable,
        }),
    );
}

#[cfg(test)]
#[path = "outbound_tests.rs"]
mod tests;
