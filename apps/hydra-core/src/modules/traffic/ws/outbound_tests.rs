//! Direct unit tests for the bounded outbound queue contract.
//!
//! Scope is deliberately narrow: the 64-frame channel cap, the independent
//! 4 MiB queued-byte cap, exact byte release through `complete`, and the
//! `TooLarge` vs `Full` classification. Nothing here claims anything about
//! OS socket buffering or the `1013` close path — those need a real socket and
//! are covered by the WS integration tests and `tests/e2e/streams_workspace.py`.
//!
//! The module is a child of `outbound`, so it uses the real constructor and
//! the same private `send`/`raw`/`complete`/`queued` items the socket loop
//! uses; no reimplementation of the accounting is asserted here.
use super::{MAX_FRAME, MAX_QUEUED, Outbound, QUEUE_FRAMES, SendError};
use axum::extract::ws::Message;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

/// A real `counters`-shaped frame with a controlled serialized size.
fn counters(pad: usize) -> Value {
    json!({"type": "counters", "pad": "x".repeat(pad)})
}

fn queued(out: &Outbound) -> usize {
    out.queued.load(Ordering::Relaxed)
}

/// Drain the channel exactly as the socket loop does and release each frame's
/// bytes through `complete`; returns the number of frames released.
fn drain_all(out: &Outbound, rx: &mut mpsc::Receiver<(Message, usize)>) -> usize {
    let mut frames = 0;
    while let Ok((_, bytes)) = rx.try_recv() {
        out.complete(bytes);
        frames += 1;
    }
    frames
}

#[test]
fn frame_cap_reports_full_at_queue_frames_and_releases_to_zero() {
    let (out, mut rx) = Outbound::new();
    let frame = counters(0);

    for accepted in 0..QUEUE_FRAMES {
        assert!(
            out.send(frame.clone()).is_ok(),
            "frame {accepted} of {QUEUE_FRAMES} must fit the frame queue"
        );
    }
    assert_eq!(
        out.send(frame.clone()),
        Err(SendError::Full),
        "frame {} must report Full, never a silent drop",
        QUEUE_FRAMES + 1
    );
    // `send` reports to its caller; only `emit` marks the socket fatal.
    assert!(!out.take_fatal());
    assert!(
        queued(&out) <= MAX_QUEUED,
        "small frames must stay under the byte cap"
    );

    assert_eq!(drain_all(&out, &mut rx), QUEUE_FRAMES);
    assert_eq!(
        queued(&out),
        0,
        "every accepted frame's bytes must be released exactly once"
    );

    // Draining re-opens exactly the capacity that was released.
    assert_eq!(out.send(frame), Ok(()));
    assert_eq!(drain_all(&out, &mut rx), 1);
    assert_eq!(queued(&out), 0);
}

#[test]
fn byte_cap_is_independent_of_the_frame_cap() {
    let (out, mut rx) = Outbound::new();
    // ~1 MB serialized: large enough that a few frames cross 4 MiB, small
    // enough to stay under MAX_FRAME so the rejection must be the byte cap.
    let frame = counters(1_000_000);
    let size = serde_json::to_string(&frame).unwrap().len();
    assert!(
        size < MAX_FRAME,
        "test frame must be under the per-frame cap"
    );
    assert!(
        MAX_QUEUED / size >= 2,
        "the test needs several frames to fit below the byte cap"
    );

    let mut accepted = 0usize;
    let mut bytes = 0usize;
    loop {
        match out.send(frame.clone()) {
            Ok(()) => {
                accepted += 1;
                bytes += size;
                assert!(
                    accepted < QUEUE_FRAMES,
                    "the byte cap must trip before the {QUEUE_FRAMES}-frame cap"
                );
            }
            Err(SendError::Full) => break,
            Err(SendError::TooLarge) => {
                panic!("a frame below MAX_FRAME must never be classified TooLarge")
            }
        }
    }

    assert!(
        accepted >= 2,
        "the byte cap needs more than one accepted frame"
    );
    assert!(
        accepted < QUEUE_FRAMES,
        "the byte cap must be enforced independently of the frame cap, accepted {accepted}"
    );
    assert!(bytes <= MAX_QUEUED);
    assert!(
        bytes + size > MAX_QUEUED,
        "the rejected frame is exactly the one that would exceed 4 MiB"
    );
    assert_eq!(
        queued(&out),
        bytes,
        "only accepted frames are accounted; the rejected frame adds nothing"
    );

    assert_eq!(drain_all(&out, &mut rx), accepted);
    assert_eq!(queued(&out), 0);
}

#[test]
fn oversize_frame_is_too_large_not_full() {
    let (out, mut rx) = Outbound::new();
    let oversize = counters(MAX_FRAME);

    assert_eq!(
        out.send(oversize.clone()),
        Err(SendError::TooLarge),
        "a frame over MAX_FRAME is a size error, not queue pressure"
    );
    assert_eq!(
        queued(&out),
        0,
        "an oversize frame is rejected before accounting"
    );

    // Classification is by size, not by queue pressure: with the frame queue
    // completely full, the oversize frame must still be TooLarge.
    for _ in 0..QUEUE_FRAMES {
        out.send(counters(0))
            .expect("the frame queue must accept QUEUE_FRAMES frames");
    }
    assert_eq!(
        out.send(oversize),
        Err(SendError::TooLarge),
        "an oversize frame stays TooLarge even when the queue is full"
    );
    assert_eq!(out.send(counters(0)), Err(SendError::Full));

    assert_eq!(drain_all(&out, &mut rx), QUEUE_FRAMES);
    assert_eq!(queued(&out), 0);
}

#[test]
fn complete_accounting_returns_to_zero_after_more_than_max_queued() {
    let (out, mut rx) = Outbound::new();
    let frame = counters(600);
    let size = serde_json::to_string(&frame).unwrap().len();
    // Enough drained events that the total emitted bytes exceed 4 MiB while no
    // single point in time is anywhere near the cap.
    let iterations = MAX_QUEUED / size + 64;

    let mut total = 0usize;
    for _ in 0..iterations {
        out.send(frame.clone())
            .expect("a queue that is drained every iteration must keep accepting");
        let (_, bytes) = rx.try_recv().expect("the accepted frame must be queued");
        assert_eq!(
            bytes, size,
            "the accounted size is the serialized frame size"
        );
        out.complete(bytes);
        assert_eq!(
            queued(&out),
            0,
            "each frame's bytes are released exactly once, with no leak or double release"
        );
        total += bytes;
    }

    assert!(
        total > MAX_QUEUED,
        "the run must exceed the 4 MiB cap in total, emitted {total}"
    );
    assert!(!out.take_fatal(), "draining never marks the socket fatal");
    assert!(rx.try_recv().is_err(), "no frame may be left behind");
}
