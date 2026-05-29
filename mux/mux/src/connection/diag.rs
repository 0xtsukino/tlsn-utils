//! Diagnostic instrumentation for the connection driver.
//!
//! All metrics are atomic counters / timestamps with no synchronisation cost
//! in hot paths beyond `fetch_add` / `store(Relaxed)`. The watchdog reads
//! them without taking the runtime through tokio, so they remain accurate
//! even when the tokio runtime is starved.
//!
//! See `Handle::diag_snapshot` for the consumer-facing API.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use crate::frame::header::StreamId;

/// Per-connection-driver diagnostic counters. Shared between [`super::active::Active`]
/// (which writes them in its `poll` loop) and [`super::Handle`] (which reads
/// them in [`super::Handle::diag_snapshot`]).
#[derive(Debug)]
pub struct Diag {
    /// Monotonic instant captured at construction. All timestamps in this
    /// struct are millis-since-this.
    start: Instant,
    /// Monotonic ms-since-start at which `Active::poll`'s loop most
    /// recently executed a body iteration. Updated unconditionally on
    /// every iteration, before any inner branches are polled.
    pub(crate) last_poll_at_ms: AtomicI64,
    /// Number of `Active::poll` loop iterations executed.
    pub(crate) poll_iterations: AtomicU64,
    /// Number of frames `Active::poll` has popped from `stream_receivers`
    /// (i.e. accepted from `Stream::poll_write`).
    pub(crate) frames_popped: AtomicU64,
    /// Number of frames `Active::poll` has sent to the underlying wire.
    pub(crate) frames_sent: AtomicU64,
    /// Number of frames `Active::poll` has decoded from the wire and
    /// dispatched into per-stream buffers via `on_data`.
    pub(crate) frames_dispatched: AtomicU64,
}

impl Diag {
    pub(crate) fn new() -> Self {
        Diag {
            start: Instant::now(),
            last_poll_at_ms: AtomicI64::new(0),
            poll_iterations: AtomicU64::new(0),
            frames_popped: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            frames_dispatched: AtomicU64::new(0),
        }
    }

    pub(crate) fn now_ms(&self) -> i64 {
        self.start.elapsed().as_millis() as i64
    }

    pub(crate) fn record_poll(&self) {
        let now = self.now_ms();
        self.last_poll_at_ms.store(now, Ordering::Relaxed);
        self.poll_iterations.fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot of one stream's diagnostic state.
#[derive(Debug, Clone)]
pub struct StreamDiag {
    pub stream_id: StreamId,
    /// SendFrames pushed by `Stream::poll_write` but not yet popped by
    /// `Active::poll`. Non-zero at watchdog time means data is stranded
    /// in the local channel (= write-side wake-loss).
    pub outbound_pending: usize,
    /// Bytes currently sitting in the stream's inbound buffer, waiting
    /// for a consumer's `poll_read`. Non-zero at watchdog time with no
    /// progress means a read-side wake-loss.
    pub inbound_buffer_bytes: usize,
    /// Whether a `Waker` is currently registered on the read side
    /// (consumer is parked waiting for more data).
    pub has_reader_waker: bool,
    /// Same for write side (writer parked waiting for `send_window`
    /// credit).
    pub has_writer_waker: bool,
}

/// Snapshot of the whole connection driver's diagnostic state.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Wall-clock ms since this driver started.
    pub now_ms: i64,
    /// Ms since `Active::poll`'s loop last ran a body iteration.
    pub idle_ms: i64,
    /// Cumulative loop iteration count.
    pub poll_iterations: u64,
    /// Cumulative frames popped from `stream_receivers`.
    pub frames_popped: u64,
    /// Cumulative frames sent to the wire.
    pub frames_sent: u64,
    /// Cumulative frames dispatched from the wire into stream buffers.
    pub frames_dispatched: u64,
    /// Per-stream snapshot.
    pub streams: Vec<StreamDiag>,
}
