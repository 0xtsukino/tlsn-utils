# `Active::poll` wake-loss — residual rate with `fix/mux-deadlock` applied

This branch sits on top of [`fix/mux-deadlock`] and adds a diagnostic
classifier on top of the original reproducer. It demonstrates that the
fix substantially reduces but does not fully eliminate the wake-loss
under heavy parallel pressure.

## The bug, briefly

`tlsn_mux::connection::active::Active::poll` parks at
`Registration::poll_read_ready` while one or more `Stream::poll_write`
calls have queued `SendFrame` commands into `stream_receivers` that
`Active` never gets re-scheduled to drain. Buffered commands never reach
the wire; the peer's mux waits forever for bytes that are stranded
locally.

The dump-stack signature (both sides parked at `poll_read_ready`) looks
identical to a *stress-induced timeout* where the runtime is just slow
under heavy CPU oversubscription. The two are indistinguishable from the
dump alone — which is what the classifier added in this branch is for.

## Reproduce

```
cargo test --no-run -p test-harness --test wake_loss_repro
bash mux/test-harness/scripts/repro_wake_loss.sh
```

The script defaults to **N = 4 × nproc** parallel processes. On a 16-core
host that's N = 64. Each attempt has a 60 s in-test watchdog (using
`std::thread::sleep`, not `tokio::time::sleep` — so it fires reliably
even if the tokio runtime is starved) plus a 120 s outer `timeout`
backstop.

When the in-test watchdog fires it reads a `Handle::diag_snapshot()`
from both connection drivers (atomic-only, no tokio runtime needed) and
prints one of these classifications:

| Classification | What it means |
|---|---|
| `STRESS_FALSE_POSITIVE` | `Active::poll` polled `< 5 s` ago — the runtime is alive, the workload just didn't fit the 60 s budget under load. Not a bug. |
| `WAKE_LOSS_WRITE` | `Active::poll` idle `> 30 s` AND some stream has `outbound_pending > 0` — frames were pushed to the mpsc channel but `Active` was never re-scheduled to drain them. **The bug.** |
| `WAKE_LOSS_READ` | `Active::poll` idle `> 30 s` AND some stream has bytes buffered but unread. Read-side wake-loss. |
| `PROTOCOL_DEADLOCK` | `Active::poll` idle `> 30 s`, no work pending anywhere. Both sides genuinely waiting for the other to do something next. |
| `CPU_STARVED` | Watchdog `std::thread::sleep(60 s)` returned far late (`> 65 s`). Dump unreliable. |

## Observed rates on this branch (16-core x86_64 Linux, `fix/mux-deadlock` applied)

Aggregated over 10 batches × N=128 = **1280 attempts**:

| Outcome | Count | Rate |
|---|---|---|
| Healthy completion | 49 | 3.8 % |
| `STRESS_FALSE_POSITIVE` (workload too big for 60 s budget) | 962 | 75.2 % |
| **`WAKE_LOSS_WRITE`** (real bug recurrence) | **23** | **1.8 %** |
| `UNK` (reaped by outer timeout before watchdog completed) | 246 | 19.2 % |

So **the fix knocks the wake-loss rate down massively (vs pre-fix
~80-90 % at N=128) but ≈1.8 % of attempts still hit the bug.** The 1.8 %
is a lower bound — some of the 246 UNK attempts may have been real
wake-losses too but the runtime was so starved the watchdog itself
couldn't finish dumping in time.

A representative real `WAKE_LOSS_WRITE` dump:

```
=== HANG WATCHDOG FIRED after expected=60s, actual_wall=60.031s ===
  handle[0]: idle_ms=49465 polls=182255 popped=63533 sent=0 dispatched=0
             streams=190 outbound_pending=120 inbound_bytes=0
    sid=073ea566308f84e6 outbound_pending=11 inbound_bytes=0
                        reader_waker=true writer_waker=false
    sid=0c26e91f68c4531f outbound_pending=11 inbound_bytes=0
                        reader_waker=true writer_waker=false
    ...
  handle[1]: idle_ms=49579 polls=170567 popped=53929 sent=0 dispatched=0
             streams=190 outbound_pending=467 inbound_bytes=0
    ...
=== CLASSIFICATION: WAKE_LOSS_WRITE — Active is idle but stream has unprocessed outbound commands ===
    max_outbound_pending=11 max_inbound_bytes=0 min_idle_ms=49465 starved=false
```

Side 0 has 120 frames stranded across 25 streams; side 1 has 467 across
33; both `Active::poll`s have been idle for ~49 s while frames sit in
their `stream_receivers` channels. `reader_waker=true` on stranded
streams means consumers are parked waiting for inbound bytes that the
*other* side wants to send but its `Active` never wakes to drain its
own outbound queue.

## Platform / version

* Linux x86_64 only (`tokio::runtime::Handle::dump` is used in the
  classifier-free fallback path — though our `std::thread`-based
  watchdog itself doesn't require it).
* Sits on top of `fix/mux-deadlock` (`1c814df`).
* Cargo: `tokio_unstable` cfg (in `.cargo/config.toml`) + tokio
  `taskdump` / `tracing` features (in `mux/test-harness/Cargo.toml`).

## Diagnostic instrumentation

The classifier relies on lightweight atomic counters added to
`mux/mux/src/connection/{stream.rs,active.rs}` and exposed via
`Handle::diag_snapshot()`:

* `Shared::outbound_pending: AtomicI64` — incremented in
  `Stream::poll_write` (and `send_window_update`) after a successful
  `start_send`, decremented in `Active::poll` when popping a
  `SendFrame` from the `stream_receivers` channel.
* `Diag::last_poll_at_ms: AtomicI64` — updated at the top of every
  `Active::poll` loop iteration.
* `Handle::diag_snapshot()` — atomic loads only; safe to call from a
  non-tokio `std::thread` watchdog even when the tokio runtime is
  starved.

All instrumentation is observation-only; no behavioural changes to the
fix or to the mux protocol.

[`fix/mux-deadlock`]: ../../../../tree/fix/mux-deadlock
