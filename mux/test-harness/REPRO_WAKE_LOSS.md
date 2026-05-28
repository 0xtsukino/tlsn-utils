# `Active::poll` wake-loss minimal repro

This branch reproduces an intermittent deadlock in
`tlsn_mux::connection::active::Active::poll` where the task parks at
`Registration::poll_read_ready` waiting for inbound bytes and **does not
wake** when a new `SendFrame` command is queued into its
`stream_receivers` mpsc channel by a concurrent `Stream::poll_write`. The
buffered command never reaches the wire; the peer's mux then waits forever
for bytes that are stranded locally.

## Reproduce

```
cargo test --no-run -p test-harness --test wake_loss_repro
bash mux/test-harness/scripts/repro_wake_loss.sh
```

The script defaults to **N = 4 × nproc** parallel processes (i.e. ~4× CPU
oversubscription). On a 16-core x86_64 Linux host this means N = 64.
Observed on such a host:

| N | Hang rate |
|---|---|
| 16 (≈ 1× cores) | 0 / 16 |
| 32 (≈ 2× cores) | 0 / 32 |
| 48 (≈ 3× cores) | 15 / 48 (~31%) |
| 64 (≈ 4× cores) | 61 / 64 (~95%) |

So the test reliably hangs at ≥ 4× cores' worth of parallelism. Below 2× it
typically passes. Pass `N` explicitly if your host has a different core
count:

```
bash mux/test-harness/scripts/repro_wake_loss.sh 64
```

Per-attempt logs land in a fresh `/tmp/wake-loss-repro.XXXXXX` directory
(printed by the script). Each hang contains a tokio task dump captured by
the in-test 60 s watchdog; look for two poll-loops parked at
`tokio::runtime::io::registration::Registration::poll_read_ready`.

Each per-attempt log contains a tokio task dump captured by the in-test
60 s watchdog; look for two poll-loops parked at
`tokio::runtime::io::registration::Registration::poll_read_ready`.

## What the repro does

`wake_loss_repro` pairs two `Connection`s over loopback TCP via
`test_harness::connected_peers`, opens 256 streams with deterministic
per-stream varied workloads (payload size 256 B .. 64 KiB, 1 .. 30 rounds,
0 .. 200 ms start delays, 0 .. 20 ms between-round delays, four send/recv
flavours: ping-pong, write-first, small-chunks, bursty), plus 2 CPU-hog
tasks that busy-spin / `yield_now` in a loop to create scheduler contention.

Single-process the test passes. **Under 16-process parallel pressure
the bug reproduces in ~100% of attempts within ~75 s.** The parallel
pressure is what we believe is required to expose the wake-up race; the
script supplies it.

## What the task dump shows

Two `Connection::poll` / `Active::poll` tasks parked deep at
`Registration::poll_read_ready` — i.e. each side is waiting for the
**other** side's mux to send more bytes. The receiver-side
`stream_receivers.poll_next_unpin(cx)` has returned `Pending` (its waker is
registered). Concurrently, `Stream::poll_write` has done
`self.sender.start_send(cmd)` returning `Ok(())` — the `SendFrame`
command is sitting in the mpsc channel — but `Active::poll` is never
rescheduled to consume it.

The sender is `futures::channel::mpsc::Sender<StreamCommand>`. The
receiver wrapper is
`SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>` polled
inside `Active::poll`. The wake-up from `start_send` to this `SelectAll`
appears to be lost under contention.

## Platform / version

* Linux x86_64 only (`tokio::runtime::Handle::dump` requirement).
* tlsn-mux at this branch's commit (no patches in the mux source itself —
  the `64722f7` baseline = current `dev` HEAD for `mux/` reproduces this
  unchanged).
* Cargo: requires `tokio_unstable` cfg (set in `.cargo/config.toml` at
  the workspace root) and tokio `taskdump` / `tracing` features (set in
  `mux/test-harness/Cargo.toml`).
