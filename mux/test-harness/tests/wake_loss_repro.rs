//! Minimal reproduction of an Active::poll wake-loss in tlsn-mux.
//!
//! Pairs two Connections over loopback TCP, opens 256 streams with
//! deterministic per-stream varied workloads (payload size 256 B .. 64 KiB,
//! 1 .. 30 rounds, 0 .. 200 ms start-delays, 0 .. 20 ms between-round delays,
//! four flavours of send/recv pattern), plus 2 CPU-hog tasks for scheduler
//! contention.
//!
//! Single-process: this test passes.
//! Under 16-process parallel pressure (see scripts/repro_wake_loss.sh):
//! deadlocks reliably (~100% of attempts within ~75 s).
//!
//! On a hang, an inlined 60 s tokio taskdump watchdog panics with every
//! parked task's stack. The signature is two `Connection::poll` /
//! `Active::poll` loops parked at `Registration::poll_read_ready` while
//! `SendFrame` commands sit stranded in their local stream_receivers mpsc
//! channel — buffered by `Stream::poll_write`'s `sender.start_send(...)`
//! returning Ok, but never picked up by Active.
//!
//! Linux x86_64 only (tokio taskdump).

use std::time::{Duration, Instant};

use anyhow::Result;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use test_harness::connected_peers;
use tlsn_mux::{Config, Connection, Stream};
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

const STREAMS: usize = 256;
const CPU_HOGS: usize = 2;
const CPU_BURST_MS: u64 = 5;
const TEST_BUDGET_SECS: u64 = 50;
const WATCHDOG_SECS: u64 = 60;

fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[derive(Clone, Copy, Debug)]
enum Flavor {
    PingPong,
    WriteFirst,
    SmallChunks,
    Bursty,
}

#[derive(Clone, Copy, Debug)]
struct Profile {
    payload_size: usize,
    rounds: usize,
    start_delay_us: u64,
    between_delay_us: u64,
    flavor: Flavor,
}

fn profile_for(stream_idx: u64) -> Profile {
    let base = splitmix64(stream_idx);
    let payload_buckets = [256usize, 1024, 4096, 16384, 65536];
    Profile {
        payload_size: payload_buckets[(base as usize) % payload_buckets.len()],
        rounds: 1 + ((base >> 8) % 30) as usize,
        start_delay_us: (base >> 16) % 200_000,
        between_delay_us: (base >> 32) % 20_000,
        flavor: match (base >> 56) % 4 {
            0 => Flavor::PingPong,
            1 => Flavor::WriteFirst,
            2 => Flavor::SmallChunks,
            _ => Flavor::Bursty,
        },
    }
}

fn spawn_poll_driver(
    mut conn: Connection<Compat<TcpStream>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = futures::future::poll_fn(|cx| conn.poll(cx)).await;
    })
}

fn spawn_cpu_hog(deadline: Instant) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while Instant::now() < deadline {
            let burst_start = Instant::now();
            while burst_start.elapsed() < Duration::from_millis(CPU_BURST_MS) {
                std::hint::black_box(0u64);
            }
            tokio::task::yield_now().await;
        }
    })
}

async fn run_profile(mut stream: Stream, profile: Profile) -> Result<()> {
    if profile.start_delay_us > 0 {
        tokio::time::sleep(Duration::from_micros(profile.start_delay_us)).await;
    }
    let payload: Vec<u8> = (0..profile.payload_size)
        .map(|i| (i & 0xFF) as u8)
        .collect();
    let (mut reader, mut writer) = AsyncReadExt::split(&mut stream);
    let mut buf = vec![0u8; profile.payload_size];

    for _ in 0..profile.rounds {
        match profile.flavor {
            Flavor::PingPong => {
                let write_fut = async {
                    writer.write_all(&payload).await?;
                    writer.flush().await?;
                    Ok::<_, anyhow::Error>(())
                };
                let read_fut = async {
                    reader.read_exact(&mut buf).await?;
                    Ok::<_, anyhow::Error>(())
                };
                futures::future::try_join(write_fut, read_fut).await?;
            }
            Flavor::WriteFirst => {
                writer.write_all(&payload).await?;
                writer.flush().await?;
                reader.read_exact(&mut buf).await?;
            }
            Flavor::SmallChunks => {
                let write_fut = async {
                    for chunk in payload.chunks(64) {
                        writer.write_all(chunk).await?;
                        writer.flush().await?;
                    }
                    Ok::<_, anyhow::Error>(())
                };
                let read_fut = async {
                    reader.read_exact(&mut buf).await?;
                    Ok::<_, anyhow::Error>(())
                };
                futures::future::try_join(write_fut, read_fut).await?;
            }
            Flavor::Bursty => {
                let write_fut = async {
                    for _ in 0..3 {
                        writer.write_all(&payload).await?;
                    }
                    writer.flush().await?;
                    Ok::<_, anyhow::Error>(())
                };
                let read_fut = async {
                    for _ in 0..3 {
                        reader.read_exact(&mut buf).await?;
                    }
                    Ok::<_, anyhow::Error>(())
                };
                futures::future::try_join(write_fut, read_fut).await?;
            }
        }

        if profile.between_delay_us > 0 {
            tokio::time::sleep(Duration::from_micros(profile.between_delay_us)).await;
        }
    }
    Ok(())
}

fn spawn_hang_watchdog(timeout: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        let handle = tokio::runtime::Handle::current();
        let dump = handle.dump().await;
        eprintln!("=== HANG WATCHDOG FIRED after {timeout:?} — tokio task dump ===");
        for (i, task) in dump.tasks().iter().enumerate() {
            eprintln!("--- task #{i} id={} ---", task.id());
            eprintln!("{}", task.trace());
        }
        eprintln!("=== END TASK DUMP ===");
        eprintln!("watchdog: hang detected after {timeout:?}");
        std::process::exit(1);
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_loss_repro() -> Result<()> {
    let watchdog = spawn_hang_watchdog(Duration::from_secs(WATCHDOG_SECS));
    let deadline = Instant::now() + Duration::from_secs(TEST_BUDGET_SECS);

    let (mut server_conn, mut client_conn) =
        connected_peers(Config::default(), Config::default(), None).await?;

    let mut server_streams = Vec::with_capacity(STREAMS);
    let mut client_streams = Vec::with_capacity(STREAMS);
    for i in 0..STREAMS {
        let id = format!("stream-{i:04}");
        server_streams.push(server_conn.new_stream(id.as_bytes())?);
        client_streams.push(client_conn.new_stream(id.as_bytes())?);
    }

    let server_driver = spawn_poll_driver(server_conn);
    let client_driver = spawn_poll_driver(client_conn);
    let cpu_hogs: Vec<_> = (0..CPU_HOGS).map(|_| spawn_cpu_hog(deadline)).collect();

    let mut tasks = Vec::with_capacity(STREAMS * 2);
    for (i, stream) in server_streams.into_iter().enumerate() {
        let profile = profile_for(i as u64);
        tasks.push(tokio::spawn(run_profile(stream, profile)));
    }
    for (i, stream) in client_streams.into_iter().enumerate() {
        let profile = profile_for(i as u64);
        tasks.push(tokio::spawn(run_profile(stream, profile)));
    }

    for t in tasks {
        t.await??;
    }

    for h in cpu_hogs {
        h.abort();
    }
    server_driver.abort();
    client_driver.abort();
    watchdog.abort();
    Ok(())
}
