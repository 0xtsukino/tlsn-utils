#!/usr/bin/env bash
# Fires N parallel `cargo test --test wake_loss_repro` invocations and
# reports how many hung. Default N = 4 * nproc (≈ 4x CPU oversubscription),
# which gives ~95-100% hang rate on a 16-core x86_64 Linux host.
# Lower N reduces the hit rate; N below ≈ 2 * nproc may show 0/N hangs.
set -uo pipefail
N=${1:-$(( $(nproc) * 4 ))}
LOG_DIR=$(mktemp -d /tmp/wake-loss-repro.XXXXXX)
echo "Logs: $LOG_DIR"

# Build once so subsequent invocations don't fight over the artifact lock.
cargo test --no-run -p test-harness --test wake_loss_repro 2>&1 | tail -1

cat <<EOF

Each attempt runs the in-test 60 s taskdump watchdog. Outcomes can appear
any time between ~25 s (healthy) and ~70 s (hung — watchdog fires at 60 s,
then std::process::exit lets cargo test exit). The full batch will be
done within ~75 s. Hang lines stream out as soon as the watchdog fires;
ok lines appear when each attempt completes its workload. Nothing prints
during the early phase — that's normal.

EOF

echo "=== firing $N parallel attempts at $(date -u +%H:%M:%S) ==="
pids=()
for i in $(seq 1 "$N"); do
    out="$LOG_DIR/attempt-$(printf %04d "$i").log"
    (
        # Spawn cargo test in the background so this subshell can watch
        # the log file and announce outcomes the instant they're written —
        # rather than waiting for the child to fully exit.
        timeout --kill-after=10 90 stdbuf -oL -eL \
            cargo test -p test-harness --test wake_loss_repro -- --nocapture \
            > "$out" 2>&1 &
        cargo_pid=$!

        reported=
        while kill -0 "$cargo_pid" 2>/dev/null; do
            if [ -z "$reported" ] && grep -q "HANG WATCHDOG FIRED" "$out" 2>/dev/null; then
                printf '  [%s] attempt %4d: HANG (watchdog fired)\n' "$(date -u +%H:%M:%S)" "$i"
                reported=hang
            elif [ -z "$reported" ] && grep -q "test result: ok" "$out" 2>/dev/null; then
                printf '  [%s] attempt %4d: ok\n' "$(date -u +%H:%M:%S)" "$i"
                reported=ok
            fi
            sleep 0.2
        done

        wait "$cargo_pid" 2>/dev/null
        rc=$?
        echo "EXIT=$rc" >> "$out"

        # Final fallback: if neither pattern showed up while running
        # (timeout reaped before watchdog could log, or odd termination).
        if [ -z "$reported" ]; then
            if grep -q "HANG WATCHDOG FIRED" "$out" 2>/dev/null; then
                printf '  [%s] attempt %4d: HANG (post-exit)\n' "$(date -u +%H:%M:%S)" "$i"
            elif grep -q "test result: ok" "$out" 2>/dev/null; then
                printf '  [%s] attempt %4d: ok (post-exit)\n' "$(date -u +%H:%M:%S)" "$i"
            else
                printf '  [%s] attempt %4d: UNK (rc=%s)\n' "$(date -u +%H:%M:%S)" "$i" "$rc"
            fi
        fi
    ) &
    pids+=($!)
done

for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
done

echo "=== all attempts done at $(date -u +%H:%M:%S) ==="

hangs=0; oks=0; unk=0
for log in "$LOG_DIR"/attempt-*.log; do
    if grep -q "HANG WATCHDOG FIRED" "$log"; then
        hangs=$((hangs+1))
    elif grep -q "test result: ok" "$log"; then
        oks=$((oks+1))
    else
        unk=$((unk+1))
    fi
done

echo
echo "Hangs: $hangs / $N    healthy: $oks    unknown: $unk"
echo "Per-attempt logs in: $LOG_DIR"
