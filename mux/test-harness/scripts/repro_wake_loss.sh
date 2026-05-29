#!/usr/bin/env bash
# Estimate the residual wake-loss rate of tlsn-mux under heavy parallel pressure.
# Runs BATCHES batches of N parallel `cargo test --test wake_loss_repro`
# invocations, then aggregates the classifier output.
#
# Defaults: BATCHES=10, N = 4 × nproc.
#
# Why 10 batches: with fix/mux-deadlock applied the bug is fleeting —
# only ~1-2% of attempts fire it at N=128 on a 16-core host. A single
# batch of 128 attempts will often produce zero observations. 1280
# attempts (10 × 128) gives ≈20 observations on average — enough to
# confirm the bug still manifests without being noise.
set -uo pipefail
BATCHES="${1:-10}"
N="${2:-$(( $(nproc) * 4 ))}"

PER_ATTEMPT_BUDGET=120
EST_BATCH_SECS=$((PER_ATTEMPT_BUDGET + 15))
EST_TOTAL_MIN=$(( (BATCHES * EST_BATCH_SECS + 59) / 60 ))

cat <<EOF
================================================================
  tlsn-mux residual wake-loss probe
================================================================

Plan:  $BATCHES batches × $N parallel attempts = $((BATCHES * N)) total
Estimated wall time: ~${EST_TOTAL_MIN} minutes on this host.

The wake-loss is fleeting with fix/mux-deadlock applied — a single
attempt only fires it at ~1-2% rate even under 4× CPU oversubscription.
That's why we run many batches; a single short run will likely report
zero real wake-losses.

For each attempt the in-test 60 s watchdog (a std::thread, not
tokio-based, so it fires reliably even under heavy starvation) reads
the diagnostic counters and classifies the outcome:

  ok                       — test completed within 60 s
  STRESS_FALSE_POSITIVE    — runtime alive but workload too big for 60 s
  WAKE_LOSS_WRITE          — REAL wake-loss: outbound frames stranded
  WAKE_LOSS_READ           — REAL wake-loss: inbound bytes stranded
  PROTOCOL_DEADLOCK        — both sides idle, nothing in flight
  CPU_STARVED              — even the watchdog ran late; dump unreliable
  UNK                      — outer timeout reaped the process

The aggregate at the end reports the WAKE_LOSS_* count across all
batches. Any non-zero count means the fix has a residual hole.

Streaming output starts after the first batch's initial ~25 s.

================================================================

EOF

# Build once up front so the per-attempt cargo invocations don't fight
# over the artifact lock.
echo "=== building wake_loss_repro binary ==="
cargo test --no-run -p test-harness --test wake_loss_repro 2>&1 | tail -1
echo

# Per-batch counters accumulated into globals.
TOTAL_HANGS=0
TOTAL_OKS=0
TOTAL_UNK=0
TOTAL_STRESS=0
TOTAL_WAKE_W=0
TOTAL_WAKE_R=0
TOTAL_PROTO=0
TOTAL_STARVED=0
ALL_LOG_DIRS=()

run_one_batch() {
    local batch="$1"
    local log_dir
    log_dir=$(mktemp -d /tmp/wake-loss-repro.XXXXXX)
    ALL_LOG_DIRS+=("$log_dir")

    echo "===== batch $batch / $BATCHES @ $(date -u +%H:%M:%S) ====="
    echo "  logs: $log_dir"

    local pids=()
    for i in $(seq 1 "$N"); do
        local out="$log_dir/attempt-$(printf %04d "$i").log"
        (
            timeout --kill-after=10 "$PER_ATTEMPT_BUDGET" stdbuf -oL -eL \
                cargo test -p test-harness --test wake_loss_repro -- --nocapture \
                > "$out" 2>&1 &
            local cargo_pid=$!

            local reported=
            while kill -0 "$cargo_pid" 2>/dev/null; do
                if [ -z "$reported" ] && grep -q "HANG WATCHDOG FIRED" "$out" 2>/dev/null; then
                    printf '  [%s] b%02d attempt %4d: HANG (watchdog fired)\n' \
                        "$(date -u +%H:%M:%S)" "$batch" "$i"
                    reported=hang
                elif [ -z "$reported" ] && grep -q "test result: ok" "$out" 2>/dev/null; then
                    printf '  [%s] b%02d attempt %4d: ok\n' \
                        "$(date -u +%H:%M:%S)" "$batch" "$i"
                    reported=ok
                fi
                sleep 0.2
            done

            wait "$cargo_pid" 2>/dev/null
            local rc=$?
            echo "EXIT=$rc" >> "$out"

            if [ -z "$reported" ]; then
                if grep -q "HANG WATCHDOG FIRED" "$out" 2>/dev/null; then
                    printf '  [%s] b%02d attempt %4d: HANG (post-exit)\n' \
                        "$(date -u +%H:%M:%S)" "$batch" "$i"
                elif grep -q "test result: ok" "$out" 2>/dev/null; then
                    printf '  [%s] b%02d attempt %4d: ok (post-exit)\n' \
                        "$(date -u +%H:%M:%S)" "$batch" "$i"
                else
                    printf '  [%s] b%02d attempt %4d: UNK (rc=%s)\n' \
                        "$(date -u +%H:%M:%S)" "$batch" "$i" "$rc"
                fi
            fi
        ) &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Per-batch classification breakdown.
    local hangs=0 oks=0 unk=0 stress=0 wake_w=0 wake_r=0 proto=0 starved=0
    for log in "$log_dir"/attempt-*.log; do
        if grep -q "HANG WATCHDOG FIRED" "$log"; then
            hangs=$((hangs+1))
        elif grep -q "test result: ok" "$log"; then
            oks=$((oks+1))
        else
            unk=$((unk+1))
        fi
        if grep -q "CLASSIFICATION: STRESS_FALSE_POSITIVE" "$log"; then stress=$((stress+1)); fi
        if grep -q "CLASSIFICATION: WAKE_LOSS_WRITE"       "$log"; then wake_w=$((wake_w+1)); fi
        if grep -q "CLASSIFICATION: WAKE_LOSS_READ"        "$log"; then wake_r=$((wake_r+1)); fi
        if grep -q "CLASSIFICATION: PROTOCOL_DEADLOCK"     "$log"; then proto=$((proto+1)); fi
        if grep -q "CLASSIFICATION: CPU_STARVED"           "$log"; then starved=$((starved+1)); fi
    done

    printf '  batch %d: hangs=%d ok=%d unk=%d  |  stress=%d wake_write=%d wake_read=%d proto=%d starved=%d\n' \
        "$batch" "$hangs" "$oks" "$unk" "$stress" "$wake_w" "$wake_r" "$proto" "$starved"
    echo

    TOTAL_HANGS=$((TOTAL_HANGS + hangs))
    TOTAL_OKS=$((TOTAL_OKS + oks))
    TOTAL_UNK=$((TOTAL_UNK + unk))
    TOTAL_STRESS=$((TOTAL_STRESS + stress))
    TOTAL_WAKE_W=$((TOTAL_WAKE_W + wake_w))
    TOTAL_WAKE_R=$((TOTAL_WAKE_R + wake_r))
    TOTAL_PROTO=$((TOTAL_PROTO + proto))
    TOTAL_STARVED=$((TOTAL_STARVED + starved))
}

for b in $(seq 1 "$BATCHES"); do
    run_one_batch "$b"
done

TOTAL_ATTEMPTS=$((BATCHES * N))
WAKE_TOTAL=$((TOTAL_WAKE_W + TOTAL_WAKE_R))

echo "================================================================"
echo "  AGGREGATE — $BATCHES batches × $N parallel = $TOTAL_ATTEMPTS attempts"
echo "================================================================"
printf '  Healthy completions:        %4d\n' "$TOTAL_OKS"
printf '  STRESS_FALSE_POSITIVE:      %4d  (workload too big for 60 s budget)\n' "$TOTAL_STRESS"
printf '  WAKE_LOSS_WRITE:            %4d  *** real bug recurrence ***\n' "$TOTAL_WAKE_W"
printf '  WAKE_LOSS_READ:             %4d  *** real bug recurrence ***\n' "$TOTAL_WAKE_R"
printf '  PROTOCOL_DEADLOCK:          %4d\n' "$TOTAL_PROTO"
printf '  CPU_STARVED:                %4d\n' "$TOTAL_STARVED"
printf '  UNK (reaped pre-classify):  %4d\n' "$TOTAL_UNK"
echo
if [ "$WAKE_TOTAL" -gt 0 ]; then
    printf '  ===> %d real wake-loss observations across %d attempts (%.2f%%).\n' \
        "$WAKE_TOTAL" "$TOTAL_ATTEMPTS" "$(echo "scale=4; $WAKE_TOTAL * 100 / $TOTAL_ATTEMPTS" | bc)"
    echo '       Fix has a residual hole. Inspect logs in the dirs above.'
else
    printf '  ===> 0 real wake-loss observations across %d attempts.\n' "$TOTAL_ATTEMPTS"
    echo '       Could be true zero, or this run got unlucky — bug rate is ~1-2%.'
fi
echo
echo "  Per-batch logs in: ${ALL_LOG_DIRS[*]}"
