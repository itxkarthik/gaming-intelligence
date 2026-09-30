#!/usr/bin/env bash
# Phase 6 — automated pipeline benchmark.
#
# For each input-rate tier: run the Go simulator for $DURATION seconds while
# collecting Kafka end offsets, per-query checkpoint offsets, event-time ->
# result freshness from Redis, and docker-stats resource samples. After each
# tier the script waits (up to $DRAIN_MAX s) for consumers to catch up and
# records how long that took — the backpressure signal.
#
#   make benchmark                       # default tiers 1000..20000
#   RATES="1000 5000" DURATION=30 make benchmark   # quick pass
#
# Output: benchmarks/results/<ts>/tiers.jsonl (+ per-tier snapshots/stats/logs)
set -euo pipefail

RATES=${RATES:-"1000 2500 5000 10000 20000"}
DURATION=${DURATION:-60}     # seconds of load per tier
DRAIN_MAX=${DRAIN_MAX:-90}   # max seconds to wait for lag to settle
LAG_OK=${LAG_OK:-3000}       # considered "caught up" (events behind, max query)
PLAYERS=${PLAYERS:-300}

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COLLECT="$PROJECT/benchmarks/collect.py"
SIM="$PROJECT/simulator/bin/simulator"
OUT="$PROJECT/benchmarks/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
Tiers_JSONL="$OUT/tiers.jsonl"
STATS_CTR=0

echo "── benchmark: rates=[$RATES] duration=${DURATION}s drain_max=${DRAIN_MAX}s"
echo "── output: $OUT"

[ -x "$SIM" ] || { echo "simulator binary missing — run: make simulator-build"; exit 1; }
docker ps --format '{{.Names}}' | grep -q gaming-kafka || {
  echo "stack not running — run: make up"; exit 1; }

sample_stats() {  # background sampler: one docker-stats line every 15s
  local csv="$1"
  while true; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
      >> "$csv" 2>/dev/null || return 0
    sleep 15
  done
}

wait_drained() {  # poll until max lag across queries < LAG_OK (or DRAIN_MAX)
  local start now snap lag
  start=$(date +%s)
  while true; do
    snap="$OUT/snap_drain.json"
    python3 "$COLLECT" snapshot > "$snap"
    # real lag check: end vs ckpt from the drain snapshot itself
    lag=$(python3 - "$snap" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
worst = 0
for d, ck in s["ckpt"].items():
    for t, parts in ck.items():
        ends = {str(k): v for k, v in s["end"].get(t, {}).items()}
        for p, off in parts.items():
            worst = max(worst, ends.get(str(p), off) - off)
print(worst)
PY
)
    now=$(date +%s)
    if [ "$lag" -le "$LAG_OK" ]; then echo $((now - start)); return 0; fi
    if [ $((now - start)) -ge "$DRAIN_MAX" ]; then echo $((now - start)); return 0; fi
    sleep 5
  done
}

i=0
for RATE in $RATES; do
  i=$((i + 1))
  echo
  echo "══ tier $i: target ${RATE} events/s ══════════════════════════════"
  S0="$OUT/tier_${i}_snap0.json"; S1="$OUT/tier_${i}_snap1.json"
  FRESH="$OUT/tier_${i}_fresh.json"; STATS="$OUT/tier_${i}_stats.csv"

  python3 "$COLLECT" snapshot > "$S0"
  T0=$(date +%s)

  sample_stats "$STATS" & STATS_PID=$!
  "$SIM" --players "$PLAYERS" --events-per-sec "$RATE" \
    --duration "${DURATION}s" > "$OUT/tier_${i}_sim.log" 2>&1 || true
  T1=$(date +%s)
  kill "$STATS_PID" 2>/dev/null || true

  python3 "$COLLECT" snapshot > "$S1"
  python3 "$COLLECT" freshness "$T0" "$T1" > "$FRESH"

  DRAIN=$(wait_drained)
  python3 "$COLLECT" tier "$RATE" "$T0" "$T1" "$DRAIN" "$S0" "$S1" \
    "$FRESH" "$STATS" "$Tiers_JSONL"

  echo "── tier $i done: produced/s + lag + freshness recorded (drain ${DRAIN}s)"
done

echo
echo "── benchmark complete: $Tiers_JSONL"
echo "── plot:  uv run --with matplotlib python3 benchmarks/plot_results.py $Tiers_JSONL"
