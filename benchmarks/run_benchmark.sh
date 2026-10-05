#!/usr/bin/env bash
# Phase 6 — automated pipeline benchmark.
#
# For each input-rate tier: run the Go simulator for $DURATION seconds while
# collecting Kafka end offsets, per-query committed checkpoint offsets,
# event-time -> result freshness from Redis, and docker-stats resource
# samples. After each tier the script waits (up to $DRAIN_MAX s, counted from
# the end of the load) for consumers to catch up and records how long that
# took — the backpressure signal.
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
# The simulator sizes matches and the player pool from the rate (<= 10 shots/s
# per match), so load scales the way real traffic does: more players, not
# hotter ones. --events-per-sec counts SHOT events on gameplay_events only.

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT"   # the simulator finds simulator/profiles from the repo root
COLLECT="$PROJECT/benchmarks/collect.py"
SIM="$PROJECT/simulator/bin/simulator"
OUT="$PROJECT/benchmarks/results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
TIERS_JSONL="$OUT/tiers.jsonl"
STATS_PID=""

stop_stats() {
  if [ -n "$STATS_PID" ]; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
}
trap stop_stats EXIT
trap 'stop_stats; echo "benchmark interrupted" >&2; exit 130' INT TERM

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

# Seconds from $1 (epoch, end of load) until the worst query's lag is
# <= LAG_OK, capped at DRAIN_MAX. Lag comes from `collect.py maxlag` — the
# same definition the tier record uses. An unreadable lag is retried; if no
# reading succeeds before DRAIN_MAX the tier is invalid and we fail.
wait_drained() {
  local start=$1 now lag ok=0
  while true; do
    if lag=$(python3 "$COLLECT" maxlag) && [[ "$lag" =~ ^[0-9]+$ ]]; then
      ok=1
      now=$(date +%s)
      if [ "$lag" -le "$LAG_OK" ]; then echo $((now - start)); return 0; fi
    else
      echo "warn: collect.py maxlag failed (got '${lag:-}'), retrying" >&2
      now=$(date +%s)
    fi
    if [ $((now - start)) -ge "$DRAIN_MAX" ]; then
      [ "$ok" -eq 1 ] || { echo "no lag reading in ${DRAIN_MAX}s" >&2; return 1; }
      echo $((now - start)); return 0
    fi
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
  if ! "$SIM" --events-per-sec "$RATE" \
    --duration "${DURATION}s" > "$OUT/tier_${i}_sim.log" 2>&1; then
    echo "simulator failed at ${RATE} events/s; tier data would be invalid:" >&2
    tail -n 8 "$OUT/tier_${i}_sim.log" >&2
    exit 1
  fi
  T1=$(date +%s)
  stop_stats

  python3 "$COLLECT" snapshot > "$S1"
  python3 "$COLLECT" freshness "$T0" "$T1" > "$FRESH"

  DRAIN=$(wait_drained "$T1") || {
    echo "tier $i: could not measure drain (collect.py maxlag kept failing)" >&2
    exit 1
  }
  python3 "$COLLECT" tier "$RATE" "$T0" "$T1" "$DRAIN" "$S0" "$S1" \
    "$FRESH" "$STATS" "$TIERS_JSONL"

  echo "── tier $i done: produced/s + lag + freshness recorded (drain ${DRAIN}s)"
done

echo
echo "── benchmark complete: $TIERS_JSONL"
echo "── plot:  make benchmark-plot   (newest results by default)"
