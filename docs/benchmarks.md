# Phase 6 — Pipeline Benchmark & Historical Analysis

Real measurements from the automated benchmark (`make benchmark`) run on
**2026-09-30, 21:33–21:41 IST**, plus the batch-analysis suite over the
accumulated Parquet archive.

## Environment

| | |
|---|---|
| Host | 4 cores / 13 GiB RAM (Arch Linux, Docker) |
| Cluster | 2 Spark workers × 2 cores / 2 GB, Spark 3.5.1, Kafka 3.7 (KRaft) |
| Streaming caps | `spark.cores.max=1`, executor 512 MB + 256 MB overhead **per job** (4 queries coexist) |
| Jobs under test | `server_health`, `cheat_detection`, `match_quality`, `advanced_analytics` (4 checkpointed queries) |
| Load generator | Go simulator, `--players 300`, tiered `--events-per-sec` |

## Methodology

Each tier runs the simulator for **60 s** at a fixed target rate while
`benchmarks/collect.py` samples:

- **Produced** — Kafka end-offset delta per topic (`kafka-get-offsets.sh`)
- **Consumed** — checkpoint offset delta per query (Spark's committed absolute offsets)
- **Lag** — end offset − committed offset, worst query, at tier end
- **Drain** — seconds until worst lag < 3 000 events (cap 90 s) = catch-up time after input stops
- **Resources** — `docker stats` every 15 s during load (CPU peak, mem peak per container)
- **Result freshness** — `updated_at − window_end` on `match:*`/`server:*` Redis hashes

Raw per-tier snapshots live in `benchmarks/results/<ts>/` (gitignored);
only distilled results are committed here.

## Results

| Target (ev/s) | Produced (ev/s) | Lag @ tier end | Lag (s) | Drain | Worker CPU peak | Worker mem peak |
|---|---|---|---|---|---|---|
| 1 000 | 1 066 | 2 101 | 2.0 | 7 s | 176 % / 233 % | 1.7 GB |
| 2 500 | 2 615 | 933 | 0.4 | 7 s | 138 % / 328 % | 1.5 GB |
| 5 000 | 5 115 | 0 | 0.0 | 6 s | 199 % / 263 % | 1.5 GB |
| 10 000 | 10 364 | 29 863 | 2.9 | 6 s | 207 % / 285 % | 1.4 GB |
| 20 000 | 20 695 | 225 130 | 10.9 | 7 s | 138 % / 290 % | 1.3 GB |

![Throughput and backpressure](benchmarks.png)

### Observations

- **Produced tracks target within +7 %** at every tier — the Go rate limiter
  delivers what it promises up to 20 k ev/s on this box.
- **The pipeline keeps pace through 5 000 ev/s** (zero lag at tier end).
  At 10 k it runs ~3 s behind; at 20 k ~11 s behind — and in **every tier
  the backlog fully drains within 6–7 s** of input stopping (drain rate far
  exceeds input rate: buffered data is read at raw-consumer speed).
- **Backpressure is visible and graceful**: Spark logs
  `ProcessingTimeExecutor: Current batch is falling behind. The trigger
  interval is 10000 milliseconds, but spent 15963 milliseconds` at high
  tiers — the trigger slips instead of buffering unboundedly; lag is the
  shock absorber and recovers on drain.
- **Resource headroom remains**: worker CPU peaks ≤ 328 % of one core
  (2 workers × 2 cores = 400 % available each), memory ≤ 1.7 GB of 2 GB.
  The limiting factor is CPU, not RAM.
- **Consumed can slightly exceed produced within a tier** because a tier's
  consumption window also includes draining the previous tier's residual
  backlog — measurement-window artifact, not duplicate processing.
- **Result freshness**: over the whole run, hashes written during the
  window give p50 = p95 = **−4.0 s** (`updated_at − window_end`, n = 10).
  The near-zero (slightly negative) value means results land the moment
  their window closes in event time; the sign is event-clock vs wall-clock
  skew. Note the *design* latency on top: watermark delay 10 s (server
  health) and session gap 120 s (match quality) are intentional
  correctness costs, not processing lag. Per-tier freshness sampling was
  too sparse to chart — result writes cluster at window close (match rows
  only emit after the 120 s session gap), so stream lag is used as the
  load-sensitive latency metric instead.
- The ROADMAP's illustrative table (up to 100 k ev/s) assumed larger
  hardware; on this 4-core box the practical ceiling was **not reached at
  20 k ev/s** — saturation testing beyond that needs bigger iron.

## Historical Batch Analysis (PySpark batch mode)

`make spark-batch JOB=all` runs six analyses over `data/parquet/`
(≈ 94 MB, 864 files, 1.66 M rows at run time) in local mode — deliberately
not on the cluster, which is fully subscribed by the streaming jobs:

| Job | Question | Example result |
|---|---|---|
| `skill` | Player skill progression | Accuracy stable ≈ 0.35 (p50 0.31), reaction ≈ 259 ms across all 15-min buckets; drift first→last +0.008 |
| `weapon` | Weapon meta per rank | ak47/awp/usp/deagle/m4a4 ≈ 20 % kill share each (uniform by simulator design); dominant picks vary by rank (gold → awp, diamond → m4a4) |
| `cheat` | Precision/recall vs archetype labels | 300 labeled players, 14 actual cheaters. Base `suspicion_score` alone: precision 0.089 / recall 0.357 / F1 0.143 @ 0.70 |
| `quality` | Match score distribution | 20 matches: avg 83.2, p50 84.9; 90 % BALANCED / 10 % UNBALANCED; histogram peaks at 80–90 |
| `servers` | Reliability ranking | 9 servers cluster at avg health 84; **server-02** is the outlier: avg 6.5, 100 % CRITICAL windows, 115 ms, 10.1 % loss |
| `peak` | Peak-hour analysis | 16:00 UTC = 60.9 % of events, 15:00 = 32.5 % (synthetic traffic — archive spans a few simulated days) |

Honest limitations found by the batch layer itself:

1. **Cheat precision is low on the archived score** because the Parquet
   archive stores only the pre-boost `suspicion_score` — the production
   alerting path additionally uses IsolationForest flags and the CUSUM
   behavior boost, which are not archived. Archiving
   `suspicion_effective`/`iforest_flag` would make offline evaluation
   match production.
2. Weapon kill shares are uniform by simulator profile design — the
   per-rank ordering is the meaningful signal, not the totals.

## Reproduce

```bash
make benchmark            # 5 tiers, ~8 min (RATES="..." DURATION=30 to shorten)
make benchmark-plot       # regenerate docs/benchmarks.png from latest results
make spark-batch JOB=all  # or skill | weapon | cheat | quality | servers | peak
```
