"""Player Behavior Change Detection (CUSUM over Redis-backed state).

Tracks each player's shot-accuracy baseline with Welford's online algorithm
and a two-sided CUSUM on the z-score; sustained deviation beyond the decision
threshold raises a BEHAVIOR_ANOMALY. The CUSUM state lives in Redis
(`behavior:cusum:{id}`) so restarts don't lose cumulative history.

Each 5-second event-time bucket (with >= MIN_STEP_SHOTS shots) is one CUSUM
step per player: the mean accuracy of that player's shots in the bucket,
processed in chronological order and guarded by `last_window_end_ms` so
backlog catch-up and replayed micro-batches never collapse multiple windows
into one step or double-count a replayed window. Spark retains fragments
across micro-batches and emits completed windows only after a 30-second
watermark, allowing the simulator's up-to-19-second event lateness.

The first WARMUP_STEPS steps only build the baseline (mean and spread): a
detector judging against a two-sample spread trips on ordinary noise. h=8
keeps false alarms rare across thousands of players (in-control ARL in the
tens of thousands of steps at k=0.5) while a sustained 2-sigma shift is
still caught within ~6 steps.

A detection writes `player:behavior:{id}` (anomaly=1), which cheat_detection
reads to boost its suspicion score (ROADMAP "Cross-job data flow"). The flag
expires BEHAVIOR_FLAG_TTL_SECONDS after the latest detection (a persisting
shift keeps re-tripping the detector and refreshing it); once it has
expired, the player's next in-range step drops them from
`behavior:anomalies`.
"""

import math
import os
import time
from datetime import datetime

from pyspark.sql import functions as F

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import cached_batch, kafka_source, redis_client, run_with_retry
from src.common.schemas import SHOT_EVENT, parse_stream
from src.common.sinks import safe_parquet_archive

CUSUM_K = 0.5     # slack allowance per step (in sigmas)
CUSUM_H = 8.0     # decision threshold (sigmas of cumulative evidence)
WARMUP_STEPS = 10 # baseline-only steps before the detector judges
BEHAVIOR_BOOST = 0.15  # suspicion add-on applied by cheat_detection
MIN_STEP_SHOTS = 3
STEP_WINDOW = "5 seconds"
STEP_WATERMARK = "30 seconds"  # simulator shots can arrive up to 19 seconds late
BEHAVIOR_STATE_VERSION = "2"  # baseline/cursor built only from completed windows
BEHAVIOR_FLAG_TTL_SECONDS = 600
BEHAVIOR_EXPIRY_KEY = "behavior:anomalies:expiry"
BEHAVIOR_MIGRATION_KEY = "behavior:anomalies:expiry:migrated:v1"


def cusum_step(state, x, k=CUSUM_K, h=CUSUM_H):
    """Advance one observation through the CUSUM detector.

    state: {"n", "mean", "m2", "s_pos", "s_neg", "anomalies"}
    Returns (new_state, anomaly: bool, z: float). Pure — unit-testable.
    """
    n = int(state.get("n", 0))
    if n < WARMUP_STEPS:
        z, s_pos, s_neg, anomaly = 0.0, 0.0, 0.0, False
    else:
        sd = max(math.sqrt(float(state["m2"]) / (n - 1)),
                 0.02)  # floor: a near-constant baseline must not explode z
        z = (float(x) - float(state["mean"])) / sd
        # Clip per-step evidence to ±4σ: one batch alone adds at most
        # 4 - k = 3.5 < h, so only a SUSTAINED shift can trip the detector.
        z = max(-4.0, min(4.0, z))
        s_pos = max(0.0, float(state["s_pos"]) + z - k)
        s_neg = min(0.0, float(state["s_neg"]) + z + k)
        anomaly = s_pos > h or s_neg < -h

    # Alarm observations are out of control; feeding them into Welford would
    # move the baseline toward a sustained cheat and eventually hide it.
    if anomaly:
        n1 = n
        mean = float(state.get("mean", 0.0))
        m2 = float(state.get("m2", 0.0))
    else:
        n1 = n + 1
        prev_mean = float(state.get("mean", 0.0)) if n else float(x)
        mean = prev_mean + (float(x) - prev_mean) / n1
        m2 = float(state.get("m2", 0.0)) + (float(x) - mean) * (float(x) - prev_mean)

    new_state = {
        "state_version": BEHAVIOR_STATE_VERSION,
        "n": n1,
        "mean": mean,
        "m2": m2,
        "s_pos": 0.0 if anomaly else s_pos,   # reset after a detection
        "s_neg": 0.0 if anomaly else s_neg,
        "anomalies": int(state.get("anomalies", 0)) + (1 if anomaly else 0),
    }
    if "last_window_end_ms" in state:
        new_state["last_window_end_ms"] = int(state["last_window_end_ms"])
    return new_state, anomaly, z


def apply_behavior_boost(suspicion, behavior_flag):
    """cheat_detection blends this cross-job signal into its suspicion score."""
    boosted = float(suspicion) + (BEHAVIOR_BOOST if behavior_flag else 0.0)
    return round(min(1.0, boosted), 4)


def load_state(raw):
    """Discard incompatible baselines and cursors from partial-window detectors.

    Migration is lazy per player: the first completed v2 steps rebuild from
    zero, even if their ends precede the old cursor. The caller persists the
    version with that rebuilt state and cursor in one transaction, so replay
    loads the v2 state rather than resetting it again.
    """
    if not raw or raw.get("state_version") != BEHAVIOR_STATE_VERSION:
        raw = {}
    state = {
        "state_version": BEHAVIOR_STATE_VERSION,
        "last_window_end_ms": int(raw.get("last_window_end_ms", 0)),
        "n": int(raw.get("n", 0)),
        "mean": float(raw.get("mean", 0.0)),
        "m2": float(raw.get("m2", 0.0)),
        "s_pos": float(raw.get("s_pos", 0.0)),
        "s_neg": float(raw.get("s_neg", 0.0)),
        "anomalies": int(raw.get("anomalies", 0)),
    }
    return state


def _window_end_ms(row):
    """Extract epoch-ms end of a step row's window, or None if unwindowed."""
    try:
        win = row["window"]
    except (KeyError, TypeError, IndexError):
        return None
    if win is None:
        return None
    end = win["end"]
    if isinstance(end, datetime):
        return int(end.timestamp() * 1000)
    return int(end)


def player_steps(events):
    """Stateful step inputs; append mode emits only watermark-complete windows.

    Apply to the streaming source, never its static foreachBatch fragments:
    even a sub-threshold fragment must remain in Spark state for late shots.
    Redis sorts completed windows per player before advancing its replay cursor.
    """
    return (
        events.filter(
            (F.col("event_type") == SHOT_EVENT) & F.col("accuracy").isNotNull()
        )
        .withWatermark("event_timestamp", STEP_WATERMARK)
        .groupBy("player_id", F.window("event_timestamp", STEP_WINDOW))
        .agg(F.count(F.lit(1)).alias("shots"), F.avg("accuracy").alias("mean_accuracy"))
        .filter(F.col("shots") >= MIN_STEP_SHOTS)
    )


def update_behavior(r, alerts, steps):
    """Advance each player's CUSUM chronologically; raise, refresh or clear flags.

    steps: rows with player_id, shots, mean_accuracy, and optional window.
    """
    now = int(time.time())
    # One-time migration for anomaly IDs written before the expiry index
    # existed. Recover their remaining hash TTL or drop entries whose flag
    # hash has already expired.
    if not r.exists(BEHAVIOR_MIGRATION_KEY):
        members = r.smembers("behavior:anomalies")
        migration = r.pipeline()
        keep = []
        for player_id in members:
            key = f"player:behavior:{player_id}"
            migration.ttl(key)
            migration.hget(key, "anomaly")
        values = migration.execute() if members else []
        migration = r.pipeline()
        for player_id, ttl, anomaly in zip(members, values[::2], values[1::2]):
            if ttl > 0 and anomaly == "1":
                keep.append((player_id, now + ttl))
            else:
                migration.srem("behavior:anomalies", player_id)
        if keep:
            migration.zadd(BEHAVIOR_EXPIRY_KEY, dict(keep))
        migration.set(BEHAVIOR_MIGRATION_KEY, "1")
        migration.execute()

    # The behavior hash has a TTL, but Redis set members do not. Keep an
    # expiry index and reap stale IDs on each batch, including players who
    # have gone offline and will never produce another step.
    expired = r.zrangebyscore(BEHAVIOR_EXPIRY_KEY, "-inf", now)
    if expired:
        cleanup = r.pipeline()
        cleanup.srem("behavior:anomalies", *expired)
        cleanup.zremrangebyscore(BEHAVIOR_EXPIRY_KEY, "-inf", now)
        cleanup.execute()

    by_player = {}
    for row in steps:
        by_player.setdefault(row["player_id"], []).append(row)
    if not by_player:
        return

    player_ids = list(by_player.keys())
    pipe = r.pipeline()
    for pid in player_ids:
        pipe.hgetall(f"behavior:cusum:{pid}")
        pipe.exists(f"player:behavior:{pid}")
    reads = pipe.execute()

    pipe = r.pipeline()
    for pid, raw, flag_alive in zip(player_ids, reads[::2], reads[1::2]):
        state = load_state(raw)
        last_end_ms = int(state.get("last_window_end_ms", 0))
        player_rows = sorted(
            by_player[pid],
            key=lambda r_row: (_window_end_ms(r_row) or 0),
        )
        any_anomaly = False
        last_anomaly_info = None
        progressed = False
        for row in player_rows:
            win_end_ms = _window_end_ms(row)
            if win_end_ms is not None and win_end_ms <= last_end_ms:
                continue  # already processed (replay or overlapping batch)
            batch_mean = float(row["mean_accuracy"])
            state, anomaly, z = cusum_step(state, batch_mean)
            progressed = True
            if win_end_ms is not None:
                last_end_ms = win_end_ms
                state["last_window_end_ms"] = last_end_ms
            if anomaly:
                any_anomaly = True
                last_anomaly_info = (batch_mean, z, dict(state))

        if not progressed:
            continue

        mapping = {
            "state_version": BEHAVIOR_STATE_VERSION,
            "n": str(state["n"]),
            "mean": f"{state['mean']:.6f}",
            "m2": f"{state['m2']:.6f}",
            "s_pos": f"{state['s_pos']:.4f}",
            "s_neg": f"{state['s_neg']:.4f}",
            "anomalies": str(state["anomalies"]),
        }
        if "last_window_end_ms" in state:
            mapping["last_window_end_ms"] = str(state["last_window_end_ms"])
        pipe.hset(f"behavior:cusum:{pid}", mapping=mapping)

        if any_anomaly and last_anomaly_info is not None:
            batch_mean, z, snap_state = last_anomaly_info
            info = {
                "player_id": pid,
                "anomaly": "1",
                "anomalies": str(state["anomalies"]),
                "last_batch_mean": f"{batch_mean:.4f}",
                "last_z": f"{z:.2f}",
                "baseline_mean": f"{snap_state['mean']:.4f}",
                "baseline_n": str(snap_state["n"]),
                "detected_at": str(int(time.time())),
            }
            pipe.hset(f"player:behavior:{pid}", mapping=info)
            pipe.expire(f"player:behavior:{pid}", BEHAVIOR_FLAG_TTL_SECONDS)
            pipe.sadd("behavior:anomalies", pid)
            pipe.zadd(BEHAVIOR_EXPIRY_KEY,
                      {pid: now + BEHAVIOR_FLAG_TTL_SECONDS})
            alerts.emit(make_alert(
                "BEHAVIOR_ANOMALY", "WARNING", "PLAYER", pid,
                f"CUSUM detected sustained accuracy shift for {pid} "
                f"(z={z:.1f}, batch acc {batch_mean:.2f})",
                info,
            ))
        elif not flag_alive:
            pipe.srem("behavior:anomalies", pid)
            pipe.zrem(BEHAVIOR_EXPIRY_KEY, pid)
    alerts.stage(pipe)
    pipe.execute()


def _write_behavior_steps(steps):
    r = redis_client()
    alerts = AlertBatch(r)
    update_behavior(r, alerts, steps)
    alerts.flush()


def write_behavior_batch(batch_df, batch_id):
    # Fully consume even empty output batches so Spark can commit state updates.
    # Also drain durable alert intents when a replay advances no Redis cursors.
    steps = batch_df.collect()
    run_with_retry(f"behavior batch {batch_id}", lambda: _write_behavior_steps(steps))


def write_behavior_archive(batch_df, batch_id):
    with cached_batch(batch_df):
        if not batch_df.isEmpty():
            # Historical skill, weapon and peak jobs require raw shots, not steps.
            safe_parquet_archive(batch_df, "behavior_change", batch_id)


def start_queries(spark):
    parsed = parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay")
    steps_query = player_steps(parsed).writeStream \
        .outputMode("append") \
        .foreachBatch(write_behavior_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "behavior_change_steps_v2")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    # Retain the original stateless checkpoint's offsets for the raw archive;
    # the new aggregation must not reuse its incompatible checkpoint plan.
    archive_query = parsed.filter(F.col("event_type") == SHOT_EVENT).writeStream \
        .outputMode("append") \
        .foreachBatch(write_behavior_archive) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "behavior_change")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    return [steps_query, archive_query]
