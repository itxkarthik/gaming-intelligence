"""Player Behavior Change Detection (CUSUM over Redis-backed state).

Tracks each player's shot-accuracy baseline with Welford's online algorithm
and a two-sided CUSUM on the z-score; sustained deviation beyond the decision
threshold raises a BEHAVIOR_ANOMALY. The CUSUM state lives in Redis
(`behavior:cusum:{id}`) so restarts don't lose cumulative history.

One micro-batch is one CUSUM step per player: the mean accuracy of that
player's shots in the batch. A step needs MIN_STEP_SHOTS shots; smaller
samples are too noisy to count as evidence and are skipped, not carried
over. At ~1 shot/s per player and a 5 s trigger a step averages ~5 shots.

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

from pyspark.sql import functions as F

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import kafka_source, redis_client, warn
from src.common.schemas import SHOT_EVENT, parse_stream
from src.common.sinks import safe_parquet_archive

CUSUM_K = 0.5     # slack allowance per step (in sigmas)
CUSUM_H = 8.0     # decision threshold (sigmas of cumulative evidence)
WARMUP_STEPS = 10 # baseline-only steps before the detector judges
BEHAVIOR_BOOST = 0.15  # suspicion add-on applied by cheat_detection
MIN_STEP_SHOTS = 3
BEHAVIOR_FLAG_TTL_SECONDS = 600


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

    # Welford update of the baseline
    n1 = n + 1
    prev_mean = float(state.get("mean", 0.0)) if n else float(x)
    mean = prev_mean + (float(x) - prev_mean) / n1
    m2 = float(state.get("m2", 0.0)) + (float(x) - mean) * (float(x) - prev_mean)

    new_state = {
        "n": n1,
        "mean": mean,
        "m2": m2,
        "s_pos": 0.0 if anomaly else s_pos,   # reset after a detection
        "s_neg": 0.0 if anomaly else s_neg,
        "anomalies": int(state.get("anomalies", 0)) + (1 if anomaly else 0),
    }
    return new_state, anomaly, z


def apply_behavior_boost(suspicion, behavior_flag):
    """cheat_detection blends this cross-job signal into its suspicion score."""
    boosted = float(suspicion) + (BEHAVIOR_BOOST if behavior_flag else 0.0)
    return round(min(1.0, boosted), 4)


def load_state(raw):
    if not raw:
        return {"n": 0, "mean": 0.0, "m2": 0.0, "s_pos": 0.0, "s_neg": 0.0, "anomalies": 0}
    return {
        "n": int(raw.get("n", 0)),
        "mean": float(raw.get("mean", 0.0)),
        "m2": float(raw.get("m2", 0.0)),
        "s_pos": float(raw.get("s_pos", 0.0)),
        "s_neg": float(raw.get("s_neg", 0.0)),
        "anomalies": int(raw.get("anomalies", 0)),
    }


def player_steps(batch_df):
    """Per-player CUSUM step inputs for one micro-batch, aggregated in Spark."""
    return batch_df \
        .filter((F.col("event_type") == SHOT_EVENT) & F.col("accuracy").isNotNull()) \
        .groupBy("player_id") \
        .agg(F.count(F.lit(1)).alias("shots"), F.avg("accuracy").alias("mean_accuracy")) \
        .filter(F.col("shots") >= MIN_STEP_SHOTS)


def update_behavior(r, alerts, steps):
    """Advance each player's CUSUM by one step; raise, refresh or clear flags.

    steps: rows with player_id, shots, mean_accuracy (see player_steps).
    """
    pipe = r.pipeline()
    for row in steps:
        pipe.hgetall(f"behavior:cusum:{row['player_id']}")
        pipe.exists(f"player:behavior:{row['player_id']}")
    reads = pipe.execute()

    pipe = r.pipeline()
    for row, raw, flag_alive in zip(steps, reads[::2], reads[1::2]):
        player_id = row["player_id"]
        batch_mean = float(row["mean_accuracy"])
        new_state, anomaly, z = cusum_step(load_state(raw), batch_mean)
        pipe.hset(f"behavior:cusum:{player_id}", mapping={
            "n": str(new_state["n"]),
            "mean": f"{new_state['mean']:.6f}",
            "m2": f"{new_state['m2']:.6f}",
            "s_pos": f"{new_state['s_pos']:.4f}",
            "s_neg": f"{new_state['s_neg']:.4f}",
            "anomalies": str(new_state["anomalies"]),
        })

        if anomaly:
            info = {
                "player_id": player_id,
                "anomaly": "1",
                "anomalies": str(new_state["anomalies"]),
                "last_batch_mean": f"{batch_mean:.4f}",
                "last_z": f"{z:.2f}",
                "baseline_mean": f"{new_state['mean']:.4f}",
                "baseline_n": str(new_state["n"]),
                "detected_at": str(int(time.time())),
            }
            pipe.hset(f"player:behavior:{player_id}", mapping=info)
            pipe.expire(f"player:behavior:{player_id}", BEHAVIOR_FLAG_TTL_SECONDS)
            pipe.sadd("behavior:anomalies", player_id)
            alerts.emit(make_alert(
                "BEHAVIOR_ANOMALY", "WARNING", "PLAYER", player_id,
                f"CUSUM detected sustained accuracy shift for {player_id} "
                f"(z={z:.1f}, batch acc {batch_mean:.2f})",
                info,
            ))
        elif not flag_alive:
            pipe.srem("behavior:anomalies", player_id)
    pipe.execute()


def write_behavior_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        r = redis_client()
        alerts = AlertBatch(r)
        update_behavior(r, alerts, player_steps(batch_df).collect())
        alerts.flush(batch_df.sparkSession)
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"behavior batch {batch_id}: {e}")
    # Raw SHOT rows: the batch jobs (skill, weapon, peak) read this archive.
    safe_parquet_archive(batch_df, "behavior_change", batch_id)


def start_queries(spark):
    parsed = parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay")
    return parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(write_behavior_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "behavior_change")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
