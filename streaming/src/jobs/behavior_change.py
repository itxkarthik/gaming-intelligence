"""Player Behavior Change Detection (CUSUM over Redis-backed state).

Tracks each player's shot-accuracy baseline with Welford's online algorithm
and a two-sided CUSUM on the z-score; sustained deviation beyond the decision
threshold raises a BEHAVIOR_ANOMALY. The CUSUM state lives in Redis
(`behavior:cusum:{id}`) so restarts don't lose cumulative history — and the
flag written to `player:behavior:{id}` is read by cheat_detection to boost
its suspicion score (ROADMAP "Cross-job data flow").
"""

import sys
import os
import math
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    KAFKA_STARTING_OFFSETS,
    REDIS_HOST,
    REDIS_PORT,
    CHECKPOINT_DIR
)
from src.common.schemas import parse_gameplay_stream
from src.common.alerts import emit_alert, flush_alerts_to_kafka
from src.common.sinks import safe_parquet_archive

CUSUM_K = 0.5     # slack allowance per step (in sigmas)
CUSUM_H = 5.0     # decision threshold (sigmas of cumulative evidence)
DEFAULT_SD = 0.08 # accuracy population std used while the baseline is cold
BEHAVIOR_BOOST = 0.15  # suspicion add-on applied by cheat_detection


def cusum_step(state, x, k=CUSUM_K, h=CUSUM_H):
    """Advance one observation through the CUSUM detector.

    state: {"n", "mean", "m2", "s_pos", "s_neg", "anomalies"}
    Returns (new_state, anomaly: bool, z: float). Pure — unit-testable.
    """
    n = int(state.get("n", 0))
    if n == 0:
        return {"n": 1, "mean": float(x), "m2": 0.0,
                "s_pos": 0.0, "s_neg": 0.0, "anomalies": 0}, False, 0.0

    sd = math.sqrt(float(state["m2"]) / (n - 1)) if n > 1 else DEFAULT_SD
    sd = max(sd, 0.02)  # floor: a near-constant baseline must not explode z
    z = (float(x) - float(state["mean"])) / sd
    # Clip per-step evidence to ±4σ: one batch alone can contribute at most
    # 4 - k = 3.5 < h, so only a SUSTAINED shift can ever trip the detector.
    z = max(-4.0, min(4.0, z))

    s_pos = max(0.0, float(state["s_pos"]) + z - k)
    s_neg = min(0.0, float(state["s_neg"]) + z + k)
    anomaly = s_pos > h or s_neg < -h

    # Welford update of the baseline
    n1 = n + 1
    mean = float(state["mean"]) + (float(x) - float(state["mean"])) / n1
    m2 = float(state["m2"]) + (float(x) - mean) * (float(x) - float(state["mean"]))

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


def write_behavior_batch(batch_df, batch_id):
    """Update per-player CUSUM from this micro-batch's mean accuracy."""
    if batch_df.isEmpty():
        return

    import redis

    sums = defaultdict(lambda: [0.0, 0])
    for row in batch_df.collect():
        acc = row["accuracy"]
        if acc is None:
            continue
        s = sums[row["player_id"]]
        s[0] += float(acc)
        s[1] += 1

    if not sums:
        return

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        pending_alerts = []

        for player_id, (total, count) in sums.items():
            batch_mean = total / count
            state = load_state(r.hgetall(f"behavior:cusum:{player_id}"))
            new_state, anomaly, z = cusum_step(state, batch_mean)

            r.hset(f"behavior:cusum:{player_id}", mapping={
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
                    "last_batch_mean": f"{batch_mean:.4f}",
                    "last_z": f"{z:.2f}",
                    "baseline_mean": f"{new_state['mean']:.4f}",
                    "baseline_n": str(new_state["n"]),
                    "detected_at": str(int(time.time())),
                }
                r.hset(f"player:behavior:{player_id}", mapping=info)
                r.sadd("behavior:anomalies", player_id)
                alert_payload = {
                    "alert_id": f"behavior_{player_id}_{int(time.time())}",
                    "alert_type": "BEHAVIOR_ANOMALY",
                    "severity": "WARNING",
                    "entity_type": "PLAYER",
                    "entity_id": player_id,
                    "message": (f"CUSUM detected sustained accuracy shift for "
                                f"{player_id} (z={z:.1f}, batch acc {batch_mean:.2f})"),
                    "details": info,
                    "timestamp": int(time.time() * 1000),
                }
                emitted = emit_alert(r, alert_payload)
                if emitted:
                    pending_alerts.append(emitted)

        flush_alerts_to_kafka(batch_df.sparkSession, pending_alerts)
    except Exception as e:
        print(f"[WARN] behavior batch {batch_id}: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "behavior_change", batch_id)


def start_queries(spark):
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["gameplay"]) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load()

    query = parse_gameplay_stream(raw).writeStream \
        .outputMode("append") \
        .foreachBatch(write_behavior_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "behavior_change")) \
        .trigger(processingTime="5 seconds") \
        .start()
    return query


def main():
    spark = SparkSession.builder \
        .appName("Gaming-BehaviorChange") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topic: {KAFKA_TOPICS['gameplay']}")
    start_queries(spark)
    print("Behavior Change (CUSUM) streaming pipeline started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()