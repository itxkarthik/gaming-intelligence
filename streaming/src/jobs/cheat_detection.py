"""Cheat Detection Streaming Pipeline.

Ingests combat events from Kafka, aggregates player performance metrics over
tumbling and sliding windows, computes composite anomaly suspicion scores,
and writes live flags and alerts to Redis and Kafka.
"""

import sys
import os
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    REDIS_HOST,
    REDIS_PORT,
    CHECKPOINT_DIR
)
from src.common.schemas import parse_gameplay_stream
from src.common.alerts import emit_alert
from src.common.sinks import safe_parquet_archive
from src.jobs.behavior_change import apply_behavior_boost

IFOREST_THRESHOLD = -0.6  # score_samples below this = anomaly (offline-calibrated)
IFOREST_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "anti_cheat_isolation_forest.joblib")

_IFOREST = None
_IFOREST_TRIED = False


def load_iforest():
    """Load the OFFLINE-trained model once per process (never fit online)."""
    global _IFOREST, _IFOREST_TRIED
    if not _IFOREST_TRIED:
        _IFOREST_TRIED = True
        try:
            import joblib
            if os.path.exists(IFOREST_PATH):
                _IFOREST = joblib.load(IFOREST_PATH)
                print(f"[ML] IsolationForest loaded: {IFOREST_PATH}")
            else:
                print(f"[ML] No IsolationForest artifact at {IFOREST_PATH} — "
                      "run src/ml/train_isolation_forest.py to enable inference",
                      file=sys.stderr)
        except Exception as e:
            print(f"[WARN] IsolationForest load failed: {e}", file=sys.stderr)
    return _IFOREST


def score_iforest(model, rows):
    """Vectorized score_samples over batch rows; None entries when no model."""
    if model is None or not rows:
        return [None] * len(rows)
    import numpy as np
    X = np.array([
        [float(r["avg_accuracy"] or 0.0),
         float(r["headshot_ratio"] or 0.0),
         float(r["avg_reaction_time"] or 0.0)]
        for r in rows
    ])
    return [float(s) for s in model.score_samples(X)]


def build_suspicion_pipeline(parsed):
    """Combat-event aggregation + suspicion scoring.

    Pure transformation (no SparkSession creation, no sinks) so the scoring
    logic is unit-testable against synthetic DataFrames.

    Headshots are counted ONLY on KILL rows: DAMAGE and KILL rows both carry
    the is_headshot flag for the same shot, so counting them together while
    dividing by kill count produced ratios > 1.0 — a metric that cannot exist.
    """
    # Filter for combat events
    combat_events = parsed.filter(
        F.col("event_type").isin("KILL", "SHOT_FIRED", "HEADSHOT", "DAMAGE")
    )

    # 30-second window with 10-second watermark
    aggregated = combat_events \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(
            F.window("event_timestamp", "30 seconds", "15 seconds"),
            "player_id",
            "match_id"
        ) \
        .agg(
            F.sum(F.when(F.col("event_type") == "SHOT_FIRED", 1).otherwise(0)).alias("total_shots"),
            F.sum(F.when(F.col("event_type") == "KILL", 1).otherwise(0)).alias("total_kills"),
            F.sum(
                F.when((F.col("event_type") == "KILL") & (F.col("is_headshot") == True), 1)
                 .otherwise(0)
            ).alias("headshots"),
            F.avg("accuracy").alias("avg_accuracy"),
            F.avg("reaction_time_ms").alias("avg_reaction_time")
        ) \
        .withColumn(
            "headshot_ratio",
            F.when(F.col("total_kills") > 0, F.col("headshots") / F.col("total_kills")).otherwise(0.0)
        )

    # Suspicion Scoring Heuristics
    # Accuracy anomaly: Normal <= 0.40; Suspicious > 0.70
    acc_score = F.when(
        F.col("avg_accuracy") > 0.70,
        F.least(F.lit(1.0), (F.col("avg_accuracy") - 0.50) / 0.45)
    ).otherwise(0.0)

    # Headshot ratio anomaly: Normal <= 0.30; Suspicious > 0.65
    hs_score = F.when(
        F.col("headshot_ratio") > 0.65,
        F.least(F.lit(1.0), (F.col("headshot_ratio") - 0.35) / 0.55)
    ).otherwise(0.0)

    # Inhuman reaction time anomaly: Normal >= 200ms; Inhuman < 120ms
    rxn_score = F.when(
        (F.col("avg_reaction_time") > 0) & (F.col("avg_reaction_time") < 140),
        F.least(F.lit(1.0), (F.lit(140.0) - F.col("avg_reaction_time")) / 100.0)
    ).otherwise(0.0)

    return aggregated \
        .withColumn("acc_score", acc_score) \
        .withColumn("hs_score", hs_score) \
        .withColumn("rxn_score", rxn_score) \
        .withColumn(
            "suspicion_score",
            F.least(F.lit(1.0), (F.col("acc_score") * 0.45) + (F.col("hs_score") * 0.40) + (F.col("rxn_score") * 0.15))
        )


def write_player_scores_to_redis(batch_df, batch_id):
    """ForeachBatch sink to update player suspicion profiles and trigger alerts."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        rows = batch_df.collect()
        model = load_iforest()
        iforest_scores = score_iforest(model, rows)

        for row, iforest_score in zip(rows, iforest_scores):
            player_id = row["player_id"]
            match_id = row["match_id"]
            suspicion = round(float(row["suspicion_score"]), 3)
            total_shots = int(row["total_shots"])
            total_kills = int(row["total_kills"])
            hs_ratio = round(float(row["headshot_ratio"]), 2)
            avg_acc = round(float(row["avg_accuracy"] or 0.0), 2)
            avg_rxn = round(float(row["avg_reaction_time"] or 0.0), 1)

            # Cross-job signal: behavior_change's CUSUM flag boosts suspicion
            behavior_anomaly = r.hget(f"player:behavior:{player_id}", "anomaly") == "1"
            suspicion_eff = apply_behavior_boost(suspicion, behavior_anomaly)
            iforest_flag = (iforest_score is not None
                            and iforest_score < IFOREST_THRESHOLD)
            flagged = ((suspicion_eff >= 0.70 or iforest_flag) and total_shots >= 4)

            profile_data = {
                "player_id": player_id,
                "match_id": match_id,
                "suspicion_score": str(suspicion),
                "suspicion_effective": str(suspicion_eff),
                "behavior_anomaly": "true" if behavior_anomaly else "false",
                "iforest_score": "" if iforest_score is None else f"{iforest_score:.4f}",
                "iforest_flag": "true" if iforest_flag else "false",
                "total_shots": str(total_shots),
                "total_kills": str(total_kills),
                "headshot_ratio": str(hs_ratio),
                "avg_accuracy": str(avg_acc),
                "avg_reaction_time_ms": str(avg_rxn),
                "flagged": "true" if flagged else "false",
                "updated_at": str(int(time.time()))
            }

            r.hset(f"player:{player_id}", mapping=profile_data)

            # Emit alert if high confidence anomaly (deduped per TTL window;
            # emit_alert fans out to Redis + Kafka for the alert engine)
            if flagged:
                r.sadd("players:flagged", player_id)
                alert_payload = {
                    "alert_id": f"cheat_{player_id}_{int(time.time())}",
                    "alert_type": "CHEAT_DETECTED",
                    "severity": "CRITICAL" if max(suspicion_eff, 1.0 if iforest_flag else 0.0) >= 0.85 else "WARNING",
                    "entity_type": "PLAYER",
                    "entity_id": player_id,
                    "message": (f"Cheat anomaly for {player_id} in {match_id}: "
                                f"suspicion {suspicion_eff*100:.1f}%"
                                f"{' + CUSUM behavior' if behavior_anomaly else ''}"
                                f"{' + IsolationForest' if iforest_flag else ''}"),
                    "details": profile_data,
                    "timestamp": int(time.time() * 1000)
                }
                emit_alert(r, batch_df.sparkSession, alert_payload)
    except Exception as e:
        print(f"[WARN] Error in cheat detection batch {batch_id}: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "cheat_detection", batch_id)


def main():
    spark = SparkSession.builder \
        .appName("Gaming-CheatDetection") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topic: {KAFKA_TOPICS['gameplay']}")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["gameplay"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = parse_gameplay_stream(raw_stream)
    with_suspicion = build_suspicion_pipeline(parsed)

    # Console display for flagged players
    console_query = with_suspicion \
        .filter(F.col("suspicion_score") >= 0.50) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    # Redis state sink
    redis_query = with_suspicion \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(write_player_scores_to_redis) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "cheat_detection")) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("Cheat Detection streaming pipeline started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
