"""Cheat Detection Streaming Pipeline.

Aggregates SHOT events per player over 30-second sliding windows, scores
them (heuristic suspicion + offline-trained IsolationForest + the CUSUM
behavior boost) and writes live flags and alerts to Redis and Kafka. The
same per-window rows feed smurf_detection's evaluation.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import kafka_source, redis_client, spark_session, warn
from src.common.schemas import SHOT_EVENT, parse_stream
from src.common.sinks import safe_parquet_archive
from src.jobs.behavior_change import apply_behavior_boost
from src.jobs.smurf_detection import evaluate_smurfs
from src.ml.iforest import (
    IFOREST_THRESHOLD,
    MIN_SHOTS,
    load_model,
    score_rows,
    smoothed_headshot_ratio,
)

FLAG_THRESHOLD = 0.70     # effective suspicion that flags a player
CRITICAL_THRESHOLD = 0.85
# An IsolationForest outlier must persist this long before it counts: one
# lucky 30 s window trips the model, a cheater stays an outlier window after window.
FOREST_PERSIST_SECONDS = 30


def _ramp(col, zero, full):
    """0 up to `zero`, linear to 1 at `full` (full < zero ramps downward), 0 for null.

    The gate IS the zero point, so a score never jumps.
    """
    x = (F.col(col) - zero) / (full - zero)
    return F.coalesce(F.when(x > 0, F.least(F.lit(1.0), x)), F.lit(0.0))


def build_suspicion_pipeline(parsed):
    """SHOT events -> per-window features and suspicion score.

    Pure transformation (no session, no sinks) so it is unit-testable. Each
    SHOT row is one trigger pull with its outcome inline, so accuracy and
    reaction time average over shots exactly once, and headshot_ratio is
    headshots per HIT: the scale of the profiles' headshot_ratio and of the
    IsolationForest features, shrunk toward the population rate so a
    single headshot from a single hit does not read as 100%.
    """
    hit = F.coalesce(F.col("hit"), F.lit(False))
    aggregated = parsed \
        .filter(F.col("event_type") == SHOT_EVENT) \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(
            F.window("event_timestamp", "30 seconds", "15 seconds"),
            "player_id",
            "match_id",
        ) \
        .agg(
            F.count(F.lit(1)).alias("total_shots"),
            F.sum(hit.cast("int")).alias("total_hits"),
            F.sum(F.coalesce(F.col("is_kill"), F.lit(False)).cast("int")).alias("total_kills"),
            F.sum((hit & F.col("is_headshot")).cast("int")).alias("headshots"),
            F.avg("accuracy").alias("avg_accuracy"),
            F.avg("reaction_time_ms").alias("avg_reaction_time"),
        ) \
        .withColumn("headshot_ratio",
                    smoothed_headshot_ratio(F.col("headshots"), F.col("total_hits")))

    # Per-window profile means (simulator/profiles): accuracy normals 0.18-0.42,
    # aimbot 0.93; headshots per hit normals 0.08-0.28 (smurf 0.35), wallhack
    # 0.20, aimbot 0.88; reaction normals 190-380 ms, wallhack 120, aimbot 85.
    return aggregated \
        .withColumn("acc_score", _ramp("avg_accuracy", 0.50, 0.95)) \
        .withColumn("hs_score", _ramp("headshot_ratio", 0.40, 0.80)) \
        .withColumn("rxn_score", _ramp("avg_reaction_time", 140.0, 40.0)) \
        .withColumn(
            "suspicion_score",
            F.col("acc_score") * 0.45 + F.col("hs_score") * 0.40 + F.col("rxn_score") * 0.15,
        )


def _profile(row, suspicion, suspicion_eff, behavior_anomaly, iforest_score, iforest_flag):
    return {
        "player_id": row["player_id"],
        "match_id": row["match_id"],
        "suspicion_score": str(suspicion),
        "suspicion_effective": str(suspicion_eff),
        "behavior_anomaly": "true" if behavior_anomaly else "false",
        "iforest_score": "" if iforest_score is None else f"{iforest_score:.4f}",
        "iforest_flag": "true" if iforest_flag else "false",
        "total_shots": str(int(row["total_shots"])),
        "total_kills": str(int(row["total_kills"])),
        "headshot_ratio": f"{float(row['headshot_ratio']):.2f}",
        "avg_accuracy": f"{float(row['avg_accuracy'] or 0.0):.2f}",
        "avg_reaction_time_ms": f"{float(row['avg_reaction_time'] or 0.0):.1f}",
        "updated_at": str(int(time.time())),
    }


def best_window_per_player(rows):
    """One row per player: the window with the most shots (newest on ties).

    Sliding windows overlap, and the newest one is the youngest and
    smallest; judging it would mean judging on the least evidence.
    """
    best = {}
    for row in rows:
        rank = (int(row["total_shots"]), row["window"]["end"])
        current = best.get(row["player_id"])
        if current is None or rank > current[0]:
            best[row["player_id"]] = (rank, row)
    return [row for _, row in best.values()]


def forest_persistence(since, iforest_flag, now):
    """(new_since, persistent) for one judged window. Pure, unit-testable.

    `since` is when the model first flagged the player in the current
    unbroken run of flagged windows (None when the last judged window was
    clean). A clean window resets it.
    """
    if not iforest_flag:
        return None, False
    since = now if since is None else since
    return since, now - since >= FOREST_PERSIST_SECONDS


def cheat_verdict(suspicion_eff, iforest_flag, smurf):
    """(flagged, critical) for one judged window. Pure, unit-testable.

    `iforest_flag` is the PERSISTENT model flag (see forest_persistence).
    The heuristic flags on its own. An IsolationForest-only flag on a
    confirmed smurf is explained by the smurf verdict (a fresh account
    playing far above its rank IS a statistical outlier) and is left to
    the SMURF_DETECTED alert; a smurf who also trips the heuristic is still
    flagged as a cheat.
    """
    heuristic = suspicion_eff >= FLAG_THRESHOLD
    forest = iforest_flag and not smurf
    flagged = heuristic or forest
    critical = flagged and (forest or suspicion_eff >= CRITICAL_THRESHOLD)
    return flagged, critical


def write_player_scores(r, spark, rows):
    """Score each player's best window; update profiles, flags and alerts."""
    rows = best_window_per_player(rows)
    alerts = AlertBatch(r)
    iforest_scores = score_rows(load_model(), rows)
    evaluate_smurfs(r, alerts, rows)  # first: the cheat verdict reads it

    pipe = r.pipeline()
    for row in rows:
        pipe.hget(f"player:behavior:{row['player_id']}", "anomaly")
        pipe.hget(f"player:smurf:{row['player_id']}", "status")
        pipe.hget(f"player:{row['player_id']}", "iforest_since")
    reads = pipe.execute()
    now = time.time()

    pipe = r.pipeline()
    for row, iforest_score, behavior, smurf_status, since in zip(
            rows, iforest_scores, reads[::3], reads[1::3], reads[2::3]):
        player_id = row["player_id"]
        behavior_anomaly = behavior == "1"
        suspicion = round(float(row["suspicion_score"]), 3)
        suspicion_eff = apply_behavior_boost(suspicion, behavior_anomaly)
        iforest_flag = iforest_score is not None and iforest_score < IFOREST_THRESHOLD
        profile = _profile(row, suspicion, suspicion_eff, behavior_anomaly,
                           iforest_score, iforest_flag)

        # Too few shots to judge: refresh the stats, keep the previous verdict.
        if int(row["total_shots"]) >= MIN_SHOTS:
            since, persistent = forest_persistence(
                float(since) if since else None, iforest_flag, now)
            profile["iforest_since"] = f"{since:.0f}" if since is not None else ""
            flagged, critical = cheat_verdict(suspicion_eff, persistent,
                                              smurf_status == "SMURF")
            profile["flagged"] = "true" if flagged else "false"
            if flagged:
                pipe.sadd("players:flagged", player_id)
                alerts.emit(make_alert(
                    "CHEAT_DETECTED", "CRITICAL" if critical else "WARNING", "PLAYER", player_id,
                    f"Cheat anomaly for {player_id} in {row['match_id']}: "
                    f"suspicion {suspicion_eff * 100:.1f}%"
                    f"{' + CUSUM behavior' if behavior_anomaly else ''}"
                    f"{' + IsolationForest' if persistent else ''}",
                    profile,
                ))
            else:
                pipe.srem("players:flagged", player_id)
        pipe.hset(f"player:{player_id}", mapping=profile)
    pipe.execute()
    alerts.flush()


def write_player_scores_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        write_player_scores(redis_client(), batch_df.sparkSession, batch_df.collect())
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"cheat detection batch {batch_id}: {e}")
    safe_parquet_archive(batch_df, "cheat_detection", batch_id)


def main():
    spark = spark_session("Gaming-CheatDetection")
    parsed = parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay")
    build_suspicion_pipeline(parsed).writeStream \
        .outputMode("update") \
        .foreachBatch(write_player_scores_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "cheat_detection")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
