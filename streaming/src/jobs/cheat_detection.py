"""Cheat Detection Streaming Pipeline.

Aggregates SHOT events per player over 30-second sliding windows, scores
them (heuristic suspicion + offline-trained IsolationForest + the CUSUM
behavior boost) and writes live flags and alerts to Redis and Kafka. The
same per-window rows feed smurf_detection's evaluation, and the enriched
online verdicts are archived to Parquet so batch evaluation reflects the
true online detector.
"""

import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import (
    cached_batch,
    kafka_source,
    redis_client,
    run_with_retry,
    spark_session,
)
from src.common.schemas import SHOT_EVENT, parse_stream
from src.common.sinks import safe_parquet_archive

PLAYER_STATE_TTL_SECONDS = 30 * 24 * 60 * 60
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
FOREST_MAX_GAP_SECONDS = 30
# Source-event bounds add aggregation state; never resume the pre-bounds schema.
CHEAT_CHECKPOINT = "cheat_detection_v2"

VERDICT_SCHEMA = StructType([
    StructField("player_id", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("window_end_ms", LongType(), False),
    StructField("suspicion_effective", DoubleType(), False),
    StructField("iforest_score", DoubleType(), True),
    StructField("iforest_flag", BooleanType(), False),
    StructField("behavior_anomaly", BooleanType(), False),
    StructField("smurf_flagged", BooleanType(), False),
    StructField("flagged", BooleanType(), False),
    StructField("critical", BooleanType(), False),
    StructField("judged", BooleanType(), False),
])


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
            F.min("event_time").alias("source_event_min"),
            F.max("event_time").alias("source_event_max"),
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


def forest_persistence(since, iforest_flag, now, last_judged=None, same_context=True):
    """(new_since, persistent) for consecutive judged outlier windows.

    Repeated updates to the same window preserve but cannot extend a streak.
    Missing evidence, backwards time, gaps beyond one slide, and match/run
    resets all start over. A clean or insufficient-shot window clears it.
    """
    if not iforest_flag:
        return None, False
    if (since is None or last_judged is None or not same_context
            or now < last_judged or now < since
            or now - last_judged > FOREST_MAX_GAP_SECONDS):
        since = now
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


def decision_cache_key(checkpoint_identity, batch_id):
    """Namespace batch IDs by Spark's checkpoint generation, not its path."""
    digest = hashlib.sha256(checkpoint_identity.encode("utf-8")).hexdigest()
    return f"cheat:decisions:{digest}:{batch_id}"


def checkpoint_identity(spark):
    """Spark's persisted query UUID changes when its checkpoint is reset."""
    path = os.path.join(CHECKPOINT_DIR, CHEAT_CHECKPOINT, "metadata")
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    stream = fs.open(hadoop_path)
    scanner = jvm.java.util.Scanner(stream, "UTF-8").useDelimiter(r"\A")
    try:
        return str(json.loads(scanner.next())["id"])
    finally:
        scanner.close()


def write_player_scores(r, spark, rows, checkpoint_id=None, batch_id=None):
    """Score each player's best window; update profiles, flags and alerts.

    Returns the list of enriched verdict dicts (one per judged/evaluated player
    window) so the Parquet archive can persist the online verdicts.
    """
    alerts = AlertBatch(r)
    cache_key = decision_cache_key(checkpoint_id, batch_id) if checkpoint_id is not None else None
    previous_batch = None
    if cache_key is not None:
        current_key = decision_cache_key(checkpoint_id, "current")
        current = r.get(current_key)
        previous_batch = int(current) if current is not None else None
        if previous_batch is not None and previous_batch > batch_id:
            raise RuntimeError("Cannot replay a batch older than the retained cheat decisions")
        cached = r.get(cache_key)
        if cached is not None:
            alerts.flush()
            return json.loads(cached)["verdicts"]
        if previous_batch == batch_id:
            raise RuntimeError("Retained cheat decisions are missing; refusing to recompute")
    rows = best_window_per_player(rows)
    iforest_scores = score_rows(load_model(), rows)
    evaluate_smurfs(r, alerts, rows)  # first: the cheat verdict reads it

    pipe = r.pipeline()
    for row in rows:
        pipe.hget(f"player:behavior:{row['player_id']}", "anomaly")
        pipe.hget(f"player:smurf:{row['player_id']}", "status")
        pipe.hgetall(f"player:cheat:evidence:{row['player_id']}")
        pipe.hget(f"player:account:{row['player_id']}", "login_at")
    reads = pipe.execute()

    verdicts = []
    pipe = r.pipeline()
    for row, iforest_score, behavior, smurf_status, evidence, login_at in zip(
            rows, iforest_scores, reads[::4], reads[1::4], reads[2::4], reads[3::4]):
        player_id = row["player_id"]
        behavior_anomaly = behavior == "1"
        is_smurf = smurf_status == "SMURF"
        suspicion = round(float(row["suspicion_score"]), 3)
        suspicion_eff = apply_behavior_boost(suspicion, behavior_anomaly)
        raw_iforest_flag = iforest_score is not None and iforest_score < IFOREST_THRESHOLD
        judged = int(row["total_shots"]) >= MIN_SHOTS
        window_end_ms = int(row["window_end_ms"])
        evidence_key = f"player:cheat:evidence:{player_id}"
        run_id = json.dumps([checkpoint_id, login_at])

        if judged:
            win_ts = window_end_ms / 1000.0
            since = evidence.get("since")
            last_judged = evidence.get("last_judged_window_end_ms")
            same_context = (evidence.get("match_id") == row["match_id"]
                            and evidence.get("run_id") == run_id)
            since, persistent = forest_persistence(
                float(since) if since else None, raw_iforest_flag, win_ts,
                float(last_judged) / 1000.0 if last_judged else None, same_context)
            pipe.hset(evidence_key, mapping={
                "since": str(since) if since is not None else "",
                "last_judged_window_end_ms": str(window_end_ms),
                "match_id": row["match_id"],
                "run_id": run_id,
            })
            pipe.expire(evidence_key, PLAYER_STATE_TTL_SECONDS)
            flagged, critical = cheat_verdict(suspicion_eff, persistent, is_smurf)
            profile = _profile(row, suspicion, suspicion_eff, behavior_anomaly,
                               iforest_score, persistent)
            profile["iforest_since"] = f"{since:.0f}" if since is not None else ""
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
            pipe.expire(f"player:{player_id}", PLAYER_STATE_TTL_SECONDS)
        else:
            persistent = False
            flagged = False
            critical = False
            # Break continuity without changing the last judged profile/flag.
            # Never-judged low-shot players still get no fabricated profile.
            if evidence:
                pipe.hset(evidence_key, mapping={"since": ""})

        verdicts.append({
            "player_id": player_id,
            "match_id": row["match_id"],
            "window_end_ms": window_end_ms,
            "suspicion_effective": float(suspicion_eff),
            "iforest_score": None if iforest_score is None else float(iforest_score),
            "iforest_flag": bool(persistent),
            "behavior_anomaly": bool(behavior_anomaly),
            "smurf_flagged": bool(is_smurf),
            "flagged": bool(flagged if judged else False),
            "critical": bool(critical),
            "judged": bool(judged),
        })
    if cache_key is not None:
        pipe.set(cache_key, json.dumps({"batch_id": batch_id, "verdicts": verdicts}))
        pipe.set(current_key, str(batch_id))
        if previous_batch is not None:
            pipe.delete(decision_cache_key(checkpoint_id, previous_batch))
    alerts.stage(pipe)
    pipe.execute()
    alerts.flush()
    return verdicts


def _enrich_batch_df(batch_df, verdicts):
    """Join driver-computed online verdicts back onto `batch_df` for archival."""
    spark = batch_df.sparkSession
    verdict_df = spark.createDataFrame(verdicts, schema=VERDICT_SCHEMA)
    return (
        batch_df.withColumn("window_end_ms", F.unix_millis(F.col("window.end")))
        .join(verdict_df, on=["player_id", "match_id", "window_end_ms"], how="left")
        .withColumn(
            "suspicion_effective",
            F.coalesce(F.col("suspicion_effective"), F.col("suspicion_score")),
        )
        .withColumn("flagged", F.coalesce(F.col("flagged"), F.lit(False)))
        .withColumn("critical", F.coalesce(F.col("critical"), F.lit(False)))
        .withColumn("iforest_flag", F.coalesce(F.col("iforest_flag"), F.lit(False)))
        .withColumn("judged", F.coalesce(F.col("judged"), F.lit(False)))
    )


def write_player_scores_batch(batch_df, batch_id):
    with cached_batch(batch_df):
        if batch_df.isEmpty():
            return
        batch_df = batch_df.withColumn("window_end_ms", F.unix_millis(F.col("window.end")))
        rows = batch_df.collect()
        checkpoint_id = checkpoint_identity(batch_df.sparkSession)
        verdicts = run_with_retry(
            f"cheat detection batch {batch_id}",
            lambda: write_player_scores(
                redis_client(), batch_df.sparkSession, rows, checkpoint_id, batch_id),
        )
        enriched_df = _enrich_batch_df(batch_df, verdicts)
        safe_parquet_archive(enriched_df, "cheat_detection", batch_id)


def main():
    spark = spark_session("Gaming-CheatDetection")
    parsed = parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay")
    build_suspicion_pipeline(parsed).writeStream \
        .outputMode("update") \
        .foreachBatch(write_player_scores_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, CHEAT_CHECKPOINT)) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
