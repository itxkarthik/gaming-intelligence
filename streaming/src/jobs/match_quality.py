"""Match Quality Scoring Streaming Pipeline.

Joins combat (gameplay_events kills) with the match lifecycle (player_events)
per match using session windows, computes a composite 0-100 quality score,
and serves it from Redis plus a Parquet archive.

Quality model:
    score  = 100
            - 35 * kill_imbalance          (|kills_a - kills_b| / total, 0..1)
            - 15 * (1 - kill_entropy)      (2-team Shannon entropy in bits, 0..1)
            - 15 * (skill_imbalance / 2)   (avg tier delta, tiers in [1..3])
            -  5 * disconnects             (distinct players who dropped)
            - duration penalty             (20 * (1 - dur/180) when dur < 180 s)
    status = BALANCED (>=70) / UNBALANCED (>=40) / STOMPED (<40)

A session closes SESSION_GAP_SECONDS after a match's last event, so a match
is scored once, shortly after it ends (append mode).
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import kafka_source, redis_client, spark_session, warn
from src.common.schemas import parse_stream
from src.common.sinks import safe_parquet_archive

SESSION_GAP_SECONDS = 120
SESSION_GAP = f"{SESSION_GAP_SECONDS} seconds"
FULL_MATCH_SECONDS = 180

# skill_tier label -> numeric rank used for team imbalance
TIER_VALUES = {
    "bronze": 1.0,
    "silver": 1.5,   # cheater profiles sit between bronze and gold
    "gold": 2.0,
    "diamond": 3.0,
}


def _tier_value(label):
    expr = F.lit(None).cast("double")
    for tier, value in TIER_VALUES.items():
        expr = F.when(label == tier, value).otherwise(expr)
    return expr


def _team_tier(team):
    return F.avg(F.when(
        (F.col("event_type") == "MATCH_JOIN") & (F.col("metadata")["team_id"] == team),
        _tier_value(F.col("metadata")["skill_tier"]),
    ))


def build_match_quality_pipeline(gameplay_parsed, player_parsed):
    """Two parsed streams -> scored match-quality DataFrame.

    Pure transformation (no session, no sinks) so it is unit-testable on
    batch DataFrames as well as live streams.

    Both sides aggregate into session windows, then inner-join on match_id
    with a window-overlap constraint. The player side takes EVERY event of
    the match (joins, purchases, disconnects, MATCH_END): purchases keep its
    session continuous, so a late disconnect cannot open a second session
    that would join the same combat session twice.
    """
    combat_stats = gameplay_parsed \
        .filter(F.col("is_kill")) \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(F.session_window("event_timestamp", SESSION_GAP), "match_id") \
        .agg(
            F.sum(F.when(F.col("team_id") == "team_a", 1).otherwise(0)).alias("kills_a"),
            F.sum(F.when(F.col("team_id") == "team_b", 1).otherwise(0)).alias("kills_b"),
            F.count(F.lit(1)).alias("total_kills"),
        )

    # countDistinct is unsupported in streaming aggregations; a set is exact.
    # size() is INT; cast to the BIGINT the Parquet archive has always stored
    # (mergeSchema cannot merge INT with BIGINT).
    dropped = F.when(F.col("event_type") == "DISCONNECT", F.col("player_id"))
    player_stats = player_parsed \
        .filter(F.col("match_id").isNotNull()) \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(F.session_window("event_timestamp", SESSION_GAP), "match_id") \
        .agg(
            F.size(F.collect_set(dropped)).cast("long").alias("disconnects"),
            _team_tier("team_a").alias("tier_a"),
            _team_tier("team_b").alias("tier_b"),
        )

    # KNOWN SPARK LIMITATION: StreamingJoinHelper cannot extract a state
    # watermark from SESSION window attributes (verified on 3.5.1), so the
    # join state store is not watermark-trimmed and logs a WARN per batch.
    # State grows with distinct matches seen; acceptable at this scale.
    joined = combat_stats.alias("c").join(
        player_stats.alias("p"),
        (F.col("c.match_id") == F.col("p.match_id"))
        & (F.col("c.session_window.start") <= F.col("p.session_window.end"))
        & (F.col("p.session_window.start") <= F.col("c.session_window.end")),
        "inner",
    )

    # Match length: first to last lifecycle event (join .. MATCH_END).
    duration = F.greatest(
        F.unix_timestamp(F.col("p.session_window.end"))
        - F.unix_timestamp(F.col("p.session_window.start"))
        - F.lit(SESSION_GAP_SECONDS),
        F.lit(0),
    )

    # Every combat row carries at least one kill (inner join on kill sessions).
    total = F.col("total_kills")
    pa = F.col("kills_a") / total
    pb = F.col("kills_b") / total
    scored = joined.select(
        F.col("c.match_id").alias("match_id"),
        F.col("c.session_window").alias("session_window"),
        "kills_a", "kills_b", "total_kills", "disconnects",
        (F.abs(F.col("kills_a") - F.col("kills_b")) / total).alias("kill_imbalance"),
        (-(F.when(pa > 0, pa * F.log2(pa)).otherwise(0.0)
           + F.when(pb > 0, pb * F.log2(pb)).otherwise(0.0))).alias("kill_entropy"),
        F.abs(F.coalesce(F.col("tier_a"), F.lit(2.0))
              - F.coalesce(F.col("tier_b"), F.lit(2.0))).alias("skill_imbalance"),
        duration.alias("duration_s"),
    )

    short_penalty = F.when(
        F.col("duration_s") < FULL_MATCH_SECONDS,
        (F.lit(1.0) - (F.col("duration_s") / FULL_MATCH_SECONDS)) * 20.0,
    ).otherwise(0.0)
    return scored \
        .withColumn(
            "quality_score",
            F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                F.lit(100.0)
                - (F.col("kill_imbalance") * 35.0)
                - ((1.0 - F.col("kill_entropy")) * 15.0)
                - ((F.col("skill_imbalance") / 2.0) * 15.0)
                - (F.col("disconnects") * 5.0)
                - short_penalty
            )),
        ) \
        .withColumn(
            "status",
            F.when(F.col("quality_score") >= 70.0, "BALANCED")
             .when(F.col("quality_score") >= 40.0, "UNBALANCED")
             .otherwise("STOMPED"),
        )


def write_matches(r, spark, rows):
    """Serve each scored match; alert on stomps."""
    alerts = AlertBatch(r)
    pipe = r.pipeline()
    for row in rows:
        match_id = row["match_id"]
        score = round(float(row["quality_score"]), 2)
        match_data = {
            "match_id": match_id,
            "quality_score": str(score),
            "status": row["status"],
            "kills_a": str(int(row["kills_a"])),
            "kills_b": str(int(row["kills_b"])),
            "total_kills": str(int(row["total_kills"])),
            "kill_imbalance": f"{float(row['kill_imbalance']):.3f}",
            "kill_entropy": f"{float(row['kill_entropy']):.3f}",
            "skill_imbalance": f"{float(row['skill_imbalance']):.3f}",
            "disconnects": str(int(row["disconnects"])),
            "duration_s": str(int(row["duration_s"])),
            "window_end": str(row["session_window"]["end"]),
            "updated_at": str(int(time.time())),
        }
        pipe.hset(f"match:{match_id}", mapping=match_data)
        pipe.zadd("matches:quality", {match_id: score})
        pipe.zadd("matches:timeline", {match_id: int(match_data["updated_at"])})
        if row["status"] == "STOMPED":
            alerts.emit(make_alert(
                "MATCH_QUALITY_LOW", "WARNING", "MATCH", match_id,
                f"Match {match_id} quality {score} (STOMPED)",
                match_data,
            ))
    pipe.execute()
    alerts.flush()


def write_matches_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        write_matches(redis_client(), batch_df.sparkSession, batch_df.collect())
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"match quality batch {batch_id}: {e}")
    safe_parquet_archive(batch_df, "match_quality", batch_id)


def main():
    spark = spark_session("Gaming-MatchQuality")
    scored = build_match_quality_pipeline(
        parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay"),
        parse_stream(kafka_source(spark, KAFKA_TOPICS["player"]), "player"),
    )
    scored.writeStream \
        .outputMode("append") \
        .foreachBatch(write_matches_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "match_quality")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
