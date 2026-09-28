"""Match Quality Scoring Streaming Pipeline.

Joins combat events (gameplay_events) with session lifecycle events
(player_events) per match using session windows, computes a composite
0-100 match quality score, and serves it from Redis plus a Parquet archive.

Quality model (documented weights, Phase 2 deliverable):
    score  = 100
            - 35 * kill_imbalance          (|kills_a - kills_b| / total, 0..1)
            - 15 * (1 - kill_entropy)      (2-team Shannon entropy in bits, 0..1)
            - 15 * (skill_imbalance / 2)   (avg tier delta, tiers in [1..3])
            -  5 * disconnects             (dropped players in the match)
            - duration penalty             (20 * (1 - dur/180) when dur < 180s)
    status = BALANCED (>=70) / UNBALANCED (>=40) / STOMPED (<40)
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
from src.common.schemas import parse_gameplay_stream, parse_player_stream
from src.common.alerts import should_emit_alert
from src.common.sinks import safe_parquet_archive

SESSION_GAP = "2 minutes"
SESSION_GAP_SECONDS = 120

# skill_tier label -> numeric rank used for team imbalance
_TIER_VALUES = {
    "bronze": 1.0,
    "silver": 1.5,   # cheater profiles sit between bronze and gold
    "gold": 2.0,
    "diamond": 3.0,
}


def build_match_quality_pipeline(gameplay_parsed, player_parsed):
    """Two parsed streams -> scored match-quality DataFrame.

    Pure transformation (no session, no sinks) so it is unit-testable on
    batch DataFrames as well as live streams.

    Both sides aggregate into session windows (gap=2min: a match ends when
    its events stop for 2 minutes), then inner-join on match_id with an
    explicit window-overlap time constraint so Spark can bound the state.
    """
    # ── Side 1: combat balance from gameplay events ──
    # session_window (NOT window): gap-based sessions, a match "ends" when
    # its events stop for SESSION_GAP.
    combat_stats = gameplay_parsed \
        .filter(F.col("event_type") == "KILL") \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(F.session_window("event_timestamp", SESSION_GAP), "match_id") \
        .agg(
            F.sum(F.when(F.col("team_id") == "team_a", 1).otherwise(0)).alias("kills_a"),
            F.sum(F.when(F.col("team_id") == "team_b", 1).otherwise(0)).alias("kills_b"),
            F.count(F.lit(1)).alias("total_kills")
        )

    # ── Side 2: disconnects + team skill from player events ──
    tier_label = F.col("metadata")["skill_tier"]
    tier_value = F.when(tier_label == "diamond", 3.0) \
                  .when(tier_label == "gold", 2.0) \
                  .when(tier_label == "silver", 1.5) \
                  .when(tier_label == "bronze", 1.0)

    player_stats = player_parsed \
        .filter(
            F.col("match_id").isNotNull()
            & F.col("event_type").isin("DISCONNECT", "MATCH_LEAVE", "MATCH_JOIN")
        ) \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(F.session_window("event_timestamp", SESSION_GAP), "match_id") \
        .agg(
            F.sum(F.when(F.col("event_type") == "DISCONNECT", 1).otherwise(0)).alias("disconnects"),
            F.sum(F.when(F.col("event_type") == "MATCH_LEAVE", 1).otherwise(0)).alias("leaves"),
            F.avg(
                F.when((F.col("event_type") == "MATCH_JOIN")
                       & (F.col("metadata")["team_id"] == "team_a"), tier_value)
            ).alias("tier_a"),
            F.avg(
                F.when((F.col("event_type") == "MATCH_JOIN")
                       & (F.col("metadata")["team_id"] == "team_b"), tier_value)
            ).alias("tier_b")
        )

    # ── Stream-stream join: same match, overlapping session windows ──
    joined = combat_stats.alias("c").join(
        player_stats.alias("p"),
        (F.col("c.match_id") == F.col("p.match_id"))
        & (F.col("c.session_window.start") <= F.col("p.session_window.end"))
        & (F.col("p.session_window.start") <= F.col("c.session_window.end")),
        "inner",
    ).drop(F.col("p.match_id")).drop(F.col("p.session_window"))

    # ── Features ──
    total = F.col("total_kills")
    pa = F.col("kills_a") / total
    pb = F.col("kills_b") / total

    scored = joined \
        .withColumn(
            "kill_imbalance",
            F.when(total > 0, F.abs(F.col("kills_a") - F.col("kills_b")) / total).otherwise(0.0)
        ) \
        .withColumn(
            "kill_entropy",
            F.when(total > 0,
                    -(F.when(pa > 0, pa * F.log2(pa)).otherwise(0.0)
                      + F.when(pb > 0, pb * F.log2(pb)).otherwise(0.0))
                    ).otherwise(1.0)
        ) \
        .withColumn(
            "skill_imbalance",
            F.abs(
                F.coalesce(F.col("tier_a"), F.lit(2.0))
                - F.coalesce(F.col("tier_b"), F.lit(2.0))
            )
        ) \
        .withColumn(
            "duration_s",
            F.greatest(
                F.unix_timestamp(F.col("session_window.end"))
                - F.unix_timestamp(F.col("session_window.start"))
                - F.lit(SESSION_GAP_SECONDS),
                F.lit(0)
            )
        )

    # ── Composite score ──
    return scored \
        .withColumn(
            "quality_score",
            F.greatest(F.lit(0.0), F.least(F.lit(100.0),
                F.lit(100.0)
                - (F.col("kill_imbalance") * 35.0)
                - ((1.0 - F.col("kill_entropy")) * 15.0)
                - ((F.col("skill_imbalance") / 2.0) * 15.0)
                - (F.col("disconnects") * 5.0)
                - F.when(F.col("duration_s") < 180,
                         (F.lit(1.0) - (F.col("duration_s") / 180.0)) * 20.0
                         ).otherwise(0.0)
            ))
        ) \
        .withColumn(
            "status",
            F.when(F.col("quality_score") >= 70.0, "BALANCED")
             .when(F.col("quality_score") >= 40.0, "UNBALANCED")
             .otherwise("STOMPED")
        )


def write_matches_to_redis(batch_df, batch_id):
    """ForeachBatch: Redis match state + deduped quality alerts + Parquet."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

        for row in batch_df.collect():
            match_id = row["match_id"]
            score = round(float(row["quality_score"]), 2)
            status = row["status"]

            match_data = {
                "match_id": match_id,
                "quality_score": str(score),
                "status": status,
                "kills_a": str(int(row["kills_a"])),
                "kills_b": str(int(row["kills_b"])),
                "total_kills": str(int(row["total_kills"])),
                "kill_imbalance": f"{float(row['kill_imbalance']):.3f}",
                "kill_entropy": f"{float(row['kill_entropy']):.3f}",
                "skill_imbalance": f"{float(row['skill_imbalance']):.3f}",
                "disconnects": str(int(row["disconnects"])),
                "duration_s": str(int(row["duration_s"])),
                "window_end": str(row["session_window"]["end"]),
                "updated_at": str(int(time.time()))
            }

            r.hset(f"match:{match_id}", mapping=match_data)
            r.zadd("matches:quality", {match_id: score})

            if status == "STOMPED" and should_emit_alert(r, "MATCH_STOMPED", match_id):
                alert_payload = {
                    "alert_id": f"match_{match_id}_{int(time.time())}",
                    "alert_type": "MATCH_STOMPED",
                    "severity": "WARNING",
                    "entity_type": "MATCH",
                    "entity_id": match_id,
                    "message": f"Match {match_id} quality {score} ({status})",
                    "details": match_data,
                    "timestamp": int(time.time() * 1000)
                }
                r.lpush("alerts:recent", json.dumps(alert_payload))
                r.ltrim("alerts:recent", 0, 99)
    except Exception as e:
        print(f"[WARN] Error writing match batch {batch_id} to Redis: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "match_quality", batch_id)


def main():
    spark = SparkSession.builder \
        .appName("Gaming-MatchQuality") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topics: {KAFKA_TOPICS['gameplay']}, {KAFKA_TOPICS['player']}")

    gameplay_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["gameplay"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    player_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["player"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    scored = build_match_quality_pipeline(
        parse_gameplay_stream(gameplay_stream),
        parse_player_stream(player_stream),
    )

    console_query = scored.select(
        "match_id", "quality_score", "status", "kills_a", "kills_b",
        "disconnects", "skill_imbalance", "duration_s"
    ).writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()

    redis_query = scored.writeStream \
        .outputMode("append") \
        .foreachBatch(write_matches_to_redis) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "match_quality")) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("Match Quality streaming pipeline started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()