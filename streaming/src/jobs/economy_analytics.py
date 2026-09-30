"""Economy Analytics Streaming Pipeline.

Two independent sliding-window queries (no join needed):
  1. weapon popularity — KILL share per weapon over the last 60s (30s slide)
  2. buy patterns      — ITEM_PURCHASE counts + spend per weapon, same window

Results land in Redis under `economy:*` guarded by window_end so replays are
idempotent, plus a Parquet archive for the Phase 6 batch analysis.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    KAFKA_STARTING_OFFSETS,
    REDIS_HOST,
    REDIS_PORT,
    CHECKPOINT_DIR
)
from src.common.schemas import parse_gameplay_stream, parse_player_stream
from src.common.sinks import safe_parquet_archive

WINDOW_DURATION = "60 seconds"
SLIDE_DURATION = "30 seconds"


def summarise_counts(counts):
    """{weapon: count} -> {weapon: (count, share_pct)} with shares summing ~100.

    Pure — unit-testable.
    """
    total = sum(counts.values())
    if total == 0:
        return {}
    return {
        key: (value, round(100.0 * value / total, 1))
        for key, value in sorted(counts.items(), key=lambda kv: -kv[1])
    }


def _window_end_ms(row):
    return int(row["window"].end.timestamp() * 1000)


def _store_window(r, prefix, row, counts, extra_info):
    """Write one completed window if it is newer than what Redis already has."""
    end_ms = _window_end_ms(row)
    current = int(r.hget(f"{prefix}:info", "window_end_ms") or 0)
    if end_ms <= current:
        return False  # replay or out-of-order window — keep serving state stable

    summary = summarise_counts(counts)
    r.delete(prefix)
    for weapon, (count, share) in summary.items():
        r.hset(prefix, mapping={weapon: f"{count}:{share}"})
    info = {
        "window_end_ms": str(end_ms),
        "window_end": str(row["window"].end),
        "total": str(sum(counts.values())),
        "top_weapon": next(iter(summary), ""),
    }
    info.update(extra_info)
    r.hset(f"{prefix}:info", mapping=info)
    return True


def write_kill_windows(batch_df, batch_id):
    """Weapon popularity from completed sliding windows."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        by_window = {}
        for row in batch_df.collect():
            end_ms = _window_end_ms(row)
            bucket = by_window.setdefault(end_ms, {"row": row, "counts": {}})
            bucket["counts"][row["weapon_id"]] = bucket["counts"].get(row["weapon_id"], 0) + int(row["kills"])

        for bucket in by_window.values():
            _store_window(r, "economy:weapons", bucket["row"],
                          bucket["counts"], {"kind": "kill_share"})
    except Exception as e:
        print(f"[WARN] economy kills batch {batch_id}: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "economy_weapon_kills", batch_id)


def write_purchase_windows(batch_df, batch_id):
    """Item purchase patterns (count + spend) from completed windows."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        by_window = {}
        for row in batch_df.collect():
            end_ms = _window_end_ms(row)
            bucket = by_window.setdefault(end_ms, {"row": row, "counts": {}})
            bucket["counts"][row["weapon_id"]] = bucket["counts"].get(row["weapon_id"], 0) + int(row["purchases"])

        for bucket in by_window.values():
            spend = int(bucket["row"]["total_spend"] or 0)  # F.sum is NULL when all costs are null
            _store_window(r, "economy:purchases", bucket["row"],
                          bucket["counts"], {"kind": "purchase_count",
                                             "total_spend": str(spend)})
    except Exception as e:
        print(f"[WARN] economy purchases batch {batch_id}: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "economy_purchases", batch_id)


def _kafka_stream(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load()


def start_queries(spark):
    queries = []

    # 1. Weapon popularity from kills
    kill_windows = parse_gameplay_stream(_kafka_stream(spark, KAFKA_TOPICS["gameplay"])) \
        .filter(F.col("event_type") == "KILL") \
        .withWatermark("event_timestamp", "30 seconds") \
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            "weapon_id",
        ) \
        .agg(F.count(F.lit(1)).alias("kills"))

    queries.append(
        kill_windows.writeStream \
            .outputMode("append") \
            .foreachBatch(write_kill_windows) \
            .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "economy_kills")) \
            .trigger(processingTime="5 seconds") \
            .start()
    )

    # 2. Buy patterns from ITEM_PURCHASE events
    purchase_windows = parse_player_stream(_kafka_stream(spark, KAFKA_TOPICS["player"])) \
        .filter(F.col("event_type") == "ITEM_PURCHASE") \
        .withWatermark("event_timestamp", "30 seconds") \
        .groupBy(
            F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION),
            F.col("metadata")["weapon_id"].alias("weapon_id"),
        ) \
        .agg(
            F.count(F.lit(1)).alias("purchases"),
            F.sum(F.col("metadata")["cost"].cast("int")).alias("total_spend"),
        )

    queries.append(
        purchase_windows.writeStream \
            .outputMode("append") \
            .foreachBatch(write_purchase_windows) \
            .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "economy_purchases")) \
            .trigger(processingTime="5 seconds") \
            .start()
    )

    # Console view for demos: latest kill-popularity rows as they complete
    queries.append(
        kill_windows.select("window", "weapon_id", "kills") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()
    )

    return queries


def main():
    spark = SparkSession.builder \
        .appName("Gaming-EconomyAnalytics") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topics: {KAFKA_TOPICS['gameplay']} (kills), {KAFKA_TOPICS['player']} (purchases)")
    start_queries(spark)
    print("Economy Analytics streaming pipelines started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()