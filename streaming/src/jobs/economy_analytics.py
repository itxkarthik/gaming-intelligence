"""Economy Analytics Streaming Pipeline.

Two independent sliding-window queries (60 s windows, 30 s slide):
  1. weapon popularity: share of kills per weapon
  2. buy patterns:      ITEM_PURCHASE count per weapon and total spend

Each completed window replaces `economy:weapons` / `economy:purchases` (and
their `:info` hashes) in one transaction, guarded by window_end so replays
and out-of-order windows never regress the served state. Both queries also
archive to Parquet for the batch analysis.
"""

import os

from pyspark.sql import functions as F

from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import kafka_source, redis_client, warn
from src.common.schemas import parse_stream
from src.common.sinks import safe_parquet_archive

WINDOW_DURATION = "60 seconds"
SLIDE_DURATION = "30 seconds"
WATERMARK = "30 seconds"


def summarise_counts(counts):
    """{weapon: count} -> {weapon: (count, share_pct)}, largest first.

    Pure, unit-testable.
    """
    total = sum(counts.values())
    if total == 0:
        return {}
    return {
        key: (value, round(100.0 * value / total, 1))
        for key, value in sorted(counts.items(), key=lambda kv: -kv[1])
    }


def group_windows(rows, count_col, spend_col=None):
    """Window rows (one per weapon) -> {window_end: {"window", "counts", "spend"}}."""
    windows = {}
    for row in rows:
        end = row["window"]["end"]
        bucket = windows.setdefault(end, {"window": row["window"], "counts": {}, "spend": 0})
        weapon = row["weapon_id"]
        bucket["counts"][weapon] = bucket["counts"].get(weapon, 0) + int(row[count_col])
        if spend_col is not None:
            bucket["spend"] += int(row[spend_col] or 0)  # sum() is null when every cost is null
    return windows


def store_window(r, prefix, window, counts, extra_info):
    """Replace the served window if this one is newer; False when skipped."""
    end_ms = int(window["end"].timestamp() * 1000)
    if end_ms <= int(r.hget(f"{prefix}:info", "window_end_ms") or 0):
        return False

    summary = summarise_counts(counts)
    info = {
        "window_end_ms": str(end_ms),
        "window_end": str(window["end"]),
        "total": str(sum(counts.values())),
        "top_weapon": next(iter(summary), ""),
        **extra_info,
    }
    pipe = r.pipeline(transaction=True)
    pipe.delete(prefix)
    if summary:
        pipe.hset(prefix, mapping={w: f"{count}:{share}" for w, (count, share) in summary.items()})
    pipe.hset(f"{prefix}:info", mapping=info)
    pipe.execute()
    return True


def write_kill_windows(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        r = redis_client()
        for _, bucket in sorted(group_windows(batch_df.collect(), "kills").items()):
            store_window(r, "economy:weapons", bucket["window"], bucket["counts"],
                         {"kind": "kill_share"})
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"economy kills batch {batch_id}: {e}")
    safe_parquet_archive(batch_df, "economy_weapon_kills", batch_id)


def write_purchase_windows(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        r = redis_client()
        windows = group_windows(batch_df.collect(), "purchases", "total_spend")
        for _, bucket in sorted(windows.items()):
            store_window(r, "economy:purchases", bucket["window"], bucket["counts"],
                         {"kind": "purchase_count", "total_spend": str(bucket["spend"])})
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"economy purchases batch {batch_id}: {e}")
    safe_parquet_archive(batch_df, "economy_purchases", batch_id)


def _sliding(df):
    return df.withWatermark("event_timestamp", WATERMARK) \
        .groupBy(F.window("event_timestamp", WINDOW_DURATION, SLIDE_DURATION), "weapon_id")


def _start(df, writer, checkpoint):
    return df.writeStream \
        .outputMode("append") \
        .foreachBatch(writer) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, checkpoint)) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()


def start_queries(spark):
    kills = parse_stream(kafka_source(spark, KAFKA_TOPICS["gameplay"]), "gameplay") \
        .filter(F.col("is_kill"))
    kill_windows = _sliding(kills).agg(F.count(F.lit(1)).alias("kills"))

    purchases = parse_stream(kafka_source(spark, KAFKA_TOPICS["player"]), "player") \
        .filter(F.col("event_type") == "ITEM_PURCHASE") \
        .withColumn("weapon_id", F.col("metadata")["weapon_id"])
    purchase_windows = _sliding(purchases).agg(
        F.count(F.lit(1)).alias("purchases"),
        F.sum(F.col("metadata")["cost"].cast("int")).alias("total_spend"),
    )

    return [
        _start(kill_windows, write_kill_windows, "economy_kills"),
        _start(purchase_windows, write_purchase_windows, "economy_purchases"),
    ]
