"""Server Health Monitor Streaming Pipeline.

Ingests server metrics from Kafka, computes rolling health scores across
tumbling windows, updates real-time server health in Redis, and archives
scored batches as Parquet for historical analysis.
"""

import sys
import os
import json
import time

# Add src to path for container imports
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
from src.common.schemas import parse_server_metric_stream
from src.common.alerts import emit_alert, flush_alerts_to_kafka
from src.common.sinks import safe_parquet_archive


def build_health_pipeline(parsed):
    """Server metrics -> windowed health scores + status labels.

    Pure transformation (no session, no sinks) so it is unit-testable.
    """
    # Tick penalty: servers are provisioned as 128-tick (>=100) or 64-tick;
    # penalize deviation from the provisioned rate.
    tick_penalty = F.when(
        F.col("tick_rate") >= 100,
        F.greatest(F.lit(0.0), (F.lit(128.0) - F.col("tick_rate")) * 2.0)
    ).otherwise(
        F.greatest(F.lit(0.0), (F.lit(64.0) - F.col("tick_rate")) * 2.5)
    )

    with_penalty = parsed.withColumn("tick_penalty", tick_penalty)

    # Aggregate in 10-second tumbling window with 10-second watermark
    return with_penalty \
        .withWatermark("metric_timestamp", "10 seconds") \
        .groupBy(
            F.window("metric_timestamp", "10 seconds"),
            "server_id",
            "region"
        ) \
        .agg(
            F.avg("cpu_percent").alias("avg_cpu"),
            F.avg("ram_percent").alias("avg_ram"),
            F.avg("tick_rate").alias("avg_tick_rate"),
            F.avg("packet_loss_percent").alias("avg_packet_loss"),
            F.avg("avg_latency_ms").alias("avg_latency"),
            F.avg("tick_penalty").alias("avg_tick_penalty"),
            F.max("active_players").alias("peak_players"),
            F.max("active_matches").alias("active_matches")
        ) \
        .withColumn(
            "raw_health_score",
            F.lit(100.0)
            - (F.col("avg_cpu") * 0.20)
            - (F.col("avg_ram") * 0.15)
            - (F.col("avg_packet_loss") * 3.5)
            - F.col("avg_tick_penalty")
            - (F.greatest(F.lit(0.0), F.col("avg_latency") - F.lit(50.0)) * 0.4)
        ) \
        .withColumn(
            "health_score",
            F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("raw_health_score")))
        ) \
        .withColumn(
            "status",
            F.when(F.col("health_score") >= 80.0, "HEALTHY")
             .when(F.col("health_score") >= 50.0, "DEGRADED")
             .otherwise("CRITICAL")
        )


def write_to_redis_batch(batch_df, batch_id):
    """ForeachBatch writer: Redis live state + deduped alerts + Parquet archive."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        pending_alerts = []
        rows = batch_df.collect()

        for row in rows:
            server_id = row["server_id"]
            health_score = round(float(row["health_score"]), 2)
            status = row["status"]

            server_data = {
                "server_id": server_id,
                "region": str(row["region"]),
                "health_score": str(health_score),
                "status": status,
                "avg_cpu": f"{float(row['avg_cpu']):.1f}",
                "avg_ram": f"{float(row['avg_ram']):.1f}",
                "avg_latency": f"{float(row['avg_latency']):.1f}",
                "avg_packet_loss": f"{float(row['avg_packet_loss']):.1f}",
                "peak_players": str(row["peak_players"]),
                "window_end": str(row["window"]["end"]),
                "updated_at": str(int(time.time()))
            }

            # Update live server state
            r.hset(f"server:{server_id}", mapping=server_data)
            r.sadd("servers:active", server_id)

            # Alert on degraded/critical servers, at most once per TTL window;
            # emit_alert returns the payload (or None if deduped) for the batch flush
            if health_score < 50.0:
                alert_payload = {
                    "alert_id": f"srv_{server_id}_{int(time.time())}",
                    "alert_type": "SERVER_DEGRADED",
                    "severity": "CRITICAL" if health_score < 30.0 else "WARNING",
                    "entity_type": "SERVER",
                    "entity_id": server_id,
                    "message": f"Server {server_id} degraded (Health Score: {health_score})",
                    "details": server_data,
                    "timestamp": int(time.time() * 1000)
                }
                emitted = emit_alert(r, alert_payload)
                if emitted:
                    pending_alerts.append(emitted)

        flush_alerts_to_kafka(batch_df.sparkSession, pending_alerts)
    except Exception as e:
        print(f"[WARN] Error writing batch {batch_id} to Redis: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "server_health", batch_id)


def main():
    spark = SparkSession.builder \
        .appName("Gaming-ServerHealthMonitor") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topic: {KAFKA_TOPICS['server']}")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["server"]) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load()

    parsed = parse_server_metric_stream(raw_stream)
    health_scores = build_health_pipeline(parsed)

    # Console output for monitoring
    console_query = health_scores.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Redis real-time state sink
    redis_query = health_scores.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_redis_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "server_health")) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("Server Health Monitor streaming pipeline started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()