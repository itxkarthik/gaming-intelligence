"""Server Health Monitor Streaming Pipeline.

Scores server metrics over 10-second tumbling windows, serves the latest
health per server from Redis, alerts on CRITICAL servers and archives scored
windows as Parquet for the batch analysis.

Status bands (the dashboard maps this field directly):
    HEALTHY >= 80, DEGRADED >= 50, CRITICAL < 50
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import functions as F

from src.common.alerts import AlertBatch, make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import (
    kafka_source,
    latest_per_key,
    redis_client,
    spark_session,
    warn,
)
from src.common.schemas import parse_stream
from src.common.sinks import safe_parquet_archive

HEALTHY_MIN = 80.0
DEGRADED_MIN = 50.0


def build_health_pipeline(parsed):
    """Server metrics -> windowed health scores + status labels.

    Pure transformation (no session, no sinks) so it is unit-testable.
    """
    # Servers are provisioned as 128-tick (>= 100) or 64-tick; penalize
    # deviation from the provisioned rate.
    tick_penalty = F.when(
        F.col("tick_rate") >= 100,
        F.greatest(F.lit(0.0), (F.lit(128.0) - F.col("tick_rate")) * 2.0),
    ).otherwise(
        F.greatest(F.lit(0.0), (F.lit(64.0) - F.col("tick_rate")) * 2.5)
    )

    return parsed \
        .withColumn("tick_penalty", tick_penalty) \
        .withWatermark("metric_timestamp", "10 seconds") \
        .groupBy(F.window("metric_timestamp", "10 seconds"), "server_id", "region") \
        .agg(
            F.avg("cpu_percent").alias("avg_cpu"),
            F.avg("ram_percent").alias("avg_ram"),
            F.avg("tick_rate").alias("avg_tick_rate"),
            F.avg("packet_loss_percent").alias("avg_packet_loss"),
            F.avg("avg_latency_ms").alias("avg_latency"),
            F.avg("tick_penalty").alias("avg_tick_penalty"),
            F.max("active_players").alias("peak_players"),
            F.max("active_matches").alias("active_matches"),
        ) \
        .withColumn(
            "raw_health_score",
            F.lit(100.0)
            - (F.col("avg_cpu") * 0.20)
            - (F.col("avg_ram") * 0.15)
            - (F.col("avg_packet_loss") * 3.5)
            - F.col("avg_tick_penalty")
            - (F.greatest(F.lit(0.0), F.col("avg_latency") - F.lit(50.0)) * 0.4),
        ) \
        .withColumn(
            "health_score",
            F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("raw_health_score"))),
        ) \
        .withColumn(
            "status",
            F.when(F.col("health_score") >= HEALTHY_MIN, "HEALTHY")
             .when(F.col("health_score") >= DEGRADED_MIN, "DEGRADED")
             .otherwise("CRITICAL"),
        )


def write_server_health(r, spark, rows):
    """Serve the latest window per server; alert on CRITICAL ones."""
    alerts = AlertBatch(r)
    pipe = r.pipeline()
    for row in latest_per_key(rows, "server_id"):
        server_id = row["server_id"]
        health_score = round(float(row["health_score"]), 2)
        server_data = {
            "server_id": server_id,
            "region": str(row["region"]),
            "health_score": str(health_score),
            "status": row["status"],
            "avg_cpu": f"{float(row['avg_cpu']):.1f}",
            "avg_ram": f"{float(row['avg_ram']):.1f}",
            "avg_latency": f"{float(row['avg_latency']):.1f}",
            "avg_packet_loss": f"{float(row['avg_packet_loss']):.1f}",
            "peak_players": str(row["peak_players"]),
            "window_end": str(row["window"]["end"]),
            "updated_at": str(int(time.time())),
        }
        pipe.hset(f"server:{server_id}", mapping=server_data)
        pipe.sadd("servers:active", server_id)
        if row["status"] == "CRITICAL":
            alerts.emit(make_alert(
                "SERVER_DEGRADED", "CRITICAL", "SERVER", server_id,
                f"Server {server_id} critical (health score {health_score})",
                server_data,
            ))
    pipe.execute()
    alerts.flush()


def write_server_health_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    try:
        write_server_health(redis_client(), batch_df.sparkSession, batch_df.collect())
    except Exception as e:  # noqa: BLE001 - keep the stream alive; next batch retries
        warn(f"server health batch {batch_id}: {e}")
    safe_parquet_archive(batch_df, "server_health", batch_id)


def main():
    spark = spark_session("Gaming-ServerHealthMonitor")
    parsed = parse_stream(kafka_source(spark, KAFKA_TOPICS["server"]), "server")
    build_health_pipeline(parsed).writeStream \
        .outputMode("update") \
        .foreachBatch(write_server_health_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "server_health")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
