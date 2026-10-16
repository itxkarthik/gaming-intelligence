"""Session, source and client factories shared by every streaming job."""

import contextlib
import random
import sys
import time

import redis
from kafka.errors import KafkaError

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_MAX_OFFSETS_PER_TRIGGER,
    KAFKA_STARTING_OFFSETS,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
)


def warn(message):
    """One-line warning on the driver's stderr (the job log)."""
    print(f"[WARN] {message}", file=sys.stderr)


def spark_session(name):
    # Imported here, not at module level: offline training (src/ml) uses
    # warn() from this module and runs as plain Python without pyspark.
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName(name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER) \
        .option("failOnDataLoss", "false") \
        .load()


def redis_client():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                       decode_responses=True, socket_connect_timeout=5, socket_timeout=5)


@contextlib.contextmanager
def cached_batch(batch_df):
    """Persist a foreachBatch DataFrame across isEmpty(), collect() and Parquet write."""
    batch_df.persist()
    try:
        yield batch_df
    finally:
        batch_df.unpersist()


def run_with_retry(name, fn, attempts=5, initial_delay=0.25, max_delay=2.0):
    """Run `fn()` with bounded exponential backoff on transient failures.

    If every attempt fails, re-raise so Spark does not commit the micro-batch
    offset and silently drop append-mode records.
    """
    delay = initial_delay
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except (redis.ConnectionError, redis.TimeoutError, redis.BusyLoadingError, KafkaError) as e:
            if attempt >= attempts:
                warn(f"{name} failed after {attempt} attempts: {e}")
                raise
            warn(f"{name} failed (attempt {attempt}/{attempts}), retrying in {delay:.2f}s: {e}")
            time.sleep(random.uniform(delay / 2, delay))
            delay = min(delay * 2.0, max_delay)


def latest_per_key(rows, key, window_col="window"):
    """One row per `key`: the one whose window ends last.

    Sliding windows overlap and update-mode batches can carry several windows
    of one entity; writing them all leaves Redis with whichever came last.
    """
    latest = {}
    for row in rows:
        current = latest.get(row[key])
        if current is None or row[window_col]["end"] > current[window_col]["end"]:
            latest[row[key]] = row
    return list(latest.values())
