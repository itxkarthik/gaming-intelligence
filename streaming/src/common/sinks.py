"""Checkpoint-critical, replay-safe output sinks for streaming jobs."""

import contextlib
import json
import os
import uuid

from pyspark.sql import functions as F

from src.common.config import CHECKPOINT_DIR, HDFS_OUTPUT_DIR

_CHECKPOINTS = {
    "cheat_detection": "cheat_detection_v3",
    "economy_weapon_kills": "economy_kills",
}


def checkpoint_generation(spark, checkpoint):
    """Read Spark's query UUID; resetting a checkpoint creates a new namespace."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(os.path.join(CHECKPOINT_DIR, checkpoint, "metadata"))
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    scanner = jvm.java.util.Scanner(fs.open(path), "UTF-8").useDelimiter(r"\A")
    try:
        return str(json.loads(scanner.next())["id"])
    finally:
        scanner.close()


def safe_parquet_archive(batch_df, subdir, batch_id):
    """Publish one complete file per query generation/batch, or raise.

    A hidden staging directory is invisible to readers. Atomic rename publishes
    the finished file; a replay after an ambiguous rename finds that same file
    and does not append duplicate rows. Hidden receipts survive compaction. Requires a rename-capable filesystem
    (the deployed local/bind-mounted filesystem, or HDFS), not an object store.
    """
    spark = batch_df.sparkSession
    generation = checkpoint_generation(spark, _CHECKPOINTS.get(subdir, subdir))
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path
    root = path(os.path.join(HDFS_OUTPUT_DIR, subdir))
    fs = root.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    fs.mkdirs(root)
    identity = f"batch-{generation}-{batch_id}"
    destination = path(root, identity + ".parquet")
    receipts = path(root, "_archive_commits")
    receipt = path(receipts, identity)

    def mark_committed():
        fs.mkdirs(receipts)
        if not fs.exists(receipt):
            fs.create(receipt, False).close()

    if fs.exists(receipt):
        return
    if fs.exists(destination):
        mark_committed()
        return
    staging = path(root, f".staging-{uuid.uuid4().hex}")
    try:
        (batch_df.withColumn("archive_written_at", F.current_timestamp())
         .withColumn("archive_batch_id", F.lit(batch_id))
         .withColumn("archive_query_id", F.lit(generation))
         .coalesce(1).write.mode("error").parquet(staging.toString()))
        parts = [status.getPath() for status in fs.listStatus(staging)
                 if status.getPath().getName().endswith(".parquet")]
        if len(parts) != 1:
            raise RuntimeError(f"Expected one completed Parquet file, found {len(parts)}")
        if not fs.rename(parts[0], destination) and not fs.exists(destination):
            raise OSError(f"Could not publish archive batch {destination}")
        mark_committed()
    finally:
        with contextlib.suppress(Exception):
            fs.delete(staging, True)
