#!/usr/bin/env python3
"""Compact Parquet archive directories while streaming writers are stopped.

Writes a compacted copy beside each archive, then swaps directories only
after the write succeeds. The temporary copy uses the same Parquet schema.
"""

import argparse
import os
import shutil
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import HDFS_OUTPUT_DIR


def compact(spark, name, target_files):
    source = os.path.join(HDFS_OUTPUT_DIR, name)
    if not os.path.isdir(source):
        print(f"Skipping {name}: archive directory not found")
        return

    suffix = uuid.uuid4().hex
    temporary = os.path.join(HDFS_OUTPUT_DIR, f".{name}.compact-{suffix}")
    backup = os.path.join(HDFS_OUTPUT_DIR, f".{name}.backup-{suffix}")
    try:
        archived = spark.read.option("mergeSchema", "true").parquet(source)
        if "archive_written_at" not in archived.columns:
            archived = archived.withColumn(
                "archive_written_at", F.col("_metadata.file_modification_time"))
        else:
            archived = archived.withColumn(
                "archive_written_at",
                F.coalesce(F.col("archive_written_at"),
                           F.col("_metadata.file_modification_time")))
        archived.coalesce(target_files).write.mode("overwrite").parquet(temporary)
        os.rename(source, backup)
        try:
            os.rename(temporary, source)
        except Exception:
            os.rename(backup, source)
            raise
        shutil.rmtree(backup)
        print(f"Compacted {name} to at most {target_files} Parquet files per archive copy")
    finally:
        if os.path.exists(temporary):
            shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", default="all", help="archive name or all")
    parser.add_argument("--files", type=int, default=4,
                        help="target output file count per archive (default: 4)")
    parser.add_argument("--streams-stopped", action="store_true",
                        help="confirm no streaming job is writing to the archive")
    args = parser.parse_args()
    if not args.streams_stopped:
        parser.error("stop all streaming jobs, then pass --streams-stopped")
    if args.files < 1:
        parser.error("--files must be at least 1")

    spark = SparkSession.builder.master("local[1]").appName("ParquetCompaction").getOrCreate()
    try:
        names = sorted(name for name in os.listdir(HDFS_OUTPUT_DIR)
                       if os.path.isdir(os.path.join(HDFS_OUTPUT_DIR, name))
                       and not name.startswith(".")) if os.path.isdir(HDFS_OUTPUT_DIR) else []
        selected = names if args.job == "all" else [args.job]
        if args.job != "all" and args.job not in names:
            parser.error(f"archive {args.job!r} not found under {HDFS_OUTPUT_DIR}")
        for name in selected:
            compact(spark, name, args.files)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
