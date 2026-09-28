"""Shared output sinks for streaming jobs."""

import os
import sys

from src.common.config import HDFS_OUTPUT_DIR


def write_parquet_archive(batch_df, subdir):
    """Append a micro-batch to the local Parquet archive for historical/batch jobs.

    Called inside foreachBatch: under streaming replays this is at-least-once
    (a batch may be re-computed), so archives tolerate duplicate rows — they
    are an audit trail, the serving source of truth stays Redis.
    coalesce(1) keeps one file per batch for easy inspection in Phase 6.
    """
    path = os.path.join(HDFS_OUTPUT_DIR, subdir)
    batch_df.coalesce(1).write.mode("append").parquet(path)


def safe_parquet_archive(batch_df, subdir, batch_id):
    """Archive without killing the stream on failure (realtime > archival)."""
    try:
        write_parquet_archive(batch_df, subdir)
    except Exception as e:
        print(f"[WARN] Parquet archive '{subdir}' batch {batch_id} failed: {e}",
              file=sys.stderr)