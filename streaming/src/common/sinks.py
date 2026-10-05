"""Shared output sinks for streaming jobs."""

import os

from src.common.config import HDFS_OUTPUT_DIR
from src.common.runtime import warn


def safe_parquet_archive(batch_df, subdir, batch_id):
    """Append a micro-batch to the Parquet archive used by the batch jobs.

    At-least-once under streaming replays (a batch may be recomputed), so the
    archive tolerates duplicate rows: it is an audit trail, Redis stays the
    serving source of truth. coalesce(1) keeps one file per batch. A failure
    is logged and never kills the stream (realtime > archival).
    """
    try:
        batch_df.coalesce(1).write.mode("append").parquet(os.path.join(HDFS_OUTPUT_DIR, subdir))
    except Exception as e:  # noqa: BLE001 - any Spark/IO failure; the stream survives
        warn(f"Parquet archive '{subdir}' batch {batch_id} failed: {e}")
