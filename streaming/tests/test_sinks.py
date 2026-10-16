"""Real local-filesystem archive commit and replay regressions."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from src.common import sinks


@pytest.fixture
def archive(tmp_path, monkeypatch):
    checkpoint = tmp_path / "checkpoints" / "behavior_change"
    checkpoint.mkdir(parents=True)
    metadata = checkpoint / "metadata"
    metadata.write_text(json.dumps({"id": "first-generation"}))
    monkeypatch.setattr(sinks, "CHECKPOINT_DIR", str(checkpoint.parent))
    output = tmp_path / "archive"
    monkeypatch.setattr(sinks, "HDFS_OUTPUT_DIR", str(output))
    return output / "behavior_change", metadata


def test_replay_publishes_exactly_one_complete_file(spark, archive):
    output, _ = archive
    df = spark.createDataFrame([("shot-1",), ("shot-2",)], "event_id string")
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    original = spark.read.parquet(str(output)).collect()
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    assert spark.read.parquet(str(output)).collect() == original
    assert len(list(output.glob("*.parquet"))) == 1
    assert not list(output.glob(".staging-*"))


def test_checkpoint_reset_has_distinct_archive_identity(spark, archive):
    output, metadata = archive
    df = spark.createDataFrame([("shot-1",)], "event_id string")
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    metadata.write_text(json.dumps({"id": "second-generation"}))
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    rows = spark.read.parquet(str(output)).collect()
    assert len(rows) == 2
    assert {row.archive_query_id for row in rows} == {"first-generation", "second-generation"}


def test_archive_failure_raises_and_retry_publishes(spark, archive):
    output, _ = archive

    class BrokenBatch:
        sparkSession = spark

        def withColumn(self, *args):
            return self

        def coalesce(self, *args):
            return self

        @property
        def write(self):
            raise OSError("injected archive failure")

    with pytest.raises(OSError, match="injected archive failure"):
        sinks.safe_parquet_archive(BrokenBatch(), "behavior_change", 1)
    assert not list(output.glob("*.parquet"))
    assert not list(output.glob(".staging-*"))
    df = spark.createDataFrame([("shot-1",)], "event_id string")
    sinks.safe_parquet_archive(df, "behavior_change", 1)
    assert spark.read.parquet(str(output)).count() == 1


def test_compaction_preserves_published_batch_replay_identity(spark, archive, monkeypatch):
    from src.batch import compact_parquet

    output, _ = archive
    monkeypatch.setattr(compact_parquet, "HDFS_OUTPUT_DIR", str(output.parent))
    df = spark.createDataFrame([("shot-1",)], "event_id string")
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    # Reproduce a driver crash after publish but before its receipt or Spark
    # commit: compaction must recover identity from the published filename.
    receipt = output / "_archive_commits" / "batch-first-generation-0"
    receipt.unlink()
    compact_parquet.compact(spark, "behavior_change", 1)
    assert receipt.is_file()
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    compact_parquet.compact(spark, "behavior_change", 1)
    sinks.safe_parquet_archive(df, "behavior_change", 0)
    assert spark.read.parquet(str(output)).count() == 1


def test_compaction_skips_when_output_root_is_missing(tmp_path, monkeypatch, capsys):
    from src.batch import compact_parquet

    monkeypatch.setattr(compact_parquet, "HDFS_OUTPUT_DIR", str(tmp_path / "missing"))
    compact_parquet.compact(object(), "behavior_change", 1)
    assert "Skipping behavior_change: archive directory not found" in capsys.readouterr().out


def test_missing_checkpoint_identity_is_not_silently_ignored(spark, archive):
    _, metadata = archive
    metadata.unlink()
    df = spark.createDataFrame([("shot-1",)], "event_id string")
    with pytest.raises(Exception):
        sinks.safe_parquet_archive(df, "behavior_change", 0)


def test_checkpoint_mapping_matches_cheat_detection_job():
    from src.jobs.cheat_detection import CHEAT_CHECKPOINT

    assert sinks._CHECKPOINTS["cheat_detection"] == CHEAT_CHECKPOINT
