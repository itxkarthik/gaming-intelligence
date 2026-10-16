"""Real stateful Spark regressions; run inside the Spark Docker container."""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.jobs import behavior_change
from test_behavior_change import _DummyAlerts, _FakeRedis


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("behavior-change-streaming-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_split_windows_late_shots_restart_and_replay(spark, tmp_path, monkeypatch):
    source = tmp_path / "input"
    source.mkdir()
    checkpoint = tmp_path / "checkpoints"
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("player_id", StringType()),
        StructField("event_type", StringType()),
        StructField("accuracy", DoubleType()),
        StructField("event_timestamp", TimestampType()),
    ])
    events = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(str(source))
    monkeypatch.setattr(behavior_change, "kafka_source", lambda *args: events)
    monkeypatch.setattr(behavior_change, "parse_stream", lambda df, kind: df)
    monkeypatch.setattr(behavior_change, "CHECKPOINT_DIR", str(checkpoint))
    monkeypatch.setattr(behavior_change, "DEFAULT_PROCESSING_TIME", "500 milliseconds")

    r = _FakeRedis()
    completed = []
    archived = []


    # Capture the real callback before patching it to avoid recursive dispatch.
    write_completed = behavior_change.write_behavior_batch

    def consume(batch, batch_id):
        # Spark grouping provides no order; force reverse chronological output.
        rows = sorted(batch.collect(), key=lambda row: row["window"]["end"], reverse=True)
        completed.extend(rows)

        class CollectedBatch:
            def collect(self):
                return rows

        write_completed(CollectedBatch(), batch_id)

    monkeypatch.setattr(behavior_change, "write_behavior_batch", consume)
    monkeypatch.setattr(behavior_change, "redis_client", lambda: r)
    monkeypatch.setattr(behavior_change, "AlertBatch", _DummyAlerts)
    monkeypatch.setattr(
        behavior_change, "safe_parquet_archive",
        lambda batch, subdir, batch_id: archived.extend((subdir, row) for row in batch.collect()),
    )
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def shot(event_id, sec, accuracy, player="p1", event_type="SHOT"):
        return {
            "event_id": event_id, "player_id": player, "event_type": event_type,
            "accuracy": accuracy,
            "event_timestamp": (base + timedelta(seconds=sec)).isoformat(),
        }

    def add_file(index, rows):
        # Rename into the source so Spark never observes a partial JSON file.
        pending = tmp_path / f"pending-{index}.json"
        pending.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
        pending.rename(source / f"{index}.json")

    def drain(queries):
        for query in queries:
            query.processAllAvailable()

    queries = []
    try:
        queries = behavior_change.start_queries(spark)
        assert isinstance(queries, list) and len(queries) == 2
        add_file(0, [
            shot("early-1", 1, 0.1), shot("early-2", 2, 0.3),
            *[shot(f"next-{i}", 6 + i, 0.7) for i in range(3)],
            shot("not-a-shot", 2, 1.0, event_type="LOGIN"),
            shot("null-accuracy", 2, None),
            shot("few-1", 1, 0.5, player="few"),
            shot("few-2", 2, 0.5, player="few"),
        ])
        drain(queries)
        assert completed == []  # even the full newer window is not final
        assert "behavior:cusum:p1" not in r.hashes

        # An incomplete window must survive a state-store checkpoint restart.
        for query in queries:
            query.stop()
        queries = behavior_change.start_queries(spark)
        add_file(1, [shot("late", 3, 0.5), shot("clock-22", 22, 0.5, player="clock")])
        drain(queries)
        assert completed == []  # the shot at t=3 is 19s behind the newest
        assert "behavior:cusum:p1" not in r.hashes

        add_file(2, [shot("clock-46", 46, 0.5, player="clock")])
        drain(queries)
        steps = sorted(completed, key=lambda row: row["window"]["end"])
        assert len(steps) == 2
        assert [row["player_id"] for row in steps] == ["p1", "p1"]
        assert [row["shots"] for row in steps] == [3, 3]
        assert [row["mean_accuracy"] for row in steps] == pytest.approx([0.3, 0.7])
        state = dict(r.hashes["behavior:cusum:p1"])
        assert state["state_version"] == behavior_change.BEHAVIOR_STATE_VERSION
        assert int(state["n"]) == 2
        assert float(state["mean"]) == pytest.approx(0.5)
        assert float(state["m2"]) == pytest.approx(0.08)
        assert int(state["last_window_end_ms"]) == int((base + timedelta(seconds=10)).timestamp() * 1000)

        # Replay the actual Spark output out of order, including duplicates.
        behavior_change.update_behavior(r, _DummyAlerts(), list(reversed(steps)) + steps)
        assert r.hashes["behavior:cusum:p1"] == state
        assert all(subdir == "behavior_change" for subdir, _ in archived)
        archived_ids = [row["event_id"] for _, row in archived]
        assert len(archived_ids) == len(set(archived_ids)) == 11
        assert "null-accuracy" in archived_ids  # archive raw shots, not aggregates
        assert "not-a-shot" not in archived_ids
        assert (checkpoint / "behavior_change").is_dir()
        assert (checkpoint / "behavior_change_steps_v2").is_dir()
    finally:
        for query in queries:
            query.stop()
