"""Unit tests for the cheat-detection suspicion pipeline.

Runs inside the Spark container (pyspark + pytest are installed there):

    docker exec -w /opt/spark-apps gaming-spark-master python3 -m pytest tests -q
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.schemas import GAMEPLAY_EVENT_SCHEMA
from src.jobs.cheat_detection import build_suspicion_pipeline

# Fixed event time => every synthetic event lands in one window.
EVENT_TIME = 1_700_000_000_000


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("cheat-detection-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _event(idx, event_type, player_id, match_id, accuracy, reaction, is_headshot):
    return {
        "event_id": f"e{idx:05d}",
        "event_type": event_type,
        "match_id": match_id,
        "player_id": player_id,
        "team_id": "team_a",
        "target_player_id": "player_victim",
        "weapon_id": "ak47",
        "damage": 120.0 if is_headshot else 40.0,
        "position_x": 0.0,
        "position_y": 0.0,
        "position_z": 0.0,
        "accuracy": accuracy,
        "distance": 25.0,
        "reaction_time_ms": reaction,
        "is_headshot": is_headshot,
        "event_time": EVENT_TIME,
        "server_id": "server-01",
    }


def _chain(player_id, match_id, accuracy, reaction, shots, hits, kill_hs, kills, start=0):
    """Build one player's combat chain the way the Go simulator emits it:
    SHOT_FIRED rows, DAMAGE rows for hits, KILL rows for kills."""
    rows = []
    idx = start
    for _ in range(shots):
        rows.append(_event(idx, "SHOT_FIRED", player_id, match_id, accuracy, reaction, False))
        idx += 1
    for _ in range(hits):
        rows.append(_event(idx, "DAMAGE", player_id, match_id, accuracy, reaction, False))
        idx += 1
    for i in range(kills):
        rows.append(_event(idx, "KILL", player_id, match_id, accuracy, reaction, i < kill_hs))
        idx += 1
    return rows


def _score(spark, rows):
    df = spark.createDataFrame(rows, schema=GAMEPLAY_EVENT_SCHEMA)
    df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("event_time") / 1000.0)),
    )
    result = build_suspicion_pipeline(df).collect()
    return {r["player_id"]: r.asDict() for r in result}


def test_aimbot_is_flagged(spark):
    rows = _chain("p_aimbot", "m1", accuracy=0.93, reaction=85,
                  shots=40, hits=35, kill_hs=27, kills=30)
    scored = _score(spark, rows)["p_aimbot"]

    assert scored["total_shots"] == 40
    assert scored["total_kills"] == 30
    assert scored["suspicion_score"] >= 0.70, (
        f"aimbot should be flagged, got {scored['suspicion_score']}"
    )
    assert scored["avg_accuracy"] == pytest.approx(0.93, abs=1e-3)
    assert scored["avg_reaction_time"] == pytest.approx(85, abs=1e-3)


def test_normal_player_is_not_flagged(spark):
    rows = _chain("p_normal", "m1", accuracy=0.28, reaction=260,
                  shots=40, hits=15, kill_hs=1, kills=10)
    scored = _score(spark, rows)["p_normal"]

    assert scored["suspicion_score"] < 0.50, (
        f"normal player must stay under console threshold, got {scored['suspicion_score']}"
    )


def test_headshot_ratio_never_exceeds_one(spark):
    """Regression: headshots used to be counted on DAMAGE and KILL rows
    while dividing by KILL count only — yielding ratios like 1.92 in
    production alerts. A ratio of headshot-kills / kills is bounded by 1."""
    rows = []
    rows += _chain("p_allhs", "m1", accuracy=0.93, reaction=90,
                   shots=30, hits=0, kill_hs=0, kills=0)
    # Every DAMAGE and every KILL flagged as headshot (old bug: 60/30 = 2.0).
    idx = 30
    for _ in range(30):
        rows.append(_event(idx, "DAMAGE", "p_allhs", "m1", 0.93, 90, True))
        idx += 1
    for _ in range(30):
        rows.append(_event(idx, "KILL", "p_allhs", "m1", 0.93, 90, True))
        idx += 1

    scored = _score(spark, rows)["p_allhs"]
    assert scored["total_kills"] == 30
    assert scored["headshot_ratio"] == pytest.approx(1.0, abs=1e-9), (
        f"headshot_ratio must be <= 1.0, got {scored['headshot_ratio']}"
    )


def test_zero_kill_player_has_zero_ratio(spark):
    rows = _chain("p_nokill", "m1", accuracy=0.30, reaction=250,
                  shots=10, hits=3, kill_hs=0, kills=0)
    scored = _score(spark, rows)["p_nokill"]

    assert scored["total_kills"] == 0
    assert scored["headshot_ratio"] == 0.0
    assert scored["suspicion_score"] < 0.50
