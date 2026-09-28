"""Unit tests for the match quality scoring pipeline (runs in the Spark container)."""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.schemas import GAMEPLAY_EVENT_SCHEMA, PLAYER_EVENT_SCHEMA
from src.jobs.match_quality import build_match_quality_pipeline, SESSION_GAP_SECONDS

T0 = 1_700_000_000_000  # match start (ms)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("match-quality-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _kill(idx, match_id, team, offset_s):
    return {
        "event_id": f"k{idx:04d}",
        "event_type": "KILL",
        "match_id": match_id,
        "player_id": f"shooter_{idx}",
        "team_id": team,
        "target_player_id": "victim",
        "weapon_id": "ak47",
        "damage": 100.0,
        "position_x": 0.0, "position_y": 0.0, "position_z": 0.0,
        "accuracy": 0.4, "distance": 20.0, "reaction_time_ms": 250,
        "is_headshot": False,
        "event_time": T0 + offset_s * 1000,
        "server_id": "server-01",
    }


def _player(idx, event_type, match_id, team=None, tier=None, offset_s=0):
    metadata = {}
    if team:
        metadata["team_id"] = team
    if tier:
        metadata["skill_tier"] = tier
    return {
        "event_id": f"p{idx:04d}",
        "player_id": f"player_{idx:04d}",
        "event_type": event_type,
        "match_id": match_id,
        "metadata": metadata or None,
        "event_time": T0 + offset_s * 1000,
        "server_id": "server-01",
    }


def _joins(n_team_a, n_team_b, tier_a="gold", tier_b="gold"):
    rows = []
    idx = 0
    for team, count in (("team_a", n_team_a), ("team_b", n_team_b)):
        for i in range(count):
            rows.append(_player(idx, "MATCH_JOIN", "m_test", team=team, tier=tier_a if team == "team_a" else tier_b))
            idx += 1
    return rows


def _score(spark, gameplay_rows, player_rows):
    gdf = spark.createDataFrame(gameplay_rows, schema=GAMEPLAY_EVENT_SCHEMA).withColumn(
        "event_timestamp", F.to_timestamp(F.from_unixtime(F.col("event_time") / 1000.0))
    )
    pdf_ = spark.createDataFrame(player_rows, schema=PLAYER_EVENT_SCHEMA).withColumn(
        "event_timestamp", F.to_timestamp(F.from_unixtime(F.col("event_time") / 1000.0))
    )
    result = build_match_quality_pipeline(gdf, pdf_).collect()
    assert len(result) == 1, f"expected exactly one scored match, got {len(result)}"
    return result[0].asDict()


def test_balanced_long_match_scores_high(spark):
    kills = [_kill(i, "m_test", "team_a" if i % 2 == 0 else "team_b", offset_s=i * 40)
             for i in range(10)]  # 5v5 over ~6.5 minutes
    players = _joins(5, 5)

    scored = _score(spark, kills, players)

    assert scored["total_kills"] == 10
    assert scored["kills_a"] == 5 and scored["kills_b"] == 5
    assert scored["kill_imbalance"] == 0.0
    assert scored["kill_entropy"] == pytest.approx(1.0, abs=1e-6)
    assert scored["skill_imbalance"] == 0.0
    assert scored["quality_score"] >= 95.0
    assert scored["status"] == "BALANCED"


def test_stomped_match_scores_low(spark):
    kills = [_kill(i, "m_test", "team_a" if i < 9 else "team_b", offset_s=i * 30)
             for i in range(10)]  # 9:1 stomp over ~4.5 minutes
    players = _joins(5, 5)
    players.append(_player(99, "DISCONNECT", "m_test", offset_s=100))

    scored = _score(spark, kills, players)

    assert scored["kill_imbalance"] == pytest.approx(0.8, abs=1e-6)
    expected_entropy = -(0.9 * math.log2(0.9) + 0.1 * math.log2(0.1))
    assert scored["kill_entropy"] == pytest.approx(expected_entropy, abs=1e-3)
    assert scored["disconnects"] == 1
    assert scored["quality_score"] < 70.0
    assert scored["status"] in ("UNBALANCED", "STOMPED")


def test_skill_imbalance_lowers_score(spark):
    kills = [_kill(i, "m_test", "team_a" if i % 2 == 0 else "team_b", offset_s=i * 60)
             for i in range(6)]  # balanced 3v3, long duration
    players = _joins(5, 5, tier_a="diamond", tier_b="bronze")

    scored = _score(spark, kills, players)

    assert scored["skill_imbalance"] == pytest.approx(2.0, abs=1e-6)
    # Perfectly balanced combat, but max tier gap: 100 - 15 - small duration term
    assert 60.0 < scored["quality_score"] <= 85.0


def test_short_match_gets_duration_penalty(spark):
    kills = [_kill(i, "m_test", "team_a" if i % 2 == 0 else "team_b", offset_s=i * 5)
             for i in range(4)]  # all action inside a 15-second window
    players = _joins(5, 5)

    scored = _score(spark, kills, players)

    assert scored["duration_s"] == pytest.approx(15, abs=5)
    # Balanced stats but a 15s match => duration penalty 20*(1-15/180) ~= 18.3
    assert 75.0 <= scored["quality_score"] < 90.0


def test_session_gap_excluded_from_duration(spark):
    """duration_s must be activity span, not session-window span (gap excluded)."""
    # 6 alternating kills 2 minutes apart: perfectly balanced, spans 10 minutes
    kills = [_kill(i, "m_test", "team_a" if i % 2 == 0 else "team_b", offset_s=i * 120)
             for i in range(6)]
    players = _joins(5, 5)

    scored = _score(spark, kills, players)

    expected = 5 * 120  # first->last kill activity span
    assert scored["duration_s"] == pytest.approx(expected, abs=2)
    assert scored["quality_score"] == pytest.approx(100.0, abs=0.5)