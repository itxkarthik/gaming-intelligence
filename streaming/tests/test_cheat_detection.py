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

from src.common.schemas import GAMEPLAY_EVENT_SCHEMA, SHOT_EVENT
from src.jobs.cheat_detection import best_window_per_player, build_suspicion_pipeline
from src.ml.iforest import HS_PRIOR, HS_PRIOR_HITS

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


def _shot(idx, player_id, match_id, accuracy, reaction,
          hit=False, headshot=False, kill=False):
    """One SHOT event exactly as the Go simulator emits it."""
    return {
        "event_id": f"e{idx:05d}",
        "event_type": SHOT_EVENT,
        "match_id": match_id,
        "player_id": player_id,
        "team_id": "team_a",
        "target_player_id": "player_victim",
        "weapon_id": "ak47",
        "damage": (100.0 if kill else 36.0) if hit else None,
        "position_x": 0.0,
        "position_y": 0.0,
        "position_z": 0.0,
        "accuracy": accuracy,
        "distance": 25.0,
        "reaction_time_ms": reaction,
        "hit": hit,
        "is_headshot": headshot,
        "is_kill": kill,
        "victim_hp_after": (0 if kill else 64) if hit else None,
        "assister_id": None,
        "event_time": EVENT_TIME,
        "server_id": "server-01",
    }


def _chain(player_id, match_id, accuracy, reaction, shots, hits, kills, kill_hs, start=0):
    """`shots` SHOT rows: the first `kills` are lethal (the first `kill_hs`
    of them headshots), the next hits - kills are non-lethal hits, and the
    rest are misses."""
    assert kill_hs <= kills <= hits <= shots
    rows = []
    for i in range(shots):
        kill = i < kills
        rows.append(_shot(start + i, player_id, match_id, accuracy, reaction,
                          hit=i < hits, headshot=i < kill_hs, kill=kill))
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
                  shots=40, hits=35, kills=30, kill_hs=27)
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
                  shots=40, hits=15, kills=10, kill_hs=1)
    scored = _score(spark, rows)["p_normal"]

    assert scored["suspicion_score"] < 0.50, (
        f"normal player must stay under console threshold, got {scored['suspicion_score']}"
    )


def test_every_shot_counts_once_in_averages(spark):
    """Regression: the simulator used to emit SHOT_FIRED + DAMAGE + KILL for
    one lethal shot, each carrying that shot's accuracy and reaction time,
    so plain averages weighted lethal shots 3x. One SHOT row per shot makes
    the average exact: (0.9 + 9 x 0.1) / 10 = 0.18, not 0.33."""
    rows = [_shot(0, "p_mix", "m1", 0.9, 100, hit=True, headshot=True, kill=True)]
    rows += [_shot(i, "p_mix", "m1", 0.1, 400) for i in range(1, 10)]
    scored = _score(spark, rows)["p_mix"]

    assert scored["total_shots"] == 10
    assert scored["avg_accuracy"] == pytest.approx(0.18, abs=1e-6)
    assert scored["avg_reaction_time"] == pytest.approx(370.0, abs=1e-6)


def test_headshot_ratio_is_per_hit_and_smoothed(spark):
    """Headshots per HIT (lethal or not), shrunk toward the population rate:
    (headshots + HS_PRIOR * HS_PRIOR_HITS) / (hits + HS_PRIOR_HITS)."""
    rows = _chain("p_hs", "m1", accuracy=0.5, reaction=250,
                  shots=40, hits=20, kills=4, kill_hs=1)
    rows += [_shot(100 + i, "p_hs", "m1", 0.5, 250, hit=True, headshot=True)
             for i in range(4)]  # 4 non-lethal headshot hits count too

    scored = _score(spark, rows)["p_hs"]
    assert scored["total_hits"] == 24
    assert scored["headshots"] == 5
    assert scored["headshot_ratio"] == pytest.approx(
        (5 + HS_PRIOR * HS_PRIOR_HITS) / (24 + HS_PRIOR_HITS), abs=1e-9)


def test_one_lucky_headshot_is_not_a_headshot_machine(spark):
    rows = _chain("p_lucky", "m1", accuracy=0.28, reaction=260,
                  shots=12, hits=1, kills=1, kill_hs=1)
    scored = _score(spark, rows)["p_lucky"]

    assert scored["headshot_ratio"] < 0.40, "1/1 must not read as a 100% headshot rate"
    assert scored["hs_score"] == 0.0


def test_all_miss_window_scores_cleanly(spark):
    """is_kill and is_headshot are null on misses; sums must still be 0."""
    rows = _chain("p_miss", "m1", accuracy=0.30, reaction=250,
                  shots=10, hits=0, kills=0, kill_hs=0)
    scored = _score(spark, rows)["p_miss"]

    assert scored["total_kills"] == 0
    assert scored["headshots"] == 0
    assert scored["headshot_ratio"] == pytest.approx(HS_PRIOR, abs=1e-9)
    assert scored["suspicion_score"] < 0.50


def test_best_window_is_the_one_with_most_shots():
    def row(player, end, shots):
        return {"player_id": player, "window": {"end": end}, "total_shots": shots}

    picked = best_window_per_player([
        row("a", 15, 30), row("a", 30, 6),    # newest window is the young one
        row("b", 15, 10), row("b", 30, 10),   # tie -> newest
    ])
    by_player = {r["player_id"]: r for r in picked}
    assert by_player["a"]["window"]["end"] == 15
    assert by_player["b"]["window"]["end"] == 30
