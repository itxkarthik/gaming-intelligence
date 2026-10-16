"""Historical cheat metrics: exact online coverage is not a heuristic sweep."""

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.batch.historical_analysis import cheat_evaluation_frames, _eval_row
from pyspark.sql import functions as F


SCHEMA = (
    "player_id string, match_id string, window struct<start:timestamp,end:timestamp>, "
    "total_shots long, suspicion_score double, suspicion_effective double, "
    "judged boolean, flagged boolean, source_event_min long, source_event_max long, "
    "archive_written_at timestamp"
)


def archived_row(player="p", raw=0.2, effective=0.2, judged=True, flagged=False,
                 start=1020, end=1050, source_min=1021000, source_max=1022000, version=1):
    return (player, "m", (datetime.fromtimestamp(start), datetime.fromtimestamp(end)),
            40, raw, effective, judged, flagged, source_min, source_max,
            datetime.fromtimestamp(2000 + version))


def evaluate(spark, tmp_path, rows, legacy=False):
    df = spark.createDataFrame(rows, SCHEMA)
    if legacy:
        df = df.drop("judged", "flagged", "source_event_min", "source_event_max")
    # latest_per_key intentionally uses file-source metadata for old archives.
    path = str(tmp_path / "cheat")
    df.write.mode("overwrite").parquet(path)
    archive = spark.read.parquet(path)
    players = sorted({row[0] for row in rows})
    labels = spark.createDataFrame([(p, "cheater_aimbot", 1000000) for p in players],
                                   "player_id string, archetype string, since long")
    return cheat_evaluation_frames(archive, labels)


def test_earlier_positive_decision_survives_latest_clean_update(spark, tmp_path):
    ev = evaluate(spark, tmp_path, [
        archived_row(raw=0.9, effective=0.9, flagged=True, version=1),
        archived_row(raw=0.2, effective=0.2, flagged=False, version=2),
    ])
    row = ev.first()
    assert row.online_covered is True
    assert row.online_flagged is True
    assert row.max_suspicion == 0.2  # raw sweep still uses the final window
    assert _eval_row(ev, "online", F.col("online_flagged"))[2:5] == [1, 0, 0]


def test_unevaluated_and_legacy_rows_are_not_online_clean_decisions(spark, tmp_path):
    ev = evaluate(spark, tmp_path, [
        archived_row(player="unevaluated", raw=0.99, judged=False, flagged=True),
        archived_row(player="legacy", raw=0.99, judged=None, flagged=True),
        archived_row(player="clean", raw=0.99, effective=1.0, judged=True, flagged=False),
    ])
    rows = {row.player_id: row for row in ev.collect()}
    assert rows["unevaluated"].online_covered is False
    assert rows["unevaluated"].online_flagged is None
    assert rows["legacy"].online_covered is False
    assert rows["legacy"].legacy_scored == 1
    assert rows["clean"].online_covered is True
    assert rows["clean"].online_flagged is False  # never OR with a heuristic proxy
    online = ev.filter("online_covered")
    assert _eval_row(online, "online", F.col("online_flagged"))[2:5] == [0, 0, 1]


def test_heuristic_sweep_uses_raw_not_behavior_boosted_scores(spark, tmp_path):
    row = evaluate(spark, tmp_path, [
        archived_row(raw=0.2, effective=0.9, flagged=True),
    ]).first()
    assert row.max_suspicion == 0.2
    assert row.online_flagged is True


def test_source_bounds_retain_initial_windows_and_reject_old_run_evidence(spark, tmp_path):
    ev = evaluate(spark, tmp_path, [
        archived_row(player="initial", start=990, end=1020,
                     source_min=1001000, source_max=1002000, flagged=True),
        archived_row(player="previous", start=990, end=1020,
                     source_min=995000, source_max=999000, flagged=True),
        archived_row(player="mixed", start=990, end=1020,
                     source_min=999000, source_max=1001000, flagged=True),
        archived_row(player="unknown_boundary", start=990, end=1020,
                     source_min=None, source_max=None, judged=None),
        archived_row(player="legacy_interior", source_min=None, source_max=None, judged=None),
    ])
    rows = {row.player_id: row for row in ev.collect()}
    assert rows["initial"].online_flagged is True
    assert rows["initial"].scored is True
    for player in ("previous", "mixed", "unknown_boundary"):
        assert rows[player].scored is False
        assert rows[player].online_covered is False
    assert rows["legacy_interior"].scored is True
    assert rows["legacy_interior"].legacy_scored == 1
    assert rows["legacy_interior"].online_covered is False


def test_legacy_only_archive_has_explicit_unknown_online_coverage(spark, tmp_path):
    row = evaluate(spark, tmp_path, [archived_row(raw=0.9)], legacy=True).first()
    assert row.scored is True
    assert row.max_suspicion == 0.9
    assert row.legacy_scored == 1
    assert row.online_covered is False
    assert row.online_flagged is None
