"""Unit tests for the cheat-detection suspicion pipeline.

Runs inside the Spark container (pyspark + pytest are installed there):

    docker exec -w /opt/spark-apps gaming-spark-master python3 -m pytest tests -q
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pyspark.sql import functions as F

from src.common.schemas import GAMEPLAY_EVENT_SCHEMA, SHOT_EVENT
from src.jobs.cheat_detection import (
    FOREST_MAX_GAP_SECONDS,
    FOREST_PERSIST_SECONDS,
    best_window_per_player,
    build_suspicion_pipeline,
    cheat_verdict,
    forest_persistence,
    write_player_scores,
)
from src.ml.iforest import HS_PRIOR, HS_PRIOR_HITS

# Fixed event time => every synthetic event lands in one window.
EVENT_TIME = 1_700_000_000_000


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
    assert scored["source_event_min"] == EVENT_TIME
    assert scored["source_event_max"] == EVENT_TIME
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


def test_forest_flag_on_a_confirmed_smurf_is_not_a_cheat():
    assert cheat_verdict(0.30, iforest_flag=True, smurf=True) == (False, False)
    assert cheat_verdict(0.30, iforest_flag=True, smurf=False) == (True, True)


def test_smurf_who_trips_the_heuristic_is_still_a_cheat():
    assert cheat_verdict(0.75, iforest_flag=True, smurf=True) == (True, False)
    assert cheat_verdict(0.90, iforest_flag=False, smurf=True) == (True, True)
    assert cheat_verdict(0.50, iforest_flag=False, smurf=False) == (False, False)


def test_single_forest_window_does_not_count_until_it_persists():
    since, persistent = forest_persistence(None, True, now=1000.0)
    assert (since, persistent) == (1000.0, False)
    since, persistent = forest_persistence(since, True, now=1015.0, last_judged=1000.0)
    assert persistent is False
    assert forest_persistence(since, True, now=1000.0 + FOREST_PERSIST_SECONDS,
                              last_judged=1015.0) == (1000.0, True)


def test_contiguous_30s_window_preserves_streak_when_intermediate_slide_unselected():
    assert FOREST_MAX_GAP_SECONDS == 30
    since, persistent = forest_persistence(None, True, now=1000.0)
    assert (since, persistent) == (1000.0, False)
    # [0,30] -> [30,60] when [15,45] had fewer shots and wasn't selected:
    assert forest_persistence(since, True, now=1030.0, last_judged=1000.0) == (1000.0, True)
    # Gap > 30s (e.g. 45s) resets since:
    assert forest_persistence(since, True, now=1045.0, last_judged=1000.0) == (1045.0, False)


def test_isolated_outlier_an_hour_later_starts_over():
    assert forest_persistence(1000.0, True, now=4600.0, last_judged=1000.0) == (4600.0, False)


@pytest.mark.parametrize("last_judged,same_context", [(None, True), (1030.0, True), (1000.0, False)])
def test_missing_backwards_or_reset_evidence_starts_over(last_judged, same_context):
    assert forest_persistence(970.0, True, now=1015.0, last_judged=last_judged,
                              same_context=same_context) == (1015.0, False)


def test_repeated_window_updates_do_not_accumulate_elapsed_evidence():
    assert forest_persistence(1000.0, True, now=1000.0, last_judged=1000.0) == (1000.0, False)


def test_clean_window_resets_forest_persistence():
    assert forest_persistence(1000.0, False, now=2000.0) == (None, False)
    # a new outlier run starts from scratch
    assert forest_persistence(None, True, now=2001.0) == (2001.0, False)


def test_low_shot_window_does_not_overwrite_flagged_profile(detector):
    class FakePipe:
        def __init__(self, r):
            self.r = r
            self.ops = []

        def hgetall(self, k):
            self.ops.append(("hgetall", k))

        def hget(self, k, field):
            self.ops.append(("hget", k, field))

        def hset(self, k, mapping=None):
            self.ops.append(("hset", k, mapping))

        def sadd(self, k, v):
            self.ops.append(("sadd", k, v))

        def srem(self, k, v):
            self.ops.append(("srem", k, v))

        def execute(self):
            out = []
            for op in self.ops:
                if op[0] == "hgetall":
                    out.append(dict(self.r.hashes.get(op[1], {})))
                elif op[0] == "hget":
                    out.append(self.r.hashes.get(op[1], {}).get(op[2]))
                elif op[0] == "hset":
                    self.r.hashes.setdefault(op[1], {}).update(op[2])
                    out.append(1)
                elif op[0] == "sadd":
                    self.r.sets.setdefault(op[1], set()).add(op[2])
                    out.append(1)
                elif op[0] == "srem":
                    self.r.sets.setdefault(op[1], set()).discard(op[2])
                    out.append(1)
            self.ops.clear()
            return out

    class FakeRedis:
        def __init__(self):
            self.hashes = {
                "player:p_cheater": {
                    "player_id": "p_cheater",
                    "flagged": "true",
                    "suspicion_score": "0.910",
                    "suspicion_effective": "0.910",
                    "total_shots": "35",
                }
            }
            self.sets = {"players:flagged": {"p_cheater"}}
            self.kv = {}

        def set(self, k, v, nx=False, ex=None):
            if nx and k in self.kv:
                return False
            self.kv[k] = v
            return True

        def delete(self, *keys):
            for k in keys:
                self.kv.pop(k, None)

        def pipeline(self):
            return FakePipe(self)

    r = FakeRedis()
    low_shot_row = {
        "player_id": "p_cheater",
        "match_id": "m2",
        "window": {"end": 2000.0},
        "window_end_ms": 2000000,
        "total_shots": 3,
        "total_kills": 0,
        "headshot_ratio": 0.15,
        "avg_accuracy": 0.10,
        "avg_reaction_time": 300.0,
        "suspicion_score": 0.0,
    }
    verdicts = write_player_scores(r, None, [low_shot_row])
    assert r.hashes["player:p_cheater"]["suspicion_effective"] == "0.910"
    assert r.hashes["player:p_cheater"]["total_shots"] == "35"
    assert verdicts[0]["judged"] is False
    assert verdicts[0]["flagged"] is False


class StatePipe:
    def __init__(self, redis):
        self.redis = redis
        self.ops = []

    def __getattr__(self, name):
        def queue(*args, **kwargs):
            self.ops.append((name, args, kwargs))
            return self
        return queue

    def execute(self):
        self.redis.transactions.append([name for name, _, _ in self.ops])
        results = [getattr(self.redis, name)(*args, **kwargs)
                   for name, args, kwargs in self.ops]
        self.ops.clear()
        return results


class StateRedis:
    def __init__(self):
        self.hashes = {}
        self.kv = {}
        self.sets = {}
        self.outbox = []
        self.transactions = []
        self.flushes = 0
        self.fail_flush = False

    def pipeline(self):
        return StatePipe(self)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def expire(self, key, seconds):
        # Expiry timing is not relevant to these state transition tests.
        return key in self.hashes

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = value

    def delete(self, key):
        self.kv.pop(key, None)

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def srem(self, key, value):
        self.sets.setdefault(key, set()).discard(value)

    def stage_alerts(self, pending):
        self.outbox.extend(pending)


class StagedAlerts:
    def __init__(self, redis):
        self.redis = redis
        self.pending = []

    def emit(self, payload):
        self.pending.append(payload)

    def stage(self, pipe):
        if self.pending:
            pipe.stage_alerts(list(self.pending))
            self.pending.clear()

    def flush(self):
        if hasattr(self.redis, "flushes"):
            self.redis.flushes += 1
            if self.redis.fail_flush:
                raise RuntimeError("Kafka unavailable")
            self.redis.outbox.clear()


@pytest.fixture
def detector(monkeypatch):
    from src.jobs import cheat_detection as module
    monkeypatch.setattr(module, "AlertBatch", StagedAlerts)
    monkeypatch.setattr(module, "load_model", lambda: None)
    monkeypatch.setattr(module, "score_rows", lambda model, rows: [None] * len(rows))
    monkeypatch.setattr(module, "evaluate_smurfs", lambda *args: None)
    return module


@pytest.fixture
def outlier_detector(detector, monkeypatch):
    monkeypatch.setattr(detector, "score_rows",
                        lambda model, rows: [detector.IFOREST_THRESHOLD - 1.0] * len(rows))
    return detector


def decision_row(shots=40, suspicion=0.9, end=2000.0):
    return {
        "player_id": "p", "match_id": "m", "window": {"end": end},
        "window_end_ms": int(end * 1000),
        "total_shots": shots, "total_kills": 0, "headshot_ratio": 0.15,
        "avg_accuracy": 0.9, "avg_reaction_time": 80.0,
        "suspicion_score": suspicion,
    }


def test_never_judged_lowshot_profile_is_not_created_or_changed(detector):
    redis = StateRedis()
    for shots in (1, 3):
        verdicts = detector.write_player_scores(redis, None, [decision_row(shots=shots)])
        assert verdicts[0]["judged"] is False
        assert "player:p" not in redis.hashes
    redis.hashes["player:p"] = {"total_shots": "2", "suspicion_score": "0.1"}
    before = dict(redis.hashes["player:p"])
    detector.write_player_scores(redis, None, [decision_row(shots=3)])
    assert redis.hashes["player:p"] == before


def test_cached_decisions_share_profile_transaction_and_flush_on_replay(detector, monkeypatch):
    redis = StateRedis()
    redis.fail_flush = True
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        detector.write_player_scores(redis, None, [decision_row()], "checkpoint-a", 0)
    assert redis.hashes["player:p"]["flagged"] == "true"
    assert redis.outbox
    assert redis.transactions[-1][-5:] == ["hset", "expire", "set", "set", "stage_alerts"]
    monkeypatch.setattr(detector, "score_rows", lambda *args: pytest.fail("recomputed cached verdict"))
    redis.fail_flush = False
    replay = detector.write_player_scores(redis, None, [decision_row(suspicion=0)], "checkpoint-a", 0)
    assert replay[0]["flagged"] is True
    assert replay[0]["suspicion_effective"] == 0.9
    assert redis.flushes == 2
    assert not redis.outbox
    assert redis.get(detector.decision_cache_key("checkpoint-a", 0)) is not None


def test_forest_evidence_does_not_bridge_an_hour_gap(outlier_detector):
    redis = StateRedis()
    first = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=1000)], "query", 0)
    later = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=4600)], "query", 1)
    assert first[0]["iforest_flag"] is False
    assert later[0]["iforest_flag"] is False
    assert later[0]["flagged"] is False
    assert redis.hashes["player:cheat:evidence:p"]["since"] == "4600.0"


@pytest.mark.parametrize("reset", ["match", "login", "checkpoint"])
def test_consecutive_forest_evidence_resets_with_match_or_run(outlier_detector, reset):
    redis = StateRedis()
    redis.hashes["player:account:p"] = {"login_at": "900000"}
    for batch, end in enumerate((1000, 1015)):
        verdicts = outlier_detector.write_player_scores(
            redis, None, [decision_row(suspicion=0, end=end)], "query", batch)
        assert verdicts[0]["iforest_flag"] is False
    row = decision_row(suspicion=0, end=1030)
    query, batch = "query", 2
    if reset == "match":
        row["match_id"] = "another-match"
    elif reset == "login":
        redis.hashes["player:account:p"]["login_at"] = "1020000"
    else:
        query, batch = "new-checkpoint-query", 0
    verdicts = outlier_detector.write_player_scores(redis, None, [row], query, batch)
    assert verdicts[0]["iforest_flag"] is False
    assert verdicts[0]["flagged"] is False
    assert redis.hashes["player:cheat:evidence:p"]["since"] == "1030.0"


def test_insufficient_window_breaks_streak_without_overwriting_profile(outlier_detector):
    redis = StateRedis()
    for batch, end in enumerate((1000, 1015, 1030)):
        verdicts = outlier_detector.write_player_scores(
            redis, None, [decision_row(suspicion=0, end=end)], "query", batch)
    assert verdicts[0]["iforest_flag"] is True
    assert verdicts[0]["flagged"] is True
    profile = dict(redis.hashes["player:p"])
    low = outlier_detector.write_player_scores(
        redis, None, [decision_row(shots=3, suspicion=0, end=1045)], "query", 3)
    assert low[0]["judged"] is False
    assert redis.hashes["player:p"] == profile
    assert "p" in redis.sets["players:flagged"]
    assert redis.hashes["player:cheat:evidence:p"]["since"] == ""
    assert redis.hashes["player:cheat:evidence:p"]["last_judged_window_end_ms"] == "1030000"
    # That same young window later has enough shots; the cleared streak
    # must not recover the old start even though the time gap is only 15 s.
    judged = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=1045)], "query", 4)
    assert judged[0]["iforest_flag"] is False
    assert judged[0]["flagged"] is False
    assert redis.hashes["player:cheat:evidence:p"]["since"] == "1045.0"


def test_clean_judged_window_clears_forest_streak(outlier_detector, monkeypatch):
    redis = StateRedis()
    for batch, end in enumerate((1000, 1015)):
        outlier_detector.write_player_scores(
            redis, None, [decision_row(suspicion=0, end=end)], "query", batch)
    monkeypatch.setattr(outlier_detector, "score_rows", lambda model, rows: [None] * len(rows))
    clean = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=1030)], "query", 2)
    assert clean[0]["judged"] is True
    assert clean[0]["iforest_flag"] is False
    assert redis.hashes["player:cheat:evidence:p"]["since"] == ""
    monkeypatch.setattr(outlier_detector, "score_rows",
                        lambda model, rows: [outlier_detector.IFOREST_THRESHOLD - 1.0] * len(rows))
    next_verdict = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=1045)], "query", 3)
    assert next_verdict[0]["iforest_flag"] is False
    assert redis.hashes["player:cheat:evidence:p"]["since"] == "1045.0"


def test_cached_persistent_verdict_survives_evidence_reset(outlier_detector, monkeypatch):
    redis = StateRedis()
    for batch, end in enumerate((1000, 1015)):
        outlier_detector.write_player_scores(
            redis, None, [decision_row(suspicion=0, end=end)], "query", batch)
    redis.fail_flush = True
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        outlier_detector.write_player_scores(
            redis, None, [decision_row(suspicion=0, end=1030)], "query", 2)
    redis.hashes["player:cheat:evidence:p"]["since"] = ""
    before = dict(redis.hashes["player:cheat:evidence:p"])
    monkeypatch.setattr(outlier_detector, "score_rows", lambda *args: pytest.fail("recomputed verdict"))
    redis.fail_flush = False
    replay = outlier_detector.write_player_scores(
        redis, None, [decision_row(suspicion=0, end=4600)], "query", 2)
    assert replay[0]["window_end_ms"] == 1030000
    assert replay[0]["iforest_flag"] is True
    assert replay[0]["flagged"] is True
    assert redis.hashes["player:cheat:evidence:p"] == before
    assert not redis.outbox


def test_missing_committed_decisions_refuses_recomputation(detector, monkeypatch):
    redis = StateRedis()
    detector.write_player_scores(redis, None, [decision_row()], "query", 0)
    redis.delete(detector.decision_cache_key("query", 0))
    monkeypatch.setattr(detector, "score_rows", lambda *args: pytest.fail("recomputed verdict"))
    with pytest.raises(RuntimeError, match="refusing to recompute"):
        detector.write_player_scores(redis, None, [decision_row()], "query", 0)


def test_checkpoint_generation_prevents_reset_batch_id_collision(detector):
    redis = StateRedis()
    first = detector.write_player_scores(redis, None, [decision_row()], "old-query", 0)
    second = detector.write_player_scores(redis, None, [decision_row(suspicion=0)], "new-query", 0)
    assert first[0]["flagged"] is True
    assert second[0]["flagged"] is False


def test_later_batch_replaces_retained_cache(detector):
    import json
    redis = StateRedis()
    detector.write_player_scores(redis, None, [decision_row()], "query", 0)
    detector.write_player_scores(redis, None, [decision_row(suspicion=0)], "query", 1)
    saved = json.loads(redis.get(detector.decision_cache_key("query", 1)))
    assert redis.get(detector.decision_cache_key("query", 0)) is None
    assert saved["batch_id"] == 1
    assert saved["verdicts"][0]["flagged"] is False
    with pytest.raises(RuntimeError, match="older"):
        detector.write_player_scores(redis, None, [decision_row()], "query", 0)


def test_unselected_highshot_window_is_not_judged(spark, detector):
    from datetime import datetime
    df = spark.createDataFrame([
        ("p", "m", (datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 0, 30)), 40, 0.9),
        ("p", "m", (datetime(2024, 1, 1, 0, 0, 15), datetime(2024, 1, 1, 0, 0, 45)), 30, 0.8),
    ], "player_id string, match_id string, window struct<start:timestamp,end:timestamp>, "
       "total_shots long, suspicion_score double")
    row = decision_row()
    row.update(df.withColumn("window_end_ms", F.unix_millis("window.end")).first().asDict())
    verdicts = detector.write_player_scores(StateRedis(), None, [row])
    archived = detector._enrich_batch_df(df, verdicts).orderBy("window.end").collect()
    assert archived[0].judged is True
    assert archived[1].judged is False


def test_integer_window_identity_handles_fractional_seconds_and_timezone(spark, detector):
    from datetime import datetime, timezone
    end = datetime(2024, 1, 1, 0, 0, 30, 123456, tzinfo=timezone.utc)
    df = spark.createDataFrame([
        ("p", "m", (datetime(2024, 1, 1, tzinfo=timezone.utc), end), 40, 0.9),
    ], "player_id string, match_id string, window struct<start:timestamp,end:timestamp>, "
       "total_shots long, suspicion_score double")
    row = decision_row()
    row.update(df.withColumn("window_end_ms", F.unix_millis("window.end")).first().asDict())
    verdicts = detector.write_player_scores(StateRedis(), None, [row])
    assert verdicts[0]["window_end_ms"] == 1704067230123
    old_timezone = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
        archived = detector._enrich_batch_df(df, verdicts).first()
        assert archived.window_end_ms == verdicts[0]["window_end_ms"]
        assert archived.judged is True
        assert archived.flagged is True
    finally:
        spark.conf.set("spark.sql.session.timeZone", old_timezone)


def test_archive_failure_replays_original_decision(spark, detector, monkeypatch):
    from contextlib import nullcontext
    from datetime import datetime
    redis = StateRedis()
    df = spark.createDataFrame([
        ("p", "m", (datetime(2024, 1, 1), datetime(2024, 1, 1, 0, 0, 30)),
         40, 0, 0.15, 0.9, 80.0, 0.9),
    ], "player_id string, match_id string, window struct<start:timestamp,end:timestamp>, "
       "total_shots long, total_kills long, headshot_ratio double, avg_accuracy double, "
       "avg_reaction_time double, suspicion_score double")
    monkeypatch.setattr(detector, "redis_client", lambda: redis)
    monkeypatch.setattr(detector, "cached_batch", lambda df: nullcontext())
    monkeypatch.setattr(detector, "run_with_retry", lambda label, fn: fn())
    monkeypatch.setattr(detector, "checkpoint_identity", lambda spark: "query")
    attempts = []

    def archive(enriched, name, batch_id):
        attempts.append([row.asDict(recursive=True) for row in enriched.collect()])
        if len(attempts) == 1:
            raise RuntimeError("archive unavailable")

    monkeypatch.setattr(detector, "safe_parquet_archive", archive)
    with pytest.raises(RuntimeError, match="archive unavailable"):
        detector.write_player_scores_batch(df, 0)
    monkeypatch.setattr(detector, "score_rows", lambda *args: pytest.fail("recomputed verdict"))
    detector.write_player_scores_batch(df, 0)
    assert attempts[0] == attempts[1]
    assert redis.get(detector.decision_cache_key("query", 0)) is not None


def test_checkpoint_identity_reads_persisted_query_uuid(spark, detector, tmp_path, monkeypatch):
    import json
    assert detector.CHEAT_CHECKPOINT == "cheat_detection_v2"
    old_checkpoint = tmp_path / "cheat_detection"
    old_checkpoint.mkdir()
    (old_checkpoint / "metadata").write_text(json.dumps({"id": "incompatible-old-query"}))
    checkpoint = tmp_path / detector.CHEAT_CHECKPOINT
    checkpoint.mkdir()
    metadata = checkpoint / "metadata"
    monkeypatch.setattr(detector, "CHECKPOINT_DIR", str(tmp_path))
    metadata.write_text(json.dumps({"id": "original-query"}))
    assert detector.checkpoint_identity(spark) == "original-query"
    metadata.write_text(json.dumps({"id": "reset-query"}))
    assert detector.checkpoint_identity(spark) == "reset-query"
