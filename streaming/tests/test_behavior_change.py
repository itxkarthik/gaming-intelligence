"""Unit tests for CUSUM behavior-change detection and the cross-job boost."""

import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.jobs.behavior_change import (
    BEHAVIOR_BOOST,
    BEHAVIOR_STATE_VERSION,
    WARMUP_STEPS,
    apply_behavior_boost,
    cusum_step,
    load_state,
    update_behavior,
)


class _FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def hgetall(self, k):
        self.ops.append(("hgetall", k))

    def exists(self, k):
        self.ops.append(("exists", k))

    def hset(self, k, mapping=None):
        self.ops.append(("hset", k, mapping))

    def expire(self, k, ttl):
        self.ops.append(("expire", k, ttl))

    def sadd(self, k, *vals):
        self.ops.append(("sadd", k, vals))

    def srem(self, k, *vals):
        self.ops.append(("srem", k, vals))

    def zadd(self, k, mapping):
        self.ops.append(("zadd", k, mapping))

    def zrem(self, k, *vals):
        self.ops.append(("zrem", k, vals))

    def zremrangebyscore(self, k, lo, hi):
        self.ops.append(("zremrangebyscore", k, lo, hi))

    def set(self, k, v):
        self.ops.append(("set", k, v))

    def execute(self):
        out = []
        for op in self.ops:
            kind = op[0]
            if kind == "hgetall":
                out.append(dict(self.r.hashes.get(op[1], {})))
            elif kind == "exists":
                out.append(1 if op[1] in self.r.hashes else 0)
            elif kind == "hset":
                self.r.hashes.setdefault(op[1], {}).update(op[2])
                out.append(1)
            elif kind == "sadd":
                self.r.sets.setdefault(op[1], set()).update(op[2])
                out.append(1)
            elif kind == "srem":
                self.r.sets.setdefault(op[1], set()).difference_update(op[2])
                out.append(1)
            elif kind == "set":
                self.r.kv[op[1]] = op[2]
                out.append(True)
            elif kind == "alert_intent":
                self.r.outbox.append(op[1])
                out.append(1)
            else:
                out.append(1)
        self.ops.clear()
        return out


class _FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.sets = {}
        self.outbox = []
        self.kv = {"behavior:anomalies:expiry:migrated:v1": "1"}

    def exists(self, k):
        return k in self.kv or k in self.hashes

    def smembers(self, k):
        return set(self.sets.get(k, set()))

    def zrangebyscore(self, k, lo, hi):
        return []

    def set(self, k, v, nx=False, ex=None):
        if nx and k in self.kv:
            return False
        self.kv[k] = v
        return True

    def pipeline(self):
        return _FakePipe(self)


class _DummyAlerts:
    def __init__(self, r=None):
        self.r = r
        self.emitted = []
        self.staged_ops = []

    def emit(self, payload):
        self.emitted.append(payload)

    def stage(self, pipe):
        self.staged_ops = list(pipe.ops)
        for payload in self.emitted:
            pipe.ops.append(("alert_intent", payload))

    def flush(self):
        self.r.outbox.clear()


def _run(series, state=None):
    state = state or load_state(None)
    anomalies = 0
    for x in series:
        state, anomaly, _ = cusum_step(state, x)
        anomalies += int(anomaly)
    return state, anomalies


def test_stable_series_never_flags():
    state, anomalies = _run([0.30, 0.28, 0.31, 0.29, 0.30, 0.27, 0.32] * 6)
    assert anomalies == 0
    assert state["n"] == 42


def test_sustained_shift_flags_once():
    # Baseline established, then a persistent 3σ jump
    baseline = [0.28] * 30
    state, anomalies = _run(baseline)
    assert anomalies == 0

    shifted, anomalies_after = _run([0.60] * 12, state=state)
    assert anomalies_after >= 1
    assert shifted["anomalies"] >= 1


def test_single_spike_does_not_flag():
    # One outlier is absorbed by the CUSUM slack (k=0.5, h=8.0)
    series = [0.28] * 40 + [0.75] + [0.28] * 5
    _, anomalies = _run(series)
    assert anomalies == 0, "one-shot spike must accumulate < h evidence"


def test_cusum_state_roundtrip():
    series = [0.10, 0.12] * 6 + [0.45] * 8
    state, _ = _run(series)
    restored = load_state({k: str(v) for k, v in state.items()})
    assert restored == state
    # Alarm observations are counted as detections but excluded from the
    # in-control baseline sample count.
    assert restored["n"] == len(series) - restored["anomalies"]


def test_behavior_boost_clamps_and_applies():
    assert apply_behavior_boost(0.65, False) == 0.65
    assert apply_behavior_boost(0.65, True) == round(0.65 + BEHAVIOR_BOOST, 4)
    assert apply_behavior_boost(0.95, True) == 1.0  # clamped at 1.0
    assert apply_behavior_boost(0, True) == BEHAVIOR_BOOST


def test_no_verdict_while_the_baseline_warms_up():
    # A jump inside the warm-up only widens the baseline; it is never judged.
    _, anomalies = _run([0.28, 0.29] + [0.60] * (WARMUP_STEPS - 2))
    assert anomalies == 0


def test_noisy_but_stable_player_never_flags():
    # Batch means of a few shots scatter widely around a steady skill level.
    series = [0.28, 0.36, 0.21, 0.31, 0.25, 0.34, 0.19, 0.30, 0.27, 0.33] * 10
    _, anomalies = _run(series)
    assert anomalies == 0


def test_windowed_steps_advance_chronologically_and_skip_replays():
    r = _FakeRedis()
    alerts = _DummyAlerts()

    def win_step(sec, acc):
        end = datetime.fromtimestamp(1_700_000_000 + sec, tz=timezone.utc)
        return {"player_id": "p1", "shots": 5, "mean_accuracy": acc, "window": {"end": end}}

    # Out-of-order backlog windows in one micro-batch must sort chronologically
    batch = [win_step(i * 5, 0.28) for i in range(1, 13)]
    batch_shuffled = batch[6:] + batch[:6]
    update_behavior(r, alerts, batch_shuffled)
    assert int(r.hashes["behavior:cusum:p1"]["n"]) == 12
    assert int(r.hashes["behavior:cusum:p1"]["last_window_end_ms"]) == 1_700_000_060_000

    # Replaying the same batch must not advance n again
    update_behavior(r, alerts, batch_shuffled)
    assert int(r.hashes["behavior:cusum:p1"]["n"]) == 12


@pytest.mark.parametrize("old_version", [None, "1"])
def test_legacy_detector_rebuilds_atomically_and_does_not_reset_on_replay(old_version):
    r = _FakeRedis()
    legacy = {
        "n": "400", "mean": "0.95", "m2": "12",
        "s_pos": "7", "s_neg": "-7", "anomalies": "9",
        "last_window_end_ms": "1700000060000",
    }
    if old_version is not None:
        legacy["state_version"] = old_version
    r.hashes["behavior:cusum:p1"] = dict(legacy)

    def step(index, accuracy):
        return {
            "player_id": "p1", "shots": 3, "mean_accuracy": accuracy,
            "window": {"end": 1_700_000_000_000 + index * 5000},
        }

    # Both corrected windows precede the invalid legacy cursor.
    steps = [step(2, 0.4), step(1, 0.2)]

    class FailingAlerts(_DummyAlerts):
        def stage(self, pipe):
            super().stage(pipe)
            raise RuntimeError("migration transaction not committed")

    with pytest.raises(RuntimeError, match="migration transaction not committed"):
        update_behavior(r, FailingAlerts(), steps)
    assert r.hashes["behavior:cusum:p1"] == legacy

    alerts = _DummyAlerts()
    update_behavior(r, alerts, steps)
    rebuilt = dict(r.hashes["behavior:cusum:p1"])
    assert rebuilt == {
        "state_version": BEHAVIOR_STATE_VERSION,
        "n": "2", "mean": "0.300000", "m2": "0.020000",
        "s_pos": "0.0000", "s_neg": "0.0000", "anomalies": "0",
        "last_window_end_ms": "1700000010000",
    }
    assert alerts.emitted == []
    # Version, corrected cursor and baseline are queued in the same HSET,
    # immediately before alert staging and the transaction's single execute.
    assert next(
        op[2] for op in alerts.staged_ops
        if op[:2] == ("hset", "behavior:cusum:p1")
    ) == rebuilt

    update_behavior(r, _DummyAlerts(), list(reversed(steps)) + steps)
    assert r.hashes["behavior:cusum:p1"] == rebuilt
    update_behavior(r, _DummyAlerts(), [step(3, 0.6)] + steps)
    assert r.hashes["behavior:cusum:p1"]["n"] == "3"
    assert r.hashes["behavior:cusum:p1"]["mean"] == "0.400000"
    assert r.hashes["behavior:cusum:p1"]["last_window_end_ms"] == "1700000015000"


def _alarm_steps():
    # Three clipped high observations cross h=8 after an established baseline.
    return [
        {"player_id": "p1", "shots": 3, "mean_accuracy": 0.9,
         "window": {"end": 1_700_000_000_000 + i * 5000}}
        for i in range(1, 4)
    ]


def _alarm_redis():
    r = _FakeRedis()
    r.hashes["behavior:cusum:p1"] = {
        "state_version": BEHAVIOR_STATE_VERSION,
        "last_window_end_ms": "1700000000000",
        "n": "30", "mean": "0.28", "m2": "0",
        "s_pos": "0", "s_neg": "0", "anomalies": "0",
    }
    return r


def test_alert_intent_is_staged_with_state_and_cursor():
    r = _alarm_redis()
    alerts = _DummyAlerts()
    update_behavior(r, alerts, list(reversed(_alarm_steps())))

    assert len(r.outbox) == 1
    assert r.outbox == alerts.emitted
    state_write = next(
        op for op in alerts.staged_ops
        if op[:2] == ("hset", "behavior:cusum:p1")
    )
    assert state_write[2]["state_version"] == BEHAVIOR_STATE_VERSION
    assert state_write[2]["last_window_end_ms"] == "1700000015000"
    assert state_write[2]["anomalies"] == "1"
    # A replay cannot produce a second alert intent or advance the baseline.
    saved = dict(r.hashes["behavior:cusum:p1"])
    update_behavior(r, _DummyAlerts(), _alarm_steps())
    assert r.hashes["behavior:cusum:p1"] == saved
    assert len(r.outbox) == 1


def test_staging_failure_does_not_commit_state_or_cursor():
    class FailingAlerts(_DummyAlerts):
        def stage(self, pipe):
            super().stage(pipe)
            raise RuntimeError("stage failed")

    r = _alarm_redis()
    saved = dict(r.hashes["behavior:cusum:p1"])
    with pytest.raises(RuntimeError, match="stage failed"):
        update_behavior(r, FailingAlerts(), _alarm_steps())
    assert r.hashes["behavior:cusum:p1"] == saved
    assert r.outbox == []
    update_behavior(r, _DummyAlerts(), _alarm_steps())
    assert len(r.outbox) == 1


def test_failed_flush_propagates_and_replay_drains_existing_intent(monkeypatch):
    from src.jobs import behavior_change

    class FailingOnceAlerts(_DummyAlerts):
        fail = True

        def flush(self):
            if self.fail:
                type(self).fail = False
                raise RuntimeError("Kafka unavailable")
            super().flush()

    r = _alarm_redis()
    monkeypatch.setattr(behavior_change, "redis_client", lambda: r)
    monkeypatch.setattr(behavior_change, "AlertBatch", FailingOnceAlerts)
    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        behavior_change._write_behavior_steps(_alarm_steps())
    saved = dict(r.hashes["behavior:cusum:p1"])
    assert len(r.outbox) == 1

    behavior_change._write_behavior_steps(list(reversed(_alarm_steps())))
    assert r.hashes["behavior:cusum:p1"] == saved
    assert r.outbox == []


def test_empty_completed_batch_still_retries_outbox_flush(monkeypatch):
    from src.jobs import behavior_change

    r = _FakeRedis()
    r.outbox.append({"alert_id": "pending"})
    monkeypatch.setattr(behavior_change, "redis_client", lambda: r)
    monkeypatch.setattr(behavior_change, "AlertBatch", _DummyAlerts)

    class EmptyBatch:
        def collect(self):
            return []

    behavior_change.write_behavior_batch(EmptyBatch(), 0)
    assert r.outbox == []
