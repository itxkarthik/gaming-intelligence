"""Unit tests for CUSUM behavior-change detection and the cross-job boost."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.jobs.behavior_change import (
    cusum_step,
    apply_behavior_boost,
    load_state,
    BEHAVIOR_BOOST,
)


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
    # One outlier is absorbed by the CUSUM slack (k=0.5, h=5.0)
    series = [0.28] * 40 + [0.75] + [0.28] * 5
    state, anomalies = _run(series)
    assert anomalies == 0, "one-shot spike must accumulate < h evidence"


def test_cusum_state_roundtrip():
    state, anomalies = _run([0.10] * 5 + [0.45] * 15)
    restored = load_state({k: str(v) for k, v in state.items()})
    assert restored == state
    assert restored["n"] == 20


def test_behavior_boost_clamps_and_applies():
    assert apply_behavior_boost(0.65, False) == 0.65
    assert apply_behavior_boost(0.65, True) == round(0.65 + BEHAVIOR_BOOST, 4)
    assert apply_behavior_boost(0.95, True) == 1.0  # clamped at 1.0
    assert apply_behavior_boost(0, True) == BEHAVIOR_BOOST