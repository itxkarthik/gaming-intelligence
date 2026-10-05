"""Unit tests for the IsolationForest scoring contract (src/ml/iforest.py)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
from sklearn.ensemble import IsolationForest

from src.ml.iforest import FEATURE_NAMES, IFOREST_THRESHOLD, feature_matrix, score_rows


def _tiny_model(seed=42):
    rng = np.random.default_rng(seed)
    normals = rng.normal([0.28, 0.15, 260.0], [0.05, 0.03, 30.0], size=(300, 3))
    model = IsolationForest(n_estimators=60, contamination=0.05, random_state=seed)
    model.fit(normals)
    return model


def test_null_model_returns_none_scores():
    rows = [{"avg_accuracy": 0.5, "headshot_ratio": 0.3, "avg_reaction_time": 150.0}]
    assert score_rows(None, rows) == [None]
    assert score_rows(None, []) == []


def test_normal_row_scores_above_threshold():
    model = _tiny_model()
    rows = [{
        "avg_accuracy": 0.29, "headshot_ratio": 0.14, "avg_reaction_time": 265.0,
    }]
    (score,) = score_rows(model, rows)
    assert score is not None
    assert score > IFOREST_THRESHOLD, "population-typical player must NOT flag"


def test_cheater_grade_row_flags():
    model = _tiny_model()
    rows = [
        {"avg_accuracy": 0.29, "headshot_ratio": 0.14, "avg_reaction_time": 265.0},
        {"avg_accuracy": 0.72, "headshot_ratio": 0.48, "avg_reaction_time": 120.0},
    ]
    normal_score, anomaly_score = score_rows(model, rows)
    assert anomaly_score < IFOREST_THRESHOLD, "superhuman profile must flag"
    assert anomaly_score < normal_score


def test_scores_are_deterministic_for_fixed_model():
    model = _tiny_model()
    rows = [{"avg_accuracy": 0.5, "headshot_ratio": 0.3, "avg_reaction_time": 150.0}]
    first = score_rows(model, rows)
    second = score_rows(model, rows)
    assert first == second


def test_feature_matrix_follows_model_column_order():
    row = {"avg_reaction_time": 150.0, "avg_accuracy": 0.5, "headshot_ratio": None}
    assert FEATURE_NAMES == ("avg_accuracy", "headshot_ratio", "avg_reaction_time")
    assert feature_matrix([row]).tolist() == [[0.5, 0.0, 150.0]]
    assert feature_matrix([]).shape == (0, 3)
