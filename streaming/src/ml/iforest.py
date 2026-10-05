"""IsolationForest contract shared by offline training and streaming scoring.

The model is fit OFFLINE (src/ml/train_isolation_forest.py) and only scored
online, on the per-window features cheat_detection computes. Both sides take
the feature order, artifact path and decision threshold from here, so a
retrain can never silently disagree with the scorer.
"""

import os

import numpy as np

from src.common.runtime import warn

# Per-window features in model column order (cheat_detection column names).
# headshot_ratio is per HIT (the profiles' headshot_ratio), smoothed below.
FEATURE_NAMES = ("avg_accuracy", "headshot_ratio", "avg_reaction_time")
IFOREST_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "anti_cheat_isolation_forest.joblib")
# score_samples below this = anomaly (calibrated on the training holdout)
IFOREST_THRESHOLD = -0.6

# Evidence rules shared by training and scoring. A window is judged only
# with MIN_SHOTS shots (~4 hits at normal accuracy), and headshot_ratio is
# shrunk toward the population rate with a HS_PRIOR_HITS-hit prior: one
# headshot from one hit is not a 100% headshot player.
MIN_SHOTS = 12
HS_PRIOR = 0.15
HS_PRIOR_HITS = 4.0


def smoothed_headshot_ratio(headshots, hits):
    """Headshots per hit, shrunk toward HS_PRIOR (works on numbers and Spark columns)."""
    return (headshots + HS_PRIOR * HS_PRIOR_HITS) / (hits + HS_PRIOR_HITS)

_model = None
_load_attempted = False


def feature_matrix(rows):
    """Rows (dict-like, FEATURE_NAMES keys) -> (n, 3) float array; nulls -> 0."""
    return np.array([[float(row[name] or 0.0) for name in FEATURE_NAMES] for row in rows],
                    dtype=float).reshape(len(rows), len(FEATURE_NAMES))


def load_model():
    """The trained model, loaded once per process; None if unavailable."""
    global _model, _load_attempted
    if not _load_attempted:
        _load_attempted = True
        if not os.path.exists(IFOREST_PATH):
            warn(f"no IsolationForest at {IFOREST_PATH} (run `make train-model`); "
                 "scoring disabled")
        else:
            try:
                import joblib
                _model = joblib.load(IFOREST_PATH)
            except Exception as e:  # noqa: BLE001 - unpickling fails many ways; scoring is optional
                warn(f"IsolationForest load failed, scoring disabled: {e}")
    return _model


def score_rows(model, rows):
    """score_samples per row; all None when there is no model."""
    if model is None or not rows:
        return [None] * len(rows)
    return [float(s) for s in model.score_samples(feature_matrix(rows))]
