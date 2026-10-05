"""Train the anti-cheat IsolationForest OFFLINE and serialize it.

Fit once, then only `score_samples` online inside cheat_detection's
foreachBatch (never fit on micro-batches: that would flag a fixed share of
every batch). The forest learns what LEGITIMATE play looks like, so it is fit
on non-cheater windows only; cheater windows are generated for the holdout.
Smurfs and toxic players are legitimate humans and part of the fit, so the
cheat model does not flag skilled new accounts (smurf_detection's job).

Training rows are simulated 30-second windows, built shot by shot from the
simulator profiles exactly as cheat_detection aggregates them: mean per-shot
accuracy, headshots per hit, mean reaction time. Window-level noise (a few
hits make the headshot ratio jumpy) is therefore part of what the model
learns as normal.

Run inside the Spark image (has scikit-learn): `make train-model`.
Artifact: streaming/models/anti_cheat_isolation_forest.joblib (untracked;
random_state is fixed, so re-running reproduces it).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from src.ml.iforest import (
    FEATURE_NAMES,
    IFOREST_PATH,
    IFOREST_THRESHOLD,
    MIN_SHOTS,
    score_rows,
    smoothed_headshot_ratio,
)

SEED = 42
MAX_SHOTS = 40  # a busy 30 s window; MIN_SHOTS is the scorer's evidence floor

# (mean, std) of accuracy, headshot-per-hit and reaction ms: simulator/profiles/*.yaml
PROFILES = {
    "normal_bronze": ((0.18, 0.06), (0.08, 0.03), (380.0, 80.0)),
    "normal_gold": ((0.28, 0.08), (0.15, 0.05), (260.0, 50.0)),
    "normal_diamond": ((0.42, 0.07), (0.28, 0.06), (190.0, 30.0)),
    "smurf": ((0.62, 0.06), (0.35, 0.05), (160.0, 25.0)),
    "toxic": ((0.25, 0.08), (0.12, 0.04), (280.0, 60.0)),
    "cheater_aimbot": ((0.93, 0.03), (0.88, 0.05), (85.0, 10.0)),
    "cheater_wallhack": ((0.45, 0.08), (0.20, 0.06), (120.0, 20.0)),
}
# Legitimate population shares at the simulator defaults (5% smurf, 5% toxic,
# normals split 40/40/20 bronze/gold/diamond; the 5% cheaters are left out)
LEGIT_MIX = {"normal_bronze": 0.34, "normal_gold": 0.34, "normal_diamond": 0.17,
             "smurf": 0.05, "toxic": 0.05}


def simulate_windows(profile, n, rng):
    """n windows of one profile -> {feature: value} rows, as Spark computes them."""
    (acc_m, acc_s), (hs_m, hs_s), (rxn_m, rxn_s) = PROFILES[profile]
    rows = []
    for shots in rng.integers(MIN_SHOTS, MAX_SHOTS + 1, size=n):
        accuracy = np.clip(rng.normal(acc_m, acc_s, shots), 0.05, 1.0)
        hit = rng.random(shots) < accuracy
        headshot = hit & (rng.random(shots) < np.clip(rng.normal(hs_m, hs_s, shots), 0.02, 1.0))
        hits = int(hit.sum())
        rows.append({
            "avg_accuracy": float(accuracy.mean()),
            "headshot_ratio": float(smoothed_headshot_ratio(int(headshot.sum()), hits)),
            "avg_reaction_time": float(np.clip(rng.normal(rxn_m, rxn_s, shots), 50, 600).mean()),
        })
    return rows


def legit_windows(n, rng):
    total = sum(LEGIT_MIX.values())
    rows = []
    for profile, share in LEGIT_MIX.items():
        rows += simulate_windows(profile, int(n * share / total), rng)
    return rows


def train(n_windows=6000, seed=SEED):
    """Fit on legitimate windows only. contamination stays at its default: it
    only sets predict()'s offset, and scoring compares score_samples against
    IFOREST_THRESHOLD instead."""
    rng = np.random.default_rng(seed)
    X = np.array([[row[f] for f in FEATURE_NAMES] for row in legit_windows(n_windows, rng)])
    model = IsolationForest(n_estimators=200, max_samples=min(1024, len(X)),
                            random_state=seed, n_jobs=1)
    return model.fit(X)


def flag_rates(model, n_windows=1000, seed=SEED + 1):
    """Holdout share of windows flagged at IFOREST_THRESHOLD, per profile."""
    rng = np.random.default_rng(seed)
    rates = {}
    for profile in PROFILES:
        scores = score_rows(model, simulate_windows(profile, n_windows, rng))
        rates[profile] = float(np.mean([s < IFOREST_THRESHOLD for s in scores]))
    return rates


def main():
    model = train()
    os.makedirs(os.path.dirname(IFOREST_PATH), exist_ok=True)
    joblib.dump(model, IFOREST_PATH)
    print(f"IsolationForest -> {IFOREST_PATH} (features {', '.join(FEATURE_NAMES)})")
    for profile, rate in flag_rates(model).items():
        print(f"  {profile:<17} windows flagged at {IFOREST_THRESHOLD}: {rate * 100:5.1f}%")


if __name__ == "__main__":
    main()
