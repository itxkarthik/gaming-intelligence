"""Train the anti-cheat IsolationForest OFFLINE and serialize it.

ROADMAP "Approach B": fit once on population data, then only run
`score_samples` online inside streaming foreachBatch (never fit_predict on
micro-batches — that would arbitrarily flag a fixed % of every batch).

Run inside the Spark image (has scikit-learn):
    docker exec -w /opt/spark-apps gaming-spark-master \
        python3 src/ml/train_isolation_forest.py

Artifact: streaming/models/anti_cheat_isolation_forest.joblib (gitignored —
re-run this script to reproduce; random_state is fixed).
"""

import os

import numpy as np
from sklearn.ensemble import IsolationForest
import joblib

SEED = 42
MODEL_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "models")
MODEL_PATH = os.path.join(MODEL_DIR, "anti_cheat_isolation_forest.joblib")

# Population baselines mirror the simulator profiles: mostly gold-like normals
# with bronze/diamond spread, plus injected cheater-grade anomalies.
FEATURE_NAMES = ["avg_accuracy", "headshot_ratio", "avg_reaction_time"]


def generate_population(n_normal, n_anomaly, seed):
    rng = np.random.default_rng(seed)

    normals_a = rng.normal([0.28, 0.15, 260.0], [0.09, 0.05, 55.0], size=(n_normal, 3))
    # a second, weaker cluster (bronze/silver population)
    normals_b = rng.normal([0.18, 0.08, 360.0], [0.06, 0.04, 70.0], size=(n_normal // 2, 3))
    normals = np.vstack([normals_a, normals_b])

    anomalies = np.column_stack([
        rng.uniform(0.50, 0.78, n_anomaly),   # superhuman accuracy
        rng.uniform(0.30, 0.55, n_anomaly),   # headshot rates no human sustains
        rng.uniform(110, 190, n_anomaly),     # reaction times below human floor
    ])

    X = np.vstack([normals, anomalies])
    X[:, 0] = np.clip(X[:, 0], 0.02, 1.0)
    X[:, 1] = np.clip(X[:, 1], 0.0, 1.0)
    X[:, 2] = np.clip(X[:, 2], 80.0, 600.0)
    y = np.concatenate([np.zeros(len(normals)), np.ones(len(anomalies))])
    return X, y


def main():
    X_train, _ = generate_population(2000, 100, SEED)

    model = IsolationForest(
        n_estimators=150,
        contamination=0.05,
        max_samples="auto",
        random_state=SEED,
        n_jobs=1,  # fits comfortably in the driver's executor slot
    )
    model.fit(X_train)

    # Holdout sanity check on fresh draws (different seed)
    X_test, y_test = generate_population(800, 80, SEED + 1)
    scores = model.score_samples(X_test)
    flagged = scores < -0.6  # streaming threshold used by cheat_detection

    normal_flag_rate = float(flagged[y_test == 0].mean())
    anomaly_recall = float(flagged[y_test == 1].mean())

    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)

    print("=" * 64)
    print("IsolationForest trained (offline) — anti-cheat anomaly model")
    print("=" * 64)
    print(f"  features          : {FEATURE_NAMES}")
    print(f"  train rows        : {len(X_train)}")
    print(f"  artifact          : {MODEL_PATH}")
    print(f"  holdout normals   : {len(X_test) - int(y_test.sum())} rows, "
          f"flagged at -0.6 -> {normal_flag_rate*100:.1f}% (want < 8%)")
    print(f"  holdout anomalies : {int(y_test.sum())} rows, "
          f"detected -> {anomaly_recall*100:.1f}% (want > 90%)")
    verdict = "OK" if normal_flag_rate < 0.08 and anomaly_recall > 0.90 else "REVIEW"
    print(f"  verdict           : {verdict}")
    print("=" * 64)


if __name__ == "__main__":
    main()