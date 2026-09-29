"""Smurf Detection Streaming Pipeline.

Fresh accounts whose combat performance far exceeds their declared rank are
flagged as probable smurfs. Trigger: LOGIN events on player_events carrying
account_age_days / games_played / rank metadata (simulator ground truth lives
in `archetype` but is never used for scoring — only for later evaluation).

Logic (ROADMAP "Smurf Detection"):
    candidate  = account_age_days < 14 AND games_played < 30
    skill z    = max( (avg_accuracy - rank_acc_mean)/acc_std,
                      (rank_rxn_mean - avg_reaction_time)/rxn_std )
    probability= sigmoid(2 * (skill_z - 2))     # z=2 -> 0.5, z=3 -> 0.88
    FLAG when probability >= 0.85

Combat features come from Redis `player:{id}` (written by cheat_detection) —
a deliberate cross-job data flow instead of a third stream join.
"""

import sys
import os
import json
import math
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    REDIS_HOST,
    REDIS_PORT,
    CHECKPOINT_DIR
)
from src.common.schemas import parse_player_stream
from src.common.alerts import should_emit_alert
from src.common.sinks import safe_parquet_archive

CANDIDATE_MAX_AGE_DAYS = 14
CANDIDATE_MAX_GAMES = 30
SMURF_PROB_THRESHOLD = 0.85
MIN_COMBAT_SHOTS = 8

# Declared-rank population baselines (acc_mean, acc_std, rxn_mean, rxn_std),
# mirroring the simulator's YAML profile distributions.
RANK_BASELINES = {
    "bronze":  (0.18, 0.06, 380.0, 80.0),
    "silver":  (0.23, 0.07, 320.0, 60.0),
    "gold":    (0.28, 0.08, 260.0, 50.0),
    "diamond": (0.42, 0.07, 190.0, 30.0),
}


def _sigmoid(x):
    if x > 30:
        return 1.0
    if x < -30:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def evaluate_smurf(account, combat_stats):
    """Return (smurf_probability, reason). Pure — unit-testable.

    account:     {"age_days": int, "games_played": int, "rank": str}
    combat_stats: {"total_shots": int, "avg_accuracy": float,
                   "avg_reaction_time": float} or None when no history.
    """
    if account["age_days"] >= CANDIDATE_MAX_AGE_DAYS:
        return 0.0, f"established account ({account['age_days']}d)"
    if account["games_played"] >= CANDIDATE_MAX_GAMES:
        return 0.0, f"established account ({account['games_played']} games)"
    if not combat_stats or int(combat_stats.get("total_shots", 0)) < MIN_COMBAT_SHOTS:
        return 0.0, "insufficient combat data"

    acc_mean, acc_std, rxn_mean, rxn_std = RANK_BASELINES.get(
        account["rank"], RANK_BASELINES["gold"])

    z_acc = (float(combat_stats["avg_accuracy"]) - acc_mean) / acc_std
    z_rxn = (rxn_mean - float(combat_stats["avg_reaction_time"])) / rxn_std
    skill_z = max(z_acc, z_rxn)

    prob = _sigmoid(2.0 * (skill_z - 2.0))
    return prob, f"skill_z={skill_z:.2f} vs declared {account['rank']} (acc {z_acc:.1f}σ, rxn {z_rxn:.1f}σ)"


def write_smurf_batch(batch_df, batch_id):
    """Evaluate LOGIN candidates against Redis combat history."""
    if batch_df.isEmpty():
        return

    import redis

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

        for row in batch_df.collect():
            md = row["metadata"] or {}
            account = {
                "age_days": int(md.get("account_age_days", 10 * 365)),
                "games_played": int(md.get("games_played", 10 ** 6)),
                "rank": md.get("rank", "gold"),
            }
            if (account["age_days"] >= CANDIDATE_MAX_AGE_DAYS
                    or account["games_played"] >= CANDIDATE_MAX_GAMES):
                continue  # not a candidate — never even reads combat history

            player_id = row["player_id"]
            raw = r.hgetall(f"player:{player_id}")
            combat = None
            if raw:
                combat = {
                    "total_shots": int(raw.get("total_shots", 0)),
                    "avg_accuracy": float(raw.get("avg_accuracy", 0.0)),
                    "avg_reaction_time": float(raw.get("avg_reaction_time_ms", 999.0)),
                }

            prob, reason = evaluate_smurf(account, combat)
            evaluation = {
                "player_id": player_id,
                "probability": f"{prob:.3f}",
                "status": "SMURF" if prob >= SMURF_PROB_THRESHOLD else "CLEAN",
                "account_age_days": str(account["age_days"]),
                "games_played": str(account["games_played"]),
                "declared_rank": account["rank"],
                "archetype": md.get("archetype", ""),
                "reason": reason,
                "updated_at": str(int(time.time())),
            }
            r.hset(f"player:smurf:{player_id}", mapping=evaluation)

            if prob >= SMURF_PROB_THRESHOLD:
                r.sadd("players:smurf", player_id)
                if should_emit_alert(r, "SMURF_DETECTED", player_id):
                    alert_payload = {
                        "alert_id": f"smurf_{player_id}_{int(time.time())}",
                        "alert_type": "SMURF_DETECTED",
                        "severity": "WARNING",
                        "entity_type": "PLAYER",
                        "entity_id": player_id,
                        "message": (f"New account ({account['age_days']}d, "
                                    f"{account['games_played']} games) performs at "
                                    f"{prob*100:.0f}% smurf probability — {reason}"),
                        "details": evaluation,
                        "timestamp": int(time.time() * 1000),
                    }
                    r.lpush("alerts:recent", json.dumps(alert_payload))
                    r.ltrim("alerts:recent", 0, 99)
    except Exception as e:
        print(f"[WARN] smurf batch {batch_id}: {e}", file=sys.stderr)

    safe_parquet_archive(batch_df, "smurf_detection", batch_id)


def start_queries(spark):
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPICS["player"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    logins = parse_player_stream(raw).filter(F.col("event_type") == "LOGIN")

    query = logins.writeStream \
        .outputMode("append") \
        .foreachBatch(write_smurf_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "smurf_detection")) \
        .trigger(processingTime="5 seconds") \
        .start()
    return query


def main():
    spark = SparkSession.builder \
        .appName("Gaming-SmurfDetection") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topic: {KAFKA_TOPICS['player']} (LOGIN)")
    start_queries(spark)
    print("Smurf Detection streaming pipeline started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()