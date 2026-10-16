"""Smurf Detection.

Fresh accounts whose combat performance far exceeds their declared rank are
flagged as probable smurfs. Two halves, because LOGIN arrives once per run,
before any combat:

  1. LOGIN events (player_events) store the account in `player:account:{id}`:
     account_age_days, games_played, rank, and the simulator's ground-truth
     archetype (kept for evaluation only, never used for scoring).
  2. cheat_detection hands every scored window to evaluate_smurfs(), which
     joins the window's combat stats with the stored account.

Logic (ROADMAP "Smurf Detection"):
    candidate   = account_age_days < 14 AND games_played < 30
    skill z     = max( (avg_accuracy - rank_acc_mean)/acc_std,
                       (rank_rxn_mean - avg_reaction_time)/rxn_std )
    probability = sigmoid(2 * (skill_z - 2))     # z=2 -> 0.5, z=3 -> 0.88
    FLAG when probability >= 0.85
"""

import json
import math
import os
import time

from pyspark.sql import functions as F

from src.common import alerts as alerts_mod
from src.common.alerts import make_alert
from src.common.config import CHECKPOINT_DIR, DEFAULT_PROCESSING_TIME, KAFKA_TOPICS
from src.common.runtime import cached_batch, kafka_source, redis_client, run_with_retry
from src.common.schemas import parse_stream
from src.common.sinks import safe_parquet_archive

CANDIDATE_MAX_AGE_DAYS = 14
CANDIDATE_MAX_GAMES = 30
SMURF_PROB_THRESHOLD = 0.85
MIN_COMBAT_SHOTS = 8
ACCOUNT_FIELDS = ("account_age_days", "games_played", "rank", "archetype")
PIPELINE_CHUNK_SIZE = 200
PLAYER_STATE_TTL_SECONDS = 30 * 24 * 60 * 60

_WRITE_ACCOUNT = """
local stored_login = tonumber(redis.call('HGET', KEYS[1], 'login_at'))
local login_at = tonumber(ARGV[2])
if stored_login and login_at < stored_login then
    return 0
end
local reset_session = (stored_login and login_at > stored_login) or not stored_login
if reset_session then
    if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
        redis.call('DEL', KEYS[2])
        redis.call('SREM', KEYS[3], ARGV[1])
    end
    local last_cheat_ms = tonumber(redis.call('HGET', KEYS[6], 'last_judged_window_end_ms')) or 0
    if last_cheat_ms <= login_at then
        redis.call('DEL', KEYS[6])
        redis.call('SREM', KEYS[5], ARGV[1])
        if redis.call('EXISTS', KEYS[4]) == 1 then
            redis.call('HSET', KEYS[4], 'flagged', 'false')
        end
    end
    local last_cusum_ms = tonumber(redis.call('HGET', KEYS[7], 'last_window_end_ms')) or 0
    if last_cusum_ms <= login_at then
        redis.call('DEL', KEYS[7], KEYS[10])
        redis.call('SREM', KEYS[8], ARGV[1])
        redis.call('ZREM', KEYS[9], ARGV[1])
    end
end
redis.call('HSET', KEYS[1], 'login_at', ARGV[2], unpack(ARGV, 6))
local age = tonumber(redis.call('HGET', KEYS[1], 'account_age_days'))
local games = tonumber(redis.call('HGET', KEYS[1], 'games_played'))
if age and games and (age >= tonumber(ARGV[4]) or games >= tonumber(ARGV[5])) then
    local rank = redis.call('HGET', KEYS[1], 'rank')
    if not rank or rank == '' then
        rank = 'gold'
    end
    local archetype = redis.call('HGET', KEYS[1], 'archetype') or ''
    redis.call(
        'HSET', KEYS[2],
        'player_id', ARGV[1],
        'probability', '0.000',
        'status', 'CLEAN',
        'account_age_days', tostring(age),
        'games_played', tostring(games),
        'declared_rank', rank,
        'archetype', archetype,
        'reason', 'established account',
        'updated_at', ARGV[3]
    )
    redis.call('SREM', KEYS[3], ARGV[1])
end
return 1
"""

_EVALUATE_SMURF = """
if (redis.call('HGET', KEYS[1], 'login_at') or '') ~= ARGV[1]
    or (redis.call('HGET', KEYS[1], 'account_age_days') or '') ~= ARGV[2]
    or (redis.call('HGET', KEYS[1], 'games_played') or '') ~= ARGV[3]
    or (redis.call('HGET', KEYS[1], 'rank') or '') ~= ARGV[4]
    or (redis.call('HGET', KEYS[1], 'archetype') or '') ~= ARGV[5] then
    return 0
end
local action = ARGV[6]
if action == 'SKIP' then
    return 1
end
redis.call(
    'HSET', KEYS[2],
    'player_id', ARGV[7],
    'probability', ARGV[8],
    'status', ARGV[9],
    'account_age_days', ARGV[10],
    'games_played', ARGV[11],
    'declared_rank', ARGV[12],
    'archetype', ARGV[13],
    'reason', ARGV[14],
    'updated_at', ARGV[15]
)
local staged = 0
if action == 'SMURF' then
    redis.call('SADD', KEYS[3], ARGV[7])
    if ARGV[16] ~= '' then
        local holder = redis.call('GET', KEYS[4])
        if not holder then
            redis.call('SET', KEYS[4], ARGV[16], 'EX', ARGV[18])
            redis.call('HSET', KEYS[5], ARGV[16], ARGV[17])
            staged = 1
        end
    end
else
    redis.call('SREM', KEYS[3], ARGV[7])
end
return 1 + staged
"""

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


def parse_account(raw):
    """`player:account:{id}` hash -> account dict; None when absent or malformed."""
    try:
        return {
            "age_days": int(raw["account_age_days"]),
            "games_played": int(raw["games_played"]),
            "rank": raw.get("rank") or "gold",
            "archetype": raw.get("archetype", ""),
        }
    except (KeyError, TypeError, ValueError):
        return None


def is_candidate(account):
    """Whether an account is young/inexperienced enough to be a smurf candidate."""
    return (account["age_days"] < CANDIDATE_MAX_AGE_DAYS
            and account["games_played"] < CANDIDATE_MAX_GAMES)


def evaluate_smurf(account, combat_stats):
    """(smurf_probability, reason), or None when there is nothing to judge.

    None means: not a candidate (established account) or fewer than
    MIN_COMBAT_SHOTS shots. Pure, unit-testable.

    account:      {"age_days": int, "games_played": int, "rank": str}
    combat_stats: {"total_shots": int, "avg_accuracy": float,
                   "avg_reaction_time": float}
    """
    if (not is_candidate(account)
            or int(combat_stats["total_shots"]) < MIN_COMBAT_SHOTS):
        return None

    acc_mean, acc_std, rxn_mean, rxn_std = RANK_BASELINES.get(
        account["rank"], RANK_BASELINES["gold"])
    z_acc = (float(combat_stats["avg_accuracy"]) - acc_mean) / acc_std
    z_rxn = (rxn_mean - float(combat_stats["avg_reaction_time"])) / rxn_std
    skill_z = max(z_acc, z_rxn)

    prob = _sigmoid(2.0 * (skill_z - 2.0))
    return prob, (f"skill_z={skill_z:.2f} vs declared {account['rank']} "
                  f"(acc {z_acc:.1f}σ, rxn {z_rxn:.1f}σ)")


def _dedup_combat_rows(rows):
    """Pick one representative window per player (most shots, then newest end)."""
    best = {}
    for idx, row in enumerate(rows):
        pid = row["player_id"]
        shots = int(row.get("total_shots", 0))
        end_ms = int(row.get("window_end_ms", 0))
        rank = (shots, end_ms, idx)
        prev = best.get(pid)
        if prev is None or rank > prev[0]:
            best[pid] = (rank, row)
    return [row for _, row in best.values()]


def evaluate_smurfs(r, alerts, rows):
    """Judge each scored combat window against its player's stored account.

    rows: cheat_detection window rows (player_id, total_shots, avg_accuracy,
    avg_reaction_time). Pipelines `HGETALL` per chunk and applies a per-player
    Lua CAS script so concurrent LOGINs only retry the conflicted player(s).
    """
    rows = _dedup_combat_rows(rows)
    if not rows:
        if alerts.pending:
            with r.pipeline() as pipe:
                alerts.stage(pipe)
                pipe.execute()
        return []

    emitted = []
    buffered = list(alerts.pending)
    alerts.pending.clear()
    for start in range(0, len(rows), PIPELINE_CHUNK_SIZE):
        pending_rows = list(rows[start:start + PIPELINE_CHUNK_SIZE])
        while pending_rows:
            with r.pipeline() as read_pipe:
                for row in pending_rows:
                    read_pipe.hgetall(f"player:account:{row['player_id']}")
                accounts = read_pipe.execute()

            committed = False
            staged_alerts = []
            try:
                with r.pipeline() as write_pipe:
                    for row, raw in zip(pending_rows, accounts):
                        staged_alerts.append(_queue_smurf_evaluation(write_pipe, row, raw or {}))
                    alerts.pending[:] = buffered
                    alerts.stage(write_pipe)
                    results = write_pipe.execute()
                    committed = True
            finally:
                if not committed:
                    alerts.pending[:] = buffered

            buffered = []
            # Each player queues EVAL plus two TTL commands. Select only each
            # EVAL result; code==2 means a new alert was staged into OUTBOX_KEY.
            eval_results = results[0:3 * len(pending_rows):3]
            emitted.extend(alert for alert, code in zip(staged_alerts, eval_results)
                           if code == 2 and alert is not None)
            pending_rows = [
                row for row, code in zip(pending_rows, eval_results) if not code
            ]
    return emitted


def _queue_smurf_evaluation(pipe, row, raw):
    player_id = row["player_id"]
    exp_login_at = str(raw.get("login_at", ""))
    exp_age = str(raw.get("account_age_days", ""))
    exp_games = str(raw.get("games_played", ""))
    exp_rank = str(raw.get("rank", ""))
    exp_archetype = str(raw.get("archetype", ""))

    account = parse_account(raw)
    action = "SKIP"
    evaluation = {}
    alert = None

    if account is not None:
        now_str = str(int(time.time()))
        if not is_candidate(account):
            action = "CLEAN"
            evaluation = {
                "player_id": player_id,
                "probability": "0.000",
                "status": "CLEAN",
                "account_age_days": str(account["age_days"]),
                "games_played": str(account["games_played"]),
                "declared_rank": account["rank"],
                "archetype": account["archetype"],
                "reason": "established account",
                "updated_at": now_str,
            }
        else:
            result = evaluate_smurf(account, row)
            if result is not None:
                prob, reason = result
                smurf = prob >= SMURF_PROB_THRESHOLD
                action = "SMURF" if smurf else "CLEAN"
                evaluation = {
                    "player_id": player_id,
                    "probability": f"{prob:.3f}",
                    "status": action,
                    "account_age_days": str(account["age_days"]),
                    "games_played": str(account["games_played"]),
                    "declared_rank": account["rank"],
                    "archetype": account["archetype"],
                    "reason": reason,
                    "updated_at": now_str,
                }
                if smurf:
                    alert = make_alert(
                        "SMURF_DETECTED", "WARNING", "PLAYER", player_id,
                        f"New account ({account['age_days']}d, {account['games_played']} games) "
                        f"performs at {prob * 100:.0f}% smurf probability: {reason}",
                        evaluation,
                    )

    dedup_key = alerts_mod._dedup_key(alert) if alert is not None else ""
    pipe.eval(
        _EVALUATE_SMURF,
        5,
        f"player:account:{player_id}",
        f"player:smurf:{player_id}",
        "players:smurf",
        dedup_key,
        alerts_mod.OUTBOX_KEY,
        exp_login_at,
        exp_age,
        exp_games,
        exp_rank,
        exp_archetype,
        action,
        player_id,
        evaluation.get("probability", ""),
        evaluation.get("status", ""),
        evaluation.get("account_age_days", ""),
        evaluation.get("games_played", ""),
        evaluation.get("declared_rank", ""),
        evaluation.get("archetype", ""),
        evaluation.get("reason", ""),
        evaluation.get("updated_at", ""),
        alert["alert_id"] if alert is not None else "",
        json.dumps(alert) if alert is not None else "",
        str(alerts_mod.DEDUP_TTL_SECONDS),
    )
    pipe.expire(f"player:account:{player_id}", PLAYER_STATE_TTL_SECONDS)
    pipe.expire(f"player:smurf:{player_id}", PLAYER_STATE_TTL_SECONDS)
    return alert


def _dedup_logins(rows):
    """Deduplicate LOGIN rows per player_id, keeping max event_time."""
    latest = {}
    for row in rows:
        metadata = row["metadata"] or {}
        update = {k: str(metadata[k]) for k in ACCOUNT_FIELDS if metadata.get(k) is not None}
        if not update:
            continue
        player_id = row["player_id"]
        login_at = int(row["event_time"])
        prev = latest.get(player_id)
        if prev is None or login_at >= prev[0]:
            latest[player_id] = (login_at, update)
    return latest


def write_accounts(r, rows):
    """Apply deduplicated LOGIN metadata and reconcile candidacy in pipeline chunks."""
    latest = _dedup_logins(rows)
    if not latest:
        return
    items = list(latest.items())
    now_str = str(int(time.time()))
    for start in range(0, len(items), PIPELINE_CHUNK_SIZE):
        chunk = items[start:start + PIPELINE_CHUNK_SIZE]
        with r.pipeline() as pipe:
            for player_id, (login_at, update) in chunk:
                flat_update = [item for pair in update.items() for item in pair]
                pipe.eval(
                    _WRITE_ACCOUNT,
                    10,
                    f"player:account:{player_id}",
                    f"player:smurf:{player_id}",
                    "players:smurf",
                    f"player:{player_id}",
                    "players:flagged",
                    f"player:cheat:evidence:{player_id}",
                    f"behavior:cusum:{player_id}",
                    "behavior:anomalies",
                    "behavior:anomalies:expiry",
                    f"player:behavior:{player_id}",
                    player_id,
                    str(login_at),
                    now_str,
                    str(CANDIDATE_MAX_AGE_DAYS),
                    str(CANDIDATE_MAX_GAMES),
                    *flat_update,
                )
                pipe.expire(f"player:account:{player_id}", PLAYER_STATE_TTL_SECONDS)
            pipe.execute()


def write_accounts_batch(batch_df, batch_id):
    with cached_batch(batch_df):
        if batch_df.isEmpty():
            return
        rows = batch_df.select("player_id", "metadata", "event_time").collect()
        run_with_retry(
            f"smurf account batch {batch_id}",
            lambda: write_accounts(redis_client(), rows),
        )
        safe_parquet_archive(batch_df, "smurf_detection", batch_id)


def start_queries(spark):
    logins = parse_stream(kafka_source(spark, KAFKA_TOPICS["player"]), "player") \
        .filter(F.col("event_type") == "LOGIN")
    return logins.writeStream \
        .outputMode("append") \
        .foreachBatch(write_accounts_batch) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "smurf_detection")) \
        .trigger(processingTime=DEFAULT_PROCESSING_TIME) \
        .start()
