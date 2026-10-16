"""Unit tests for smurf detection scoring (runs in the Spark container)."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.common.alerts import AlertBatch
from src.jobs.smurf_detection import (
    PIPELINE_CHUNK_SIZE,
    SMURF_PROB_THRESHOLD,
    evaluate_smurf,
    evaluate_smurfs,
    parse_account,
    write_accounts,
)


def _account(age=3, games=12, rank="bronze"):
    return {"age_days": age, "games_played": games, "rank": rank}


def _combat(acc=0.62, rxn=160.0, shots=50):
    return {"avg_accuracy": acc, "avg_reaction_time": rxn, "total_shots": shots}


def test_smurf_archetype_features_flag():
    prob, reason = evaluate_smurf(_account(), _combat())
    assert prob >= SMURF_PROB_THRESHOLD, reason
    assert "bronze" in reason


def test_established_account_is_not_judged():
    assert evaluate_smurf(_account(age=400, games=1200), _combat()) is None
    assert evaluate_smurf(_account(age=3, games=60), _combat()) is None, \
        "30+ games excludes candidacy regardless of skill"


def test_too_few_shots_is_not_judged():
    assert evaluate_smurf(_account(), _combat(shots=3)) is None


def test_new_account_with_weak_stats_not_flagged():
    prob, _ = evaluate_smurf(_account(), _combat(acc=0.19, rxn=395.0))
    assert prob < 0.30


def test_skill_matching_declared_rank_not_flagged():
    # Fresh diamond account performing like a diamond is legitimate
    prob, _ = evaluate_smurf(_account(rank="diamond"), _combat(acc=0.43, rxn=185.0))
    assert prob < SMURF_PROB_THRESHOLD


def test_reaction_only_superiority_can_flag():
    # Mediocre accuracy but inhuman reaction vs bronze baseline
    prob, _ = evaluate_smurf(_account(), _combat(acc=0.20, rxn=150.0))
    assert prob >= SMURF_PROB_THRESHOLD


def test_parse_account_reads_the_login_hash():
    raw = {"account_age_days": "5", "games_played": "9", "rank": "silver", "archetype": "smurf"}
    assert parse_account(raw) == {"age_days": 5, "games_played": 9, "rank": "silver",
                                  "archetype": "smurf"}
    assert parse_account({}) is None
    assert parse_account({"account_age_days": "x", "games_played": "1"}) is None


class FakePipe:
    def __init__(self, r):
        self.r = r
        self.ops = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.ops.clear()

    def hgetall(self, k):
        self.ops.append(("hgetall", k))

    def hset(self, k, mapping=None):
        self.ops.append(("hset", k, mapping))

    def sadd(self, k, v):
        self.ops.append(("sadd", k, v))

    def srem(self, k, v):
        self.ops.append(("srem", k, v))

    def expire(self, k, seconds):
        self.ops.append(("expire", k, seconds))

    def eval(self, script, numkeys, *args):
        if numkeys == 2:
            _, _, _, payload, _ = args
            self.ops.append(("outbox", json.loads(payload)))
        elif numkeys == 3:
            self.ops.append(("write_account", args))
        elif numkeys == 5:
            self.ops.append(("evaluate_smurf", args))

    def execute(self):
        if self.ops and all(op[0] == "hgetall" for op in self.ops):
            self.r.read_batches.append(list(self.ops))
            res = [dict(self.r.hashes.get(op[1], {})) for op in self.ops]
            self.ops.clear()
            return res

        self.r.attempts.append(list(self.ops))
        if self.r.on_conflict and any(op[0] != "hgetall" for op in self.ops):
            on_conflict, self.r.on_conflict = self.r.on_conflict, None
            on_conflict(self.r)
        self.r.transactions.append(list(self.ops))
        res = []
        for op in self.ops:
            if op[0] == "hgetall":
                res.append(dict(self.r.hashes.get(op[1], {})))
            elif op[0] == "hset":
                self.r.hashes.setdefault(op[1], {}).update(op[2])
                res.append(1)
            elif op[0] == "sadd":
                self.r.sets.setdefault(op[1], set()).add(op[2])
                res.append(1)
            elif op[0] == "srem":
                self.r.sets.setdefault(op[1], set()).discard(op[2])
                res.append(1)
            elif op[0] == "expire":
                res.append(1)
            elif op[0] == "outbox":
                self.r.outbox.append(op[1])
                res.append(1)
            elif op[0] == "write_account":
                (account_key, smurf_key, smurf_set_key, player_id, login_at_str,
                 updated_at_str, max_age_str, max_games_str, *flat_update) = op[1]
                stored = self.r.hashes.get(account_key, {})
                if stored.get("login_at") and int(login_at_str) < int(stored["login_at"]):
                    res.append(0)
                    continue
                update = dict(zip(flat_update[::2], flat_update[1::2]))
                update["login_at"] = login_at_str
                self.r.hashes.setdefault(account_key, {}).update(update)
                account = parse_account(self.r.hashes[account_key])
                if account is not None and (
                    account["age_days"] >= int(max_age_str)
                    or account["games_played"] >= int(max_games_str)
                ):
                    self.r.hashes.setdefault(smurf_key, {}).update({
                        "player_id": player_id,
                        "probability": "0.000",
                        "status": "CLEAN",
                        "account_age_days": str(account["age_days"]),
                        "games_played": str(account["games_played"]),
                        "declared_rank": account["rank"],
                        "archetype": account["archetype"],
                        "reason": "established account",
                        "updated_at": updated_at_str,
                    })
                    self.r.sets.setdefault(smurf_set_key, set()).discard(player_id)
                res.append(1)
            elif op[0] == "evaluate_smurf":
                (account_key, smurf_key, smurf_set_key, _dedup_key, _outbox_key,
                 exp_login_at, exp_age, exp_games, exp_rank, exp_archetype,
                 action, player_id, prob_str, status_str, age_str, games_str,
                 rank_str, archetype_str, reason_str, updated_at_str,
                 _alert_id, alert_json, _ttl_str) = op[1]
                stored = self.r.hashes.get(account_key, {})
                if (
                    str(stored.get("login_at", "")) != exp_login_at
                    or str(stored.get("account_age_days", "")) != exp_age
                    or str(stored.get("games_played", "")) != exp_games
                    or str(stored.get("rank", "")) != exp_rank
                    or str(stored.get("archetype", "")) != exp_archetype
                ):
                    res.append(0)
                    continue
                if action == "SKIP":
                    res.append(1)
                    continue
                self.r.hashes.setdefault(smurf_key, {}).update({
                    "player_id": player_id,
                    "probability": prob_str,
                    "status": status_str,
                    "account_age_days": age_str,
                    "games_played": games_str,
                    "declared_rank": rank_str,
                    "archetype": archetype_str,
                    "reason": reason_str,
                    "updated_at": updated_at_str,
                })
                if action == "SMURF":
                    self.r.sets.setdefault(smurf_set_key, set()).add(player_id)
                    if alert_json:
                        self.r.outbox.append(json.loads(alert_json))
                else:
                    self.r.sets.setdefault(smurf_set_key, set()).discard(player_id)
                res.append(1)
        self.ops.clear()
        return res


class FakeRedis:
    def __init__(self, age=30, games=50, login_at=1700000000000):
        self.hashes = {
            "player:account:p1": {
                "account_age_days": str(age),
                "games_played": str(games),
                "rank": "bronze",
                "archetype": "normal_bronze",
                "login_at": str(login_at),
            },
            "player:smurf:p1": {"status": "SMURF", "probability": "0.950"},
        }
        self.sets = {"players:smurf": {"p1"}}
        self.read_batches = []
        self.transactions = []
        self.attempts = []
        self.outbox = []
        self.on_conflict = None

    def pipeline(self):
        return FakePipe(self)


class DummyAlerts:
    def __init__(self):
        self.emitted = []
        self.pending = []
        self.staged_ops = []

    def emit(self, payload):
        self.emitted.append(payload)
        self.pending.append(payload)

    def stage(self, pipe):
        self.staged_ops = list(pipe.ops)
        for payload in self.pending:
            pipe.ops.append(("outbox", payload))
        self.pending.clear()

    def flush(self):
        raise AssertionError("evaluate_smurfs must leave flushing to its caller")


def _login(event_time=1700000001000, **metadata):
    return {"player_id": "p1", "event_time": event_time, "metadata": metadata}


def _assert_clean(r):
    assert "p1" not in r.sets["players:smurf"]
    evaluation = r.hashes["player:smurf:p1"]
    assert evaluation["status"] == "CLEAN"
    assert evaluation["probability"] == "0.000"
    assert evaluation["reason"] == "established account"


def test_aged_out_account_is_removed_from_smurf_set():
    r = FakeRedis()
    evaluate_smurfs(r, DummyAlerts(), [{"player_id": "p1", **_combat()}])
    _assert_clean(r)


@pytest.mark.parametrize("metadata", [
    {"account_age_days": "14"},
    {"games_played": "30"},
    {"account_age_days": "400", "games_played": "1200", "rank": "diamond"},
])
def test_login_only_established_account_clears_stale_smurf(metadata):
    r = FakeRedis(age=3, games=12)
    write_accounts(r, [_login(**metadata)])
    _assert_clean(r)
    assert r.hashes["player:account:p1"]["login_at"] == "1700000001000"
    # Metadata, evaluation, and membership are committed together in one atomic Lua call.
    assert len(r.transactions) == 1
    assert [op[0] for op in r.transactions[0]] == ["write_account", "expire"]


@pytest.mark.parametrize("metadata", [
    {"account_age_days": "1", "games_played": "2", "rank": "silver", "archetype": "smurf"},
    {"account_age_days": "400", "games_played": "1200", "rank": "diamond"},
])
def test_old_login_cannot_regress_metadata_or_change_eligibility(metadata):
    r = FakeRedis(age=3, games=12)
    saved = {key: dict(value) for key, value in r.hashes.items()}
    write_accounts(r, [_login(event_time=1699999999999, **metadata)])
    assert r.hashes == saved
    assert r.sets["players:smurf"] == {"p1"}
    assert len(r.transactions) == 1


def test_out_of_order_logins_keep_newest_established_metadata():
    r = FakeRedis(age=3, games=12)
    write_accounts(r, [
        _login(account_age_days="100", games_played="200", rank="diamond"),
        _login(event_time=1700000000500, account_age_days="1", games_played="2", rank="bronze"),
    ])
    _assert_clean(r)
    assert r.hashes["player:account:p1"]["rank"] == "diamond"
    assert r.hashes["player:account:p1"]["account_age_days"] == "100"
    assert len(r.transactions) == 1
    assert len(r.transactions[0]) == 2


def test_login_candidate_does_not_clear_existing_combat_flag():
    r = FakeRedis(age=3, games=12)
    write_accounts(r, [_login(account_age_days="4", games_played="13")])
    assert r.hashes["player:smurf:p1"]["status"] == "SMURF"
    assert r.sets["players:smurf"] == {"p1"}


def test_partial_first_login_is_stored_without_inventing_eligibility():
    r = FakeRedis()
    r.hashes.clear()
    write_accounts(r, [_login(rank="silver"), _login(metadata_ignored="value")])
    assert r.hashes == {"player:account:p1": {"rank": "silver", "login_at": "1700000001000"}}


def test_legacy_account_without_login_time_can_be_updated_and_replayed():
    r = FakeRedis(age=3, games=12)
    del r.hashes["player:account:p1"]["login_at"]
    row = _login(account_age_days="14")
    write_accounts(r, [row, row])
    _assert_clean(r)
    assert r.hashes["player:account:p1"]["account_age_days"] == "14"


def test_concurrent_newer_login_is_not_overwritten_after_watch_retry():
    r = FakeRedis(age=3, games=12)

    def newer_login(redis):
        redis.hashes["player:account:p1"].update({
            "login_at": "1700000002000", "rank": "diamond",
        })

    r.on_conflict = newer_login
    write_accounts(r, [_login(account_age_days="100", rank="bronze")])
    assert r.hashes["player:account:p1"]["rank"] == "diamond"
    assert r.hashes["player:account:p1"]["account_age_days"] == "3"
    assert r.hashes["player:smurf:p1"]["status"] == "SMURF"
    assert len(r.transactions) == 1


def test_write_accounts_deduplicates_and_batches_in_pipeline_chunks():
    r = FakeRedis(age=3, games=12)
    rows = []
    for idx in range(PIPELINE_CHUNK_SIZE * 2 + 5):
        pid = f"p{idx}"
        rows.append({"player_id": pid, "event_time": 1000, "metadata": {"rank": "bronze"}})
        rows.append({"player_id": pid, "event_time": 2000, "metadata": {"rank": "gold"}})
    write_accounts(r, rows)
    assert len(r.transactions) == 3
    assert [len(tx) for tx in r.transactions] == [2 * PIPELINE_CHUNK_SIZE,
                                                    2 * PIPELINE_CHUNK_SIZE, 10]
    assert r.hashes["player:account:p0"]["rank"] == "gold"
    assert r.hashes["player:account:p0"]["login_at"] == "2000"


def test_smurf_alert_is_staged_in_state_transaction_not_flushed():
    r = FakeRedis(age=3, games=12)
    alerts = DummyAlerts()
    emitted = evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat()}])
    assert len(emitted) == 1
    assert r.outbox == emitted
    assert [op[0] for op in alerts.staged_ops] == ["evaluate_smurf", "expire", "expire"]
    assert [op[0] for op in r.transactions[-1]] == ["evaluate_smurf", "expire", "expire"]


def test_staging_failure_does_not_commit_smurf_state():
    class FailingAlerts(DummyAlerts):
        def stage(self, pipe):
            super().stage(pipe)
            raise RuntimeError("stage failed")

    r = FakeRedis(age=3, games=12)
    saved = dict(r.hashes["player:smurf:p1"])
    alerts = FailingAlerts()
    unrelated = {"alert_id": "unrelated"}
    alerts.emit(unrelated)
    with pytest.raises(RuntimeError, match="stage failed"):
        evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat()}])
    assert r.hashes["player:smurf:p1"] == saved
    assert r.outbox == []
    assert alerts.pending == [unrelated]


def test_clean_candidate_combat_still_removes_stale_smurf():
    r = FakeRedis(age=3, games=12)
    alerts = DummyAlerts()
    emitted = evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat(acc=0.19, rxn=395)}])
    assert r.hashes["player:smurf:p1"]["status"] == "CLEAN"
    assert "p1" not in r.sets["players:smurf"]
    assert emitted == []


@pytest.mark.parametrize("alerts_factory", [lambda r: DummyAlerts(), AlertBatch])
def test_established_login_between_combat_read_and_exec_cannot_restore_smurf(alerts_factory):
    r = FakeRedis(age=3, games=12)
    alerts = alerts_factory(r)
    unrelated = {
        "alert_id": "queued-cheat-alert", "alert_type": "CHEAT_DETECTED", "entity_id": "p1",
    }
    alerts.emit(unrelated)

    def established_login(redis):
        write_accounts(redis, [_login(account_age_days="14", games_played="30")])
        _assert_clean(redis)

    # Interleave a real LOGIN reconciliation after combat has read the young
    # account and queued SMURF + alert intent, immediately before its EXEC.
    r.on_conflict = established_login
    evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat()}])

    assert r.on_conflict is None
    _assert_clean(r)
    assert r.hashes["player:smurf:p1"]["account_age_days"] == "14"
    assert r.outbox == [unrelated]
    assert alerts.pending == []
    # The obsolete SMURF intent was rejected by the per-player CAS check on pass 1.
    assert [op[0] for op in r.attempts[0]] == ["evaluate_smurf", "expire", "expire", "outbox"]
    assert [op[0] for op in r.transactions[-1]] == ["evaluate_smurf", "expire", "expire"]


def test_watch_retry_uses_new_rank_and_retains_only_matching_smurf_intent():
    r = FakeRedis(age=3, games=12)
    alerts = DummyAlerts()
    unrelated = {"alert_id": "queued-cheat-alert"}
    alerts.emit(unrelated)

    def silver_login(redis):
        write_accounts(redis, [_login(rank="silver")])

    r.on_conflict = silver_login
    evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat()}])

    assert r.hashes["player:smurf:p1"]["declared_rank"] == "silver"
    assert len(r.outbox) == 2
    assert r.outbox[0] == unrelated
    assert r.outbox[1]["alert_type"] == "SMURF_DETECTED"
    assert r.outbox[1]["details"]["declared_rank"] == "silver"
    assert alerts.pending == []


def test_watch_covers_every_account_in_multi_player_batch():
    r = FakeRedis(age=3, games=12)
    r.hashes["player:account:p2"] = dict(r.hashes["player:account:p1"])
    alerts = DummyAlerts()

    def second_player_established(redis):
        row = {**_login(games_played="30"), "player_id": "p2"}
        write_accounts(redis, [row])

    r.on_conflict = second_player_established
    evaluate_smurfs(r, alerts, [
        {"player_id": "p1", **_combat()},
        {"player_id": "p2", **_combat()},
    ])

    assert r.hashes["player:smurf:p1"]["status"] == "SMURF"
    assert r.hashes["player:smurf:p2"]["status"] == "CLEAN"
    assert r.sets["players:smurf"] == {"p1"}
    assert len(r.outbox) == 1
    assert r.outbox[0]["entity_id"] == "p1"
    assert alerts.pending == []
    # Pass 1 pipelined both reads; pass 2 only re-read the conflicted player p2.
    assert [[op[1] for op in batch] for batch in r.read_batches] == [
        ["player:account:p1", "player:account:p2"],
        ["player:account:p2"],
    ]


@pytest.mark.parametrize("alerts_factory", [lambda r: DummyAlerts(), AlertBatch])
def test_repeated_watch_retries_do_not_lose_unrelated_or_duplicate_smurf_intents(alerts_factory):
    r = FakeRedis(age=3, games=12)
    alerts = alerts_factory(r)
    unrelated = {
        "alert_id": "queued-cheat-alert", "alert_type": "CHEAT_DETECTED", "entity_id": "p1",
    }
    alerts.emit(unrelated)

    def silver_login(redis):
        write_accounts(redis, [_login(event_time=1700000002000, rank="silver")])

    def bronze_login(redis):
        write_accounts(redis, [_login(games_played="13")])
        redis.on_conflict = silver_login

    r.on_conflict = bronze_login
    evaluate_smurfs(r, alerts, [{"player_id": "p1", **_combat()}])

    assert len(r.outbox) == 2
    assert r.outbox[0] == unrelated
    assert r.outbox[1]["details"]["declared_rank"] == "silver"
    assert r.outbox[1]["details"]["games_played"] == "13"
    assert alerts.pending == []


def test_write_accounts_and_evaluate_smurfs_lua_scripts_against_real_redis(monkeypatch):
    import uuid
    import redis
    from src.common import alerts as alerts_mod

    host = os.getenv("REDIS_TEST_HOST")
    if not host:
        pytest.skip("set REDIS_TEST_HOST to a disposable Redis for Lua integration tests")
    client = redis.Redis(host=host, db=15, decode_responses=True)
    prefix = f"test_smurf_{uuid.uuid4().hex}:"
    pid = prefix + "p1"
    acct_key = f"player:account:{pid}"
    smurf_key = f"player:smurf:{pid}"
    outbox_key = prefix + "outbox"
    monkeypatch.setattr(alerts_mod, "OUTBOX_KEY", outbox_key)
    monkeypatch.setattr(alerts_mod, "_dedup_key", lambda p: prefix + "dedup:" + p["entity_id"])
    try:
        client.hset(acct_key, mapping={
            "account_age_days": "3", "games_played": "12", "rank": "bronze",
            "archetype": "smurf", "login_at": "1000",
        })
        batch = AlertBatch(client)
        evaluate_smurfs(client, batch, [{"player_id": pid, **_combat()}])
        assert client.hget(smurf_key, "status") == "SMURF"
        assert client.sismember("players:smurf", pid)
        assert client.hlen(outbox_key) == 1

        # Older login is ignored by Lua script; newer established login reconciles state
        write_accounts(client, [
            {"player_id": pid, "event_time": 500, "metadata": {"account_age_days": "1"}},
            {"player_id": pid, "event_time": 2000, "metadata": {"account_age_days": "14", "rank": "gold"}},
            {"player_id": pid, "event_time": 1500, "metadata": {"account_age_days": "2", "rank": "silver"}},
        ])
        assert client.hget(acct_key, "account_age_days") == "14"
        assert client.hget(acct_key, "rank") == "gold"
        assert client.hget(acct_key, "login_at") == "2000"
        assert client.hget(smurf_key, "status") == "CLEAN"
        assert not client.sismember("players:smurf", pid)

        # Subsequent older login in a separate batch cannot regress stored state
        write_accounts(client, [
            {"player_id": pid, "event_time": 1999, "metadata": {"account_age_days": "1", "rank": "bronze"}},
        ])
        assert client.hget(acct_key, "account_age_days") == "14"
        assert client.hget(acct_key, "rank") == "gold"
    finally:
        keys = list(client.scan_iter(prefix + "*")) + [acct_key, smurf_key]
        if keys:
            client.delete(*keys)
        client.srem("players:smurf", pid)
