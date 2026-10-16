"""Outbox failure/replay regressions; use a disposable Redis, never serving state."""

import json
import os
import sys
import threading
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import redis
from kafka.errors import KafkaTimeoutError

from src.common import alerts


@pytest.fixture
def outbox(monkeypatch):
    host = os.getenv("REDIS_TEST_HOST")
    if not host:
        pytest.skip("set REDIS_TEST_HOST to a disposable Redis for outbox integration tests")
    client = redis.Redis(host=host, decode_responses=True)
    prefix = f"test:{uuid.uuid4().hex}:"
    monkeypatch.setattr(alerts, "OUTBOX_KEY", prefix + "outbox")
    monkeypatch.setattr(alerts, "_dedup_key", lambda p: prefix + p["alert_type"] + p["entity_id"])
    monkeypatch.setattr(alerts, "_lease_key", lambda aid: prefix + "lease:" + aid)
    try:
        yield client, prefix
    finally:
        keys = list(client.scan_iter(prefix + "*"))
        if keys:
            client.delete(*keys)


def payload(alert_id="first", entity_id="p1"):
    return {"alert_id": alert_id, "alert_type": "BEHAVIOR_ANOMALY", "entity_id": entity_id}


class Producer:
    def __init__(self):
        self.sent = []
        self.fail = False

    def send(self, topic, value):
        self.sent.append(value)
        return self

    def get(self, timeout):
        if self.fail:
            raise KafkaTimeoutError("injected publication failure")


def test_discarded_transaction_never_claims_or_enqueues(outbox):
    r, prefix = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload())
    assert not r.exists(alerts._dedup_key(payload()))
    pipe = r.pipeline()
    pipe.hset(prefix + "state", mapping={"n": "1"})
    batch.stage(pipe)
    pipe.reset()  # failed/discarded attempt before EXEC
    assert not list(r.scan_iter(prefix + "*"))


def test_state_and_delivery_intent_commit_together(outbox):
    r, prefix = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload())
    pipe = r.pipeline()
    pipe.hset(prefix + "state", mapping={"n": "1", "last_window_end_ms": "5000"})
    batch.stage(pipe)
    pipe.execute()
    assert r.hget(prefix + "state", "last_window_end_ms") == "5000"
    assert r.hlen(alerts.OUTBOX_KEY) == 1
    assert r.get(alerts._dedup_key(payload())) == "first"


def test_failed_publication_survives_retry_without_new_detection(outbox, monkeypatch):
    r, _ = outbox
    producer = Producer()
    producer.fail = True
    monkeypatch.setattr(alerts, "_kafka_producer", lambda: producer)
    batch = alerts.AlertBatch(r)
    batch.emit(payload())
    with pytest.raises(KafkaTimeoutError):
        batch.flush()
    assert r.hlen(alerts.OUTBOX_KEY) == 1
    # A writer replay can skip its already-committed state transition and still
    # deliver the original intent, without recreating or renaming its alert.
    producer.fail = False
    alerts.AlertBatch(r).flush()
    assert [p["alert_id"] for p in producer.sent] == ["first", "first"]
    assert r.hlen(alerts.OUTBOX_KEY) == 0


def test_new_attempt_dedup_keeps_original_unpublished_intent(outbox, monkeypatch):
    r, _ = outbox
    for alert_id in ("first", "retry"):
        batch = alerts.AlertBatch(r)
        batch.emit(payload(alert_id))
        pipe = r.pipeline()
        batch.stage(pipe)
        pipe.execute()
    assert list(r.hkeys(alerts.OUTBOX_KEY)) == ["first"]
    producer = Producer()
    monkeypatch.setattr(alerts, "_kafka_producer", lambda: producer)
    alerts.AlertBatch(r).flush()
    assert [p["alert_id"] for p in producer.sent] == ["first"]


def test_ack_removal_failure_redelivers_same_id(outbox, monkeypatch):
    r, _ = outbox
    producer = Producer()
    monkeypatch.setattr(alerts, "_kafka_producer", lambda: producer)
    original = r.hdel
    monkeypatch.setattr(r, "hdel", lambda *args: (_ for _ in ()).throw(redis.ConnectionError("lost ACK removal")))
    batch = alerts.AlertBatch(r)
    batch.emit(payload())
    with pytest.raises(redis.ConnectionError):
        batch.flush()
    assert r.hlen(alerts.OUTBOX_KEY) == 1
    monkeypatch.setattr(r, "hdel", original)
    alerts.AlertBatch(r).flush()
    assert [p["alert_id"] for p in producer.sent] == ["first", "first"]


def test_concurrent_flusher_skips_leased_in_flight_alerts(outbox, monkeypatch):
    r, _ = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload("first"))
    pipe = r.pipeline()
    batch.stage(pipe)
    pipe.execute()

    sent = []

    class InterleavedProducer:
        def send(self, topic, value):
            sent.append(value["alert_id"])
            return self

        def get(self, timeout):
            # A second driver scanning the outbox while the first is awaiting
            # Kafka ACK must see the active lease and skip duplicate publish.
            alerts.AlertBatch(r).flush()

    monkeypatch.setattr(alerts, "_kafka_producer", lambda: InterleavedProducer())
    alerts.AlertBatch(r).flush()
    assert sent == ["first"]
    assert r.hlen(alerts.OUTBOX_KEY) == 0
    assert not r.exists(alerts._lease_key("first"))


def test_expired_lease_reclaim_not_deleted_by_stale_flusher(outbox, monkeypatch):
    r, _ = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload("first"))
    pipe = r.pipeline()
    batch.stage(pipe)
    pipe.execute()

    sent = []
    lease_key = alerts._lease_key("first")
    flusher2_in_flight = threading.Event()
    release_flusher2 = threading.Event()

    class Flusher2Producer:
        def send(self, topic, value):
            sent.append(("flusher2", value["alert_id"]))
            return self

        def get(self, timeout):
            # Signal that Flusher 2 has reclaimed the expired lease and sent,
            # then stay in flight while Flusher 1 cleans up and Flusher 3 runs.
            flusher2_in_flight.set()
            assert release_flusher2.wait(timeout=5)

    flusher2_thread = threading.Thread(target=lambda: alerts.AlertBatch(r).flush())

    class Flusher1SlowProducer:
        def send(self, topic, value):
            sent.append(("flusher1", value["alert_id"]))
            return self

        def get(self, timeout):
            # Simulate Flusher 1 stalling past lease TTL expiry: its lease
            # expires and Flusher 2 reclaims the lease before Flusher 1 exits.
            r.delete(lease_key)
            monkeypatch.setattr(alerts, "_kafka_producer", lambda: Flusher2Producer())
            flusher2_thread.start()
            assert flusher2_in_flight.wait(timeout=5)
            raise KafkaTimeoutError("flusher1 stalled past lease TTL")

    monkeypatch.setattr(alerts, "_kafka_producer", lambda: Flusher1SlowProducer())
    with pytest.raises(KafkaTimeoutError):
        alerts.AlertBatch(r).flush()

    # Flusher 1's cleanup must compare token before deleting and leave Flusher 2's
    # reclaimed lease intact, so a third concurrent flusher skips the alert.
    assert r.exists(lease_key)

    class Flusher3Producer:
        def send(self, topic, value):
            sent.append(("flusher3", value["alert_id"]))
            return self

        def get(self, timeout):
            pass

    monkeypatch.setattr(alerts, "_kafka_producer", lambda: Flusher3Producer())
    alerts.AlertBatch(r).flush()
    assert sent == [("flusher1", "first"), ("flusher2", "first")]
    assert r.exists(lease_key)

    # Now let Flusher 2 finish its ACK, delete from outbox, and release its lease.
    release_flusher2.set()
    flusher2_thread.join(timeout=5)
    assert r.hlen(alerts.OUTBOX_KEY) == 0
    assert not r.exists(lease_key)


def test_slow_chunk_renews_leases_and_skips_reclaimed_unsent_alerts(outbox, monkeypatch):
    r, _ = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload("a1", "p1"))
    batch.emit(payload("a2", "p2"))
    batch.emit(payload("a3", "p3"))
    pipe = r.pipeline()
    batch.stage(pipe)
    pipe.execute()

    first_id, second_id, third_id = [
        k for k, _ in r.hscan_iter(alerts.OUTBOX_KEY, count=alerts.FLUSH_CHUNK_SIZE)
    ]
    clock = [100.0]
    monkeypatch.setattr(alerts.time, "monotonic", lambda: clock[0])
    sent = []

    class FakeFuture:
        def __init__(self, alert_id):
            self.alert_id = alert_id

        def get(self, timeout):
            if self.alert_id == first_id:
                assert r.ttl(alerts._lease_key(first_id)) > 10
                # Simulate first_id's future.get() taking >= LEASE_TTL_SECONDS / 3
                # while third_id's lease TTL has ticked down.
                r.expire(alerts._lease_key(third_id), 5)
                clock[0] += alerts.LEASE_TTL_SECONDS / 3 + 1
            elif self.alert_id == third_id:
                # Renewal before third_id.get() must have refreshed third_id's TTL.
                assert r.ttl(alerts._lease_key(third_id)) > 10

    class SlowChunkProducer:
        def send(self, topic, value):
            aid = value["alert_id"]
            sent.append(aid)
            if aid == first_id:
                # Simulate first_id's send taking >= LEASE_TTL_SECONDS / 3 while
                # first_id's TTL has ticked down and second_id's lease expired
                # and was reclaimed by another flusher.
                r.expire(alerts._lease_key(first_id), 5)
                r.set(
                    alerts._lease_key(second_id),
                    "reclaimed-by-other",
                    ex=alerts.LEASE_TTL_SECONDS,
                )
                clock[0] += alerts.LEASE_TTL_SECONDS / 3 + 1
            return FakeFuture(aid)

    monkeypatch.setattr(alerts, "_kafka_producer", lambda: SlowChunkProducer())
    alerts.AlertBatch(r).flush()

    # first_id and third_id were renewed, acked, and removed; second_id was
    # skipped because its lease was reclaimed before send(second_id), and the
    # new owner's lease remains intact.
    assert sent == [first_id, third_id]
    assert list(r.hkeys(alerts.OUTBOX_KEY)) == [second_id]
    assert r.get(alerts._lease_key(second_id)) == "reclaimed-by-other"
    assert not r.exists(alerts._lease_key(first_id))
    assert not r.exists(alerts._lease_key(third_id))


def test_stale_scan_skips_alert_deleted_before_claim(outbox, monkeypatch):
    r, _ = outbox
    batch = alerts.AlertBatch(r)
    batch.emit(payload("first"))
    pipe = r.pipeline()
    batch.stage(pipe)
    pipe.execute()

    original_hscan = r.hscan_iter

    def hscan_then_concurrent_ack(*args, **kwargs):
        entries = list(original_hscan(*args, **kwargs))
        # Another flusher finishes Kafka ACK + HDEL + lease release after this
        # flusher's HSCAN snapshot was taken, before _CLAIM_LEASE executes.
        r.hdel(alerts.OUTBOX_KEY, "first")
        return iter(entries)

    monkeypatch.setattr(r, "hscan_iter", hscan_then_concurrent_ack)
    producer = Producer()
    monkeypatch.setattr(alerts, "_kafka_producer", lambda: producer)
    alerts.AlertBatch(r).flush()
    assert producer.sent == []
    assert not r.exists(alerts._lease_key("first"))


def test_flush_pipelines_sends_per_chunk_and_batches_hdel(monkeypatch):
    events = []
    hdels = []

    class FakeFuture:
        def __init__(self, alert_id):
            self.alert_id = alert_id

        def get(self, timeout):
            events.append(("get", self.alert_id))

    class ChunkProducer:
        def send(self, topic, value):
            events.append(("send", value["alert_id"]))
            return FakeFuture(value["alert_id"])

    class FakeClaimPipe:
        def __init__(self, r):
            self.r = r
            self.claims = []

        def eval(self, script, numkeys, *keys_and_args):
            _, lease_key, alert_id, token, _ = keys_and_args
            self.claims.append((alert_id, lease_key, token))

        def execute(self):
            res = []
            for alert_id, lease_key, token in self.claims:
                if alert_id not in self.r.items or lease_key in self.r.leases:
                    res.append(0)
                else:
                    self.r.leases[lease_key] = token
                    res.append(1)
            self.claims.clear()
            return res

    class FakeOutboxRedis:
        def __init__(self):
            self.items = {
                f"a{i}": json.dumps(payload(f"a{i}", f"p{i}"))
                for i in range(3)
            }
            self.leases = {alerts._lease_key("a_leased"): "other-driver"}
            self.items["a_leased"] = json.dumps(payload("a_leased", "p_leased"))

        def pipeline(self):
            return FakeClaimPipe(self)

        def hscan_iter(self, key, count=100):
            return iter(list(self.items.items()))

        def hdel(self, key, *fields):
            hdels.append(fields)
            for field in fields:
                self.items.pop(field, None)

        def eval(self, script, numkeys, *keys_and_args):
            keys = keys_and_args[:numkeys]
            token = keys_and_args[numkeys]
            released = 0
            for key in keys:
                if self.leases.get(key) == token:
                    self.leases.pop(key, None)
                    released += 1
            return released

    monkeypatch.setattr(alerts, "_kafka_producer", lambda: ChunkProducer())
    monkeypatch.setattr(alerts, "FLUSH_CHUNK_SIZE", 2)
    r = FakeOutboxRedis()
    alerts.AlertBatch(r).flush()
    # Chunk 1 sends a0, a1 before waiting on either get(), then batches HDEL(a0, a1);
    # Chunk 2 claims a2 and skips already-leased a_leased, then HDEL(a2).
    assert events == [
        ("send", "a0"), ("send", "a1"), ("get", "a0"), ("get", "a1"),
        ("send", "a2"), ("get", "a2"),
    ]
    assert hdels == [("a0", "a1"), ("a2",)]
    assert list(r.items.keys()) == ["a_leased"]
    assert list(r.leases.keys()) == [alerts._lease_key("a_leased")]
