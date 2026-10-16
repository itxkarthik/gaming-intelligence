"""Outbox failure/replay regressions; use a disposable Redis, never serving state."""

import json
import os
import sys
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
            self.keys = []

        def set(self, key, value, nx=False, ex=None):
            self.keys.append((key, value, nx))

        def execute(self):
            res = []
            for key, value, nx in self.keys:
                if nx and key in self.r.leases:
                    res.append(False)
                else:
                    self.r.leases[key] = value
                    res.append(True)
            self.keys.clear()
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

        def delete(self, *keys):
            for key in keys:
                self.leases.pop(key, None)

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
