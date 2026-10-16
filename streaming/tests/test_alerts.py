"""Outbox failure/replay regressions; use a disposable Redis, never serving state."""

import os
import uuid

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
    try:
        yield client, prefix
    finally:
        keys = list(client.scan_iter(prefix + "*"))
        if keys:
            client.delete(*keys)


def payload(alert_id="first"):
    return {"alert_id": alert_id, "alert_type": "BEHAVIOR_ANOMALY", "entity_id": "p1"}


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
    pipe.hset(prefix + "state", mapping={"last_window_end_ms": "5000"})
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
