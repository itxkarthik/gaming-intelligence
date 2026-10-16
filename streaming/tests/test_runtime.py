"""Bounded driver retry behavior without external services."""

import pytest
import redis
from kafka.errors import KafkaTimeoutError

from src.common import runtime


@pytest.mark.parametrize("error", [redis.ConnectionError, redis.TimeoutError, KafkaTimeoutError])
def test_transient_errors_retry_then_return(error, monkeypatch):
    calls = []
    sleeps = []
    monkeypatch.setattr(runtime.time, "sleep", sleeps.append)
    monkeypatch.setattr(runtime.random, "uniform", lambda low, high: high)

    def write():
        calls.append(1)
        if len(calls) < 3:
            raise error("outage")
        return "done"

    assert runtime.run_with_retry("test", write) == "done"
    assert sleeps == [0.25, 0.5]


def test_retry_exhaustion_raises_and_bounds_backoff(monkeypatch):
    calls = []
    sleeps = []
    monkeypatch.setattr(runtime.time, "sleep", sleeps.append)
    monkeypatch.setattr(runtime.random, "uniform", lambda low, high: high)

    def write():
        calls.append(1)
        raise redis.ConnectionError("offline")

    with pytest.raises(redis.ConnectionError):
        runtime.run_with_retry("test", write)
    assert len(calls) == 5
    assert sleeps == [0.25, 0.5, 1.0, 2.0]


def test_programming_errors_fail_immediately(monkeypatch):
    monkeypatch.setattr(runtime.time, "sleep", lambda delay: pytest.fail("retried invalid data"))
    with pytest.raises(ValueError):
        runtime.run_with_retry("test", lambda: int("invalid"))


def test_cached_batch_unpersists_on_failure():
    class Batch:
        cached = False

        def persist(self):
            self.cached = True

        def unpersist(self):
            self.cached = False

    batch = Batch()
    with pytest.raises(ValueError):
        with runtime.cached_batch(batch):
            assert batch.cached
            raise ValueError("failure")
    assert not batch.cached
