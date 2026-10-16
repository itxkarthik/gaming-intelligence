"""Alert emission shared by the streaming jobs.

Every alert goes through a producer-side Redis dedup gate (`alert:dedup:*`)
and is flushed to the Kafka `alerts` topic for the Go alert engine (second
dedup/rate-limit layer, PostgreSQL history, Redis `alerts:stream` pub/sub,
and optional webhook).
"""

import contextlib
import itertools
import json
import time
import uuid

import redis
from kafka import KafkaProducer

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS


ALERTS_TOPIC = KAFKA_TOPICS["alerts"]
DEDUP_TTL_SECONDS = 300
SEND_TIMEOUT_SECONDS = 15
# Exceeds single-item Kafka send + retry budget (max_block_ms=10s +
# 3 * request_timeout_ms=10s = 40s); slow multi-item chunks also renew active
# leases every LEASE_TTL_SECONDS / 3.
LEASE_TTL_SECONDS = 60
OUTBOX_KEY = "alerts:outbox"
FLUSH_CHUNK_SIZE = 100
_producer = None

# Run inside the same MULTI/EXEC as the serving-state update. A claim cannot
# exist without a durable delivery intent, even if the connection dies at EXEC.
_STAGE_ALERT = """
local holder = redis.call('GET', KEYS[1])
if holder and holder ~= ARGV[1] then
    return 0
end
if not holder then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
    redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
end
return 1
"""

_CLAIM_LEASE = """
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
    return 0
end
if redis.call('SET', KEYS[2], ARGV[2], 'NX', 'EX', tonumber(ARGV[3])) then
    return 1
end
return 0
"""

_RENEW_LEASES = """
local status = {}
local outbox = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
for i = 2, #KEYS do
    local aid = ARGV[i + 1]
    if redis.call('HEXISTS', outbox, aid) == 1 and redis.call('GET', KEYS[i]) == token then
        redis.call('EXPIRE', KEYS[i], ttl)
        status[i - 1] = 1
    else
        status[i - 1] = 0
    end
end
return status
"""

_RELEASE_LEASES = """
local released = 0
for i = 1, #KEYS do
    if redis.call('GET', KEYS[i]) == ARGV[1] then
        released = released + redis.call('DEL', KEYS[i])
    end
end
return released
"""


def _kafka_producer():
    """Reuse one driver-side producer per streaming process."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
            max_block_ms=10_000,
            request_timeout_ms=10_000,
        )
    return _producer


def make_alert(alert_type, severity, entity_type, entity_id, message, details):
    """Payload matching schemas/alert.avsc (details values must be strings)."""
    now_ms = int(time.time() * 1000)
    return {
        "alert_id": str(uuid.uuid4()),
        "alert_type": alert_type,
        "severity": severity,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "message": message,
        "details": details,
        "timestamp": now_ms,
    }


def _dedup_key(payload):
    return (f"alert:dedup:{payload['alert_type']}:{payload['entity_id']}"
            f":{payload['severity']}")


def _lease_key(alert_id):
    return f"alert:lease:{alert_id}"


class AlertBatch:
    """Transactional Redis outbox with at-least-once Kafka delivery.

    Writers call stage(pipe) before committing serving state. flush() also
    drains intents left by earlier attempts, including replays whose state
    cursor has already advanced. An intent is removed only after Kafka ACK;
    an ambiguous ACK/removal can duplicate delivery with the same alert ID.
    """

    def __init__(self, r):
        self.r = r
        self.pending = []

    def emit(self, payload):
        self.pending.append(payload)

    def stage(self, pipe):
        for payload in self.pending:
            pipe.eval(
                _STAGE_ALERT, 2, _dedup_key(payload), OUTBOX_KEY,
                payload["alert_id"], json.dumps(payload), DEDUP_TTL_SECONDS,
            )
        self.pending.clear()

    def _renew_leases(self, alert_ids, lease_keys, token):
        return self.r.eval(
            _RENEW_LEASES,
            1 + len(lease_keys),
            OUTBOX_KEY,
            *lease_keys,
            token,
            LEASE_TTL_SECONDS,
            *alert_ids,
        )

    def flush(self):
        if self.pending:
            pipe = self.r.pipeline()
            self.stage(pipe)
            pipe.execute()
        # Claim per-alert leases atomically with HEXISTS on the outbox before
        # sending to Kafka so concurrent drivers skip in-flight or already-acked
        # entries; renew during slow chunks and verify token ownership before
        # deleting so an expired flusher never removes a new owner's lease.
        token = uuid.uuid4().hex
        it = self.r.hscan_iter(OUTBOX_KEY, count=FLUSH_CHUNK_SIZE)
        while True:
            chunk = list(itertools.islice(it, FLUSH_CHUNK_SIZE))
            if not chunk:
                break
            claim_pipe = self.r.pipeline()
            for alert_id, _ in chunk:
                claim_pipe.eval(
                    _CLAIM_LEASE,
                    2,
                    OUTBOX_KEY,
                    _lease_key(alert_id),
                    alert_id,
                    token,
                    LEASE_TTL_SECONDS,
                )
            claims = claim_pipe.execute()
            leased = [
                (alert_id, encoded)
                for (alert_id, encoded), claimed in zip(chunk, claims)
                if claimed
            ]
            if not leased:
                continue
            alert_ids = [alert_id for alert_id, _ in leased]
            lease_keys = [_lease_key(alert_id) for alert_id in alert_ids]
            owned = set(alert_ids)
            last_renewed = time.monotonic()
            renew_after = LEASE_TTL_SECONDS / 3
            producer = _kafka_producer()
            acked = []
            try:
                in_flight = []
                for alert_id, encoded in leased:
                    if time.monotonic() - last_renewed >= renew_after:
                        statuses = self._renew_leases(alert_ids, lease_keys, token)
                        owned = {
                            aid
                            for aid, ok in zip(alert_ids, statuses)
                            if ok
                        }
                        last_renewed = time.monotonic()
                    if alert_id not in owned:
                        continue
                    in_flight.append(
                        (alert_id, producer.send(ALERTS_TOPIC, json.loads(encoded)))
                    )
                for alert_id, future in in_flight:
                    if time.monotonic() - last_renewed >= renew_after:
                        self._renew_leases(alert_ids, lease_keys, token)
                        last_renewed = time.monotonic()
                    future.get(timeout=SEND_TIMEOUT_SECONDS)
                    acked.append(alert_id)
            finally:
                try:
                    if acked:
                        self.r.hdel(OUTBOX_KEY, *acked)
                finally:
                    with contextlib.suppress(redis.RedisError):
                        self.r.eval(
                            _RELEASE_LEASES, len(lease_keys), *lease_keys, token
                        )
