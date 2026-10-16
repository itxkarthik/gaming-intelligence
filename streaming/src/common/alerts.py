"""Alert emission shared by the streaming jobs.

Every alert goes through a producer-side Redis dedup gate (`alert:dedup:*`)
and is flushed to the Kafka `alerts` topic for the Go alert engine (second
dedup/rate-limit layer, PostgreSQL history, Redis `alerts:stream` pub/sub,
and optional webhook).
"""

import json
import time

from kafka import KafkaProducer

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS


ALERTS_TOPIC = KAFKA_TOPICS["alerts"]
DEDUP_TTL_SECONDS = 300
OUTBOX_KEY = "alerts:outbox"
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
        "alert_id": f"{alert_type.lower()}_{entity_id}_{now_ms}",
        "alert_type": alert_type,
        "severity": severity,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "message": message,
        "details": details,
        "timestamp": now_ms,
    }


def _dedup_key(payload):
    return f"alert:dedup:{payload['alert_type']}:{payload['entity_id']}"


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

    def flush(self):
        if self.pending:
            pipe = self.r.pipeline()
            self.stage(pipe)
            pipe.execute()
        # All drivers may drain the shared outbox. Concurrent delivery is safe
        # at-least-once; the engine recognizes redelivery by the original ID.
        for alert_id, encoded in self.r.hscan_iter(OUTBOX_KEY, count=100):
            payload = json.loads(encoded)
            _kafka_producer().send(ALERTS_TOPIC, payload).get(timeout=15)
            self.r.hdel(OUTBOX_KEY, alert_id)
