"""Alert emission shared by the streaming jobs.

Every alert goes to BOTH:
  - Redis `alerts:recent` -> fast path for the REST API
  - Kafka alerts topic    -> transport for the Go alert engine (second dedup
    layer, PostgreSQL history, webhooks, WebSocket fan-out)
"""

import json
import time

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
from src.common.runtime import warn

ALERTS_TOPIC = KAFKA_TOPICS["alerts"]
DEDUP_TTL_SECONDS = 300
RECENT_KEY = "alerts:recent"
RECENT_MAX = 100


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
    """Alerts raised during one micro-batch.

    emit() claims the dedup key (SET NX EX: at most one alert per type and
    entity per TTL, or every micro-batch would re-alert the same entity) and
    pushes the Redis copy. flush() publishes everything to Kafka in ONE Spark
    write; a per-alert write would launch a distributed job per alert.

    A failed Kafka write never kills the micro-batch. It releases the dedup
    claims, so the next batch that still sees the condition publishes again
    instead of the alert staying muted for the TTL; the Redis copy already
    landed (and is pushed once more on that retry).
    """

    def __init__(self, r):
        self.r = r
        self.pending = []

    def emit(self, payload):
        if not self.r.set(_dedup_key(payload), "1", nx=True, ex=DEDUP_TTL_SECONDS):
            return
        pipe = self.r.pipeline()
        pipe.lpush(RECENT_KEY, json.dumps(payload))
        pipe.ltrim(RECENT_KEY, 0, RECENT_MAX - 1)
        pipe.execute()
        self.pending.append(payload)

    def flush(self, spark):
        if not self.pending:
            return
        pending, self.pending = self.pending, []
        try:
            spark.createDataFrame([(json.dumps(p),) for p in pending],
                                  schema="value string") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", ALERTS_TOPIC) \
                .save()
        except Exception as e:  # noqa: BLE001 - any Spark/Kafka failure; the batch survives
            warn(f"alert Kafka flush failed ({len(pending)} alerts): {e}")
            self.r.delete(*(_dedup_key(p) for p in pending))
