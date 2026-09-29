"""Alert emission helpers shared by streaming jobs (dedup + dual sink).

Every alert goes to BOTH:
  - Redis `alerts:recent`  → fast path for the REST API (Phase 2 behavior)
  - Kafka `alerts` topic   → transport for the Go alert engine (Phase 4):
    dedup/rate-limit layer, PostgreSQL history, webhooks, WebSocket fan-out
"""

import json
import os
import sys

from src.common.config import KAFKA_BOOTSTRAP_SERVERS

DEFAULT_ALERT_TTL_SECONDS = 300
ALERTS_TOPIC = "alerts"


def should_emit_alert(redis_client, alert_type, entity_id,
                      ttl_seconds=DEFAULT_ALERT_TTL_SECONDS):
    """True at most once per (alert_type, entity) per TTL window.

    Without this, every micro-batch re-alerts the same degraded server or
    flagged player and floods the capped alerts:recent list. Backed by
    Redis SET NX EX — atomic, no extra round trips.
    """
    key = f"alert:dedup:{alert_type}:{entity_id}"
    return bool(redis_client.set(key, "1", nx=True, ex=ttl_seconds))


def emit_alert(redis_client, spark, payload,
               ttl_seconds=DEFAULT_ALERT_TTL_SECONDS):
    """Dedup gate, then publish to Redis (API) and Kafka (alert engine).

    Kafka failures must never kill the micro-batch: they are logged and the
    Redis copy still lands, so the API keeps serving even if the broker hiccups.
    Returns True when the alert was emitted.
    """
    if not should_emit_alert(redis_client, payload["alert_type"],
                             payload["entity_id"], ttl_seconds):
        return False

    raw = json.dumps(payload)
    redis_client.lpush("alerts:recent", raw)
    redis_client.ltrim("alerts:recent", 0, 99)  # Keep latest 100 alerts

    try:
        spark.createDataFrame([(raw,)], schema="value string") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", ALERTS_TOPIC) \
            .save()
    except Exception as e:
        print(f"[WARN] alert Kafka publish failed ({payload['alert_id']}): {e}",
              file=sys.stderr)
    return True
