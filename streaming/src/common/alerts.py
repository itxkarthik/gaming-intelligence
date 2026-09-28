"""Alert emission helpers shared by streaming jobs (dedup + rate limiting)."""

DEFAULT_ALERT_TTL_SECONDS = 300


def should_emit_alert(redis_client, alert_type, entity_id,
                      ttl_seconds=DEFAULT_ALERT_TTL_SECONDS):
    """True at most once per (alert_type, entity) per TTL window.

    Without this, every micro-batch re-alerts the same degraded server or
    flagged player and floods the capped alerts:recent list. Backed by
    Redis SET NX EX — atomic, no extra round trips.
    """
    key = f"alert:dedup:{alert_type}:{entity_id}"
    return bool(redis_client.set(key, "1", nx=True, ex=ttl_seconds))