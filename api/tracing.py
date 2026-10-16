"""Lightweight OpenTelemetry OTLP Tracer client for Jaeger.

Sends spans as OTLP JSON HTTP payloads to Jaeger's native OTLP HTTP port (:4318).
Standard library only (no external dependencies required).
"""

import asyncio
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from typing import Any

log = logging.getLogger("api.tracing")

JAEGER_ENDPOINT = os.getenv("JAEGER_ENDPOINT", "http://jaeger:4318/v1/traces")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "gaming-api")

_trace_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None


def generate_trace_id() -> str:
    """32-hex-character trace ID."""
    return f"{random.getrandbits(128):032x}"


def generate_span_id() -> str:
    """16-hex-character span ID."""
    return f"{random.getrandbits(64):016x}"


def parse_traceparent(header: str | None) -> tuple[str, str | None]:
    """Parse W3C traceparent header: 00-{trace_id}-{span_id}-{flags}."""
    if not header:
        return generate_trace_id(), None
    parts = header.strip().split("-")
    if len(parts) >= 3 and len(parts[1]) == 32:
        return parts[1], parts[2]
    return generate_trace_id(), None


def format_traceparent(trace_id: str, span_id: str) -> str:
    """Format W3C traceparent header."""
    return f"00-{trace_id}-{span_id}-01"


class Span:
    def __init__(self, name: str, trace_id: str, span_id: str, parent_span_id: str | None = None,
                 attributes: dict[str, Any] | None = None):
        self.name = name
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.attributes = attributes or {}
        self.start_time_ns = int(time.time() * 1_000_000_000)
        self.end_time_ns: int | None = None
        self.status_code = 1  # 1 = OK, 2 = ERROR

    def set_attribute(self, key: str, value: Any):
        self.attributes[key] = value

    def set_error(self, err: BaseException):
        self.status_code = 2
        self.attributes["error"] = True
        self.attributes["error.message"] = str(err)
        self.attributes["error.type"] = type(err).__name__

    def finish(self):
        if self.end_time_ns is None:
            self.end_time_ns = int(time.time() * 1_000_000_000)
            record_span(self)

    def to_otlp(self) -> dict[str, Any]:
        attrs = []
        for k, v in self.attributes.items():
            if isinstance(v, bool):
                val = {"boolValue": v}
            elif isinstance(v, int):
                val = {"intValue": v}
            elif isinstance(v, float):
                val = {"doubleValue": v}
            else:
                val = {"stringValue": str(v)}
            attrs.append({"key": k, "value": val})

        span_dict: dict[str, Any] = {
            "traceId": self.trace_id,
            "spanId": self.span_id,
            "name": self.name,
            "kind": 1,  # SPAN_KIND_INTERNAL
            "startTimeUnixNano": str(self.start_time_ns),
            "endTimeUnixNano": str(self.end_time_ns or self.start_time_ns),
            "attributes": attrs,
            "status": {"code": self.status_code},
        }
        if self.parent_span_id:
            span_dict["parentSpanId"] = self.parent_span_id
        return span_dict


def record_span(span: Span):
    global _trace_queue
    if _trace_queue is not None:
        try:
            _trace_queue.put_nowait(span)
        except asyncio.QueueFull:
            pass


@asynccontextmanager
async def start_span(name: str, parent_trace_id: str | None = None,
                     parent_span_id: str | None = None,
                     attributes: dict[str, Any] | None = None):
    trace_id = parent_trace_id or generate_trace_id()
    span_id = generate_span_id()
    span = Span(name, trace_id, span_id, parent_span_id, attributes)
    try:
        yield span
    except BaseException as e:
        span.set_error(e)
        raise
    finally:
        span.finish()


async def _export_worker():
    import urllib.request
    global _trace_queue
    while True:
        try:
            span = await _trace_queue.get()
            batch = [span]
            # Coalesce up to 50 spans
            while len(batch) < 50 and not _trace_queue.empty():
                batch.append(_trace_queue.get_nowait())

            otlp_payload = {
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {"key": "service.name", "value": {"stringValue": SERVICE_NAME}}
                            ]
                        },
                        "scopeSpans": [
                            {
                                "scope": {"name": "gaming-api"},
                                "spans": [s.to_otlp() for s in batch]
                            }
                        ]
                    }
                ]
            }

            body = json.dumps(otlp_payload).encode("utf-8")
            req = urllib.request.Request(
                JAEGER_ENDPOINT,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            # Send in background thread to avoid blocking asyncio loop
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: _send_req(req))
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.debug("failed to export trace to %s: %v", JAEGER_ENDPOINT, e)


def _send_req(req):
    import urllib.request
    try:
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            pass
    except Exception:
        pass


def start_tracer():
    global _trace_queue, _worker_task
    if _trace_queue is None:
        _trace_queue = asyncio.Queue(maxsize=1000)
        _worker_task = asyncio.create_task(_export_worker(), name="jaeger-tracer-worker")


def stop_tracer():
    global _worker_task
    if _worker_task is not None:
        _worker_task.cancel()
        _worker_task = None
