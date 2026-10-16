"""Outage regressions using stdlib unittest and the application's ASGI interface.

Run in the existing API image with the API source mounted at /app:
    python -m unittest discover -s tests -v

No lifespan is started: readers are mocked and no backing services are needed.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg
import redis

import main


async def request(path):
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await main.app({
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": "GET", "scheme": "http",
        "path": path, "raw_path": path.encode(), "root_path": "",
        "query_string": b"", "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 12345), "server": ("testserver", 80),
    }, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages
                    if message["type"] == "http.response.body")
    return start["status"], dict(start["headers"]), body.decode()


class OutageTests(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_outages_render_styled_html_503(self):
        for error, service in (
            (redis.ConnectionError("private Redis connection details"), "Redis"),
            (redis.TimeoutError("private Redis timeout details"), "Redis"),
            (asyncpg.PostgresError("private PostgreSQL connection details"), "PostgreSQL"),
        ):
            with self.subTest(service=service, error=type(error).__name__):
                with patch("state.read_servers", new=AsyncMock(side_effect=error)):
                    status, headers, body = await request("/servers")
                self.assertEqual(status, 503)
                self.assertIn(b"text/html", headers[b"content-type"])
                self.assertIn("Service unavailable", body)
                self.assertIn(f"{service} is temporarily unavailable", body)
                self.assertIn('/static/dashboard.css', body)
                self.assertIn('href="/servers"', body)
                self.assertIn('href="/"', body)
                self.assertNotIn(str(error), body)
                self.assertNotIn("STREAM CONNECTING", body)
                self.assertNotIn("data-init=", body)

    async def test_api_outages_remain_json_503(self):
        for error, service in (
            (redis.ConnectionError("private connection details"), "Redis"),
            (asyncpg.PostgresError("private connection details"), "PostgreSQL"),
        ):
            with self.subTest(service=service):
                with patch("state.read_servers", new=AsyncMock(side_effect=error)):
                    status, headers, body = await request("/api/v1/servers")
                self.assertEqual(status, 503)
                self.assertIn(b"application/json", headers[b"content-type"])
                self.assertEqual(json.loads(body), {"detail": f"{service} unavailable"})
                self.assertNotIn(str(error), body)

    async def test_unrelated_oserror_is_not_reported_as_service_unavailable(self):
        error = OSError("private filesystem details")
        for path in ("/servers", "/api/v1/servers"):
            with self.subTest(path=path):
                with patch("state.read_servers", new=AsyncMock(side_effect=error)):
                    with self.assertRaisesRegex(OSError, "private filesystem details"):
                        await request(path)

    async def test_unreachable_postgres_closed_port_returns_503(self):
        await main.state.close_pg_pool()
        with patch.object(
            main.state,
            "PG_DSN",
            "postgresql://gaming:gaming_dev@127.0.0.1:1/gaming_platform",
        ):
            status, headers, body = await request("/api/v1/alerts/history")
            self.assertEqual(status, 503)
            self.assertIn(b"application/json", headers[b"content-type"])
            self.assertEqual(json.loads(body), {"detail": "PostgreSQL unavailable"})

            status, headers, body = await request("/alerts")
            self.assertEqual(status, 503)
            self.assertIn(b"text/html", headers[b"content-type"])


if __name__ == "__main__":
    unittest.main()
