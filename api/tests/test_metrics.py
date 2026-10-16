import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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


class MetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_metrics_endpoint_returns_prometheus_format(self):
        with patch("state.current_counts", new=AsyncMock(return_value={
            "cheat_flagged": 5, "smurf": 3, "behavior_anomalies": 2,
        })), patch("state.read_servers", new=AsyncMock(return_value=[
            {"server_id": "s1", "health_score": "95.0", "status": "HEALTHY", "avg_cpu": "25", "avg_ram": "40", "avg_latency": "15", "peak_players": "10"},
        ])), patch("state.read_match_summary", new=AsyncMock(return_value={
            "tracked": 10, "balanced": 7, "unbalanced": 2, "stomped": 1, "histogram": [0]*10,
        })), patch("state.throughput_snapshot", return_value={
            "events_per_sec": 500.0, "ingested_total": 15000,
        }):
            status, headers, body = await request("/metrics")
            self.assertEqual(status, 200)
            self.assertIn("text/plain", headers[b"content-type"].decode())
            self.assertIn("gaming_cheaters_flagged 5", body)
            self.assertIn("gaming_smurfs_detected 3", body)
            self.assertIn("gaming_behavior_anomalies 2", body)
            self.assertIn("gaming_servers_active 1", body)
            self.assertIn("gaming_matches_scored_total 10", body)
            self.assertIn("gaming_kafka_events_per_second 500.00", body)


if __name__ == "__main__":
    unittest.main()
