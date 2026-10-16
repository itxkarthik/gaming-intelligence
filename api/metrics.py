"""Prometheus metrics exporter for Gaming Intelligence Platform.

Generates Prometheus text exposition format (version 0.0.4).
Exposes real-time game telemetry, anti-cheat state, match quality,
and API HTTP request counts / latencies.
"""

import time
from typing import Any
import state


class MetricsCollector:
    def __init__(self):
        self.request_counts: dict[tuple[str, str, int], int] = {}
        self.request_latencies: dict[tuple[str, str], list[float]] = {}

    def record_request(self, method: str, path: str, status: int, duration_s: float):
        # Normalize paths for low cardinality
        normalized_path = path
        if path.startswith("/players/"):
            normalized_path = "/players/{player_id}"
        elif path.startswith("/matches/"):
            normalized_path = "/matches/{match_id}"
        elif path.startswith("/sse/"):
            normalized_path = "/sse/{view}"

        key = (method, normalized_path, status)
        self.request_counts[key] = self.request_counts.get(key, 0) + 1

        lat_key = (method, normalized_path)
        lat_list = self.request_latencies.setdefault(lat_key, [])
        lat_list.append(duration_s)
        if len(lat_list) > 100:
            lat_list.pop(0)

    async def generate_metrics_text(self) -> str:
        lines: list[str] = []

        # ── 1. Anti-Cheat & Player Intelligence Metrics (from Redis) ──────
        try:
            counts = await state.current_counts()
            lines.extend([
                "# HELP gaming_cheaters_flagged Current number of flagged cheaters",
                "# TYPE gaming_cheaters_flagged gauge",
                f"gaming_cheaters_flagged {counts.get('cheat_flagged', 0)}",
                "",
                "# HELP gaming_smurfs_detected Current number of detected smurf accounts",
                "# TYPE gaming_smurfs_detected gauge",
                f"gaming_smurfs_detected {counts.get('smurf', 0)}",
                "",
                "# HELP gaming_behavior_anomalies Current number of players with CUSUM behavior shifts",
                "# TYPE gaming_behavior_anomalies gauge",
                f"gaming_behavior_anomalies {counts.get('behavior_anomalies', 0)}",
                "",
            ])
        except Exception:
            pass

        # ── 2. Server Fleet Metrics ───────────────────────────────────────
        try:
            servers = await state.read_servers()
            active_count = len(servers)
            degraded_count = sum(1 for s in servers if s.get("status") != state.SERVER_HEALTHY)
            avg_health = (sum(state.as_float(s.get("health_score")) for s in servers) / active_count
                          if active_count else 100.0)
            avg_cpu = (sum(state.as_float(s.get("avg_cpu")) for s in servers) / active_count
                       if active_count else 0.0)
            avg_ram = (sum(state.as_float(s.get("avg_ram")) for s in servers) / active_count
                       if active_count else 0.0)
            avg_lat = (sum(state.as_float(s.get("avg_latency")) for s in servers) / active_count
                       if active_count else 0.0)
            peak_players = sum(int(state.as_float(s.get("peak_players"))) for s in servers)

            lines.extend([
                "# HELP gaming_servers_active Total active game servers",
                "# TYPE gaming_servers_active gauge",
                f"gaming_servers_active {active_count}",
                "",
                "# HELP gaming_servers_degraded Degraded game servers count",
                "# TYPE gaming_servers_degraded gauge",
                f"gaming_servers_degraded {degraded_count}",
                "",
                "# HELP gaming_servers_avg_health Fleet average server health score (0-100)",
                "# TYPE gaming_servers_avg_health gauge",
                f"gaming_servers_avg_health {avg_health:.2f}",
                "",
                "# HELP gaming_servers_avg_cpu Fleet average CPU percent",
                "# TYPE gaming_servers_avg_cpu gauge",
                f"gaming_servers_avg_cpu {avg_cpu:.2f}",
                "",
                "# HELP gaming_servers_avg_ram Fleet average RAM percent",
                "# TYPE gaming_servers_avg_ram gauge",
                f"gaming_servers_avg_ram {avg_ram:.2f}",
                "",
                "# HELP gaming_servers_avg_latency_ms Fleet average latency in ms",
                "# TYPE gaming_servers_avg_latency_ms gauge",
                f"gaming_servers_avg_latency_ms {avg_lat:.2f}",
                "",
                "# HELP gaming_servers_active_players Total active players on servers",
                "# TYPE gaming_servers_active_players gauge",
                f"gaming_servers_active_players {peak_players}",
                "",
            ])
        except Exception:
            pass

        # ── 3. Match Quality Metrics ──────────────────────────────────────
        try:
            summary = await state.read_match_summary()
            lines.extend([
                "# HELP gaming_matches_scored_total Total matches evaluated for match quality",
                "# TYPE gaming_matches_scored_total gauge",
                f"gaming_matches_scored_total {summary.get('tracked', 0)}",
                "",
                "# HELP gaming_matches_balanced Matches with quality >= 70 (balanced)",
                "# TYPE gaming_matches_balanced gauge",
                f"gaming_matches_balanced {summary.get('balanced', 0)}",
                "",
                "# HELP gaming_matches_unbalanced Matches with quality between 40 and 70",
                "# TYPE gaming_matches_unbalanced gauge",
                f"gaming_matches_unbalanced {summary.get('unbalanced', 0)}",
                "",
                "# HELP gaming_matches_stomped Matches with quality < 40 (one-sided)",
                "# TYPE gaming_matches_stomped gauge",
                f"gaming_matches_stomped {summary.get('stomped', 0)}",
                "",
                "# HELP gaming_matches_live Live ongoing matches detected from server metrics",
                "# TYPE gaming_matches_live gauge",
                f"gaming_matches_live {state.feed.live_matches()}",
                "",
            ])
        except Exception:
            pass

        # ── 4. Kafka Event Stream Ingestion Throughput ────────────────────
        try:
            snap = state.throughput_snapshot()
            eps = snap.get("events_per_sec") or 0.0
            total_events = snap.get("ingested_total") or 0
            lines.extend([
                "# HELP gaming_kafka_events_per_second Ingestion throughput rate (events/sec)",
                "# TYPE gaming_kafka_events_per_second gauge",
                f"gaming_kafka_events_per_second {eps:.2f}",
                "",
                "# HELP gaming_kafka_ingested_events_total Cumulative events ingested",
                "# TYPE gaming_kafka_ingested_events_total counter",
                f"gaming_kafka_ingested_events_total {total_events}",
                "",
            ])
        except Exception:
            pass

        # ── 5. Alert History & Memory Log ─────────────────────────────────
        lines.extend([
            "# HELP gaming_alerts_buffer_count Recent alerts retained in-memory",
            "# TYPE gaming_alerts_buffer_count gauge",
            f"gaming_alerts_buffer_count {len(state.alert_log)}",
            "",
        ])

        # ── 6. HTTP API Performance ───────────────────────────────────────
        lines.extend([
            "# HELP gaming_api_http_requests_total Total HTTP requests handled by the API",
            "# TYPE gaming_api_http_requests_total counter",
        ])
        for (method, path, status), count in sorted(self.request_counts.items()):
            lines.append(f'gaming_api_http_requests_total{{method="{method}",path="{path}",status="{status}"}} {count}')
        lines.append("")

        lines.extend([
            "# HELP gaming_api_http_request_duration_seconds Average latency of recent requests",
            "# TYPE gaming_api_http_request_duration_seconds gauge",
        ])
        for (method, path), lats in sorted(self.request_latencies.items()):
            if lats:
                avg_lat = sum(lats) / len(lats)
                lines.append(f'gaming_api_http_request_duration_seconds{{method="{method}",path="{path}"}} {avg_lat:.5f}')
        lines.append("")

        return "\n".join(lines)


collector = MetricsCollector()
