"""Stdlib-only benchmark checks; no Docker services are needed by the probes."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("collect", Path(__file__).resolve().parents[1] / "collect.py")
collect = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collect)


class CollectionTests(unittest.TestCase):
    def test_all_current_queries_are_required(self):
        self.assertIn("cheat_detection_v3", collect.DIRS)
        self.assertIn("behavior_change_steps_v2", collect.DIRS)
        self.assertIn("behavior_change", collect.DIRS)
        self.assertEqual(len(collect.DIRS), 8)

    def test_missing_checkpoints_are_not_zero_lag(self):
        for checkpoints in ({}, {"server_health": {"server_metrics": {"0": 10}}}):
            with self.subTest(checkpoints=checkpoints), patch.object(
                collect, "cmd_snapshot", return_value={"end": {}, "ckpt": checkpoints}
            ):
                with self.assertRaises(SystemExit):
                    collect.cmd_maxlag()

    def test_complete_checkpoints_report_worst_lag(self):
        snapshot = {
            "end": {"gameplay_events": {"0": 100}},
            "ckpt": {query: {"gameplay_events": {"0": 99}} for query in collect.DIRS},
        }
        snapshot["ckpt"]["behavior_change_steps_v2"]["gameplay_events"]["0"] = 80
        with patch.object(collect, "cmd_snapshot", return_value=snapshot):
            self.assertEqual(collect.cmd_maxlag(), 20)

    def test_partial_windows_do_not_produce_negative_freshness(self):
        updated = int(collect._parse_ts("2026-10-16 12:00:05"))
        response = (
            f"server:partial|2026-10-16 12:00:10|{updated}\n"
            f"match:complete|2026-10-16 12:00:00|{updated}\n"
        )
        with patch.object(collect, "docker", return_value=response):
            self.assertEqual(collect.cmd_freshness(updated - 1, updated + 1), {
                "n": 1, "p50": 5.0, "p95": 5.0, "p99": 5.0, "max": 5.0,
            })


if __name__ == "__main__":
    unittest.main()
