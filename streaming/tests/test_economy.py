"""Unit tests for economy analytics helpers."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.jobs.economy_analytics import summarise_counts


def test_shares_sum_to_100_and_sort_desc():
    summary = summarise_counts({"ak47": 60, "awp": 30, "deagle": 10})
    keys = list(summary.keys())
    assert keys == ["ak47", "awp", "deagle"]
    shares = [share for _, share in summary.values()]
    assert abs(sum(shares) - 100.0) < 0.5
    assert summary["ak47"] == (60, 60.0)


def test_zero_counts_yield_empty_summary():
    assert summarise_counts({}) == {}
    assert summarise_counts({"awp": 0}) == {} or summarise_counts({"awp": 0})["awp"][0] == 0


def test_single_weapon_is_100_pct():
    summary = summarise_counts({"usp": 7})
    assert summary == {"usp": (7, 100.0)}