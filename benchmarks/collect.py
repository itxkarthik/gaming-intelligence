#!/usr/bin/env python3
"""Metric collection for the Phase 6 benchmark (stdlib only).

Subcommands (called by benchmarks/run_benchmark.sh):

    snapshot               Kafka end offsets + all checkpoint offsets -> JSON
    lag SNAP0 SNAP1 DUR    produced/consumed during [SNAP0,SNAP1], lag at SNAP1
    freshness T0 T1        event-time -> Redis-write latency for results
                           written in [T0,T1]: p50/p95 in seconds
    stats CSV              peak/avg CPU + peak mem per container from
                           docker-stats samples
    tier JSON...           fold everything into one tier record (printed)

All docker access goes through subprocess and therefore inherits the
caller's group membership (run under `newgrp docker` or a docker group shell).
"""

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
CKPT = PROJECT / "data" / "checkpoints"
KAFKA = "gaming-kafka"
REDIS = "gaming-redis"

# checkpoint dirs owned by each streaming query (advanced_analytics = 4 queries)
DIRS = [
    "server_health",
    "cheat_detection",
    "match_quality",
    "behavior_change",
    "smurf_detection",
    "economy_kills",
    "economy_purchases",
]
TOPICS = ["gameplay_events", "player_events", "server_metrics", "alerts"]


def docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True
    ).stdout


def kafka_end_offsets() -> dict:
    """{topic: {partition: end_offset}} via GetOffsetShell (the only reliable
    offset CLI in this image — see gaming-intelligence-ops skill)."""
    out: dict = {}
    for t in TOPICS:
        raw = docker(
            "exec", KAFKA, "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "localhost:9092", "--topic", t,
        )
        parts: dict = {}
        for line in raw.splitlines():
            m = re.match(r"^(.+):(\d+):(\d+)$", line.strip())
            if m:
                parts[int(m.group(2))] = int(m.group(3))
        out[t] = parts
    return out


def checkpoint_offsets() -> dict:
    """{checkpoint_dir: {topic: {partition: committed_offset}}} — latest batch file."""
    out: dict = {}
    for d in DIRS:
        offs = CKPT / d / "offsets"
        if not offs.is_dir():
            continue
        numeric = sorted(
            (p for p in offs.iterdir() if p.name.isdigit()), key=lambda p: int(p.name)
        )
        if not numeric:
            continue
        # Spark writes one JSON line per source: a metadata line, a conf line,
        # then one {"<topic>": {partition: offset}} map per Kafka source —
        # match_quality reads gameplay+player, so ALL topic lines must merge.
        merged: dict = {}
        with open(numeric[-1]) as fh:
            for line in fh:
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict) and any(k in TOPICS for k in obj):
                    for t, parts in obj.items():
                        if t in TOPICS:
                            merged.setdefault(t, {}).update(parts)
        if merged:
            out[d] = merged
    return out


def cmd_snapshot() -> dict:
    return {"end": kafka_end_offsets(), "ckpt": checkpoint_offsets()}


def cmd_lag(snap0_path: str, snap1_path: str, duration: float) -> dict:
    s0 = json.load(open(snap0_path))
    s1 = json.load(open(snap1_path))
    produced = {}
    for t in TOPICS:
        e0, e1 = s0["end"].get(t, {}), s1["end"].get(t, {})
        produced[t] = sum(e1.values()) - sum(e0.values())
    per_dir = {}
    for d, ck1 in s1["ckpt"].items():
        ck0 = s0["ckpt"].get(d, {})
        consumed = 0
        for t, parts in ck1.items():
            p0 = ck0.get(t, {})
            for p, off in parts.items():
                consumed += off - p0.get(p, 0)
        # lag at snap1: end - committed, per matching topic/partition
        lag = 0
        for t, parts in ck1.items():
            ends = {str(k): v for k, v in s1["end"].get(t, {}).items()}
            for p, off in parts.items():
                lag += max(0, ends.get(str(p), off) - off)
        per_dir[d] = {
            "consumed": consumed,
            "rate": round(consumed / duration, 1) if duration else 0.0,
            "lag": lag,
        }
    return {
        "produced": produced,
        "produced_total": sum(produced.values()),
        "per_dir": per_dir,
        "max_lag": max((v["lag"] for v in per_dir.values()), default=0),
        "total_lag": sum(v["lag"] for v in per_dir.values()),
    }


def _parse_ts(s: str) -> float:
    """'2026-09-29 21:23:45[.ffffff]' (UTC — containers run UTC) -> epoch."""
    s = s.strip()
    if "." in s:
        head, frac = s.split(".", 1)
        frac = (frac + "000000")[:6]
        s = f"{head}.{frac}"
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f" if "." in s else
                             "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def cmd_freshness(t0: int, t1: int) -> dict:
    """updated_at in [t0,t1] minus window_end (event-time end) for match/server
    result hashes — the ROADMAP's 'event-time to result-time' metric."""
    sh = (
        "for k in $(redis-cli --scan --pattern 'match:*'; "
        "redis-cli --scan --pattern 'server:*'); do "
        "printf '%s|%s|%s\\n' \"$k\" \"$(redis-cli hget \"$k\" window_end)\" "
        "\"$(redis-cli hget \"$k\" updated_at)\"; done"
    )
    raw = docker("exec", REDIS, "sh", "-c", sh)
    samples = []
    for line in raw.splitlines():
        parts = line.split("|")
        if len(parts) != 3 or not parts[1] or not parts[2]:
            continue
        try:
            upd = int(parts[2])
            if t0 <= upd <= t1:
                samples.append(upd - _parse_ts(parts[1]))
        except ValueError:
            continue
    samples.sort()
    if not samples:
        return {"n": 0}

    def pct(p: float) -> float:
        if not samples:
            return 0.0
        idx = min(len(samples) - 1, max(0, round(p * (len(samples) - 1))))
        return round(samples[idx], 1)

    return {
        "n": len(samples),
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "max": round(samples[-1], 1),
    }


def _mem_gb(s: str) -> float:
    m = re.match(r"^\s*([\d.]+)\s*([KMG]i?B)", s)
    if not m:
        return 0.0
    val, unit = float(m.group(1)), m.group(2)
    scale = {"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30,
             "KB": 1e3, "MB": 1e6, "GB": 1e9, "kB": 1e3}
    return round(val * scale.get(unit, 1) / (1 << 30), 3)


def cmd_stats(csv_path: str) -> dict:
    """docker-stats samples (Name,CPUPerc,MemUsage per line) -> per-container peaks."""
    per: dict = {}
    try:
        lines = open(csv_path).read().splitlines()
    except FileNotFoundError:
        return {}
    for line in lines:
        bits = line.split(",")
        if len(bits) != 3:
            continue
        name, cpu, mem = bits[0].strip(), bits[1].strip(), bits[2].strip()
        try:
            cpu_v = float(cpu.rstrip("%"))
        except ValueError:
            continue
        e = per.setdefault(name, {"cpu_max": 0.0, "cpu_sum": 0.0, "n": 0,
                                  "mem_max_gb": 0.0})
        e["cpu_max"] = max(e["cpu_max"], cpu_v)
        e["cpu_sum"] += cpu_v
        e["n"] += 1
        e["mem_max_gb"] = max(e["mem_max_gb"], _mem_gb(mem))
    for e in per.values():
        e["cpu_avg"] = round(e.pop("cpu_sum") / max(1, e.pop("n")), 1)
    return per


def main() -> None:
    sub = sys.argv[1]
    if sub == "snapshot":
        print(json.dumps(cmd_snapshot()))
    elif sub == "lag":
        print(json.dumps(cmd_lag(sys.argv[2], sys.argv[3], float(sys.argv[4]))))
    elif sub == "freshness":
        print(json.dumps(cmd_freshness(int(sys.argv[2]), int(sys.argv[3]))))
    elif sub == "stats":
        print(json.dumps(cmd_stats(sys.argv[2])))
    elif sub == "tier":
        # tier RATE T0 T1 DRAIN_S SNAP0 SNAP1 FRESH STATS OUT
        (rate, t0, t1, drain, p0, p1, pf, ps, out) = sys.argv[2:11]
        snap1 = json.load(open(p1))
        lag = cmd_lag(p0, p1, float(t1) - float(t0))
        rec = {
            "target_rate": int(rate),
            "load_s": int(t1) - int(t0),
            "drain_s": int(drain),
            "produced_per_s": round(lag["produced_total"] / (float(t1) - float(t0)), 1),
            "produced": lag["produced"],
            "consumed": lag["per_dir"],
            "lag_at_end": lag["max_lag"],
            "freshness": json.load(open(pf)),
            "resources": cmd_stats(ps),
            "snap1": snap1,
        }
        with open(out, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        print(json.dumps({"rate": rate, "lag_at_end": rec["lag_at_end"],
                          "freshness": rec["freshness"]}))
    else:
        sys.exit(f"unknown subcommand: {sub}")


if __name__ == "__main__":
    main()
