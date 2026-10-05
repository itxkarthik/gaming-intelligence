#!/usr/bin/env python3
"""Metric collection for the Phase 6 benchmark (stdlib only).

Subcommands (called by benchmarks/run_benchmark.sh):

    snapshot               Kafka end offsets + committed checkpoint offsets
                           -> JSON on stdout
    maxlag                 take a fresh snapshot, print the worst per-query
                           lag (events behind) as one integer
    freshness T0 T1        event-time -> Redis-write latency for results
                           written in [T0,T1]: p50/p95/p99/max in seconds
    tier RATE T0 T1 DRAIN_S SNAP0 SNAP1 FRESH STATS OUT
                           fold one tier into a record appended to OUT

Lag has ONE definition here (`lag_by_dir`): Kafka end offset minus the
offset of the latest *committed* micro-batch, summed per checkpoint dir.
`maxlag` and `tier` both use it.

The simulator's --events-per-sec targets SHOT events on gameplay_events, so
throughput ("produced") counts that topic only. player_events and
server_metrics are side streams that scale with the match count and are
recorded for information; alerts are pipeline OUTPUT and are reported
separately, never as produced input.

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
INPUT_TOPIC = "gameplay_events"          # what --events-per-sec controls
SIDE_TOPICS = ["player_events", "server_metrics"]
ALERTS_TOPIC = "alerts"                  # pipeline output
TOPICS = [INPUT_TOPIC, *SIDE_TOPICS, ALERTS_TOPIC]


def docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=True
    ).stdout


def kafka_end_offsets() -> dict:
    """{topic: {"partition": end_offset}} via kafka-get-offsets.sh (the
    GetOffsetShell class run through kafka-run-class prints nothing in this
    image). Partition keys are strings, matching the checkpoint JSON."""
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
                parts[m.group(2)] = int(m.group(3))
        out[t] = parts
    return out


def _batch_ids(d: Path) -> set:
    return {int(p.name) for p in d.iterdir() if p.name.isdigit()} if d.is_dir() else set()


def checkpoint_offsets() -> dict:
    """{checkpoint_dir: {topic: {"partition": offset}}} of the latest batch
    that is both planned (offsets/N) and finished (commits/N).

    offsets/N is written when batch N STARTS, so its offsets are in flight;
    only a batch with a matching commits/N has actually been processed.
    """
    out: dict = {}
    for d in DIRS:
        done = _batch_ids(CKPT / d / "offsets") & _batch_ids(CKPT / d / "commits")
        if not done:
            continue
        # Spark writes one JSON line per source: a metadata line, a conf line,
        # then one {"<topic>": {partition: offset}} map per Kafka source —
        # match_quality reads gameplay+player, so ALL topic lines must merge.
        merged: dict = {}
        text = (CKPT / d / "offsets" / str(max(done))).read_text()
        for line in text.splitlines():
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if isinstance(obj, dict) and any(k in TOPICS for k in obj):
                for t, parts in obj.items():
                    if t in TOPICS:
                        merged.setdefault(t, {}).update(
                            {str(p): int(o) for p, o in parts.items()}
                        )
        if merged:
            out[d] = merged
    return out


def cmd_snapshot() -> dict:
    return {"end": kafka_end_offsets(), "ckpt": checkpoint_offsets()}


def _topic_total(snap: dict, topic: str) -> int:
    return sum(snap["end"].get(topic, {}).values())


def lag_by_dir(snap: dict) -> dict:
    """{dir: events behind} = sum over its partitions of end - committed."""
    lags = {}
    for d, ck in snap["ckpt"].items():
        lag = 0
        for t, parts in ck.items():
            ends = {str(k): v for k, v in snap["end"].get(t, {}).items()}
            for p, off in parts.items():
                lag += max(0, ends.get(str(p), off) - off)
        lags[d] = lag
    return lags


def cmd_maxlag() -> int:
    return max(lag_by_dir(cmd_snapshot()).values(), default=0)


def cmd_lag(s0: dict, s1: dict, duration: float) -> dict:
    """produced/consumed during [s0, s1] and lag at s1."""
    delta = {t: _topic_total(s1, t) - _topic_total(s0, t) for t in TOPICS}
    lags = lag_by_dir(s1)
    per_dir = {}
    for d, ck1 in s1["ckpt"].items():
        ck0 = s0["ckpt"].get(d)
        consumed = None
        if ck0 is not None:
            # Only partitions with a baseline: a partition missing from snap0
            # would otherwise count its whole history as consumed this tier.
            consumed = {
                t: sum(off - ck0[t][p] for p, off in parts.items() if p in ck0.get(t, {}))
                for t, parts in ck1.items()
                if t in ck0
            }
        per_dir[d] = {
            "consumed": consumed,
            # rate of the benchmarked input; side-stream-only queries
            # (server_health) report their own topic's rate
            "rate": (
                round(consumed.get(INPUT_TOPIC, sum(consumed.values())) / duration, 1)
                if consumed is not None and duration else None
            ),
            "lag": lags.get(d, 0),
        }
    return {
        "produced": {INPUT_TOPIC: delta[INPUT_TOPIC]},
        "side_streams": {t: delta[t] for t in SIDE_TOPICS},
        "alerts_out": delta[ALERTS_TOPIC],
        "per_dir": per_dir,
        "max_lag": max(lags.values(), default=0),
    }


def _parse_ts(s: str) -> float:
    """'2026-09-29 21:23:45[.ffffff]' (UTC — containers run UTC) -> epoch."""
    s = s.strip()
    fmt = "%Y-%m-%d %H:%M:%S"
    if "." in s:
        head, frac = s.split(".", 1)
        s = f"{head}.{(frac + '000000')[:6]}"
        fmt += ".%f"
    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()


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
    if not samples:
        return {"n": 0}
    samples.sort()

    def pct(p: float) -> float:
        return round(samples[min(len(samples) - 1, round(p * (len(samples) - 1)))], 1)

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
        lines = Path(csv_path).read_text().splitlines()
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


def _load(path: str):
    return json.loads(Path(path).read_text())


def cmd_tier(rate, t0, t1, drain, p0, p1, pf, ps, out) -> dict:
    load_s = int(t1) - int(t0)
    snap1 = _load(p1)
    lag = cmd_lag(_load(p0), snap1, float(load_s))
    rec = {
        "target_rate": int(rate),
        "load_s": load_s,
        "drain_s": int(drain),
        "produced_per_s": round(lag["produced"][INPUT_TOPIC] / load_s, 1) if load_s else 0.0,
        "produced": lag["produced"],
        "side_streams": lag["side_streams"],
        "alerts_out": lag["alerts_out"],
        "consumed": lag["per_dir"],
        "lag_at_end": lag["max_lag"],
        "freshness": _load(pf),
        "resources": cmd_stats(ps),
        "snap1": snap1,
    }
    with open(out, "a") as fh:
        fh.write(json.dumps(rec) + "\n")
    return rec


USAGE = "usage: collect.py snapshot | maxlag | freshness T0 T1 | tier RATE T0 T1 DRAIN_S SNAP0 SNAP1 FRESH STATS OUT"


def main() -> None:
    args = sys.argv[1:]
    sub = args[0] if args else ""
    if sub == "snapshot" and len(args) == 1:
        print(json.dumps(cmd_snapshot()))
    elif sub == "maxlag" and len(args) == 1:
        print(cmd_maxlag())
    elif sub == "freshness" and len(args) == 3:
        print(json.dumps(cmd_freshness(int(args[1]), int(args[2]))))
    elif sub == "tier" and len(args) == 10:
        rec = cmd_tier(*args[1:])
        print(json.dumps({"rate": rec["target_rate"],
                          "produced_per_s": rec["produced_per_s"],
                          "lag_at_end": rec["lag_at_end"],
                          "freshness": rec["freshness"]}))
    else:
        sys.exit(USAGE)


if __name__ == "__main__":
    main()
