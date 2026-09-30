#!/usr/bin/env python3
"""Historical batch analysis over the streaming Parquet archive (Phase 6).

Runs offline in PySpark BATCH mode against data/parquet/ (mounted into the
Spark containers at /tmp/gaming-data/parquet). Six analyses map to the
ROADMAP Phase 6 spec:

    skill     Player skill progression over time (accuracy / reaction time)
    weapon    Weapon meta — kill share, accuracy, headshot rate, per-rank picks
    cheat     Cheat detection precision/recall vs simulator archetype labels
    quality   Match quality distribution histogram + status mix
    servers   Server reliability ranking (health, critical time, latency)
    peak      Peak-hour analysis (events per hour of day)
    all       (default) every analysis, in order

Submit with `make spark-batch JOB=<name>` — local mode on purpose: the
4-core cluster is fully subscribed by the streaming jobs, and batch analysis
should not steal from them.

Ground truth for the accuracy job: the simulator stamps every LOGIN event
with its `archetype` metadata (simulator/cmd/simulator/main.go) which lands
in the smurf_detection archive's `metadata` map — labels never touch Redis.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import HDFS_OUTPUT_DIR

RANK_ORDER = ["bronze", "silver", "gold", "diamond"]


def banner(title: str) -> None:
    pad = max(0, 72 - len(title) - 4)
    print(f"\n── {title} " + "─" * pad)


def note(msg: str) -> None:
    print(f"   {msg}")


def read(spark, name):
    path = os.path.join(HDFS_OUTPUT_DIR, name)
    if not os.path.isdir(path):
        print(f"\n── {name}: archive not found at {path} (skipped)")
        return None
    df = spark.read.parquet(path)
    if df.rdd.isEmpty():
        print(f"\n── {name}: archive empty (skipped)")
        return None
    return df


def labels(spark):
    """player_id -> (archetype, rank) from LOGIN metadata — simulator truth."""
    smurf = read(spark, "smurf_detection")
    if smurf is None:
        return None
    return (
        smurf.filter(F.col("event_type") == "LOGIN")
        .select(
            "player_id",
            F.col("metadata")["archetype"].alias("archetype"),
            F.col("metadata")["rank"].alias("rank"),
        )
        .filter(F.col("archetype").isNotNull())
        .dropDuplicates(["player_id"])
    )


def table(headers, rows, aligns=None):
    """Minimal aligned text table — no tabulate dependency."""
    aligns = aligns or ["<"] * len(headers)
    fmts = [
        (f"{{:{a}}}" if a in "<>" else f"{{:{a}}}")
        for a in aligns
    ]
    print("   " + "  ".join(f.format(h) for f, h in zip(fmts, headers)))
    for row in rows:
        cells = []
        for f, v in zip(fmts, row):
            cells.append(f.format(v))
        print("   " + "  ".join(cells))


# ── skill ─────────────────────────────────────────────────────────────────
def job_skill(spark):
    banner("PLAYER SKILL PROGRESSION (15-min buckets)")
    beh = read(spark, "behavior_change")
    if beh is None:
        return
    agg = (
        beh.filter(F.col("accuracy").isNotNull() & F.col("event_time").isNotNull())
        .withColumn(
            "bucket",
            F.window(F.from_unixtime(F.col("event_time") / 1000), "15 minutes").start,
        )
        .groupBy("bucket")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.approx_count_distinct("player_id").alias("players"),
            F.avg("accuracy").alias("avg_acc"),
            F.percentile_approx("accuracy", 0.5).alias("p50_acc"),
            F.avg("reaction_time_ms").alias("avg_rxn"),
        )
        .orderBy("bucket")
        .collect()
    )
    if not agg:
        note("no rows")
        return
    table(
        ["BUCKET (UTC)", "EVENTS", "PLAYERS", "AVG ACC", "P50 ACC", "AVG RXN ms"],
        [
            [
                str(r["bucket"])[:16],
                f"{r['events']:,}",
                r["players"],
                f"{r['avg_acc']:.3f}",
                f"{r['p50_acc']:.3f}",
                f"{r['avg_rxn']:.0f}",
            ]
            for r in agg
        ],
        ["<", ">", ">", ">", ">", ">"],
    )
    first, last = agg[0], agg[-1]
    delta = last["avg_acc"] - first["avg_acc"]
    note(
        f"accuracy drift first→last bucket: {delta:+.3f} "
        f"({first['avg_acc']:.3f} → {last['avg_acc']:.3f})"
    )


# ── weapon ────────────────────────────────────────────────────────────────
def job_weapon(spark):
    banner("WEAPON META — kills, accuracy, headshots")
    beh = read(spark, "behavior_change")
    if beh is None:
        return
    kills = beh.filter(F.col("event_type") == "KILL")
    lab = labels(spark)

    meta = (
        kills.groupBy("weapon_id")
        .agg(
            F.count(F.lit(1)).alias("kills"),
            F.avg(F.col("is_headshot").cast("int")).alias("hs_rate"),
            F.avg("accuracy").alias("avg_acc"),
        )
        .orderBy(F.col("kills").desc())
        .collect()
    )
    if not meta:
        note("no KILL rows")
        return
    total_kills = sum(r["kills"] for r in meta)
    # purchase mix from the economy archive (small: one row per weapon)
    purch = {}
    econ = read(spark, "economy_purchases")
    if econ is not None:
        for r in econ.groupBy("weapon_id").agg(F.sum("purchases").alias("p")).collect():
            purch[r["weapon_id"]] = int(r["p"])
    table(
        ["WEAPON", "KILLS", "SHARE", "HS RATE", "AVG ACC", "PURCHASES"],
        [
            [
                r["weapon_id"],
                f"{r['kills']:,}",
                f"{100.0 * r['kills'] / total_kills:.1f}%",
                f"{100.0 * r['hs_rate']:.1f}%",
                f"{r['avg_acc']:.3f}",
                f"{purch.get(r['weapon_id'], 0):,}",
            ]
            for r in meta
        ],
        ["<", ">", ">", ">", ">", ">"],
    )

    if lab is None:
        note("no archetype labels — per-rank table skipped")
        return
    by_rank = (
        kills.join(lab, "player_id")
        .groupBy("rank", "weapon_id")
        .agg(F.count(F.lit(1)).alias("kills"))
    )
    w = Window.partitionBy("rank").orderBy(F.col("kills").desc())
    top = (
        by_rank.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") <= 3)
        .collect()
    )
    banner("DOMINANT WEAPON PER RANK (top 3)")
    for rank in RANK_ORDER:
        picks = [r for r in top if r["rank"] == rank]
        if not picks:
            continue
        line = ", ".join(f"{r['weapon_id']} ({r['kills']})" for r in picks)
        note(f"{rank:<8} {line}")


# ── cheat ─────────────────────────────────────────────────────────────────
def job_cheat(spark):
    banner("CHEAT DETECTION ACCURACY vs ARCHETYPE LABELS")
    lab = labels(spark)
    cheat = read(spark, "cheat_detection")
    if lab is None or cheat is None:
        note("missing labels or cheat archive — skipped")
        return
    pred = cheat.groupBy("player_id").agg(
        F.max("suspicion_score").alias("max_suspicion")
    )
    ev = (
        lab.join(pred, "player_id", "left")
        .withColumn("max_suspicion", F.coalesce(F.col("max_suspicion"), F.lit(0.0)))
        .withColumn("actual", F.col("archetype").startswith("cheater"))
        .cache()
    )
    total = ev.count()
    positives = ev.filter(F.col("actual")).count()
    covered = ev.filter(F.col("max_suspicion") > 0).count()
    note(
        f"labeled players: {total} · actual cheaters: {positives} · "
        f"scored in cheat windows: {covered}"
    )
    table(
        ["THRESHOLD", "PRED+", "TP", "FP", "FN", "PRECISION", "RECALL", "F1"],
        _sweep(ev, [0.50, 0.60, 0.70, 0.80, 0.90]),
        ["<", ">", ">", ">", ">", ">", ">", ">"],
    )
    note("primary operating point = 0.70 (matches the alerting threshold)")
    ev.unpersist()


def _sweep(ev, thresholds):
    rows = []
    for thr in thresholds:
        tp = ev.filter(F.col("actual") & (F.col("max_suspicion") >= thr)).count()
        fp = ev.filter(~F.col("actual") & (F.col("max_suspicion") >= thr)).count()
        fn = ev.filter(F.col("actual") & (F.col("max_suspicion") < thr)).count()
        pred_pos = tp + fp
        precision = tp / pred_pos if pred_pos else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall)
            else 0.0
        )
        rows.append(
            [f"{thr:.2f}", pred_pos, tp, fp, fn,
             f"{precision:.3f}", f"{recall:.3f}", f"{f1:.3f}"]
        )
    return rows


# ── quality ───────────────────────────────────────────────────────────────
def job_quality(spark):
    banner("MATCH QUALITY DISTRIBUTION (latest score per match)")
    mq = read(spark, "match_quality")
    if mq is None:
        return
    w = Window.partitionBy("match_id").orderBy(
        F.col("session_window")["end"].desc()
    )
    latest = (
        mq.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .cache()
    )
    n = latest.count()
    if not n:
        note("no matches")
        return
    buckets = (
        latest.withColumn(
            "bucket", (F.floor(F.col("quality_score") / 10)).cast("int")
        )
        .groupBy("bucket")
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    )
    by_bucket = {int(r["bucket"]): int(r["n"]) for r in buckets}
    peak = max(by_bucket.values())
    for b in range(10):
        c = by_bucket.get(b, 0)
        bar = "█" * max(1, round(30 * c / peak)) if c else ""
        lo, hi = b * 10, b * 10 + 10
        print(f"   {lo:>3}–{hi:<3} {c:>4} {bar}")
    statuses = (
        latest.groupBy("status").agg(F.count(F.lit(1)).alias("n")).collect()
    )
    stats = latest.agg(
        F.avg("quality_score").alias("avg"),
        F.percentile_approx("quality_score", 0.5).alias("p50"),
        F.min("quality_score").alias("lo"),
        F.max("quality_score").alias("hi"),
    ).collect()[0]
    table(
        ["STATUS", "MATCHES", "SHARE"],
        [
            [r["status"], r["n"], f"{100.0 * r['n'] / n:.1f}%"]
            for r in sorted(statuses, key=lambda x: -x["n"])
        ],
    )
    note(
        f"score avg {stats['avg']:.1f} · p50 {stats['p50']:.1f} · "
        f"range {stats['lo']:.1f}–{stats['hi']:.1f} · {n} matches"
    )
    latest.unpersist()


# ── servers ───────────────────────────────────────────────────────────────
def job_servers(spark):
    banner("SERVER RELIABILITY RANKING (all archived windows)")
    sh = read(spark, "server_health")
    if sh is None:
        return
    agg = (
        sh.groupBy("server_id", "region")
        .agg(
            F.count(F.lit(1)).alias("windows"),
            F.avg("health_score").alias("avg_health"),
            F.min("health_score").alias("min_health"),
            F.avg("avg_latency").alias("avg_lat"),
            F.avg("avg_packet_loss").alias("avg_loss"),
            F.avg(F.when(F.col("status") == "CRITICAL", 1.0).otherwise(0.0)).alias(
                "critical_pct"
            ),
            F.avg(F.when(F.col("status") == "DEGRADED", 1.0).otherwise(0.0)).alias(
                "degraded_pct"
            ),
        )
        .orderBy(F.col("avg_health").desc())
        .collect()
    )
    table(
        ["#", "SERVER", "REGION", "WINDOWS", "AVG HLTH", "MIN HLTH",
         "AVG LAT", "LOSS", "CRIT%", "DEGR%"],
        [
            [
                i + 1, r["server_id"], r["region"], r["windows"],
                f"{r['avg_health']:.1f}", f"{r['min_health']:.1f}",
                f"{r['avg_lat']:.0f}ms", f"{r['avg_loss']:.1f}%",
                f"{100 * r['critical_pct']:.0f}%", f"{100 * r['degraded_pct']:.0f}%",
            ]
            for i, r in enumerate(agg)
        ],
        ["<", "<", "<", ">", ">", ">", ">", ">", ">", ">"],
    )


# ── peak ──────────────────────────────────────────────────────────────────
def job_peak(spark):
    banner("PEAK HOUR ANALYSIS (events per hour of day, event time)")
    beh = read(spark, "behavior_change")
    if beh is None:
        return
    agg = (
        beh.filter(F.col("event_time").isNotNull())
        .withColumn("hour", F.hour(F.from_unixtime(F.col("event_time") / 1000)))
        .groupBy("hour")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.approx_count_distinct("player_id").alias("players"),
        )
        .orderBy("hour")
        .collect()
    )
    if not agg:
        note("no rows")
        return
    total = sum(r["events"] for r in agg)
    peak = max(r["events"] for r in agg)
    for r in agg:
        bar = "█" * max(1, round(36 * r["events"] / peak))
        print(
            f"   {r['hour']:>2}:00 {r['events']:>9,} {100.0 * r['events'] / total:>5.1f}% "
            f"{bar}  ({r['players']} players)"
        )
    note("synthetic traffic: archive spans a handful of simulated days")


def main():
    ap = argparse.ArgumentParser(description="Phase 6 batch analysis")
    ap.add_argument(
        "--job",
        default="all",
        choices=["skill", "weapon", "cheat", "quality", "servers", "peak", "all"],
    )
    args = ap.parse_args()

    spark = SparkSession.builder.appName("Gaming-BatchAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jobs = {
        "skill": job_skill,
        "weapon": job_weapon,
        "cheat": job_cheat,
        "quality": job_quality,
        "servers": job_servers,
        "peak": job_peak,
    }
    todo = jobs.values() if args.job == "all" else [jobs[args.job]]
    print(f"Historical batch analysis · archive={HDFS_OUTPUT_DIR} · job={args.job}")
    for fn in todo:
        fn(spark)
    print()
    spark.stop()


if __name__ == "__main__":
    main()
