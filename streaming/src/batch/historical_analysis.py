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
    peak      Peak-hour analysis (shots per hour of day)
    all       (default) every analysis, in order

Submit with `make spark-batch JOB=<name>` — local mode on purpose: the
4-core cluster is fully subscribed by the streaming jobs, and batch analysis
should not steal from them.

Ground truth for the cheat job: the simulator stamps every LOGIN event
with its `archetype` metadata (simulator/cmd/simulator/world.go) which lands
in the smurf_detection archive's `metadata` map — labels never touch Redis.

Archive semantics this job has to undo:
  * server_health and cheat_detection stream in UPDATE mode, so a window is
    archived again by every micro-batch that touched it (partial results).
    `latest_per_key` keeps only the last archived row per (entity, window).
  * economy_purchases uses 60 s windows sliding by 30 s, so every purchase
    sits in two windows; only the minute-aligned windows are summed.
  * economy_weapon_kills is not read here: weapon kill stats come from the
    per-kill gameplay rows (behavior_change), which also allow the per-rank
    join. That archive remains an audit trail of the streaming output.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import HDFS_OUTPUT_DIR
from src.common.schemas import SHOT_EVENT
from src.ml.iforest import MIN_SHOTS  # cheat_detection's evidence floor

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
    # mergeSchema: the archive holds files written before and after columns
    # were added (e.g. is_kill); without it Spark may take one file's schema.
    df = spark.read.option("mergeSchema", "true").parquet(path)
    if df.isEmpty():
        print(f"\n── {name}: archive empty (skipped)")
        return None
    return df


# The gameplay archive spans two wire formats. Legacy runs emitted a
# SHOT_FIRED row per shot plus separate DAMAGE and KILL rows that repeated
# the shot's accuracy and reaction time; current runs emit a single SHOT row
# with the outcome inline. These selectors return one row per shot and one
# row per kill under either format, so history is never double-counted.
LEGACY_SHOT, LEGACY_KILL = "SHOT_FIRED", "KILL"


def shot_rows(df):
    return df.filter(F.col("event_type").isin(SHOT_EVENT, LEGACY_SHOT))


def kill_rows(df):
    current = (F.col("event_type") == SHOT_EVENT) & F.col("is_kill") \
        if "is_kill" in df.columns else F.lit(False)
    return df.filter(current | (F.col("event_type") == LEGACY_KILL))


def latest_per_key(df, keys):
    """Keep the last archived row per key.

    Update-mode queries re-archive a window each micro-batch until the
    watermark closes it; the final row is the complete one. Every micro-batch
    writes its own file (coalesce(1)), so the source file's modification time
    orders the versions. Call on a DataFrame straight from read() — the
    hidden _metadata column only exists on the file source.
    """
    w = Window.partitionBy(*keys).orderBy(
        F.col("_metadata.file_modification_time").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


_LABELS = {}


def labels(spark):
    """player_id -> (archetype, rank, since) from each player's latest LOGIN.

    An account's archetype depends on the run's --cheater/--smurf/--toxic
    ratios, so one player can carry different labels in different runs.
    Ground truth is the most recent LOGIN, and evaluations only use rows
    from that run on (event time >= since). Picking an arbitrary LOGIN per
    player mixed labels from unrelated runs.

    Built once per session and cached: --job all uses it twice.
    """
    if spark not in _LABELS:
        smurf = read(spark, "smurf_detection")
        archetype = F.col("metadata")["archetype"]
        _LABELS[spark] = None if smurf is None else (
            smurf.filter((F.col("event_type") == "LOGIN") & archetype.isNotNull())
            .groupBy("player_id")
            .agg(
                F.max_by(archetype, "event_time").alias("archetype"),
                F.max_by(F.col("metadata")["rank"], "event_time").alias("rank"),
                F.max("event_time").alias("since"),
            )
            .cache()
        )
    return _LABELS[spark]


def table(headers, rows, aligns=None):
    """Minimal aligned text table — no tabulate dependency."""
    fmts = [f"{{:{a}}}" for a in aligns or ["<"] * len(headers)]
    for row in [headers, *rows]:
        print("   " + "  ".join(f.format(v) for f, v in zip(fmts, row)))


# ── skill ─────────────────────────────────────────────────────────────────
def job_skill(spark):
    banner("PLAYER SKILL PROGRESSION (15-min buckets)")
    beh = read(spark, "behavior_change")
    if beh is None:
        return
    agg = (
        shot_rows(beh)
        .filter(F.col("accuracy").isNotNull() & F.col("event_time").isNotNull())
        .withColumn(
            "bucket",
            F.window(F.from_unixtime(F.col("event_time") / 1000), "15 minutes").start,
        )
        .groupBy("bucket")
        .agg(
            F.count(F.lit(1)).alias("shots"),
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
        ["BUCKET (UTC)", "SHOTS", "PLAYERS", "AVG ACC", "P50 ACC", "AVG RXN ms"],
        [
            [
                str(r["bucket"])[:16],
                f"{r['shots']:,}",
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
    kills = kill_rows(beh)
    lab = labels(spark)

    # Kill-side stats from kill rows; accuracy from shot rows, where each
    # shot counts once (legacy KILL rows repeated the killing shot's value).
    accuracy = shot_rows(beh).groupBy("weapon_id").agg(F.avg("accuracy").alias("avg_acc"))
    meta = (
        kills.groupBy("weapon_id")
        .agg(
            F.count(F.lit(1)).alias("kills"),
            F.avg(F.col("is_headshot").cast("int")).alias("hs_rate"),
        )
        .join(accuracy, "weapon_id", "left")
        .orderBy(F.col("kills").desc())
        .collect()
    )
    if not meta:
        note("no kills")
        return
    total_kills = sum(r["kills"] for r in meta)
    # Purchase mix from the economy archive. Its 60 s windows slide by 30 s,
    # so each purchase is in two windows: sum only the minute-aligned ones,
    # which tile time exactly once. Append mode emits a window once; the
    # dropDuplicates absorbs replayed micro-batches.
    purch = {}
    econ = read(spark, "economy_purchases")
    if econ is not None:
        aligned = (
            econ.filter(F.unix_timestamp(F.col("window.start")) % 60 == 0)
            .dropDuplicates(["window", "weapon_id"])
        )
        for r in aligned.groupBy("weapon_id").agg(F.sum("purchases").alias("p")).collect():
            purch[r["weapon_id"]] = int(r["p"])
    table(
        ["WEAPON", "KILLS", "SHARE", "HS RATE", "AVG ACC", "PURCHASES"],
        [
            [
                r["weapon_id"],
                f"{r['kills']:,}",
                f"{100.0 * r['kills'] / total_kills:.1f}%",
                f"{100.0 * r['hs_rate']:.1f}%",
                f"{r['avg_acc']:.3f}" if r["avg_acc"] is not None else "—",
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
        .filter(F.col("event_time") >= F.col("since"))
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
    # Final row per window (the archive holds every update-mode partial),
    # then production's flag gate: a window needs >= MIN_SHOTS shots
    # (cheat_detection.py: flagged = (... >= 0.70 or iforest) and
    # total_shots >= MIN_SHOTS). Judge each player only on windows from their latest
    # labelled run.
    run_start = (F.col("since") / 1000).cast("timestamp")
    pred = (
        latest_per_key(cheat, ["player_id", "match_id", "window"])
        .filter(F.col("total_shots") >= MIN_SHOTS)
        .join(lab.select("player_id", "since"), "player_id")
        .filter(F.col("window.start") >= run_start)
        .groupBy("player_id")
        .agg(F.max("suspicion_score").alias("max_suspicion"))
    )
    ev = (
        lab.join(pred, "player_id", "left")
        # coverage BEFORE the coalesce: a scored window with suspicion 0.0
        # still counts as covered
        .withColumn("scored", F.col("max_suspicion").isNotNull())
        .withColumn("max_suspicion", F.coalesce(F.col("max_suspicion"), F.lit(0.0)))
        .withColumn("actual", F.col("archetype").startswith("cheater"))
        .cache()
    )
    total = ev.count()
    positives = ev.filter(F.col("actual")).count()
    covered = ev.filter(F.col("scored")).count()
    note(
        f"labeled players: {total} · actual cheaters: {positives} · "
        f"scored in a >= {MIN_SHOTS}-shot window: {covered}"
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
            # a perfect 100 joins the 90–100 bucket instead of an 11th
            "bucket", F.least(F.floor(F.col("quality_score") / 10), F.lit(9)).cast("int")
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
    # one row per 10 s window: the archive holds every update-mode partial
    agg = (
        latest_per_key(sh, ["server_id", "region", "window"])
        .groupBy("server_id", "region")
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
    banner("PEAK HOUR ANALYSIS (shots per hour of day, event time)")
    beh = read(spark, "behavior_change")
    if beh is None:
        return
    # one row per shot under either archive format (legacy DAMAGE/KILL rows
    # would otherwise inflate hours with more hits)
    agg = (
        shot_rows(beh)
        .filter(F.col("event_time").isNotNull())
        .withColumn("hour", F.hour(F.from_unixtime(F.col("event_time") / 1000)))
        .groupBy("hour")
        .agg(
            F.count(F.lit(1)).alias("shots"),
            F.approx_count_distinct("player_id").alias("players"),
        )
        .orderBy("hour")
        .collect()
    )
    if not agg:
        note("no rows")
        return
    total = sum(r["shots"] for r in agg)
    peak = max(r["shots"] for r in agg)
    for r in agg:
        bar = "█" * max(1, round(36 * r["shots"] / peak))
        print(
            f"   {r['hour']:>2}:00 {r['shots']:>9,} shots {100.0 * r['shots'] / total:>5.1f}% "
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
