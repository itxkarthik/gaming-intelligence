"""Unit tests for the server health scoring pipeline (runs in the Spark container)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.schemas import SERVER_METRIC_SCHEMA
from src.jobs.server_health import build_health_pipeline

TS = 1_700_000_000_000  # fixed metric time => single 10s window


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("server-health-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _metric(server_id, cpu, ram, loss, latency, tick, region="eu-west"):
    # FloatType fields reject Python ints; tick_rate/players/matches are IntegerType
    return {
        "server_id": server_id,
        "region": region,
        "cpu_percent": float(cpu),
        "ram_percent": float(ram),
        "tick_rate": int(tick),
        "packet_loss_percent": float(loss),
        "avg_latency_ms": float(latency),
        "active_players": 30,
        "active_matches": 2,
        "timestamp": TS,
    }


def _score(spark, rows):
    df = spark.createDataFrame(rows, schema=SERVER_METRIC_SCHEMA)
    df = df.withColumn(
        "metric_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("timestamp") / 1000.0)),
    )
    return {r["server_id"]: r.asDict() for r in build_health_pipeline(df).collect()}


def test_healthy_server_scores_high(spark):
    scored = _score(spark, [_metric("srv_ok", cpu=40, ram=45, loss=0.3, latency=30, tick=128)])

    row = scored["srv_ok"]
    assert row["status"] == "HEALTHY"
    assert row["health_score"] >= 80.0
    assert row["raw_health_score"] == pytest.approx(84.2, abs=0.1)


def test_degraded_server_is_critical(spark):
    scored = _score(spark, [_metric("srv_bad", cpu=90, ram=95, loss=9.0, latency=131, tick=128)])

    row = scored["srv_bad"]
    assert row["status"] == "CRITICAL"
    assert row["health_score"] < 50.0
    assert row["health_score"] >= 0.0  # clamped, never negative


def test_low_tick_rate_is_penalized(spark):
    healthy = _metric("srv_128", cpu=40, ram=45, loss=0.3, latency=30, tick=128)
    laggy = _metric("srv_60", cpu=40, ram=45, loss=0.3, latency=30, tick=60)

    scored = _score(spark, [healthy, laggy])

    assert scored["srv_60"]["avg_tick_penalty"] == pytest.approx(10.0, abs=0.01)  # (64-60)*2.5
    assert scored["srv_60"]["health_score"] < scored["srv_128"]["health_score"]
    # Same machine, only tick rate differs -> drops from HEALTHY to DEGRADED
    assert scored["srv_128"]["status"] == "HEALTHY"
    assert scored["srv_60"]["status"] == "DEGRADED"


def test_averages_aggregate_over_window(spark):
    rows = [
        {**_metric("srv_avg", cpu=20, ram=40, loss=0.1, latency=20, tick=128),
         "timestamp": TS},
        {**_metric("srv_avg", cpu=60, ram=40, loss=0.1, latency=20, tick=128),
         "timestamp": TS + 5000},
    ]
    scored = _score(spark, rows)

    assert scored["srv_avg"]["avg_cpu"] == pytest.approx(40.0, abs=0.01)