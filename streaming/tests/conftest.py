"""Shared pytest fixtures and path bootstrap for streaming pipeline tests."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("streaming-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
