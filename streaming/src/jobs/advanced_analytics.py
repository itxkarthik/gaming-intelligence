"""Phase 3 Advanced Analytics orchestrator.

Starts the three advanced pipelines inside ONE Spark application so they
share a single executor slot on the 4-core standalone cluster:
  - smurf_detection     (player_events LOGIN -> Redis combat history)
  - behavior_change     (gameplay -> Redis-backed CUSUM -> cheat_detection)
  - economy_analytics   (kills/purchases sliding windows -> Redis)

Submit with: make spark-submit JOB=advanced_analytics
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql import SparkSession

from src.common.config import KAFKA_BOOTSTRAP_SERVERS
from src.jobs import smurf_detection, behavior_change, economy_analytics


def main():
    spark = SparkSession.builder \
        .appName("Gaming-AdvancedAnalytics") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to Kafka at: {KAFKA_BOOTSTRAP_SERVERS}")

    smurf_detection.start_queries(spark)
    print("  [1/3] Smurf detection query started (LOGIN -> account + combat z-scores)")

    behavior_change.start_queries(spark)
    print("  [2/3] Behavior change query started (CUSUM on per-batch accuracy)")

    economy_analytics.start_queries(spark)
    print("  [3/3] Economy queries started (weapon popularity + buy patterns)")

    print("Advanced Analytics pipelines started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()