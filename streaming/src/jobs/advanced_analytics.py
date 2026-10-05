"""Advanced Analytics: one Spark application for three pipelines.

They share a single executor slot on the 4-core standalone cluster:
  - smurf_detection   (LOGIN -> stored account; cheat_detection evaluates combat)
  - behavior_change   (gameplay -> Redis-backed CUSUM -> cheat_detection boost)
  - economy_analytics (kill / purchase sliding windows -> Redis)

Submit with: make spark-submit JOB=advanced_analytics
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.common.runtime import spark_session
from src.jobs import behavior_change, economy_analytics, smurf_detection


def main():
    spark = spark_session("Gaming-AdvancedAnalytics")
    smurf_detection.start_queries(spark)
    behavior_change.start_queries(spark)
    economy_analytics.start_queries(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
