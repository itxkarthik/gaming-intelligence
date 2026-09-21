"""Central streaming configuration and environment settings."""

import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094")
KAFKA_TOPICS = {
    "gameplay": os.getenv("TOPIC_GAMEPLAY", "gameplay_events"),
    "player": os.getenv("TOPIC_PLAYER", "player_events"),
    "server": os.getenv("TOPIC_SERVER", "server_metrics"),
    "alerts": os.getenv("TOPIC_ALERTS", "alerts")
}

# Redis Configuration (Real-time state sink)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Checkpoints and Output Storage
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
HDFS_OUTPUT_DIR = os.getenv("HDFS_OUTPUT_DIR", "/tmp/gaming-data/parquet")

# Streaming Trigger Intervals
DEFAULT_PROCESSING_TIME = os.getenv("DEFAULT_PROCESSING_TIME", "5 seconds")
ALERT_PROCESSING_TIME = os.getenv("ALERT_PROCESSING_TIME", "2 seconds")
