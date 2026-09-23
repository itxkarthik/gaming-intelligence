"""Central streaming configuration and environment settings."""

import os

# Kafka Configuration
# Defaults to container hostname 'kafka:9092'. When testing on host outside Docker, set KAFKA_BOOTSTRAP_SERVERS=localhost:9094.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = {
    "gameplay": os.getenv("TOPIC_GAMEPLAY", "gameplay_events"),
    "player": os.getenv("TOPIC_PLAYER", "player_events"),
    "server": os.getenv("TOPIC_SERVER", "server_metrics"),
    "alerts": os.getenv("TOPIC_ALERTS", "alerts")
}

# Redis Configuration (Real-time state sink)
# Defaults to container hostname 'redis'. When testing on host outside Docker, set REDIS_HOST=localhost.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# PostgreSQL Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "gaming_platform")
POSTGRES_USER = os.getenv("POSTGRES_USER", "gaming")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "gaming_dev")

# Checkpoints and Output Storage
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
HDFS_OUTPUT_DIR = os.getenv("HDFS_OUTPUT_DIR", "/tmp/gaming-data/parquet")

# Streaming Trigger Intervals
DEFAULT_PROCESSING_TIME = os.getenv("DEFAULT_PROCESSING_TIME", "5 seconds")
ALERT_PROCESSING_TIME = os.getenv("ALERT_PROCESSING_TIME", "2 seconds")
