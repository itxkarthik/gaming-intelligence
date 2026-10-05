"""Central streaming configuration and environment settings."""

import os

# Kafka. Defaults to the container hostname; on the host outside Docker set
# KAFKA_BOOTSTRAP_SERVERS=localhost:9094.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = {
    "gameplay": os.getenv("TOPIC_GAMEPLAY", "gameplay_events"),
    "player": os.getenv("TOPIC_PLAYER", "player_events"),
    "server": os.getenv("TOPIC_SERVER", "server_metrics"),
    "alerts": os.getenv("TOPIC_ALERTS", "alerts"),
}
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
# Per-source cap on records read by one micro-batch. Bounds the first batch
# after a restart on a backlog; 500k leaves headroom above the steady state
# (20k ev/s x a slow 15 s batch = 300k), so it never throttles live load.
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "500000"))

# Redis (real-time state sink). On the host outside Docker set REDIS_HOST=localhost.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Checkpoints and Parquet archive
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
HDFS_OUTPUT_DIR = os.getenv("HDFS_OUTPUT_DIR", "/tmp/gaming-data/parquet")

# Micro-batch trigger interval shared by every streaming query
DEFAULT_PROCESSING_TIME = os.getenv("DEFAULT_PROCESSING_TIME", "5 seconds")
