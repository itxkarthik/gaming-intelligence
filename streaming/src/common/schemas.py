"""PySpark StructType Schema definitions for structured streaming parsing."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    BooleanType,
    LongType,
    TimestampType,
    MapType
)

GAMEPLAY_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("player_id", StringType(), False),
    StructField("team_id", StringType(), False),
    StructField("target_player_id", StringType(), True),
    StructField("weapon_id", StringType(), True),
    StructField("damage", FloatType(), True),
    StructField("position_x", FloatType(), False),
    StructField("position_y", FloatType(), False),
    StructField("position_z", FloatType(), False),
    StructField("accuracy", FloatType(), True),
    StructField("distance", FloatType(), True),
    StructField("reaction_time_ms", IntegerType(), True),
    StructField("is_headshot", BooleanType(), True),
    StructField("event_time", LongType(), False),
    StructField("server_id", StringType(), False)
])

PLAYER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("player_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("match_id", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
    StructField("event_time", LongType(), False),
    StructField("server_id", StringType(), True)
])

SERVER_METRIC_SCHEMA = StructType([
    StructField("server_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("cpu_percent", FloatType(), False),
    StructField("ram_percent", FloatType(), False),
    StructField("tick_rate", IntegerType(), False),
    StructField("packet_loss_percent", FloatType(), False),
    StructField("avg_latency_ms", FloatType(), False),
    StructField("active_players", IntegerType(), False),
    StructField("active_matches", IntegerType(), False),
    StructField("timestamp", LongType(), False)
])

ALERT_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("alert_type", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("entity_type", StringType(), False),
    StructField("entity_id", StringType(), False),
    StructField("message", StringType(), False),
    StructField("details", MapType(StringType(), StringType()), True),
    StructField("timestamp", LongType(), False)
])
