"""PySpark StructType Schema definitions and Kafka stream parsing utilities."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

# The simulator emits exactly one gameplay event per trigger pull, with its
# outcome inline (hit, damage, is_headshot, is_kill). Every row is therefore
# one shot: per-shot averages need no de-duplication, and a kill is a row
# with is_kill = true.
SHOT_EVENT = "SHOT"

GAMEPLAY_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("match_id", StringType(), False),
    StructField("player_id", StringType(), False),
    StructField("team_id", StringType(), False),
    StructField("target_player_id", StringType(), True),
    StructField("weapon_id", StringType(), True),
    StructField("damage", FloatType(), True),               # null on a miss
    StructField("position_x", FloatType(), False),
    StructField("position_y", FloatType(), False),
    StructField("position_z", FloatType(), False),
    StructField("accuracy", FloatType(), True),
    StructField("distance", FloatType(), True),
    StructField("reaction_time_ms", IntegerType(), True),
    StructField("hit", BooleanType(), True),
    StructField("is_headshot", BooleanType(), True),
    StructField("is_kill", BooleanType(), True),
    StructField("victim_hp_after", IntegerType(), True),    # null on a miss
    StructField("assister_id", StringType(), True),         # set on assisted kills
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

# kind (same keys as config.KAFKA_TOPICS) -> (schema, epoch-millis column,
# derived TimestampType column used for windows and watermarks)
STREAMS = {
    "gameplay": (GAMEPLAY_EVENT_SCHEMA, "event_time", "event_timestamp"),
    "player": (PLAYER_EVENT_SCHEMA, "event_time", "event_timestamp"),
    "server": (SERVER_METRIC_SCHEMA, "timestamp", "metric_timestamp"),
}


def with_timestamp(df, kind):
    """Add the stream's event-time column, keeping millisecond precision."""
    _, millis_col, ts_col = STREAMS[kind]
    return df.withColumn(ts_col, F.timestamp_millis(F.col(millis_col)))


def parse_stream(raw, kind):
    """Raw Kafka DataFrame -> typed columns of `kind` plus its event-time column."""
    schema = STREAMS[kind][0]
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    return with_timestamp(parsed, kind)
