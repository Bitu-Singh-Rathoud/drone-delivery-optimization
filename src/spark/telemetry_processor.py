"""
Spark Structured Streaming processor for drone telemetry.

Reads raw JSON messages from Kafka, applies a schema, and writes the
structured stream to S3 (Parquet) and a Kafka processed topic.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import kafka_config, spark_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

TELEMETRY_SCHEMA = StructType(
    [
        StructField("drone_id", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("altitude", FloatType(), False),
        StructField("speed", FloatType(), False),
        StructField("heading", FloatType(), False),
        StructField("battery_level", FloatType(), False),
        StructField("status", StringType(), False),
        StructField("timestamp", DoubleType(), True),
        StructField("destination_lat", DoubleType(), True),
        StructField("destination_lon", DoubleType(), True),
        StructField("payload_weight", FloatType(), True),
    ]
)


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def create_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """Build and return a :class:`SparkSession` configured for the pipeline."""
    name = app_name or spark_config.SPARK_APP_NAME
    builder = (
        SparkSession.builder.appName(name)
        .master(spark_config.SPARK_MASTER)
        .config("spark.jars.packages", spark_config.SPARK_PACKAGES)
        # Kafka source
        .config("spark.sql.streaming.schemaInference", "true")
        # S3a settings populated from environment
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
    )
    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Core processor
# ---------------------------------------------------------------------------

class TelemetryProcessor:
    """
    Reads drone telemetry from Kafka, parses JSON, and writes to S3.

    Typical usage::

        spark = create_spark_session()
        processor = TelemetryProcessor(spark)
        query = processor.start()
        query.awaitTermination()
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_stream(self) -> DataFrame:
        """Return a streaming DataFrame of parsed telemetry records."""
        raw = (
            self._spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", kafka_config.TELEMETRY_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed = raw.select(
            F.col("key").cast(StringType()).alias("message_key"),
            F.from_json(F.col("value").cast(StringType()), TELEMETRY_SCHEMA).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        ).select(
            "message_key",
            "kafka_timestamp",
            "data.*",
        )

        # Enrich with event-time column
        return parsed.withColumn(
            "event_time",
            F.to_timestamp(F.col("timestamp").cast(DoubleType())),
        )

    def write_to_s3(
        self,
        df: DataFrame,
        s3_path: str,
        checkpoint_location: Optional[str] = None,
        trigger_interval: Optional[str] = None,
    ):
        """Write streaming DataFrame to S3 in Parquet format."""
        checkpoint = checkpoint_location or (
            spark_config.STREAMING_CHECKPOINT_LOCATION + "/telemetry"
        )
        trigger = trigger_interval or spark_config.STREAMING_TRIGGER_INTERVAL
        return (
            df.writeStream.format("parquet")
            .option("path", s3_path)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=trigger)
            .outputMode(spark_config.STREAMING_OUTPUT_MODE)
            .start()
        )

    def write_to_kafka(
        self,
        df: DataFrame,
        topic: str = kafka_config.PROCESSED_TOPIC,
        checkpoint_location: Optional[str] = None,
    ):
        """Forward enriched records to a downstream Kafka topic."""
        checkpoint = checkpoint_location or (
            spark_config.STREAMING_CHECKPOINT_LOCATION + "/processed-kafka"
        )
        value_df = df.select(
            F.col("drone_id").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        return (
            value_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", topic)
            .option("checkpointLocation", checkpoint)
            .outputMode(spark_config.STREAMING_OUTPUT_MODE)
            .start()
        )

    def start(
        self,
        s3_path: str = "s3a://drone-telemetry-data/processed/",
    ):
        """Convenience method: start the full processing pipeline."""
        stream_df = self.read_stream()
        return self.write_to_s3(stream_df, s3_path)
