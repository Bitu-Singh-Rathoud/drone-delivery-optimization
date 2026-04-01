"""Spark configuration."""

import os

# ------------------------------------------------------------------
# Application
# ------------------------------------------------------------------

SPARK_APP_NAME: str = os.environ.get("SPARK_APP_NAME", "DroneDeliveryPipeline")
SPARK_MASTER: str = os.environ.get("SPARK_MASTER", "local[*]")

# ------------------------------------------------------------------
# Streaming
# ------------------------------------------------------------------

STREAMING_TRIGGER_INTERVAL: str = os.environ.get(
    "STREAMING_TRIGGER_INTERVAL", "10 seconds"
)
STREAMING_CHECKPOINT_LOCATION: str = os.environ.get(
    "STREAMING_CHECKPOINT_LOCATION", "/tmp/drone-pipeline-checkpoints"
)
STREAMING_OUTPUT_MODE: str = os.environ.get("STREAMING_OUTPUT_MODE", "append")

# ------------------------------------------------------------------
# Collision detection
# ------------------------------------------------------------------

# Minimum safe horizontal separation in metres
COLLISION_SAFE_DISTANCE_M: float = float(
    os.environ.get("COLLISION_SAFE_DISTANCE_M", "50.0")
)
# Minimum safe vertical separation in metres
COLLISION_SAFE_ALTITUDE_M: float = float(
    os.environ.get("COLLISION_SAFE_ALTITUDE_M", "10.0")
)

# ------------------------------------------------------------------
# Route optimisation
# ------------------------------------------------------------------

LOW_BATTERY_THRESHOLD: float = float(
    os.environ.get("LOW_BATTERY_THRESHOLD", "20.0")
)
MAX_PAYLOAD_WEIGHT_KG: float = float(
    os.environ.get("MAX_PAYLOAD_WEIGHT_KG", "5.0")
)

# ------------------------------------------------------------------
# Spark / Kafka integration packages
# ------------------------------------------------------------------

SPARK_PACKAGES: str = os.environ.get(
    "SPARK_PACKAGES",
    (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ),
)
