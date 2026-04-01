"""
Collision detector for drone swarms using Spark.

For every micro-batch of telemetry, performs a self-join on the
DataFrame to find pairs of drones whose horizontal (and vertical)
separation falls below configurable safety thresholds.
"""

import logging
import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType, StringType, StructField, StructType

from config import kafka_config, spark_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# UDF helpers
# ---------------------------------------------------------------------------

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return horizontal distance (metres) between two coordinate pairs."""
    if None in (lat1, lon1, lat2, lon2):
        return float("inf")
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


_haversine_udf = F.udf(_haversine, FloatType())

# Alert schema (used when writing to Kafka)
ALERT_SCHEMA = StructType(
    [
        StructField("drone_a", StringType(), False),
        StructField("drone_b", StringType(), False),
        StructField("horizontal_distance_m", FloatType(), True),
        StructField("vertical_distance_m", FloatType(), True),
        StructField("is_collision_risk", BooleanType(), False),
        StructField("alert_level", StringType(), False),
    ]
)


# ---------------------------------------------------------------------------
# CollisionDetector
# ---------------------------------------------------------------------------

class CollisionDetector:
    """
    Detects potential collisions between drones in a telemetry DataFrame.

    Two drones are flagged when:
    - Horizontal separation < ``safe_distance_m``
    - Vertical separation   < ``safe_altitude_m``

    Alert levels:
    - ``WARNING``: one threshold breached.
    - ``CRITICAL``: both thresholds breached simultaneously.
    """

    def __init__(
        self,
        safe_distance_m: float = spark_config.COLLISION_SAFE_DISTANCE_M,
        safe_altitude_m: float = spark_config.COLLISION_SAFE_ALTITUDE_M,
    ) -> None:
        self._safe_distance_m = safe_distance_m
        self._safe_altitude_m = safe_altitude_m

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(self, df: DataFrame) -> DataFrame:
        """
        Return a DataFrame of collision-risk pairs from a *batch* DataFrame.

        Each row describes a pair (drone_a, drone_b) with separation metrics.
        Only pairs where drone_a < drone_b (lexicographic) are emitted to
        avoid duplicates.
        """
        a = df.alias("a")
        b = df.alias("b")

        pairs = a.join(b, F.col("a.drone_id") < F.col("b.drone_id"))

        pairs = pairs.withColumn(
            "horizontal_distance_m",
            _haversine_udf(
                F.col("a.latitude"),
                F.col("a.longitude"),
                F.col("b.latitude"),
                F.col("b.longitude"),
            ),
        ).withColumn(
            "vertical_distance_m",
            F.abs(F.col("a.altitude") - F.col("b.altitude")).cast(FloatType()),
        )

        too_close_h = F.col("horizontal_distance_m") < self._safe_distance_m
        too_close_v = F.col("vertical_distance_m") < self._safe_altitude_m

        pairs = pairs.withColumn(
            "is_collision_risk",
            too_close_h & too_close_v,
        ).withColumn(
            "alert_level",
            F.when(too_close_h & too_close_v, F.lit("CRITICAL"))
            .when(too_close_h | too_close_v, F.lit("WARNING"))
            .otherwise(F.lit("OK")),
        )

        # Only raise an alert when drones are horizontally close.
        # Vertical proximity alone does not indicate a collision risk.
        alerts = pairs.filter(too_close_h).select(
            F.col("a.drone_id").alias("drone_a"),
            F.col("b.drone_id").alias("drone_b"),
            "horizontal_distance_m",
            "vertical_distance_m",
            "is_collision_risk",
            "alert_level",
        )
        return alerts

    def detect_stream(
        self,
        df: DataFrame,
        checkpoint_location: str = spark_config.STREAMING_CHECKPOINT_LOCATION + "/alerts",
        kafka_topic: str = kafka_config.ALERTS_TOPIC,
    ):
        """
        Apply collision detection to a *streaming* DataFrame using foreachBatch.

        Detected alerts are published to the Kafka alerts topic.
        """

        def _process_batch(batch_df: DataFrame, _epoch_id: int) -> None:
            if batch_df.rdd.isEmpty():
                return
            alerts = self.detect(batch_df)
            if alerts.rdd.isEmpty():
                return
            alert_count = alerts.count()
            logger.warning("Epoch %d: %d collision alert(s) detected.", _epoch_id, alert_count)

            # Write to Kafka alerts topic
            alerts.select(
                F.col("drone_a").alias("key"),
                F.to_json(F.struct("*")).alias("value"),
            ).write.format("kafka").option(
                "kafka.bootstrap.servers", kafka_config.KAFKA_BOOTSTRAP_SERVERS
            ).option(
                "topic", kafka_topic
            ).save()

        return (
            df.writeStream.foreachBatch(_process_batch)
            .option("checkpointLocation", checkpoint_location)
            .outputMode("update")
            .start()
        )
