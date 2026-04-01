"""
Spark-based route optimiser for drone delivery.

Analyses a streaming (or batch) DataFrame of drone telemetry and
recommends route adjustments based on battery level, payload weight,
distance to destination, and current heading.
"""

import logging
import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType

from config import spark_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pure-Python helper (used as a UDF)
# ---------------------------------------------------------------------------

def _haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance (metres) between two WGS-84 coordinates."""
    if None in (lat1, lon1, lat2, lon2):
        return -1.0
    R = 6_371_000.0  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _recommend_action(
    battery_level: float,
    distance_to_dest: float,
    status: str,
    payload_weight: float,
) -> str:
    """
    Derive a recommended action string from current drone state.

    Returns one of: 'continue', 'return_to_base', 'emergency_land',
                    'reduce_speed', 'optimal'.
    """
    if battery_level is None or status is None:
        return "unknown"
    if battery_level < 5.0 or status == "emergency":
        return "emergency_land"
    if battery_level < spark_config.LOW_BATTERY_THRESHOLD:
        # Check whether we can still reach the destination
        if distance_to_dest > 0 and battery_level / 100.0 * 8000 < distance_to_dest:
            return "return_to_base"
        return "return_to_base"
    if payload_weight is not None and payload_weight > spark_config.MAX_PAYLOAD_WEIGHT_KG:
        return "reduce_speed"
    if distance_to_dest > 0:
        return "continue"
    return "optimal"


# ---------------------------------------------------------------------------
# UDF registrations
# ---------------------------------------------------------------------------

_haversine_udf = F.udf(_haversine_distance, FloatType())
_action_udf = F.udf(_recommend_action, StringType())


# ---------------------------------------------------------------------------
# RouteOptimizer
# ---------------------------------------------------------------------------

class RouteOptimizer:
    """
    Adds route-optimisation columns to a telemetry DataFrame.

    Columns added:
    - ``distance_to_dest_m``: metres remaining to destination.
    - ``estimated_flight_time_s``: seconds to destination at current speed.
    - ``recommended_action``: suggested action string.
    - ``optimisation_score``: 0–100 efficiency score (higher is better).
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def optimise(self, df: DataFrame) -> DataFrame:
        """Return *df* enriched with route-optimisation columns."""
        df = self._add_distance(df)
        df = self._add_flight_time(df)
        df = self._add_recommended_action(df)
        df = self._add_score(df)
        return df

    def optimise_stream(self, df: DataFrame) -> DataFrame:
        """Same as :meth:`optimise` but intended for streaming DataFrames."""
        return self.optimise(df)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _add_distance(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "distance_to_dest_m",
            _haversine_udf(
                F.col("latitude"),
                F.col("longitude"),
                F.col("destination_lat"),
                F.col("destination_lon"),
            ),
        )

    @staticmethod
    def _add_flight_time(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "estimated_flight_time_s",
            F.when(
                (F.col("speed") > 0) & (F.col("distance_to_dest_m") >= 0),
                F.col("distance_to_dest_m") / F.col("speed"),
            ).otherwise(F.lit(-1.0).cast(FloatType())),
        )

    @staticmethod
    def _add_recommended_action(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "recommended_action",
            _action_udf(
                F.col("battery_level"),
                F.col("distance_to_dest_m"),
                F.col("status"),
                F.col("payload_weight"),
            ),
        )

    @staticmethod
    def _add_score(df: DataFrame) -> DataFrame:
        """
        Compute a simple 0–100 efficiency score:
            score = 0.5 * battery_level + 0.3 * speed_factor + 0.2 * payload_factor
        where speed_factor and payload_factor are normalised 0–1 values.
        """
        max_speed = 20.0  # m/s
        speed_factor = F.least(F.col("speed") / max_speed, F.lit(1.0))
        payload_factor = F.lit(1.0) - (
            F.col("payload_weight") / spark_config.MAX_PAYLOAD_WEIGHT_KG
        ).cast(FloatType())
        score = (
            F.lit(0.5) * F.col("battery_level")
            + F.lit(0.3) * speed_factor * F.lit(100.0)
            + F.lit(0.2) * F.greatest(payload_factor, F.lit(0.0)) * F.lit(100.0)
        )
        return df.withColumn("optimisation_score", score.cast(FloatType()))
