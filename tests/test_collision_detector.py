"""Unit tests for the Spark CollisionDetector."""

import pytest


class TestCollisionDetectorWithSpark:
    """Integration-style tests using a local SparkSession."""

    @pytest.fixture(scope="class")
    def spark(self):
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder.master("local[1]")
            .appName("test-collision-detector")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()

    def _make_df(self, spark, rows):
        cols = ["drone_id", "latitude", "longitude", "altitude"]
        return spark.createDataFrame(rows, cols)

    def test_no_alerts_when_drones_far_apart(self, spark):
        from src.spark.collision_detector import CollisionDetector
        # Drones ~12 km apart
        rows = [
            ("DRONE-A", 37.7749, -122.4194, 100.0),
            ("DRONE-B", 37.8044, -122.2711, 100.0),
        ]
        df = self._make_df(spark, rows)
        detector = CollisionDetector(safe_distance_m=50.0, safe_altitude_m=10.0)
        alerts = detector.detect(df)
        assert alerts.count() == 0

    def test_critical_alert_when_very_close(self, spark):
        from src.spark.collision_detector import CollisionDetector
        # Drones at nearly identical position and altitude
        rows = [
            ("DRONE-A", 37.7749, -122.4194, 80.0),
            ("DRONE-B", 37.7749, -122.4195, 80.5),  # ~8 m away, 0.5 m altitude diff
        ]
        df = self._make_df(spark, rows)
        detector = CollisionDetector(safe_distance_m=50.0, safe_altitude_m=10.0)
        alerts = detector.detect(df)
        rows_collected = alerts.collect()
        assert len(rows_collected) == 1
        assert rows_collected[0]["alert_level"] == "CRITICAL"
        assert rows_collected[0]["is_collision_risk"] is True

    def test_no_duplicate_pairs(self, spark):
        from src.spark.collision_detector import CollisionDetector
        rows = [
            ("DRONE-A", 37.7749, -122.4194, 80.0),
            ("DRONE-B", 37.7749, -122.4195, 80.0),
            ("DRONE-C", 37.7749, -122.4196, 80.0),
        ]
        df = self._make_df(spark, rows)
        detector = CollisionDetector(safe_distance_m=50.0, safe_altitude_m=10.0)
        alerts = detector.detect(df)
        # Should have C(3,2)=3 pairs, all critical
        assert alerts.count() == 3
        for row in alerts.collect():
            assert row["drone_a"] < row["drone_b"]  # no duplicates

    def test_warning_alert_when_only_horizontal_close(self, spark):
        from src.spark.collision_detector import CollisionDetector
        rows = [
            ("DRONE-A", 37.7749, -122.4194, 80.0),
            ("DRONE-B", 37.7749, -122.4195, 200.0),  # close horizontally, far vertically
        ]
        df = self._make_df(spark, rows)
        detector = CollisionDetector(safe_distance_m=50.0, safe_altitude_m=10.0)
        alerts = detector.detect(df)
        rows_collected = alerts.collect()
        assert len(rows_collected) == 1
        assert rows_collected[0]["alert_level"] == "WARNING"
        assert rows_collected[0]["is_collision_risk"] is False

    def test_single_drone_no_alerts(self, spark):
        from src.spark.collision_detector import CollisionDetector
        rows = [("DRONE-A", 37.7749, -122.4194, 80.0)]
        df = self._make_df(spark, rows)
        detector = CollisionDetector()
        alerts = detector.detect(df)
        assert alerts.count() == 0
