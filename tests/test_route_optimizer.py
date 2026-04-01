"""Unit tests for the Spark route optimiser (no live Spark cluster required)."""

import math

import pytest

from src.spark.route_optimizer import RouteOptimizer, _haversine_distance, _recommend_action


class TestHaversineDistance:
    def test_same_point_is_zero(self):
        assert _haversine_distance(37.77, -122.42, 37.77, -122.42) == pytest.approx(0.0, abs=1e-3)

    def test_known_distance(self):
        # San Francisco -> Oakland (~12 km across the bay)
        dist = _haversine_distance(37.7749, -122.4194, 37.8044, -122.2711)
        assert 11_000 < dist < 14_000

    def test_none_coordinates_returns_negative(self):
        assert _haversine_distance(None, -122.42, 37.77, -122.42) == -1.0

    def test_symmetry(self):
        d1 = _haversine_distance(37.77, -122.42, 37.80, -122.40)
        d2 = _haversine_distance(37.80, -122.40, 37.77, -122.42)
        assert d1 == pytest.approx(d2, rel=1e-6)


class TestRecommendAction:
    def test_emergency_land_on_critical_battery(self):
        assert _recommend_action(3.0, 1000.0, "en_route", 1.0) == "emergency_land"

    def test_emergency_land_on_emergency_status(self):
        assert _recommend_action(50.0, 1000.0, "emergency", 1.0) == "emergency_land"

    def test_return_to_base_on_low_battery(self):
        assert _recommend_action(15.0, 5000.0, "en_route", 1.0) == "return_to_base"

    def test_reduce_speed_on_heavy_payload(self):
        # 10 kg > MAX_PAYLOAD_WEIGHT_KG (5 kg default)
        result = _recommend_action(80.0, 500.0, "en_route", 10.0)
        assert result == "reduce_speed"

    def test_continue_when_normal(self):
        assert _recommend_action(80.0, 500.0, "en_route", 1.0) == "continue"

    def test_optimal_when_distance_zero(self):
        assert _recommend_action(80.0, 0.0, "hovering", 0.0) == "optimal"

    def test_unknown_on_none_inputs(self):
        assert _recommend_action(None, 0.0, None, 0.0) == "unknown"


class TestRouteOptimizerWithSpark:
    """Integration-style tests using a local SparkSession."""

    @pytest.fixture(scope="class")
    def spark(self):
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder.master("local[1]")
            .appName("test-route-optimizer")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()

    @pytest.fixture()
    def sample_df(self, spark):
        data = [
            ("DRONE-001", 37.7749, -122.4194, 80.0, 10.0, 90.0, 75.0, "en_route", 1.5, 37.780, -122.410),
            ("DRONE-002", 37.7800, -122.4100, 60.0, 5.0, 180.0, 15.0, "returning", 1.0, 37.790, -122.420),
            ("DRONE-003", 37.7760, -122.4180, 70.0, 0.0, 0.0, 4.0, "emergency", 0.0, 37.780, -122.415),
        ]
        cols = ["drone_id", "latitude", "longitude", "altitude", "speed", "heading",
                "battery_level", "status", "payload_weight", "destination_lat", "destination_lon"]
        return spark.createDataFrame(data, cols)

    def test_optimise_adds_required_columns(self, sample_df):
        optimizer = RouteOptimizer()
        result = optimizer.optimise(sample_df)
        expected_cols = {"distance_to_dest_m", "estimated_flight_time_s",
                         "recommended_action", "optimisation_score"}
        assert expected_cols.issubset(set(result.columns))

    def test_distance_positive_for_valid_destinations(self, sample_df):
        optimizer = RouteOptimizer()
        result = optimizer.optimise(sample_df)
        rows = {r["drone_id"]: r["distance_to_dest_m"] for r in result.collect()}
        assert rows["DRONE-001"] > 0
        assert rows["DRONE-002"] > 0

    def test_emergency_drone_gets_emergency_land_action(self, sample_df):
        optimizer = RouteOptimizer()
        result = optimizer.optimise(sample_df)
        rows = {r["drone_id"]: r["recommended_action"] for r in result.collect()}
        assert rows["DRONE-003"] == "emergency_land"

    def test_low_battery_drone_gets_return_action(self, sample_df):
        optimizer = RouteOptimizer()
        result = optimizer.optimise(sample_df)
        rows = {r["drone_id"]: r["recommended_action"] for r in result.collect()}
        assert rows["DRONE-002"] == "return_to_base"

    def test_optimisation_score_in_range(self, sample_df):
        optimizer = RouteOptimizer()
        result = optimizer.optimise(sample_df)
        for row in result.collect():
            assert 0.0 <= row["optimisation_score"] <= 100.0
