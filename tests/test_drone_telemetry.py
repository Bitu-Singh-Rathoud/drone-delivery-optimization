"""Unit tests for DroneTelemetry data model."""

import json
import time

import pytest

from src.models.drone_telemetry import DroneTelemetry


@pytest.fixture()
def sample_telemetry() -> DroneTelemetry:
    return DroneTelemetry(
        drone_id="DRONE-001",
        latitude=37.7749,
        longitude=-122.4194,
        altitude=80.0,
        speed=10.0,
        heading=90.0,
        battery_level=75.0,
        status="en_route",
        destination_lat=37.7800,
        destination_lon=-122.4100,
        payload_weight=1.5,
    )


class TestDroneTelemetrySerialization:
    def test_to_dict_contains_all_fields(self, sample_telemetry):
        d = sample_telemetry.to_dict()
        assert d["drone_id"] == "DRONE-001"
        assert d["latitude"] == 37.7749
        assert d["battery_level"] == 75.0
        assert "timestamp" in d

    def test_to_json_is_valid_json(self, sample_telemetry):
        raw = sample_telemetry.to_json()
        parsed = json.loads(raw)
        assert parsed["drone_id"] == "DRONE-001"

    def test_from_dict_roundtrip(self, sample_telemetry):
        d = sample_telemetry.to_dict()
        restored = DroneTelemetry.from_dict(d)
        assert restored.drone_id == sample_telemetry.drone_id
        assert restored.latitude == sample_telemetry.latitude
        assert restored.status == sample_telemetry.status

    def test_from_json_roundtrip(self, sample_telemetry):
        restored = DroneTelemetry.from_json(sample_telemetry.to_json())
        assert restored.drone_id == sample_telemetry.drone_id
        assert restored.heading == sample_telemetry.heading


class TestDroneTelemetryHelpers:
    def test_low_battery_false_when_above_threshold(self, sample_telemetry):
        assert sample_telemetry.is_low_battery(threshold=20.0) is False

    def test_low_battery_true_when_below_threshold(self):
        t = DroneTelemetry(
            drone_id="X", latitude=0, longitude=0, altitude=0,
            speed=0, heading=0, battery_level=15.0, status="returning",
        )
        assert t.is_low_battery(threshold=20.0) is True

    def test_is_emergency_true(self):
        t = DroneTelemetry(
            drone_id="X", latitude=0, longitude=0, altitude=0,
            speed=0, heading=0, battery_level=3.0, status="emergency",
        )
        assert t.is_emergency() is True

    def test_is_emergency_false(self, sample_telemetry):
        assert sample_telemetry.is_emergency() is False

    def test_default_timestamp_is_recent(self, sample_telemetry):
        assert sample_telemetry.timestamp == pytest.approx(time.time(), abs=5.0)

    def test_optional_destination_none_by_default(self):
        t = DroneTelemetry(
            drone_id="X", latitude=0, longitude=0, altitude=0,
            speed=0, heading=0, battery_level=50.0, status="hovering",
        )
        assert t.destination_lat is None
        assert t.destination_lon is None
