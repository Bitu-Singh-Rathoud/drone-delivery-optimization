"""Unit tests for the Kafka DroneProducer (mocked confluent_kafka)."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from src.models.drone_telemetry import DroneTelemetry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_producer_cls():
    with patch("src.kafka.drone_producer.Producer") as mock_cls:
        yield mock_cls


@pytest.fixture()
def mock_admin_client():
    with patch("src.kafka.drone_producer.AdminClient") as mock_cls:
        topics_mock = MagicMock()
        topics_mock.topics = {}
        mock_cls.return_value.list_topics.return_value = topics_mock
        mock_cls.return_value.create_topics.return_value = {}
        yield mock_cls


@pytest.fixture()
def producer(mock_producer_cls):
    from src.kafka.drone_producer import DroneProducer
    return DroneProducer(config={"bootstrap.servers": "localhost:9092"}, topic="test-topic")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDroneProducer:
    def test_send_calls_produce(self, producer, mock_producer_cls):
        telemetry = DroneTelemetry(
            drone_id="DRONE-001",
            latitude=37.77,
            longitude=-122.42,
            altitude=80.0,
            speed=10.0,
            heading=90.0,
            battery_level=75.0,
            status="en_route",
        )
        producer.send(telemetry)
        mock_instance = mock_producer_cls.return_value
        mock_instance.produce.assert_called_once()
        call_kwargs = mock_instance.produce.call_args.kwargs
        assert call_kwargs["topic"] == "test-topic"
        assert call_kwargs["key"] == b"DRONE-001"
        payload = json.loads(call_kwargs["value"].decode("utf-8"))
        assert payload["drone_id"] == "DRONE-001"
        assert payload["battery_level"] == 75.0

    def test_flush_delegates_to_confluent(self, producer, mock_producer_cls):
        mock_producer_cls.return_value.flush.return_value = 0
        result = producer.flush(timeout=5.0)
        mock_producer_cls.return_value.flush.assert_called_once_with(5.0)
        assert result == 0

    def test_simulate_publishes_correct_number_of_messages(self, producer, mock_producer_cls):
        drone_ids = ["DRONE-001", "DRONE-002"]
        max_messages = 4  # 2 drones × 2 rounds
        producer.simulate(drone_ids=drone_ids, interval_seconds=0, max_messages=max_messages)
        mock_instance = mock_producer_cls.return_value
        assert mock_instance.produce.call_count == max_messages

    def test_simulate_uses_all_drone_ids(self, producer, mock_producer_cls):
        drone_ids = ["DRONE-A", "DRONE-B", "DRONE-C"]
        producer.simulate(drone_ids=drone_ids, interval_seconds=0, max_messages=3)
        produced_keys = {
            c.kwargs["key"] for c in mock_producer_cls.return_value.produce.call_args_list
        }
        assert produced_keys == {b"DRONE-A", b"DRONE-B", b"DRONE-C"}


class TestEnsureTopics:
    def test_creates_missing_topics(self, mock_admin_client):
        from src.kafka.drone_producer import ensure_topics
        ensure_topics(["new-topic"])
        mock_admin_client.return_value.create_topics.assert_called_once()

    def test_skips_existing_topics(self, mock_admin_client):
        mock_admin_client.return_value.list_topics.return_value.topics = {"existing": MagicMock()}
        from src.kafka.drone_producer import ensure_topics
        ensure_topics(["existing"])
        mock_admin_client.return_value.create_topics.assert_not_called()
