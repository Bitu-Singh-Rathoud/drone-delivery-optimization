"""Unit tests for the Kafka DroneConsumer (mocked confluent_kafka)."""

from unittest.mock import MagicMock, patch

import pytest

from src.models.drone_telemetry import DroneTelemetry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_message(telemetry: DroneTelemetry, error=None):
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = telemetry.to_json().encode("utf-8")
    msg.key.return_value = telemetry.drone_id.encode("utf-8")
    return msg


def _sentinel_message():
    """A message with no error and no value — simulates a None poll result."""
    return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_consumer_cls():
    with patch("src.kafka.drone_consumer.Consumer") as mock_cls:
        yield mock_cls


@pytest.fixture()
def consumer(mock_consumer_cls):
    from src.kafka.drone_consumer import DroneConsumer
    return DroneConsumer(
        topics=["test-topic"],
        config={"bootstrap.servers": "localhost:9092", "group.id": "test-group", "auto.offset.reset": "earliest", "enable.auto.commit": False},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDroneConsumer:
    def test_subscribe_called_on_consume(self, consumer, mock_consumer_cls):
        telemetry = DroneTelemetry(
            drone_id="DRONE-001", latitude=37.77, longitude=-122.42,
            altitude=80.0, speed=10.0, heading=90.0, battery_level=75.0, status="en_route",
        )
        mock_instance = mock_consumer_cls.return_value
        mock_instance.poll.side_effect = [_make_message(telemetry), None, None]

        received = []
        consumer.consume(handler=received.append, max_messages=1)

        mock_instance.subscribe.assert_called_once_with(["test-topic"])

    def test_handler_called_with_deserialized_telemetry(self, consumer, mock_consumer_cls):
        telemetry = DroneTelemetry(
            drone_id="DRONE-002", latitude=37.77, longitude=-122.42,
            altitude=80.0, speed=10.0, heading=90.0, battery_level=60.0, status="hovering",
        )
        mock_instance = mock_consumer_cls.return_value
        mock_instance.poll.side_effect = [_make_message(telemetry), None]

        received = []
        consumer.consume(handler=received.append, max_messages=1)

        assert len(received) == 1
        assert received[0].drone_id == "DRONE-002"
        assert received[0].battery_level == 60.0

    def test_malformed_message_is_skipped(self, consumer, mock_consumer_cls):
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.value.return_value = b"not valid json"

        good_telemetry = DroneTelemetry(
            drone_id="DRONE-003", latitude=37.77, longitude=-122.42,
            altitude=80.0, speed=10.0, heading=90.0, battery_level=50.0, status="en_route",
        )
        mock_instance = mock_consumer_cls.return_value
        mock_instance.poll.side_effect = [bad_msg, _make_message(good_telemetry)]

        received = []
        consumer.consume(handler=received.append, max_messages=1)

        assert len(received) == 1
        assert received[0].drone_id == "DRONE-003"

    def test_commit_called_after_successful_message(self, consumer, mock_consumer_cls):
        telemetry = DroneTelemetry(
            drone_id="DRONE-004", latitude=37.77, longitude=-122.42,
            altitude=80.0, speed=10.0, heading=90.0, battery_level=80.0, status="en_route",
        )
        mock_instance = mock_consumer_cls.return_value
        msg = _make_message(telemetry)
        mock_instance.poll.side_effect = [msg]

        consumer.consume(handler=lambda t: None, max_messages=1)

        mock_instance.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_close_called_on_exit(self, consumer, mock_consumer_cls):
        mock_instance = mock_consumer_cls.return_value
        mock_instance.poll.return_value = None

        consumer.consume(handler=lambda t: None, max_messages=0)

        mock_instance.close.assert_called_once()
