"""Kafka producer that simulates real-time drone telemetry data."""

import json
import logging
import random
import time
from typing import Callable, List, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import kafka_config
from src.models.drone_telemetry import DroneTelemetry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: create topics if they do not exist
# ---------------------------------------------------------------------------

def ensure_topics(topics: List[str], num_partitions: int = 3, replication_factor: int = 1) -> None:
    """Create Kafka topics if they do not already exist."""
    admin = AdminClient({"bootstrap.servers": kafka_config.KAFKA_BOOTSTRAP_SERVERS})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor)
        for t in topics
        if t not in existing
    ]
    if new_topics:
        futures = admin.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Created topic: %s", topic)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("Topic %s may already exist: %s", topic, exc)


# ---------------------------------------------------------------------------
# Telemetry simulator
# ---------------------------------------------------------------------------

def _simulate_telemetry(drone_id: str, prev: Optional[DroneTelemetry] = None) -> DroneTelemetry:
    """Generate a plausible next telemetry reading for a drone."""
    if prev is None:
        return DroneTelemetry(
            drone_id=drone_id,
            latitude=random.uniform(37.7, 37.8),
            longitude=random.uniform(-122.5, -122.4),
            altitude=random.uniform(50.0, 120.0),
            speed=random.uniform(5.0, 15.0),
            heading=random.uniform(0.0, 360.0),
            battery_level=random.uniform(60.0, 100.0),
            status=random.choice(["en_route", "hovering"]),
            destination_lat=random.uniform(37.7, 37.8),
            destination_lon=random.uniform(-122.5, -122.4),
            payload_weight=random.uniform(0.1, 5.0),
        )

    # Drift existing values slightly
    lat = prev.latitude + random.uniform(-0.001, 0.001)
    lon = prev.longitude + random.uniform(-0.001, 0.001)
    alt = max(0.0, prev.altitude + random.uniform(-2.0, 2.0))
    speed = max(0.0, prev.speed + random.uniform(-1.0, 1.0))
    heading = (prev.heading + random.uniform(-5.0, 5.0)) % 360
    battery = max(0.0, prev.battery_level - random.uniform(0.1, 0.5))
    status = prev.status
    if battery < 20.0:
        status = "returning"
    elif battery < 5.0:
        status = "emergency"

    return DroneTelemetry(
        drone_id=drone_id,
        latitude=lat,
        longitude=lon,
        altitude=alt,
        speed=speed,
        heading=heading,
        battery_level=battery,
        status=status,
        destination_lat=prev.destination_lat,
        destination_lon=prev.destination_lon,
        payload_weight=prev.payload_weight,
    )


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Message delivery failed for %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered %s [partition %d] @ offset %d",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ---------------------------------------------------------------------------
# Producer class
# ---------------------------------------------------------------------------

class DroneProducer:
    """Publishes drone telemetry messages to a Kafka topic."""

    def __init__(
        self,
        config: Optional[dict] = None,
        topic: str = kafka_config.TELEMETRY_TOPIC,
        delivery_callback: Optional[Callable] = None,
    ) -> None:
        self._config = config or kafka_config.PRODUCER_CONFIG
        self._topic = topic
        self._delivery_callback = delivery_callback or _delivery_report
        self._producer = Producer(self._config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send(self, telemetry: DroneTelemetry) -> None:
        """Serialise *telemetry* and publish it to Kafka."""
        self._producer.produce(
            topic=self._topic,
            key=telemetry.drone_id.encode("utf-8"),
            value=telemetry.to_json().encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush outstanding messages. Returns remaining message count."""
        return self._producer.flush(timeout)

    def simulate(
        self,
        drone_ids: List[str],
        interval_seconds: float = 1.0,
        max_messages: Optional[int] = None,
    ) -> None:
        """
        Continuously publish simulated telemetry for *drone_ids*.

        Args:
            drone_ids: List of drone identifier strings.
            interval_seconds: Sleep duration between publishing rounds.
            max_messages: Stop after this many total messages (``None`` = run forever).
        """
        state: dict[str, DroneTelemetry] = {}
        count = 0
        try:
            while max_messages is None or count < max_messages:
                for drone_id in drone_ids:
                    telemetry = _simulate_telemetry(drone_id, state.get(drone_id))
                    state[drone_id] = telemetry
                    self.send(telemetry)
                    count += 1
                    logger.info("Published telemetry for %s (battery=%.1f%%)", drone_id, telemetry.battery_level)
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user.")
        finally:
            self.flush()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    drone_ids = [f"DRONE-{i:03d}" for i in range(1, 6)]
    ensure_topics([kafka_config.TELEMETRY_TOPIC, kafka_config.PROCESSED_TOPIC, kafka_config.ALERTS_TOPIC])
    producer = DroneProducer()
    producer.simulate(drone_ids, interval_seconds=1.0)
