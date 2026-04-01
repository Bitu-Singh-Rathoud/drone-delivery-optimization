"""Kafka consumer that reads processed drone telemetry messages."""

import logging
from typing import Callable, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from config import kafka_config
from src.models.drone_telemetry import DroneTelemetry

logger = logging.getLogger(__name__)


class DroneConsumer:
    """
    Consumes drone telemetry messages from one or more Kafka topics.

    Usage::

        consumer = DroneConsumer(topics=[kafka_config.TELEMETRY_TOPIC])
        consumer.consume(handler=my_handler)
    """

    def __init__(
        self,
        topics: Optional[List[str]] = None,
        config: Optional[dict] = None,
    ) -> None:
        self._topics = topics or [kafka_config.TELEMETRY_TOPIC]
        self._config = config or kafka_config.CONSUMER_CONFIG
        self._consumer = Consumer(self._config)
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume(
        self,
        handler: Callable[[DroneTelemetry], None],
        poll_timeout: float = 1.0,
        max_messages: Optional[int] = None,
    ) -> None:
        """
        Start consuming messages and invoke *handler* for each valid telemetry record.

        Args:
            handler: Callable that receives a :class:`DroneTelemetry` instance.
            poll_timeout: Seconds to wait in each ``poll()`` call.
            max_messages: Stop after processing this many messages (``None`` = run forever).
        """
        self._consumer.subscribe(self._topics)
        self._running = True
        count = 0
        try:
            while self._running and (max_messages is None or count < max_messages):
                msg: Optional[Message] = self._consumer.poll(poll_timeout)
                if msg is None:
                    continue
                if msg.error():
                    self._handle_error(msg)
                    continue
                telemetry = self._deserialise(msg)
                if telemetry is not None:
                    handler(telemetry)
                    self._consumer.commit(message=msg, asynchronous=False)
                    count += 1
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")
        finally:
            self._consumer.close()

    def stop(self) -> None:
        """Signal the consume loop to stop."""
        self._running = False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _handle_error(msg: Message) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            logger.debug(
                "End of partition reached: %s [%d] @ %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )
        else:
            raise KafkaException(err)

    @staticmethod
    def _deserialise(msg: Message) -> Optional[DroneTelemetry]:
        try:
            return DroneTelemetry.from_json(msg.value().decode("utf-8"))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Failed to deserialise message: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def _print_handler(telemetry: DroneTelemetry) -> None:
        logger.info(
            "[%s] alt=%.1fm battery=%.1f%% status=%s",
            telemetry.drone_id,
            telemetry.altitude,
            telemetry.battery_level,
            telemetry.status,
        )

    consumer = DroneConsumer(topics=[kafka_config.TELEMETRY_TOPIC])
    consumer.consume(handler=_print_handler)
