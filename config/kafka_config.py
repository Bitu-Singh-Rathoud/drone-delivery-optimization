"""Kafka configuration."""

import os

# ------------------------------------------------------------------
# Broker connection
# ------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# ------------------------------------------------------------------
# Topics
# ------------------------------------------------------------------

TELEMETRY_TOPIC: str = os.environ.get("KAFKA_TELEMETRY_TOPIC", "drone-telemetry")
PROCESSED_TOPIC: str = os.environ.get("KAFKA_PROCESSED_TOPIC", "drone-processed")
ALERTS_TOPIC: str = os.environ.get("KAFKA_ALERTS_TOPIC", "drone-alerts")

# ------------------------------------------------------------------
# Producer defaults
# ------------------------------------------------------------------

PRODUCER_CONFIG: dict = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": os.environ.get("KAFKA_CLIENT_ID", "drone-producer"),
    "acks": "all",
    "retries": 3,
    "batch.size": 16384,
    "linger.ms": 5,
    "compression.type": "gzip",
}

# ------------------------------------------------------------------
# Consumer defaults
# ------------------------------------------------------------------

CONSUMER_CONFIG: dict = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": os.environ.get("KAFKA_CONSUMER_GROUP", "drone-pipeline"),
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "session.timeout.ms": 30000,
    "max.poll.interval.ms": 300000,
}
