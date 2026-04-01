#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# start_pipeline.sh
#
# Convenience wrapper to:
#  1. Start the Docker Compose services (Kafka, ZooKeeper, etc.)
#  2. Wait for Kafka to be ready
#  3. Create required Kafka topics
# ---------------------------------------------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load optional .env file
ENV_FILE="${ROOT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TELEMETRY_TOPIC="${KAFKA_TELEMETRY_TOPIC:-drone-telemetry}"
PROCESSED_TOPIC="${KAFKA_PROCESSED_TOPIC:-drone-processed}"
ALERTS_TOPIC="${KAFKA_ALERTS_TOPIC:-drone-alerts}"

echo "==> Starting Docker Compose services …"
docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d zookeeper kafka

echo "==> Waiting for Kafka to be ready …"
until docker compose -f "${ROOT_DIR}/docker-compose.yml" exec kafka \
  kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo "    Kafka not ready yet, retrying in 5 s …"
  sleep 5
done
echo "    Kafka is ready."

echo "==> Creating Kafka topics …"
for TOPIC in "$TELEMETRY_TOPIC" "$PROCESSED_TOPIC" "$ALERTS_TOPIC"; do
  docker compose -f "${ROOT_DIR}/docker-compose.yml" exec kafka \
    kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists \
      --topic "$TOPIC" \
      --partitions 3 \
      --replication-factor 1
  echo "    Topic '$TOPIC' ready."
done

echo "==> Starting all services …"
docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d

echo ""
echo "Pipeline services are running."
echo "  Kafka UI:       http://localhost:8080"
echo "  Schema Registry: http://localhost:8081"
echo ""
echo "To stop the pipeline:  docker compose down"
