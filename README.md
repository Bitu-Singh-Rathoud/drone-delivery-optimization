# Drone Delivery Optimization – Big Data Pipeline

Real-time drone delivery optimization using **Apache Kafka**, **Apache Spark Structured Streaming**, **AWS S3**, and **AWS Redshift** for route efficiency and collision avoidance.

---

## Architecture

```
Drone Fleet
    │  (JSON telemetry, 1 Hz per drone)
    ▼
┌─────────────────────────┐
│   Kafka Producer        │  src/kafka/drone_producer.py
│   Topic: drone-telemetry│
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│              Spark Structured Streaming                     │
│                                                             │
│  ┌─────────────────────┐   ┌───────────────────────────┐   │
│  │  TelemetryProcessor │   │    CollisionDetector       │   │
│  │  (schema, parse)    │──▶│  (self-join, safe radius)  │   │
│  └────────┬────────────┘   └─────────────┬─────────────┘   │
│           │                              │ alerts           │
│  ┌────────▼────────────┐                 ▼                  │
│  │   RouteOptimizer    │    Kafka topic: drone-alerts       │
│  │  (haversine, score) │                                    │
│  └────────┬────────────┘                                    │
└───────────┼─────────────────────────────────────────────────┘
            │ Parquet (partitioned by date)
            ▼
┌─────────────────────────┐
│       AWS S3            │  src/aws/s3_handler.py
│  s3://bucket/processed/ │
└──────────┬──────────────┘
           │  Redshift COPY
           ▼
┌─────────────────────────┐
│    AWS Redshift          │  src/aws/redshift_handler.py
│  drone_analytics DB      │
│  • drone_telemetry       │
│  • drone_routes          │
│  • collision_alerts      │
└─────────────────────────┘
```

---

## Project Structure

```
drone-delivery-optimization/
├── config/
│   ├── kafka_config.py        # Kafka broker / topic settings
│   ├── spark_config.py        # Spark / streaming settings
│   └── aws_config.py          # S3 & Redshift settings
├── src/
│   ├── models/
│   │   └── drone_telemetry.py # Telemetry dataclass + (de)serialisation
│   ├── kafka/
│   │   ├── drone_producer.py  # Telemetry simulator & Kafka producer
│   │   └── drone_consumer.py  # Kafka consumer with pluggable handler
│   ├── spark/
│   │   ├── telemetry_processor.py  # Structured Streaming read/write
│   │   ├── route_optimizer.py      # Haversine distance + action scoring
│   │   └── collision_detector.py   # Self-join proximity alerts
│   └── aws/
│       ├── s3_handler.py      # S3 upload / download / list helpers
│       └── redshift_handler.py# Redshift DDL, bulk inserts, COPY, queries
├── tests/
│   ├── test_drone_telemetry.py
│   ├── test_drone_producer.py
│   ├── test_drone_consumer.py
│   ├── test_route_optimizer.py
│   ├── test_collision_detector.py
│   ├── test_s3_handler.py
│   └── test_redshift_handler.py
├── scripts/
│   ├── run_pipeline.py        # Spark pipeline entrypoint
│   └── start_pipeline.sh      # Docker Compose helper
├── docker-compose.yml         # ZooKeeper, Kafka, Schema Registry, Kafka UI
├── Dockerfile
└── requirements.txt
```

---

## Quick Start

### Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.11 |
| Docker & Docker Compose | ≥ 24 |
| Java (JDK) | ≥ 11 (required by PySpark) |

### 1 – Install dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2 – Configure environment

Copy `.env.example` to `.env` and fill in your AWS credentials:

```bash
cp .env.example .env
```

Key variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `S3_BUCKET` | Target S3 bucket | `drone-telemetry-data` |
| `REDSHIFT_HOST` | Redshift cluster endpoint | _(required for Redshift)_ |
| `REDSHIFT_USER` / `REDSHIFT_PASSWORD` | Redshift credentials | _(required for Redshift)_ |
| `COLLISION_SAFE_DISTANCE_M` | Horizontal safety radius (m) | `50` |
| `COLLISION_SAFE_ALTITUDE_M` | Vertical safety clearance (m) | `10` |

### 3 – Start infrastructure

```bash
./scripts/start_pipeline.sh
```

This starts ZooKeeper, Kafka, Schema Registry, and the Kafka UI at http://localhost:8080.

### 4 – Run the producer (simulated drones)

```bash
python -m src.kafka.drone_producer
```

### 5 – Run the Spark pipeline

```bash
python -m scripts.run_pipeline
```

---

## Running Tests

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Tests that exercise Spark (route optimiser, collision detector) spin up a local `SparkSession` and do **not** require a running cluster.

---

## Key Components

### Telemetry Model (`src/models/drone_telemetry.py`)

A `@dataclass` capturing: `drone_id`, `latitude`, `longitude`, `altitude`, `speed`, `heading`, `battery_level`, `status`, `timestamp`, `destination_lat/lon`, `payload_weight`. Includes JSON serialisation helpers.

### Kafka Producer (`src/kafka/drone_producer.py`)

Simulates a configurable number of drones publishing telemetry at a configurable rate. Uses `confluent-kafka` with GZIP compression, idempotent delivery (`acks=all`), and automatic topic creation.

### Spark Telemetry Processor (`src/spark/telemetry_processor.py`)

Reads from the `drone-telemetry` Kafka topic, applies the telemetry schema, and writes Parquet files to S3 partitioned by date. Also forwards enriched records to `drone-processed`.

### Route Optimizer (`src/spark/route_optimizer.py`)

Adds four columns to the streaming DataFrame:
- `distance_to_dest_m` – great-circle distance to destination via the Haversine formula
- `estimated_flight_time_s` – ETA at current speed
- `recommended_action` – one of `continue`, `return_to_base`, `emergency_land`, `reduce_speed`, `optimal`
- `optimisation_score` – 0–100 efficiency score (battery × 0.5, speed × 0.3, payload × 0.2)

### Collision Detector (`src/spark/collision_detector.py`)

Performs a self-join on each micro-batch to find drone pairs whose **horizontal** separation is below `COLLISION_SAFE_DISTANCE_M` **and/or** whose **vertical** separation is below `COLLISION_SAFE_ALTITUDE_M`. Publishes `WARNING` / `CRITICAL` alerts to the `drone-alerts` Kafka topic.

### AWS S3 Handler (`src/aws/s3_handler.py`)

Provides `upload_json_records`, `upload_file`, `download_json_records`, `list_objects`, and `delete_object` helpers backed by `boto3`. Supports Hive-style date partitioning (`year=/month=/day=`).

### AWS Redshift Handler (`src/aws/redshift_handler.py`)

- Auto-creates `drone_telemetry`, `drone_routes`, and `collision_alerts` tables with `DISTKEY` / `SORTKEY` optimisations.
- Bulk inserts via `psycopg2` `execute_values`.
- `COPY … FROM S3` for high-throughput Parquet loads.
- Analytics helpers: `get_low_battery_drones`, `get_recent_alerts`.

---

## License

MIT
