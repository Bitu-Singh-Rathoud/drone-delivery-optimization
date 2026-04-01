"""
Pipeline entrypoint.

Starts Spark Structured Streaming to:
  1. Read raw telemetry from Kafka
  2. Optimise routes
  3. Detect collisions and publish alerts to Kafka
  4. Persist enriched records to S3 (Parquet)
"""

import logging
import sys

from config import aws_config, kafka_config, spark_config
from src.spark.collision_detector import CollisionDetector
from src.spark.route_optimizer import RouteOptimizer
from src.spark.telemetry_processor import TelemetryProcessor, create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting Drone Delivery Pipeline …")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    processor = TelemetryProcessor(spark)
    optimizer = RouteOptimizer()
    detector = CollisionDetector()

    # Step 1: parse raw telemetry stream
    telemetry_stream = processor.read_stream()

    # Step 2: add route-optimisation columns
    optimised_stream = optimizer.optimise_stream(telemetry_stream)

    # Step 3: write enriched data to S3 (Parquet)
    s3_path = f"s3a://{aws_config.S3_BUCKET}/{aws_config.S3_PROCESSED_PREFIX}"
    s3_query = processor.write_to_s3(
        optimised_stream,
        s3_path=s3_path,
        checkpoint_location=spark_config.STREAMING_CHECKPOINT_LOCATION + "/s3-sink",
    )

    # Step 4: forward to processed Kafka topic
    kafka_query = processor.write_to_kafka(
        optimised_stream,
        topic=kafka_config.PROCESSED_TOPIC,
        checkpoint_location=spark_config.STREAMING_CHECKPOINT_LOCATION + "/kafka-sink",
    )

    # Step 5: collision detection (foreachBatch on raw stream)
    alert_query = detector.detect_stream(
        telemetry_stream,
        checkpoint_location=spark_config.STREAMING_CHECKPOINT_LOCATION + "/alerts",
        kafka_topic=kafka_config.ALERTS_TOPIC,
    )

    logger.info("All streaming queries started.")
    logger.info("  S3 sink:      %s", s3_query.id)
    logger.info("  Kafka sink:   %s", kafka_query.id)
    logger.info("  Alert query:  %s", alert_query.id)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user.")
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        logger.info("Pipeline stopped.")


if __name__ == "__main__":
    main()
