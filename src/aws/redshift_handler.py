"""AWS Redshift handler for drone analytics."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from config import aws_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

CREATE_TELEMETRY_TABLE = f"""
CREATE TABLE IF NOT EXISTS {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_TELEMETRY_TABLE} (
    id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    drone_id    VARCHAR(64)  NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    altitude    REAL,
    speed       REAL,
    heading     REAL,
    battery_level REAL,
    status      VARCHAR(32),
    event_time  TIMESTAMP,
    payload_weight REAL,
    ingested_at TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(drone_id)
SORTKEY(event_time);
"""

CREATE_ALERTS_TABLE = f"""
CREATE TABLE IF NOT EXISTS {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_ALERTS_TABLE} (
    id                    BIGINT IDENTITY(1,1) PRIMARY KEY,
    drone_a               VARCHAR(64) NOT NULL,
    drone_b               VARCHAR(64) NOT NULL,
    horizontal_distance_m REAL,
    vertical_distance_m   REAL,
    is_collision_risk     BOOLEAN,
    alert_level           VARCHAR(16),
    detected_at           TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(drone_a)
SORTKEY(detected_at);
"""

CREATE_ROUTES_TABLE = f"""
CREATE TABLE IF NOT EXISTS {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_ROUTES_TABLE} (
    id                       BIGINT IDENTITY(1,1) PRIMARY KEY,
    drone_id                 VARCHAR(64) NOT NULL,
    distance_to_dest_m       REAL,
    estimated_flight_time_s  REAL,
    recommended_action       VARCHAR(32),
    optimisation_score       REAL,
    recorded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(drone_id)
SORTKEY(recorded_at);
"""


# ---------------------------------------------------------------------------
# RedshiftHandler
# ---------------------------------------------------------------------------

class RedshiftHandler:
    """
    Manages a connection to Amazon Redshift and provides helpers for
    inserting drone telemetry, routes, and collision-alert data.
    """

    def __init__(
        self,
        host: str = aws_config.REDSHIFT_HOST,
        port: int = aws_config.REDSHIFT_PORT,
        database: str = aws_config.REDSHIFT_DATABASE,
        user: str = aws_config.REDSHIFT_USER,
        password: str = aws_config.REDSHIFT_PASSWORD,
    ) -> None:
        self._dsn: Dict[str, Any] = {
            "host": host,
            "port": port,
            "dbname": database,
            "user": user,
            "password": password,
        }
        self._conn: Optional[PgConnection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a connection to Redshift."""
        self._conn = psycopg2.connect(**self._dsn)
        logger.info("Connected to Redshift at %s:%s/%s", self._dsn["host"], self._dsn["port"], self._dsn["dbname"])

    def disconnect(self) -> None:
        """Close the Redshift connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Disconnected from Redshift.")

    @contextmanager
    def cursor(self) -> Generator:
        """Context manager that yields a cursor and commits on success."""
        if self._conn is None or self._conn.closed:
            self.connect()
        cur = self._conn.cursor()
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def initialise_schema(self) -> None:
        """Create tables if they do not already exist."""
        with self.cursor() as cur:
            for ddl in (CREATE_TELEMETRY_TABLE, CREATE_ALERTS_TABLE, CREATE_ROUTES_TABLE):
                cur.execute(ddl)
        logger.info("Redshift schema initialised.")

    # ------------------------------------------------------------------
    # Bulk inserts
    # ------------------------------------------------------------------

    def insert_telemetry_batch(self, records: List[dict]) -> int:
        """
        Insert a batch of telemetry dictionaries into the telemetry table.
        Returns the number of rows inserted.
        """
        if not records:
            return 0
        rows: List[Tuple] = [
            (
                r["drone_id"],
                r["latitude"],
                r["longitude"],
                r.get("altitude"),
                r.get("speed"),
                r.get("heading"),
                r.get("battery_level"),
                r.get("status"),
                r.get("event_time"),
                r.get("payload_weight"),
            )
            for r in records
        ]
        sql = f"""
            INSERT INTO {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_TELEMETRY_TABLE}
            (drone_id, latitude, longitude, altitude, speed, heading,
             battery_level, status, event_time, payload_weight)
            VALUES %s
        """
        with self.cursor() as cur:
            execute_values(cur, sql, rows)
        logger.info("Inserted %d telemetry rows into Redshift.", len(rows))
        return len(rows)

    def insert_alerts_batch(self, alerts: List[dict]) -> int:
        """Insert collision-alert records into the alerts table."""
        if not alerts:
            return 0
        rows = [
            (
                a["drone_a"],
                a["drone_b"],
                a.get("horizontal_distance_m"),
                a.get("vertical_distance_m"),
                a.get("is_collision_risk", False),
                a.get("alert_level", "WARNING"),
            )
            for a in alerts
        ]
        sql = f"""
            INSERT INTO {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_ALERTS_TABLE}
            (drone_a, drone_b, horizontal_distance_m, vertical_distance_m, is_collision_risk, alert_level)
            VALUES %s
        """
        with self.cursor() as cur:
            execute_values(cur, sql, rows)
        logger.info("Inserted %d alert rows into Redshift.", len(rows))
        return len(rows)

    def insert_routes_batch(self, routes: List[dict]) -> int:
        """Insert route-optimisation records."""
        if not routes:
            return 0
        rows = [
            (
                r["drone_id"],
                r.get("distance_to_dest_m"),
                r.get("estimated_flight_time_s"),
                r.get("recommended_action"),
                r.get("optimisation_score"),
            )
            for r in routes
        ]
        sql = f"""
            INSERT INTO {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_ROUTES_TABLE}
            (drone_id, distance_to_dest_m, estimated_flight_time_s, recommended_action, optimisation_score)
            VALUES %s
        """
        with self.cursor() as cur:
            execute_values(cur, sql, rows)
        logger.info("Inserted %d route rows into Redshift.", len(rows))
        return len(rows)

    # ------------------------------------------------------------------
    # Analytics queries
    # ------------------------------------------------------------------

    def get_low_battery_drones(self, threshold: float = 20.0) -> List[dict]:
        """Return drones whose latest battery reading is below *threshold*."""
        sql = f"""
            SELECT drone_id, battery_level, status, event_time
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY drone_id ORDER BY event_time DESC) AS rn
                FROM {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_TELEMETRY_TABLE}
            ) sub
            WHERE rn = 1 AND battery_level < %s
            ORDER BY battery_level ASC;
        """
        with self.cursor() as cur:
            cur.execute(sql, (threshold,))
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_recent_alerts(self, limit: int = 100) -> List[dict]:
        """Return the most recent *limit* collision alerts."""
        sql = f"""
            SELECT * FROM {aws_config.REDSHIFT_SCHEMA}.{aws_config.REDSHIFT_ALERTS_TABLE}
            ORDER BY detected_at DESC
            LIMIT %s;
        """
        with self.cursor() as cur:
            cur.execute(sql, (limit,))
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def copy_from_s3(self, s3_path: str, table: str, iam_role: str = aws_config.REDSHIFT_IAM_ROLE) -> None:
        """
        Use the Redshift COPY command to bulk-load Parquet data from S3.

        Args:
            s3_path: Full S3 URI, e.g. ``s3://bucket/prefix/``.
            table: Fully-qualified target table name.
            iam_role: IAM role ARN with read access to S3.
        """
        sql = f"""
            COPY {table}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
        """
        with self.cursor() as cur:
            cur.execute(sql)
        logger.info("COPY from %s into %s completed.", s3_path, table)
