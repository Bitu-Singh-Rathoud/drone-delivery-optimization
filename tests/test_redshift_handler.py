"""Unit tests for RedshiftHandler (mocked psycopg2)."""

from unittest.mock import MagicMock, call, patch

import pytest

from src.aws.redshift_handler import RedshiftHandler


@pytest.fixture()
def mock_psycopg2():
    with patch("src.aws.redshift_handler.psycopg2") as mock_pg:
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_pg.connect.return_value = mock_conn
        yield mock_pg, mock_conn


@pytest.fixture()
def handler(mock_psycopg2):
    _, _ = mock_psycopg2
    return RedshiftHandler(
        host="localhost",
        port=5439,
        database="drone_analytics",
        user="admin",
        password="secret",
    )


class TestRedshiftHandlerConnection:
    def test_connect_calls_psycopg2_connect(self, handler, mock_psycopg2):
        mock_pg, mock_conn = mock_psycopg2
        handler.connect()
        mock_pg.connect.assert_called_once()
        call_kwargs = mock_pg.connect.call_args.kwargs
        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["dbname"] == "drone_analytics"

    def test_disconnect_closes_connection(self, handler, mock_psycopg2):
        mock_pg, mock_conn = mock_psycopg2
        handler.connect()
        handler.disconnect()
        mock_conn.close.assert_called_once()


class TestRedshiftHandlerInserts:
    def _setup_cursor(self, mock_psycopg2):
        mock_pg, mock_conn = mock_psycopg2
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        return mock_cursor

    def test_insert_telemetry_batch_returns_count(self, handler, mock_psycopg2):
        with patch("src.aws.redshift_handler.execute_values") as mock_ev:
            self._setup_cursor(mock_psycopg2)
            records = [
                {
                    "drone_id": "DRONE-001",
                    "latitude": 37.77,
                    "longitude": -122.42,
                    "altitude": 80.0,
                    "speed": 10.0,
                    "heading": 90.0,
                    "battery_level": 75.0,
                    "status": "en_route",
                    "event_time": None,
                    "payload_weight": 1.5,
                }
            ]
            count = handler.insert_telemetry_batch(records)
            assert count == 1
            mock_ev.assert_called_once()

    def test_insert_telemetry_batch_empty_returns_zero(self, handler, mock_psycopg2):
        count = handler.insert_telemetry_batch([])
        assert count == 0

    def test_insert_alerts_batch_returns_count(self, handler, mock_psycopg2):
        with patch("src.aws.redshift_handler.execute_values") as mock_ev:
            self._setup_cursor(mock_psycopg2)
            alerts = [
                {
                    "drone_a": "DRONE-001",
                    "drone_b": "DRONE-002",
                    "horizontal_distance_m": 30.0,
                    "vertical_distance_m": 5.0,
                    "is_collision_risk": True,
                    "alert_level": "CRITICAL",
                }
            ]
            count = handler.insert_alerts_batch(alerts)
            assert count == 1

    def test_insert_routes_batch_empty_returns_zero(self, handler, mock_psycopg2):
        count = handler.insert_routes_batch([])
        assert count == 0

    def test_initialise_schema_executes_ddl(self, handler, mock_psycopg2):
        mock_cursor = self._setup_cursor(mock_psycopg2)
        handler.initialise_schema()
        # Three CREATE TABLE statements
        assert mock_cursor.execute.call_count == 3


class TestRedshiftHandlerQueries:
    def test_get_low_battery_drones(self, handler, mock_psycopg2):
        mock_pg, mock_conn = mock_psycopg2
        mock_cursor = MagicMock()
        mock_cursor.description = [("drone_id",), ("battery_level",), ("status",), ("event_time",)]
        mock_cursor.fetchall.return_value = [("DRONE-005", 12.0, "returning", None)]
        mock_conn.cursor.return_value = mock_cursor
        results = handler.get_low_battery_drones(threshold=20.0)
        assert len(results) == 1
        assert results[0]["drone_id"] == "DRONE-005"
        assert results[0]["battery_level"] == 12.0
