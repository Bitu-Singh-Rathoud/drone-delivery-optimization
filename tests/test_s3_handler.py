"""Unit tests for S3Handler (mocked boto3)."""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from src.aws.s3_handler import S3Handler


@pytest.fixture()
def mock_boto3_client():
    with patch("src.aws.s3_handler.boto3") as mock_boto3:
        yield mock_boto3


@pytest.fixture()
def handler(mock_boto3_client):
    return S3Handler(bucket="test-bucket", region="us-east-1")


class TestS3HandlerUpload:
    def test_upload_json_records_calls_put_object(self, handler, mock_boto3_client):
        records = [{"drone_id": "DRONE-001", "battery_level": 75.0}]
        key = handler.upload_json_records(records, prefix="raw/", partition_by_date=False)
        mock_client = mock_boto3_client.client.return_value
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "test-bucket"
        assert "raw/" in call_kwargs["Key"]
        body_lines = call_kwargs["Body"].decode("utf-8").strip().split("\n")
        assert len(body_lines) == 1
        assert json.loads(body_lines[0])["drone_id"] == "DRONE-001"

    def test_upload_json_records_returns_key(self, handler, mock_boto3_client):
        key = handler.upload_json_records([{"x": 1}], prefix="raw/", partition_by_date=False)
        assert key.startswith("raw/")

    def test_upload_file_calls_upload_file(self, handler, mock_boto3_client):
        handler.upload_file("/tmp/test.parquet", "processed/test.parquet")
        mock_client = mock_boto3_client.client.return_value
        mock_client.upload_file.assert_called_once_with(
            "/tmp/test.parquet", "test-bucket", "processed/test.parquet"
        )

    def test_partition_by_date_adds_year_month_day(self, handler, mock_boto3_client):
        handler.upload_json_records([{"a": 1}], prefix="raw/", partition_by_date=True)
        mock_client = mock_boto3_client.client.return_value
        key = mock_client.put_object.call_args.kwargs["Key"]
        assert "year=" in key
        assert "month=" in key
        assert "day=" in key


class TestS3HandlerDownload:
    def test_download_returns_list_of_dicts(self, handler, mock_boto3_client):
        body_content = '{"drone_id": "DRONE-001"}\n{"drone_id": "DRONE-002"}'
        mock_client = mock_boto3_client.client.return_value
        mock_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=body_content.encode("utf-8")))
        }
        result = handler.download_json_records("raw/data.ndjson")
        assert len(result) == 2
        assert result[0]["drone_id"] == "DRONE-001"
        assert result[1]["drone_id"] == "DRONE-002"


class TestS3HandlerListDelete:
    def test_list_objects_returns_keys(self, handler, mock_boto3_client):
        mock_client = mock_boto3_client.client.return_value
        mock_client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "raw/file1.json"}, {"Key": "raw/file2.json"}]}
        ]
        keys = handler.list_objects("raw/")
        assert keys == ["raw/file1.json", "raw/file2.json"]

    def test_delete_object_calls_delete(self, handler, mock_boto3_client):
        handler.delete_object("raw/old.json")
        mock_client = mock_boto3_client.client.return_value
        mock_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="raw/old.json"
        )
