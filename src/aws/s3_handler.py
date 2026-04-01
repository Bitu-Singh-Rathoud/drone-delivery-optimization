"""AWS S3 handler for drone telemetry data storage."""

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from config import aws_config

logger = logging.getLogger(__name__)


class S3Handler:
    """
    Handles reading and writing drone telemetry data to AWS S3.

    Data is stored as newline-delimited JSON (NDJSON) or Parquet via
    partitioned prefixes: ``<prefix>year=YYYY/month=MM/day=DD/``.
    """

    def __init__(
        self,
        bucket: str = aws_config.S3_BUCKET,
        region: str = aws_config.AWS_REGION,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ) -> None:
        self._bucket = bucket
        session_kwargs: Dict[str, Any] = {"region_name": region}
        key_id = aws_access_key_id or aws_config.AWS_ACCESS_KEY_ID
        secret = aws_secret_access_key or aws_config.AWS_SECRET_ACCESS_KEY
        if key_id and secret:
            session_kwargs["aws_access_key_id"] = key_id
            session_kwargs["aws_secret_access_key"] = secret
        self._s3 = boto3.client("s3", **session_kwargs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload_json_records(
        self,
        records: List[dict],
        prefix: str = aws_config.S3_RAW_PREFIX,
        partition_by_date: bool = True,
    ) -> str:
        """
        Serialise *records* as NDJSON and upload to S3.

        Returns the full S3 key of the uploaded object.
        """
        key = self._build_key(prefix, "telemetry.ndjson", partition_by_date)
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        self._put_object(key, body, content_type="application/x-ndjson")
        logger.info("Uploaded %d records to s3://%s/%s", len(records), self._bucket, key)
        return key

    def upload_file(self, local_path: str, s3_key: str) -> None:
        """Upload a local file to *s3_key* within the configured bucket."""
        try:
            self._s3.upload_file(local_path, self._bucket, s3_key)
            logger.info("Uploaded %s -> s3://%s/%s", local_path, self._bucket, s3_key)
        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to upload %s: %s", local_path, exc)
            raise

    def download_json_records(self, s3_key: str) -> List[dict]:
        """Download and parse NDJSON from *s3_key*."""
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=s3_key)
            body = response["Body"].read().decode("utf-8")
            return [json.loads(line) for line in body.splitlines() if line.strip()]
        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to download s3://%s/%s: %s", self._bucket, s3_key, exc)
            raise

    def list_objects(self, prefix: str) -> List[str]:
        """Return a list of S3 keys under *prefix*."""
        keys: List[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def delete_object(self, s3_key: str) -> None:
        """Delete a single object at *s3_key*."""
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=s3_key)
            logger.info("Deleted s3://%s/%s", self._bucket, s3_key)
        except (BotoCoreError, ClientError) as exc:
            logger.error("Failed to delete s3://%s/%s: %s", self._bucket, s3_key, exc)
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _put_object(self, key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType=content_type,
            )
        except (BotoCoreError, ClientError) as exc:
            logger.error("put_object failed for key %s: %s", key, exc)
            raise

    @staticmethod
    def _build_key(prefix: str, filename: str, partition_by_date: bool) -> str:
        if partition_by_date:
            now = datetime.now(tz=timezone.utc)
            partition = f"year={now.year:04d}/month={now.month:02d}/day={now.day:02d}/"
            return f"{prefix}{partition}{filename}"
        return f"{prefix}{filename}"
