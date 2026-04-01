"""AWS configuration."""

import os

# ------------------------------------------------------------------
# Credentials (prefer IAM roles / environment variables in production)
# ------------------------------------------------------------------

AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID: str = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

# ------------------------------------------------------------------
# S3
# ------------------------------------------------------------------

S3_BUCKET: str = os.environ.get("S3_BUCKET", "drone-telemetry-data")
S3_RAW_PREFIX: str = os.environ.get("S3_RAW_PREFIX", "raw/")
S3_PROCESSED_PREFIX: str = os.environ.get("S3_PROCESSED_PREFIX", "processed/")
S3_ALERTS_PREFIX: str = os.environ.get("S3_ALERTS_PREFIX", "alerts/")

# ------------------------------------------------------------------
# Redshift
# ------------------------------------------------------------------

REDSHIFT_HOST: str = os.environ.get("REDSHIFT_HOST", "")
REDSHIFT_PORT: int = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE: str = os.environ.get("REDSHIFT_DATABASE", "drone_analytics")
REDSHIFT_USER: str = os.environ.get("REDSHIFT_USER", "")
REDSHIFT_PASSWORD: str = os.environ.get("REDSHIFT_PASSWORD", "")
REDSHIFT_IAM_ROLE: str = os.environ.get("REDSHIFT_IAM_ROLE", "")

REDSHIFT_SCHEMA: str = os.environ.get("REDSHIFT_SCHEMA", "public")
REDSHIFT_TELEMETRY_TABLE: str = "drone_telemetry"
REDSHIFT_ROUTES_TABLE: str = "drone_routes"
REDSHIFT_ALERTS_TABLE: str = "collision_alerts"
