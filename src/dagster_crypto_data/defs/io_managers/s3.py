"""S3-compatible IO Manager (MinIO, AWS S3, etc.)."""

from __future__ import annotations

import json
from typing import Any, cast
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    RunRecord,
)
from pydantic import Field


class S3IOManager(ConfigurableIOManager):
    """Store data as JSON files in S3-compatible storage (MinIO, AWS S3).

    This IO manager works with any S3-compatible storage backend including
    MinIO (local development) and AWS S3 (production).

    Attributes:
        endpoint_url: S3 endpoint URL (e.g., http://localhost:9000 for MinIO)
        access_key: S3 access key ID
        secret_key: S3 secret access key (use EnvVar for security)
        bucket: S3 bucket name
        region: AWS region (default: us-east-1)
        use_ssl: Whether to use SSL/TLS (default: True for production)

    Example:
        ```python
        from dagster import Definitions, EnvVar
        from dagster_crypto_data.defs.io_managers import S3IOManager

        # MinIO (local)
        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": S3IOManager(
                    endpoint_url="http://localhost:9000",
                    access_key=EnvVar("MINIO_ACCESS_KEY"),
                    secret_key=EnvVar("MINIO_SECRET_KEY"),
                    bucket="crypto-raw",
                    use_ssl=False,
                ),
            },
        )

        # AWS S3 (production)
        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": S3IOManager(
                    endpoint_url=None,  # Use default AWS S3
                    access_key=EnvVar("AWS_ACCESS_KEY_ID"),
                    secret_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
                    bucket="my-production-bucket",
                    region="us-west-2",
                ),
            },
        )
        ```
    """

    endpoint_url: str | None = Field(
        default=None,
        description="S3 endpoint URL (None for AWS S3, http://localhost:9000 for MinIO)",
    )
    access_key: str = Field(description="S3 access key ID")
    secret_key: str = Field(description="S3 secret access key (use EnvVar for security)")
    bucket: str = Field(description="S3 bucket name")
    region: str = Field(default="us-east-1", description="AWS region")
    use_ssl: bool = Field(
        default=True,
        description="Use SSL/TLS (False for local MinIO, True for production)",
    )

    def _get_s3_client(self) -> Any:
        """Get boto3 S3 client.

        Returns:
            Configured boto3 S3 client
        """
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            use_ssl=self.use_ssl,
        )

    def _get_run_info(self, context: OutputContext | InputContext) -> dict:
        if isinstance(context, OutputContext):
            run_id: str = context.run_id
        elif isinstance(context, InputContext):
            run_id: str = context.upstream_output.run_id or "No run id"
        run: RunRecord | None = context.step_context.instance.get_run_record_by_id(
            run_id
        )
        dt: datetime = run.create_timestamp or datetime.now(timezone.utc)
        timestamp: str = str(int(dt.timestamp() * 1000))
        return {
            "run_id": run_id,
            "timestamp": timestamp,
            "dt": dt.strftime("%y-%m-%d %H:%M:%S.%f"),
        }

    def _get_s3_key(self, context: OutputContext | InputContext, timestamp: int) -> str:
        """Get the S3 key (path) for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            S3 key string (e.g., "extract/binance/ohlcv.json")
        """
        # Use asset key path to create S3 key
        asset_path: str = "/".join(context.asset_key.path).replace("_", "/")
        return f"{asset_path}_{timestamp}.json"

    def handle_output(self, context: OutputContext, obj: dict[str, Any]) -> None:
        """Store a dictionary as a JSON file in S3.

        Args:
            context: Dagster output context
            obj: Dictionary to store

        Raises:
            TypeError: If obj is not a dictionary
            ClientError: If S3 upload fails
        """
        if not isinstance(obj, dict):
            raise TypeError(f"S3IOManager expects dict, got {type(obj).__name__}")

        run_info: dict = self._get_run_info(context)
        run_id: str = run_info["run_id"]
        timestamp: str = run_info["timestamp"]
        dt: str = run_info["dt"]
        context.log.info(f'Successfully loaded {{"run_id": "{run_id}"}} details.')
        s3_client = self._get_s3_client()
        s3_key = self._get_s3_key(context, timestamp)

        # Serialize to JSON
        json_data = json.dumps(obj, indent=2, default=str)

        # Upload to S3
        try:
            s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json_data.encode("utf-8"),
                Metadata={
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "dt": dt,
                    "loaded": "false",
                },
                ContentType="application/json",
            )
            context.log.info(f"Stored asset to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            context.log.error(f"Failed to upload to S3: {e}")
            raise

    def load_input(self, context: InputContext) -> dict[str, Any]:
        """Load a dictionary from a JSON file in S3.

        Args:
            context: Dagster input context

        Returns:
            Dictionary loaded from S3

        Raises:
            ClientError: If S3 download fails or object doesn't exist
        """
        run_info: dict = self._get_run_info(context)
        run_id = run_info["run_id"]
        timestamp: int = int(run_info["timestamp"])
        context.log.info(f'Successfully loaded {{"run_id": "{run_id}"}} details.')
        s3_client = self._get_s3_client()
        s3_key = self._get_s3_key(context, timestamp)

        # Download from S3
        try:
            response = s3_client.get_object(Bucket=self.bucket, Key=s3_key)
            content_type = response.get("ContentType")
            metadata = response.get("Metadata", {})
            metadata["loaded"] = "true"
            json_data = response["Body"].read().decode("utf-8")
            data = json.loads(json_data)
            context.log.info(f"Loaded asset from s3://{self.bucket}/{s3_key}")
            s3_client.copy_object(
                Key=s3_key,
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": s3_key},
                Metadata=metadata,
                MetadataDirective="REPLACE",
                ContentType=content_type,
            )
            return cast("dict[str, Any]", data)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(
                    f"Asset not found in S3: s3://{self.bucket}/{s3_key}. "
                    f"Make sure the upstream asset has been materialized."
                ) from e
            context.log.error(f"Failed to download from S3: {e}")
            raise
