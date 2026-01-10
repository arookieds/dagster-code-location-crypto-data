"""S3-compatible IO Manager (MinIO, AWS S3, etc.)."""

from __future__ import annotations
from ssl import ALERT_DESCRIPTION_HANDSHAKE_FAILURE

import contextlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import boto3
from botocore.exceptions import ClientError
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from pydantic import Field
from sqlalchemy import create_engine, text

from dagster_crypto_data.defs.utils import get_run_info

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


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

    # Optional database configuration for timestamp-based filtering
    db_host: str | None = Field(
        default=None,
        description="Database host for querying max timestamp (optional)",
    )
    db_port: int | None = Field(
        default=None,
        description="Database port (optional)",
    )
    db_name: str | None = Field(
        default=None,
        description="Database name (optional)",
    )
    db_username: str | None = Field(
        default=None,
        description="Database username (optional)",
    )
    db_password: str | None = Field(
        default=None,
        description="Database password (optional, use EnvVar for security)",
    )
    db_schema: str | None = Field(
        default=None,
        description="Database schema for target tables (optional)",
    )
    target_table_name: str | None = Field(
        default=None,
        description="Target table name to query for max timestamp (e.g., 'raw_tickers')",
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

    def _get_s3_key(
        self, context: OutputContext | InputContext, timestamp: str | int
    ) -> str:
        """Get the S3 key (path) for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            S3 key string (e.g., "binance/raw/tickers_1234567890.json")
        """
        # For OutputContext, use the asset's own key
        # For InputContext, use the upstream asset key (the one that wrote the data)
        if isinstance(context, InputContext):
            asset_key = self._get_upstream_asset_key(context)
        else:
            asset_key = context.asset_key

        asset_path: str = "/".join(asset_key.path).replace("_", "/")
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

        run_info: dict = get_run_info(context)
        run_id: str = run_info["run_id"]
        run_timestamp: str = run_info["timestamp"]
        run_dt: str = run_info["dt"]
        context.log.info(f'Successfully loaded {{"run_id": "{run_id}"}} details.')
        s3_client = self._get_s3_client()
        s3_key = self._get_s3_key(context, run_timestamp)

        # Serialize to JSON
        json_data = json.dumps(obj, indent=2, default=str)
        timestamp = obj.get("metadata", {}).get("timestamp")

        # Safely parse timestamp
        try:
            if timestamp is not None:
                dt = datetime.fromtimestamp(float(timestamp) / 1000)
                dt_str = dt.strftime("%y-%m-%d %H:%M:%S.%f")
            else:
                dt_str = datetime.now(UTC).strftime("%y-%m-%d %H:%M:%S.%f")
        except (ValueError, TypeError):
            dt_str = datetime.now(UTC).strftime("%y-%m-%d %H:%M:%S.%f")

        # Upload to S3
        try:
            s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json_data.encode("utf-8"),
                Metadata={
                    "run_id": run_id,
                    # Extraction timestamp
                    "timestamp": str(timestamp) if timestamp else "",
                    # Extraction datetime
                    "dt": dt_str,
                    # Dagster job timestamp
                    "run_timestamp": run_timestamp,
                    # Dagster job datetime
                    "run_dt": run_dt,
                    "loaded": "false",
                },
                ContentType="application/json",
            )
            context.log.info(f"Stored asset to s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            context.log.error(f"Failed to upload to S3: {e}")
            raise

    def _get_upstream_asset_key(self, context: InputContext) -> AssetKey:
        """Get the upstream asset key from InputContext.

        When an asset loads an input, we need the key of the upstream asset
        that produced the data, not the key of the consuming asset.

        Args:
            context: Dagster input context

        Returns:
            AssetKey of the upstream asset
        """
        # In Dagster, InputContext.asset_key should be the upstream asset key
        # But if there's an upstream_output, we can get it from there too
        upstream = getattr(context, "upstream_output", None)
        if upstream is not None:
            upstream_asset_key = getattr(upstream, "asset_key", None)
            if upstream_asset_key is not None:
                return cast("AssetKey", upstream_asset_key)

        # Fallback to context.asset_key (should be upstream asset in normal cases)
        return context.asset_key

    def _has_upstream_output(self, context: InputContext) -> bool:
        """Check if input context has a valid upstream output (running with extract).

        Args:
            context: Dagster input context

        Returns:
            True if running with extract (has upstream output with run_id),
            False if running standalone
        """
        upstream = getattr(context, "upstream_output", None)
        if upstream is None:
            return False

        # Accessing run_id can raise DagsterInvariantViolationError if the
        # OutputContext was created without a run_id (standalone transform)
        try:
            run_id = getattr(upstream, "run_id", None)
            return run_id is not None
        except Exception:
            return False

    def _get_max_extraction_timestamp(
        self, context: InputContext, exchange_id: str
    ) -> tuple[list[float] | None, bool]:
        """Query database for max extraction_timestamp from target table.

        Args:
            context: Dagster input context

        Returns:
            Tuple of (max_timestamp, table_is_empty):
            - max_timestamp: Max extraction_timestamp as ISO string, or None if not found
            - table_is_empty: True if table exists but is empty, False otherwise
        """
        # Check if database and target table are configured
        if not all(
            [
                self.db_host,
                self.db_port,
                self.db_name,
                self.db_username,
                self.db_password,
                self.target_table_name,
            ]
        ):
            context.log.debug(
                "Database or target_table_name not configured - skipping timestamp query"
            )
            return (None, False)

        try:
            # Build connection URL
            db_url = f"postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            engine: Engine = create_engine(db_url)

            # Use configured target table name
            schema_prefix = f"{self.db_schema}." if self.db_schema else ""
            full_table_name = f"{schema_prefix}{self.target_table_name}"

            # Query max extraction_timestamp
            query = text(
                f"SELECT distinct(extraction_timestamp) as max_ts FROM {full_table_name} where exchange_id = '{exchange_id}'"
            )

            with engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
                max_ts = [r[0] for r in rows] if rows else None

            engine.dispose()

            if max_ts:
                context.log.info(
                    f"extraction_timestamp in {full_table_name}: {max_ts, }"
                )
                return (list(map(float, max_ts)), False)
            else:
                context.log.info(
                    f"Table {full_table_name} is empty, for exchange_id '{exchange_id}' - will load ALL files"
                )
                return (None, True)  # Table exists but is empty

        except Exception as e:
            context.log.warning(f"Failed to query max timestamp from database: {e}")
            context.log.warning("Falling back to metadata-based filtering")
            return (None, False)

    def _list_unloaded_files(
        self, context: InputContext, asset_prefix: str
    ) -> list[str]:
        """List S3 objects to load based on timestamp comparison.

        Strategy:
        1. Query database for max extraction_timestamp (if DB configured)
        2. If table is empty: Load ALL files
        3. If table has data: Load files where extraction_timestamp > max DB timestamp
        4. Fallback to metadata loaded=false if DB query fails or not configured

        Args:
            context: Dagster input context
            asset_prefix: S3 key prefix for the asset (e.g., "extract/binance/ohlcv")

        Returns:
            List of S3 keys for unloaded files
        """
        s3_client = self._get_s3_client()
        unloaded_keys: list[str] = []
        exchange_id = asset_prefix.split("/")[0]
        # Get max timestamp from database
        max_db_timestamps, table_is_empty = self._get_max_extraction_timestamp(
            context, exchange_id
        )

        try:
            # List all objects with the asset prefix
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=asset_prefix)

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    key = obj["Key"]
                    # Get object metadata
                    head_response = s3_client.head_object(Bucket=self.bucket, Key=key)
                    metadata = head_response.get("Metadata", {})

                    # Strategy 1: Table is empty - load ALL files
                    if table_is_empty:
                        unloaded_keys.append(key)
                        context.log.debug(f"Table empty - loading file {key}")
                    # Strategy 2: Table has data - compare timestamps
                    elif max_db_timestamps is not None:
                        file_extraction_ts = float(
                            metadata.get("timestamp")
                        )  # ISO format timestamp
                        if file_extraction_ts and int(file_extraction_ts) not in map(
                            int, max_db_timestamps
                        ):
                            unloaded_keys.append(key)
                            context.log.debug(
                                f"File {key} timestamp {int(file_extraction_ts)} not in DB {max_db_timestamps, }"
                            )
                    # Strategy 3: Fallback to loaded metadata flag
                    else:
                        if metadata.get("loaded", "false").lower() == "false":
                            unloaded_keys.append(key)

            context.log.info(
                f"Found {len(unloaded_keys)} unloaded files with prefix: {asset_prefix}"
            )
            return unloaded_keys

        except ClientError as e:
            context.log.error(f"Failed to list objects in S3: {e}")
            raise

    def load_input(self, context: InputContext) -> dict[str, Any]:
        """Load dictionary/dictionaries from JSON file(s) in S3.

        Hybrid behavior:
        - If running with extract (has upstream_output): Load only the current run's file
        - If running standalone: Load ALL unloaded files and merge them

        Args:
            context: Dagster input context

        Returns:
            Dictionary loaded from S3 (single file or merged from multiple files)

        Raises:
            ClientError: If S3 download fails or object doesn't exist
        """
        s3_client = self._get_s3_client()

        # Check if running standalone (no upstream output) or with extract
        has_upstream = self._has_upstream_output(context)

        if has_upstream:
            # Normal mode: Load only the file from the current run
            run_info: dict = get_run_info(context)
            run_id = run_info["run_id"]
            timestamp: int = int(run_info["timestamp"])
            context.log.info(
                f"Running with extract - loading single file from run: {run_id}"
            )
            s3_key = self._get_s3_key(context, timestamp)
            keys_to_load = [s3_key]
        else:
            # Standalone mode: Load all unloaded files
            context.log.info("Running standalone - loading ALL unloaded files")
            # Get asset prefix from upstream asset (the extract asset that wrote the data)
            upstream_asset_key = self._get_upstream_asset_key(context)
            asset_path = "/".join(upstream_asset_key.path).replace("_", "/")
            context.log.info(
                f"Looking for S3 files with prefix: {asset_path} (upstream asset: {upstream_asset_key.path})"
            )
            keys_to_load = self._list_unloaded_files(context, asset_path)

            if not keys_to_load:
                context.log.warning(
                    f"No unloaded files found for asset: {context.asset_key.path}"
                )
                return {"metadata": {}, "data": []}

        # Load all files and accumulate records
        # Each file contains: {"metadata": {...}, "data": {symbol: ticker_data, ...}}
        # We accumulate ALL ticker records from ALL files as a list
        # This ensures no data is lost when multiple files have the same symbols
        all_records: list[dict[str, Any]] = []
        metadatas: list[dict[str, Any]] = []
        timestamps: list[int] = []

        for s3_key in keys_to_load:
            try:
                response = s3_client.get_object(Bucket=self.bucket, Key=s3_key)
                content_type = response.get("ContentType")
                metadata = response.get("Metadata", {})
                json_data = response["Body"].read().decode("utf-8")
                file_data = json.loads(json_data)

                context.log.info(f"Loaded asset from s3://{self.bucket}/{s3_key}")

                # Accumulate all ticker records from this file
                if "data" in file_data:
                    file_metadata = file_data.get("metadata", {})
                    ts = metadata.get("timestamp")
                    timestamps.append(ts)
                    # Store both S3 metadata and file content metadata
                    metadatas.append(
                        {
                            "timestamp": ts,
                            "s3_metadata": metadata,
                            "file_metadata": file_metadata,
                        }
                    )

                    # Safely parse timestamp
                    extraction_ts = None
                    if ts is not None:
                        with contextlib.suppress(ValueError, TypeError):
                            extraction_ts = int(float(ts))

                    for _symbol, ticker_data in file_data["data"].items():
                        # Add file metadata to each record for traceability
                        record = {
                            **ticker_data,
                            "extraction_timestamp": extraction_ts,
                            "exchange_id": file_metadata.get("exchange_id"),
                        }
                        all_records.append(record)

                # Mark as loaded
                metadata["loaded"] = "true"
                s3_client.copy_object(
                    Key=s3_key,
                    Bucket=self.bucket,
                    CopySource={"Bucket": self.bucket, "Key": s3_key},
                    Metadata=metadata,
                    MetadataDirective="REPLACE",
                    ContentType=content_type,
                )
                context.log.info(f"Marked as loaded: s3://{self.bucket}/{s3_key}")

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "NoSuchKey":
                    context.log.warning(
                        f"File not found (skipping): s3://{self.bucket}/{s3_key}"
                    )
                    continue
                context.log.error(f"Failed to download from S3: {e}")
                raise

        context.log.info(
            f"Loaded {len(keys_to_load)} files with {len(all_records)} total records"
        )

        # Return accumulated records
        # Note: 'data' is now a list of records, not a dict of dicts
        latest_metadata = {}
        if metadatas:
            # Safely find max timestamp
            try:
                latest_entry = max(
                    metadatas, key=lambda x: float(x.get("timestamp") or 0)
                )
                # Return the file content metadata from the latest file
                latest_metadata = latest_entry.get("file_metadata", {})
            except (ValueError, TypeError):
                # Fallback if timestamps are invalid
                latest_metadata = metadatas[-1].get("file_metadata", {})

        return {
            "metadata": latest_metadata,
            "data": all_records,
        }

        context.log.info(
            f"Loaded {len(keys_to_load)} files with {len(all_records)} total records"
        )

        # Return accumulated records
        # Note: 'data' is now a list of records, not a dict of dicts
        latest_metadata = {}
        if metadatas:
            # Safely find max timestamp
            try:
                latest_entry = max(
                    metadatas, key=lambda x: float(x.get("timestamp") or 0)
                )
                latest_metadata = latest_entry.get("metadata", {})
            except (ValueError, TypeError):
                # Fallback if timestamps are invalid
                latest_metadata = metadatas[-1].get("metadata", {})

        return {
            "metadata": latest_metadata,
            "data": all_records,
        }
