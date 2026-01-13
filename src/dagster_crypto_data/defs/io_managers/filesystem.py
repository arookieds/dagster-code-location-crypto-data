"""Filesystem IO Manager for local development."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from dagster import AssetKey, ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

if TYPE_CHECKING:
    from sqlmodel import SQLModel

from dagster_crypto_data.defs import models
from dagster_crypto_data.defs.resources.database import DatabaseConfig  # noqa: TC001
from dagster_crypto_data.defs.utils import get_max_extraction_timestamp, get_run_info


class FilesystemIOManager(ConfigurableIOManager):
    """Store data as JSON files on the local filesystem.

    This IO manager is designed for local development and testing.
    It stores dictionaries as JSON files and can load them back.

    Attributes:
        base_path: Base directory for storing files (default: ./data)
        create_dirs: Whether to create directories if they don't exist (default: True)

    Example:
        ```python
        from dagster import Definitions
        from dagster_crypto_data.defs.io_managers import FilesystemIOManager

        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": FilesystemIOManager(base_path="./data/raw"),
            },
        )
        ```
    """

    base_path: str = Field(
        default="./data",
        description="Base directory for storing files",
    )
    create_dirs: bool = Field(
        default=True,
        description="Create directories if they don't exist",
    )
    database: DatabaseConfig | None = Field(
        default=None,
        description="Database configuration for querying existing timestamps (optional for local filesystem mode)",
    )

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

    def _get_path(
        self, context: OutputContext | InputContext, timestamp: str | int
    ) -> Path:
        """Get the file path for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            Path object for the asset file
        """
        if isinstance(context, InputContext):
            asset_key = self._get_upstream_asset_key(context)
        else:
            asset_key = context.asset_key

        asset_path: str = "/".join(asset_key.path).replace("_", "/")
        return Path(self.base_path) / f"{asset_path}_{timestamp}.json"

    def _get_metadata_path(self, filepath: Path) -> Path:
        """Get the metadata file path for a data file.

        Args:
            filepath: Path to the data file

        Returns:
            Path to the metadata file (.meta.json)
        """
        return filepath.with_suffix(".meta.json")

    def handle_output(self, context: OutputContext, obj: dict[str, Any]) -> None:
        """Store a dictionary as a JSON file.

        Args:
            context: Dagster output context
            obj: Dictionary to store

        Raises:
            TypeError: If obj is not a dictionary
        """
        if not isinstance(obj, dict):
            raise TypeError(
                f"FilesystemIOManager expects dict, got {type(obj).__name__}"
            )
        run_info: dict = get_run_info(context)
        run_timestamp: str = run_info["timestamp"]
        filepath = self._get_path(context, run_timestamp)

        # Create parent directories if needed
        if self.create_dirs:
            filepath.parent.mkdir(parents=True, exist_ok=True)

        # Extract timestamp from data
        timestamp = obj.get("metadata", {}).get("timestamp")

        # Safely parse timestamp
        from datetime import UTC, datetime

        try:
            if timestamp is not None:
                dt = datetime.fromtimestamp(float(timestamp) / 1000)
                dt_str = dt.strftime("%y-%m-%d %H:%M:%S.%f")
            else:
                dt_str = datetime.now(UTC).strftime("%y-%m-%d %H:%M:%S.%f")
        except (ValueError, TypeError):
            dt_str = datetime.now(UTC).strftime("%y-%m-%d %H:%M:%S.%f")

        # Write JSON file
        with open(filepath, "w") as f:
            json.dump(obj, f, indent=2, default=str)

        # Write metadata file
        metadata_path = self._get_metadata_path(filepath)
        run_id = run_info.get("run_id", "No run id")
        run_dt = run_info.get("dt", "")

        metadata = {
            "run_id": run_id,
            "timestamp": str(timestamp) if timestamp else "",
            "dt": dt_str,
            "run_timestamp": run_timestamp,
            "run_dt": run_dt,
            "loaded": "false",
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        context.log.info(f"Stored asset to {filepath}")

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

        try:
            run_id = getattr(upstream, "run_id", None)
            return run_id is not None
        except Exception:
            return False

    def _list_unloaded_files(
        self,
        context: InputContext,
        asset_prefix: str,
        model: type[SQLModel] | None = None,
    ) -> list[str]:
        """List files to load based on timestamp comparison or metadata flags.

        Strategy for database mode (model provided):
        1. Query database for max extraction_timestamp
        2. If table is empty: Load ALL files
        3. If table has data: Load files where extraction_timestamp not in DB

        Strategy for filesystem mode (no model):
        1. Use metadata "loaded" flag (loaded=false means unloaded)

        Args:
            context: Dagster input context
            asset_prefix: File path prefix for the asset (e.g., "extract/binance")
            model: Optional SQLModel class defining the target table

        Returns:
            List of filenames for unloaded files
        """
        unloaded_keys: list[str] = []
        exchange_id = asset_prefix.split("/")[0]

        # Construct path relative to base_path
        # asset_prefix is like "bybit/raw/tickers", we want the directory containing files
        files_path = "/".join(asset_prefix.split("/")[:2])  # "bybit/raw"
        path = Path(self.base_path) / files_path

        try:
            files: list = [f.name for f in path.iterdir() if f.is_file()]
        except FileNotFoundError:
            context.log.warning(f"Path not found: {files_path}")
            return []

        data_files: list = sorted([f for f in files if "meta" not in f])
        metadata_files: list = sorted([f for f in files if "meta" in f])

        # If model is provided and database is available, use database-based filtering
        if model is not None and self.database is not None:
            # Cast model from SQLModel to CryptoModel (both have same schema)
            max_db_timestamps, table_is_empty = get_max_extraction_timestamp(
                context,
                self.database,
                exchange_id,
                cast("type", model),
            )

            for file, meta in zip(data_files, metadata_files, strict=False):
                try:
                    meta_path = path / meta
                    with open(meta_path) as fm:
                        m = json.load(fm)
                        timestamp = int(float(m.get("timestamp")))
                    if timestamp not in map(int, max_db_timestamps):
                        context.log.debug(
                            f"Loading data from file with metadata: \n{json.dumps(m)}"
                        )
                        unloaded_keys.append(file)
                except (json.JSONDecodeError, ValueError, FileNotFoundError) as e:
                    context.log.warning(f"Failed to read metadata {meta}: {e}")
                    continue
        else:
            # Fallback to metadata "loaded" flag for filesystem mode
            context.log.info("Using metadata files for filtering (no model provided)")
            for file, meta in zip(data_files, metadata_files, strict=False):
                try:
                    meta_path = path / meta
                    with open(meta_path) as fm:
                        meta_data = json.load(fm)
                    # Load file if marked as not loaded
                    if meta_data.get("loaded", "false").lower() != "true":
                        unloaded_keys.append(file)
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    context.log.warning(f"Failed to read metadata {meta}: {e}")
                    # If we can't read metadata, treat as unloaded
                    unloaded_keys.append(file)

        return unloaded_keys

    def load_input(self, context: InputContext) -> dict[str, Any]:
        """Load data from JSON file(s).

        Supports two modes:
        - Single file mode: Load current run's file when running with extract
        - Multi-file mode: Load all unloaded files when running standalone

        Args:
            context: Dagster input context

        Returns:
            Dictionary with metadata and data loaded from JSON file(s)

        Raises:
            FileNotFoundError: If files don't exist in single-file mode
        """
        has_upstream = self._has_upstream_output(context)

        if has_upstream:
            # Running with extract - load single file
            run_info: dict = get_run_info(context)
            run_id = run_info["run_id"]
            timestamp: str = run_info["timestamp"]
            context.log.info(
                f"Running with extract - loading single file from run: {run_id}"
            )
            filepath = self._get_path(context, timestamp)

            if not filepath.exists():
                raise FileNotFoundError(f"File not found: {filepath}")

            with open(filepath) as f:
                data = json.load(f)

            context.log.info(f"Loaded asset from {filepath}")
            return cast("dict[str, Any]", data)
        else:
            # Running standalone - load ALL unloaded files
            context.log.info("Running standalone - loading ALL unloaded files")
            upstream_asset_key = self._get_upstream_asset_key(context)
            asset_path = "/".join(upstream_asset_key.path).replace("_", "/")
            context.log.info(
                f"Looking for files with prefix: {asset_path} (upstream asset: {upstream_asset_key.path})"
            )

            # Try to get model from context metadata, but fall back to None for local mode
            # (we use metadata files for filtering instead)
            metadata = getattr(context, "definition_metadata", None) or {}
            model_name: str = metadata.get("model", "")
            model = getattr(models, model_name, None) if model_name else None

            # Pass model to _list_unloaded_files (may be None for local filesystem mode)
            files_to_load = self._list_unloaded_files(context, asset_path, model)

            if not files_to_load:
                context.log.warning(
                    f"No unloaded files found for asset: {context.asset_key.path}"
                )
                return {"metadata": {}, "data": []}

            # Load all files and accumulate records
            all_records: list[dict[str, Any]] = []
            metadatas: list[dict[str, Any]] = []

            # Construct the base path for files
            base_path = Path(self.base_path) / asset_path.rsplit("/", 1)[0]

            for filename in files_to_load:
                filepath = base_path / filename

                try:
                    with open(filepath) as f:
                        file_data = json.load(f)

                    context.log.info(f"Loaded asset from {filepath}")

                    # Extract metadata and data
                    file_metadata = file_data.get("metadata", {})
                    ticker_data = file_data.get("data", {})
                    metadatas.append(file_metadata)

                    # Get extraction timestamp
                    extraction_ts = file_metadata.get("timestamp")

                    # Flatten ticker data into records
                    for _symbol, ticker_info in ticker_data.items():
                        record = {
                            **ticker_info,
                            "extraction_timestamp": extraction_ts,
                            "exchange_id": file_metadata.get("exchange_id"),
                        }
                        all_records.append(record)

                    # Mark as loaded in metadata file
                    metadata_path = self._get_metadata_path(filepath)
                    if metadata_path.exists():
                        with open(metadata_path) as f:
                            meta = json.load(f)
                        meta["loaded"] = "true"
                        with open(metadata_path, "w") as f:
                            json.dump(meta, f, indent=2)
                        context.log.info(f"Marked as loaded: {filepath}")

                except FileNotFoundError:
                    context.log.warning(f"File not found (skipping): {filepath}")
                    continue
                except json.JSONDecodeError as e:
                    context.log.error(f"Failed to parse JSON from {filepath}: {e}")
                    raise

            context.log.info(
                f"Loaded {len(files_to_load)} files with {len(all_records)} total records"
            )

            # Return latest metadata and accumulated records
            latest_metadata = metadatas[-1] if metadatas else {}
            return {
                "metadata": latest_metadata,
                "data": all_records,
            }
