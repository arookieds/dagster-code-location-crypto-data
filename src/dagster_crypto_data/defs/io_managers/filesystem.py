"""Filesystem IO Manager for local development."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from dagster import AssetKey, ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

from dagster_crypto_data.defs.utils import get_run_info


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

        # Write JSON file
        with open(filepath, "w") as f:
            json.dump(obj, f, indent=2, default=str)

        context.log.info(f"Stored asset to {filepath}")

    def load_input(self, context: InputContext) -> dict[str, Any]:
        """Load a dictionary from a JSON file.

        Args:
            context: Dagster input context

        Returns:
            Dictionary loaded from JSON file

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        run_info: dict = get_run_info(context)
        run_timestamp: str = run_info["timestamp"]
        filepath = self._get_path(context, run_timestamp)

        if not filepath.exists():
            raise FileNotFoundError(
                f"Asset file not found: {filepath}. "
                f"Make sure the upstream asset has been materialized."
            )

        # Read JSON file
        with open(filepath) as f:
            data = json.load(f)

        context.log.info(f"Loaded asset from {filepath}")
        return cast("dict[str, Any]", data)
