"""Filesystem IO Manager for local development."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field


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

    def _get_path(self, context: OutputContext | InputContext) -> Path:
        """Get the file path for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            Path object for the asset file
        """
        # Use asset key path to create nested directory structure
        # e.g., extract_binance_ohlcv -> extract/binance/ohlcv.json
        asset_path = "/".join(context.asset_key.path)
        return Path(self.base_path) / f"{asset_path}.json"

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

        filepath = self._get_path(context)

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
        filepath = self._get_path(context)

        if not filepath.exists():
            raise FileNotFoundError(
                f"Asset file not found: {filepath}. "
                f"Make sure the upstream asset has been materialized."
            )

        # Read JSON file
        with open(filepath) as f:
            data = json.load(f)

        context.log.info(f"Loaded asset from {filepath}")
        return data
