"""Tests for FilesystemIOManager."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest
from dagster import (
    AssetKey,
    InputContext,
    OutputContext,
    build_input_context,
    build_output_context,
)

from dagster_crypto_data.defs.io_managers import FilesystemIOManager

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def filesystem_io_manager(temp_data_dir: Path) -> FilesystemIOManager:
    """Create FilesystemIOManager instance."""
    return FilesystemIOManager(base_path=str(temp_data_dir))


@pytest.fixture
def sample_data() -> dict[str, Any]:
    """Sample data for testing."""
    return {
        "data": [
            [1735689600000, 50000.0, 51000.0, 49000.0, 50500.0, 100.5],
            [1735693200000, 50500.0, 51500.0, 49500.0, 51000.0, 120.3],
        ],
        "metadata": {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "timestamp": 1735689600000,
        },
    }


@pytest.fixture
def output_context() -> OutputContext:
    """Create output context for testing."""
    return build_output_context(asset_key=AssetKey(["extract_binance_ohlcv"]))


@pytest.fixture
def input_context() -> InputContext:
    """Create input context for testing."""
    return build_input_context(asset_key=AssetKey(["extract_binance_ohlcv"]))


class TestFilesystemIOManager:
    """Test FilesystemIOManager functionality."""

    def test_handle_output_creates_file(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
    ) -> None:
        """Test that handle_output creates a JSON file."""
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Check file exists
        expected_file = temp_data_dir / "extract_binance_ohlcv.json"
        assert expected_file.exists()

        # Check file content
        with open(expected_file) as f:
            loaded_data = json.load(f)
        assert loaded_data == sample_data

    def test_handle_output_creates_nested_directories(
        self,
        filesystem_io_manager: FilesystemIOManager,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
    ) -> None:
        """Test that handle_output creates nested directories."""
        context = build_output_context(
            asset_key=AssetKey(["extract", "binance", "ohlcv"])
        )
        filesystem_io_manager.handle_output(context, sample_data)

        # Check nested file exists
        expected_file = temp_data_dir / "extract" / "binance" / "ohlcv.json"
        assert expected_file.exists()

    def test_handle_output_with_invalid_type_raises_error(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-dict."""
        with pytest.raises(TypeError, match="FilesystemIOManager expects dict"):
            filesystem_io_manager.handle_output(output_context, "not a dict")  # type: ignore

    def test_load_input_reads_file(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        input_context: InputContext,
        sample_data: dict[str, Any],
    ) -> None:
        """Test that load_input reads the JSON file."""
        # First, write the file
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Then, read it back
        loaded_data = filesystem_io_manager.load_input(input_context)
        assert loaded_data == sample_data

    def test_load_input_file_not_found_raises_error(
        self,
        filesystem_io_manager: FilesystemIOManager,
        input_context: InputContext,
    ) -> None:
        """Test that load_input raises FileNotFoundError if file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Asset file not found"):
            filesystem_io_manager.load_input(input_context)

    def test_handle_output_overwrites_existing_file(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
    ) -> None:
        """Test that handle_output overwrites existing files."""
        # Write first version
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Write second version
        updated_data = {**sample_data, "version": 2}
        filesystem_io_manager.handle_output(output_context, updated_data)

        # Load and verify it's the updated version
        loaded_data = filesystem_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )
        assert loaded_data["version"] == 2

    def test_filesystem_io_manager_without_create_dirs(
        self,
        temp_data_dir: Path,
        sample_data: dict[str, Any],
    ) -> None:
        """Test FilesystemIOManager with create_dirs=False."""
        io_manager = FilesystemIOManager(base_path=str(temp_data_dir), create_dirs=False)

        # Should fail for nested paths
        context = build_output_context(
            asset_key=AssetKey(["extract", "binance", "ohlcv"])
        )

        with pytest.raises(FileNotFoundError):
            io_manager.handle_output(context, sample_data)

    def test_handle_output_with_special_characters_in_asset_key(
        self,
        filesystem_io_manager: FilesystemIOManager,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
    ) -> None:
        """Test handle_output with special characters in asset key."""
        context = build_output_context(asset_key=AssetKey(["extract_btc_usdt"]))
        filesystem_io_manager.handle_output(context, sample_data)

        expected_file = temp_data_dir / "extract_btc_usdt.json"
        assert expected_file.exists()
