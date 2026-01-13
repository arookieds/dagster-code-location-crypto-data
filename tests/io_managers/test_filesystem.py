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

import dagster_crypto_data.defs.io_managers.filesystem as fs_module
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
    """Sample data for testing - matches extract asset output format."""
    return {
        "data": {
            "BTC/USDT": {
                "timestamp": 1735689600000,
                "last": 50000.0,
                "bid": 49000.0,
                "ask": 51000.0,
                "high": 51000.0,
                "low": 49000.0,
                "volume": 100.5,
            },
            "ETH/USDT": {
                "timestamp": 1735693200000,
                "last": 2500.0,
                "bid": 2450.0,
                "ask": 2550.0,
                "high": 2550.0,
                "low": 2450.0,
                "volume": 200.3,
            },
        },
        "metadata": {
            "exchange_id": "binance",
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that handle_output creates a JSON file."""
        timestamp = "123456789"
        monkeypatch.setattr(
            fs_module, "get_run_info", lambda ctx: {"timestamp": timestamp}
        )
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Check file exists - new path format: extract/binance/ohlcv_123456789.json
        expected_file = temp_data_dir / "extract" / "binance" / f"ohlcv_{timestamp}.json"
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that handle_output creates nested directories."""
        timestamp = "123456789"
        monkeypatch.setattr(
            fs_module, "get_run_info", lambda ctx: {"timestamp": timestamp}
        )
        context = build_output_context(
            asset_key=AssetKey(["extract", "binance", "ohlcv"])
        )
        filesystem_io_manager.handle_output(context, sample_data)

        # Check nested file exists
        expected_file = temp_data_dir / "extract" / "binance" / f"ohlcv_{timestamp}.json"
        assert expected_file.exists()

        # Check file content
        with open(expected_file) as f:
            loaded_data = json.load(f)
        assert loaded_data == sample_data

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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input reads the JSON file in single-file mode."""
        monkeypatch.setattr(
            fs_module,
            "get_run_info",
            lambda ctx: {
                "run_id": "test-run-123",
                "timestamp": "123456789",
                "dt": "25-01-12 12:00:00.000000",
            },
        )
        # Mock _has_upstream_output to simulate single-file mode (with upstream)
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: True
        )
        # First, write the file
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Then, read it back
        loaded_data = filesystem_io_manager.load_input(input_context)
        assert loaded_data == sample_data

    def test_load_input_file_not_found_raises_error(
        self,
        filesystem_io_manager: FilesystemIOManager,
        input_context: InputContext,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that load_input raises FileNotFoundError if file doesn't exist in single-file mode."""
        monkeypatch.setattr(
            fs_module,
            "get_run_info",
            lambda ctx: {
                "run_id": "test-run-123",
                "timestamp": "123456789",
                "dt": "25-01-12 12:00:00.000000",
            },
        )
        # Mock _has_upstream_output to simulate single-file mode (with upstream)
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: True
        )
        with pytest.raises(FileNotFoundError, match="File not found"):
            filesystem_io_manager.load_input(input_context)

    def test_handle_output_overwrites_existing_file(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that handle_output overwrites existing files in single-file mode."""
        monkeypatch.setattr(
            fs_module,
            "get_run_info",
            lambda ctx: {
                "run_id": "test-run-123",
                "timestamp": "123456789",
                "dt": "25-01-12 12:00:00.000000",
            },
        )
        # Mock _has_upstream_output to simulate single-file mode (with upstream)
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: True
        )
        # Write first version
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Write second version with updated data
        updated_data = sample_data.copy()
        updated_data["metadata"] = {**sample_data["metadata"], "version": 2}
        filesystem_io_manager.handle_output(output_context, updated_data)

        # Load and verify it's the updated version
        loaded_data = filesystem_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )
        assert loaded_data["metadata"]["version"] == 2

    def test_filesystem_io_manager_without_create_dirs(
        self,
        temp_data_dir: Path,
        sample_data: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test FilesystemIOManager with create_dirs=False."""
        monkeypatch.setattr(
            fs_module, "get_run_info", lambda ctx: {"timestamp": "123456789"}
        )
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
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test handle_output with special characters in asset key."""
        timestamp = "123456789"
        monkeypatch.setattr(
            fs_module, "get_run_info", lambda ctx: {"timestamp": timestamp}
        )
        context = build_output_context(asset_key=AssetKey(["extract_btc_usdt"]))
        filesystem_io_manager.handle_output(context, sample_data)

        # New path format: extract/btc/usdt_123456789.json
        expected_file = temp_data_dir / "extract" / "btc" / f"usdt_{timestamp}.json"
        assert expected_file.exists()

    def test_load_input_multi_file_mode_loads_all_files(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test load_input in multi-file mode loads all unloaded files."""
        # Write multiple files with different timestamps
        for i, timestamp in enumerate(["100000000", "200000000", "300000000"]):
            monkeypatch.setattr(
                fs_module,
                "get_run_info",
                lambda ctx, ts=timestamp, run=i: {
                    "run_id": f"test-run-{run}",
                    "timestamp": ts,
                    "dt": "25-01-12 12:00:00.000000",
                },
            )
            filesystem_io_manager.handle_output(output_context, sample_data)

        # Mock _has_upstream_output to return False (standalone mode)
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: False
        )

        # Load input in multi-file mode
        loaded_data = filesystem_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )

        # Should have data from multiple files
        assert "data" in loaded_data
        assert len(loaded_data["data"]) > 0
        assert "metadata" in loaded_data

    def test_database_field_is_optional(
        self,
        temp_data_dir: Path,
    ) -> None:
        """Test that database field is optional for local filesystem mode."""
        # Create manager without database field
        io_manager = FilesystemIOManager(base_path=str(temp_data_dir))

        # Should not raise error
        assert io_manager.database is None

    def test_load_input_without_upstream_and_no_files_returns_empty(
        self,
        filesystem_io_manager: FilesystemIOManager,
        input_context: InputContext,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test load_input returns empty dict when no files found in standalone mode."""
        # Mock _has_upstream_output to return False
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: False
        )

        # Load when no files exist
        loaded_data = filesystem_io_manager.load_input(input_context)

        assert loaded_data == {"metadata": {}, "data": []}

    def test_list_unloaded_files_without_database_uses_metadata(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test _list_unloaded_files uses metadata when no database provided."""
        # Write a file
        timestamp = "123456789"
        monkeypatch.setattr(
            fs_module, "get_run_info", lambda ctx: {"timestamp": timestamp}
        )
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Get the file path
        asset_path = "/".join(output_context.asset_key.path).replace("_", "/")
        files = filesystem_io_manager._list_unloaded_files(
            build_input_context(asset_key=output_context.asset_key),
            asset_path,
            model=None,  # No model
        )

        # Should find the file (has metadata with loaded=false)
        assert len(files) > 0

    def test_metadata_file_marks_loaded(
        self,
        filesystem_io_manager: FilesystemIOManager,
        output_context: OutputContext,
        sample_data: dict[str, Any],
        temp_data_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that files are marked as loaded after reading in multi-file mode."""
        timestamp = "123456789"
        monkeypatch.setattr(
            fs_module,
            "get_run_info",
            lambda ctx: {
                "run_id": "test-run",
                "timestamp": timestamp,
                "dt": "25-01-12 12:00:00.000000",
            },
        )
        filesystem_io_manager.handle_output(output_context, sample_data)

        # Mock to get file path and verify metadata
        expected_file = temp_data_dir / "extract" / "binance" / f"ohlcv_{timestamp}.json"
        metadata_file = expected_file.with_suffix(".meta.json")

        # Before load - should be marked as not loaded
        import json

        with open(metadata_file) as f:
            meta = json.load(f)
        assert meta.get("loaded") == "false"

        # Load the file in multi-file mode
        monkeypatch.setattr(
            filesystem_io_manager, "_has_upstream_output", lambda ctx: False
        )
        filesystem_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )

        # After load - should be marked as loaded
        with open(metadata_file) as f:
            meta = json.load(f)
        assert meta.get("loaded") == "true"
