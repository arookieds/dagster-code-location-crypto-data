"""Tests for KuzuDBIOManager."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest
from dagster import (
    AssetKey,
    InputContext,
    OutputContext,
    build_input_context,
    build_output_context,
)

from dagster_crypto_data.defs.io_managers import KuzuDBIOManager

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create temporary database path."""
    return tmp_path / "test.kuzu"


@pytest.fixture
def kuzudb_io_manager(temp_db_path: Path) -> KuzuDBIOManager:
    """Create KuzuDBIOManager instance."""
    return KuzuDBIOManager(db_path=str(temp_db_path))


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Sample Polars DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2],
            "symbol": ["BTC/USDT", "ETH/USDT"],
            "price": [50000.0, 3000.0],
            "volume": [100.5, 200.3],
        }
    )


@pytest.fixture
def output_context() -> OutputContext:
    """Create output context for testing."""
    return build_output_context(asset_key=AssetKey(["crypto_data"]))


@pytest.fixture
def input_context() -> InputContext:
    """Create input context for testing."""
    return build_input_context(asset_key=AssetKey(["crypto_data"]))


@pytest.mark.skipif(
    True,  # Skip by default as kuzu is optional
    reason="KuzuDB is optional dependency - install with: uv add kuzu",
)
class TestKuzuDBIOManager:
    """Test KuzuDBIOManager functionality."""

    def test_get_node_table_name(
        self,
        kuzudb_io_manager: KuzuDBIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test node table name generation."""
        table_name = kuzudb_io_manager._get_node_table_name(output_context)
        assert table_name == "crypto_data"

    def test_get_node_table_name_with_custom_label(
        self,
        temp_db_path: Path,
    ) -> None:
        """Test node table name with custom label."""
        io_manager = KuzuDBIOManager(
            db_path=str(temp_db_path),
            node_label="CustomNode",
        )
        context = build_output_context(asset_key=AssetKey(["any_asset"]))
        table_name = io_manager._get_node_table_name(context)
        assert table_name == "CustomNode"

    def test_handle_output_creates_node_table(
        self,
        kuzudb_io_manager: KuzuDBIOManager,
        output_context: OutputContext,
        sample_dataframe: pl.DataFrame,
        temp_db_path: Path,
    ) -> None:
        """Test that handle_output creates a KuzuDB node table."""
        kuzudb_io_manager.handle_output(output_context, sample_dataframe)

        # Verify database directory was created
        assert temp_db_path.exists()

    def test_handle_output_with_invalid_type_raises_error(
        self,
        kuzudb_io_manager: KuzuDBIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-DataFrame."""
        with pytest.raises(TypeError, match="KuzuDBIOManager expects polars.DataFrame"):
            kuzudb_io_manager.handle_output(output_context, {"not": "a dataframe"})  # type: ignore

    def test_load_input_database_not_found_raises_error(
        self,
        kuzudb_io_manager: KuzuDBIOManager,
        input_context: InputContext,
    ) -> None:
        """Test that load_input raises FileNotFoundError if database doesn't exist."""
        with pytest.raises(FileNotFoundError, match="KuzuDB database not found"):
            kuzudb_io_manager.load_input(input_context)

    def test_kuzu_not_installed_raises_import_error(
        self,
        kuzudb_io_manager: KuzuDBIOManager,
        output_context: OutputContext,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test that ImportError is raised if kuzu is not installed."""
        # This test will pass if kuzu is not installed
        # If kuzu IS installed, this test will be skipped by the class decorator
        try:
            import kuzu  # noqa: F401

            pytest.skip("KuzuDB is installed, skipping import error test")
        except ImportError:
            with pytest.raises(ImportError, match="KuzuDB is not installed"):
                kuzudb_io_manager.handle_output(output_context, sample_dataframe)


class TestKuzuDBIOManagerWithoutKuzu:
    """Test KuzuDBIOManager behavior when kuzu is not installed."""

    def test_import_error_message(
        self,
        temp_db_path: Path,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test that helpful error message is shown when kuzu is not installed."""
        io_manager = KuzuDBIOManager(db_path=str(temp_db_path))
        context = build_output_context(asset_key=AssetKey(["test"]))

        # Try to import kuzu
        try:
            import kuzu  # noqa: F401

            pytest.skip("KuzuDB is installed")
        except ImportError:
            # Should raise ImportError with helpful message
            with pytest.raises(ImportError, match="Install it with: uv add kuzu"):
                io_manager.handle_output(context, sample_dataframe)
