"""Tests for SQLIOManager."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from dagster import (
    AssetKey,
    InputContext,
    OutputContext,
    build_input_context,
    build_output_context,
)

from dagster_crypto_data.defs.io_managers import SQLIOManager


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create temporary database path."""
    return tmp_path / "test"


@pytest.fixture
def sqlite_io_manager(
    temp_db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> SQLIOManager:
    """Create SQLIOManager instance for SQLite."""
    # Change to temp directory so relative path works
    monkeypatch.chdir(temp_db_path.parent)
    return SQLIOManager(
        db_type="sqlite",
        db_name="test",  # Use relative path
    )


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Sample Polars DataFrame for testing."""
    return pl.DataFrame(
        {
            "timestamp": [1735689600000, 1735693200000],
            "open": [50000.0, 50500.0],
            "high": [51000.0, 51500.0],
            "low": [49000.0, 49500.0],
            "close": [50500.0, 51000.0],
            "volume": [100.5, 120.3],
            "exchange": ["binance", "binance"],
            "symbol": ["BTC/USDT", "BTC/USDT"],
        }
    )


@pytest.fixture
def output_context() -> OutputContext:
    """Create output context for testing."""
    return build_output_context(asset_key=AssetKey(["transform_ohlcv"]))


@pytest.fixture
def input_context() -> InputContext:
    """Create input context for testing."""
    return build_input_context(asset_key=AssetKey(["transform_ohlcv"]))


class TestSQLIOManagerSQLite:
    """Test SQLIOManager with SQLite."""

    def test_get_table_name(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test table name generation."""
        table_name = sqlite_io_manager._get_table_name(output_context)
        assert table_name == "transform_ohlcv"

    def test_get_table_name_with_nested_asset(
        self,
        sqlite_io_manager: SQLIOManager,
    ) -> None:
        """Test table name generation with nested asset key."""
        context = build_output_context(
            asset_key=AssetKey(["transform", "binance", "ohlcv"])
        )
        table_name = sqlite_io_manager._get_table_name(context)
        assert table_name == "transform_binance_ohlcv"

    def test_handle_output_creates_table(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
        sample_dataframe: pl.DataFrame,
        temp_db_path: Path,
    ) -> None:
        """Test that handle_output creates a SQL table."""
        sqlite_io_manager.handle_output(output_context, sample_dataframe)

        # Verify database file was created
        db_file = Path(str(temp_db_path) + ".db")
        assert db_file.exists()

    def test_handle_output_with_invalid_type_raises_error(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-DataFrame."""
        with pytest.raises(TypeError, match="SQLIOManager expects a DataFrame"):
            sqlite_io_manager.handle_output(output_context, {"not": "a dataframe"})  # type: ignore

    def test_handle_output_appends_to_existing_table(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test that handle_output appends to existing tables instead of replacing."""
        # Write first batch
        sqlite_io_manager.handle_output(output_context, sample_dataframe)

        # Write second batch with same schema
        second_batch = sample_dataframe.with_columns(
            (pl.col("timestamp") + 1000).alias("timestamp")
        )
        sqlite_io_manager.handle_output(output_context, second_batch)

        # Load and verify both batches are present
        loaded_df = sqlite_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )
        # Should have rows from both batches (doubled)
        assert len(loaded_df) == len(sample_dataframe) * 2

    def test_load_input_reads_table(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
        input_context: InputContext,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test that load_input reads the SQL table."""
        # First, write the table
        sqlite_io_manager.handle_output(output_context, sample_dataframe)

        # Then, read it back
        loaded_df = sqlite_io_manager.load_input(input_context)

        assert loaded_df.shape == sample_dataframe.shape
        assert loaded_df.columns == sample_dataframe.columns

    def test_load_input_table_not_found_raises_error(
        self,
        sqlite_io_manager: SQLIOManager,
        input_context: InputContext,
        temp_db_path: Path,
    ) -> None:
        """Test that load_input raises ValueError if table doesn't exist."""
        # Create empty database
        db_file = Path(str(temp_db_path) + ".db")
        db_file.touch()

        with pytest.raises(ValueError, match="Failed to load table"):
            sqlite_io_manager.load_input(input_context)

    def test_handle_output_with_empty_dataframe(
        self,
        sqlite_io_manager: SQLIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test handle_output with empty DataFrame."""
        empty_df = pl.DataFrame(
            {
                "timestamp": pl.Series([], dtype=pl.Int64),
                "value": pl.Series([], dtype=pl.Float64),
            }
        )

        sqlite_io_manager.handle_output(output_context, empty_df)

        # Load and verify
        loaded_df = sqlite_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )
        assert len(loaded_df) == 0
        assert loaded_df.columns == empty_df.columns


class TestSQLIOManagerPostgreSQL:
    """Test SQLIOManager with PostgreSQL configuration."""

    def test_postgresql_configuration(self) -> None:
        """Test PostgreSQL IO manager configuration."""
        io_manager = SQLIOManager(
            db_type="postgresql",
            host="localhost",
            port=5432,
            db_name="test_db",
            username="test_user",
            password="test_password",
            db_schema="analytics",
        )

        assert io_manager.db_type == "postgresql"
        assert io_manager.host == "localhost"
        assert io_manager.port == 5432
        assert io_manager.db_name == "test_db"
        assert io_manager.username == "test_user"
        assert io_manager.db_schema == "analytics"

    def test_postgresql_requires_credentials(self) -> None:
        """Test that PostgreSQL requires host, port, username, and password."""
        io_manager = SQLIOManager(
            db_type="postgresql",
            host="localhost",
            port=5432,
            db_name="test_db",
            username="",  # Missing username
            password=None,  # Missing password
        )

        with pytest.raises(ValueError, match="PostgreSQL requires"):
            io_manager._get_db_manager()

    def test_sqlite_does_not_require_credentials(
        self,
        temp_db_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that SQLite doesn't require credentials."""
        # Change to temp directory so relative path works
        monkeypatch.chdir(temp_db_path.parent)
        io_manager = SQLIOManager(
            db_type="sqlite",
            db_name="test",  # Use relative path
        )

        # Should not raise error
        db_manager = io_manager._get_db_manager()
        assert db_manager.db_type == "sqlite"

    def test_mysql_configuration(self) -> None:
        """Test MySQL IO manager configuration (now supported)."""
        io_manager = SQLIOManager(
            db_type="mysql",
            host="localhost",
            port=3306,
            db_name="test_db",
            username="test_user",
            password="test_password",
            db_schema="analytics",
        )

        assert io_manager.db_type == "mysql"
        assert io_manager.host == "localhost"
        assert io_manager.port == 3306
        assert io_manager.db_name == "test_db"
        assert io_manager.username == "test_user"
        assert io_manager.db_schema == "analytics"
