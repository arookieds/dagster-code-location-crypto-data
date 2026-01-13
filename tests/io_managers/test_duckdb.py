"""Tests for DuckDBIOManager."""

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

from dagster_crypto_data.defs.io_managers import DuckDBIOManager

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create temporary database path."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def duckdb_io_manager(temp_db_path: Path) -> DuckDBIOManager:
    """Create DuckDBIOManager instance."""
    return DuckDBIOManager(db_path=str(temp_db_path), db_schema="main")


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


class TestDuckDBIOManager:
    """Test DuckDBIOManager functionality."""

    def test_get_table_name(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test table name generation."""
        table_name = duckdb_io_manager._get_table_name(output_context)
        assert table_name == "transform_ohlcv"

    def test_get_table_name_with_nested_asset(
        self,
        duckdb_io_manager: DuckDBIOManager,
    ) -> None:
        """Test table name generation with nested asset key."""
        context = build_output_context(
            asset_key=AssetKey(["transform", "binance", "ohlcv"])
        )
        table_name = duckdb_io_manager._get_table_name(context)
        assert table_name == "transform_binance_ohlcv"

    def test_handle_output_creates_table(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
        sample_dataframe: pl.DataFrame,
        temp_db_path: Path,
    ) -> None:
        """Test that handle_output creates a DuckDB table."""
        duckdb_io_manager.handle_output(output_context, sample_dataframe)

        # Verify database file was created
        assert temp_db_path.exists()

        # Verify table exists and has correct data using native DuckDB
        import duckdb

        conn = duckdb.connect(str(temp_db_path), read_only=True)
        try:
            result = conn.execute(
                "SELECT * FROM main.transform_ohlcv"
            ).fetch_arrow_table()
            loaded_df = pl.from_arrow(result)
            assert loaded_df.shape == sample_dataframe.shape
            assert loaded_df.columns == sample_dataframe.columns
        finally:
            conn.close()

    def test_handle_output_with_invalid_type_raises_error(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test that handle_output raises TypeError for non-DataFrame."""
        with pytest.raises(TypeError, match="DuckDBIOManager expects a DataFrame"):
            duckdb_io_manager.handle_output(output_context, {"not": "a dataframe"})  # type: ignore

    def test_load_input_reads_table(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
        input_context: InputContext,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test that load_input reads the DuckDB table."""
        # First, write the table
        duckdb_io_manager.handle_output(output_context, sample_dataframe)

        # Then, read it back
        loaded_df = duckdb_io_manager.load_input(input_context)

        assert loaded_df.shape == sample_dataframe.shape
        assert loaded_df.columns == sample_dataframe.columns
        assert (
            loaded_df["timestamp"].to_list() == sample_dataframe["timestamp"].to_list()
        )

    def test_load_input_database_not_found_raises_error(
        self,
        duckdb_io_manager: DuckDBIOManager,
        input_context: InputContext,
    ) -> None:
        """Test that load_input raises FileNotFoundError if database doesn't exist."""
        with pytest.raises(FileNotFoundError, match="DuckDB database not found"):
            duckdb_io_manager.load_input(input_context)

    def test_load_input_table_not_found_raises_error(
        self,
        duckdb_io_manager: DuckDBIOManager,
        input_context: InputContext,
        temp_db_path: Path,
    ) -> None:
        """Test that load_input raises ValueError if table doesn't exist."""
        # Create empty database
        temp_db_path.touch()

        with pytest.raises(ValueError, match="Failed to load table"):
            duckdb_io_manager.load_input(input_context)

    def test_handle_output_with_empty_dataframe(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
    ) -> None:
        """Test handle_output with empty DataFrame."""
        empty_df = pl.DataFrame(
            {
                "timestamp": pl.Series([], dtype=pl.Int64),
                "value": pl.Series([], dtype=pl.Float64),
            }
        )

        duckdb_io_manager.handle_output(output_context, empty_df)

        # Load and verify
        loaded_df = duckdb_io_manager.load_input(
            build_input_context(asset_key=output_context.asset_key)
        )
        assert len(loaded_df) == 0
        assert loaded_df.columns == empty_df.columns

    def test_duckdb_io_manager_with_custom_schema(
        self,
        temp_db_path: Path,
        sample_dataframe: pl.DataFrame,
    ) -> None:
        """Test DuckDBIOManager with custom schema."""
        io_manager = DuckDBIOManager(db_path=str(temp_db_path), db_schema="analytics")
        context = build_output_context(asset_key=AssetKey(["test_table"]))

        io_manager.handle_output(context, sample_dataframe)

        # Verify table exists in custom schema using native DuckDB
        import duckdb

        conn = duckdb.connect(str(temp_db_path), read_only=True)
        try:
            result = conn.execute(
                "SELECT * FROM analytics.test_table"
            ).fetch_arrow_table()
            loaded_df = pl.from_arrow(result)
            assert len(loaded_df) == len(sample_dataframe)
        finally:
            conn.close()

    def test_get_model_from_context_with_model_metadata(
        self,
    ) -> None:
        """Test _get_model_from_context extracts model from metadata."""
        from dagster_crypto_data.defs.models import Ticker

        context = build_output_context(
            asset_key=AssetKey(["test_asset"]),
            definition_metadata={"model": "Ticker"},
        )

        model = DuckDBIOManager._get_model_from_context(context)
        assert model is not None
        assert model is Ticker

    def test_get_model_from_context_returns_none_without_metadata(
        self,
    ) -> None:
        """Test _get_model_from_context returns None without model metadata."""
        context = build_output_context(asset_key=AssetKey(["test_asset"]))

        model = DuckDBIOManager._get_model_from_context(context)
        assert model is None

    def test_get_model_from_context_returns_none_with_invalid_model_name(
        self,
    ) -> None:
        """Test _get_model_from_context returns None with invalid model name."""
        context = build_output_context(
            asset_key=AssetKey(["test_asset"]),
            definition_metadata={"model": "NonExistentModel"},
        )

        model = DuckDBIOManager._get_model_from_context(context)
        assert model is None

    def test_handle_output_with_model_creates_all_columns(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
        temp_db_path: Path,
    ) -> None:
        """Test handle_output with model creates table with all columns defined."""
        from dagster_crypto_data.defs.models import Ticker

        # Create context with model metadata
        context = build_output_context(
            asset_key=AssetKey(["test_asset"]),
            definition_metadata={"model": "Ticker"},
        )

        # Create sample dataframe with only some fields
        sample_df = pl.DataFrame(
            {
                "symbol": ["BTC/USDT", "BTC/USDT"],
                "ticker_timestamp_ms": [1735689600000, 1735693200000],
                "last": [50000.0, 50500.0],
            }
        )

        duckdb_io_manager.handle_output(context, sample_df)

        # Verify all model columns exist in table
        import duckdb

        conn = duckdb.connect(str(temp_db_path), read_only=True)
        try:
            # Use Ticker's table name, not the asset key
            table_name = Ticker.__tablename__
            result = conn.execute(
                f"SELECT * FROM {duckdb_io_manager.db_schema}.{table_name} LIMIT 0"
            ).description
            table_columns = [col[0] for col in result]

            # Check that model fields are in the table
            # Ticker model should have these fields defined
            assert "symbol" in table_columns
            assert "ticker_timestamp_ms" in table_columns
            assert "last" in table_columns
            # Verify it has all columns from the model
            assert len(table_columns) >= 10  # Ticker model has many fields
        finally:
            conn.close()

    def test_handle_output_filters_dataframe_columns(
        self,
        duckdb_io_manager: DuckDBIOManager,
        temp_db_path: Path,
    ) -> None:
        """Test handle_output filters DataFrame columns to match table schema."""
        import duckdb

        # Create a table with specific columns
        conn = duckdb.connect(str(temp_db_path))
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {duckdb_io_manager.db_schema}")
            conn.execute(
                f"""
                CREATE TABLE {duckdb_io_manager.db_schema}.test_table (
                    symbol VARCHAR,
                    price DOUBLE,
                    timestamp BIGINT
                )
            """
            )
        finally:
            conn.close()

        # Create DataFrame with extra columns
        context = build_output_context(asset_key=AssetKey(["test_table"]))
        df = pl.DataFrame(
            {
                "symbol": ["BTC/USDT", "ETH/USDT"],
                "price": [50000.0, 2500.0],
                "timestamp": [1735689600000, 1735693200000],
                "extra_field": ["extra1", "extra2"],  # Extra column not in table
            }
        )

        # This should not raise an error, filtering out extra_field
        duckdb_io_manager.handle_output(context, df)

        # Verify data was inserted correctly
        conn = duckdb.connect(str(temp_db_path), read_only=True)
        try:
            result = conn.execute(
                f"SELECT * FROM {duckdb_io_manager.db_schema}.test_table"
            ).fetch_arrow_table()
            loaded_df = pl.from_arrow(result)
            assert len(loaded_df) == 2
            assert list(loaded_df.columns) == ["symbol", "price", "timestamp"]
        finally:
            conn.close()

    def test_create_table_from_model_with_various_types(
        self,
        duckdb_io_manager: DuckDBIOManager,
        output_context: OutputContext,
        temp_db_path: Path,
    ) -> None:
        """Test _create_table_from_model correctly maps Python types to SQL types."""
        import duckdb

        from dagster_crypto_data.defs.models import Ticker

        conn = duckdb.connect(str(temp_db_path))
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {duckdb_io_manager.db_schema}")

            # Create table from Ticker model (has various field types)
            duckdb_io_manager._create_table_from_model(
                output_context, Ticker, conn, Ticker.__tablename__
            )

            # Verify table was created
            exists = conn.execute(
                f"""
                SELECT count(*) > 0 FROM duckdb_tables
                WHERE table_name = '{Ticker.__tablename__}'
                AND schema_name = '{duckdb_io_manager.db_schema}'
            """
            ).fetchone()[0]

            assert exists is True

            # Verify column types
            result = conn.execute(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{duckdb_io_manager.db_schema}'
                AND table_name = '{Ticker.__tablename__}'
            """
            ).fetchall()

            columns_dict = dict(result)

            # Verify Ticker model columns exist and have correct types
            assert "symbol" in columns_dict
            assert "last" in columns_dict
            assert columns_dict["last"] == "DOUBLE"  # float -> DOUBLE
            assert columns_dict["symbol"] == "VARCHAR"  # str -> VARCHAR
        finally:
            conn.close()
