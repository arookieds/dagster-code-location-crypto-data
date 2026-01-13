"""Tests for filesystems utility functions."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock

import pytest
from dagster import InputContext, build_input_context

from dagster_crypto_data.defs.resources.database import DatabaseConfig
from dagster_crypto_data.defs.utils.filesystems import (
    _get_db_ts_from_config,
    _get_duckdb_ts,
    get_max_extraction_timestamp,
)


@pytest.fixture
def mock_logger() -> MagicMock:
    """Create mock logger."""
    return MagicMock()


@pytest.fixture
def mock_model() -> Mock:
    """Create mock CryptoModel."""
    model = Mock()
    model.__tablename__ = "tickers"
    model.__table_args__ = {"schema": "crypto_data"}
    return model


@pytest.fixture
def input_context() -> InputContext:
    """Create input context for testing."""
    return build_input_context()


@pytest.fixture
def duckdb_database_config(tmp_path) -> DatabaseConfig:
    """Create DuckDB DatabaseConfig for testing."""
    db_file = tmp_path / "test.duckdb"
    return DatabaseConfig(
        db_type="duckdb",
        host="",
        port=0,
        db_name=str(db_file),
        username="",
        password="",
        db_schema="crypto_data",
    )


class TestGetMaxExtractionTimestamp:
    """Test get_max_extraction_timestamp function."""

    def test_returns_timestamps_and_not_empty_flag(
        self,
        input_context: InputContext,
        mock_model: Mock,
        duckdb_database_config: DatabaseConfig,
        tmp_path,
    ) -> None:
        """Test get_max_extraction_timestamp returns timestamps and not_empty flag."""
        import duckdb

        # Create test database with data
        db_file = tmp_path / "test.duckdb"
        model = Mock()
        model.__tablename__ = "tickers"
        model.__table_args__ = {"schema": "crypto_data"}

        # Create database and table with sample data
        with duckdb.connect(str(db_file)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS crypto_data")
            conn.execute("""
                CREATE TABLE crypto_data.tickers (
                    extraction_timestamp BIGINT,
                    exchange_id VARCHAR,
                    symbol VARCHAR
                )
            """)
            conn.execute(
                """
                INSERT INTO crypto_data.tickers VALUES
                (1000, 'binance', 'BTC/USDT'),
                (1000, 'binance', 'ETH/USDT'),
                (2000, 'binance', 'BTC/USDT')
            """
            )

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result, is_empty = get_max_extraction_timestamp(
            input_context, db_config, "binance", model
        )

        assert sorted(result) == [1000.0, 2000.0]
        assert is_empty is False

    def test_returns_empty_list_when_duckdb_file_not_found(
        self,
        input_context: InputContext,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test get_max_extraction_timestamp returns empty list when DuckDB file not found."""
        # Use non-existent database file
        db_file = tmp_path / "nonexistent.duckdb"

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result, is_empty = get_max_extraction_timestamp(
            input_context, db_config, "binance", mock_model
        )

        assert result == []
        assert is_empty is True

    def test_raises_type_error_for_invalid_database_type(
        self,
        input_context: InputContext,
        mock_model: Mock,
    ) -> None:
        """Test get_max_extraction_timestamp raises TypeError for invalid database type."""
        db_config = DatabaseConfig(
            db_type="invalid_type",  # type: ignore
            host="localhost",
            port=5432,
            db_name="crypto_db",
            username="user",
            password="pass",
            db_schema="public",
        )

        with pytest.raises(
            TypeError, match="database should be DatabaseConfig or DuckDBIOManager"
        ):
            get_max_extraction_timestamp(input_context, db_config, "binance", mock_model)


class TestGetDuckdbTs:
    """Test _get_duckdb_ts private function."""

    def test_returns_timestamps_when_table_exists_with_data(
        self,
        mock_logger: MagicMock,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test _get_duckdb_ts returns timestamps when table exists with data."""
        import duckdb

        db_file = tmp_path / "test.duckdb"

        # Create test database with data
        with duckdb.connect(str(db_file)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS crypto_data")
            conn.execute("""
                CREATE TABLE crypto_data.tickers (
                    extraction_timestamp BIGINT,
                    exchange_id VARCHAR
                )
            """)
            conn.execute(
                """
                INSERT INTO crypto_data.tickers VALUES
                (1735689600000, 'binance'),
                (1735693200000, 'binance'),
                (1735689600000, 'bybit')
            """
            )

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result = _get_duckdb_ts(mock_logger, db_config, "binance", mock_model)

        assert sorted(result) == [1735689600000, 1735693200000]

    def test_returns_empty_list_when_table_does_not_exist(
        self,
        mock_logger: MagicMock,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test _get_duckdb_ts returns empty list when table does not exist."""
        import duckdb

        db_file = tmp_path / "test.duckdb"

        # Create empty database
        with duckdb.connect(str(db_file)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS crypto_data")

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result = _get_duckdb_ts(mock_logger, db_config, "binance", mock_model)

        assert result == []

    def test_returns_empty_list_when_duckdb_file_not_found(
        self,
        mock_logger: MagicMock,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test _get_duckdb_ts returns empty list when DuckDB file not found."""
        db_file = tmp_path / "nonexistent.duckdb"

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result = _get_duckdb_ts(mock_logger, db_config, "binance", mock_model)

        assert result == []
        # Verify logger was called with appropriate message
        assert any(
            "does not exist" in str(call) for call in mock_logger.info.call_args_list
        )

    def test_returns_empty_list_when_table_exists_but_empty(
        self,
        mock_logger: MagicMock,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test _get_duckdb_ts returns empty list when table exists but has no data."""
        import duckdb

        db_file = tmp_path / "test.duckdb"

        # Create empty table
        with duckdb.connect(str(db_file)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS crypto_data")
            conn.execute("""
                CREATE TABLE crypto_data.tickers (
                    extraction_timestamp BIGINT,
                    exchange_id VARCHAR
                )
            """)

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        result = _get_duckdb_ts(mock_logger, db_config, "binance", mock_model)

        assert result == []

    def test_filters_by_exchange_id(
        self,
        mock_logger: MagicMock,
        mock_model: Mock,
        tmp_path,
    ) -> None:
        """Test _get_duckdb_ts filters results by exchange_id."""
        import duckdb

        db_file = tmp_path / "test.duckdb"

        with duckdb.connect(str(db_file)) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS crypto_data")
            conn.execute("""
                CREATE TABLE crypto_data.tickers (
                    extraction_timestamp BIGINT,
                    exchange_id VARCHAR
                )
            """)
            conn.execute(
                """
                INSERT INTO crypto_data.tickers VALUES
                (1000, 'binance'),
                (1000, 'binance'),
                (2000, 'bybit'),
                (2000, 'bybit')
            """
            )

        db_config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="crypto_data",
        )

        binance_result = _get_duckdb_ts(mock_logger, db_config, "binance", mock_model)
        bybit_result = _get_duckdb_ts(mock_logger, db_config, "bybit", mock_model)

        assert binance_result == [1000]
        assert bybit_result == [2000]


class TestGetDbTsFromConfig:
    """Test _get_db_ts_from_config private function."""

    def test_returns_timestamps_from_sqlite(
        self,
        input_context: InputContext,
        mock_model: Mock,
    ) -> None:
        """Test _get_db_ts_from_config returns timestamps from SQLite."""
        # Mock the database manager and engine
        mock_db_manager = MagicMock()
        mock_engine = MagicMock()
        mock_db_manager.engine = mock_engine

        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1735689600000,), (1735693200000,)]
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_connection
        )
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

        db_config = MagicMock(spec=DatabaseConfig)
        db_config.get_db_manager.return_value = mock_db_manager

        mock_model.__table_args__ = {}  # SQLite doesn't use schema

        result = _get_db_ts_from_config(input_context, db_config, "binance", mock_model)

        assert len(result) == 2
        assert result == [1735689600000.0, 1735693200000.0]

    def test_returns_empty_list_when_table_not_found(
        self,
        input_context: InputContext,
        mock_model: Mock,
        tmp_path,
        monkeypatch,
    ) -> None:
        """Test _get_db_ts_from_config returns empty list when table not found."""
        # Use relative path for SQLite
        monkeypatch.chdir(tmp_path)
        db_file = "test.db"

        db_config = DatabaseConfig(
            db_type="sqlite",
            host="",
            port=0,
            db_name=db_file,
            username="",
            password="",
            db_schema="main",
        )

        result = _get_db_ts_from_config(input_context, db_config, "binance", mock_model)

        assert result == []

    def test_handles_schema_correctly(
        self,
        input_context: InputContext,
        mock_model: Mock,
    ) -> None:
        """Test _get_db_ts_from_config handles schema parameter correctly."""
        # Mock the database manager and engine
        mock_db_manager = MagicMock()
        mock_engine = MagicMock()
        mock_db_manager.engine = mock_engine

        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1000,), (2000,)]
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(
            return_value=mock_connection
        )
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=None)

        db_config = MagicMock(spec=DatabaseConfig)
        db_config.get_db_manager.return_value = mock_db_manager

        result = _get_db_ts_from_config(input_context, db_config, "binance", mock_model)

        assert result == [1000, 2000]

    def test_handles_missing_table_args(
        self,
        input_context: InputContext,
        tmp_path,
    ) -> None:
        """Test _get_db_ts_from_config handles models without __table_args__."""
        db_file = tmp_path / "test.db"

        model = Mock(spec=["__tablename__"])
        model.__tablename__ = "tickers"

        db_config = DatabaseConfig(
            db_type="sqlite",
            host="",
            port=0,
            db_name=str(db_file),
            username="",
            password="",
            db_schema="main",
        )

        result = _get_db_ts_from_config(input_context, db_config, "binance", model)

        # Should return empty list without error
        assert result == []
