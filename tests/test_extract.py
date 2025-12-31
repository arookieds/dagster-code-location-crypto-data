from __future__ import annotations

from unittest.mock import MagicMock

import ccxt
import pytest
from dagster import materialize
from pydantic import ValidationError

from dagster_crypto_data.assets.extract import (
    ExtractAssetConfig,
    extract_asset_factory,
)


class TestExtractAssetConfig:
    """Test suite for ExtractAssetConfig Pydantic model."""

    def test_valid_config(self) -> None:
        """Test valid configuration passes validation."""
        config = ExtractAssetConfig(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )
        assert config.asset_name == "binance_tickers"
        assert config.group_name == "raw_data"
        assert config.exchange_id == "binance"
        assert config.io_manager_key == "io_manager"

    def test_valid_config_with_custom_io_manager(self) -> None:
        """Test valid configuration with custom IO manager key."""
        config = ExtractAssetConfig(
            asset_name="bybit_tickers",
            group_name="raw_data",
            exchange_id="bybit",
            io_manager_key="custom_io_manager",
        )
        assert config.io_manager_key == "custom_io_manager"

    def test_invalid_exchange_id(self) -> None:
        """Test invalid exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            ExtractAssetConfig(
                asset_name="test_asset",
                group_name="test_group",
                exchange_id="invalid_exchange_that_does_not_exist",
            )

    def test_invalid_asset_name_uppercase(self) -> None:
        """Test asset name with uppercase letters raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            ExtractAssetConfig(
                asset_name="InvalidName",
                group_name="test_group",
                exchange_id="binance",
            )

    def test_invalid_asset_name_starts_with_number(self) -> None:
        """Test asset name starting with number raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            ExtractAssetConfig(
                asset_name="1_invalid_name",
                group_name="test_group",
                exchange_id="binance",
            )

    def test_invalid_asset_name_special_characters(self) -> None:
        """Test asset name with special characters raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            ExtractAssetConfig(
                asset_name="invalid-name",
                group_name="test_group",
                exchange_id="binance",
            )

    def test_empty_asset_name(self) -> None:
        """Test empty asset name raises ValidationError."""
        with pytest.raises(
            ValidationError, match="String should have at least 1 character"
        ):
            ExtractAssetConfig(
                asset_name="",
                group_name="test_group",
                exchange_id="binance",
            )

    def test_empty_group_name(self) -> None:
        """Test empty group name raises ValidationError."""
        with pytest.raises(
            ValidationError, match="String should have at least 1 character"
        ):
            ExtractAssetConfig(
                asset_name="test_asset",
                group_name="",
                exchange_id="binance",
            )

    def test_valid_asset_name_with_underscores(self) -> None:
        """Test valid asset name with underscores."""
        config = ExtractAssetConfig(
            asset_name="binance_spot_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )
        assert config.asset_name == "binance_spot_tickers"

    def test_valid_asset_name_with_numbers(self) -> None:
        """Test valid asset name with numbers (not at start)."""
        config = ExtractAssetConfig(
            asset_name="binance_tickers_v2",
            group_name="raw_data",
            exchange_id="binance",
        )
        assert config.asset_name == "binance_tickers_v2"

    def test_multiple_valid_exchanges(self) -> None:
        """Test multiple valid exchange IDs."""
        exchanges = ["binance", "bybit", "kraken", "coinbase", "okx"]
        for exchange in exchanges:
            config = ExtractAssetConfig(
                asset_name=f"{exchange}_tickers",
                group_name="raw_data",
                exchange_id=exchange,
            )
            assert config.exchange_id == exchange


class TestExtractAssetFactory:
    """Test suite for extract_asset_factory function."""

    def test_factory_creates_asset_definition(self) -> None:
        """Test factory returns an AssetsDefinition."""
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )
        assert asset_def is not None
        assert hasattr(asset_def, "node_def")

    def test_factory_with_invalid_exchange_raises_validation_error(self) -> None:
        """Test factory with invalid exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            extract_asset_factory(
                asset_name="test_asset",
                group_name="test_group",
                exchange_id="invalid_exchange",
            )

    def test_factory_with_invalid_asset_name_raises_validation_error(self) -> None:
        """Test factory with invalid asset name raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            extract_asset_factory(
                asset_name="InvalidName",
                group_name="test_group",
                exchange_id="binance",
            )

    def test_factory_with_custom_io_manager_key(self) -> None:
        """Test factory with custom IO manager key."""
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
            io_manager_key="custom_io_manager",
        )
        assert asset_def is not None

    def test_asset_execution_success(self) -> None:
        """Test successful asset execution with mocked exchange."""

        # Create mock exchange resource
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()
        mock_ticker_data = {
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "last": 50000.0,
                "bid": 49999.0,
                "ask": 50001.0,
            },
            "ETH/USDT": {
                "symbol": "ETH/USDT",
                "last": 3000.0,
                "bid": 2999.0,
                "ask": 3001.0,
            },
            "SOL/USDT": {
                "symbol": "SOL/USDT",
                "last": 100.0,
                "bid": 99.9,
                "ask": 100.1,
            },
            "ADA/USDT": {
                "symbol": "ADA/USDT",
                "last": 0.5,
                "bid": 0.49,
                "ask": 0.51,
            },
            "DOT/USDT": {
                "symbol": "DOT/USDT",
                "last": 7.5,
                "bid": 7.49,
                "ask": 7.51,
            },
        }
        mock_client.fetch_tickers.return_value = mock_ticker_data
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset using materialize
        result = materialize([asset_def], resources={"exchange": mock_exchange_resource})

        # Verify execution was successful
        assert result.success
        materialization = result.asset_materializations_for_node("binance_tickers")[0]

        # Verify metadata exists
        assert "record_count" in materialization.metadata
        assert "extraction_time_seconds" in materialization.metadata
        assert "exchange_id" in materialization.metadata
        assert "sample_records" in materialization.metadata

    def test_asset_execution_with_empty_response(self) -> None:
        """Test asset execution with empty ticker response."""

        # Create mock exchange resource
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()
        mock_client.fetch_tickers.return_value = {}
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset
        result = materialize([asset_def], resources={"exchange": mock_exchange_resource})

        # Verify execution was successful
        assert result.success
        materialization = result.asset_materializations_for_node("binance_tickers")[0]

        # Verify metadata exists
        assert "record_count" in materialization.metadata
        assert "extraction_time_seconds" in materialization.metadata

    def test_asset_execution_network_error(self) -> None:
        """Test asset execution handles NetworkError correctly."""
        # Create mock exchange resource that raises NetworkError
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()
        mock_client.fetch_tickers.side_effect = ccxt.NetworkError("Connection timeout")
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset and expect NetworkError to be raised
        with pytest.raises(ccxt.NetworkError, match="Connection timeout"):
            materialize([asset_def], resources={"exchange": mock_exchange_resource})

    def test_asset_execution_exchange_error(self) -> None:
        """Test asset execution handles ExchangeError correctly."""
        # Create mock exchange resource that raises ExchangeError
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()
        mock_client.fetch_tickers.side_effect = ccxt.ExchangeError(
            "API rate limit exceeded"
        )
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset and expect ExchangeError to be raised
        with pytest.raises(ccxt.ExchangeError, match="API rate limit exceeded"):
            materialize([asset_def], resources={"exchange": mock_exchange_resource})

    def test_asset_execution_unexpected_error(self) -> None:
        """Test asset execution handles unexpected errors correctly."""
        # Create mock exchange resource that raises unexpected error
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()
        mock_client.fetch_tickers.side_effect = RuntimeError("Unexpected error occurred")
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset and expect RuntimeError to be raised
        with pytest.raises(RuntimeError, match="Unexpected error occurred"):
            materialize([asset_def], resources={"exchange": mock_exchange_resource})

    def test_asset_execution_large_dataset(self) -> None:
        """Test asset execution with large dataset (100+ tickers)."""

        # Create mock exchange resource with large dataset
        mock_exchange_resource = MagicMock()
        mock_client = MagicMock()

        # Generate 150 mock tickers
        mock_ticker_data = {
            f"COIN{i}/USDT": {
                "symbol": f"COIN{i}/USDT",
                "last": 100.0 + i,
                "bid": 99.0 + i,
                "ask": 101.0 + i,
            }
            for i in range(150)
        }
        mock_client.fetch_tickers.return_value = mock_ticker_data
        mock_exchange_resource.get_client.return_value = mock_client

        # Create asset
        asset_def = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        # Execute asset
        result = materialize([asset_def], resources={"exchange": mock_exchange_resource})

        # Verify execution was successful
        assert result.success
        materialization = result.asset_materializations_for_node("binance_tickers")[0]

        # Verify metadata exists
        assert "record_count" in materialization.metadata
        assert "extraction_time_seconds" in materialization.metadata
        assert "sample_records" in materialization.metadata

    def test_multiple_assets_from_factory(self) -> None:
        """Test creating multiple assets from the same factory."""
        binance_asset = extract_asset_factory(
            asset_name="binance_tickers",
            group_name="raw_data",
            exchange_id="binance",
        )

        bybit_asset = extract_asset_factory(
            asset_name="bybit_tickers",
            group_name="raw_data",
            exchange_id="bybit",
        )

        kraken_asset = extract_asset_factory(
            asset_name="kraken_tickers",
            group_name="raw_data",
            exchange_id="kraken",
        )

        # Verify all assets are created
        assert binance_asset is not None
        assert bybit_asset is not None
        assert kraken_asset is not None

        # Verify they are different instances
        assert binance_asset != bybit_asset
        assert binance_asset != kraken_asset
        assert bybit_asset != kraken_asset
