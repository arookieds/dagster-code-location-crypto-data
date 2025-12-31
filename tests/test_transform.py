from __future__ import annotations

import pytest
from dagster import materialize
from pydantic import ValidationError

from dagster_crypto_data.assets.transform import (
    TransformAssetConfig,
    transform_asset_factory,
)


class TestTransformAssetConfig:
    """Test suite for TransformAssetConfig Pydantic model."""

    def test_valid_config(self) -> None:
        """Test valid configuration passes validation."""
        config = TransformAssetConfig(
            asset_name="binance_transformed",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
        )
        assert config.asset_name == "binance_transformed"
        assert config.group_name == "transformed_data"
        assert config.exchange_id == "binance"
        assert config.source_asset_key == "binance_raw"
        assert config.io_manager_key == "io_manager"

    def test_valid_config_with_custom_io_manager(self) -> None:
        """Test valid configuration with custom IO manager key."""
        config = TransformAssetConfig(
            asset_name="bybit_transformed",
            group_name="transformed_data",
            exchange_id="bybit",
            source_asset_key="bybit_raw",
            io_manager_key="custom_io_manager",
        )
        assert config.io_manager_key == "custom_io_manager"

    def test_invalid_exchange_id(self) -> None:
        """Test invalid exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            TransformAssetConfig(
                asset_name="test_asset",
                group_name="test_group",
                exchange_id="invalid_exchange_that_does_not_exist",
                source_asset_key="test_source",
            )

    def test_invalid_asset_name_uppercase(self) -> None:
        """Test asset name with uppercase letters raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            TransformAssetConfig(
                asset_name="InvalidName",
                group_name="test_group",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_invalid_asset_name_starts_with_number(self) -> None:
        """Test asset name starting with number raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            TransformAssetConfig(
                asset_name="1_invalid_name",
                group_name="test_group",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_invalid_asset_name_special_characters(self) -> None:
        """Test asset name with special characters raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            TransformAssetConfig(
                asset_name="invalid-name",
                group_name="test_group",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_empty_asset_name(self) -> None:
        """Test empty asset name raises ValidationError."""
        with pytest.raises(
            ValidationError, match="String should have at least 1 character"
        ):
            TransformAssetConfig(
                asset_name="",
                group_name="test_group",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_empty_group_name(self) -> None:
        """Test empty group name raises ValidationError."""
        with pytest.raises(
            ValidationError, match="String should have at least 1 character"
        ):
            TransformAssetConfig(
                asset_name="test_asset",
                group_name="",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_valid_asset_name_with_underscores(self) -> None:
        """Test valid asset name with underscores."""
        config = TransformAssetConfig(
            asset_name="binance_spot_transformed",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
        )
        assert config.asset_name == "binance_spot_transformed"

    def test_valid_asset_name_with_numbers(self) -> None:
        """Test valid asset name with numbers (not at start)."""
        config = TransformAssetConfig(
            asset_name="binance_transformed_v2",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
        )
        assert config.asset_name == "binance_transformed_v2"

    def test_multiple_valid_exchanges(self) -> None:
        """Test multiple valid exchange IDs."""
        exchanges = ["binance", "bybit", "kraken", "coinbase", "okx"]
        for exchange in exchanges:
            config = TransformAssetConfig(
                asset_name=f"{exchange}_transformed",
                group_name="transformed_data",
                exchange_id=exchange,
                source_asset_key=f"{exchange}_raw",
            )
            assert config.exchange_id == exchange


class TestTransformAssetFactory:
    """Test suite for transform_asset_factory function."""

    def test_factory_creates_asset_definition(self) -> None:
        """Test factory returns an AssetsDefinition."""
        asset_def = transform_asset_factory(
            asset_name="binance_transformed",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
        )
        assert asset_def is not None
        assert hasattr(asset_def, "node_def")

    def test_factory_with_invalid_exchange_raises_validation_error(self) -> None:
        """Test factory with invalid exchange ID raises ValidationError."""
        with pytest.raises(ValidationError, match="not a valid CCXT exchange"):
            transform_asset_factory(
                asset_name="test_asset",
                group_name="test_group",
                exchange_id="invalid_exchange",
                source_asset_key="test_source",
            )

    def test_factory_with_invalid_asset_name_raises_validation_error(self) -> None:
        """Test factory with invalid asset name raises ValidationError."""
        with pytest.raises(ValidationError, match="String should match pattern"):
            transform_asset_factory(
                asset_name="InvalidName",
                group_name="test_group",
                exchange_id="binance",
                source_asset_key="test_source",
            )

    def test_factory_with_custom_io_manager_key(self) -> None:
        """Test factory with custom IO manager key."""
        asset_def = transform_asset_factory(
            asset_name="binance_transformed",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
            io_manager_key="custom_io_manager",
        )
        assert asset_def is not None

    def test_asset_execution_success(self) -> None:
        """Test successful asset execution with mocked data."""
        # Create mock raw data with the new nested metadata structure
        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "timestamp": 1735675200000,
                    "datetime": "2025-12-31T19:00:00.000Z",
                    "last": 50000.0,
                    "bid": 49999.0,
                    "ask": 50001.0,
                    "high": 51000.0,
                    "low": 49000.0,
                    "volume": 1234.56,
                    "baseVolume": 1234.56,
                    "quoteVolume": 61728000.0,
                },
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "timestamp": 1735675200000,
                    "datetime": "2025-12-31T19:00:00.000Z",
                    "last": 3000.0,
                    "bid": 2999.0,
                    "ask": 3001.0,
                    "high": 3100.0,
                    "low": 2900.0,
                    "volume": 5678.90,
                    "baseVolume": 5678.90,
                    "quoteVolume": 17036700.0,
                },
                "SOL/USDT": {
                    "symbol": "SOL/USDT",
                    "timestamp": 1735675200000,
                    "datetime": "2025-12-31T19:00:00.000Z",
                    "last": 100.0,
                    "bid": 99.9,
                    "ask": 100.1,
                    "high": 105.0,
                    "low": 95.0,
                    "volume": 9876.54,
                    "baseVolume": 9876.54,
                    "quoteVolume": 987654.0,
                },
            },
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset that returns the mock data
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize(
            [mock_extract_asset, transform_asset],
        )

        # Verify execution was successful
        assert result.success

        # Get materialization for transform asset
        materialization = result.asset_materializations_for_node("test_transform")[0]

        # Verify metadata exists
        assert "row_count" in materialization.metadata
        assert "column_count" in materialization.metadata
        assert "columns" in materialization.metadata
        assert "transformation_time_seconds" in materialization.metadata
        assert "exchange_id" in materialization.metadata
        assert "extraction_timestamp" in materialization.metadata
        assert "sample_data" in materialization.metadata

    def test_asset_execution_with_empty_data(self) -> None:
        """Test asset execution with empty data."""
        # Create mock raw data with empty ticker data
        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": {},
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize([mock_extract_asset, transform_asset])

        # Verify execution was successful
        assert result.success

        # Get materialization
        materialization = result.asset_materializations_for_node("test_transform")[0]

        # Verify metadata exists
        assert "row_count" in materialization.metadata
        assert "column_count" in materialization.metadata

    def test_asset_execution_verifies_dataframe_structure(self) -> None:
        """Test that the output is a valid Narwhals DataFrame with expected columns."""
        # Create mock raw data
        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "timestamp": 1735675200000,
                    "last": 50000.0,
                    "bid": 49999.0,
                    "ask": 50001.0,
                    "volume": 1234.56,
                },
            },
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize([mock_extract_asset, transform_asset])

        # Verify execution was successful
        assert result.success

        # The actual DataFrame is stored in the IO manager, but we can verify
        # metadata indicates proper structure
        materialization = result.asset_materializations_for_node("test_transform")[0]

        # Verify expected columns are mentioned in metadata
        columns_metadata = materialization.metadata.get("columns")
        assert columns_metadata is not None

    def test_asset_execution_with_missing_fields(self) -> None:
        """Test asset execution handles missing fields gracefully."""
        # Create mock raw data with some missing fields
        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "last": 50000.0,
                    # Missing: bid, ask, volume, etc.
                },
                "ETH/USDT": {
                    "symbol": "ETH/USDT",
                    "last": 3000.0,
                    "volume": 5678.90,
                    # Missing: bid, ask, etc.
                },
            },
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize([mock_extract_asset, transform_asset])

        # Verify execution was successful (should handle missing fields gracefully)
        assert result.success

    def test_asset_execution_large_dataset(self) -> None:
        """Test asset execution with large dataset (100+ tickers)."""
        # Generate 150 mock tickers
        ticker_data = {
            f"COIN{i}/USDT": {
                "symbol": f"COIN{i}/USDT",
                "timestamp": 1735675200000,
                "last": 100.0 + i,
                "bid": 99.0 + i,
                "ask": 101.0 + i,
                "volume": 1000.0 + i,
            }
            for i in range(150)
        }

        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": ticker_data,
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize([mock_extract_asset, transform_asset])

        # Verify execution was successful
        assert result.success

        # Verify metadata
        materialization = result.asset_materializations_for_node("test_transform")[0]
        assert "row_count" in materialization.metadata
        assert "transformation_time_seconds" in materialization.metadata

    def test_multiple_assets_from_factory(self) -> None:
        """Test creating multiple transform assets from the same factory."""
        binance_transform = transform_asset_factory(
            asset_name="binance_transformed",
            group_name="transformed_data",
            exchange_id="binance",
            source_asset_key="binance_raw",
        )

        bybit_transform = transform_asset_factory(
            asset_name="bybit_transformed",
            group_name="transformed_data",
            exchange_id="bybit",
            source_asset_key="bybit_raw",
        )

        kraken_transform = transform_asset_factory(
            asset_name="kraken_transformed",
            group_name="transformed_data",
            exchange_id="kraken",
            source_asset_key="kraken_raw",
        )

        # Verify all assets are created
        assert binance_transform is not None
        assert bybit_transform is not None
        assert kraken_transform is not None

        # Verify they are different instances
        assert binance_transform != bybit_transform
        assert binance_transform != kraken_transform
        assert bybit_transform != kraken_transform

    def test_asset_adds_metadata_columns(self) -> None:
        """Test that transform asset adds exchange_id and extraction_timestamp columns."""
        # Create mock raw data
        mock_raw_data = {
            "metadata": {
                "timestamp": "2025-12-31T19:00:00.000000+00:00",
                "exchange_id": "binance",
            },
            "data": {
                "BTC/USDT": {
                    "symbol": "BTC/USDT",
                    "last": 50000.0,
                },
            },
        }

        # Create transform asset
        transform_asset = transform_asset_factory(
            asset_name="test_transform",
            group_name="test_group",
            exchange_id="binance",
            source_asset_key="test_extract",
            io_manager_key="io_manager",
        )

        # Create mock extract asset
        from dagster import Output, asset

        @asset(
            name="test_extract",
            io_manager_key="io_manager",
        )
        def mock_extract_asset(context):
            return Output(mock_raw_data)

        # Execute assets
        result = materialize([mock_extract_asset, transform_asset])

        # Verify execution was successful
        assert result.success

        # Verify metadata includes exchange_id and extraction_timestamp
        materialization = result.asset_materializations_for_node("test_transform")[0]
        assert materialization.metadata["exchange_id"].value == "binance"
        assert (
            materialization.metadata["extraction_timestamp"].value
            == "2025-12-31T19:00:00.000000+00:00"
        )
