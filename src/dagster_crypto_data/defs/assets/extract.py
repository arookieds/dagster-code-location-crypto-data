import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import ccxt
from dagster import AssetExecutionContext, AssetsDefinition, MetadataValue, Output, asset
from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from dagster_crypto_data.defs.resources.exchange import CCXTExchangeResource


class ExtractAssetConfig(BaseModel):
    """Configuration for extract asset factory with validation."""

    asset_name: str = Field(
        ...,
        min_length=1,
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Asset name (snake_case, starts with letter)",
    )
    group_name: str = Field(
        ...,
        min_length=1,
        description="Dagster asset group name",
    )
    exchange_id: str = Field(
        ...,
        description="CCXT exchange identifier",
    )
    io_manager_key: str = Field(
        default="io_manager",
        description="Dagster IO manager key",
    )

    @field_validator("exchange_id")
    @classmethod
    def validate_exchange_id(cls, v: str) -> str:
        """Validate exchange_id is supported by CCXT."""
        if v not in ccxt.exchanges:
            raise ValueError(f"'{v}' is not a valid CCXT exchange")
        return v


def extract_asset_factory(
    asset_name: str,
    group_name: str,
    exchange_id: str,
    io_manager_key: str = "io_manager",
) -> AssetsDefinition:
    """Factory function to build a Dagster asset for crypto exchange data extraction.

    This factory validates inputs using Pydantic and creates a Dagster asset that
    fetches ticker data from the specified cryptocurrency exchange using CCXT.
    The asset requires an 'exchange' resource to be configured in the Dagster
    Definitions.

    Args:
        asset_name: Name of the asset to create (must be snake_case, lowercase)
        group_name: Dagster asset group name for organization
        exchange_id: CCXT exchange identifier (e.g., 'binance', 'bybit', 'kraken')
        io_manager_key: Dagster IO manager key for handling asset output

    Returns:
        AssetsDefinition configured for the specified exchange that returns
        ticker data with metadata including record count, extraction time,
        and sample records.

    Raises:
        ValidationError: If any input parameter fails Pydantic validation
            (e.g., invalid exchange_id, malformed asset_name)

    Example:
        >>> from dagster import Definitions
        >>> from dagster_crypto_data.defs.resources.exchange import CCXTExchangeResource
        >>>
        >>> binance_asset = extract_asset_factory(
        ...     asset_name="binance_tickers",
        ...     group_name="raw_data",
        ...     exchange_id="binance",
        ... )
        >>>
        >>> defs = Definitions(
        ...     assets=[binance_asset],
        ...     resources={"exchange": CCXTExchangeResource(exchange_id="binance")},
        ... )
    """
    config = ExtractAssetConfig(
        asset_name=asset_name,
        group_name=group_name,
        exchange_id=exchange_id,
        io_manager_key=io_manager_key,
    )

    @asset(
        name=config.asset_name,
        group_name=config.group_name,
        io_manager_key=config.io_manager_key,
        required_resource_keys={"exchange"},
    )
    def build_asset(context: AssetExecutionContext) -> Output[dict[str, Any]]:
        # Access the resource from context
        exchange: CCXTExchangeResource = context.resources.exchange
        client = exchange.get_client()

        context.log.info(f"Fetching tickers from {config.exchange_id}")

        try:
            # Capture extraction timestamp
            extraction_timestamp = datetime.now(UTC).isoformat()

            start_time = time.perf_counter()
            raw: dict[str, Any] = client.fetch_tickers()
            execution_time = time.perf_counter() - start_time
        except ccxt.NetworkError as e:
            context.log.error(
                f"Network error fetching data from {config.exchange_id}: {e}"
            )
            raise
        except ccxt.ExchangeError as e:
            context.log.error(f"Exchange API error from {config.exchange_id}: {e}")
            raise
        except Exception as e:
            context.log.error(
                f"Unexpected error fetching data from {config.exchange_id}: {e}"
            )
            raise

        # Prepare output with metadata and data structure
        output_data = {
            "metadata": {
                "timestamp": extraction_timestamp,
                "exchange_id": config.exchange_id,
            },
            "data": raw,
        }

        # Prepare metadata
        record_count = len(raw)
        sample = dict(list(raw.items())[:5])

        context.log.info(
            f"Successfully fetched {record_count} tickers in {execution_time:.2f}s"
        )

        return Output(
            value=output_data,
            metadata={
                "record_count": MetadataValue.int(record_count),
                "extraction_time_seconds": MetadataValue.float(execution_time),
                "extraction_timestamp": MetadataValue.text(extraction_timestamp),
                "sample_records": MetadataValue.json(sample),
                "exchange_id": MetadataValue.text(config.exchange_id),
            },
        )

    return build_asset
