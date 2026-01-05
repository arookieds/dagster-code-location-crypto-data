import time
from typing import Any

import ccxt
import narwhals as nw
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetsDefinition,
    MetadataValue,
    Output,
    asset,
)
from pydantic import BaseModel, Field, field_validator


class TransformAssetConfig(BaseModel):
    """Configuration for transform asset factory with validation."""

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
        description="CCXT exchange identifier for metadata context",
    )
    source_asset_key: str = Field(
        ...,
        description="Key of the source extract asset to transform",
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


def transform_asset_factory(
    asset_name: str,
    group_name: str,
    exchange_id: str,
    source_asset_key: str,
    io_manager_key: str = "io_manager",
) -> AssetsDefinition:
    """Factory function to build a Dagster asset for transforming crypto ticker data.

    This factory validates inputs using Pydantic and creates a Dagster asset that
    transforms raw ticker data from extract assets into a structured Narwhals DataFrame.
    The transformation includes:
    - Flattening nested ticker data structure using Polars
    - Extracting relevant fields (price, volume, etc.)
    - Adding metadata columns (exchange_id, extraction_timestamp)

    Args:
        asset_name: Name of the asset to create (must be snake_case, lowercase)
        group_name: Dagster asset group name for organization
        exchange_id: CCXT exchange identifier for metadata context
        source_asset_key: Asset key of the upstream extract asset to transform
        io_manager_key: Dagster IO manager key for handling asset output

    Returns:
        AssetsDefinition configured to transform ticker data into a Narwhals DataFrame

    Raises:
        ValidationError: If any input parameter fails Pydantic validation

    Example:
        >>> from dagster import Definitions
        >>> from dagster_crypto_data.defs.resources.exchange import CCXTExchangeResource
        >>>
        >>> # Create extract asset
        >>> extract_asset = extract_asset_factory(
        ...     asset_name="binance_raw_tickers",
        ...     group_name="raw_data",
        ...     exchange_id="binance",
        ... )
        >>>
        >>> # Create transform asset that depends on extract
        >>> transform_asset = transform_asset_factory(
        ...     asset_name="binance_transformed_tickers",
        ...     group_name="transformed_data",
        ...     exchange_id="binance",
        ...     source_asset_key="binance_raw_tickers",
        ... )
        >>>
        >>> defs = Definitions(
        ...     assets=[extract_asset, transform_asset],
        ...     resources={"exchange": CCXTExchangeResource(exchange_id="binance")},
        ... )
    """
    # Validate inputs using Pydantic
    config = TransformAssetConfig(
        asset_name=asset_name,
        group_name=group_name,
        exchange_id=exchange_id,
        source_asset_key=source_asset_key,
        io_manager_key=io_manager_key,
    )

    @asset(
        name=config.asset_name,
        group_name=config.group_name,
        io_manager_key=config.io_manager_key,
        ins={
            "raw_data": AssetIn(
                key=AssetKey(config.source_asset_key),
            ),
        },
    )
    def build_asset(
        context: AssetExecutionContext, raw_data: dict[str, Any]
    ) -> Output[nw.DataFrame]:
        """Transform raw ticker data into structured DataFrame.

        Args:
            context: Dagster asset execution context
            raw_data: Raw data from extract asset with structure:
                {"metadata": {"timestamp": "...", "exchange_id": "..."}, "data": {symbol: ticker_info}}

        Returns:
            Output containing Narwhals DataFrame with transformed and enriched data
        """
        start_time = time.perf_counter()

        try:
            # Extract metadata and ticker data from the structured output
            metadata = raw_data.get("metadata", {})
            extraction_timestamp = metadata.get("timestamp")
            exchange_id_from_extract = metadata.get("exchange_id")
            ticker_data = raw_data.get("data", {})

            # Validate exchange_id consistency (optional validation)
            if (
                exchange_id_from_extract
                and exchange_id_from_extract != config.exchange_id
            ):
                context.log.warning(
                    f"Exchange ID mismatch: config={config.exchange_id}, "
                    f"extract={exchange_id_from_extract}"
                )

            # Validate input data
            if not ticker_data:
                context.log.warning(
                    f"Received empty data from {config.source_asset_key}"
                )
                # Return empty DataFrame with schema using Narwhals
                # Create empty dict with schema
                empty_data: dict[str, list[Any]] = {
                    "symbol": [],
                    "timestamp": [],
                    "datetime": [],
                    "last": [],
                    "bid": [],
                    "ask": [],
                    "high": [],
                    "low": [],
                    "volume": [],
                    "base_volume": [],
                    "quote_volume": [],
                    "exchange_id": [],
                    "extraction_timestamp": [],
                }
                # Use Polars to create empty DataFrame, then wrap with Narwhals
                import polars as pl

                df = nw.from_native(pl.DataFrame(empty_data))
                return Output(
                    value=df,
                    metadata={
                        "row_count": MetadataValue.int(0),
                        "column_count": MetadataValue.int(len(df.columns)),
                    },
                )

            context.log.info(
                f"Transforming {len(ticker_data)} tickers from {config.source_asset_key}"
            )

            # Use Polars to efficiently flatten the nested dict structure
            # Convert dict of dicts to list of dicts with symbol as a field
            records = [
                {"symbol": symbol, **ticker_info}
                for symbol, ticker_info in ticker_data.items()
            ]

            # Create DataFrame using Narwhals with Polars backend
            # Polars will automatically infer schema and handle nested structures
            import polars as pl

            df = nw.from_native(pl.DataFrame(records))

            # Select and rename relevant columns
            # This handles cases where some fields might be missing
            columns_to_keep = {
                "symbol": "symbol",
                "timestamp": "timestamp",
                "datetime": "datetime",
                "last": "last",
                "bid": "bid",
                "ask": "ask",
                "high": "high",
                "low": "low",
                "volume": "volume",
                "baseVolume": "base_volume",
                "quoteVolume": "quote_volume",
            }

            # Select only columns that exist in the DataFrame
            available_columns = set(df.columns)
            select_exprs = []

            for source_col, target_col in columns_to_keep.items():
                if source_col in available_columns:
                    if source_col != target_col:
                        select_exprs.append(nw.col(source_col).alias(target_col))
                    else:
                        select_exprs.append(nw.col(source_col))

            if select_exprs:
                df = df.select(select_exprs)

            # Add metadata columns
            df = df.with_columns(
                [
                    nw.lit(config.exchange_id).alias("exchange_id"),
                    nw.lit(extraction_timestamp).alias("extraction_timestamp"),
                ]
            )

            transformation_time = time.perf_counter() - start_time

            context.log.info(
                f"Successfully transformed {len(df)} tickers in {transformation_time:.2f}s"
            )

            # Get sample of first 5 rows for metadata
            sample_df = df.head(5)
            sample_dict = sample_df.to_dict(as_series=False)

            return Output(
                value=df,
                metadata={
                    "row_count": MetadataValue.int(len(df)),
                    "column_count": MetadataValue.int(len(df.columns)),
                    "columns": MetadataValue.text(", ".join(df.columns)),
                    "transformation_time_seconds": MetadataValue.float(
                        transformation_time
                    ),
                    "exchange_id": MetadataValue.text(config.exchange_id),
                    "extraction_timestamp": MetadataValue.text(
                        extraction_timestamp or "unknown"
                    ),
                    "sample_data": MetadataValue.md(f"```json\n{sample_dict}\n```"),
                },
            )

        except Exception as e:
            context.log.error(
                f"Failed to transform data from {config.source_asset_key}: {e}"
            )
            raise

    return build_asset
