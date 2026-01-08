"""Ticker data models for cryptocurrency market data.

Note: These models define the expected schema for ticker data.
The actual table creation is handled by the SQLIOManager using Polars'
write_database, which infers schema from the DataFrame.

These models serve as documentation and can be used for validation.
"""

from __future__ import annotations

from typing import ClassVar

from sqlalchemy import BigInteger, Column
from sqlmodel import Field, SQLModel


class Ticker(SQLModel, table=True):
    """Ticker data model for cryptocurrency market data.

    This model represents the transformed ticker data output from the transform asset.
    The columns match the output of transform_asset_factory.

    Schema: crypto_data.raw_tickers

    Note: This table stores raw ticker data from all exchanges. The exchange_id
    column differentiates between different exchanges (binance, bybit, etc.).

    Columns:
        symbol: Trading pair symbol (e.g., "BTC/USDT")
        ticker_timestamp_ms: Unix timestamp in milliseconds when ticker was updated on exchange
        ticker_datetime: ISO 8601 datetime string of ticker update time
        last: Last trade price
        bid: Current best bid price
        ask: Current best ask price
        high: 24h high price
        low: 24h low price
        volume: 24h trading volume in base currency
        base_volume: Same as volume (CCXT naming)
        quote_volume: 24h trading volume in quote currency
        exchange_id: Exchange identifier (e.g., "binance")
        extraction_timestamp: Unix timestamp in ms of when data was extracted
    """

    __tablename__: ClassVar[str] = "raw_tickers"
    __table_args__: ClassVar[dict] = {"schema": "crypto_data"}

    # Primary key - auto-generated row ID
    id: int | None = Field(default=None, primary_key=True)

    # Core ticker data
    symbol: str = Field(description="Trading pair symbol (e.g., BTC/USDT)")
    ticker_timestamp_ms: int | None = Field(
        default=None,
        sa_column=Column("ticker_timestamp_ms", BigInteger(), nullable=True),
        description="Unix timestamp in milliseconds when ticker was updated on exchange",
    )
    ticker_datetime: str | None = Field(
        default=None,
        description="ISO 8601 datetime string of ticker update time",
    )

    # Price data
    last: float | None = Field(default=None, description="Last trade price")
    bid: float | None = Field(default=None, description="Current best bid price")
    ask: float | None = Field(default=None, description="Current best ask price")
    high: float | None = Field(default=None, description="24h high price")
    low: float | None = Field(default=None, description="24h low price")

    # Volume data
    volume: float | None = Field(
        default=None, description="24h trading volume in base currency"
    )
    base_volume: float | None = Field(
        default=None, description="24h trading volume in base currency (CCXT naming)"
    )
    quote_volume: float | None = Field(
        default=None, description="24h trading volume in quote currency"
    )

    # Metadata
    exchange_id: str = Field(description="Exchange identifier (e.g., binance)")
    extraction_timestamp: int | None = Field(
        default=None,
        sa_column=Column("extraction_timestamp", BigInteger(), nullable=True),
        description="Unix timestamp of when data was extracted",
    )
