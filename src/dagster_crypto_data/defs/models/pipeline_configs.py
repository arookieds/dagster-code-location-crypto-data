from __future__ import annotations

import uuid
from datetime import datetime
from typing import ClassVar

from sqlalchemy import ARRAY, Integer, String
from sqlmodel import BOOLEAN, JSON, TEXT, TIMESTAMP, Column, Field, SQLModel


class PipelineConfig(SQLModel, table=True):
    """Configuration table for crypto data extraction and transformation pipelines.

    This table stores configuration for which exchanges should be processed,
    what data to extract, and how to transform it. Each row represents a
    pipeline configuration for a specific exchange.

    Resides in: config.pipeline_configs
    """

    __tablename__: ClassVar[str] = "pipeline_configs"
    __table_args__: ClassVar[dict] = {"schema": "config"}

    # Primary Key
    id: uuid.UUID = Field(
        default_factory=uuid.uuid7,
        primary_key=True,
        description="Unique identifier for this pipeline configuration",
    )

    # Exchange Configuration
    exchange_id: str = Field(
        sa_column=Column(TEXT, nullable=False, unique=True, index=True),
        description="CCXT exchange identifier (e.g., 'binance', 'bybit', 'kraken')",
    )

    # Pipeline Control
    enabled: bool = Field(
        sa_column=Column(BOOLEAN, nullable=False, default=True),
        description="Whether this pipeline is enabled for execution",
    )

    # Asset Configuration
    extract_asset_name: str | None = Field(
        sa_column=Column(TEXT, nullable=True),
        default=None,
        description="Name of the extract asset (e.g., 'binance_raw_tickers')",
    )

    transform_asset_name: str | None = Field(
        sa_column=Column(TEXT, nullable=True),
        default=None,
        description="Name of the transform asset (e.g., 'binance_transformed_tickers')",
    )

    # Asset Grouping
    extract_group_name: str = Field(
        sa_column=Column(TEXT, nullable=False, default="raw_data"),
        default="raw_data",
        description="Dagster asset group for extract assets",
    )

    transform_group_name: str = Field(
        sa_column=Column(TEXT, nullable=False, default="transformed_data"),
        default="transformed_data",
        description="Dagster asset group for transform assets",
    )

    # IO Manager Configuration
    extract_io_manager_key: str = Field(
        sa_column=Column(TEXT, nullable=False, default="s3_io_manager"),
        default="s3_io_manager",
        description="IO manager key for extract asset output (e.g., 's3_io_manager', 'filesystem_io_manager')",
    )

    transform_io_manager_key: str = Field(
        sa_column=Column(TEXT, nullable=False, default="postgres_io_manager"),
        default="postgres_io_manager",
        description="IO manager key for transform asset output (e.g., 'postgres_io_manager', 'duckdb_io_manager')",
    )

    # Schedule Configuration
    schedule_enabled: bool = Field(
        sa_column=Column(BOOLEAN, nullable=False, default=False),
        default=False,
        description="Whether to enable scheduled execution for this pipeline",
    )

    schedule_cron: str | None = Field(
        sa_column=Column(TEXT, nullable=True),
        default=None,
        description="Cron expression for scheduled execution (e.g., '0 * * * *' for hourly)",
    )

    # Data Extraction Configuration
    symbols: list[str] | None = Field(
        sa_column=Column(ARRAY(String), nullable=True),
        default=None,
        description="List of trading pair symbols to extract (e.g., ['BTC/USDT', 'ETH/USDT']). NULL means fetch all tickers.",
    )

    fetch_all_tickers: bool = Field(
        sa_column=Column(BOOLEAN, nullable=False, default=True),
        default=True,
        description="Whether to fetch all available tickers from the exchange (ignores symbols list if True)",
    )

    rate_limit_per_minute: int | None = Field(
        sa_column=Column(Integer, nullable=True),
        default=None,
        description="Maximum API requests per minute (NULL uses exchange default)",
    )

    timeout_seconds: int = Field(
        sa_column=Column(Integer, nullable=False, default=30),
        default=30,
        description="API request timeout in seconds",
    )

    # Future extensibility - for fields we haven't thought of yet
    config: dict | None = Field(
        sa_column=Column(JSON, nullable=True),
        default=None,
        description="Additional configuration as JSON for future extensibility (avoid using if possible - add explicit columns instead)",
    )

    # Metadata and Auditing
    description: str | None = Field(
        sa_column=Column(TEXT, nullable=True),
        default=None,
        description="Human-readable description of this pipeline configuration",
    )

    created_at: datetime = Field(
        sa_column=Column(TIMESTAMP, nullable=False),
        default_factory=datetime.utcnow,
        description="Timestamp when this configuration was created",
    )

    updated_at: datetime = Field(
        sa_column=Column(TIMESTAMP, nullable=False),
        default_factory=datetime.utcnow,
        description="Timestamp when this configuration was last updated",
    )

    last_run_at: datetime | None = Field(
        sa_column=Column(TIMESTAMP, nullable=True),
        default=None,
        description="Timestamp of the last successful pipeline execution",
    )

    # Priority and Ordering
    priority: int = Field(
        sa_column=Column(Integer, nullable=False, default=0),
        default=0,
        description="Execution priority (higher values run first)",
    )

    # Error Handling
    retry_enabled: bool = Field(
        sa_column=Column(BOOLEAN, nullable=False, default=True),
        default=True,
        description="Whether to retry failed pipeline executions",
    )

    max_retries: int = Field(
        sa_column=Column(Integer, nullable=False, default=3),
        default=3,
        description="Maximum number of retry attempts for failed executions",
    )

    # Tags for filtering and organization
    tags: dict | None = Field(
        sa_column=Column(JSON, nullable=True),
        default=None,
        description="Custom tags for filtering and organization (e.g., {'region': 'us', 'tier': 'premium'})",
    )


# Alias for backward compatibility
DagsterConfig = PipelineConfig
