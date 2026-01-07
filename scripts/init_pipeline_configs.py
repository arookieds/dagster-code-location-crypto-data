#!/usr/bin/env python
"""Initialize pipeline_configs table and populate with default configurations.

This script:
1. Creates the pipeline_configs table in PostgreSQL
2. Populates it with initial exchange configurations
3. Can be run multiple times safely (idempotent)

Usage:
    uv run python scripts/init_pipeline_configs.py

Environment Variables:
    DB_HOST: PostgreSQL host (default: localhost)
    DB_PORT: PostgreSQL port (default: 5432)
    DB_NAME: Database name (default: crypto)
    DB_USERNAME: Database username (default: postgres)
    DB_PASSWORD: Database password (required)
"""

from __future__ import annotations

import sys

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, select

from dagster_crypto_data.defs.models.pipeline_configs import PipelineConfig
from dagster_crypto_data.defs.utils.logger import get_logger
from dagster_crypto_data.defs.utils.settings import Settings

# Initialize logger
logger = get_logger(__name__)


def create_database_url(settings: Settings) -> str:
    """Create PostgreSQL database URL from settings."""
    password = settings.db_password.get_secret_value() if settings.db_password else ""
    return (
        f"postgresql://{settings.db_username}:{password}"
        f"@{settings.db_host}:{settings.db_port}/{settings.db_name}"
    )


def create_schema(engine) -> None:  # type: ignore[no-untyped-def]
    """Create config schema if it doesn't exist."""
    logger.info("Creating 'config' schema...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS config"))
        conn.commit()
    logger.info("✓ Schema created successfully")


def create_table(engine) -> None:  # type: ignore[no-untyped-def]
    """Create pipeline_configs table if it doesn't exist."""
    logger.info("Creating pipeline_configs table...")
    SQLModel.metadata.create_all(engine)
    logger.info("✓ Table created successfully")


def get_default_configs() -> list[dict]:
    """Get default pipeline configurations for common exchanges.

    Returns:
        List of configuration dictionaries for initial population
    """
    return [
        {
            "exchange_id": "binance",
            "enabled": True,
            "extract_asset_name": "binance_raw_tickers",
            "transform_asset_name": "binance_transformed_tickers",
            "extract_group_name": "raw_data",
            "transform_group_name": "transformed_data",
            "extract_io_manager_key": "s3_io_manager",
            "transform_io_manager_key": "postgres_io_manager",
            "schedule_enabled": True,
            "schedule_cron": "0 * * * *",  # Hourly
            "description": "Binance exchange - largest crypto exchange by volume",
            "priority": 100,
            "retry_enabled": True,
            "max_retries": 3,
            "symbols": ["BTC/USDT", "ETH/USDT", "BNB/USDT"],
            "fetch_all_tickers": True,
            "rate_limit_per_minute": None,  # Use exchange default
            "timeout_seconds": 30,
            "config": None,  # Reserved for future extensibility
            "tags": {"region": "global", "tier": "tier1", "type": "spot"},
        },
        {
            "exchange_id": "bybit",
            "enabled": True,
            "extract_asset_name": "bybit_raw_tickers",
            "transform_asset_name": "bybit_transformed_tickers",
            "extract_group_name": "raw_data",
            "transform_group_name": "transformed_data",
            "extract_io_manager_key": "s3_io_manager",
            "transform_io_manager_key": "postgres_io_manager",
            "schedule_enabled": True,
            "schedule_cron": "0 * * * *",  # Hourly
            "description": "Bybit exchange - derivatives and spot trading",
            "priority": 90,
            "retry_enabled": True,
            "max_retries": 3,
            "symbols": ["BTC/USDT", "ETH/USDT"],
            "fetch_all_tickers": True,
            "rate_limit_per_minute": None,
            "timeout_seconds": 30,
            "config": None,
            "tags": {"region": "global", "tier": "tier1", "type": "derivatives"},
        },
        {
            "exchange_id": "kraken",
            "enabled": False,
            "extract_asset_name": "kraken_raw_tickers",
            "transform_asset_name": "kraken_transformed_tickers",
            "extract_group_name": "raw_data",
            "transform_group_name": "transformed_data",
            "extract_io_manager_key": "s3_io_manager",
            "transform_io_manager_key": "postgres_io_manager",
            "schedule_enabled": False,
            "schedule_cron": "0 */2 * * *",  # Every 2 hours
            "description": "Kraken exchange - US-based exchange (disabled by default)",
            "priority": 50,
            "retry_enabled": True,
            "max_retries": 5,
            "symbols": ["BTC/USD", "ETH/USD"],
            "fetch_all_tickers": True,
            "rate_limit_per_minute": 60,  # Kraken has strict rate limits
            "timeout_seconds": 45,
            "config": None,
            "tags": {"region": "us", "tier": "tier2", "type": "spot"},
        },
        {
            "exchange_id": "coinbase",
            "enabled": False,
            "extract_asset_name": "coinbase_raw_tickers",
            "transform_asset_name": "coinbase_transformed_tickers",
            "extract_group_name": "raw_data",
            "transform_group_name": "transformed_data",
            "extract_io_manager_key": "s3_io_manager",
            "transform_io_manager_key": "postgres_io_manager",
            "schedule_enabled": False,
            "schedule_cron": "0 */6 * * *",  # Every 6 hours
            "description": "Coinbase exchange - US-based exchange (disabled by default)",
            "priority": 40,
            "retry_enabled": True,
            "max_retries": 3,
            "symbols": ["BTC/USD", "ETH/USD"],
            "fetch_all_tickers": True,
            "rate_limit_per_minute": None,
            "timeout_seconds": 30,
            "config": None,
            "tags": {"region": "us", "tier": "tier2", "type": "spot"},
        },
        {
            "exchange_id": "gateio",
            "enabled": False,
            "extract_asset_name": "gateio_raw_tickers",
            "transform_asset_name": "gateio_transformed_tickers",
            "extract_group_name": "raw_data",
            "transform_group_name": "transformed_data",
            "extract_io_manager_key": "s3_io_manager",
            "transform_io_manager_key": "postgres_io_manager",
            "schedule_enabled": False,
            "schedule_cron": "0 */4 * * *",  # Every 4 hours
            "description": "Gate.io exchange - altcoin focused (disabled by default)",
            "priority": 30,
            "retry_enabled": True,
            "max_retries": 3,
            "symbols": ["BTC/USDT", "ETH/USDT"],
            "fetch_all_tickers": True,
            "rate_limit_per_minute": None,
            "timeout_seconds": 30,
            "config": None,
            "tags": {"region": "global", "tier": "tier3", "type": "spot"},
        },
    ]


def populate_configs(session: Session) -> None:
    """Populate pipeline_configs table with default configurations.

    Args:
        session: SQLModel database session
    """
    configs = get_default_configs()
    logger.info(f"Populating {len(configs)} default configurations...")

    inserted = 0
    skipped = 0

    for config_data in configs:
        try:
            # Create PipelineConfig instance
            config = PipelineConfig(**config_data)

            # Add to session
            session.add(config)
            session.commit()

            logger.info(f"✓ Inserted config for exchange: {config.exchange_id}")
            inserted += 1

        except IntegrityError:
            # Config already exists (unique constraint on exchange_id)
            session.rollback()
            logger.info(
                f"⊘ Skipped existing config for exchange: {config_data['exchange_id']}"
            )
            skipped += 1

        except Exception as e:
            session.rollback()
            logger.error(
                f"✗ Failed to insert config for {config_data['exchange_id']}: {e}"
            )
            raise

    logger.info(f"Summary: {inserted} inserted, {skipped} skipped")


def verify_configs(session: Session) -> None:
    """Verify configurations were inserted correctly.

    Args:
        session: SQLModel database session
    """
    logger.info("Verifying configurations...")

    # Count total configs
    statement = select(PipelineConfig)
    results = session.exec(statement).all()
    total = len(results)
    logger.info(f"Total configurations: {total}")

    # Count enabled configs
    statement = select(PipelineConfig).where(PipelineConfig.enabled == True)  # noqa: E712
    enabled_results = session.exec(statement).all()
    enabled = len(enabled_results)
    logger.info(f"Enabled configurations: {enabled}")

    # List all exchanges
    all_configs_list = session.exec(select(PipelineConfig)).all()
    all_configs = sorted(all_configs_list, key=lambda x: x.priority, reverse=True)

    logger.info("Configured exchanges:")
    for config in all_configs:
        status = "✓ enabled" if config.enabled else "✗ disabled"
        schedule = "scheduled" if config.schedule_enabled else "manual"
        logger.info(f"  - {config.exchange_id}: {status}, {schedule}")


def main() -> int:
    """Main execution function.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Load settings from environment
        logger.info("Loading settings from environment...")
        settings = Settings()

        # Validate database password is set
        if not settings.db_password or not settings.db_password.get_secret_value():
            logger.error("DB_PASSWORD environment variable is required")
            return 1

        # Create database URL
        db_url = create_database_url(settings)
        logger.info(
            f"Connecting to database: {settings.db_host}:{settings.db_port}/{settings.db_name}"
        )

        # Create engine
        engine = create_engine(db_url, echo=False)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("✓ Connected to PostgreSQL successfully")

        # Create schema
        create_schema(engine)

        # Create table
        create_table(engine)

        # Populate configurations
        with Session(engine) as session:
            populate_configs(session)
            verify_configs(session)

        logger.info("✓ Pipeline configurations initialized successfully")
        return 0

    except Exception as e:
        logger.error(f"✗ Failed to initialize pipeline configurations: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
