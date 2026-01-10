from dagster import (
    ConfigurableIOManager,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_crypto_data.defs.assets.extract import extract_asset_factory
from dagster_crypto_data.defs.assets.transform import transform_asset_factory
from dagster_crypto_data.defs.io_managers import (
    DuckDBIOManager,
    FilesystemIOManager,
    S3IOManager,
    SQLIOManager,
)
from dagster_crypto_data.defs.models.tickers import Ticker
from dagster_crypto_data.defs.resources.exchange import CCXTExchangeResource
from dagster_crypto_data.defs.utils import get_logger, get_settings

logger = get_logger(__name__)
settings = get_settings()

# Extract assets - fetch data from exchanges
extract_assets = [
    binance_extract := extract_asset_factory(
        asset_name="binance_raw_tickers",
        group_name="extract",
        exchange_id="binance",
    ),
    bybit_extract_extract := extract_asset_factory(
        asset_name="bybit_raw_tickers",
        group_name="extract",
        exchange_id="bybit",
    ),
    gate_extract := extract_asset_factory(
        asset_name="gate_raw_tickers",
        group_name="extract",
        exchange_id="gate",
    ),
]

# Transform assets - process raw data
transform_assets = [
    binance_transform := transform_asset_factory(
        asset_name="binance_transformed_tickers",
        group_name="transform",
        exchange_id="binance",
        source_asset_key="binance_raw_tickers",
        model=Ticker,
        io_manager_key="transform_io_manager",
    ),
    bybit_transform := transform_asset_factory(
        asset_name="bybit_transformed_tickers",
        group_name="transform",
        exchange_id="bybit",
        source_asset_key="bybit_raw_tickers",
        model=Ticker,
        io_manager_key="transform_io_manager",
    ),
    gate_transform := transform_asset_factory(
        asset_name="gate_transformed_tickers",
        group_name="transform",
        exchange_id="gate",
        source_asset_key="gate_raw_tickers",
        model=Ticker,
        io_manager_key="transform_io_manager",
    ),
]

# Define jobs
extract_job = define_asset_job(
    name="extract_crypto_data",
    selection=extract_assets,
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "500m", "memory": "1024Mi"},
                }
            }
        }
    },
)

transform_job = define_asset_job(
    name="transform_crypto_data",
    selection=transform_assets,
    tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "100m", "memory": "128Mi"},
                    "limits": {"cpu": "500m", "memory": "2048Mi"},
                }
            }
        }
    },
)

# Schedules
extract_schedule = ScheduleDefinition(
    job=extract_job,
    cron_schedule="* * * * *",  # Every minute
)

transform_schedule = ScheduleDefinition(
    job=transform_job,
    cron_schedule="*/10 * * * *",  # Every 10 minutes
)

# Configure IO managers based on IS_PRODUCTION
extract_io_manager: ConfigurableIOManager
transform_io_manager: ConfigurableIOManager

if settings.is_production:
    extract_io_manager = S3IOManager(
        endpoint_url=settings.s3_url,
        access_key=settings.s3_user,
        secret_key=settings.s3_password.get_secret_value(),
        bucket=settings.s3_bucket,
        use_ssl=not settings.s3_url.startswith("http://"),
        # Database config for timestamp-based filtering
        db_host=settings.db_host,
        db_port=settings.db_port,
        db_name=settings.db_name,
        db_username=settings.db_username,
        db_password=settings.db_password.get_secret_value(),
        db_schema="crypto_data",
        target_table_name="raw_tickers",  # Table where transform writes the data
    )
    transform_io_manager = SQLIOManager(
        db_type="postgresql",
        host=settings.db_host,
        port=settings.db_port,
        db_name=settings.db_name,
        username=settings.db_username,
        password=settings.db_password.get_secret_value(),
        db_schema="public",  # Fallback schema (models define their own schema)
    )
else:
    extract_io_manager = FilesystemIOManager(base_path="./local_runs")
    transform_io_manager = DuckDBIOManager(db_path="./local_runs/crypto.duckdb")

defs = Definitions(
    assets=[*extract_assets, *transform_assets],
    jobs=[extract_job, transform_job],
    schedules=[extract_schedule, transform_schedule],
    resources={
        "io_manager": extract_io_manager,
        "transform_io_manager": transform_io_manager,
        "exchange": CCXTExchangeResource(),
    },
)
