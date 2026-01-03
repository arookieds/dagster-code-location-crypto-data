from dagster import Definitions, ScheduleDefinition, define_asset_job

from dagster_crypto_data.assets.extract import extract_asset_factory
from dagster_crypto_data.assets.transform import transform_asset_factory
from dagster_crypto_data.io_managers import FilesystemIOManager
from dagster_crypto_data.resources.exchange import CCXTExchangeResource
from dagster_crypto_data.utils import get_logger, get_settings

logger = get_logger(__name__)
settings = get_settings()

# Extract assets - fetch data from exchanges
binance_extract = extract_asset_factory(
    asset_name="binance_raw_tickers",
    group_name="extract",
    exchange_id="binance",
)

# Transform assets - process raw data
binance_transform = transform_asset_factory(
    asset_name="binance_transformed_tickers",
    group_name="transform",
    exchange_id="binance",
    source_asset_key="binance_raw_tickers",
)

# Define jobs
extract_job = define_asset_job(
    name="extract_crypto_data",
    selection=[binance_extract],
)

transform_job = define_asset_job(
    name="transform_crypto_data",
    selection=[binance_transform],
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

defs = Definitions(
    assets=[binance_extract, binance_transform],
    jobs=[extract_job, transform_job],
    schedules=[extract_schedule, transform_schedule],
    resources={
        "io_manager": FilesystemIOManager(base_path="./local_runs"),
        "exchange": CCXTExchangeResource(exchange_id="binance"),
    },
)
