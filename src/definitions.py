from dagster import Definitions

from dagster_crypto_data.utils import get_logger, get_settings

logger = get_logger()
settings = get_settings()

# Loading assets
## Creating assets, using the proper factory function, depending if the assets is for extracting data using ccxt, or transforming it (loading from minio, or filesystem, transforming and loading to postgresql). 


# create the necessary schedules, running every minutes, for data extraction, and running every 10mins, for data transformation


defs = Definitions()
