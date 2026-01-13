from typing import TYPE_CHECKING

try:
    from duckdb import IOException
except ImportError:
    IOException = Exception  # type: ignore

if TYPE_CHECKING:
    from dagster import DagsterLogManager, InputContext
    from sqlalchemy import Engine

    from dagster_crypto_data.defs.connectors.database import DatabaseManagement
    from dagster_crypto_data.defs.models import CryptoModel
    from dagster_crypto_data.defs.resources.database import DatabaseConfig

__all__ = ["get_max_extraction_timestamp"]


def get_max_extraction_timestamp(
    context: InputContext,
    database: DatabaseConfig,
    exchange_id: str,
    model: type[CryptoModel],
) -> tuple[list[float], bool]:
    """Get maximum extraction timestamps from database for incremental loading.

    Supports both DatabaseConfig resource and DuckDBIOManager (for backward compatibility).

    Args:
        context: Dagster input context for logging
        database: DatabaseConfig resource or DuckDBIOManager for database access
        exchange_id: Exchange identifier to filter by
        model: CryptoModel class defining the table structure

    Returns:
        Tuple of (timestamps_list, is_empty):
        - timestamps_list: List of extraction timestamps found in database
        - is_empty: True if table exists but is empty, False otherwise
    """
    # Handle DuckDBIOManager directly (backward compatibility)
    if database.db_type == "duckdb":
        max_ts = _get_duckdb_ts(context.log, database, exchange_id, model)
    elif database.db_type in ("sqlite", "postgresql"):
        # DatabaseConfig resource
        max_ts = _get_db_ts_from_config(context, database, exchange_id, model)
    else:
        raise TypeError(
            f"database should be DatabaseConfig or DuckDBIOManager, got {type(database).__name__}"
        )

    full_table_name = model.__tablename__

    if max_ts:
        context.log.info(f"extraction_timestamp in {full_table_name}: {max_ts}")
        return (list(map(float, max_ts)), False)
    else:
        context.log.info(
            f"Table {full_table_name} is empty, for exchange_id '{exchange_id}' - will load ALL files"
        )
        return ([], True)  # Table exists but is empty


def _get_duckdb_ts(
    logger: DagsterLogManager,
    database: DatabaseConfig,
    exchange_id: str,
    model: type[CryptoModel],
) -> list:
    import duckdb

    table: str = model.__tablename__
    schema: str = model.__table_args__["schema"]

    try:
        with duckdb.connect(database.db_name, read_only=True) as conn:
            logger.info(f"Checking if table '{schema}.{table} exists'")
            result = conn.execute(
                f"select count(*) > 0 from duckdb_tables where table_name = '{table}' and schema_name = '{schema}'"
            ).fetchone()
            exists: bool = result[0] if result is not None else False
            if exists:
                logger.info(
                    f"Table '{schema}.{table}' exists. Trying to retrieve extraction_timestamp"
                )
                rows = conn.execute(
                    f"select distinct extraction_timestamp from {schema}.{table} where exchange_id = '{exchange_id}'"
                ).fetchall()
                max_ts: list = [r[0] for r in rows] if rows else []
                logger.info(f"Retrieved {len(max_ts) if max_ts else 0} timestamps")
                return max_ts
            else:
                logger.info(f"Table '{schema}.{table}' does not exist!")
                return []
    except IOException as e:
        # Database file doesn't exist yet - this is first run
        logger.info(
            f"DuckDB database file does not exist yet ({database.db_name}): {e}. "
            f"Will load all files on first run."
        )
        return []
    except Exception as e:
        # Catch any other errors and log them
        logger.warning(
            f"Failed to query DuckDB for extraction timestamps: {e}. "
            f"Will proceed with loading all files."
        )
        return []


def _get_db_ts_from_config(
    context: InputContext,
    database: DatabaseConfig,
    exchange_id: str,
    model: type[CryptoModel],
) -> list:
    """Get timestamps from database using DatabaseConfig resource.

    Args:
        context: Dagster input context for logging
        database: DatabaseConfig resource for database access
        exchange_id: Exchange identifier to filter by
        model: CryptoModel class defining the table structure

    Returns:
        List of extraction timestamps found in the database
    """
    table: str = model.__tablename__
    table_args = getattr(model, "__table_args__", {})
    schema = table_args.get("schema") if isinstance(table_args, dict) else None

    full_table_name = f"{schema}.{table}" if schema else table

    try:
        from sqlalchemy import text

        db_manager: DatabaseManagement = database.get_db_manager()
        engine: Engine = db_manager.engine
        db_manager._create_schema_and_tables(engine)

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"select distinct extraction_timestamp from {full_table_name} where exchange_id = '{exchange_id}'"
                )
            ).fetchall()
            max_ts: list = [r[0] for r in rows] if rows else []
            context.log.info(f"Retrieved {len(max_ts) if max_ts else 0} timestamps")
            return max_ts
    except Exception as e:
        context.log.warning(f"Failed to query timestamps from {full_table_name}: {e}")
        return []
