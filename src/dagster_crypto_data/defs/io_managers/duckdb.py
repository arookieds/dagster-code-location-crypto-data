"""DuckDB IO Manager for local analytics."""

from __future__ import annotations

import random
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import narwhals as nw
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

from dagster_crypto_data.defs import models

try:
    from duckdb import IOException
except ImportError:
    # Fallback if duckdb module structure changes
    IOException = Exception  # type: ignore

if TYPE_CHECKING:
    from narwhals.typing import FrameT
    from sqlmodel import SQLModel


class DuckDBIOManager(ConfigurableIOManager):
    """Store DataFrames in DuckDB for local analytics.

    This IO manager is designed for local development and testing with
    DuckDB, an in-process analytical database. It accepts Narwhals-compatible
    DataFrames and stores them as DuckDB tables.

    Includes retry logic with exponential backoff to handle concurrent writes,
    since DuckDB doesn't support simultaneous writes from different processes.

    Attributes:
        db_path: Path to DuckDB database file (default: ./data/local.duckdb)
        db_schema: Database schema name (default: crypto_data)

    Example:
        ```python
        from dagster import Definitions
        from dagster_crypto_data.defs.io_managers import DuckDBIOManager

        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": DuckDBIOManager(
                    db_path="./data/crypto.duckdb",
                    schema="analytics",
                ),
            },
        )
        ```
    """

    db_path: str = Field(
        default="./data/local.duckdb",
        description="Path to DuckDB database file",
    )
    db_schema: str = Field(
        default="crypto_data",
        description="Database schema name",
    )

    @staticmethod
    def _get_model_from_context(
        context: OutputContext | InputContext,
    ) -> type[SQLModel] | None:
        """Get the SQLModel class from asset metadata.

        Args:
            context: Dagster context with asset metadata

        Returns:
            SQLModel class if found in metadata, None otherwise
        """
        metadata = getattr(context, "definition_metadata", None) or {}
        model_name: str = metadata.get("model", "")
        if model_name:
            return getattr(models, model_name, None)
        return None

    @staticmethod
    def _get_table_name(context: OutputContext | InputContext) -> str:
        """Get the table name for an asset.

        Checks for model metadata first (for shared tables),
        then falls back to asset key path.

        Args:
            context: Dagster context with asset key information

        Returns:
            Table name (e.g., "tickers" or "extract_binance_ohlcv")
        """
        # Check if model is in metadata (for shared tables like "tickers")
        context.log.debug(f"Process using a '{type(context)}' context")
        model = DuckDBIOManager._get_model_from_context(context)
        if model is not None:
            return getattr(model, "__tablename__", "_".join(context.asset_key.path))

        # Fall back to asset key path joined with underscores
        return "_".join(context.asset_key.path)

    def _ensure_db_exists(self) -> None:
        """Ensure the database file and parent directories exist."""
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

    def _create_table_from_model(
        self,
        context: OutputContext,
        model: type[SQLModel],
        conn: Any,
        table_name: str,
    ) -> None:
        """Create DuckDB table from SQLModel definition with all columns.

        Args:
            context: Dagster output context for logging
            model: SQLModel class defining table structure
            conn: DuckDB connection object
            table_name: Name of the table to create
        """
        # Extract columns from model fields
        columns = []
        for field_name, field_info in model.model_fields.items():
            # Get the annotation (handle Optional types)
            annotation = field_info.annotation

            # Handle Optional types (e.g., int | None, str | None)
            args = getattr(annotation, "__args__", None)
            inner_type = args[0] if args is not None and len(args) > 0 else annotation

            # Map Python type to SQL type
            if inner_type is int:
                sql_type = "BIGINT"
            elif inner_type is float:
                sql_type = "DOUBLE"
            elif inner_type is bool:
                sql_type = "BOOLEAN"
            else:
                sql_type = "VARCHAR"

            # Build column definition
            columns.append(f'"{field_name}" {sql_type}')

        # Build and execute CREATE TABLE statement
        columns_sql = ",\n        ".join(columns)
        create_sql = f"""CREATE TABLE IF NOT EXISTS {self.db_schema}.{table_name} (
        {columns_sql}
    )"""

        conn.execute(create_sql)
        context.log.debug(
            f"Created table {self.db_schema}.{table_name} from model {model.__name__} with {len(columns)} columns"
        )

    def handle_output(self, context: OutputContext, obj: FrameT) -> None:
        """Store a DataFrame in DuckDB.

        Args:
            context: Dagster output context
            obj: Narwhals-compatible DataFrame to store

        Raises:
            TypeError: If obj is not a DataFrame
        """
        # Convert to Narwhals DataFrame for compatibility
        try:
            df = nw.from_native(obj).to_native()
        except Exception as e:
            raise TypeError(
                f"DuckDBIOManager expects a DataFrame, got {type(obj).__name__}"
            ) from e

        self._ensure_db_exists()
        table_name = self._get_table_name(context)

        # Import DuckDB for table existence checks
        try:
            import duckdb
        except ImportError as e:
            raise ImportError(
                "DuckDB is not installed. Install it with: uv add duckdb"
            ) from e

        # Get native DataFrame (Polars)
        native_df = df
        row_count = len(native_df)

        # Retry logic for concurrent write handling
        # DuckDB doesn't support concurrent writes, so we retry with exponential backoff + jitter
        # Jitter prevents multiple processes from synchronized retries
        max_retries = 5
        base_wait = 0.1  # 100ms
        last_error = None

        for attempt in range(max_retries):
            try:
                # Connect to DuckDB to set up schema and table
                conn = duckdb.connect(self.db_path)
                try:
                    # Create schema if it doesn't exist (unless it's 'main')
                    if self.db_schema != "main":
                        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}")

                    # Get model from context metadata
                    model = self._get_model_from_context(context)

                    # Create table from model with all columns if model exists
                    # This ensures all columns are defined, even if not present in current data
                    if model is not None:
                        self._create_table_from_model(context, model, conn, table_name)

                    # Check if table exists after model creation
                    result = conn.execute(
                        f"SELECT count(*) > 0 FROM duckdb_tables WHERE table_name = '{table_name}' AND schema_name = '{self.db_schema}'"
                    ).fetchone()
                    table_exists = result[0] if result is not None else False

                    if not table_exists:
                        context.log.warning(
                            f"Table {self.db_schema}.{table_name} does not exist. "
                            f"No model provided to create table structure. "
                            f"Creating table implicitly from DataFrame."
                        )
                        # Register DataFrame and create table from it
                        conn.register("source_data", native_df)
                        conn.execute(
                            f"CREATE TABLE {self.db_schema}.{table_name} AS SELECT * FROM source_data"
                        )
                    else:
                        # Table exists - get table columns and filter DataFrame to match
                        # This prevents errors when DataFrame has extra columns not in table
                        table_info = conn.execute(
                            f"SELECT * FROM {self.db_schema}.{table_name} LIMIT 0"
                        ).description
                        table_columns = [col[0] for col in table_info]

                        # Filter DataFrame to only columns that exist in the table
                        df_columns_to_insert = [
                            col for col in native_df.columns if col in table_columns
                        ]
                        df_filtered = native_df.select(df_columns_to_insert)

                        # Use INSERT BY NAME for intelligent column mapping
                        # BY NAME handles column mapping gracefully - missing columns get NULL
                        conn.register("source_data", df_filtered)
                        conn.execute(
                            f"INSERT INTO {self.db_schema}.{table_name} BY NAME SELECT * FROM source_data"
                        )
                finally:
                    conn.close()

                context.log.info(
                    f"Stored {row_count} rows to DuckDB table {self.db_schema}.{table_name}"
                )
                return  # Success

            except (IOException, OSError, RuntimeError) as e:
                # IOException: DuckDB lock conflicts ("Could not set lock on file")
                # OSError: file lock/access issues
                # RuntimeError: DuckDB internal locking/concurrency errors
                last_error = e
                if attempt < max_retries - 1:
                    # Exponential backoff with random jitter
                    # This prevents synchronized retries from multiple processes
                    exponential_wait = base_wait * (2**attempt)
                    jitter = random.uniform(
                        0, exponential_wait * 0.5
                    )  # Jitter up to 50% of wait time
                    wait_time = exponential_wait + jitter
                    context.log.warning(
                        f"DuckDB write attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {wait_time:.2f}s (exponential: {exponential_wait:.2f}s + jitter: {jitter:.2f}s)..."
                    )
                    time.sleep(wait_time)
                else:
                    context.log.error(
                        f"DuckDB write failed after {max_retries} attempts"
                    )

        # If we get here, all retries failed
        raise RuntimeError(
            f"Failed to write to DuckDB after {max_retries} attempts. Last error: {last_error}"
        ) from last_error

    def load_input(self, context: InputContext) -> Any:
        """Load a DataFrame from DuckDB.

        Args:
            context: Dagster input context

        Returns:
            Polars DataFrame loaded from DuckDB (Narwhals-compatible)

        Raises:
            FileNotFoundError: If database file doesn't exist
            ValueError: If table doesn't exist
        """
        db_file = Path(self.db_path)
        if not db_file.exists():
            raise FileNotFoundError(
                f"DuckDB database not found: {self.db_path}. "
                f"Make sure the upstream asset has been materialized."
            )

        table_name = self._get_table_name(context)

        # Read DataFrame from DuckDB using native API
        try:
            import duckdb
        except ImportError as e:
            raise ImportError(
                "DuckDB is not installed. Install it with: uv add duckdb"
            ) from e

        try:
            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Query and convert to Arrow, then to Polars
                result = conn.execute(
                    f"SELECT * FROM {self.db_schema}.{table_name}"
                ).fetch_arrow_table()

                # Convert Arrow to Polars (Narwhals-compatible)
                import polars as pl

                df = pl.from_arrow(result)
                context.log.info(
                    f"Loaded {len(df)} rows from DuckDB table {self.db_schema}.{table_name}"
                )
                # Return Polars DataFrame (Narwhals-compatible)
                return df
            finally:
                conn.close()
        except Exception as e:
            raise ValueError(
                f"Failed to load table {self.db_schema}.{table_name} from DuckDB: {e}"
            ) from e
