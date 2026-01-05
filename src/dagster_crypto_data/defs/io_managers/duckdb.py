"""DuckDB IO Manager for local analytics."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import narwhals as nw
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

if TYPE_CHECKING:
    from narwhals.typing import FrameT


class DuckDBIOManager(ConfigurableIOManager):
    """Store DataFrames in DuckDB for local analytics.

    This IO manager is designed for local development and testing with
    DuckDB, an in-process analytical database. It accepts Narwhals-compatible
    DataFrames and stores them as DuckDB tables.

    Attributes:
        db_path: Path to DuckDB database file (default: ./data/local.duckdb)
        schema: Database schema name (default: main)

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
        default="main",
        description="Database schema name",
    )

    def _get_table_name(self, context: OutputContext | InputContext) -> str:
        """Get the table name for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            Table name (e.g., "extract_binance_ohlcv")
        """
        # Use asset key path joined with underscores
        return "_".join(context.asset_key.path)

    def _ensure_db_exists(self) -> None:
        """Ensure the database file and parent directories exist."""
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

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
            df = nw.from_native(obj)
        except Exception as e:
            raise TypeError(
                f"DuckDBIOManager expects a DataFrame, got {type(obj).__name__}"
            ) from e

        self._ensure_db_exists()
        table_name = self._get_table_name(context)

        # Use native DuckDB API for writing
        # DuckDB can directly consume Arrow tables
        try:
            import duckdb
        except ImportError as e:
            raise ImportError(
                "DuckDB is not installed. Install it with: uv add duckdb"
            ) from e

        # Convert to Arrow for DuckDB consumption
        # Get native DataFrame first, then convert to Arrow
        native_df = nw.to_native(df)
        arrow_table = native_df.to_arrow()
        row_count = arrow_table.num_rows

        # Connect to DuckDB and write table
        conn = duckdb.connect(self.db_path)
        try:
            # Create schema if it doesn't exist (unless it's 'main')
            if self.db_schema != "main":
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}")

            # Create or replace table from Arrow
            conn.execute(
                f"CREATE OR REPLACE TABLE {self.db_schema}.{table_name} AS SELECT * FROM arrow_table"
            )
            context.log.info(
                f"Stored {row_count} rows to DuckDB table {self.db_schema}.{table_name}"
            )
        finally:
            conn.close()

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
