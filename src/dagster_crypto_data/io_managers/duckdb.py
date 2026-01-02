"""DuckDB IO Manager for local analytics."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

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
        from dagster_crypto_data.io_managers import DuckDBIOManager

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
    schema: str = Field(
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

        # Convert to native Polars for database operations
        # Polars has native ADBC support for DuckDB
        import polars as pl

        native_df = nw.to_native(df)
        if not isinstance(native_df, pl.DataFrame):
            # If not Polars, convert via Arrow
            native_df = pl.from_arrow(df.to_arrow())

        # Write DataFrame to DuckDB using ADBC
        connection_string = f"duckdb:///{self.db_path}"

        # Create table (replace if exists)
        native_df.write_database(
            table_name=table_name,
            connection=connection_string,
            if_table_exists="replace",
            engine="adbc",
        )

        context.log.info(
            f"Stored {len(df)} rows to DuckDB table {self.schema}.{table_name}"
        )

    def load_input(self, context: InputContext) -> FrameT:
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
        connection_string = f"duckdb:///{self.db_path}"

        # Read DataFrame from DuckDB using ADBC
        try:
            import polars as pl

            df = pl.read_database(
                query=f"SELECT * FROM {self.schema}.{table_name}",
                connection=connection_string,
                engine="adbc",
            )
            context.log.info(
                f"Loaded {len(df)} rows from DuckDB table {self.schema}.{table_name}"
            )
            # Return Polars DataFrame (Narwhals-compatible)
            return df  # type: ignore[return-value]
        except Exception as e:
            raise ValueError(
                f"Failed to load table {self.schema}.{table_name} from DuckDB: {e}"
            ) from e
