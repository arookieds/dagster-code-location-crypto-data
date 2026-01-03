"""KuzuDB IO Manager for graph database operations."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import narwhals as nw
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

if TYPE_CHECKING:
    from narwhals.typing import FrameT


class KuzuDBIOManager(ConfigurableIOManager):
    """Store DataFrames in KuzuDB, an embedded graph database.

    KuzuDB is an embedded graph database optimized for OLAP-style graph analytics.
    This IO manager converts Polars DataFrames to KuzuDB node tables for graph
    operations.

    Note: This is experimental and designed for testing graph database capabilities.

    Attributes:
        db_path: Path to KuzuDB database directory (default: ./data/kuzu)
        node_label: Default node label for tables (default: None, uses table name)

    Example:
        ```python
        from dagster import Definitions
        from dagster_crypto_data.defs.io_managers import KuzuDBIOManager

        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": KuzuDBIOManager(
                    db_path="./data/graph.kuzu",
                ),
            },
        )
        ```

    Graph Usage Example:
        ```python
        import kuzu

        # After materializing assets with KuzuDB IO Manager
        db = kuzu.Database("./data/graph.kuzu")
        conn = kuzu.Connection(db)

        # Query graph
        result = conn.execute(
            "MATCH (n:ohlcv_data) WHERE n.symbol = 'BTC/USDT' RETURN n"
        )
        ```
    """

    db_path: str = Field(
        default="./data/kuzu",
        description="Path to KuzuDB database directory",
    )
    node_label: str | None = Field(
        default=None,
        description="Default node label (None uses table name)",
    )

    def _get_node_table_name(self, context: OutputContext | InputContext) -> str:
        """Get the node table name for an asset.

        Args:
            context: Dagster context with asset key information

        Returns:
            Node table name (e.g., "ohlcv_data")
        """
        if self.node_label:
            return self.node_label
        # Use asset key path joined with underscores
        return "_".join(context.asset_key.path)

    def _ensure_db_exists(self) -> None:
        """Ensure the database directory exists."""
        db_dir = Path(self.db_path)
        db_dir.mkdir(parents=True, exist_ok=True)

    def _get_kuzu_connection(self) -> tuple[Any, Any]:
        """Get KuzuDB database and connection.

        Returns:
            Tuple of (database, connection)
        """
        try:
            import kuzu
        except ImportError as e:
            raise ImportError(
                "KuzuDB is not installed. Install it with: uv add kuzu"
            ) from e

        self._ensure_db_exists()
        db = kuzu.Database(self.db_path)
        conn = kuzu.Connection(db)
        return db, conn

    def handle_output(self, context: OutputContext, obj: FrameT) -> None:
        """Store a DataFrame as a KuzuDB node table.

        Args:
            context: Dagster output context
            obj: Narwhals-compatible DataFrame to store

        Raises:
            TypeError: If obj is not a DataFrame
            ImportError: If kuzu is not installed
        """
        # Convert to Narwhals DataFrame for compatibility
        try:
            df = nw.from_native(obj)
        except Exception as e:
            raise TypeError(
                f"KuzuDBIOManager expects a DataFrame, got {type(obj).__name__}"
            ) from e

        # Convert to native Polars for operations
        import polars as pl

        native_df = nw.to_native(df)
        # Ensure we have a Polars DataFrame
        if not isinstance(native_df, pl.DataFrame):
            # Convert via Arrow if not Polars
            arrow_table = df.to_arrow()  # type: ignore[attr-defined]
            native_df = pl.from_arrow(arrow_table)

        db, conn = self._get_kuzu_connection()
        table_name = self._get_node_table_name(context)

        # Drop existing table if it exists
        try:
            conn.execute(f"DROP TABLE {table_name}")
            context.log.info(f"Dropped existing table {table_name}")
        except Exception:
            # Table doesn't exist, that's fine
            pass

        # Infer schema from Polars DataFrame
        # Create node table schema
        schema_parts = []
        primary_key = None

        for col_name, dtype in zip(native_df.columns, native_df.dtypes):  # type: ignore[attr-defined]
            # Map Polars types to KuzuDB types
            if dtype == pl.Int64 or dtype == pl.Int32:
                kuzu_type = "INT64"
            elif dtype == pl.Float64 or dtype == pl.Float32:
                kuzu_type = "DOUBLE"
            elif dtype == pl.Utf8:
                kuzu_type = "STRING"
            elif dtype == pl.Boolean:
                kuzu_type = "BOOLEAN"
            elif dtype == pl.Date:
                kuzu_type = "DATE"
            elif dtype == pl.Datetime:
                kuzu_type = "TIMESTAMP"
            else:
                kuzu_type = "STRING"  # Default to STRING

            schema_parts.append(f"{col_name} {kuzu_type}")

            # Use first column as primary key if not set
            if primary_key is None:
                primary_key = col_name

        schema_str = ", ".join(schema_parts)

        # Create node table
        create_table_query = (
            f"CREATE NODE TABLE {table_name}({schema_str}, PRIMARY KEY({primary_key}))"
        )
        conn.execute(create_table_query)
        context.log.info(f"Created KuzuDB node table: {table_name}")

        # Insert data from Polars DataFrame
        # Convert DataFrame to list of tuples for insertion
        for row in native_df.iter_rows():  # type: ignore[attr-defined]
            # Build INSERT query
            values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
            insert_query = f"CREATE (:{table_name} {{{', '.join([f'{col}: {val}' for col, val in zip(native_df.columns, row)])}}});"  # type: ignore[attr-defined]
            try:
                conn.execute(insert_query)
            except Exception as e:
                context.log.warning(f"Failed to insert row: {e}")
                continue

        context.log.info(
            f"Stored {len(native_df)} rows to KuzuDB node table {table_name}"
        )

    def load_input(self, context: InputContext) -> FrameT:
        """Load a DataFrame from KuzuDB node table.

        Args:
            context: Dagster input context

        Returns:
            Polars DataFrame loaded from KuzuDB (Narwhals-compatible)

        Raises:
            FileNotFoundError: If database doesn't exist
            ValueError: If table doesn't exist
        """
        db_dir = Path(self.db_path)
        if not db_dir.exists():
            raise FileNotFoundError(
                f"KuzuDB database not found: {self.db_path}. "
                f"Make sure the upstream asset has been materialized."
            )

        db, conn = self._get_kuzu_connection()
        table_name = self._get_node_table_name(context)

        # Query all nodes from table
        try:
            query = f"MATCH (n:{table_name}) RETURN n.*"
            result = conn.execute(query)

            # Convert to Polars DataFrame
            df = result.get_as_df()

            context.log.info(
                f"Loaded {len(df)} rows from KuzuDB node table {table_name}"
            )
            return df
        except Exception as e:
            raise ValueError(
                f"Failed to load node table {table_name} from KuzuDB: {e}"
            ) from e
