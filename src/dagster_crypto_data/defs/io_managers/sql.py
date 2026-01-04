"""SQL IO Manager for PostgreSQL and SQLite using DatabaseManagement connector."""

from __future__ import annotations

from typing import TYPE_CHECKING

import narwhals as nw
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field

from dagster_crypto_data.defs.connectors.database import DatabaseManagement

if TYPE_CHECKING:
    from narwhals.typing import FrameT


class SQLIOManager(ConfigurableIOManager):
    """Store DataFrames in SQL databases using DatabaseManagement and ADBC/Arrow.

    This IO manager leverages the existing DatabaseManagement connector and uses
    ADBC (Arrow Database Connectivity) for efficient, zero-copy data transfers.
    It stores Polars DataFrames as SQL tables.

    Supports any SQLAlchemy-compatible database including PostgreSQL, MySQL,
    MariaDB, Oracle, SQL Server, SQLite, and more.

    Attributes:
        db_type: Database type (e.g., 'postgresql', 'mysql', 'sqlite')
        host: Database host (required for server-based databases)
        port: Database port (required for server-based databases)
        database: Database name or SQLite file path
        username: Database username (required for server-based databases)
        password: Database password (required for server-based databases, use EnvVar)
        schema: Database schema name (default: public, not used for SQLite)

    Example:
        ```python
        from dagster import Definitions, EnvVar
        from dagster_crypto_data.defs.io_managers import SQLIOManager

        # PostgreSQL (production)
        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": SQLIOManager(
                    db_type="postgresql",
                    host=EnvVar("POSTGRESQL_HOST"),
                    port=5432,
                    database=EnvVar("POSTGRESQL_DATABASE"),
                    username=EnvVar("POSTGRESQL_USER"),
                    password=EnvVar("POSTGRESQL_PASSWORD"),
                    schema="analytics",
                ),
            },
        )

        # MySQL (production)
        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": SQLIOManager(
                    db_type="mysql",
                    host=EnvVar("MYSQL_HOST"),
                    port=3306,
                    database=EnvVar("MYSQL_DATABASE"),
                    username=EnvVar("MYSQL_USER"),
                    password=EnvVar("MYSQL_PASSWORD"),
                    schema="analytics",
                ),
            },
        )

        # SQLite (local development)
        defs = Definitions(
            assets=[...],
            resources={
                "io_manager": SQLIOManager(
                    db_type="sqlite",
                    db_name="./data/local",
                ),
            },
        )
        ```
    """

    db_type: str = Field(
        description="Database type (e.g., 'postgresql', 'mysql', 'mariadb', 'sqlite')",
    )
    host: str = Field(
        default="localhost",
        description="Database host (required for PostgreSQL)",
    )
    port: int = Field(
        default=5432,
        description="Database port (required for PostgreSQL)",
    )
    db_name: str = Field(description="Database name or SQLite file path")
    username: str = Field(
        default="",
        description="Database username (required for PostgreSQL)",
    )
    password: str | None = Field(
        default=None,
        description="Database password (required for PostgreSQL, use EnvVar for security)",
    )
    schema: str = Field(
        default="public",
        description="Database schema name",
    )

    def _get_db_manager(self) -> DatabaseManagement:
        """Get DatabaseManagement instance.

        Returns:
            Configured DatabaseManagement instance

        Raises:
            ValueError: If required fields are missing for PostgreSQL
        """
        if self.db_type == "postgresql":
            if not all([self.host, self.port, self.username, self.password]):
                raise ValueError(
                    "PostgreSQL requires host, port, username, and password"
                )
            return DatabaseManagement(
                db_type="postgresql",
                host=self.host,
                port=self.port,
                db_name=self.db_name,
                username=self.username,
                password=self.password if self.password else "",
            )
        else:  # sqlite
            return DatabaseManagement(
                db_type="sqlite",
                host="",
                port=0,
                db_name=self.db_name,
                username="",
                password="",
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

    def _get_full_table_name(self, table_name: str) -> str:
        """Get the full table name with schema prefix if applicable.

        SQLite doesn't support schemas, so we only use schema prefix for PostgreSQL.

        Args:
            table_name: Base table name

        Returns:
            Full table name (e.g., "public.table" for PostgreSQL, "table" for SQLite)
        """
        if self.db_type == "postgresql":
            return f"{self.schema}.{table_name}"
        else:  # sqlite
            return table_name

    def handle_output(self, context: OutputContext, obj: FrameT) -> None:
        """Store a DataFrame in SQL database.

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
                f"SQLIOManager expects a DataFrame, got {type(obj).__name__}"
            ) from e

        db_manager = self._get_db_manager()
        table_name = self._get_table_name(context)
        full_table_name = self._get_full_table_name(table_name)

        # Get SQLAlchemy engine
        engine = db_manager.engine

        # Write DataFrame to SQL
        # Convert to native DataFrame for database operations
        native_df = nw.to_native(df)

        # Build connection URI string for ADBC
        # SQLAlchemy URL.render_as_string() with hide_password=False gives us the real URI
        # SQLite is file-based and doesn't need authentication, all others do
        if self.db_type == "sqlite":
            connection_uri = str(engine.url)
        else:  # postgresql, mysql, mariadb, oracle, mssql, etc.
            connection_uri = engine.url.render_as_string(hide_password=False)

        # Use Polars write_database with ADBC engine for Arrow-based writes
        native_df.write_database(
            table_name=full_table_name,  # Use full table name with schema
            connection=connection_uri,
            if_table_exists="replace",
            engine="adbc",  # Use ADBC/Arrow for efficient writes
        )

        row_count = len(native_df) if hasattr(native_df, "__len__") else 0
        context.log.info(
            f"Stored {row_count} rows to {self.db_type} table {full_table_name} using ADBC"
        )

    def load_input(self, context: InputContext) -> FrameT:
        """Load a DataFrame from SQL database.

        Args:
            context: Dagster input context

        Returns:
            Polars DataFrame loaded from SQL database (Narwhals-compatible)

        Raises:
            ValueError: If table doesn't exist
        """
        db_manager = self._get_db_manager()
        table_name = self._get_table_name(context)
        full_table_name = self._get_full_table_name(table_name)
        engine = db_manager.engine

        # Read DataFrame from SQL
        # Use SQLAlchemy connection directly (Polars auto-detects)
        try:
            import polars as pl

            df = pl.read_database(
                query=f"SELECT * FROM {full_table_name}",
                connection=engine,
            )
            context.log.info(
                f"Loaded {len(df)} rows from {self.db_type} table {full_table_name}"
            )
            # Return Polars DataFrame (Narwhals-compatible)
            return df  # type: ignore[return-value]
        except Exception as e:
            raise ValueError(
                f"Failed to load table {full_table_name} from {self.db_type}: {e}"
            ) from e
