"""SQL IO Manager for PostgreSQL and SQLite using DatabaseManagement connector."""

from __future__ import annotations

import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext
from pydantic import Field, SecretStr

from dagster_crypto_data.connectors.database import DatabaseManagement


class SQLIOManager(ConfigurableIOManager):
    """Store DataFrames in SQL databases (PostgreSQL, SQLite) using DatabaseManagement.

    This IO manager leverages the existing DatabaseManagement connector which
    supports both PostgreSQL and SQLite. It stores Polars DataFrames as SQL tables.

    Attributes:
        db_type: Database type ('postgresql' or 'sqlite')
        host: Database host (for PostgreSQL)
        port: Database port (for PostgreSQL)
        database: Database name or SQLite file path
        username: Database username (for PostgreSQL)
        password: Database password (for PostgreSQL)
        schema: Database schema name (default: public)

    Example:
        ```python
        from dagster import Definitions, EnvVar
        from dagster_crypto_data.io_managers import SQLIOManager

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

        # SQLite (local)
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
        description="Database type: 'postgresql' or 'sqlite'",
        pattern="^(postgresql|sqlite)$",
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
    password: SecretStr | None = Field(
        default=None,
        description="Database password (required for PostgreSQL)",
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
                password=self.password.get_secret_value() if self.password else "",
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

    def handle_output(self, context: OutputContext, obj: pl.DataFrame) -> None:
        """Store a Polars DataFrame in SQL database.

        Args:
            context: Dagster output context
            obj: Polars DataFrame to store

        Raises:
            TypeError: If obj is not a Polars DataFrame
        """
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(
                f"SQLIOManager expects polars.DataFrame, got {type(obj).__name__}"
            )

        db_manager = self._get_db_manager()
        table_name = self._get_table_name(context)

        # Get SQLAlchemy engine
        engine = db_manager.engine

        # Write DataFrame to SQL
        # Using Polars' write_database method
        connection_string = str(engine.url)

        obj.write_database(
            table_name=table_name,
            connection=connection_string,
            if_table_exists="replace",
        )

        context.log.info(
            f"Stored {len(obj)} rows to {self.db_type} table {self.schema}.{table_name}"
        )

    def load_input(self, context: InputContext) -> pl.DataFrame:
        """Load a Polars DataFrame from SQL database.

        Args:
            context: Dagster input context

        Returns:
            Polars DataFrame loaded from SQL database

        Raises:
            ValueError: If table doesn't exist
        """
        db_manager = self._get_db_manager()
        table_name = self._get_table_name(context)
        engine = db_manager.engine

        # Read DataFrame from SQL
        connection_string = str(engine.url)

        try:
            df = pl.read_database(
                query=f"SELECT * FROM {self.schema}.{table_name}",
                connection=connection_string,
            )
            context.log.info(
                f"Loaded {len(df)} rows from {self.db_type} table "
                f"{self.schema}.{table_name}"
            )
            return df
        except Exception as e:
            raise ValueError(
                f"Failed to load table {self.schema}.{table_name} "
                f"from {self.db_type}: {e}"
            ) from e
