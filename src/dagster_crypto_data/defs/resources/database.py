"""Database configuration resource for Dagster assets.

This module provides a centralized database configuration resource that can be
shared across multiple IO managers (FilesystemIOManager, S3IOManager, SQLIOManager).

This eliminates the need to duplicate database configuration in multiple places
and provides a clean separation of concerns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dagster import ConfigurableResource
from pydantic import Field

if TYPE_CHECKING:
    from dagster_crypto_data.defs.connectors.database import DatabaseManagement


class DatabaseConfig(ConfigurableResource):
    """Shared database configuration resource.

    Provides centralized database connection configuration that can be
    shared across multiple IO managers and components.

    This resource manages database credentials and connection parameters,
    and provides a DatabaseManagement instance for actual database operations.

    Attributes:
        db_type: Database type (e.g., 'postgresql', 'sqlite')
        host: Database host (empty string for SQLite)
        port: Database port (0 for SQLite)
        db_name: Database name (file path for SQLite)
        username: Database username (empty string for SQLite)
        password: Database password (use SecretStr for security)
        db_schema: Database schema name (default: public for PostgreSQL, main for SQLite)

    Example:
        ```python
        from dagster import Definitions
        from dagster_crypto_data.defs.resources import DatabaseConfig

        db_config = DatabaseConfig(
            db_type="postgresql",
            host="localhost",
            port=5432,
            db_name="crypto_data",
            username="postgres",
            password=SecretStr("password"),
            db_schema="crypto_data",
        )

        defs = Definitions(
            assets=[...],
            resources={
                "database": db_config,
                "io_manager": FilesystemIOManager(database=db_config),
            },
        )
        ```
    """

    db_type: str = Field(
        default="postgresql",
        description="Database type (postgresql, sqlite, mysql, mariadb, etc.)",
    )
    host: str = Field(
        default="localhost",
        description="Database host (empty string for SQLite)",
    )
    port: int = Field(
        default=5432,
        description="Database port (0 for SQLite)",
    )
    db_name: str = Field(
        description="Database name (file path for SQLite)",
    )
    username: str = Field(
        default="",
        description="Database username (empty for SQLite)",
    )
    password: str | None = Field(
        default=None,
        description="Database password (use EnvVar for security)",
    )
    db_schema: str = Field(
        default="public",
        description="Database schema name (public for PostgreSQL, main for SQLite)",
    )

    def get_db_manager(self) -> DatabaseManagement:
        """Get DatabaseManagement instance with this configuration.

        Creates and returns a DatabaseManagement instance configured with
        the settings from this resource. This instance can be used to:
        - Execute database queries
        - Get SQLAlchemy engine for advanced operations
        - Create tables and schemas

        Returns:
            Configured DatabaseManagement instance

        Raises:
            ImportError: If DatabaseManagement cannot be imported
            ValueError: If configuration is invalid for the specified db_type
        """
        from dagster_crypto_data.defs.connectors.database import DatabaseManagement

        return DatabaseManagement(
            db_type=self.db_type,
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            username=self.username,
            password=self.password if self.password else "",
        )
