from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr, computed_field, field_validator
from sqlalchemy import URL, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import SQLModel, create_engine

if TYPE_CHECKING:
    from sqlalchemy import Engine
logger = logging.getLogger(__name__)


class DatabaseManagement(BaseModel):
    """Manages database connections and schema creation.
    Provides a unified interface for connecting to various database types
    and automatically creates tables based on SQLModel definitions.
    Attributes:
        host: Database server hostname
        port: Database server port
        db_name: Database name (or filename for SQLite/DuckDB)
        username: Database username (empty for file-based DBs)
        password: Database password (empty for file-based DBs)
        db_type: Type of database (sqlite, duckdb, postgresql, etc.)
    Example:
        >>> db = DatabaseManagement(
        ...     host="localhost",
        ...     port=5432,
        ...     db_name="crypto",
        ...     username="user",
        ...     password="secret",
        ...     db_type="postgresql"
        ... )
        >>> engine = db.engine
    """

    host: str
    port: int
    db_name: str
    username: str = Field(default="")
    password: str = Field(default="")
    db_type: str = Field(default="sqlite")
    # Private attribute - not part of model schema
    _engine: Engine | None = PrivateAttr(default=None)
    model_config = {"arbitrary_types_allowed": True}

    @field_validator("db_name")
    @classmethod
    def validate_db_name(cls, v: str) -> str:
        """Prevent path traversal attacks for file-based databases.
        Blocks path traversal and absolute paths, but allows relative
        subdirectories (e.g., "temp/mydb", "testing/mydb").
        Args:
            v: Database name value.
        Returns:
            Validated database name.
        Raises:
            ValueError: If db_name contains path traversal or absolute paths.
        """
        # Prevent path traversal up the directory tree
        if ".." in v:
            raise ValueError(f"db_name cannot contain '..': {v}")
        # Prevent absolute paths (Unix)
        if v.startswith("/"):
            raise ValueError(f"db_name cannot be an absolute path: {v}")
        # Prevent absolute paths (Windows)
        if v.startswith("\\") or (len(v) > 1 and v[1] == ":"):
            raise ValueError(f"db_name cannot be an absolute path: {v}")
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def url(self) -> URL | str:
        """Generate SQLAlchemy connection URL.
        Returns:
            Formatted database connection URL string.
        """
        if self.db_type not in ("sqlite", "duckdb"):
            # Strip whitespace from password (handles newlines from env vars)
            clean_password = self.password.strip()

            # Use SQLAlchemy's URL.create() for secure URL building
            # This avoids exposing credentials in f-strings or logs
            url_obj = URL.create(
                drivername=self.db_type,
                username=self.username,
                password=clean_password,
                host=self.host,
                port=self.port,
                database=self.db_name,
            )
            return url_obj
        return f"{self.db_type}:///{self.db_name}.db"

    @property
    def engine(self) -> Engine:
        """Get or create the SQLAlchemy engine.
        The engine is lazily created on first access and cached per instance.
        Schema and tables are automatically created on first access.
        Returns:
            SQLAlchemy Engine instance.
        Raises:
            RuntimeError: If database connection or schema creation fails.
        """
        if self._engine is None:
            try:
                logger.info(f"Initializing database engine for {self.db_type}")
                self._engine = create_engine(
                    self.url,
                    pool_pre_ping=True,  # Verify connections before use
                    pool_recycle=3600,  # Recycle connections after 1 hour
                )
                self._create_schema_and_tables(self._engine)
            except SQLAlchemyError as e:
                logger.error(f"Failed to create database engine: {e}")
                raise RuntimeError(f"Database connection failed: {e}") from e
        return self._engine

    def inspect_table_exists(self, model: type[SQLModel]) -> bool:
        """Check if a table exists in the database.
        Args:
            model: SQLModel class to check for.
        Returns:
            True if the table exists, False otherwise.
        """
        inspector = inspect(self.engine)
        return inspector.has_table(str(model.__tablename__))

    def _create_schema_and_tables(self, engine: Engine) -> None:
        """Create database schema and tables from SQLModel metadata.
        Args:
            engine: SQLAlchemy engine instance.
        Raises:
            RuntimeError: If schema creation fails.
        """
        try:
            SQLModel.metadata.create_all(engine)
            logger.info(f"Successfully created schema for {self.db_type} database")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database schema: {e}")
            raise RuntimeError(f"Schema creation failed: {e}") from e

    def __repr__(self) -> str:
        """String representation without exposing credentials."""
        return (
            f"DatabaseManagement(host={self.host!r}, port={self.port}, "
            f"db_name={self.db_name!r}, db_type={self.db_type!r})"
        )

    def __str__(self) -> str:
        """String representation without exposing credentials."""
        return self.__repr__()
