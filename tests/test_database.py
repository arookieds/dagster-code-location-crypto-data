import os
import tempfile
from collections.abc import Generator

import pytest
from sqlalchemy import inspect
from sqlmodel import Field, SQLModel

from dagster_crypto_data.connectors.database import DatabaseManagement


# Define sample models for testing
class SampleUser(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str


class SampleTeam(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str


@pytest.fixture
def temp_db_file() -> Generator[str]:
    """Provides a temporary file path for a SQLite database."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    # Remove the file so the connector can create it from scratch
    if os.path.exists(path):
        os.remove(path)

    yield path

    # Cleanup after test
    if os.path.exists(path):
        os.remove(path)
    if os.path.exists(f"{path}.db"):  # In case .db was appended
        os.remove(f"{path}.db")


@pytest.fixture
def db_manager(temp_db_file: str) -> DatabaseManagement:
    """Provides a DatabaseManagement instance configured with a temporary SQLite DB."""
    # SQLite URL will be sqlite:///path/to/file.db
    # In DatabaseManagement, it appends .db to the db_name if sqlite
    # So we pass the path without the .db suffix if we want it to match exactly
    base_path = temp_db_file.removesuffix(".db")
    return DatabaseManagement(
        host="localhost", port=5432, db_name=base_path, db_type="sqlite"
    )


def test_url_generation():
    """Verifies connection URL generation for different database types."""
    # SQLite
    dm_sqlite = DatabaseManagement(
        host="localhost", port=5432, db_name="test", db_type="sqlite"
    )
    assert dm_sqlite.url == "sqlite:///test.db"

    # PostgreSQL
    dm_pg = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="crypto",
        username="user",
        password="pass",
        db_type="postgresql",
    )
    assert dm_pg.url == "postgresql://user:pass@localhost:5432/crypto"


def test_table_creation_on_engine_access(db_manager: DatabaseManagement):
    """Verifies that accessing the engine property triggers table creation."""
    # Before accessing engine, check if tables exist (they shouldn't even be in a DB yet)
    # Actually, the file shouldn't even exist yet.

    # Accessing engine triggers create_all
    engine = db_manager.engine
    assert engine is not None

    # Verify tables exist using the inspector
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert "sampleuser" in tables
    assert "sampleteam" in tables


def test_inspect_table_exists(db_manager: DatabaseManagement):
    """Verifies the inspect_table_exists method."""
    # Trigger engine and table creation
    _ = db_manager.engine

    assert db_manager.inspect_table_exists(SampleUser) is True
    assert db_manager.inspect_table_exists(SampleTeam) is True

    # Define a model that wasn't created
    class UncreatedModel(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)

    assert db_manager.inspect_table_exists(UncreatedModel) is False
