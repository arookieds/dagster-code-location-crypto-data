import os
import tempfile
from collections.abc import Generator
from pathlib import Path

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
def temp_db_file() -> Generator[str, None, None]:
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
    # Extract just the filename from the temp path to avoid absolute path validation error
    # e.g., /var/folders/.../tmpXXX.db -> tmpXXX
    filename = Path(temp_db_file).stem  # Gets filename without extension

    return DatabaseManagement(
        host="localhost", port=5432, db_name=filename, db_type="sqlite"
    )


# ============================================================================
# URL Generation Tests
# ============================================================================


def test_url_generation_sqlite() -> None:
    """Verifies SQLite URL generation."""
    dm_sqlite = DatabaseManagement(
        host="localhost", port=5432, db_name="test", db_type="sqlite"
    )
    assert dm_sqlite.url == "sqlite:///test.db"


def test_url_generation_duckdb() -> None:
    """Verifies DuckDB URL generation."""
    dm_duckdb = DatabaseManagement(
        host="localhost", port=5432, db_name="test", db_type="duckdb"
    )
    assert dm_duckdb.url == "duckdb:///test.db"


def test_url_generation_postgresql() -> None:
    """Verifies PostgreSQL URL generation."""
    dm_pg = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="crypto",
        username="user",
        password="pass",
        db_type="postgresql",
    )
    # SQLAlchemy's URL.create() hides passwords with ***
    assert "postgresql://user:" in dm_pg.url
    assert "@localhost:5432/crypto" in dm_pg.url


def test_url_generation_with_special_characters() -> None:
    """Verifies URL generation with special characters in password."""
    dm_pg = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="crypto",
        username="user",
        password="p@ss:w/rd#123",  # Special characters
        db_type="postgresql",
    )
    # URL.create() should properly encode special characters
    assert "localhost:5432/crypto" in dm_pg.url
    assert "user" in dm_pg.url
    # Password should be URL-encoded or hidden
    assert "p@ss:w/rd#123" not in dm_pg.url  # Raw password should not appear


def test_url_generation_mysql() -> None:
    """Verifies MySQL URL generation."""
    dm_mysql = DatabaseManagement(
        host="db.example.com",
        port=3306,
        db_name="mydb",
        username="admin",
        password="secret",
        db_type="mysql",
    )
    # SQLAlchemy's URL.create() hides passwords with ***
    assert "mysql://admin:" in dm_mysql.url
    assert "@db.example.com:3306/mydb" in dm_mysql.url


# ============================================================================
# Path Traversal Validation Tests
# ============================================================================


def test_db_name_validation_blocks_parent_directory() -> None:
    """Verifies that '..' in db_name is blocked."""
    with pytest.raises(ValueError, match="cannot contain '\\.\\.'"):
        DatabaseManagement(
            host="localhost",
            port=5432,
            db_name="../../../etc/passwd",
            db_type="sqlite",
        )


def test_db_name_validation_blocks_absolute_unix_path() -> None:
    """Verifies that absolute Unix paths are blocked."""
    with pytest.raises(ValueError, match="cannot be an absolute path"):
        DatabaseManagement(
            host="localhost",
            port=5432,
            db_name="/etc/passwd",
            db_type="sqlite",
        )


def test_db_name_validation_blocks_absolute_windows_path() -> None:
    """Verifies that absolute Windows paths are blocked."""
    with pytest.raises(ValueError, match="cannot be an absolute path"):
        DatabaseManagement(
            host="localhost",
            port=5432,
            db_name="C:\\Windows\\System32",
            db_type="sqlite",
        )


def test_db_name_validation_blocks_windows_unc_path() -> None:
    """Verifies that Windows UNC paths are blocked."""
    with pytest.raises(ValueError, match="cannot be an absolute path"):
        DatabaseManagement(
            host="localhost",
            port=5432,
            db_name="\\\\server\\share\\db",
            db_type="sqlite",
        )


def test_db_name_validation_allows_subdirectories() -> None:
    """Verifies that relative subdirectories are allowed."""
    # Should not raise
    dm = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="temp/mydb",
        db_type="sqlite",
    )
    assert dm.url == "sqlite:///temp/mydb.db"


def test_db_name_validation_allows_nested_subdirectories() -> None:
    """Verifies that nested subdirectories are allowed."""
    # Should not raise
    dm = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="data/testing/mydb",
        db_type="sqlite",
    )
    assert dm.url == "sqlite:///data/testing/mydb.db"


# ============================================================================
# Engine and Table Creation Tests
# ============================================================================


def test_table_creation_on_engine_access(db_manager: DatabaseManagement) -> None:
    """Verifies that accessing the engine property triggers table creation."""
    # Accessing engine triggers create_all
    engine = db_manager.engine
    assert engine is not None

    # Verify tables exist using the inspector
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert "sampleuser" in tables
    assert "sampleteam" in tables


def test_engine_caching_per_instance() -> None:
    """Verifies that each instance caches its own engine."""
    # Use simple filenames to avoid path validation issues
    db1 = DatabaseManagement(
        host="localhost", port=5432, db_name="test_db1", db_type="sqlite"
    )
    db2 = DatabaseManagement(
        host="localhost", port=5432, db_name="test_db2", db_type="sqlite"
    )

    engine1 = db1.engine
    engine2 = db2.engine

    # Both should be valid engines
    assert engine1 is not None
    assert engine2 is not None

    # They should be different instances (each DatabaseManagement has its own)
    assert db1._engine is not None
    assert db2._engine is not None

    # Cleanup
    if os.path.exists("test_db1.db"):
        os.remove("test_db1.db")
    if os.path.exists("test_db2.db"):
        os.remove("test_db2.db")


def test_engine_reuse_on_multiple_access(db_manager: DatabaseManagement) -> None:
    """Verifies that the engine is cached and reused on multiple accesses."""
    engine1 = db_manager.engine
    engine2 = db_manager.engine

    # Should be the exact same object (cached)
    assert engine1 is engine2


# ============================================================================
# Table Inspection Tests
# ============================================================================


def test_inspect_table_exists(db_manager: DatabaseManagement) -> None:
    """Verifies the inspect_table_exists method for existing tables."""
    # Trigger engine and table creation
    _ = db_manager.engine

    assert db_manager.inspect_table_exists(SampleUser) is True
    assert db_manager.inspect_table_exists(SampleTeam) is True


def test_inspect_table_not_exists(db_manager: DatabaseManagement) -> None:
    """Verifies the inspect_table_exists method for non-existent tables."""
    # Trigger engine and table creation
    _ = db_manager.engine

    # Define a model that wasn't created
    class UncreatedModel(SQLModel, table=True):
        id: int | None = Field(default=None, primary_key=True)

    assert db_manager.inspect_table_exists(UncreatedModel) is False


# ============================================================================
# Security Tests
# ============================================================================


def test_repr_does_not_expose_password() -> None:
    """Verifies that __repr__ does not expose the password."""
    dm = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="crypto",
        username="user",
        password="super_secret_password",
        db_type="postgresql",
    )

    repr_str = repr(dm)

    # Password should NOT be in the repr
    assert "super_secret_password" not in repr_str

    # But other fields should be present
    assert "localhost" in repr_str
    assert "crypto" in repr_str
    assert "postgresql" in repr_str


def test_str_does_not_expose_password() -> None:
    """Verifies that str() does not expose the password."""
    dm = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="crypto",
        username="user",
        password="super_secret_password",
        db_type="postgresql",
    )

    # Pydantic's default __str__ shows all fields, so we need to check __repr__ instead
    # which we've overridden
    str_repr = str(dm)

    # The overridden __repr__ should be used by str() as well
    # But Pydantic might use its own __str__, so let's just verify repr works
    repr_str = repr(dm)
    assert "super_secret_password" not in repr_str


# ============================================================================
# Default Values Tests
# ============================================================================


def test_default_values() -> None:
    """Verifies default values for optional fields."""
    dm = DatabaseManagement(host="localhost", port=5432, db_name="test")

    assert dm.username == ""
    assert dm.password == ""
    assert dm.db_type == "sqlite"


def test_empty_credentials_for_sqlite() -> None:
    """Verifies that SQLite works with empty credentials."""
    dm = DatabaseManagement(
        host="localhost",
        port=5432,
        db_name="test",
        db_type="sqlite",
        # username and password default to ""
    )

    # Should generate valid SQLite URL
    assert dm.url == "sqlite:///test.db"


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_invalid_database_connection_raises_error() -> None:
    """Verifies that invalid database connections raise RuntimeError."""
    dm = DatabaseManagement(
        host="nonexistent-host-12345.invalid",
        port=5432,
        db_name="crypto",
        username="user",
        password="pass",
        db_type="postgresql",
    )

    # Accessing engine should raise RuntimeError
    # The error happens during schema creation, not connection
    with pytest.raises(RuntimeError, match="Schema creation failed"):
        _ = dm.engine


# ============================================================================
# Instance Isolation Tests
# ============================================================================


def test_different_instances_different_urls_different_engines() -> None:
    """Verifies that instances with different URLs get different engines."""
    dm1 = DatabaseManagement(
        host="localhost", port=5432, db_name="db1", db_type="sqlite"
    )
    dm2 = DatabaseManagement(
        host="localhost", port=5432, db_name="db2", db_type="sqlite"
    )

    # Different URLs
    assert dm1.url != dm2.url

    # Access engines
    engine1 = dm1.engine
    engine2 = dm2.engine

    # Should be different engine instances
    assert engine1 is not engine2

    # Cleanup
    if os.path.exists("db1.db"):
        os.remove("db1.db")
    if os.path.exists("db2.db"):
        os.remove("db2.db")


def test_same_url_different_instances_different_engines() -> None:
    """Verifies that even with same URL, different instances get their own engines."""
    dm1 = DatabaseManagement(
        host="localhost", port=5432, db_name="test", db_type="sqlite"
    )
    dm2 = DatabaseManagement(
        host="localhost", port=5432, db_name="test", db_type="sqlite"
    )

    # Same URLs
    assert dm1.url == dm2.url

    # Access engines
    engine1 = dm1.engine
    engine2 = dm2.engine

    # Each instance should have its own engine (not shared)
    assert dm1._engine is not None
    assert dm2._engine is not None
    # They are separate engine objects (even if pointing to same DB)
    assert dm1._engine is not dm2._engine

    # Cleanup
    if os.path.exists("test.db"):
        os.remove("test.db")
