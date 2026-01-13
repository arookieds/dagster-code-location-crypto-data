"""Tests for DatabaseConfig resource."""

from dagster_crypto_data.defs.resources.database import DatabaseConfig


class TestDatabaseConfig:
    """Test DatabaseConfig resource initialization and methods."""

    def test_postgresql_configuration(self) -> None:
        """Test PostgreSQL database configuration."""
        config = DatabaseConfig(
            db_type="postgresql",
            host="localhost",
            port=5432,
            db_name="crypto_db",
            username="user",
            password="pass",
            db_schema="public",
        )

        assert config.db_type == "postgresql"
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.db_name == "crypto_db"
        assert config.username == "user"
        assert config.password == "pass"
        assert config.db_schema == "public"

    def test_sqlite_configuration(self) -> None:
        """Test SQLite database configuration."""
        config = DatabaseConfig(
            db_type="sqlite",
            host="",
            port=0,
            db_name="./crypto.db",
            username="",
            password="",
            db_schema="main",
        )

        assert config.db_type == "sqlite"
        assert config.db_name == "./crypto.db"
        assert config.db_schema == "main"

    def test_duckdb_configuration(self) -> None:
        """Test DuckDB database configuration."""
        config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name="./crypto.duckdb",
            username="",
            password="",
            db_schema="crypto_data",
        )

        assert config.db_type == "duckdb"
        assert config.db_name == "./crypto.duckdb"
        assert config.db_schema == "crypto_data"

    def test_get_db_manager_postgresql(self, tmp_path) -> None:
        """Test get_db_manager creates valid DatabaseManagement for PostgreSQL."""
        config = DatabaseConfig(
            db_type="postgresql",
            host="localhost",
            port=5432,
            db_name="crypto_db",
            username="user",
            password="pass",
            db_schema="public",
        )

        db_manager = config.get_db_manager()
        assert db_manager is not None
        assert db_manager.db_type == "postgresql"

    def test_get_db_manager_sqlite(self, tmp_path, monkeypatch) -> None:
        """Test get_db_manager creates valid DatabaseManagement for SQLite."""
        # Use relative path for SQLite
        monkeypatch.chdir(tmp_path)
        config = DatabaseConfig(
            db_type="sqlite",
            host="",
            port=0,
            db_name="test.db",
            username="",
            password="",
            db_schema="main",
        )

        db_manager = config.get_db_manager()
        assert db_manager is not None
        assert db_manager.db_type == "sqlite"

    def test_get_db_manager_duckdb(self, tmp_path, monkeypatch) -> None:
        """Test get_db_manager creates valid DatabaseManagement for DuckDB."""
        # Use relative path for DuckDB
        monkeypatch.chdir(tmp_path)
        config = DatabaseConfig(
            db_type="duckdb",
            host="",
            port=0,
            db_name="test.duckdb",
            username="",
            password="",
            db_schema="crypto_data",
        )

        db_manager = config.get_db_manager()
        assert db_manager is not None
        assert db_manager.db_type == "duckdb"
