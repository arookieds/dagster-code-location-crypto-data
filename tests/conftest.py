"""Shared pytest configuration and fixtures."""

from __future__ import annotations

import gc
import sys
import warnings
from typing import TYPE_CHECKING, Any, Generator

import pytest

if TYPE_CHECKING:
    from dagster import AssetKey


class FakeLog:
    """Fake logger for testing that doesn't require Dagster infrastructure."""

    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def info(self, msg: str, **kwargs: Any) -> None:
        """Log info message."""
        self.messages.append({"level": "info", "msg": msg, **kwargs})

    def warning(self, msg: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.messages.append({"level": "warning", "msg": msg, **kwargs})

    def error(self, msg: str, **kwargs: Any) -> None:
        """Log error message."""
        self.messages.append({"level": "error", "msg": msg, **kwargs})

    def debug(self, msg: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.messages.append({"level": "debug", "msg": msg, **kwargs})


class FakeOutputContext:
    """Fake output context for testing without Dagster ephemeral instances."""

    def __init__(self, asset_key: AssetKey) -> None:
        self.asset_key = asset_key
        self.log = FakeLog()
        self.run_id = "test-run-id"


class FakeInputContext:
    """Fake input context for testing without Dagster ephemeral instances."""

    def __init__(self, asset_key: AssetKey) -> None:
        self.asset_key = asset_key
        self.log = FakeLog()
        self.upstream_output = None


@pytest.fixture(scope="session", autouse=True)
def suppress_sqlite_cleanup_errors() -> Generator[None, None, None]:
    """Suppress SQLite cleanup errors from Dagster ephemeral instances.

    This runs once per test session (not per test) to set up error suppression
    for the SQLite connection cleanup issues that occur when Dagster's
    ephemeral instances are garbage collected.
    """
    # Suppress the specific SQLite error that occurs during cleanup
    warnings.filterwarnings(
        "ignore",
        message=".*Cannot operate on a closed database.*",
        category=Warning,
    )
    yield


@pytest.fixture(scope="session", autouse=True)
def cleanup_at_session_end() -> Generator[None, None, None]:
    """Run garbage collection once at the end of the test session.

    This is more efficient than running gc.collect() after every test.
    """
    yield
    # Clean up at the end of the session
    gc.collect()


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest to suppress SQLite cleanup errors."""
    # Suppress stderr output for SQLite errors during cleanup
    # These are not actual test failures, just cleanup noise
    warnings.filterwarnings(
        "ignore",
        message=".*Cannot operate on a closed database.*",
    )


# Hook to suppress exception output during interpreter shutdown
_original_excepthook = sys.excepthook


def _quiet_excepthook(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_tb: Any,
) -> None:
    """Suppress SQLite cleanup errors during interpreter shutdown."""
    if "Cannot operate on a closed database" in str(exc_value):
        return  # Suppress this specific error
    _original_excepthook(exc_type, exc_value, exc_tb)


sys.excepthook = _quiet_excepthook
