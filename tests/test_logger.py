"""Tests for the structured logging module."""

import logging

import pytest
import structlog

from dagster_crypto_data.defs.utils.logger import get_logger


@pytest.fixture(autouse=True)
def reset_logging_config() -> None:
    """Reset logging configuration before each test.

    This ensures tests don't interfere with each other by
    resetting the global _configured flag.
    """
    # Reset the module-level _configured flag
    import dagster_crypto_data.utils.logger as logger_module

    logger_module._configured = False

    # Clear any existing structlog configuration
    structlog.reset_defaults()

    # Reset logging handlers
    logging.root.handlers = []
    logging.root.setLevel(logging.WARNING)  # Reset to default


# ============================================================================
# Basic Functionality Tests
# ============================================================================


def test_get_logger_returns_bound_logger() -> None:
    """Verifies that get_logger returns a structlog logger instance."""
    logger = get_logger(__name__)
    # structlog returns BoundLoggerLazyProxy, not BoundLogger directly
    assert logger is not None
    assert hasattr(logger, "info")
    assert hasattr(logger, "error")
    assert hasattr(logger, "debug")


def test_get_logger_with_custom_name() -> None:
    """Verifies that logger name can be customized."""
    logger = get_logger("custom.module.name")
    assert logger is not None


def test_get_logger_default_parameters() -> None:
    """Verifies that get_logger works with default parameters."""
    # Should not raise with defaults
    logger = get_logger(__name__)
    assert logger is not None


# ============================================================================
# Configuration Tests
# ============================================================================


def test_logging_configured_only_once() -> None:
    """Verifies that logging is configured only on first call."""
    import dagster_crypto_data.utils.logger as logger_module

    # Initially not configured
    assert logger_module._configured is False

    # First call configures
    logger1 = get_logger("module1")
    assert logger_module._configured is True

    # Second call doesn't reconfigure
    logger2 = get_logger("module2")
    assert logger_module._configured is True

    # Both loggers should be valid
    assert logger1 is not None
    assert logger2 is not None


def test_multiple_loggers_same_configuration() -> None:
    """Verifies that multiple loggers share the same configuration."""
    logger1 = get_logger("module1", log_level="DEBUG", use_json=False)
    logger2 = get_logger("module2", log_level="INFO", use_json=True)

    # Both should be valid (second call's params are ignored)
    assert logger1 is not None
    assert logger2 is not None


# ============================================================================
# Log Level Tests
# ============================================================================


def test_log_level_info() -> None:
    """Verifies INFO log level configuration."""
    logger = get_logger(__name__, log_level="INFO")
    assert logger is not None
    # Just verify the logger works
    logger.info("test")


def test_log_level_debug() -> None:
    """Verifies DEBUG log level configuration."""
    logger = get_logger(__name__, log_level="DEBUG")
    assert logger is not None
    logger.debug("test")


def test_log_level_warning() -> None:
    """Verifies WARNING log level configuration."""
    logger = get_logger(__name__, log_level="WARNING")
    assert logger is not None
    assert logging.root.level == logging.WARNING


def test_log_level_error() -> None:
    """Verifies ERROR log level configuration."""
    logger = get_logger(__name__, log_level="ERROR")
    assert logger is not None
    logger.error("test")


def test_log_level_critical() -> None:
    """Verifies CRITICAL log level configuration."""
    logger = get_logger(__name__, log_level="CRITICAL")
    assert logger is not None
    logger.critical("test")


def test_log_level_case_insensitive() -> None:
    """Verifies that log level is case insensitive."""
    # Lowercase should work
    logger1 = get_logger(__name__, log_level="info")
    assert logger1 is not None
    logger1.info("test")


def test_log_level_mixed_case() -> None:
    """Verifies that mixed case log level works."""
    logger = get_logger(__name__, log_level="DeBuG")
    assert logger is not None
    logger.debug("test")


# ============================================================================
# Renderer Tests (JSON vs Console)
# ============================================================================


def test_console_renderer_in_development() -> None:
    """Verifies that ConsoleRenderer is used when use_json=False."""
    logger = get_logger(__name__, use_json=False)

    # Get the structlog configuration
    config = structlog.get_config()

    # Check that ConsoleRenderer is in processors
    processors = config["processors"]
    has_console_renderer = any(
        isinstance(p, structlog.dev.ConsoleRenderer) for p in processors
    )
    assert has_console_renderer


def test_json_renderer_in_production() -> None:
    """Verifies that JSONRenderer is used when use_json=True."""
    logger = get_logger(__name__, use_json=True)

    # Get the structlog configuration
    config = structlog.get_config()

    # Check that JSONRenderer is in processors
    processors = config["processors"]
    has_json_renderer = any(
        isinstance(p, structlog.processors.JSONRenderer) for p in processors
    )
    assert has_json_renderer


# ============================================================================
# Logger Functionality Tests
# ============================================================================


def test_logger_can_log_info_message(capsys: pytest.CaptureFixture[str]) -> None:
    """Verifies that logger can log INFO messages."""
    logger = get_logger(__name__, log_level="INFO")

    logger.info("Test message")

    # Capture stdout (structlog writes to stdout via PrintLoggerFactory)
    captured = capsys.readouterr()

    # Message should appear in output
    assert "Test message" in captured.out


def test_logger_can_log_with_context() -> None:
    """Verifies that logger can log with context variables."""
    logger = get_logger(__name__, log_level="INFO")

    # Should not raise
    logger.info("Processing data", record_count=100, job_id="abc123")


def test_logger_respects_log_level(capsys: pytest.CaptureFixture[str]) -> None:
    """Verifies that logger respects the configured log level."""
    logger = get_logger(__name__, log_level="WARNING")

    # INFO should not be logged (below WARNING)
    logger.info("This should not appear")

    # WARNING should be logged
    logger.warning("This should appear")

    captured = capsys.readouterr()

    # WARNING message should be in output
    assert "This should appear" in captured.out
    # INFO message should not be in output (filtered by log level)
    assert "This should not appear" not in captured.out


def test_logger_can_log_error_with_exception() -> None:
    """Verifies that logger can log errors with exception info."""
    logger = get_logger(__name__, log_level="ERROR")

    try:
        raise ValueError("Test error")
    except ValueError as e:
        # Should not raise
        logger.error("An error occurred", error=str(e))


# ============================================================================
# Processor Tests
# ============================================================================


def test_logger_has_timestamp_processor() -> None:
    """Verifies that TimeStamper processor is configured."""
    logger = get_logger(__name__)

    config = structlog.get_config()
    processors = config["processors"]

    has_timestamp = any(
        isinstance(p, structlog.processors.TimeStamper) for p in processors
    )
    assert has_timestamp


def test_logger_has_log_level_processor() -> None:
    """Verifies that add_log_level processor is configured."""
    logger = get_logger(__name__)

    config = structlog.get_config()
    processors = config["processors"]

    # add_log_level is a function, not a class instance
    assert structlog.processors.add_log_level in processors


def test_logger_has_stack_info_processor() -> None:
    """Verifies that StackInfoRenderer processor is configured."""
    logger = get_logger(__name__)

    config = structlog.get_config()
    processors = config["processors"]

    has_stack_info = any(
        isinstance(p, structlog.processors.StackInfoRenderer) for p in processors
    )
    assert has_stack_info


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


def test_invalid_log_level_raises_error() -> None:
    """Verifies that invalid log level raises AttributeError."""
    with pytest.raises(AttributeError):
        get_logger(__name__, log_level="INVALID_LEVEL")


def test_empty_logger_name() -> None:
    """Verifies that empty logger name works."""
    logger = get_logger("")
    assert logger is not None


def test_logger_name_with_dots() -> None:
    """Verifies that logger names with dots work (module paths)."""
    logger = get_logger("dagster_crypto_data.connectors.database")
    assert logger is not None


# ============================================================================
# Integration Tests
# ============================================================================


def test_logger_integration_development_mode() -> None:
    """Integration test for development mode logging."""
    logger = get_logger(
        "test.module",
        log_level="DEBUG",
        use_json=False,
    )

    # Should be able to log at various levels
    logger.debug("Debug message", detail="test")
    logger.info("Info message", count=42)
    logger.warning("Warning message")
    logger.error("Error message", error_code=500)

    # All should succeed without raising


def test_logger_integration_production_mode() -> None:
    """Integration test for production mode logging."""
    logger = get_logger(
        "test.module",
        log_level="INFO",
        use_json=True,
    )

    # Should be able to log at various levels
    logger.info("Info message", count=42)
    logger.warning("Warning message")
    logger.error("Error message", error_code=500)

    # All should succeed without raising
