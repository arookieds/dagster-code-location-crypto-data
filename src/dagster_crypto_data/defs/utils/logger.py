"""
Structured logging configuration using structlog.
Provides consistent, JSON-formatted logging for better observability
in Kubernetes environments.
"""

import logging
import sys

import structlog

# Track if logging has been configured
_configured = False


def get_logger(
    name: str,
    log_level: str = "INFO",
    use_json: bool = False,
) -> structlog.BoundLogger:
    """Get a structured logger instance.
    Configures logging on first call with the provided settings.
    Subsequent calls return loggers without reconfiguration.
    Args:
        name: Logger name (typically __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                   Default: INFO
        use_json: If True, use JSON renderer for production.
                  If False, use console renderer for development.
                  Default: False
    Returns:
        Configured structured logger instance.
    Example:
        >>> # Development usage
        >>> logger = get_logger(__name__, log_level="DEBUG", use_json=False)
        >>> logger.info("Processing started", job_id=123)

        >>> # Production usage
        >>> logger = get_logger(__name__, log_level="INFO", use_json=True)
        >>> logger.error("Failed to process", error=str(e))
    """
    global _configured
    if not _configured:
        # Normalize log level to uppercase
        log_level_upper = log_level.upper()
        # Configure standard library logging
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=getattr(logging, log_level_upper),
        )
        # Configure structlog
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.StackInfoRenderer(),
                structlog.dev.set_exc_info,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                (
                    structlog.processors.JSONRenderer()
                    if use_json
                    else structlog.dev.ConsoleRenderer()
                ),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(
                getattr(logging, log_level_upper)
            ),
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
        _configured = True
    from typing import cast

    return cast("structlog.BoundLogger", structlog.get_logger(name))
