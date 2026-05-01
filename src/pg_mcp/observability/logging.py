"""Structured logging configuration using structlog.

Provides a single :func:`configure_logging` entry-point that sets up
JSON-formatted logs with standard processors for level, timestamp, and
exception rendering.
"""

from __future__ import annotations

import structlog
import structlog.typing


def configure_logging(log_level: str, log_format: str = "json") -> None:
    """Configure structlog for structured logging.

    Args:
        log_level: Minimum log level (e.g. ``"DEBUG"``, ``"INFO"``).
        log_format: Output format -- ``"json"`` or ``"console"`` (human-readable).
    """
    renderer: structlog.typing.Processor = (
        structlog.dev.ConsoleRenderer(colors=False)
        if log_format == "console"
        else structlog.processors.JSONRenderer()
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            renderer,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger():
    """Get a structlog logger instance."""
    return structlog.get_logger()
