"""Metrics and timing utilities.

Provides an async context manager :func:`timed` that records the elapsed
duration of a code block and emits a structured log event.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import structlog


@asynccontextmanager
async def timed(
    log: structlog.stdlib.BoundLogger,
    event: str,
) -> AsyncGenerator[dict[str, Any], None]:
    """Async context manager that times a block and logs the duration.

    Usage::

        async with timed(log, "sql_executed") as extra:
            rows = await conn.fetch(sql)
            extra["row_count"] = len(rows)

    The ``extra`` dict can be populated inside the block; its contents are
    merged into the final log event along with ``elapsed_ms``.

    Args:
        log: A structlog bound logger.
        event: The event name to log on completion.

    Yields:
        A mutable dict that will be merged into the final log record.
    """
    start = time.monotonic()
    extra: dict[str, Any] = {}
    try:
        yield extra
    finally:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        log.info(event, elapsed_ms=elapsed_ms, **extra)
