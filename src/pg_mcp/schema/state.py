"""Schema loading state machine enumeration.

The :class:`SchemaState` enum tracks the lifecycle of schema metadata
for each database in the cache.
"""

from __future__ import annotations

from enum import Enum


class SchemaState(str, Enum):
    """States in the schema loading lifecycle."""

    UNLOADED = "unloaded"
    LOADING = "loading"
    READY = "ready"
    FAILED = "failed"
