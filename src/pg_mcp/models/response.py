"""Response data models for pg-mcp queries.

These models define the structured JSON returned by the MCP ``query`` tool.
All fields are optional except where noted, allowing the same schema to
represent successful results, administrative actions, and error responses.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    """Structured error information returned in a :class:`QueryResponse`."""

    code: str
    message: str
    retry_after_ms: int | None = None
    candidates: list[str] | None = None


class AdminRefreshResult(BaseModel):
    """Result of a schema refresh administrative action."""

    succeeded: list[str] = Field(default_factory=list)
    failed: list[dict[str, str]] = Field(default_factory=list)


class QueryResponse(BaseModel):
    """Unified response model for all query tool invocations.

    On success *error* is ``None`` and execution metadata is populated.
    On failure *error* contains a structured :class:`ErrorDetail` and
    remaining fields may be ``None``.
    """

    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    database: str | None = None
    sql: str | None = None
    columns: list[str] | None = None
    column_types: list[str] | None = None
    rows: list[list[Any]] | None = None
    row_count: int | None = None
    truncated: bool = False
    truncated_reason: str | None = None
    validation_used: bool = False
    schema_loaded_at: str | None = None
    refresh_result: AdminRefreshResult | None = None
    warnings: list[str] = Field(default_factory=list)
    error: ErrorDetail | None = None
