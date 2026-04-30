"""Public data models for pg-mcp.

This module re-exports all Pydantic models and the exception hierarchy so
that consumers can import everything from a single namespace.
"""

from __future__ import annotations

from pg_mcp.models.errors import (
    CrossDbUnsupportedError,
    DbConnectError,
    DbInferAmbiguousError,
    DbInferNoMatchError,
    DbNotFoundError,
    ErrorCode,
    InvalidArgumentError,
    LlmError,
    LlmTimeoutError,
    PgMcpError,
    RateLimitedError,
    ResultTooLargeError,
    SchemaNotReadyError,
    SqlExecuteError,
    SqlGenerateError,
    SqlParseError,
    SqlTimeoutError,
    SqlUnsafeError,
    ValidationFailedError,
)
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import AdminRefreshResult, ErrorDetail, QueryResponse
from pg_mcp.models.schema import (
    ColumnInfo,
    CompositeTypeInfo,
    ConstraintInfo,
    DatabaseSchema,
    EnumTypeInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
    ViewInfo,
)

__all__ = [
    # errors
    "ErrorCode",
    "PgMcpError",
    "InvalidArgumentError",
    "DbConnectError",
    "DbNotFoundError",
    "DbInferAmbiguousError",
    "DbInferNoMatchError",
    "CrossDbUnsupportedError",
    "SchemaNotReadyError",
    "SqlGenerateError",
    "SqlUnsafeError",
    "SqlParseError",
    "SqlExecuteError",
    "SqlTimeoutError",
    "ValidationFailedError",
    "LlmTimeoutError",
    "LlmError",
    "ResultTooLargeError",
    "RateLimitedError",
    # request / response
    "QueryRequest",
    "QueryResponse",
    "ErrorDetail",
    "AdminRefreshResult",
    # schema
    "ColumnInfo",
    "TableInfo",
    "ViewInfo",
    "IndexInfo",
    "ForeignKeyInfo",
    "ConstraintInfo",
    "EnumTypeInfo",
    "CompositeTypeInfo",
    "DatabaseSchema",
]
