"""Error code enum and PgMcpError exception hierarchy.

All business exceptions inherit from :class:`PgMcpError` and carry a
machine-readable :class:`ErrorCode`.  This allows the MCP server layer to
convert any business error into a structured :class:`ErrorDetail` response
without inspecting string messages.
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """Machine-readable error codes returned in API responses."""

    E_INVALID_ARGUMENT = "E_INVALID_ARGUMENT"
    E_DB_CONNECT = "E_DB_CONNECT"
    E_DB_NOT_FOUND = "E_DB_NOT_FOUND"
    E_DB_INFER_AMBIGUOUS = "E_DB_INFER_AMBIGUOUS"
    E_DB_INFER_NO_MATCH = "E_DB_INFER_NO_MATCH"
    E_CROSS_DB_UNSUPPORTED = "E_CROSS_DB_UNSUPPORTED"
    E_SCHEMA_NOT_READY = "E_SCHEMA_NOT_READY"
    E_SQL_GENERATE = "E_SQL_GENERATE"
    E_SQL_UNSAFE = "E_SQL_UNSAFE"
    E_SQL_PARSE = "E_SQL_PARSE"
    E_SQL_EXECUTE = "E_SQL_EXECUTE"
    E_SQL_TIMEOUT = "E_SQL_TIMEOUT"
    E_VALIDATION_FAILED = "E_VALIDATION_FAILED"
    E_LLM_TIMEOUT = "E_LLM_TIMEOUT"
    E_LLM_ERROR = "E_LLM_ERROR"
    E_RESULT_TOO_LARGE = "E_RESULT_TOO_LARGE"
    E_RATE_LIMITED = "E_RATE_LIMITED"


class PgMcpError(Exception):
    """Base class for all pg-mcp business exceptions.

    Attributes:
        code: The machine-readable error code.
        retry_after_ms: Hint for clients when the error is transient
            (e.g. schema still loading).
        candidates: List of suggested alternatives when the error is
            ambiguous (e.g. database inference).
    """

    code: ErrorCode = ErrorCode.E_INVALID_ARGUMENT
    retry_after_ms: int | None = None
    candidates: list[str] | None = None

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidArgumentError(PgMcpError):
    """Request argument failed validation."""

    code = ErrorCode.E_INVALID_ARGUMENT


class DbConnectError(PgMcpError):
    """Failed to connect to the target PostgreSQL database."""

    code = ErrorCode.E_DB_CONNECT


class DbNotFoundError(PgMcpError):
    """The requested database does not exist or is not accessible."""

    code = ErrorCode.E_DB_NOT_FOUND


class DbInferAmbiguousError(PgMcpError):
    """Database inference could not pick a single unambiguous target.

    Attributes:
        candidates: Top candidate database names, ordered by relevance.
    """

    code = ErrorCode.E_DB_INFER_AMBIGUOUS

    def __init__(self, message: str, candidates: list[str]) -> None:
        super().__init__(message)
        self.candidates = candidates


class DbInferNoMatchError(PgMcpError):
    """Database inference found no matching database for the query."""

    code = ErrorCode.E_DB_INFER_NO_MATCH


class CrossDbUnsupportedError(PgMcpError):
    """The query appears to span multiple databases, which is not supported."""

    code = ErrorCode.E_CROSS_DB_UNSUPPORTED


class SchemaNotReadyError(PgMcpError):
    """Schema metadata for the target database is not yet loaded.

    Clients should retry after *retry_after_ms* milliseconds.
    """

    code = ErrorCode.E_SCHEMA_NOT_READY

    def __init__(self, message: str, retry_after_ms: int = 2000) -> None:
        super().__init__(message)
        self.retry_after_ms = retry_after_ms


class SqlGenerateError(PgMcpError):
    """The LLM failed to generate a valid SQL query."""

    code = ErrorCode.E_SQL_GENERATE


class SqlUnsafeError(PgMcpError):
    """The generated or provided SQL failed the safety check."""

    code = ErrorCode.E_SQL_UNSAFE


class SqlParseError(PgMcpError):
    """The SQL could not be parsed by the SQL parser."""

    code = ErrorCode.E_SQL_PARSE


class SqlExecuteError(PgMcpError):
    """The SQL execution failed at the database level."""

    code = ErrorCode.E_SQL_EXECUTE


class SqlTimeoutError(PgMcpError):
    """The SQL query exceeded the configured execution timeout."""

    code = ErrorCode.E_SQL_TIMEOUT


class ValidationFailedError(PgMcpError):
    """AI result validation determined the query/result is incorrect."""

    code = ErrorCode.E_VALIDATION_FAILED


class LlmTimeoutError(PgMcpError):
    """An LLM API call exceeded the configured timeout."""

    code = ErrorCode.E_LLM_TIMEOUT


class LlmError(PgMcpError):
    """An LLM API call failed for a non-timeout reason."""

    code = ErrorCode.E_LLM_ERROR


class ResultTooLargeError(PgMcpError):
    """The query result exceeded the hard size limit."""

    code = ErrorCode.E_RESULT_TOO_LARGE


class RateLimitedError(PgMcpError):
    """The request was rejected because the server is at capacity."""

    code = ErrorCode.E_RATE_LIMITED
