"""Shared pytest fixtures for pg-mcp test suite."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Optional, Protocol
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from pg_mcp.models.errors import ErrorCode, PgMcpError
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import QueryResponse, ErrorDetail, AdminRefreshResult
from pg_mcp.models.schema import (
    ColumnInfo,
    DatabaseSchema,
    TableInfo,
    ViewInfo,
    IndexInfo,
    ForeignKeyInfo,
    ConstraintInfo,
    EnumTypeInfo,
)
from pg_mcp.protocols import (
    SqlGenerationResult,
    ValidationResult,
    ExecutionResult,
    ValidationVerdict,
    RefreshResult,
)


# =============================================================================
# Mock Protocol Implementations
# =============================================================================

class MockSqlGenerator:
    """Mock SQL generator that returns pre-configured SQL."""

    def __init__(
        self,
        sql: str = "SELECT 1",
        logprob: float = 0.0,
        prompt_tokens: int = 100,
        completion_tokens: int = 50,
        raise_error: Optional[Exception] = None,
    ):
        self._sql = sql
        self._logprob = logprob
        self._prompt_tokens = prompt_tokens
        self._completion_tokens = completion_tokens
        self._raise_error = raise_error
        self.generate_calls: list[tuple[str, str, Optional[str]]] = []

    async def generate(
        self, query: str, schema_context: str, feedback: Optional[str] = None
    ) -> SqlGenerationResult:
        self.generate_calls.append((query, schema_context, feedback))
        if self._raise_error:
            raise self._raise_error
        return SqlGenerationResult(
            sql=self._sql,
            prompt_tokens=self._prompt_tokens,
            completion_tokens=self._completion_tokens,
            avg_logprob=self._logprob,
        )


class MockSqlValidator:
    """Mock SQL validator with configurable pass/fail behavior."""

    def __init__(
        self,
        valid: bool = True,
        code: Optional[str] = None,
        reason: Optional[str] = None,
        is_explain: bool = False,
        toggle_on_call: bool = False,
    ):
        self._valid = valid
        self._code = code or "E_SQL_UNSAFE"
        self._reason = reason or "Mock rejection"
        self._is_explain = is_explain
        self._toggle_on_call = toggle_on_call
        self._current_valid = valid
        self.validate_calls: list[tuple[str, Optional[object], Optional[list[str]]]] = []

    def validate(
        self,
        sql: str,
        schema: Optional[DatabaseSchema] = None,
        schema_names: Optional[list[str]] = None,
    ) -> ValidationResult:
        self.validate_calls.append((sql, schema, schema_names))
        result = ValidationResult(
            valid=self._current_valid,
            code=None if self._current_valid else self._code,
            reason=None if self._current_valid else self._reason,
            is_explain=self._is_explain,
        )
        if self._toggle_on_call:
            self._current_valid = not self._current_valid
        return result


class MockSqlExecutor:
    """Mock SQL executor returning pre-configured results."""

    def __init__(
        self,
        columns: Optional[list[str]] = None,
        column_types: Optional[list[str]] = None,
        rows: Optional[list[list]] = None,
        row_count: Optional[int] = None,
        truncated: bool = False,
        truncated_reason: Optional[str] = None,
        raise_error: Optional[Exception] = None,
    ):
        self._columns = columns or ["id"]
        self._column_types = column_types or ["integer"]
        self._rows = rows or [[1]]
        self._row_count = row_count if row_count is not None else len(self._rows)
        self._truncated = truncated
        self._truncated_reason = truncated_reason
        self._raise_error = raise_error
        self.execute_calls: list[tuple[str, str, Optional[list[str]], bool]] = []

    async def execute(
        self,
        database: str,
        sql: str,
        schema_names: Optional[list[str]] = None,
        is_explain: bool = False,
    ) -> ExecutionResult:
        self.execute_calls.append((database, sql, schema_names, is_explain))
        if self._raise_error:
            raise self._raise_error
        return ExecutionResult(
            columns=self._columns,
            column_types=self._column_types,
            rows=self._rows,
            row_count=self._row_count,
            truncated=self._truncated,
            truncated_reason=self._truncated_reason,
        )


class MockSchemaCache:
    """Mock schema cache with pre-configured schemas."""

    def __init__(
        self,
        schemas: Optional[dict[str, DatabaseSchema]] = None,
        databases: Optional[list[str]] = None,
        raise_on_get: Optional[Exception] = None,
    ):
        self._schemas = schemas or {}
        self._databases = databases or list(self._schemas.keys())
        self._raise_on_get = raise_on_get
        self.get_calls: list[str] = []
        self.refresh_calls: list[Optional[str]] = []

    async def get_schema(self, database: str) -> DatabaseSchema:
        self.get_calls.append(database)
        if self._raise_on_get:
            raise self._raise_on_get
        if database not in self._schemas:
            raise PgMcpError(f"Schema not found for {database}")
        return self._schemas[database]

    async def refresh(self, database: Optional[str] = None) -> RefreshResult:
        self.refresh_calls.append(database)
        return RefreshResult(succeeded=[database] if database else [], failed=[])

    def discovered_databases(self) -> list[str]:
        return list(self._databases)


class MockDbInference:
    """Mock database inference returning a pre-configured database name."""

    def __init__(self, database: str = "test_db", raise_error: Optional[Exception] = None):
        self._database = database
        self._raise_error = raise_error
        self.infer_calls: list[str] = []

    async def infer(self, user_query: str) -> str:
        self.infer_calls.append(user_query)
        if self._raise_error:
            raise self._raise_error
        return self._database


class MockResultValidator:
    """Mock result validator with configurable verdict.

    Supports both a single fixed verdict and a sequence of verdicts so
    tests can exercise the orchestrator's fix loop (e.g. ``fix`` then
    ``pass``).
    """

    def __init__(
        self,
        should_validate: bool = False,
        verdict: str = "pass",
        reason: Optional[str] = None,
        suggested_sql: Optional[str] = None,
        raise_error: Optional[Exception] = None,
        verdict_sequence: Optional[list[str]] = None,
    ):
        self._should_validate = should_validate
        self._verdict = verdict
        self._reason = reason
        self._suggested_sql = suggested_sql
        self._raise_error = raise_error
        # When provided, validate() advances through this sequence on each
        # call, falling back to the last verdict once exhausted.
        self._verdict_sequence: list[str] = list(verdict_sequence or [])
        self.should_validate_calls: list[tuple] = []
        self.validate_calls: list[tuple] = []

    def should_validate(
        self,
        database: str,
        sql: str,
        result: ExecutionResult,
        generation: SqlGenerationResult,
    ) -> bool:
        self.should_validate_calls.append((database, sql, result, generation))
        return self._should_validate

    async def validate(
        self,
        user_query: str,
        sql: str,
        result: ExecutionResult,
        schema: DatabaseSchema,
    ) -> ValidationVerdict:
        self.validate_calls.append((user_query, sql, result, schema))
        if self._raise_error:
            raise self._raise_error
        if self._verdict_sequence:
            verdict = self._verdict_sequence.pop(0)
        else:
            verdict = self._verdict
        return ValidationVerdict(
            verdict=verdict,
            reason=self._reason,
            suggested_sql=self._suggested_sql,
        )


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_database_schema() -> DatabaseSchema:
    """Return a sample DatabaseSchema for testing."""
    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False, is_primary_key=True),
                    ColumnInfo(name="name", type="text", nullable=False),
                    ColumnInfo(name="email", type="text", nullable=True),
                    ColumnInfo(name="created_at", type="timestamp", nullable=False),
                ],
                comment="User accounts table",
            ),
            TableInfo(
                schema_name="public",
                table_name="orders",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False, is_primary_key=True),
                    ColumnInfo(name="user_id", type="integer", nullable=False),
                    ColumnInfo(name="total", type="numeric", nullable=False),
                    ColumnInfo(name="status", type="text", nullable=False),
                ],
                comment="Orders table",
            ),
            TableInfo(
                schema_name="public",
                table_name="products",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False, is_primary_key=True),
                    ColumnInfo(name="name", type="text", nullable=False),
                    ColumnInfo(name="price", type="numeric", nullable=False),
                ],
            ),
        ],
        views=[
            ViewInfo(
                schema_name="public",
                view_name="active_users",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                    ColumnInfo(name="name", type="text", nullable=False),
                ],
                definition="SELECT id, name FROM users WHERE active = true",
            ),
        ],
        indexes=[
            IndexInfo(
                schema_name="public",
                table_name="users",
                index_name="idx_users_email",
                columns=["email"],
                index_type="btree",
                is_unique=True,
            ),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                constraint_name="fk_orders_user_id",
                source_schema="public",
                source_table="orders",
                source_columns=["user_id"],
                target_schema="public",
                target_table="users",
                target_columns=["id"],
            ),
        ],
        constraints=[
            ConstraintInfo(
                schema_name="public",
                table_name="users",
                constraint_name="chk_users_email",
                constraint_type="CHECK",
                definition="CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')",
            ),
        ],
        enum_types=[
            EnumTypeInfo(
                schema_name="public",
                type_name="order_status",
                values=["pending", "processing", "shipped", "delivered"],
            ),
        ],
        composite_types=[],
        allowed_functions={
            "upper", "lower", "count", "sum", "avg", "max", "min",
            "coalesce", "nullif", "date_trunc", "extract",
        },
        loaded_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def sample_query_request() -> QueryRequest:
    """Return a sample QueryRequest."""
    return QueryRequest(query="List all users", database="test_db", return_type="result")


@pytest.fixture
def sample_query_request_sql_only() -> QueryRequest:
    """Return a sample QueryRequest with return_type=sql."""
    return QueryRequest(query="List all users", database="test_db", return_type="sql")


@pytest.fixture
def mock_sql_generator() -> MockSqlGenerator:
    """Return a default MockSqlGenerator."""
    return MockSqlGenerator(sql="SELECT * FROM users")


@pytest.fixture
def mock_sql_validator() -> MockSqlValidator:
    """Return a default MockSqlValidator that passes."""
    return MockSqlValidator(valid=True)


@pytest.fixture
def mock_sql_executor() -> MockSqlExecutor:
    """Return a default MockSqlExecutor."""
    return MockSqlExecutor(
        columns=["id", "name"],
        column_types=["integer", "text"],
        rows=[[1, "Alice"], [2, "Bob"]],
        row_count=2,
    )


@pytest.fixture
def mock_schema_cache(sample_database_schema: DatabaseSchema) -> MockSchemaCache:
    """Return a default MockSchemaCache with one schema."""
    return MockSchemaCache(
        schemas={"test_db": sample_database_schema},
        databases=["test_db", "other_db"],
    )


@pytest.fixture
def mock_db_inference() -> MockDbInference:
    """Return a default MockDbInference."""
    return MockDbInference(database="test_db")


@pytest.fixture
def mock_result_validator() -> MockResultValidator:
    """Return a default MockResultValidator that does not trigger."""
    return MockResultValidator(should_validate=False)


@pytest_asyncio.fixture
async def mock_redis_client() -> AsyncGenerator[AsyncMock, None]:
    """Return an AsyncMock redis client."""
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.flushdb = AsyncMock(return_value=True)
    client.aclose = AsyncMock(return_value=None)
    yield client
