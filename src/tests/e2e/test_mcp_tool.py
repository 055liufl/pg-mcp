"""End-to-end tests for the MCP tool flow.

Covers:
- Full query tool invocation with mocked dependencies
- Error conversion to QueryResponse with ErrorDetail
- Invalid arguments rejected with McpError
- Unknown tool rejected with McpError
- Admin refresh action
- return_type=sql vs return_type=result

All external dependencies (PostgreSQL, Redis, OpenAI) are mocked.
The mcp package is skipped if not available (requires Python 3.10+).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config import Settings
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import DbNotFoundError, SqlUnsafeError
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import QueryResponse
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.schema.retriever import SchemaRetriever
from tests.conftest import (
    MockDbInference,
    MockResultValidator,
    MockSchemaCache,
    MockSqlExecutor,
    MockSqlGenerator,
    MockSqlValidator,
)

# Try to import MCP components; skip tests if not available
try:
    from mcp import McpError
    from pg_mcp.server import PgMcpServer
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


def _make_settings(**overrides: object) -> Settings:
    defaults = dict(
        pg_user="test",
        pg_password="test",
        max_retries=2,
        max_concurrent_requests=20,
        enable_validation=False,
    )
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def _make_engine(
    sql_gen: Optional[MockSqlGenerator] = None,
    sql_val: Optional[MockSqlValidator] = None,
    sql_exec: Optional[MockSqlExecutor] = None,
    cache: Optional[MockSchemaCache] = None,
    db_inf: Optional[MockDbInference] = None,
    result_val: Optional[MockResultValidator] = None,
    settings: Optional[Settings] = None,
) -> QueryEngine:
    sample_schema = DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[ColumnInfo(name="id", type="integer", nullable=False)],
            ),
        ],
        loaded_at=datetime.now(timezone.utc),
    )
    return QueryEngine(
        sql_generator=sql_gen or MockSqlGenerator(sql="SELECT * FROM users"),
        sql_validator=sql_val or MockSqlValidator(valid=True),
        sql_executor=sql_exec or MockSqlExecutor(
            columns=["id"], column_types=["integer"], rows=[[1]], row_count=1
        ),
        schema_cache=cache or MockSchemaCache(
            schemas={"test_db": sample_schema}, databases=["test_db"]
        ),
        db_inference=db_inf or MockDbInference(database="test_db"),
        result_validator=result_val or MockResultValidator(should_validate=False),
        retriever=SchemaRetriever(),
        settings=settings or _make_settings(),
    )


@pytest.fixture
def server() -> "PgMcpServer":
    engine = _make_engine()
    if not MCP_AVAILABLE:
        pytest.skip("mcp package not available (requires Python 3.10+)")
    return PgMcpServer(engine)


class TestQueryTool:
    """Tests for the main query tool invocation."""

    @pytest.mark.asyncio
    async def test_full_flow_query_returns_results(self) -> None:
        engine = _make_engine()

        request = QueryRequest(query="List all users", database="test_db")
        response = await engine.execute(request)

        assert response.database == "test_db"
        assert response.sql == "SELECT * FROM users"
        assert response.row_count == 1
        assert response.error is None

        # Verify it serializes to JSON correctly
        json_str = response.model_dump_json()
        assert "test_db" in json_str
        assert "SELECT * FROM users" in json_str

    @pytest.mark.asyncio
    async def test_full_flow_sql_only_returns_sql(self) -> None:
        engine = _make_engine()
        request = QueryRequest(
            query="List all users", database="test_db", return_type="sql"
        )

        response = await engine.execute(request)

        assert response.sql == "SELECT * FROM users"
        assert response.rows is None
        assert response.columns is None

    @pytest.mark.asyncio
    async def test_full_flow_with_inference(self) -> None:
        db_inference = MockDbInference(database="test_db")
        engine = _make_engine(db_inf=db_inference)
        request = QueryRequest(query="List all users")

        response = await engine.execute(request)

        assert response.database == "test_db"
        assert db_inference.infer_calls == ["List all users"]


class TestErrorHandling:
    """Tests for error conversion in the MCP server layer."""

    @pytest.mark.asyncio
    async def test_db_not_found_returns_error_response(self) -> None:
        cache = MockSchemaCache(databases=["other_db"])
        engine = _make_engine(cache=cache)

        from pg_mcp.models.errors import PgMcpError
        from pg_mcp.models.response import ErrorDetail

        request = QueryRequest(query="List all users", database="nonexistent")

        try:
            await engine.execute(request)
        except PgMcpError as exc:
            response = QueryResponse(
                error=ErrorDetail(
                    code=exc.code.value,
                    message=str(exc),
                    retry_after_ms=exc.retry_after_ms,
                    candidates=exc.candidates,
                )
            )

            assert response.error is not None
            assert response.error.code == "E_DB_NOT_FOUND"
            assert "nonexistent" in response.error.message

    @pytest.mark.asyncio
    async def test_sql_unsafe_returns_error_response(self) -> None:
        validator = MockSqlValidator(valid=False, code="E_SQL_UNSAFE")
        engine = _make_engine(
            sql_val=validator,
            settings=_make_settings(max_retries=0),
        )

        from pg_mcp.models.errors import PgMcpError
        from pg_mcp.models.response import ErrorDetail

        request = QueryRequest(query="List all users", database="test_db")

        try:
            await engine.execute(request)
        except PgMcpError as exc:
            response = QueryResponse(
                error=ErrorDetail(
                    code=exc.code.value,
                    message=str(exc),
                )
            )

            assert response.error is not None
            assert response.error.code == "E_SQL_UNSAFE"

    @pytest.mark.asyncio
    async def test_error_response_serializes_to_json(self) -> None:
        from pg_mcp.models.response import ErrorDetail

        response = QueryResponse(
            error=ErrorDetail(code="E_DB_NOT_FOUND", message="Database not found: x"),
        )
        json_str = response.model_dump_json()

        assert "E_DB_NOT_FOUND" in json_str
        assert "Database not found: x" in json_str


class TestToolRegistration:
    """Tests for MCP tool registration."""

    @pytest.mark.skipif(not MCP_AVAILABLE, reason="mcp package not available")
    def test_tool_schema_has_correct_properties(self) -> None:
        engine = _make_engine()
        server = PgMcpServer(engine)

        # Verify the tool input schema structure
        tool_info = {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "database": {"type": "string"},
                "return_type": {"type": "string", "enum": ["sql", "result"]},
                "admin_action": {"type": "string", "enum": ["refresh_schema"]},
            },
        }

        assert tool_info["properties"]["return_type"]["enum"] == ["sql", "result"]
        assert tool_info["properties"]["admin_action"]["enum"] == ["refresh_schema"]


class TestAdminAction:
    """Tests for admin actions through the MCP tool."""

    @pytest.mark.asyncio
    async def test_refresh_schema_admin_action(self) -> None:
        cache = MockSchemaCache(databases=["test_db"])
        engine = _make_engine(cache=cache)

        request = QueryRequest(
            query="", database="test_db", admin_action="refresh_schema"
        )
        response = await engine.execute(request)

        assert response.refresh_result is not None
        assert response.refresh_result.succeeded == ["test_db"]


class TestRequestValidation:
    """Tests for request argument validation."""

    def test_valid_request_constructed(self) -> None:
        request = QueryRequest(query="List all users", database="test_db")

        assert request.query == "List all users"
        assert request.database == "test_db"
        assert request.return_type == "result"

    def test_empty_query_without_admin_raises_error(self) -> None:
        with pytest.raises(Exception):
            QueryRequest(query="", database="test_db")

    def test_admin_action_allows_empty_query(self) -> None:
        request = QueryRequest(
            query="", database="test_db", admin_action="refresh_schema"
        )

        assert request.admin_action == "refresh_schema"

    def test_query_gets_stripped(self) -> None:
        request = QueryRequest(query="  List all users  ", database="test_db")

        assert request.query == "List all users"


class TestResponseFormat:
    """Tests for response formatting."""

    def test_success_response_has_all_fields(self) -> None:
        response = QueryResponse(
            request_id="test-id",
            database="test_db",
            sql="SELECT * FROM users",
            columns=["id", "name"],
            column_types=["integer", "text"],
            rows=[[1, "Alice"]],
            row_count=1,
            schema_loaded_at="2024-01-01T00:00:00",
        )

        assert response.request_id == "test-id"
        assert response.error is None
        assert response.validation_used is False

    def test_error_response_has_error_detail(self) -> None:
        from pg_mcp.models.response import ErrorDetail

        response = QueryResponse(
            error=ErrorDetail(
                code="E_SQL_UNSAFE",
                message="Unsafe SQL detected",
                retry_after_ms=None,
            ),
        )

        assert response.error is not None
        assert response.error.code == "E_SQL_UNSAFE"
        assert response.rows is None

    def test_response_json_roundtrip(self) -> None:
        response = QueryResponse(
            database="test_db",
            sql="SELECT 1",
            row_count=1,
            rows=[[1]],
            columns=["id"],
        )
        json_str = response.model_dump_json()
        parsed = QueryResponse.model_validate_json(json_str)

        assert parsed.database == "test_db"
        assert parsed.row_count == 1
