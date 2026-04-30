"""Unit tests for QueryEngine orchestrator.

Covers:
- Happy path: full execute flow
- return_type=sql skips execution
- Admin refresh action
- Database inference when not specified
- SQL validation retry loop
- Result validation trigger and fix loop
- Rate limiting (semaphore)
- Error propagation and conversion
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pg_mcp.config import Settings
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import (
    DbNotFoundError,
    RateLimitedError,
    SqlUnsafeError,
    ValidationFailedError,
)
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import QueryResponse
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.protocols import ExecutionResult, SqlGenerationResult, ValidationResult
from tests.conftest import (
    MockDbInference,
    MockResultValidator,
    MockSchemaCache,
    MockSqlExecutor,
    MockSqlGenerator,
    MockSqlValidator,
)
from pg_mcp.schema.retriever import SchemaRetriever


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
    sql_gen: MockSqlGenerator | None = None,
    sql_val: MockSqlValidator | None = None,
    sql_exec: MockSqlExecutor | None = None,
    cache: MockSchemaCache | None = None,
    db_inf: MockDbInference | None = None,
    result_val: MockResultValidator | None = None,
    retriever: SchemaRetriever | None = None,
    settings: Settings | None = None,
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
        retriever=retriever or SchemaRetriever(),
        settings=settings or _make_settings(),
    )


class TestHappyPath:
    """Tests for successful query execution."""

    @pytest.mark.asyncio
    async def test_execute_returns_query_response_with_results(self) -> None:
        engine = _make_engine()
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert isinstance(response, QueryResponse)
        assert response.database == "test_db"
        assert response.sql == "SELECT * FROM users"
        assert response.row_count == 1
        assert response.columns == ["id"]
        assert response.rows == [[1]]

    @pytest.mark.asyncio
    async def test_execute_infers_database_when_not_specified(self) -> None:
        db_inference = MockDbInference(database="test_db")
        engine = _make_engine(db_inf=db_inference)
        request = QueryRequest(query="List all users")

        response = await engine.execute(request)

        assert response.database == "test_db"
        assert db_inference.infer_calls == ["List all users"]

    @pytest.mark.asyncio
    async def test_execute_sets_request_id(self) -> None:
        engine = _make_engine()
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.request_id is not None
        assert len(response.request_id) > 0

    @pytest.mark.asyncio
    async def test_execute_sets_schema_loaded_at(self) -> None:
        engine = _make_engine()
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.schema_loaded_at is not None


class TestReturnTypeSql:
    """Tests for return_type='sql' which skips execution."""

    @pytest.mark.asyncio
    async def test_execute_sql_only_returns_sql_without_executing(self) -> None:
        executor = MockSqlExecutor()
        engine = _make_engine(sql_exec=executor)
        request = QueryRequest(
            query="List all users", database="test_db", return_type="sql"
        )

        response = await engine.execute(request)

        assert response.sql == "SELECT * FROM users"
        assert response.rows is None
        assert response.columns is None
        assert len(executor.execute_calls) == 0


class TestAdminAction:
    """Tests for admin_action handling."""

    @pytest.mark.asyncio
    async def test_admin_refresh_schema_returns_refresh_result(self) -> None:
        cache = MockSchemaCache(databases=["test_db"])
        engine = _make_engine(cache=cache)
        request = QueryRequest(
            query="", database="test_db", admin_action="refresh_schema"
        )

        response = await engine.execute(request)

        assert response.refresh_result is not None
        assert response.refresh_result.succeeded == ["test_db"]


class TestValidationRetry:
    """Tests for SQL validation retry loop."""

    @pytest.mark.asyncio
    async def test_validation_failure_triggers_retry(self) -> None:
        # First call fails, second succeeds
        validator = MockSqlValidator(
            valid=False, toggle_on_call=True, reason="Mock failure"
        )
        generator = MockSqlGenerator(sql="SELECT * FROM users")
        engine = _make_engine(sql_val=validator, sql_gen=generator)
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.sql == "SELECT * FROM users"
        # Generator called twice: initial + retry
        assert len(generator.generate_calls) == 2

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_raises_sql_unsafe(self) -> None:
        validator = MockSqlValidator(valid=False, code="E_SQL_UNSAFE")
        engine = _make_engine(
            sql_val=validator,
            settings=_make_settings(max_retries=1),
        )
        request = QueryRequest(query="List all users", database="test_db")

        with pytest.raises(SqlUnsafeError):
            await engine.execute(request)


class TestResultValidation:
    """Tests for AI result validation flow."""

    @pytest.mark.asyncio
    async def test_result_validation_fix_triggers_regeneration(self) -> None:
        result_validator = MockResultValidator(
            should_validate=True, verdict="fix", reason="Missing LIMIT"
        )
        # After fix, pass
        result_validator._verdict = "pass"
        generator = MockSqlGenerator(sql="SELECT * FROM users")
        engine = _make_engine(
            sql_gen=generator,
            result_val=result_validator,
            settings=_make_settings(enable_validation=True),
        )
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.sql is not None
        assert response.validation_used is True

    @pytest.mark.asyncio
    async def test_result_validation_fail_raises_error(self) -> None:
        result_validator = MockResultValidator(
            should_validate=True, verdict="fail", reason="Wrong aggregation"
        )
        engine = _make_engine(
            result_val=result_validator,
            settings=_make_settings(enable_validation=True),
        )
        request = QueryRequest(query="List all users", database="test_db")

        with pytest.raises(ValidationFailedError):
            await engine.execute(request)

    @pytest.mark.asyncio
    async def test_result_validation_not_triggered_when_disabled(self) -> None:
        result_validator = MockResultValidator(should_validate=False)
        engine = _make_engine(
            result_val=result_validator,
            settings=_make_settings(enable_validation=False),
        )
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.validation_used is False


class TestRateLimiting:
    """Tests for concurrent request limiting."""

    @pytest.mark.asyncio
    async def test_rate_limit_raised_when_semaphore_full(self) -> None:
        settings = _make_settings(max_concurrent_requests=1)
        engine = _make_engine(settings=settings)
        # Acquire the only slot
        await engine._semaphore.acquire()

        request = QueryRequest(query="List all users", database="test_db")
        with pytest.raises(RateLimitedError):
            await engine.execute(request)

        engine._semaphore.release()

    @pytest.mark.asyncio
    async def test_concurrent_requests_allowed_within_limit(self) -> None:
        settings = _make_settings(max_concurrent_requests=5)
        engine = _make_engine(settings=settings)
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.database == "test_db"


class TestErrorPropagation:
    """Tests for error handling and propagation."""

    @pytest.mark.asyncio
    async def test_db_not_found_raises_error(self) -> None:
        cache = MockSchemaCache(databases=["other_db"])
        engine = _make_engine(cache=cache)
        request = QueryRequest(query="List all users", database="test_db")

        with pytest.raises(DbNotFoundError):
            await engine.execute(request)

    @pytest.mark.asyncio
    async def test_executor_error_converted_to_sql_execute_error(self) -> None:
        import asyncpg

        executor = MockSqlExecutor(raise_error=asyncpg.PostgresError("syntax error"))
        engine = _make_engine(sql_exec=executor)
        request = QueryRequest(query="List all users", database="test_db")

        from pg_mcp.models.errors import SqlExecuteError

        with pytest.raises(SqlExecuteError):
            await engine.execute(request)

    @pytest.mark.asyncio
    async def test_sql_timeout_propagated(self) -> None:
        from pg_mcp.models.errors import SqlTimeoutError

        executor = MockSqlExecutor(raise_error=SqlTimeoutError("timeout"))
        engine = _make_engine(sql_exec=executor)
        request = QueryRequest(query="List all users", database="test_db")

        with pytest.raises(SqlTimeoutError):
            await engine.execute(request)


class TestSchemaRetrieval:
    """Tests for schema context selection."""

    @pytest.mark.asyncio
    async def test_small_schema_uses_full_context(self) -> None:
        schema = DatabaseSchema(
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
        cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
        generator = MockSqlGenerator(sql="SELECT * FROM users")
        engine = _make_engine(sql_gen=generator, cache=cache)
        request = QueryRequest(query="List all users", database="test_db")

        await engine.execute(request)

        # Should use to_prompt_text for small schema
        _, schema_context, _ = generator.generate_calls[0]
        assert "users" in schema_context

    @pytest.mark.asyncio
    async def test_large_schema_uses_retrieval(self) -> None:
        tables = [
            TableInfo(
                schema_name="public",
                table_name=f"table_{i}",
                columns=[ColumnInfo(name="id", type="integer", nullable=False)],
            )
            for i in range(60)
        ]
        schema = DatabaseSchema(
            database="big_db", tables=tables, loaded_at=datetime.now(timezone.utc)
        )
        cache = MockSchemaCache(schemas={"big_db": schema}, databases=["big_db"])
        generator = MockSqlGenerator(sql="SELECT * FROM table_0")
        retriever = SchemaRetriever(max_tables_for_full=50)
        engine = _make_engine(sql_gen=generator, cache=cache, retriever=retriever)
        request = QueryRequest(query="show table_0", database="big_db")

        await engine.execute(request)

        _, schema_context, _ = generator.generate_calls[0]
        assert "Database: big_db" in schema_context
