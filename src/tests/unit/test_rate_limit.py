"""Unit tests for rate limiting via asyncio.Semaphore.

Covers:
- Semaphore acquisition and release
- Concurrent request limiting
- Timeout behavior
"""

from __future__ import annotations

import asyncio

import pytest

from pg_mcp.config import Settings
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import RateLimitedError
from pg_mcp.models.request import QueryRequest
from tests.conftest import (
    MockDbInference,
    MockResultValidator,
    MockSchemaCache,
    MockSqlExecutor,
    MockSqlGenerator,
    MockSqlValidator,
)
from pg_mcp.schema.retriever import SchemaRetriever


def _make_settings(max_concurrent: int = 20) -> Settings:
    return Settings(
        pg_user="test",
        pg_password="test",
        max_concurrent_requests=max_concurrent,
    )


def _make_engine(settings: Settings | None = None) -> QueryEngine:
    from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
    from datetime import datetime, timezone

    s = settings or _make_settings()
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
    return QueryEngine(
        sql_generator=MockSqlGenerator(sql="SELECT * FROM users"),
        sql_validator=MockSqlValidator(valid=True),
        sql_executor=MockSqlExecutor(
            columns=["id"], column_types=["integer"], rows=[[1]], row_count=1
        ),
        schema_cache=MockSchemaCache(
            schemas={"test_db": schema}, databases=["test_db"]
        ),
        db_inference=MockDbInference(database="test_db"),
        result_validator=MockResultValidator(should_validate=False),
        retriever=SchemaRetriever(),
        settings=s,
    )


class TestSemaphoreBehavior:
    """Tests for semaphore-based concurrency control."""

    @pytest.mark.asyncio
    async def test_single_request_acquires_and_releases(self) -> None:
        engine = _make_engine()
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        assert response.database == "test_db"
        # Semaphore should be released after execution
        assert engine._semaphore._value == 20

    @pytest.mark.asyncio
    async def test_concurrent_requests_within_limit_succeed(self) -> None:
        engine = _make_engine(_make_settings(max_concurrent=5))
        request = QueryRequest(query="List all users", database="test_db")

        responses = await asyncio.gather(
            engine.execute(request),
            engine.execute(request),
            engine.execute(request),
        )

        assert len(responses) == 3
        for r in responses:
            assert r.database == "test_db"

    @pytest.mark.asyncio
    async def test_rate_limit_raised_when_at_capacity(self) -> None:
        engine = _make_engine(_make_settings(max_concurrent=1))
        # Manually acquire the only permit
        await engine._semaphore.acquire()

        request = QueryRequest(query="List all users", database="test_db")
        with pytest.raises(RateLimitedError) as exc_info:
            await engine.execute(request)

        assert "busy" in str(exc_info.value).lower() or "retry" in str(exc_info.value).lower()

        engine._semaphore.release()

    @pytest.mark.asyncio
    async def test_semaphore_released_even_on_error(self) -> None:
        from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
        from datetime import datetime, timezone

        settings = _make_settings(max_concurrent=1)
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
        # Use a validator that always fails to trigger an error
        validator = MockSqlValidator(valid=False, code="E_SQL_UNSAFE")
        engine = QueryEngine(
            sql_generator=MockSqlGenerator(sql="SELECT * FROM users"),
            sql_validator=validator,
            sql_executor=MockSqlExecutor(),
            schema_cache=MockSchemaCache(
                schemas={"test_db": schema}, databases=["test_db"]
            ),
            db_inference=MockDbInference(database="test_db"),
            result_validator=MockResultValidator(should_validate=False),
            retriever=SchemaRetriever(),
            settings=settings,
        )
        request = QueryRequest(query="List all users", database="test_db")

        with pytest.raises(Exception):
            await engine.execute(request)

        # Semaphore should be released even after error
        assert engine._semaphore._value == 1

    @pytest.mark.asyncio
    async def test_max_concurrent_requests_respected(self) -> None:
        for max_concurrent in [1, 5, 10, 50]:
            settings = _make_settings(max_concurrent=max_concurrent)
            engine = _make_engine(settings)
            assert engine._semaphore._value == max_concurrent
