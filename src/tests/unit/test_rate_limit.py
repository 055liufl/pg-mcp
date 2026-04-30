"""Unit tests for rate limiting / semaphore concurrency (engine/orchestrator.py).

Tests cover semaphore acquisition, timeout rejection, and concurrent
request limiting.
"""

import asyncio

import pytest

from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import RateLimitedError
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.schema import (
    ColumnInfo,
    DatabaseSchema,
    TableInfo,
)
from pg_mcp.protocols import ExecutionResult
from tests.conftest import (
    MockSqlGenerator,
    MockSqlValidator,
    MockSqlExecutor,
    MockSchemaCache,
    MockDbInference,
    MockResultValidator,
)


# =============================================================================
# Helpers
# =============================================================================

def _make_settings(max_concurrent: int = 2) -> "Settings":
    """Build settings with limited concurrency."""
    from pg_mcp.config import Settings

    return Settings(
        pg_user="test",
        pg_password="test",  # type: ignore[arg-type]
        openai_api_key="sk-test",  # type: ignore[arg-type]
        max_concurrent_requests=max_concurrent,
        max_retries=2,
    )


def _make_schema() -> DatabaseSchema:
    """Build a minimal DatabaseSchema."""
    from datetime import datetime, timezone

    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[ColumnInfo(name="id", type="integer", nullable=False)],
            ),
        ],
        views=[],
        indexes=[],
        foreign_keys=[],
        constraints=[],
        enum_types=[],
        composite_types=[],
        allowed_functions=set(),
        loaded_at=datetime.now(timezone.utc),
    )


def _make_engine(max_concurrent: int = 2) -> QueryEngine:
    """Build a QueryEngine with limited concurrency."""
    from pg_mcp.schema.retriever import SchemaRetriever

    schema = _make_schema()
    settings = _make_settings(max_concurrent)
    return QueryEngine(
        sql_generator=MockSqlGenerator(),
        sql_validator=MockSqlValidator(valid=True),
        sql_executor=MockSqlExecutor(),
        schema_cache=MockSchemaCache(
            schemas={"test_db": schema},
            databases=["test_db"],
        ),
        db_inference=MockDbInference(),
        result_validator=MockResultValidator(should_validate=False),
        retriever=SchemaRetriever(max_tables_for_full=50),
        settings=settings,
    )


# =============================================================================
# Semaphore acquisition
# =============================================================================

@pytest.mark.asyncio
async def test_rate_limit_single_request_acquired() -> None:
    """A single request should successfully acquire the semaphore."""
    engine = _make_engine(max_concurrent=1)
    request = QueryRequest(query="List all users", database="test_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.database == "test_db"


@pytest.mark.asyncio
async def test_rate_limit_exceeded_raises() -> None:
    """When semaphore is exhausted, new requests should raise RateLimitedError."""
    engine = _make_engine(max_concurrent=1)
    request = QueryRequest(query="List all users", database="test_db")

    # Hold the semaphore by starting a request that blocks
    semaphore = engine._semaphore
    await semaphore.acquire()

    try:
        with pytest.raises(RateLimitedError) as exc_info:
            await engine.execute(request)

        assert "繁忙" in str(exc_info.value) or "rate" in str(exc_info.value).lower()
    finally:
        semaphore.release()


@pytest.mark.asyncio
async def test_rate_limit_released_after_completion() -> None:
    """Semaphore should be released after request completes."""
    engine = _make_engine(max_concurrent=1)
    request = QueryRequest(query="List all users", database="test_db")

    # First request
    response1 = await engine.execute(request)
    assert response1.error is None

    # Second request should succeed because semaphore was released
    response2 = await engine.execute(request)
    assert response2.error is None


@pytest.mark.asyncio
async def test_rate_limit_released_on_error() -> None:
    """Semaphore should be released even when request raises an exception."""
    from pg_mcp.models.errors import DbNotFoundError

    engine = _make_engine(max_concurrent=1)
    bad_request = QueryRequest(query="List all users", database="missing_db")

    # First request fails
    with pytest.raises(DbNotFoundError):
        await engine.execute(bad_request)

    # Semaphore should be released; second request can proceed
    # (but will also fail with same error since DB doesn't exist)
    with pytest.raises(DbNotFoundError):
        await engine.execute(bad_request)


@pytest.mark.asyncio
async def test_rate_limit_concurrent_requests_within_limit() -> None:
    """Multiple concurrent requests within limit should all succeed."""
    engine = _make_engine(max_concurrent=3)
    request = QueryRequest(query="List all users", database="test_db")

    # Launch 3 concurrent requests
    responses = await asyncio.gather(
        engine.execute(request),
        engine.execute(request),
        engine.execute(request),
    )

    for response in responses:
        assert response.error is None
        assert response.database == "test_db"


@pytest.mark.asyncio
async def test_rate_limit_concurrent_requests_over_limit() -> None:
    """Concurrent requests exceeding limit should have some rejected."""
    engine = _make_engine(max_concurrent=1)
    request = QueryRequest(query="List all users", database="test_db")

    # Hold the semaphore
    await engine._semaphore.acquire()

    async def delayed_release() -> None:
        await asyncio.sleep(0.1)
        engine._semaphore.release()

    # Start a task that will release the semaphore after a delay
    release_task = asyncio.create_task(delayed_release())

    try:
        # This should fail immediately because semaphore is held
        with pytest.raises(RateLimitedError):
            await engine.execute(request)
    finally:
        await release_task


@pytest.mark.asyncio
async def test_rate_limit_exact_capacity() -> None:
    """Exactly max_concurrent requests should all succeed."""
    engine = _make_engine(max_concurrent=2)
    request = QueryRequest(query="List all users", database="test_db")

    # Use a slow executor to hold requests in-flight
    slow_executor = MockSqlExecutor()
    original_execute = slow_executor.execute

    async def slow_execute(database: str, sql: str, schema_names: list[str] | None = None, is_explain: bool = False) -> ExecutionResult:
        await asyncio.sleep(0.05)
        return ExecutionResult(
            columns=["id"],
            column_types=["integer"],
            rows=[[1]],
            row_count=1,
        )

    slow_executor.execute = slow_execute  # type: ignore[method-assign]

    from pg_mcp.schema.retriever import SchemaRetriever
    from pg_mcp.config import Settings

    schema = _make_schema()
    settings = Settings(
        pg_user="test",
        pg_password="test",  # type: ignore[arg-type]
        openai_api_key="sk-test",  # type: ignore[arg-type]
        max_concurrent_requests=2,
        max_retries=2,
    )
    engine = QueryEngine(
        sql_generator=MockSqlGenerator(),
        sql_validator=MockSqlValidator(valid=True),
        sql_executor=slow_executor,
        schema_cache=MockSchemaCache(
            schemas={"test_db": schema},
            databases=["test_db"],
        ),
        db_inference=MockDbInference(),
        result_validator=MockResultValidator(should_validate=False),
        retriever=SchemaRetriever(max_tables_for_full=50),
        settings=settings,
    )

    # Launch exactly 2 concurrent requests
    responses = await asyncio.gather(
        engine.execute(request),
        engine.execute(request),
    )

    for response in responses:
        assert response.error is None


@pytest.mark.asyncio
async def test_rate_limit_zero_concurrency() -> None:
    """max_concurrent_requests=0 means semaphore starts at 0; all requests rejected."""
    engine = _make_engine(max_concurrent=0)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(RateLimitedError):
        await engine.execute(request)
