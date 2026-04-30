"""Unit tests for QueryEngine orchestrator (engine/orchestrator.py).

Tests cover the full execution flow, error conversion, retry logic,
result validation fix path, and admin action handling using mocked dependencies.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import (
    DbNotFoundError,
    DbInferAmbiguousError,
    SchemaNotReadyError,
    SqlUnsafeError,
    SqlParseError,
    SqlExecuteError,
    SqlTimeoutError,
    ValidationFailedError,
    RateLimitedError,
    LlmTimeoutError,
    LlmError,
)
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import QueryResponse
from pg_mcp.models.schema import (
    ColumnInfo,
    DatabaseSchema,
    TableInfo,
)
from pg_mcp.protocols import (
    SqlGenerationResult,
    ValidationResult,
    ExecutionResult,
    ValidationVerdict,
)
from pg_mcp.schema.retriever import SchemaRetriever
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

def _make_settings() -> "Settings":
    """Build minimal settings for testing."""
    from pg_mcp.config import Settings

    return Settings(
        pg_user="test",
        pg_password="test",  # type: ignore[arg-type]
        openai_api_key="sk-test",  # type: ignore[arg-type]
        max_retries=2,
        max_concurrent_requests=20,
    )


def _make_engine(
    sql_generator: MockSqlGenerator | None = None,
    sql_validator: MockSqlValidator | None = None,
    sql_executor: MockSqlExecutor | None = None,
    schema_cache: MockSchemaCache | None = None,
    db_inference: MockDbInference | None = None,
    result_validator: MockResultValidator | None = None,
    retriever: SchemaRetriever | None = None,
) -> QueryEngine:
    """Build a QueryEngine with mocked dependencies."""
    from pg_mcp.config import Settings

    settings = _make_settings()
    return QueryEngine(
        sql_generator=sql_generator or MockSqlGenerator(),
        sql_validator=sql_validator or MockSqlValidator(valid=True),
        sql_executor=sql_executor or MockSqlExecutor(),
        schema_cache=schema_cache or MockSchemaCache(),
        db_inference=db_inference or MockDbInference(),
        result_validator=result_validator or MockResultValidator(should_validate=False),
        retriever=retriever or SchemaRetriever(max_tables_for_full=50),
        settings=settings,
    )


def _make_schema(database: str = "test_db") -> DatabaseSchema:
    """Build a minimal DatabaseSchema."""
    return DatabaseSchema(
        database=database,
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


# =============================================================================
# Full flow tests
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_full_flow_returns_result() -> None:
    """Complete flow: validate, generate, validate SQL, execute, return result."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(
        schemas={"test_db": schema},
        databases=["test_db"],
    )
    executor = MockSqlExecutor(
        columns=["id", "name"],
        column_types=["integer", "text"],
        rows=[[1, "Alice"], [2, "Bob"]],
        row_count=2,
    )
    engine = _make_engine(
        schema_cache=cache,
        sql_executor=executor,
    )
    request = QueryRequest(query="List all users", database="test_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.database == "test_db"
    assert response.sql == "SELECT * FROM users"
    assert response.columns == ["id", "name"]
    assert response.rows == [[1, "Alice"], [2, "Bob"]]
    assert response.row_count == 2
    assert response.validation_used is False
    assert response.request_id is not None


@pytest.mark.asyncio
async def test_orchestrator_sql_only_return_type() -> None:
    """return_type=sql should skip execution and return only SQL."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(
        schemas={"test_db": schema},
        databases=["test_db"],
    )
    executor = MockSqlExecutor()
    engine = _make_engine(
        schema_cache=cache,
        sql_executor=executor,
    )
    request = QueryRequest(query="List all users", database="test_db", return_type="sql")

    response = await engine.execute(request)

    assert response.error is None
    assert response.sql == "SELECT * FROM users"
    assert response.columns is None
    assert response.rows is None
    assert response.row_count is None
    # Executor should NOT have been called
    assert len(executor.execute_calls) == 0


# =============================================================================
# Admin action
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_admin_refresh_schema() -> None:
    """admin_action=refresh_schema should trigger cache refresh."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(
        schemas={"test_db": schema},
        databases=["test_db"],
    )
    engine = _make_engine(schema_cache=cache)
    request = QueryRequest(
        query="",
        database="test_db",
        admin_action="refresh_schema",
    )

    response = await engine.execute(request)

    assert response.error is None
    assert response.refresh_result is not None
    assert "test_db" in cache.refresh_calls


# =============================================================================
# Database resolution
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_explicit_database_not_found() -> None:
    """Explicit database not in discovered list should raise DbNotFoundError."""
    cache = MockSchemaCache(schemas={}, databases=["other_db"])
    engine = _make_engine(schema_cache=cache)
    request = QueryRequest(query="List all users", database="missing_db")

    with pytest.raises(DbNotFoundError):
        await engine.execute(request)


@pytest.mark.asyncio
async def test_orchestrator_inferred_database_ambiguous() -> None:
    """Ambiguous database inference should raise DbInferAmbiguousError."""
    cache = MockSchemaCache(schemas={}, databases=["db1", "db2"])
    inference = MockDbInference(
        raise_error=DbInferAmbiguousError(
            "Ambiguous",
            candidates=["db1", "db2"],
        ),
    )
    engine = _make_engine(schema_cache=cache, db_inference=inference)
    request = QueryRequest(query="List all users")

    with pytest.raises(DbInferAmbiguousError) as exc_info:
        await engine.execute(request)

    assert exc_info.value.candidates == ["db1", "db2"]


@pytest.mark.asyncio
async def test_orchestrator_inferred_database_success() -> None:
    """Successful database inference should proceed with the inferred DB."""
    schema = _make_schema("inferred_db")
    cache = MockSchemaCache(
        schemas={"inferred_db": schema},
        databases=["inferred_db"],
    )
    inference = MockDbInference(database="inferred_db")
    engine = _make_engine(schema_cache=cache, db_inference=inference)
    request = QueryRequest(query="List all users")

    response = await engine.execute(request)

    assert response.database == "inferred_db"
    assert inference.infer_calls == ["List all users"]


# =============================================================================
# Schema not ready
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_schema_not_ready_raises() -> None:
    """Schema not ready should raise SchemaNotReadyError."""
    cache = MockSchemaCache(
        schemas={},
        databases=["test_db"],
        raise_on_get=SchemaNotReadyError("Loading", retry_after_ms=2000),
    )
    engine = _make_engine(schema_cache=cache)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(SchemaNotReadyError) as exc_info:
        await engine.execute(request)

    assert exc_info.value.retry_after_ms == 2000


# =============================================================================
# SQL generation errors
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_llm_timeout_raises() -> None:
    """LLM timeout during SQL generation should raise LlmTimeoutError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    generator = MockSqlGenerator(raise_error=LlmTimeoutError("LLM timeout"))
    engine = _make_engine(schema_cache=cache, sql_generator=generator)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(LlmTimeoutError):
        await engine.execute(request)


@pytest.mark.asyncio
async def test_orchestrator_llm_error_raises() -> None:
    """LLM API error during SQL generation should raise LlmError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    generator = MockSqlGenerator(raise_error=LlmError("API error"))
    engine = _make_engine(schema_cache=cache, sql_generator=generator)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(LlmError):
        await engine.execute(request)


# =============================================================================
# SQL validation retry
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_validation_retry_then_success() -> None:
    """SQL validation fails once then succeeds after retry."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    validator = MockSqlValidator(valid=False, toggle_on_call=True)
    generator = MockSqlGenerator(sql="SELECT * FROM users")
    engine = _make_engine(
        schema_cache=cache,
        sql_validator=validator,
        sql_generator=generator,
    )
    request = QueryRequest(query="List all users", database="test_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.sql == "SELECT * FROM users"
    # Should have called validate at least twice (fail + success)
    assert len(validator.validate_calls) >= 2


@pytest.mark.asyncio
async def test_orchestrator_validation_retry_exhausted_raises_sql_unsafe() -> None:
    """SQL validation fails on all retries should raise SqlUnsafeError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    validator = MockSqlValidator(valid=False, code="E_SQL_UNSAFE", reason="Unsafe")
    engine = _make_engine(schema_cache=cache, sql_validator=validator)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(SqlUnsafeError):
        await engine.execute(request)


@pytest.mark.asyncio
async def test_orchestrator_validation_parse_error_raises_sql_parse() -> None:
    """SQL parse error on final retry should raise SqlParseError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    validator = MockSqlValidator(valid=False, code="E_SQL_PARSE", reason="Parse failed")
    engine = _make_engine(schema_cache=cache, sql_validator=validator)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(SqlParseError):
        await engine.execute(request)


# =============================================================================
# SQL execution errors
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_execute_timeout_raises() -> None:
    """SQL execution timeout should raise SqlTimeoutError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    executor = MockSqlExecutor(raise_error=SqlTimeoutError("Query timed out"))
    engine = _make_engine(schema_cache=cache, sql_executor=executor)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(SqlTimeoutError):
        await engine.execute(request)


@pytest.mark.asyncio
async def test_orchestrator_execute_postgres_error_raises_sql_execute() -> None:
    """PostgreSQL execution error should raise SqlExecuteError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    import asyncpg
    executor = MockSqlExecutor(raise_error=asyncpg.PostgresError("relation does not exist"))
    engine = _make_engine(schema_cache=cache, sql_executor=executor)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(SqlExecuteError):
        await engine.execute(request)


# =============================================================================
# Result validation
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_result_validation_pass() -> None:
    """Result validation triggered and passes should return normal response."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    result_validator = MockResultValidator(should_validate=True, verdict="pass")
    engine = _make_engine(schema_cache=cache, result_validator=result_validator)
    request = QueryRequest(query="List all users", database="test_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.validation_used is True


@pytest.mark.asyncio
async def test_orchestrator_result_validation_fail_raises() -> None:
    """Result validation verdict=fail should raise ValidationFailedError."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    result_validator = MockResultValidator(should_validate=True, verdict="fail", reason="Bad SQL")
    engine = _make_engine(schema_cache=cache, result_validator=result_validator)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(ValidationFailedError) as exc_info:
        await engine.execute(request)

    assert "Bad SQL" in str(exc_info.value)


@pytest.mark.asyncio
async def test_orchestrator_result_validation_fix_then_pass() -> None:
    """Result validation verdict=fix, retry succeeds, then passes."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    result_validator = MockResultValidator(
        should_validate=True,
        verdict="fix",
        reason="Add WHERE clause",
        suggested_sql="SELECT * FROM users WHERE active = true",
    )
    # After fix, the second should_validate call returns False (no re-validation)
    call_count = 0
    original_should_validate = result_validator.should_validate

    def tracking_should_validate(database, sql, result, generation):
        nonlocal call_count
        call_count += 1
        return call_count == 1  # Only trigger on first call

    result_validator.should_validate = tracking_should_validate  # type: ignore[method-assign]

    # Second validate call (after fix) returns pass
    validate_call_count = 0
    original_validate = result_validator.validate

    async def tracking_validate(user_query, sql, result, schema):
        nonlocal validate_call_count
        validate_call_count += 1
        if validate_call_count == 1:
            return ValidationVerdict(verdict="fix", reason="Add WHERE", suggested_sql="SELECT * FROM users WHERE active = true")
        return ValidationVerdict(verdict="pass")

    result_validator.validate = tracking_validate  # type: ignore[method-assign]

    generator = MockSqlGenerator(sql="SELECT * FROM users WHERE active = true")
    engine = _make_engine(
        schema_cache=cache,
        result_validator=result_validator,
        sql_generator=generator,
    )
    request = QueryRequest(query="List active users", database="test_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.sql == "SELECT * FROM users WHERE active = true"


@pytest.mark.asyncio
async def test_orchestrator_result_validation_not_triggered() -> None:
    """When should_validate returns False, validation is skipped."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    result_validator = MockResultValidator(should_validate=False)
    engine = _make_engine(schema_cache=cache, result_validator=result_validator)
    request = QueryRequest(query="List all users", database="test_db")

    response = await engine.execute(request)

    assert response.validation_used is False
    assert len(result_validator.validate_calls) == 0


# =============================================================================
# Schema retrieval (large schema)
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_large_schema_uses_retrieval() -> None:
    """Large schema should trigger retrieval-based context building."""
    from pg_mcp.schema.retriever import SchemaRetriever

    # Build a large schema
    tables = []
    for i in range(60):
        tables.append(
            TableInfo(
                schema_name="public",
                table_name=f"table_{i:03d}",
                columns=[ColumnInfo(name="id", type="integer", nullable=False)],
            )
        )
    schema = DatabaseSchema(
        database="large_db",
        tables=tables,
        views=[],
        indexes=[],
        foreign_keys=[],
        constraints=[],
        enum_types=[],
        composite_types=[],
        allowed_functions=set(),
        loaded_at=datetime.now(timezone.utc),
    )
    cache = MockSchemaCache(schemas={"large_db": schema}, databases=["large_db"])
    retriever = SchemaRetriever(max_tables_for_full=50)
    engine = _make_engine(schema_cache=cache, retriever=retriever)
    request = QueryRequest(query="Show table_001", database="large_db")

    response = await engine.execute(request)

    assert response.error is None
    assert response.database == "large_db"


# =============================================================================
# Request ID
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_generates_unique_request_id() -> None:
    """Each request should get a unique request_id."""
    schema = _make_schema("test_db")
    cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
    engine = _make_engine(schema_cache=cache)
    request = QueryRequest(query="List all users", database="test_db")

    response1 = await engine.execute(request)
    response2 = await engine.execute(request)

    assert response1.request_id != response2.request_id
    assert len(response1.request_id) > 0
    assert len(response2.request_id) > 0


# =============================================================================
# Error propagation
# =============================================================================

@pytest.mark.asyncio
async def test_orchestrator_pg_mcp_error_propagates() -> None:
    """PgMcpError subclasses should propagate without wrapping."""
    cache = MockSchemaCache(schemas={}, databases=["test_db"])
    engine = _make_engine(schema_cache=cache)
    request = QueryRequest(query="List all users", database="test_db")

    with pytest.raises(DbNotFoundError):
        await engine.execute(request)
