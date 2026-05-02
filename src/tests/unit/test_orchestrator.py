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

from datetime import UTC, datetime

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
from pg_mcp.schema.retriever import SchemaRetriever
from tests.conftest import (
    MockDbInference,
    MockResultValidator,
    MockSchemaCache,
    MockSqlExecutor,
    MockSqlGenerator,
    MockSqlRewriter,
    MockSqlValidator,
)


def _make_settings(**overrides: object) -> Settings:
    defaults = {
        "pg_user": "test",
        "pg_password": "test",
        "max_retries": 2,
        "max_concurrent_requests": 20,
        "enable_validation": False,
    }
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
    sql_rewriter: MockSqlRewriter | None = None,
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
        loaded_at=datetime.now(UTC),
    )
    return QueryEngine(
        sql_generator=sql_gen or MockSqlGenerator(sql="SELECT * FROM users"),
        sql_rewriter=sql_rewriter or MockSqlRewriter(),
        sql_validator=sql_val or MockSqlValidator(valid=True),
        sql_executor=sql_exec
        or MockSqlExecutor(columns=["id"], column_types=["integer"], rows=[[1]], row_count=1),
        schema_cache=cache
        or MockSchemaCache(schemas={"test_db": sample_schema}, databases=["test_db"]),
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
        request = QueryRequest(query="List all users", database="test_db", return_type="sql")

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
        request = QueryRequest(query="", database="test_db", admin_action="refresh_schema")

        response = await engine.execute(request)

        assert response.refresh_result is not None
        assert response.refresh_result.succeeded == ["test_db"]


class TestValidationRetry:
    """Tests for SQL validation retry loop."""

    @pytest.mark.asyncio
    async def test_validation_failure_triggers_retry(self) -> None:
        # First call fails, second succeeds
        validator = MockSqlValidator(valid=False, toggle_on_call=True, reason="Mock failure")
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


class TestExecuteRetry:
    """Tests for the execute-stage retry loop (PG undefined column / table)."""

    @pytest.mark.asyncio
    async def test_undefined_column_triggers_retry_then_succeeds(self) -> None:
        # SqlExecuteError carrying sqlstate 42703 (undefined_column);
        # the orchestrator should retry instead of immediately raising.
        from pg_mcp.models.errors import SqlExecuteError

        err = SqlExecuteError("column f.sales_amount does not exist", sqlstate="42703")
        executor = MockSqlExecutor(raise_errors=[err, None])
        engine = _make_engine(sql_exec=executor, settings=_make_settings(max_retries=2))
        request = QueryRequest(query="2025 revenue", database="test_db")

        response = await engine.execute(request)

        assert response.error is None
        assert len(executor.execute_calls) == 2

    @pytest.mark.asyncio
    async def test_undefined_table_triggers_retry_then_succeeds(self) -> None:
        from pg_mcp.models.errors import SqlExecuteError

        err = SqlExecuteError('relation "fact.fact_gmv" does not exist', sqlstate="42P01")
        executor = MockSqlExecutor(raise_errors=[err, None])
        engine = _make_engine(sql_exec=executor, settings=_make_settings(max_retries=2))
        request = QueryRequest(query="rolling GMV", database="test_db")

        response = await engine.execute(request)

        assert response.error is None
        assert len(executor.execute_calls) == 2

    @pytest.mark.asyncio
    async def test_unrecoverable_error_raises_immediately(self) -> None:
        # Syntax errors / permission denied / etc. should not trigger retry.
        from pg_mcp.models.errors import SqlExecuteError

        err = SqlExecuteError("permission denied for table x", sqlstate="42501")
        executor = MockSqlExecutor(raise_errors=[err, None])
        engine = _make_engine(sql_exec=executor, settings=_make_settings(max_retries=2))
        request = QueryRequest(query="x", database="test_db")

        with pytest.raises(SqlExecuteError):
            await engine.execute(request)
        assert len(executor.execute_calls) == 1

    @pytest.mark.asyncio
    async def test_recoverable_error_persists_until_retries_exhausted(
        self,
    ) -> None:
        from pg_mcp.models.errors import SqlExecuteError

        err1 = SqlExecuteError("col1 does not exist", sqlstate="42703")
        err2 = SqlExecuteError("col2 does not exist", sqlstate="42703")
        err3 = SqlExecuteError("col3 does not exist", sqlstate="42703")
        executor = MockSqlExecutor(raise_errors=[err1, err2, err3])
        engine = _make_engine(sql_exec=executor, settings=_make_settings(max_retries=2))
        request = QueryRequest(query="x", database="test_db")

        with pytest.raises(SqlExecuteError):
            await engine.execute(request)
        # 1 initial + 2 retries
        assert len(executor.execute_calls) == 3


class TestResultValidation:
    """Tests for AI result validation flow."""

    @pytest.mark.asyncio
    async def test_result_validation_fix_triggers_regeneration(self) -> None:
        # First verdict is "fix", second (after re-execution) is "pass"
        result_validator = MockResultValidator(
            should_validate=True,
            verdict_sequence=["fix", "pass"],
            reason="Missing LIMIT",
            suggested_sql="SELECT * FROM users LIMIT 10",
        )
        generator = MockSqlGenerator(sql="SELECT * FROM users")
        executor = MockSqlExecutor(
            columns=["id"], column_types=["integer"], rows=[[1]], row_count=1
        )
        engine = _make_engine(
            sql_gen=generator,
            sql_exec=executor,
            result_val=result_validator,
            settings=_make_settings(enable_validation=True),
        )
        request = QueryRequest(query="List all users", database="test_db")

        response = await engine.execute(request)

        # Validation should have been used and the fix loop exercised:
        # - generator called once for initial + once for fix = 2 generations
        # - executor called once for initial + once for fix = 2 executions
        # - validator's validate() called once for initial fix and once
        #   to confirm the fixed result, so 2 invocations total.
        assert response.validation_used is True
        assert len(generator.generate_calls) == 2
        assert len(executor.execute_calls) == 2
        assert len(result_validator.validate_calls) == 2

    @pytest.mark.asyncio
    async def test_result_validation_fix_loop_passes_schema_names(self) -> None:
        # Ensure the orchestrator passes schema_names on EVERY execute,
        # including the fix-loop re-execution after a "fix" verdict.
        result_validator = MockResultValidator(
            should_validate=True,
            verdict_sequence=["fix", "pass"],
            reason="Bad SQL",
        )
        generator = MockSqlGenerator(sql="SELECT * FROM users")
        executor = MockSqlExecutor(
            columns=["id"], column_types=["integer"], rows=[[1]], row_count=1
        )
        engine = _make_engine(
            sql_gen=generator,
            sql_exec=executor,
            result_val=result_validator,
            settings=_make_settings(enable_validation=True),
        )
        request = QueryRequest(query="List users", database="test_db")

        await engine.execute(request)

        assert len(executor.execute_calls) == 2
        # Each execute call carries schema_names with at least "public".
        for _db, _sql, schema_names, _is_explain in executor.execute_calls:
            assert schema_names is not None
            assert "public" in schema_names

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
        # SqlExecutor wraps asyncpg.PostgresError into SqlExecuteError before
        # raising; orchestrator catches the wrapped form.
        from pg_mcp.models.errors import SqlExecuteError

        executor = MockSqlExecutor(raise_error=SqlExecuteError("syntax error"))
        engine = _make_engine(sql_exec=executor)
        request = QueryRequest(query="List all users", database="test_db")

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
    async def test_executor_receives_schema_names(self) -> None:
        # Multi-schema schemas: search_path must include all of them,
        # with ``public`` first when present.
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
                TableInfo(
                    schema_name="app",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            loaded_at=datetime.now(UTC),
        )
        cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
        executor = MockSqlExecutor(
            columns=["id"], column_types=["integer"], rows=[[1]], row_count=1
        )
        engine = _make_engine(cache=cache, sql_exec=executor)
        request = QueryRequest(query="List users", database="test_db")

        await engine.execute(request)

        assert len(executor.execute_calls) == 1
        _db, _sql, schema_names, _is_explain = executor.execute_calls[0]
        assert schema_names == ["public", "app"]

    @pytest.mark.asyncio
    async def test_validator_receives_schema_names(self) -> None:
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="app",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            loaded_at=datetime.now(UTC),
        )
        cache = MockSchemaCache(schemas={"test_db": schema}, databases=["test_db"])
        validator = MockSqlValidator(valid=True)
        engine = _make_engine(cache=cache, sql_val=validator)
        request = QueryRequest(query="List orders", database="test_db")

        await engine.execute(request)

        assert len(validator.validate_calls) == 1
        _sql, _schema, schema_names = validator.validate_calls[0]
        assert schema_names == ["app"]

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
            loaded_at=datetime.now(UTC),
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
        schema = DatabaseSchema(database="big_db", tables=tables, loaded_at=datetime.now(UTC))
        cache = MockSchemaCache(schemas={"big_db": schema}, databases=["big_db"])
        generator = MockSqlGenerator(sql="SELECT * FROM table_0")
        retriever = SchemaRetriever(max_tables_for_full=50)
        engine = _make_engine(sql_gen=generator, cache=cache, retriever=retriever)
        request = QueryRequest(query="show table_0", database="big_db")

        await engine.execute(request)

        _, schema_context, _ = generator.generate_calls[0]
        assert "Database: big_db" in schema_context
