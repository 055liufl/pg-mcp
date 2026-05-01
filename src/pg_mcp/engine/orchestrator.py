"""QueryEngine main orchestrator with retry logic."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

import asyncpg
import structlog

from pg_mcp.config import Settings
from pg_mcp.models.errors import (
    DbNotFoundError,
    LlmError,
    LlmTimeoutError,
    RateLimitedError,
    SqlExecuteError,
    SqlGenerateError,
    SqlParseError,
    SqlTimeoutError,
    SqlUnsafeError,
    ValidationFailedError,
)
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import AdminRefreshResult, QueryResponse
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.observability.logging import get_logger
from pg_mcp.observability.sanitizer import sanitize_sql
from pg_mcp.protocols import (
    DbInferenceProtocol,
    ResultValidatorProtocol,
    SchemaCacheProtocol,
    SqlExecutorProtocol,
    SqlGeneratorProtocol,
    SqlValidatorProtocol,
)

if TYPE_CHECKING:
    from pg_mcp.schema.retriever import SchemaRetriever


class QueryEngine:
    """Main orchestrator for natural-language-to-SQL query execution.

    Coordinates database inference, schema retrieval, SQL generation,
    validation, execution, and optional result validation with retry loops.

    All dependencies are injected via constructor to support testing and
    alternative implementations.
    """

    def __init__(
        self,
        sql_generator: SqlGeneratorProtocol,
        sql_validator: SqlValidatorProtocol,
        sql_executor: SqlExecutorProtocol,
        schema_cache: SchemaCacheProtocol,
        db_inference: DbInferenceProtocol,
        result_validator: ResultValidatorProtocol,
        retriever: SchemaRetriever,
        settings: Settings,
    ) -> None:
        self._sql_gen = sql_generator
        self._sql_val = sql_validator
        self._sql_exec = sql_executor
        self._cache = schema_cache
        self._db_inference = db_inference
        self._result_val = result_validator
        self._retriever = retriever
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    async def execute(self, request: QueryRequest) -> QueryResponse:
        """Execute a natural language query through the full pipeline.

        Args:
            request: The user's query request.

        Returns:
            QueryResponse with results or error details.

        Raises:
            RateLimitedError: If the server is at max concurrent capacity.
            PgMcpError: For business-level errors (propagated to caller).
        """
        request_id = str(uuid.uuid4())
        log = get_logger().bind(request_id=request_id)

        # 1. Concurrency control (non-blocking with short timeout)
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.01)
        except TimeoutError:
            raise RateLimitedError("Server is busy, please retry later")

        try:
            return await self._do_execute(request, request_id, log)
        finally:
            self._semaphore.release()

    async def _do_execute(
        self, request: QueryRequest, request_id: str, log: structlog.stdlib.BoundLogger
    ) -> QueryResponse:
        """Internal execution pipeline."""
        start_time = time.monotonic()
        log.info(
            "request_received",
            query_length=len(request.query),
            database=request.database,
            return_type=request.return_type,
        )

        # 3. Admin action handling
        if request.admin_action == "refresh_schema":
            result = await self._cache.refresh(request.database)
            log.info("admin_refresh_completed", elapsed_ms=self._elapsed_ms(start_time))
            return QueryResponse(
                request_id=request_id,
                database=request.database,
                refresh_result=AdminRefreshResult(
                    succeeded=result.succeeded,
                    failed=result.failed,
                ),
            )

        # 4. Resolve target database
        if request.database:
            if request.database not in self._cache.discovered_databases():
                raise DbNotFoundError(f"Database not found: {request.database}")
            database = request.database
        else:
            database = await self._db_inference.infer(request.query)

        # 5. Load schema
        schema = await self._cache.get_schema(database)
        schema_loaded_at = schema.loaded_at.isoformat()
        log.info(
            "schema_loaded",
            database=database,
            table_count=schema.table_count(),
            elapsed_ms=self._elapsed_ms(start_time),
        )

        # 6. Build schema context for LLM
        if self._retriever.should_use_retrieval(schema):
            schema_context = self._retriever.retrieve(request.query, schema)
        else:
            schema_context = schema.to_prompt_text()

        # Derive deterministic search_path: ``public`` first if it exists,
        # then any remaining schemas in alphabetical order. Both validator
        # and executor use this same list, so unqualified table references
        # canonicalize to the same schema PostgreSQL would resolve.
        schema_names = self._derive_schema_names(schema)

        # 7. SQL generation with validation retry loop
        feedback: str | None = None
        sql: str | None = None
        is_explain = False
        gen_result = None

        for attempt in range(self._settings.max_retries + 1):
            try:
                gen_result = await self._sql_gen.generate(
                    request.query, schema_context, feedback
                )
            except (LlmTimeoutError, LlmError):
                raise

            sql = gen_result.sql
            log.info(
                "sql_generated",
                attempt=attempt,
                prompt_tokens=gen_result.prompt_tokens,
                completion_tokens=gen_result.completion_tokens,
                elapsed_ms=self._elapsed_ms(start_time),
            )

            # 8. SQL validation
            val_result = self._sql_val.validate(
                sql, schema, schema_names=schema_names
            )
            if val_result.valid:
                is_explain = val_result.is_explain
                break

            log.warning(
                "sql_validation_failed",
                attempt=attempt,
                reason=val_result.reason,
                sql=sanitize_sql(sql),
            )

            if attempt < self._settings.max_retries:
                feedback = f"Previous SQL was rejected: {val_result.reason}"
            else:
                if val_result.code == "E_SQL_PARSE":
                    raise SqlParseError(val_result.reason or "SQL parse failed")
                raise SqlUnsafeError(val_result.reason or "SQL safety check failed")
        else:
            # All retries exhausted without valid SQL
            raise SqlGenerateError("Failed to generate valid SQL after all retries")

        # 9. return_type=sql: return generated SQL without executing
        if request.return_type == "sql":
            return QueryResponse(
                request_id=request_id,
                database=database,
                sql=sql,
                schema_loaded_at=schema_loaded_at,
            )

        # 10. SQL execution
        exec_start = time.monotonic()
        try:
            exec_result = await self._sql_exec.execute(
                database, sql, schema_names=schema_names, is_explain=is_explain
            )
        except SqlTimeoutError:
            raise
        except asyncpg.PostgresError as e:
            raise SqlExecuteError(str(e))

        log.info(
            "sql_executed",
            row_count=exec_result.row_count,
            truncated=exec_result.truncated,
            elapsed_ms=int((time.monotonic() - exec_start) * 1000),
        )

        # 11. Optional result validation
        validation_used = False
        if gen_result is not None and self._result_val.should_validate(
            database, sql, exec_result, gen_result
        ):
            validation_used = True
            verdict = await self._result_val.validate(
                request.query, sql, exec_result, schema
            )

            if verdict.verdict == "fix":
                # Re-generate and re-execute with validation feedback
                val_feedback = f"Result validation feedback: {verdict.reason}"
                if verdict.suggested_sql:
                    val_feedback += f" Suggested SQL: {verdict.suggested_sql}"

                for val_attempt in range(self._settings.max_retries + 1):
                    try:
                        gen_result = await self._sql_gen.generate(
                            request.query, schema_context, val_feedback
                        )
                    except (LlmTimeoutError, LlmError):
                        raise

                    sql = gen_result.sql
                    val_result = self._sql_val.validate(
                        sql, schema, schema_names=schema_names
                    )
                    if not val_result.valid:
                        if val_attempt < self._settings.max_retries:
                            val_feedback = (
                                f"Fix attempt {val_attempt + 1} rejected: {val_result.reason}"
                            )
                            continue
                        raise SqlUnsafeError(
                            val_result.reason or "Fixed SQL safety check failed"
                        )

                    is_explain = val_result.is_explain
                    try:
                        exec_result = await self._sql_exec.execute(
                            database,
                            sql,
                            schema_names=schema_names,
                            is_explain=is_explain,
                        )
                    except SqlTimeoutError:
                        raise
                    except asyncpg.PostgresError as e:
                        raise SqlExecuteError(str(e))

                    # Re-validate the fixed result
                    verdict = await self._result_val.validate(
                        request.query, sql, exec_result, schema
                    )
                    if verdict.verdict == "fix":
                        if val_attempt < self._settings.max_retries:
                            val_feedback = (
                                f"Fix attempt {val_attempt + 1} result feedback: {verdict.reason}"
                            )
                            continue
                        raise ValidationFailedError(
                            "Result validation could not be satisfied after repeated fixes"
                        )
                    elif verdict.verdict == "fail":
                        raise ValidationFailedError(
                            verdict.reason or "Result validation failed"
                        )
                    break
                else:
                    raise ValidationFailedError(
                        "Result validation fix loop exhausted all retries"
                    )

            elif verdict.verdict == "fail":
                raise ValidationFailedError(verdict.reason or "Result validation failed")

        # 12. Assemble response
        total_elapsed_ms = int((time.monotonic() - start_time) * 1000)
        log.info("request_completed", total_elapsed_ms=total_elapsed_ms)

        return QueryResponse(
            request_id=request_id,
            database=database,
            sql=sql,
            columns=exec_result.columns,
            column_types=exec_result.column_types,
            rows=exec_result.rows,
            row_count=exec_result.row_count,
            truncated=exec_result.truncated,
            truncated_reason=exec_result.truncated_reason,
            validation_used=validation_used,
            schema_loaded_at=schema_loaded_at,
        )

    def _elapsed_ms(self, start_time: float) -> int:
        """Calculate elapsed milliseconds since start_time."""
        return int((time.monotonic() - start_time) * 1000)

    @staticmethod
    def _derive_schema_names(schema: DatabaseSchema) -> list[str]:
        """Build a deterministic ``search_path`` list from a loaded schema.

        Places ``public`` first when present (matching PostgreSQL's
        default), then any remaining schemas in alphabetical order. Both
        validator and executor consume this list to ensure consistent
        unqualified-table resolution.
        """
        seen: set[str] = set()
        for table in schema.tables:
            seen.add(table.schema_name)
        for view in schema.views:
            seen.add(view.schema_name)
        ordered: list[str] = []
        if "public" in seen:
            ordered.append("public")
            seen.discard("public")
        ordered.extend(sorted(seen))
        return ordered or ["public"]
