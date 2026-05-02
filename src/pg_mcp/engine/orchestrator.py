"""QueryEngine main orchestrator with retry logic."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

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
    SqlRewriterProtocol,
    SqlValidatorProtocol,
)

if TYPE_CHECKING:
    from pg_mcp.schema.retriever import SchemaRetriever


# Common non-PostgreSQL function hallucinations the LLM might emit, with
# their PostgreSQL equivalents. Used to enrich validator-rejection feedback
# so retries can converge faster.
_FUNCTION_REPLACEMENT_HINTS: dict[str, str] = {
    "timestamp_trunc": "date_trunc",
    "datetime_trunc": "date_trunc",
    "time_trunc": "date_trunc",
    "timestamptz_trunc": "date_trunc",
    "datetime_part": "EXTRACT(field FROM ts) or date_part",
    "timestamp_part": "EXTRACT(field FROM ts) or date_part",
    "datetime_diff": "(a - b) interval arithmetic",
    "timestamp_diff": "(a - b) interval arithmetic",
    "date_diff": "(a::date - b::date)",
    "date_add": "ts + INTERVAL 'N units'",
    "dateadd": "ts + INTERVAL 'N units'",
    "timestampadd": "ts + INTERVAL 'N units'",
    "safe_cast": "CAST(value AS type) or value::type",
    "try_cast": "CAST(value AS type) or value::type",
}


def _build_validator_feedback(reason: str) -> str:
    """Translate a validator rejection reason into actionable LLM feedback.

    For "Function not in allowlist: X" rejections caused by non-PostgreSQL
    function hallucinations, append a concrete replacement hint so the LLM
    has a higher chance of producing correct PostgreSQL on retry.
    """
    base = f"Previous SQL was rejected: {reason}"
    prefix = "Function not in allowlist: "
    if not reason.startswith(prefix):
        return base
    bad_func = reason[len(prefix):].strip().lower()
    hint = _FUNCTION_REPLACEMENT_HINTS.get(bad_func)
    if hint is None:
        return (
            f"{base}. The function `{bad_func}` is not available in this "
            f"PostgreSQL database. Use only PostgreSQL standard functions."
        )
    return (
        f"{base}. `{bad_func}` does NOT exist in PostgreSQL — replace it "
        f"with `{hint}`."
    )


# PostgreSQL SQLSTATE codes that indicate the LLM referenced a name that
# doesn't exist in the schema. These are recoverable on retry — feed the
# error back so the LLM can pick a different column / table / function.
_RECOVERABLE_PG_SQLSTATES: frozenset[str] = frozenset({
    "42703",  # undefined_column
    "42P01",  # undefined_table
    "42883",  # undefined_function
    "42704",  # undefined_object
    "42P02",  # undefined_parameter
    "42P10",  # invalid_column_reference (e.g. ORDER BY non-existent col)
})

# Columns that are commonly hallucinated by LLMs when they cannot see the
# real schema clearly.  Used to enrich retry feedback with concrete
# alternatives.
_COMMON_AMOUNT_COLS: frozenset[str] = frozenset({
    "amount", "total_amount", "sales_amount", "revenue_amount",
    "gmv", "total_sales", "sales_total", "order_total",
})


def _build_execute_feedback(
    error: SqlExecuteError,
    schema: DatabaseSchema | None = None,
    sql: str | None = None,
) -> str | None:
    """Translate a PG execution error into actionable LLM feedback.

    Returns ``None`` for unrecoverable errors (timeout, permission denied,
    syntax error, etc.) so callers know to raise instead of retrying.
    """
    sqlstate = error.sqlstate or ""
    if sqlstate not in _RECOVERABLE_PG_SQLSTATES:
        return None
    msg = str(error).strip()

    # For undefined_column, try to extract the hallucinated column name
    # and suggest real alternatives from the schema.
    if sqlstate == "42703" and schema is not None:
        # Collect all amount / numeric columns from the schema as a
        # concrete reference list.
        real_cols: list[str] = []
        for table in schema.tables:
            for col in table.columns:
                cname = col.name.lower()
                # Include any column that looks like an amount / metric.
                if any(
                    kw in cname
                    for kw in (
                        "amount", "revenue", "total", "net", "gross",
                        "discount", "tax", "shipping", "cost", "price",
                        "quantity", "count", "value", "fee", "profit",
                    )
                ):
                    real_cols.append(
                        f"{table.schema_name}.{table.table_name}.{col.name}"
                    )
        if real_cols:
            col_hint = (
                "Available numeric/amount columns in this database: "
                + ", ".join(real_cols[:20])
            )
            if len(real_cols) > 20:
                col_hint += f" (and {len(real_cols) - 20} more)"
            return (
                f"Previous SQL failed at execution: {msg}. "
                f"Do NOT invent column names like "
                f"{', '.join(sorted(_COMMON_AMOUNT_COLS))}. "
                f"Use ONLY exact column names from the schema. "
                f"{col_hint}"
            )

    return (
        f"Previous SQL failed at execution: {msg}. The referenced "
        f"column/table/function does not exist in this PostgreSQL "
        f"database — re-read the schema context and pick a name that "
        f"actually appears there. Do NOT invent column or table names."
    )


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
        sql_rewriter: SqlRewriterProtocol,
        sql_validator: SqlValidatorProtocol,
        sql_executor: SqlExecutorProtocol,
        schema_cache: SchemaCacheProtocol,
        db_inference: DbInferenceProtocol,
        result_validator: ResultValidatorProtocol,
        retriever: SchemaRetriever,
        settings: Settings,
    ) -> None:
        self._sql_gen = sql_generator
        self._sql_rewriter = sql_rewriter
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
            raise RateLimitedError("服务器繁忙，请稍后重试")

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
                raise DbNotFoundError(f"数据库不存在: {request.database}")
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
            rewritten_sql = self._sql_rewriter.rewrite(sql)
            if rewritten_sql != sql:
                log.info(
                    "sql_rewritten",
                    attempt=attempt,
                    original=sanitize_sql(sql),
                    rewritten=sanitize_sql(rewritten_sql),
                )
                sql = rewritten_sql
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
            if not val_result.valid:
                log.warning(
                    "sql_validation_failed",
                    attempt=attempt,
                    reason=val_result.reason,
                    sql=sanitize_sql(sql),
                )
                if attempt < self._settings.max_retries:
                    feedback = _build_validator_feedback(val_result.reason or "")
                    continue
                if val_result.code == "E_SQL_PARSE":
                    raise SqlParseError(val_result.reason or "SQL 解析失败")
                raise SqlUnsafeError(val_result.reason or "SQL 安全检查未通过")

            is_explain = val_result.is_explain

            # 9. return_type=sql: return generated SQL without executing
            if request.return_type == "sql":
                return QueryResponse(
                    request_id=request_id,
                    database=database,
                    sql=sql,
                    schema_loaded_at=schema_loaded_at,
                )

            # 10. SQL execution (in retry loop so undefined column / table
            # errors can feed back to the LLM for correction).
            exec_start = time.monotonic()
            try:
                exec_result = await self._sql_exec.execute(
                    database, sql, schema_names=schema_names, is_explain=is_explain
                )
                break  # success
            except SqlTimeoutError:
                raise
            except SqlExecuteError as e:
                exec_feedback = _build_execute_feedback(e, schema=schema, sql=sql)
                log.warning(
                    "sql_execute_failed",
                    attempt=attempt,
                    sqlstate=e.sqlstate,
                    reason=str(e),
                    sql=sanitize_sql(sql),
                    recoverable=exec_feedback is not None,
                )
                if exec_feedback is None or attempt >= self._settings.max_retries:
                    raise
                feedback = exec_feedback
                continue
        else:
            # All retries exhausted without valid SQL
            raise SqlGenerateError("多次重试后仍未能生成有效的 SQL")

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
                    sql = self._sql_rewriter.rewrite(sql)
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
                            val_result.reason or "修正后的 SQL 安全检查未通过"
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
                    except SqlExecuteError:
                        raise

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
                            "多次修正后结果验证仍无法通过"
                        )
                    elif verdict.verdict == "fail":
                        raise ValidationFailedError(
                            verdict.reason or "结果验证失败"
                        )
                    break
                else:
                    raise ValidationFailedError(
                        "结果验证修正循环已耗尽所有重试次数"
                    )

            elif verdict.verdict == "fail":
                raise ValidationFailedError(verdict.reason or "结果验证失败")

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
