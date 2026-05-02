"""Read-only SQL execution with LIMIT wrapping and EXPLAIN exemption."""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import asyncpg

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.models.errors import ResultTooLargeError, SqlExecuteError, SqlTimeoutError
from pg_mcp.protocols import ExecutionResult


def _quote_ident(ident: str) -> str:
    """Quote a PostgreSQL identifier using double-quote rules."""
    return '"' + ident.replace('"', '""') + '"'


def _convert_value(value: object) -> object:
    """Convert an asyncpg-returned Python value to a JSON-serializable form."""
    # ``datetime`` is a subclass of ``date``; check it first so we keep the
    # full ISO-8601 timestamp instead of truncating to YYYY-MM-DD.
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, list):
        return [_convert_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    return value


class SqlExecutor:
    """Executes SQL queries in read-only mode with safety limits.

    Features:
    - Read-only transaction wrapper
    - Session-level timeouts and resource limits
    - Outer LIMIT wrapping (bypassed for EXPLAIN)
    - Cell-level and result-level size limits
    - JSON-serializable type conversion
    """

    def __init__(self, pool_mgr: ConnectionPoolManager, settings: Settings) -> None:
        self._pool_mgr = pool_mgr
        self._settings = settings

    async def execute(
        self,
        database: str,
        sql: str,
        schema_names: list[str] | None = None,
        is_explain: bool = False,
    ) -> ExecutionResult:
        """Execute a SQL query in read-only mode.

        Args:
            database: Target database name.
            sql: SQL query to execute.
            schema_names: Optional list of schema names to set as search_path.
            is_explain: If True, skip LIMIT wrapping (EXPLAIN statements).

        Returns:
            ExecutionResult with columns, rows, and metadata.

        Raises:
            SqlTimeoutError: If the query exceeds the configured timeout.
            SqlExecuteError: For other PostgreSQL execution errors.
            ResultTooLargeError: If the result exceeds the hard size limit.
        """
        pool = await self._pool_mgr.get_pool(database)
        async with pool.acquire() as conn:
            timeout_s = self._settings.query_timeout
            idle_timeout_s = self._settings.idle_in_transaction_session_timeout

            await conn.execute(f"SET statement_timeout = '{timeout_s}s'")
            await conn.execute(
                f"SET idle_in_transaction_session_timeout = '{idle_timeout_s}s'"
            )
            await conn.execute(f"SET work_mem = '{self._settings.session_work_mem}'")
            await conn.execute(
                f"SET temp_file_limit = '{self._settings.session_temp_file_limit}'"
            )
            await conn.execute("SET max_parallel_workers_per_gather = 2")

            limited_sql = self._apply_limit(sql, is_explain)

            try:
                async with conn.transaction(readonly=True):
                    if schema_names:
                        safe_schemas = ",".join(
                            _quote_ident(s) for s in schema_names
                        )
                        await conn.execute(
                            f"SET LOCAL search_path = {safe_schemas}"
                        )
                    rows = await conn.fetch(limited_sql)
            except asyncpg.QueryCanceledError:
                raise SqlTimeoutError(f"查询超时 ({timeout_s}s)")
            except asyncpg.PostgresError as e:
                raise SqlExecuteError(
                    str(e), sqlstate=getattr(e, "sqlstate", None)
                )

        return self._process_result(rows)

    def _apply_limit(self, sql: str, is_explain: bool = False) -> str:
        """Wrap SQL with an outer LIMIT to prevent unbounded result sets.

        EXPLAIN statements are exempted from wrapping.
        """
        if is_explain:
            return sql.strip().rstrip(";")
        stripped = sql.strip().rstrip(";")
        limit = self._settings.max_rows + 1
        return f"SELECT * FROM ({stripped}) AS __pg_mcp_q LIMIT {limit}"

    def _process_result(self, records: list[asyncpg.Record]) -> ExecutionResult:
        """Convert asyncpg records to an ExecutionResult with size limits."""
        if not records:
            return ExecutionResult(
                columns=[],
                column_types=[],
                rows=[],
                row_count=0,
            )

        columns = list(records[0].keys())
        column_types: list[str] = []

        # Best-effort type name extraction
        for col in columns:
            val = records[0][col]
            column_types.append(type(val).__name__)

        max_cell = self._settings.max_cell_bytes
        max_result = self._settings.max_result_bytes
        max_result_hard = self._settings.max_result_bytes_hard

        processed_rows: list[list] = []
        truncated = False
        truncated_reason: str | None = None
        total_bytes = 0

        for record in records:
            row: list = []
            for col in columns:
                val = _convert_value(record[col])
                if isinstance(val, str) and len(val.encode("utf-8")) > max_cell:
                    val = val[:max_cell] + "... [已截断]"
                row.append(val)

            row_bytes = len(json.dumps(row, ensure_ascii=False).encode("utf-8"))
            total_bytes += row_bytes

            if total_bytes > max_result_hard:
                raise ResultTooLargeError(
                    f"结果超出硬限制 {max_result_hard} 字节"
                )

            if total_bytes > max_result and not truncated:
                truncated = True
                truncated_reason = f"结果超出软限制 {max_result} 字节"
                # Still include this row; next rows will be skipped

            if not truncated or total_bytes <= max_result:
                processed_rows.append(row)

        row_count = len(processed_rows)
        # If we fetched max_rows + 1, we were truncated by the LIMIT wrapper
        if len(records) > self._settings.max_rows:
            truncated = True
            truncated_reason = f"结果已限制为 {self._settings.max_rows} 行"
            processed_rows = processed_rows[: self._settings.max_rows]
            row_count = len(processed_rows)

        return ExecutionResult(
            columns=columns,
            column_types=column_types,
            rows=processed_rows,
            row_count=row_count,
            truncated=truncated,
            truncated_reason=truncated_reason,
        )
