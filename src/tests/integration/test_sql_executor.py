"""Integration tests for SQL execution.

Covers:
- LIMIT wrapping for regular queries
- EXPLAIN exemption from LIMIT wrapping
- Read-only transaction
- Result processing (empty, truncation, type conversion)
- Size limit enforcement
- Error handling (timeout, postgres errors)

Note: These tests mock asyncpg to avoid requiring a real PostgreSQL instance.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from pg_mcp.config import Settings

pytestmark = pytest.mark.integration
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.sql_executor import SqlExecutor, _convert_value, _quote_ident
from pg_mcp.models.errors import ResultTooLargeError, SqlExecuteError, SqlTimeoutError


@pytest.fixture
def settings() -> Settings:
    return Settings(
        pg_user="test",
        pg_password="test",
        max_rows=100,
        max_cell_bytes=4096,
        max_result_bytes=1024,
        max_result_bytes_hard=2048,
        query_timeout=30,
        idle_in_transaction_session_timeout=60,
        session_work_mem="64MB",
        session_temp_file_limit="256MB",
    )


@pytest.fixture
def executor(settings: Settings) -> SqlExecutor:
    pool_mgr = ConnectionPoolManager(settings)
    return SqlExecutor(pool_mgr, settings)


def _make_mock_pool(records: Optional[list[dict]] = None) -> MagicMock:
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value="SET")

    # Create mock records
    mock_rows: list[MagicMock] = []
    if records:
        for rec in records:
            mock_row = MagicMock()
            mock_row.keys = lambda rec=rec: list(rec.keys())  # type: ignore[misc]
            mock_row.__getitem__ = lambda self, k, rec=rec: rec[k]  # type: ignore[misc]
            for k, v in rec.items():
                setattr(mock_row, k, v)
            mock_rows.append(mock_row)

    mock_conn.fetch = AsyncMock(return_value=mock_rows)
    mock_conn.transaction = MagicMock()
    mock_transaction = MagicMock()
    mock_transaction.__aenter__ = AsyncMock(return_value=None)
    mock_transaction.__aexit__ = AsyncMock(return_value=False)
    mock_conn.transaction.return_value = mock_transaction

    mock_pool.acquire = MagicMock()
    mock_pool.acquire.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.__aexit__ = AsyncMock(return_value=False)

    return mock_pool


class TestLimitWrapping:
    """Tests for outer LIMIT wrapping."""

    def test_apply_limit_wraps_regular_query(self, executor: SqlExecutor) -> None:
        sql = "SELECT * FROM users"
        result = executor._apply_limit(sql)

        assert result.startswith("SELECT * FROM (")
        assert "LIMIT" in result
        assert "__pg_mcp_q" in result

    def test_apply_limit_strips_trailing_semicolon(self, executor: SqlExecutor) -> None:
        sql = "SELECT * FROM users;"
        result = executor._apply_limit(sql)

        assert ";" not in result
        assert result.endswith("LIMIT 101")

    def test_apply_limit_exempts_explain(self, executor: SqlExecutor) -> None:
        sql = "EXPLAIN SELECT * FROM users"
        result = executor._apply_limit(sql, is_explain=True)

        assert result == "EXPLAIN SELECT * FROM users"
        assert "LIMIT" not in result

    def test_apply_limit_exempts_explain_with_semicolon(self, executor: SqlExecutor) -> None:
        sql = "EXPLAIN SELECT * FROM users;"
        result = executor._apply_limit(sql, is_explain=True)

        assert result == "EXPLAIN SELECT * FROM users"

    def test_apply_limit_uses_max_rows_plus_one(self, executor: SqlExecutor) -> None:
        sql = "SELECT * FROM users"
        result = executor._apply_limit(sql)

        assert "LIMIT 101" in result  # max_rows=100 + 1


class TestExecute:
    """Tests for the execute method."""

    @pytest.mark.asyncio
    async def test_execute_returns_results(self, executor: SqlExecutor) -> None:
        mock_pool = _make_mock_pool([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            result = await executor.execute("test_db", "SELECT * FROM users")

            assert result.row_count == 2
            assert result.columns == ["id", "name"]
            assert result.rows == [[1, "Alice"], [2, "Bob"]]

    @pytest.mark.asyncio
    async def test_execute_sets_session_params(self, executor: SqlExecutor) -> None:
        mock_pool = _make_mock_pool([{"id": 1}])

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            await executor.execute("test_db", "SELECT * FROM users")

            mock_conn = mock_pool.acquire.__aenter__.return_value
            execute_calls = [call.args[0] for call in mock_conn.execute.call_args_list]
            assert any("statement_timeout" in str(c) for c in execute_calls)
            assert any("work_mem" in str(c) for c in execute_calls)
            assert any("temp_file_limit" in str(c) for c in execute_calls)

    @pytest.mark.asyncio
    async def test_execute_readonly_transaction(self, executor: SqlExecutor) -> None:
        mock_pool = _make_mock_pool([{"id": 1}])

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            await executor.execute("test_db", "SELECT * FROM users")

            mock_conn = mock_pool.acquire.__aenter__.return_value
            mock_conn.transaction.assert_called_once_with(readonly=True)

    @pytest.mark.asyncio
    async def test_execute_empty_result(self, executor: SqlExecutor) -> None:
        mock_pool = _make_mock_pool([])

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            result = await executor.execute("test_db", "SELECT * FROM users")

            assert result.row_count == 0
            assert result.rows == []
            assert result.columns == []

    @pytest.mark.asyncio
    async def test_execute_timeout_raises_sql_timeout(self, executor: SqlExecutor) -> None:
        import asyncpg

        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="SET")
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.QueryCanceledError("timeout"))
        mock_conn.transaction = MagicMock()
        mock_transaction = MagicMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=None)
        mock_transaction.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction.return_value = mock_transaction
        mock_pool.acquire = MagicMock()
        mock_pool.acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.__aexit__ = AsyncMock(return_value=False)

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            with pytest.raises(SqlTimeoutError) as exc_info:
                await executor.execute("test_db", "SELECT * FROM users")

            assert "timeout" in str(exc_info.value).lower() or "30" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_postgres_error_raises_sql_execute(
        self, executor: SqlExecutor
    ) -> None:
        import asyncpg

        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="SET")
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("syntax error"))
        mock_conn.transaction = MagicMock()
        mock_transaction = MagicMock()
        mock_transaction.__aenter__ = AsyncMock(return_value=None)
        mock_transaction.__aexit__ = AsyncMock(return_value=False)
        mock_conn.transaction.return_value = mock_transaction
        mock_pool.acquire = MagicMock()
        mock_pool.acquire.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.__aexit__ = AsyncMock(return_value=False)

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            with pytest.raises(SqlExecuteError) as exc_info:
                await executor.execute("test_db", "SELECT * FROM users")

            assert "syntax error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_with_schema_names(self, executor: SqlExecutor) -> None:
        mock_pool = _make_mock_pool([{"id": 1}])

        with patch.object(
            executor._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            await executor.execute(
                "test_db", "SELECT * FROM users", schema_names=["public", "app"]
            )

            mock_conn = mock_pool.acquire.__aenter__.return_value
            execute_calls = [str(call.args[0]) for call in mock_conn.execute.call_args_list]
            assert any("search_path" in c for c in execute_calls)


class TestResultProcessing:
    """Tests for result post-processing."""

    def test_process_result_truncates_on_row_limit(self, executor: SqlExecutor) -> None:
        # Simulate max_rows=100, returning 101 rows
        records = [{"id": i} for i in range(101)]
        mock_rows: list[MagicMock] = []
        for rec in records:
            mock_row = MagicMock()
            mock_row.keys = lambda rec=rec: list(rec.keys())  # type: ignore[misc]
            mock_row.__getitem__ = lambda self, k, rec=rec: rec[k]  # type: ignore[misc]
            for k, v in rec.items():
                setattr(mock_row, k, v)
            mock_rows.append(mock_row)

        result = executor._process_result(mock_rows)

        assert result.truncated is True
        assert result.row_count == 100
        assert "limited to 100 rows" in (result.truncated_reason or "")

    def test_process_result_truncates_on_soft_size_limit(
        self, executor: SqlExecutor
    ) -> None:
        # Create rows that exceed max_result_bytes (1024)
        big_value = "x" * 600
        records = [
            {"id": 1, "data": big_value},
            {"id": 2, "data": big_value},
        ]
        mock_rows: list[MagicMock] = []
        for rec in records:
            mock_row = MagicMock()
            mock_row.keys = lambda rec=rec: list(rec.keys())  # type: ignore[misc]
            mock_row.__getitem__ = lambda self, k, rec=rec: rec[k]  # type: ignore[misc]
            for k, v in rec.items():
                setattr(mock_row, k, v)
            mock_rows.append(mock_row)

        result = executor._process_result(mock_rows)

        assert result.truncated is True
        assert "soft limit" in (result.truncated_reason or "").lower()

    def test_process_result_raises_on_hard_size_limit(self, executor: SqlExecutor) -> None:
        # Create rows that exceed max_result_bytes_hard (2048)
        huge_value = "x" * 1500
        records = [
            {"id": 1, "data": huge_value},
            {"id": 2, "data": huge_value},
        ]
        mock_rows: list[MagicMock] = []
        for rec in records:
            mock_row = MagicMock()
            mock_row.keys = lambda rec=rec: list(rec.keys())  # type: ignore[misc]
            mock_row.__getitem__ = lambda self, k, rec=rec: rec[k]  # type: ignore[misc]
            for k, v in rec.items():
                setattr(mock_row, k, v)
            mock_rows.append(mock_row)

        with pytest.raises(ResultTooLargeError) as exc_info:
            executor._process_result(mock_rows)

        assert "hard limit" in str(exc_info.value).lower()

    def test_process_result_truncates_large_cells(self, executor: SqlExecutor) -> None:
        big_value = "x" * 5000  # Exceeds max_cell_bytes=4096
        records = [{"id": 1, "data": big_value}]
        mock_rows: list[MagicMock] = []
        for rec in records:
            mock_row = MagicMock()
            mock_row.keys = lambda rec=rec: list(rec.keys())  # type: ignore[misc]
            mock_row.__getitem__ = lambda self, k, rec=rec: rec[k]  # type: ignore[misc]
            for k, v in rec.items():
                setattr(mock_row, k, v)
            mock_rows.append(mock_row)

        result = executor._process_result(mock_rows)

        cell = result.rows[0][1]
        assert "truncated" in cell


class TestConvertValue:
    """Tests for value type conversion."""

    def test_convert_decimal_to_float(self) -> None:
        result = _convert_value(Decimal("3.14"))

        assert result == 3.14
        assert isinstance(result, float)

    def test_convert_uuid_to_string(self) -> None:
        uuid_val = UUID("12345678-1234-5678-1234-567812345678")
        result = _convert_value(uuid_val)

        assert result == str(uuid_val)
        assert isinstance(result, str)

    def test_convert_bytes_to_base64(self) -> None:
        result = _convert_value(b"hello")

        assert isinstance(result, str)
        assert result == "aGVsbG8="

    def test_convert_list_recursively(self) -> None:
        result = _convert_value([Decimal("1.5"), Decimal("2.5")])

        assert result == [1.5, 2.5]

    def test_convert_dict_recursively(self) -> None:
        result = _convert_value({"value": Decimal("1.5")})

        assert result == {"value": 1.5}

    def test_convert_datetime_to_iso(self) -> None:
        from datetime import datetime, timezone

        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = _convert_value(dt)

        assert result == dt.isoformat()

    def test_convert_plain_value_unchanged(self) -> None:
        result = _convert_value("hello")

        assert result == "hello"


class TestQuoteIdent:
    """Tests for identifier quoting."""

    def test_quote_ident_simple(self) -> None:
        result = _quote_ident("users")

        assert result == '"users"'

    def test_quote_ident_with_quotes(self) -> None:
        result = _quote_ident('user"s')

        assert result == '"user""s"'

    def test_quote_ident_empty(self) -> None:
        result = _quote_ident("")

        assert result == '""'
