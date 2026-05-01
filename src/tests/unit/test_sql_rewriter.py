"""Unit tests for the SQL rewriter middleware."""

from __future__ import annotations

import pytest

from pg_mcp.engine.sql_rewriter import SqlRewriter


@pytest.fixture
def rewriter() -> SqlRewriter:
    return SqlRewriter()


class TestCrossDialectFunctionRewriting:
    """sqlglot's transpile path: BigQuery/MySQL/Snowflake → PostgreSQL."""

    def test_timestamp_trunc_rewritten_to_date_trunc(
        self, rewriter: SqlRewriter
    ) -> None:
        out = rewriter.rewrite("SELECT timestamp_trunc(ts, MONTH) FROM t")

        assert "DATE_TRUNC" in out.upper()
        assert "TIMESTAMP_TRUNC" not in out.upper()

    def test_timestamp_trunc_arg_order_swapped(
        self, rewriter: SqlRewriter
    ) -> None:
        # BigQuery: timestamp_trunc(ts, MONTH)
        # PostgreSQL: date_trunc('MONTH', ts)
        out = rewriter.rewrite(
            "SELECT timestamp_trunc(placed_at, MONTH) FROM sales.orders"
        )

        # Unit literal should appear before the column reference.
        unit_pos = out.upper().find("'MONTH'")
        col_pos = out.find("placed_at")
        assert unit_pos != -1 and col_pos != -1
        assert unit_pos < col_pos

    def test_datetime_trunc_rewritten(self, rewriter: SqlRewriter) -> None:
        out = rewriter.rewrite("SELECT datetime_trunc(ts, DAY) FROM t")

        assert "DATE_TRUNC" in out.upper()
        assert "DATETIME_TRUNC" not in out.upper()

    def test_datetime_trunc_arg_order_swapped(
        self, rewriter: SqlRewriter
    ) -> None:
        out = rewriter.rewrite("SELECT datetime_trunc(occurred_at, WEEK) FROM t")

        unit_pos = out.upper().find("'WEEK'")
        col_pos = out.find("occurred_at")
        assert unit_pos < col_pos

    def test_safe_cast_rewritten_to_cast(self, rewriter: SqlRewriter) -> None:
        out = rewriter.rewrite("SELECT safe_cast(x AS INT) FROM t")

        assert "CAST" in out.upper()
        assert "SAFE_CAST" not in out.upper()

    def test_try_cast_rewritten_to_cast(self, rewriter: SqlRewriter) -> None:
        out = rewriter.rewrite("SELECT try_cast(x AS INT) FROM t")

        assert "CAST" in out.upper()
        assert "TRY_CAST" not in out.upper()

    def test_date_add_rewritten_to_interval_arithmetic(
        self, rewriter: SqlRewriter
    ) -> None:
        out = rewriter.rewrite("SELECT date_add(d, INTERVAL 7 DAY) FROM t")

        assert "INTERVAL" in out.upper()
        assert "DATE_ADD" not in out.upper()


class TestPostgreSQLPassthrough:
    """Already-canonical PostgreSQL must not be broken by the rewriter."""

    def test_date_trunc_unchanged_semantically(
        self, rewriter: SqlRewriter
    ) -> None:
        out = rewriter.rewrite("SELECT date_trunc('month', ts) FROM t")

        # Output must still be a valid date_trunc call.
        assert "DATE_TRUNC" in out.upper()
        assert "'MONTH'" in out.upper() or "'month'" in out.lower()

    def test_simple_select_unchanged(self, rewriter: SqlRewriter) -> None:
        out = rewriter.rewrite("SELECT * FROM users WHERE id = 1")

        assert out == "SELECT * FROM users WHERE id = 1"

    def test_cte_with_window_unchanged(self, rewriter: SqlRewriter) -> None:
        sql = (
            "WITH ranked AS ("
            "SELECT id, ROW_NUMBER() OVER (PARTITION BY tag ORDER BY views DESC) rn"
            " FROM posts"
            ") SELECT * FROM ranked WHERE rn = 1"
        )
        out = rewriter.rewrite(sql)

        # Semantic check: keywords still present.
        assert "ROW_NUMBER" in out.upper()
        assert "PARTITION BY" in out.upper()


class TestParseFailureFallback:
    """Returns input unchanged when sqlglot can't parse it."""

    def test_garbage_input_returns_unchanged(
        self, rewriter: SqlRewriter
    ) -> None:
        # Use input sqlglot definitively can't parse: unbalanced quotes.
        sql = "SELECT * FROM users WHERE name = 'unterminated"

        out = rewriter.rewrite(sql)

        assert out == sql

    def test_empty_string_returns_empty(self, rewriter: SqlRewriter) -> None:
        out = rewriter.rewrite("")

        assert out == ""


class TestAnonymousFunctionRewrites:
    """Manual rewrites for function names sqlglot doesn't classify."""

    def test_datetime_part_rewritten_to_date_part(
        self, rewriter: SqlRewriter
    ) -> None:
        out = rewriter.rewrite("SELECT datetime_part('year', ts) FROM t")

        assert "DATE_PART" in out.upper()
        assert "DATETIME_PART" not in out.upper()
