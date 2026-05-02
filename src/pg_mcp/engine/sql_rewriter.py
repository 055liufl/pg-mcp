"""SQL rewriter: transpile cross-dialect functions into PostgreSQL form.

The LLM (especially smaller models) sometimes hallucinates BigQuery / MySQL
/ Snowflake-style function names (``timestamp_trunc``, ``safe_cast``,
``date_add``, etc.). The validator's pg_proc-derived allowlist correctly
rejects them, but feedback-driven retry doesn't always converge before
``max_retries`` is exhausted.

This module rewrites the generated SQL **before** validation, leveraging
sqlglot's built-in cross-dialect transpilation. It is a strict
post-generation pre-validation step in the orchestrator pipeline.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

# Manual rewrites for AST nodes that sqlglot recognizes but does not
# transpile to PostgreSQL syntax automatically. We replace the node with
# the canonical PG-renderable equivalent (``exp.TimestampTrunc``, which
# sqlglot prints as ``DATE_TRUNC('UNIT', ts)`` in postgres dialect).
#
# Mapping is keyed by ``type(node).__name__``. The value is the sqlglot
# class to construct in its place; field names ``this`` and ``unit`` are
# preserved.
_AST_NODE_REPLACE: dict[str, type[exp.Func]] = {
    "DatetimeTrunc": exp.TimestampTrunc,
    "TimeTrunc": exp.TimestampTrunc,
}

# Manual rewrites for ``Anonymous`` function calls (function names sqlglot
# couldn't classify). Same key/value semantics — keys are case-insensitive.
_ANONYMOUS_RENAMES: dict[str, str] = {
    "datetime_trunc": "date_trunc",
    "time_trunc": "date_trunc",
    "timestamptz_trunc": "date_trunc",
    "datetime_part": "date_part",
    "timestamp_part": "date_part",
}


class SqlRewriter:
    """Rewrite cross-dialect function calls in LLM-generated SQL.

    Strategy:
    1. Parse the SQL with the postgres dialect. sqlglot already transpiles
       many cross-dialect nodes during ``.sql(dialect="postgres")`` —
       e.g. ``timestamp_trunc(ts, MONTH)`` becomes ``DATE_TRUNC('MONTH', ts)``,
       ``safe_cast(x AS INT)`` becomes ``CAST(x AS INT)``,
       ``date_add(d, INTERVAL 7 DAY)`` becomes ``d + INTERVAL '7 DAY'``.
    2. For nodes sqlglot recognizes but doesn't transpile (e.g. ``DatetimeTrunc``),
       rewrite the node name in place.
    3. For ``Anonymous`` function calls (unknown to sqlglot), rename them
       via a manual map.
    4. If parsing fails, return the original SQL unchanged so the validator
       can produce a clear error.
    """

    def rewrite(self, sql: str) -> str:
        """Return SQL with cross-dialect functions rewritten."""
        try:
            statements = sqlglot.parse(sql, dialect="postgres")
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
            return sql

        rendered: list[str] = []
        for stmt in statements:
            if stmt is None:
                continue
            self._apply_manual_rewrites(stmt)  # type: ignore[arg-type]
            rendered.append(stmt.sql(dialect="postgres"))

        if not rendered:
            return sql
        return ";\n".join(rendered)

    def _apply_manual_rewrites(self, ast: exp.Expression) -> None:
        """Rewrite AST nodes sqlglot doesn't transpile automatically."""
        # Replace known classified nodes (e.g. DatetimeTrunc → TimestampTrunc).
        for node in ast.find_all(exp.Func):
            replacement_cls = _AST_NODE_REPLACE.get(type(node).__name__)
            if replacement_cls is None:
                continue
            new_node = replacement_cls(
                this=node.args.get("this"),
                unit=node.args.get("unit"),
            )
            node.replace(new_node)

        # Rename Anonymous function calls (unknown to sqlglot).
        for node in ast.find_all(exp.Anonymous):
            if not isinstance(node.this, str):
                continue
            replacement = _ANONYMOUS_RENAMES.get(node.this.lower())
            if replacement is not None:
                node.set("this", replacement)
