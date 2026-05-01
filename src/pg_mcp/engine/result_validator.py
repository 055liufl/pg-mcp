"""AI result validation with deny_list and policy handling."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

import openai
import sqlglot
from openai import AsyncOpenAI
from sqlglot import exp

from pg_mcp.config import Settings, ValidationDataPolicy
from pg_mcp.models.errors import LlmError, LlmTimeoutError
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.protocols import ExecutionResult, SqlGenerationResult, ValidationVerdict

VALIDATION_PROMPT = """You are a SQL quality validator. Given a user's question, the generated SQL, and the query result metadata, evaluate whether the SQL correctly answers the question.

Respond with a JSON object in this exact format:
{
  "verdict": "pass" | "fix" | "fail",
  "reason": "explanation of the evaluation",
  "suggested_sql": "optional corrected SQL if verdict is fix"
}

Rules:
- "pass": The SQL correctly answers the user's question.
- "fix": The SQL is close but has issues that can be corrected; provide suggested_sql.
- "fail": The SQL is fundamentally wrong or unsafe and cannot be easily fixed.

Be concise. Focus on correctness, not style."""

# Simple PII detection patterns
_EMAIL_RE = re.compile(r"[\w.-]+@[\w.-]+\.\w+")
_PHONE_RE = re.compile(r"\b1[3-9]\d{9}\b")
_ID_CARD_RE = re.compile(r"\b\d{17}[\dXx]\b")

# Column names that trigger full-column masking
_SENSITIVE_COL_NAMES: frozenset[str] = frozenset({
    "password", "passwd", "pwd", "token", "secret", "ssn", "social_security",
    "credit_card", "cvv", "pin", "api_key", "apikey", "auth_token",
    "access_token", "refresh_token", "private_key", "salt", "hash",
})


@dataclass(frozen=True)
class _DenyRule:
    """Parsed hierarchical deny-list rule.

    Each segment is a lowercase string or ``"*"`` (matches anything).
    ``column`` is ``None`` when the rule denies the table or above
    (i.e. has no column-level component).
    """

    database: str
    schema: str
    table: str
    column: str | None

    @classmethod
    def parse(cls, raw: str) -> _DenyRule | None:
        """Parse a rule of the form ``db[.schema[.table[.column]]]``.

        Missing trailing segments default to ``"*"`` (table-or-above
        wildcard); a trailing column is captured as ``column``. Returns
        ``None`` for empty/whitespace-only rules.
        """
        text = raw.strip().lower()
        if not text:
            return None
        parts = text.split(".")
        if len(parts) > 4:
            # Reject pathological inputs (likely a typo)
            return None
        database = parts[0] or "*"
        schema = parts[1] if len(parts) >= 2 else "*"
        table = parts[2] if len(parts) >= 3 else "*"
        column = parts[3] if len(parts) >= 4 else None
        return cls(
            database=database,
            schema=schema,
            table=table,
            column=column,
        )

    @staticmethod
    def _seg_match(rule_seg: str, value: str) -> bool:
        return rule_seg == "*" or rule_seg == value.lower()

    def matches_database(self, database: str) -> bool:
        """Return True if this rule applies to ``database`` at all."""
        return self._seg_match(self.database, database)

    def matches_table(
        self, database: str, schema_name: str, table_name: str
    ) -> bool:
        """Return True if this rule denies (db, schema, table)."""
        return (
            self._seg_match(self.database, database)
            and self._seg_match(self.schema, schema_name)
            and self._seg_match(self.table, table_name)
        )

    def matches_column(
        self,
        database: str,
        schema_name: str,
        table_name: str,
        column_name: str,
    ) -> bool:
        """Return True if this rule denies the given column reference."""
        if self.column is None:
            return self.matches_table(database, schema_name, table_name)
        return (
            self.matches_table(database, schema_name, table_name)
            and self._seg_match(self.column, column_name)
        )


def _mask_pii(value: str) -> str:
    """Mask PII in a string value using simple regex rules."""
    value = _EMAIL_RE.sub("***@***.***", value)
    value = _PHONE_RE.sub("***PHONE***", value)
    value = _ID_CARD_RE.sub("***ID***", value)
    return value


def _mask_row(
    row: list,
    columns: list[str],
    extra_masked_cols: set[int] | None = None,
) -> list:
    """Mask sensitive columns in a result row.

    Args:
        row: Raw result row.
        columns: Column names matching ``row`` positionally.
        extra_masked_cols: Optional set of column indices that should be
            replaced with ``"***"`` regardless of column name (used for
            hierarchical deny-list matches).
    """
    masked: list = []
    for idx, (val, col) in enumerate(zip(row, columns)):
        col_lower = col.lower()
        if extra_masked_cols and idx in extra_masked_cols or any(s in col_lower for s in _SENSITIVE_COL_NAMES):
            masked.append("***")
        elif isinstance(val, str):
            masked.append(_mask_pii(val))
        else:
            masked.append(val)
    return masked


class ResultValidator:
    """Validates SQL execution results using an LLM.

    Supports configurable data policies (metadata_only, masked, full)
    and hierarchical deny-list filtering (``db.schema.table.column``).
    Deny rules cause matching columns to be masked in sample rows; if
    any rule matches a queried table at a coarser level, the policy is
    downgraded to ``metadata_only`` to avoid leaking sensitive data.
    """

    def __init__(self, client: AsyncOpenAI, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._model = settings.openai_model
        self._deny_rules: list[_DenyRule] = self._compile_rules(
            settings.validation_deny_list_items
        )

    @staticmethod
    def _compile_rules(raw_rules: list[str]) -> list[_DenyRule]:
        """Parse raw rule strings into :class:`_DenyRule` instances."""
        compiled: list[_DenyRule] = []
        for raw in raw_rules:
            rule = _DenyRule.parse(raw)
            if rule is not None:
                compiled.append(rule)
        return compiled

    def should_validate(
        self,
        database: str,
        sql: str,
        result: ExecutionResult,
        generation: SqlGenerationResult,
    ) -> bool:
        """Determine whether result validation should be triggered.

        Validation is triggered when:
        - enable_validation is True
        - The SQL is complex (JOINs, subqueries, window functions)
        - The result is empty
        - The generation logprob is below threshold (if provided)

        Args:
            database: Target database name.
            sql: The executed SQL.
            result: Execution result.
            generation: SQL generation metadata.

        Returns:
            True if validation should run.
        """
        if not self._settings.enable_validation:
            return False

        # Check complexity
        sql_upper = sql.upper()
        join_count = sql_upper.count(" JOIN ")
        if join_count >= 2:
            return True
        if " OVER(" in sql_upper or "WINDOW " in sql_upper:
            return True
        if "(SELECT " in sql_upper:
            return True

        # Empty result
        if result.row_count == 0:
            return True

        # Low confidence (if logprob available)
        if (
            generation.avg_logprob is not None
            and generation.avg_logprob < self._settings.validation_confidence_threshold
        ):
            return True

        return False

    def _rules_for_database(self, database: str) -> list[_DenyRule]:
        """Return deny rules whose database segment matches ``database``."""
        return [
            r for r in self._deny_rules if r.matches_database(database)
        ]

    def _is_denied(self, database: str) -> bool:
        """Return True if any rule denies the entire database.

        A rule with ``schema``/``table`` wildcards (e.g. ``prod_db`` or
        ``prod_db.*.*``) covers the entire database and triggers a
        coarse-grained downgrade for every query against it.
        """
        for r in self._deny_rules:
            if (
                r.column is None
                and r.schema == "*"
                and r.table == "*"
                and r.matches_database(database)
            ):
                return True
        return False

    def _table_denied(
        self,
        rules: list[_DenyRule],
        database: str,
        schema_name: str,
        table_name: str,
    ) -> bool:
        """True if any rule denies (db, schema, table) at table-or-above level."""
        return any(
            r.column is None
            and r.matches_table(database, schema_name, table_name)
            for r in rules
        )

    def _column_denied_indices(
        self,
        rules: list[_DenyRule],
        database: str,
        sql: str,
        result: ExecutionResult,
        schema: DatabaseSchema,
    ) -> set[int]:
        """Return result-column indices to mask based on column-level rules.

        Resolves each result column to a candidate ``(schema, table)``
        pair using the SQL's FROM clause and the database schema, then
        checks whether any deny rule matches the resulting tuple.
        """
        column_rules = [r for r in rules if r.column is not None]
        if not column_rules:
            return set()

        candidates = self._resolve_query_tables(sql, schema)
        if not candidates:
            return set()

        masked: set[int] = set()
        for idx, col_name in enumerate(result.columns):
            for schema_name, table_name in candidates:
                if any(
                    r.matches_column(
                        database, schema_name, table_name, col_name
                    )
                    for r in column_rules
                ):
                    masked.add(idx)
                    break
        return masked

    @staticmethod
    def _resolve_query_tables(
        sql: str, schema: DatabaseSchema
    ) -> list[tuple[str, str]]:
        """Best-effort extraction of ``(schema, table)`` pairs from ``sql``.

        Falls back to the empty list if the SQL cannot be parsed. Tables
        without an explicit schema are resolved by looking them up in
        the loaded ``DatabaseSchema``; ambiguous unqualified names map
        to all matching schemas so deny rules conservatively apply.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="postgres")
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
            return []

        # Build name -> set of schemas lookup for unqualified resolution
        name_to_schemas: dict[str, set[str]] = {}
        for table in schema.tables:
            name_to_schemas.setdefault(table.table_name.lower(), set()).add(
                table.schema_name.lower()
            )
        for view in schema.views:
            name_to_schemas.setdefault(view.view_name.lower(), set()).add(
                view.schema_name.lower()
            )

        results: list[tuple[str, str]] = []
        for tbl in parsed.find_all(exp.Table):
            name = tbl.name.lower().strip('"')
            if tbl.db:
                results.append((tbl.db.lower().strip('"'), name))
            else:
                schemas = name_to_schemas.get(name)
                if schemas:
                    results.extend((s, name) for s in schemas)
                else:
                    results.append(("public", name))
        return results

    def _build_prompt(
        self,
        user_query: str,
        sql: str,
        result: ExecutionResult,
        schema: DatabaseSchema,
    ) -> str:
        """Build the validation prompt based on data policy."""
        parts = [
            f"User question: {user_query}",
            f"Generated SQL:\n```sql\n{sql}\n```",
            f"Result: {result.row_count} rows, columns: {result.columns}",
            f"Column types: {result.column_types}",
        ]

        rules = self._rules_for_database(schema.database)

        # Coarse-grained denial:
        # - A rule that targets the whole database (all wildcards below
        #   the database segment) immediately downgrades the policy.
        # - Otherwise, we only downgrade if the SQL actually touches a
        #   table that matches the rule.
        coarse_denied = False
        if rules:
            db_wide = [
                r
                for r in rules
                if r.column is None and r.schema == "*" and r.table == "*"
            ]
            if db_wide:
                coarse_denied = True
            else:
                for schema_name, table_name in self._resolve_query_tables(
                    sql, schema
                ):
                    if self._table_denied(
                        rules, schema.database, schema_name, table_name
                    ):
                        coarse_denied = True
                        break

        policy = (
            ValidationDataPolicy.METADATA_ONLY
            if coarse_denied
            else self._settings.validation_data_policy
        )

        # Column-level deny rules mask specific result columns.
        denied_col_indices = self._column_denied_indices(
            rules, schema.database, sql, result, schema
        )

        if policy == ValidationDataPolicy.FULL and result.rows:
            if denied_col_indices:
                # Mask only the denied columns; keep the rest as-is.
                sample = [
                    [
                        "***" if i in denied_col_indices else val
                        for i, val in enumerate(row)
                    ]
                    for row in result.rows[: self._settings.validation_sample_rows]
                ]
                parts.append(
                    f"Sample rows (deny-list applied):\n{json.dumps(sample, ensure_ascii=False)}"
                )
            else:
                sample = result.rows[: self._settings.validation_sample_rows]
                parts.append(
                    f"Sample rows:\n{json.dumps(sample, ensure_ascii=False)}"
                )
        elif policy == ValidationDataPolicy.MASKED and result.rows:
            sample = [
                _mask_row(row, result.columns, denied_col_indices)
                for row in result.rows[: self._settings.validation_sample_rows]
            ]
            parts.append(
                f"Sample rows (masked):\n{json.dumps(sample, ensure_ascii=False)}"
            )
        # metadata_only: do not append sample rows

        return "\n\n".join(parts)

    async def validate(
        self,
        user_query: str,
        sql: str,
        result: ExecutionResult,
        schema: DatabaseSchema,
    ) -> ValidationVerdict:
        """Validate a query result using an LLM.

        Args:
            user_query: Original natural language query.
            sql: The executed SQL.
            result: Execution result.
            schema: Database schema for context.

        Returns:
            ValidationVerdict with pass/fix/fail and optional corrected SQL.

        Raises:
            LlmTimeoutError: If the LLM call times out.
            LlmError: If the LLM API returns an error.
        """
        prompt = self._build_prompt(user_query, sql, result, schema)

        try:
            response = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": VALIDATION_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                ),
                timeout=self._settings.openai_timeout,
            )
        except TimeoutError:
            raise LlmTimeoutError("Result validation LLM call timed out")
        except openai.APIError as e:
            raise LlmError(f"Result validation LLM call failed: {e}")

        content = response.choices[0].message.content or "{}"
        return ValidationVerdict.model_validate_json(content)
