"""AI result validation with deny_list and policy handling."""

from __future__ import annotations

import asyncio
import json
import re

import openai
from openai import AsyncOpenAI

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


def _mask_pii(value: str) -> str:
    """Mask PII in a string value using simple regex rules."""
    value = _EMAIL_RE.sub("***@***.***", value)
    value = _PHONE_RE.sub("***PHONE***", value)
    value = _ID_CARD_RE.sub("***ID***", value)
    return value


def _mask_row(row: list, columns: list[str]) -> list:
    """Mask sensitive columns in a result row."""
    masked: list = []
    for val, col in zip(row, columns):
        col_lower = col.lower()
        if any(s in col_lower for s in _SENSITIVE_COL_NAMES):
            masked.append("***")
        elif isinstance(val, str):
            masked.append(_mask_pii(val))
        else:
            masked.append(val)
    return masked


class ResultValidator:
    """Validates SQL execution results using an LLM.

    Supports configurable data policies (metadata_only, masked, full) and
    deny_list filtering at the database level.
    """

    def __init__(self, client: AsyncOpenAI, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._model = settings.openai_model

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

    def _is_denied(self, database: str) -> bool:
        """Check if the database is in the validation deny_list.

        Supports database-level matching and wildcard '*'.
        """
        deny_list = self._settings.validation_deny_list_items
        if not deny_list:
            return False
        db_lower = database.lower()
        for rule in deny_list:
            rule_lower = rule.lower()
            if rule_lower == "*" or rule_lower == db_lower:
                return True
        return False

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

        denied = self._is_denied(schema.database)
        policy = (
            ValidationDataPolicy.metadata_only
            if denied
            else self._settings.validation_data_policy
        )

        if policy == ValidationDataPolicy.full and result.rows:
            sample = result.rows[: self._settings.validation_sample_rows]
            parts.append(f"Sample rows:\n{json.dumps(sample, ensure_ascii=False)}")
        elif policy == ValidationDataPolicy.masked and result.rows:
            sample = [
                _mask_row(row, result.columns)
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
                    temperature=0,
                    response_format={"type": "json_object"},
                ),
                timeout=self._settings.openai_timeout,
            )
        except asyncio.TimeoutError:
            raise LlmTimeoutError("Result validation LLM call timed out")
        except openai.APIError as e:
            raise LlmError(f"Result validation LLM call failed: {e}")

        content = response.choices[0].message.content or "{}"
        return ValidationVerdict.model_validate_json(content)
