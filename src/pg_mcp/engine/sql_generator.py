"""LLM SQL generation with OpenAI async client."""

from __future__ import annotations

import asyncio

import openai
from openai import AsyncOpenAI

from pg_mcp.config import Settings
from pg_mcp.models.errors import LlmError, LlmTimeoutError
from pg_mcp.protocols import SqlGenerationResult

SQL_GENERATION_PROMPT = (
    "You are a PostgreSQL SQL expert. Given the database schema below, "
    "generate a SQL query to answer the user's question.\n"
    "\n"
    "Database Schema:\n"
    "{schema_context}\n"
    "\n"
    "User Question: {query}\n"
    "\n"
    "Requirements:\n"
    "- Generate only SELECT queries (or WITH ... SELECT)\n"
    "- Do not use any functions that modify data\n"
    "- Ensure the query is syntactically correct PostgreSQL\n"
    "- Use appropriate JOINs when multiple tables are needed\n"
    "- Add LIMIT if the user asks for a limited number of results\n"
    "\n"
    "PostgreSQL dialect constraints — use **only** PostgreSQL functions. "
    "Do NOT use BigQuery / MySQL / SQL Server / Snowflake function names. "
    "The following functions DO NOT EXIST in PostgreSQL — never emit them:\n"
    "- `timestamp_trunc`, `datetime_trunc`, `time_trunc`, `timestamptz_trunc` →\n"
    "  use `date_trunc(unit, ts)` for ALL timestamp/date types.\n"
    "- `safe_cast`, `try_cast` → use `CAST(value AS type)` or `value::type`.\n"
    "- `datetime_part`, `timestamp_part`, `datetime_diff`, `timestamp_diff`,\n"
    "  `date_diff` → use `EXTRACT(field FROM ts)` or `(a - b)` arithmetic.\n"
    "- `date_add`, `dateadd`, `timestampadd` → use `ts + INTERVAL 'N units'`.\n"
    "- `concat_ws` works (PostgreSQL has it), but prefer `||` for plain\n"
    "  concatenation. Use `||` (NOT `+`) to concatenate strings.\n"
    "- Composite type field access: if a column's type is a PostgreSQL composite\n"
    "  type (e.g. `postal_address`), access its fields with parentheses:\n"
    "  `(alias.column).field_name` — e.g. `(a.address).city`.\n"
    "\n"
    "If unsure about a function name, prefer SQL standard keywords (CASE,\n"
    "COALESCE, NULLIF, GREATEST, LEAST) or stick to functions present in\n"
    "PostgreSQL's `pg_proc`.\n"
    "\n"
    "{feedback}\n"
    "\n"
    "Respond with ONLY the SQL query, no explanations."
)


class SqlGenerator:
    """Generates SQL queries from natural language using an LLM.

    Uses the OpenAI async client with configurable timeout and model.
    Cleans markdown code blocks from the response.
    """

    def __init__(self, client: AsyncOpenAI, settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._model = settings.openai_model

    async def generate(
        self,
        query: str,
        schema_context: str,
        feedback: str | None = None,
    ) -> SqlGenerationResult:
        """Generate a SQL query from a natural language question.

        Args:
            query: The user's natural language query.
            schema_context: Textual representation of the relevant database schema.
            feedback: Optional feedback from a previous failed attempt.

        Returns:
            SqlGenerationResult containing the generated SQL and token usage.

        Raises:
            LlmTimeoutError: If the LLM call exceeds the configured timeout.
            LlmError: If the LLM API returns an error.
        """
        feedback_text = f"\nPrevious attempt feedback: {feedback}" if feedback else ""
        prompt = SQL_GENERATION_PROMPT.format(
            schema_context=schema_context,
            query=query,
            feedback=feedback_text,
        )

        try:
            response = await asyncio.wait_for(
                self._client.chat.completions.create(
                    model=self._model,
                    messages=[
                        {"role": "system", "content": "You generate PostgreSQL SQL queries."},
                        {"role": "user", "content": prompt},
                    ],
                ),
                timeout=self._settings.openai_timeout,
            )
        except TimeoutError as e:
            raise LlmTimeoutError("SQL 生成 LLM 调用超时") from e
        except openai.APIError as e:
            raise LlmError(f"SQL 生成 LLM 调用失败: {e}") from e

        raw_sql = response.choices[0].message.content or ""
        sql = self._clean_sql(raw_sql)

        usage = response.usage
        return SqlGenerationResult(
            sql=sql,
            prompt_tokens=usage.prompt_tokens if usage else 0,
            completion_tokens=usage.completion_tokens if usage else 0,
            avg_logprob=None,
        )

    def _clean_sql(self, sql: str) -> str:
        """Remove markdown code block markers and excess whitespace."""
        sql = sql.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        return sql.strip()
