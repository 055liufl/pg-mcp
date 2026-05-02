r"""Unit tests for the LLM SQL generator.

Covers the contract described in Impl-Plan §2.4:
- successful generation returns SqlGenerationResult with cleaned SQL + token usage
- ``asyncio.TimeoutError`` (raised by ``asyncio.wait_for``) maps to ``LlmTimeoutError``
- ``openai.APIError`` maps to ``LlmError``
- markdown code-block fences are stripped (``\`\`\`sql ... \`\`\``)
- ``feedback`` (retry) is injected into the prompt as ``Previous attempt feedback: ...``
- prompt assembly uses ``schema_context`` and ``query`` verbatim
- empty / null model output is handled gracefully (returns empty SQL)
- ``response.usage`` missing yields zeroed token counts
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import openai
import pytest

from pg_mcp.config import Settings
from pg_mcp.engine.sql_generator import SQL_GENERATION_PROMPT, SqlGenerator
from pg_mcp.models.errors import LlmError, LlmTimeoutError
from pg_mcp.protocols import SqlGenerationResult


def _make_settings(**overrides: object) -> Settings:
    base = {
        "pg_user": "test",
        "pg_password": "test",
        "openai_api_key": "dummy",
        "openai_model": "gpt-test",
        "openai_timeout": 5,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _make_response(
    content: str | None,
    *,
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
    usage_present: bool = True,
) -> Any:
    """Build a chat-completion response-shaped MagicMock."""
    msg = MagicMock(content=content)
    choice = MagicMock(message=msg)
    if usage_present:
        usage = MagicMock(prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)
    else:
        usage = None
    return MagicMock(choices=[choice], usage=usage)


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock()
    client.chat.completions.create = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_returns_sql_and_token_usage(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response(
        "SELECT * FROM users",
        prompt_tokens=120,
        completion_tokens=30,
    )
    gen = SqlGenerator(mock_client, _make_settings())

    result = await gen.generate("list users", "schema context")

    assert isinstance(result, SqlGenerationResult)
    assert result.sql == "SELECT * FROM users"
    assert result.prompt_tokens == 120
    assert result.completion_tokens == 30
    assert result.avg_logprob is None


# ---------------------------------------------------------------------------
# Markdown stripping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("```sql\nSELECT 1\n```", "SELECT 1"),
        ("```\nSELECT 2\n```", "SELECT 2"),
        ("```sql\nSELECT 3\n```\n", "SELECT 3"),
        ("  SELECT 4  ", "SELECT 4"),
        ("SELECT 5", "SELECT 5"),
    ],
    ids=["fenced_sql", "fenced_plain", "fenced_trailing_nl", "whitespace_only", "no_fence"],
)
async def test_generate_strips_markdown_fences(
    mock_client: AsyncMock, raw: str, expected: str
) -> None:
    mock_client.chat.completions.create.return_value = _make_response(raw)
    gen = SqlGenerator(mock_client, _make_settings())

    result = await gen.generate("q", "ctx")
    assert result.sql == expected


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_timeout_maps_to_llm_timeout_error(mock_client: AsyncMock) -> None:
    async def slow(*_: Any, **__: Any) -> Any:
        await asyncio.sleep(10)

    mock_client.chat.completions.create.side_effect = slow

    gen = SqlGenerator(mock_client, _make_settings(openai_timeout=1))

    # asyncio.wait_for(timeout=1) → asyncio.TimeoutError → LlmTimeoutError
    with pytest.raises(LlmTimeoutError):
        await gen.generate("q", "ctx")


@pytest.mark.asyncio
async def test_generate_api_error_maps_to_llm_error(mock_client: AsyncMock) -> None:
    api_err = openai.APIError(
        message="upstream 500",
        request=MagicMock(),
        body=None,
    )
    mock_client.chat.completions.create.side_effect = api_err

    gen = SqlGenerator(mock_client, _make_settings())

    with pytest.raises(LlmError) as exc_info:
        await gen.generate("q", "ctx")
    assert "upstream 500" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Prompt construction (feedback / schema_context propagation)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_injects_feedback_into_prompt(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response("SELECT 1")
    gen = SqlGenerator(mock_client, _make_settings())

    await gen.generate(
        "list users",
        "TABLE users(id, name)",
        feedback="rejected because pg_sleep is forbidden",
    )

    call = mock_client.chat.completions.create.call_args
    user_msg = call.kwargs["messages"][1]["content"]
    assert "list users" in user_msg
    assert "TABLE users(id, name)" in user_msg
    assert "Previous attempt feedback: rejected because pg_sleep is forbidden" in user_msg


@pytest.mark.asyncio
async def test_generate_no_feedback_keeps_clean_prompt(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response("SELECT 1")
    gen = SqlGenerator(mock_client, _make_settings())

    await gen.generate("q", "ctx")

    user_msg = mock_client.chat.completions.create.call_args.kwargs["messages"][1]["content"]
    assert "Previous attempt feedback" not in user_msg


@pytest.mark.asyncio
async def test_generate_uses_configured_model(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response("SELECT 1")
    gen = SqlGenerator(mock_client, _make_settings(openai_model="custom-model-id"))

    await gen.generate("q", "ctx")

    assert mock_client.chat.completions.create.call_args.kwargs["model"] == "custom-model-id"


# ---------------------------------------------------------------------------
# Edge cases on response shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_handles_null_content(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response(None)
    gen = SqlGenerator(mock_client, _make_settings())

    result = await gen.generate("q", "ctx")
    assert result.sql == ""


@pytest.mark.asyncio
async def test_generate_handles_missing_usage(mock_client: AsyncMock) -> None:
    mock_client.chat.completions.create.return_value = _make_response(
        "SELECT 1",
        usage_present=False,
    )
    gen = SqlGenerator(mock_client, _make_settings())

    result = await gen.generate("q", "ctx")
    assert result.prompt_tokens == 0
    assert result.completion_tokens == 0


# ---------------------------------------------------------------------------
# Prompt template constant: keeps the public contract stable
# ---------------------------------------------------------------------------


def test_prompt_template_contains_required_placeholders() -> None:
    assert "{schema_context}" in SQL_GENERATION_PROMPT
    assert "{query}" in SQL_GENERATION_PROMPT
    assert "{feedback}" in SQL_GENERATION_PROMPT
    # safety hints
    assert "SELECT" in SQL_GENERATION_PROMPT
    assert "Do not use any functions that modify data" in SQL_GENERATION_PROMPT
    # metadata-query routing (use information_schema / pg_catalog, not user-table JOINs)
    assert "information_schema" in SQL_GENERATION_PROMPT
    assert "pg_catalog" in SQL_GENERATION_PROMPT
