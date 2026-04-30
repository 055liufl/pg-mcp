"""Unit tests for result validator (engine/result_validator.py).

Tests cover trigger condition logic, deny_list filtering, policy application,
and verdict parsing using mocked LLM client.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from pg_mcp.engine.result_validator import ResultValidator
from pg_mcp.models.errors import LlmTimeoutError, LlmError
from pg_mcp.models.schema import (
    ColumnInfo,
    DatabaseSchema,
    TableInfo,
)
from pg_mcp.protocols import (
    SqlGenerationResult,
    ExecutionResult,
    ValidationVerdict,
)
from pg_mcp.config import Settings, ValidationDataPolicy


# =============================================================================
# Helpers
# =============================================================================

def _make_settings(
    enable_validation: bool = True,
    policy: ValidationDataPolicy = ValidationDataPolicy.metadata_only,
    deny_list: str = "",
    confidence_threshold: float = -1.0,
) -> Settings:
    """Build settings for result validator tests."""
    return Settings(
        pg_user="test",
        pg_password="test",  # type: ignore[arg-type]
        openai_api_key="sk-test",  # type: ignore[arg-type]
        enable_validation=enable_validation,
        validation_data_policy=policy,
        validation_deny_list=deny_list,
        validation_confidence_threshold=confidence_threshold,
        validation_sample_rows=10,
        openai_timeout=30,
    )


def _make_schema() -> DatabaseSchema:
    """Build a minimal DatabaseSchema."""
    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                    ColumnInfo(name="name", type="text", nullable=False),
                    ColumnInfo(name="email", type="text", nullable=True),
                ],
            ),
        ],
        views=[],
        indexes=[],
        foreign_keys=[],
        constraints=[],
        enum_types=[],
        composite_types=[],
        allowed_functions=set(),
        loaded_at=datetime.now(timezone.utc),
    )


def _make_execution_result(
    rows: list[list] | None = None,
    columns: list[str] | None = None,
) -> ExecutionResult:
    """Build an ExecutionResult."""
    cols = columns or ["id", "name"]
    return ExecutionResult(
        columns=cols,
        column_types=["integer", "text"],
        rows=rows or [[1, "Alice"], [2, "Bob"]],
        row_count=len(rows or [[1, "Alice"], [2, "Bob"]]),
    )


def _make_generation_result(avg_logprob: float | None = 0.0) -> SqlGenerationResult:
    """Build a SqlGenerationResult."""
    return SqlGenerationResult(
        sql="SELECT * FROM users",
        prompt_tokens=100,
        completion_tokens=50,
        avg_logprob=avg_logprob,
    )


# =============================================================================
# Trigger conditions
# =============================================================================

class TestShouldValidate:
    """Tests for should_validate trigger conditions."""

    def test_should_validate_disabled_globally_returns_false(self) -> None:
        """When enable_validation=False, should_validate always returns False."""
        settings = _make_settings(enable_validation=False)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result()

        should = validator.should_validate("test_db", "SELECT 1", result, generation)

        assert should is False

    def test_should_validate_enabled_no_complexity_returns_false(self) -> None:
        """Simple query with results and good logprob should not trigger."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result(avg_logprob=0.0)

        should = validator.should_validate("test_db", "SELECT * FROM users", result, generation)

        assert should is False

    def test_should_validate_empty_result_returns_true(self) -> None:
        """Empty result set should trigger validation."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(rows=[])
        generation = _make_generation_result()

        should = validator.should_validate("test_db", "SELECT * FROM users", result, generation)

        assert should is True

    def test_should_validate_multiple_joins_returns_true(self) -> None:
        """Query with 2+ JOINs should trigger validation."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result()
        sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id"

        should = validator.should_validate("test_db", sql, result, generation)

        assert should is True

    def test_should_validate_subquery_returns_true(self) -> None:
        """Query with subquery should trigger validation."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result()
        sql = "SELECT * FROM (SELECT id FROM users) AS u"

        should = validator.should_validate("test_db", sql, result, generation)

        assert should is True

    def test_should_validate_window_function_returns_true(self) -> None:
        """Query with window function should trigger validation."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result()
        sql = "SELECT name, ROW_NUMBER() OVER (PARTITION BY dept) FROM employees"

        should = validator.should_validate("test_db", sql, result, generation)

        assert should is True

    def test_should_validate_low_logprob_returns_true(self) -> None:
        """Logprob below threshold should trigger validation."""
        settings = _make_settings(
            enable_validation=True,
            confidence_threshold=-0.5,
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result(avg_logprob=-1.5)

        should = validator.should_validate("test_db", "SELECT * FROM users", result, generation)

        assert should is True

    def test_should_validate_single_join_no_trigger(self) -> None:
        """Query with only 1 JOIN should not trigger validation."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        generation = _make_generation_result()
        sql = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id"

        should = validator.should_validate("test_db", sql, result, generation)

        assert should is False


# =============================================================================
# Deny list
# =============================================================================

class TestDenyList:
    """Tests for validation_deny_list handling."""

    def test_deny_list_exact_database_match(self) -> None:
        """Exact database name in deny_list should be denied."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="test_db",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("test_db") is True

    def test_deny_list_different_database_no_match(self) -> None:
        """Database not in deny_list should not be denied."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="other_db",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("test_db") is False

    def test_deny_list_wildcard_match(self) -> None:
        """Wildcard '*' in deny_list should deny all databases."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="*",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("any_db") is True
        assert validator._is_denied("test_db") is True

    def test_deny_list_multiple_entries(self) -> None:
        """Multiple entries in deny_list should be checked."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="db1, test_db, db2",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("test_db") is True
        assert validator._is_denied("db1") is True
        assert validator._is_denied("other") is False

    def test_deny_list_empty_returns_false(self) -> None:
        """Empty deny_list should not deny anything."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("test_db") is False

    def test_deny_list_case_insensitive(self) -> None:
        """Deny list matching should be case-insensitive."""
        settings = _make_settings(
            enable_validation=True,
            deny_list="TEST_DB",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)

        assert validator._is_denied("test_db") is True


# =============================================================================
# Policy application
# =============================================================================

class TestPolicyApplication:
    """Tests for validation data policy application."""

    def test_policy_metadata_only_no_rows_in_prompt(self) -> None:
        """metadata_only policy should not include sample rows."""
        settings = _make_settings(
            enable_validation=True,
            policy=ValidationDataPolicy.metadata_only,
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(rows=[[1, "Alice"], [2, "Bob"]])
        schema = _make_schema()

        prompt = validator._build_prompt("List users", "SELECT * FROM users", result, schema)

        assert "Sample rows" not in prompt
        assert "row_count" in prompt.lower() or "rows" in prompt.lower()

    def test_policy_full_includes_rows(self) -> None:
        """full policy should include sample rows."""
        settings = _make_settings(
            enable_validation=True,
            policy=ValidationDataPolicy.full,
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(rows=[[1, "Alice"], [2, "Bob"]])
        schema = _make_schema()

        prompt = validator._build_prompt("List users", "SELECT * FROM users", result, schema)

        assert "Sample rows" in prompt
        assert "Alice" in prompt

    def test_policy_masked_masks_pii(self) -> None:
        """masked policy should mask PII in sample rows."""
        settings = _make_settings(
            enable_validation=True,
            policy=ValidationDataPolicy.masked,
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(
            rows=[[1, "alice@example.com"], [2, "bob@test.org"]],
            columns=["id", "email"],
        )
        schema = _make_schema()

        prompt = validator._build_prompt("List users", "SELECT * FROM users", result, schema)

        assert "Sample rows (masked)" in prompt
        assert "alice@example.com" not in prompt

    def test_policy_deny_list_forces_metadata_only(self) -> None:
        """Denied database should force metadata_only regardless of policy."""
        settings = _make_settings(
            enable_validation=True,
            policy=ValidationDataPolicy.full,
            deny_list="test_db",
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(rows=[[1, "Alice"]])
        schema = _make_schema()

        prompt = validator._build_prompt("List users", "SELECT * FROM users", result, schema)

        # Should not contain sample rows even though policy is "full"
        assert "Sample rows" not in prompt

    def test_policy_no_rows_no_sample(self) -> None:
        """When result has no rows, sample should not be included even with full policy."""
        settings = _make_settings(
            enable_validation=True,
            policy=ValidationDataPolicy.full,
        )
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result(rows=[])
        schema = _make_schema()

        prompt = validator._build_prompt("List users", "SELECT * FROM users", result, schema)

        assert "Sample rows" not in prompt


# =============================================================================
# LLM validation
# =============================================================================

class TestValidate:
    """Tests for the async validate method."""

    @pytest.mark.asyncio
    async def test_validate_returns_pass_verdict(self) -> None:
        """LLM returning pass verdict should be parsed correctly."""
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.choices = [
            AsyncMock(message=AsyncMock(content='{"verdict": "pass", "reason": null}'))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=mock_client, settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        verdict = await validator.validate("List users", "SELECT * FROM users", result, schema)

        assert verdict.verdict == "pass"

    @pytest.mark.asyncio
    async def test_validate_returns_fix_verdict(self) -> None:
        """LLM returning fix verdict with suggested SQL should be parsed."""
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.choices = [
            AsyncMock(message=AsyncMock(content='{"verdict": "fix", "reason": "Add LIMIT", "suggested_sql": "SELECT * FROM users LIMIT 10"}'))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=mock_client, settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        verdict = await validator.validate("List users", "SELECT * FROM users", result, schema)

        assert verdict.verdict == "fix"
        assert verdict.reason == "Add LIMIT"
        assert verdict.suggested_sql == "SELECT * FROM users LIMIT 10"

    @pytest.mark.asyncio
    async def test_validate_returns_fail_verdict(self) -> None:
        """LLM returning fail verdict should be parsed correctly."""
        mock_client = AsyncMock()
        mock_response = AsyncMock()
        mock_response.choices = [
            AsyncMock(message=AsyncMock(content='{"verdict": "fail", "reason": "Cannot answer"}'))
        ]
        mock_client.chat.completions.create.return_value = mock_response

        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=mock_client, settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        verdict = await validator.validate("List users", "SELECT * FROM users", result, schema)

        assert verdict.verdict == "fail"
        assert verdict.reason == "Cannot answer"

    @pytest.mark.asyncio
    async def test_validate_timeout_raises_llm_timeout(self) -> None:
        """LLM timeout during validation should raise LlmTimeoutError."""
        mock_client = AsyncMock()
        mock_client.chat.completions.create.side_effect = TimeoutError()

        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=mock_client, settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        with pytest.raises(LlmTimeoutError):
            await validator.validate("List users", "SELECT * FROM users", result, schema)

    @pytest.mark.asyncio
    async def test_validate_api_error_raises_llm_error(self) -> None:
        """LLM API error during validation should raise LlmError."""
        mock_client = AsyncMock()
        import openai
        mock_client.chat.completions.create.side_effect = openai.APIError(
            message="API error", request=AsyncMock(), body=None
        )

        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=mock_client, settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        with pytest.raises(LlmError):
            await validator.validate("List users", "SELECT * FROM users", result, schema)


# =============================================================================
# Prompt building
# =============================================================================

class TestPromptBuilding:
    """Tests for _build_prompt method."""

    def test_prompt_includes_user_question(self) -> None:
        """Prompt should include the user's original question."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        prompt = validator._build_prompt("Show all users", "SELECT * FROM users", result, schema)

        assert "Show all users" in prompt

    def test_prompt_includes_sql(self) -> None:
        """Prompt should include the generated SQL."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        prompt = validator._build_prompt("Show users", "SELECT * FROM users", result, schema)

        assert "SELECT * FROM users" in prompt

    def test_prompt_includes_result_metadata(self) -> None:
        """Prompt should include result metadata (row count, columns)."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        prompt = validator._build_prompt("Show users", "SELECT * FROM users", result, schema)

        assert "2" in prompt  # row count
        assert "id" in prompt
        assert "name" in prompt

    def test_prompt_includes_column_types(self) -> None:
        """Prompt should include column type information."""
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(client=AsyncMock(), settings=settings)
        result = _make_execution_result()
        schema = _make_schema()

        prompt = validator._build_prompt("Show users", "SELECT * FROM users", result, schema)

        assert "integer" in prompt
        assert "text" in prompt
