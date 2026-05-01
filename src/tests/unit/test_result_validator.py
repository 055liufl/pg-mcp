"""Unit tests for AI result validation.

Covers:
- should_validate trigger conditions (complexity, empty result, low confidence)
- Deny_list matching (exact, wildcard)
- Policy handling (metadata_only, masked, full)
- _mask_pii and _mask_row helpers
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_mcp.config import Settings, ValidationDataPolicy
from pg_mcp.engine.result_validator import ResultValidator, _mask_pii, _mask_row
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.protocols import ExecutionResult, SqlGenerationResult


def _make_settings(**overrides: object) -> Settings:
    defaults = dict(
        pg_user="test",
        pg_password="test",
        enable_validation=True,
        validation_sample_rows=10,
        validation_data_policy=ValidationDataPolicy.METADATA_ONLY,
        validation_deny_list="",
        validation_confidence_threshold=-1.0,
        openai_model="gpt-5-mini",
    )
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def _make_result(
    rows: list[list] | None = None,
    row_count: Optional[int] = None,
    columns: Optional[list[str]] = None,
) -> ExecutionResult:
    cols = columns or ["id", "name"]
    r = rows or [[1, "Alice"], [2, "Bob"]]
    rc = row_count if row_count is not None else len(r)
    return ExecutionResult(
        columns=cols,
        column_types=["integer", "text"],
        rows=r,
        row_count=rc,
    )


def _make_generation(avg_logprob: float | None = None) -> SqlGenerationResult:
    return SqlGenerationResult(
        sql="SELECT * FROM users",
        prompt_tokens=100,
        completion_tokens=50,
        avg_logprob=avg_logprob,
    )


@pytest.fixture
def mock_openai_client() -> AsyncMock:
    client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices = [
        MagicMock(message=MagicMock(content='{"verdict": "pass", "reason": "good"}'))
    ]
    client.chat.completions.create = AsyncMock(return_value=mock_response)
    return client


class TestShouldValidate:
    """Tests for validation trigger logic."""

    def test_disabled_validation_returns_false(self) -> None:
        settings = _make_settings(enable_validation=False)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation()

        assert validator.should_validate("db", "SELECT 1", result, gen) is False

    def test_complex_query_with_two_joins_triggers(self) -> None:
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation()
        sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"

        assert validator.should_validate("db", sql, result, gen) is True

    def test_window_function_triggers(self) -> None:
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation()
        sql = "SELECT ROW_NUMBER() OVER(PARTITION BY id) FROM users"

        assert validator.should_validate("db", sql, result, gen) is True

    def test_subquery_triggers(self) -> None:
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation()
        sql = "SELECT * FROM (SELECT id FROM users) AS u"

        assert validator.should_validate("db", sql, result, gen) is True

    def test_empty_result_triggers(self) -> None:
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result(rows=[], row_count=0)
        gen = _make_generation()
        sql = "SELECT * FROM users WHERE id = 99999"

        assert validator.should_validate("db", sql, result, gen) is True

    def test_low_logprob_triggers(self) -> None:
        settings = _make_settings(
            enable_validation=True, validation_confidence_threshold=-0.5
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation(avg_logprob=-0.8)
        sql = "SELECT * FROM users"

        assert validator.should_validate("db", sql, result, gen) is True

    def test_simple_query_no_trigger(self) -> None:
        settings = _make_settings(enable_validation=True)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation()
        sql = "SELECT * FROM users"

        assert validator.should_validate("db", sql, result, gen) is False

    def test_high_logprob_no_trigger(self) -> None:
        settings = _make_settings(
            enable_validation=True, validation_confidence_threshold=-0.5
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        gen = _make_generation(avg_logprob=-0.3)
        sql = "SELECT * FROM users"

        assert validator.should_validate("db", sql, result, gen) is False


class TestDenyList:
    """Tests for validation deny_list matching."""

    def test_exact_database_match_denied(self) -> None:
        settings = _make_settings(validation_deny_list="prod_db")
        validator = ResultValidator(AsyncMock(), settings)

        assert validator._is_denied("prod_db") is True

    def test_case_insensitive_match(self) -> None:
        settings = _make_settings(validation_deny_list="PROD_DB")
        validator = ResultValidator(AsyncMock(), settings)

        assert validator._is_denied("prod_db") is True

    def test_wildcard_denies_all(self) -> None:
        settings = _make_settings(validation_deny_list="*")
        validator = ResultValidator(AsyncMock(), settings)

        assert validator._is_denied("any_db") is True
        assert validator._is_denied("other_db") is True

    def test_non_matching_database_allowed(self) -> None:
        settings = _make_settings(validation_deny_list="prod_db")
        validator = ResultValidator(AsyncMock(), settings)

        assert validator._is_denied("dev_db") is False

    def test_empty_deny_list_allows_all(self) -> None:
        settings = _make_settings(validation_deny_list="")
        validator = ResultValidator(AsyncMock(), settings)

        assert validator._is_denied("any_db") is False


class TestHierarchicalDenyList:
    """Tests for hierarchical ``db.schema.table.column`` deny rules."""

    def _schema_with_secrets(self, database: str = "prod_db") -> DatabaseSchema:
        return DatabaseSchema(
            database=database,
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(name="id", type="integer", nullable=False),
                        ColumnInfo(name="email", type="text", nullable=False),
                    ],
                ),
            ],
        )

    def test_table_level_rule_downgrades_to_metadata_only(self) -> None:
        settings = _make_settings(
            validation_deny_list="prod_db.public.users",
            validation_data_policy=ValidationDataPolicy.FULL,
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = ExecutionResult(
            columns=["id", "email"],
            column_types=["integer", "text"],
            rows=[[1, "alice@example.com"]],
            row_count=1,
        )
        schema = self._schema_with_secrets()

        prompt = validator._build_prompt(
            "list users", "SELECT id, email FROM public.users", result, schema
        )

        # Table-level rule must drop sample rows entirely.
        assert "Sample rows" not in prompt
        assert "alice@example.com" not in prompt

    def test_table_level_rule_does_not_apply_to_other_tables(self) -> None:
        settings = _make_settings(
            validation_deny_list="prod_db.public.orders",
            validation_data_policy=ValidationDataPolicy.FULL,
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = ExecutionResult(
            columns=["id", "email"],
            column_types=["integer", "text"],
            rows=[[1, "alice@example.com"]],
            row_count=1,
        )
        schema = self._schema_with_secrets()

        prompt = validator._build_prompt(
            "list users",
            "SELECT id, email FROM public.users",
            result,
            schema,
        )

        # Rule targets ``orders`` — ``users`` query should still ship rows.
        assert "Sample rows" in prompt
        assert "alice@example.com" in prompt

    def test_column_level_rule_masks_specific_column(self) -> None:
        settings = _make_settings(
            validation_deny_list="prod_db.public.users.email",
            validation_data_policy=ValidationDataPolicy.FULL,
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = ExecutionResult(
            columns=["id", "email"],
            column_types=["integer", "text"],
            rows=[[1, "alice@example.com"]],
            row_count=1,
        )
        schema = self._schema_with_secrets()

        prompt = validator._build_prompt(
            "list users",
            "SELECT id, email FROM public.users",
            result,
            schema,
        )

        # Column-level rule masks the email but keeps id and overall layout.
        assert "Sample rows" in prompt
        assert "alice@example.com" not in prompt
        assert "***" in prompt
        # The non-denied column should still appear.
        assert '"id": 1' in prompt or "[1," in prompt or "[[1," in prompt

    def test_wildcard_db_segment_applies_to_any_database(self) -> None:
        settings = _make_settings(
            validation_deny_list="*.public.users",
            validation_data_policy=ValidationDataPolicy.FULL,
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = ExecutionResult(
            columns=["id"],
            column_types=["integer"],
            rows=[[1]],
            row_count=1,
        )
        schema = DatabaseSchema(
            database="any_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(name="id", type="integer", nullable=False),
                    ],
                ),
            ],
        )

        prompt = validator._build_prompt(
            "show users", "SELECT id FROM users", result, schema
        )

        assert "Sample rows" not in prompt


class TestPolicyHandling:
    """Tests for data policy in prompt building."""

    def test_metadata_only_does_not_include_rows(self) -> None:
        settings = _make_settings(validation_data_policy=ValidationDataPolicy.METADATA_ONLY)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        prompt = validator._build_prompt("query", "SELECT 1", result, schema)

        assert "Sample rows" not in prompt
        assert "Result: 2 rows" in prompt

    def test_full_policy_includes_rows(self) -> None:
        settings = _make_settings(validation_data_policy=ValidationDataPolicy.FULL)
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        prompt = validator._build_prompt("query", "SELECT 1", result, schema)

        assert "Sample rows" in prompt
        assert "Alice" in prompt

    def test_masked_policy_masks_sensitive_columns(self) -> None:
        settings = _make_settings(validation_data_policy=ValidationDataPolicy.MASKED)
        validator = ResultValidator(AsyncMock(), settings)
        result = ExecutionResult(
            columns=["id", "password"],
            column_types=["integer", "text"],
            rows=[[1, "secret123"]],
            row_count=1,
        )
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        prompt = validator._build_prompt("query", "SELECT 1", result, schema)

        assert "Sample rows (masked)" in prompt
        assert "***" in prompt
        assert "secret123" not in prompt

    def test_denied_database_forces_metadata_only(self) -> None:
        settings = _make_settings(
            validation_data_policy=ValidationDataPolicy.FULL,
            validation_deny_list="prod_db",
        )
        validator = ResultValidator(AsyncMock(), settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="prod_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        prompt = validator._build_prompt("query", "SELECT 1", result, schema)

        assert "Sample rows" not in prompt


class TestMaskHelpers:
    """Tests for internal masking helpers."""

    def test_mask_pii_masks_email(self) -> None:
        result = _mask_pii("user@example.com")

        assert "user@example.com" not in result
        assert "***@***.***" in result

    def test_mask_pii_masks_phone(self) -> None:
        result = _mask_pii("13800138000")

        assert "13800138000" not in result
        assert "***PHONE***" in result

    def test_mask_pii_masks_id_card(self) -> None:
        result = _mask_pii("110101199001011234")

        assert "110101199001011234" not in result
        assert "***ID***" in result

    def test_mask_row_masks_sensitive_columns(self) -> None:
        row = [1, "secret123", "alice@example.com"]
        columns = ["id", "password", "email"]
        result = _mask_row(row, columns)

        assert result[0] == 1
        assert result[1] == "***"
        assert "alice@example.com" not in result[2]

    def test_mask_row_preserves_non_sensitive(self) -> None:
        row = [1, "Alice", 30]
        columns = ["id", "name", "age"]
        result = _mask_row(row, columns)

        assert result == [1, "Alice", 30]


class TestValidate:
    """Tests for the async validate method."""

    @pytest.mark.asyncio
    async def test_validate_returns_pass_verdict(self, mock_openai_client: AsyncMock) -> None:
        settings = _make_settings()
        validator = ResultValidator(mock_openai_client, settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        verdict = await validator.validate("query", "SELECT 1", result, schema)

        assert verdict.verdict == "pass"
        assert verdict.reason == "good"

    @pytest.mark.asyncio
    async def test_validate_calls_openai_with_json_format(
        self, mock_openai_client: AsyncMock
    ) -> None:
        settings = _make_settings()
        validator = ResultValidator(mock_openai_client, settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        await validator.validate("query", "SELECT 1", result, schema)

        call_kwargs = mock_openai_client.chat.completions.create.call_args.kwargs
        assert call_kwargs["response_format"] == {"type": "json_object"}
        assert call_kwargs["temperature"] == 0

    @pytest.mark.asyncio
    async def test_validate_timeout_raises_llm_timeout(self) -> None:
        client = AsyncMock()
        import asyncio

        client.chat.completions.create = AsyncMock(
            side_effect=asyncio.TimeoutError
        )
        settings = _make_settings()
        validator = ResultValidator(client, settings)
        result = _make_result()
        schema = DatabaseSchema(
            database="test_db",
            tables=[TableInfo(schema_name="public", table_name="users", columns=[])],
        )

        from pg_mcp.models.errors import LlmTimeoutError

        with pytest.raises(LlmTimeoutError):
            await validator.validate("query", "SELECT 1", result, schema)
