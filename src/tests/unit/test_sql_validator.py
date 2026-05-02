"""Unit tests for SQLGlot AST validation.

Covers:
- Pass cases (SELECT, CTE, EXPLAIN, UNION, safe functions)
- Fail cases (DML, DDL, blacklisted functions, multi-statement, EXPLAIN ANALYZE)
- Foreign table access denial
- Parse failures
- Function whitelist (schema-driven)
"""

from __future__ import annotations

import pytest

from pg_mcp.engine.sql_validator import SqlValidator
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.protocols import ValidationResult
from tests.fixtures.sql_samples import (
    FAIL_CASES,
    FOREIGN_TABLE_CASES,
    PARSE_FAIL_CASES,
    PASS_CASES,
)


@pytest.fixture
def validator() -> SqlValidator:
    """Return a fresh SqlValidator instance."""
    return SqlValidator()


@pytest.fixture
def sample_schema() -> DatabaseSchema:
    """Return a minimal schema with allowed functions and no foreign tables."""
    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                    ColumnInfo(name="name", type="text", nullable=False),
                ],
            ),
        ],
        allowed_functions={"upper", "lower", "count", "sum", "avg", "coalesce"},
    )


@pytest.fixture
def schema_with_foreign_table() -> DatabaseSchema:
    """Return a schema with a foreign table."""
    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                ],
            ),
            TableInfo(
                schema_name="public",
                table_name="foreign_data",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                ],
                is_foreign=True,
            ),
        ],
    )


class TestPassCases:
    """Tests for SQL statements that should be accepted."""

    @pytest.mark.parametrize("name,sql", PASS_CASES)
    def test_pass_case_validates_successfully(
        self, validator: SqlValidator, name: str, sql: str
    ) -> None:
        result = validator.validate(sql)

        assert result.valid is True, f"Case {name} should pass but got: {result.reason}"

    def test_explain_select_sets_is_explain_flag(
        self, validator: SqlValidator
    ) -> None:
        result = validator.validate("EXPLAIN SELECT * FROM orders")

        assert result.valid is True
        assert result.is_explain is True

    def test_explain_verbose_sets_is_explain_flag(
        self, validator: SqlValidator
    ) -> None:
        result = validator.validate("EXPLAIN (VERBOSE, COSTS) SELECT * FROM orders")

        assert result.valid is True
        assert result.is_explain is True


class TestFailCases:
    """Tests for SQL statements that should be rejected."""

    @pytest.mark.parametrize("name,sql,expected_code", FAIL_CASES)
    def test_fail_case_rejected(
        self,
        validator: SqlValidator,
        name: str,
        sql: str,
        expected_code: str,
    ) -> None:
        result = validator.validate(sql)

        assert result.valid is False, f"Case {name} should fail but was accepted"
        assert result.code == expected_code


class TestParseFailures:
    """Tests for unparseable SQL."""

    @pytest.mark.parametrize("name,sql", PARSE_FAIL_CASES)
    def test_parse_failure_returns_parse_error(
        self, validator: SqlValidator, name: str, sql: str
    ) -> None:
        result = validator.validate(sql)

        assert result.valid is False
        assert result.code == "E_SQL_PARSE"


class TestForeignTables:
    """Tests for foreign table access prohibition."""

    @pytest.mark.parametrize("name,sql,expected_code", FOREIGN_TABLE_CASES)
    def test_foreign_table_access_denied(
        self,
        validator: SqlValidator,
        schema_with_foreign_table: DatabaseSchema,
        name: str,
        sql: str,
        expected_code: str,
    ) -> None:
        result = validator.validate(sql, schema_with_foreign_table)

        assert result.valid is False
        assert result.code == expected_code
        assert "外部表访问被拒绝" in (result.reason or "")

    def test_non_foreign_table_allowed_with_foreign_present(
        self,
        validator: SqlValidator,
        schema_with_foreign_table: DatabaseSchema,
    ) -> None:
        result = validator.validate(
            "SELECT * FROM public.users", schema_with_foreign_table
        )

        assert result.valid is True

    def test_unqualified_foreign_in_non_public_schema_denied(
        self,
        validator: SqlValidator,
    ) -> None:
        # Regression for schema-resolution mismatch: an unqualified
        # ``orders`` should canonicalize to whichever schema is first in
        # the search_path that defines it. Here that schema is ``app``,
        # which has a foreign table — so the validator must reject.
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="app",
                    table_name="orders",
                    columns=[
                        ColumnInfo(name="id", type="integer", nullable=False)
                    ],
                    is_foreign=True,
                ),
            ],
        )

        result = validator.validate(
            "SELECT * FROM orders",
            schema,
            schema_names=["app", "public"],
        )

        assert result.valid is False
        assert "外部表访问被拒绝" in (result.reason or "")

    def test_unqualified_table_uses_default_schema_when_no_search_path(
        self,
        validator: SqlValidator,
        schema_with_foreign_table: DatabaseSchema,
    ) -> None:
        # Without an explicit search_path, the validator falls back to
        # ``public`` and rejects when the table is foreign there.
        result = validator.validate(
            "SELECT * FROM foreign_data", schema_with_foreign_table
        )

        assert result.valid is False
        assert "外部表访问被拒绝" in (result.reason or "")


class TestFunctionWhitelist:
    """Tests for schema-driven function whitelist."""

    def test_allowed_function_passes(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        result = validator.validate("SELECT UPPER(name) FROM users", sample_schema)

        assert result.valid is True

    def test_case_expression_passes_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # CASE WHEN ... END is a SQL keyword expression, not a function;
        # it must not be checked against the function allowlist even though
        # SQLGlot models it as an exp.Func subclass.
        result = validator.validate(
            "SELECT CASE WHEN id > 0 THEN 'pos' ELSE 'zero' END FROM users",
            sample_schema,
        )

        assert result.valid is True

    def test_cast_expression_passes_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # CAST(...) and ::type are type-cast expressions, not callable
        # functions. They subclass exp.Func in SQLGlot but are absent from
        # pg_proc-derived allowlists.
        result_cast = validator.validate(
            "SELECT CAST(id AS TEXT) FROM users", sample_schema
        )
        result_colon = validator.validate(
            "SELECT id::text FROM users", sample_schema
        )

        assert result_cast.valid is True
        assert result_colon.valid is True

    def test_if_expression_passes_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # IF(cond, a, b) maps to exp.If — same keyword-expression treatment.
        result = validator.validate(
            "SELECT IF(id > 0, 'pos', 'zero') FROM users", sample_schema
        )

        assert result.valid is True

    def test_boolean_operators_pass_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # AND/OR/XOR are boolean operators, not callable functions, even
        # though SQLGlot models them as exp.Func subclasses.
        result = validator.validate(
            "SELECT * FROM users WHERE id > 0 AND name IS NOT NULL OR id = 1",
            sample_schema,
        )

        assert result.valid is True

    def test_exists_subquery_passes_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # EXISTS(...) is a SQL keyword expression that subclasses exp.Func.
        result = validator.validate(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id = 1) AS has_user",
            sample_schema,
        )

        assert result.valid is True

    def test_current_date_keyword_passes_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # CURRENT_DATE / CURRENT_TIMESTAMP are SQL keywords (no parens),
        # not pg_proc functions.
        result = validator.validate(
            "SELECT CURRENT_DATE, CURRENT_TIMESTAMP FROM users",
            sample_schema,
        )

        assert result.valid is True

    def test_sql_standard_keyword_funcs_pass_without_allowlist_entry(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        # COALESCE / NULLIF / GREATEST / LEAST are SQL keyword operators in
        # PostgreSQL (not present in pg_proc) but are commonly emitted by
        # LLMs and must pass the validator.
        result = validator.validate(
            "SELECT COALESCE(name, 'N/A'), NULLIF(id, 0), "
            "GREATEST(id, 1), LEAST(id, 100) FROM users",
            sample_schema,
        )

        assert result.valid is True

    def test_date_trunc_recognized_under_sqlglot_internal_alias(
        self,
        validator: SqlValidator,
        sample_schema: DatabaseSchema,
    ) -> None:
        # Regression: SQLGlot parses postgres `date_trunc(...)` as the
        # internal `TimestampTrunc` node whose ``sql_name()`` is
        # ``"TIMESTAMP_TRUNC"``. The validator must consult the rendered
        # function name (``date_trunc``) when checking the allowlist,
        # otherwise valid PG SQL is rejected as
        # "Function not in allowlist: timestamp_trunc".
        # Add ``date_trunc`` to the schema's allowlist for this test.
        schema_with_date_trunc = sample_schema.model_copy(
            update={
                "allowed_functions": sample_schema.allowed_functions
                | {"date_trunc"}
            }
        )
        result = validator.validate(
            "SELECT date_trunc('month', created_at) FROM users",
            schema_with_date_trunc,
        )

        assert result.valid is True
    def test_unknown_function_rejected_when_whitelist_set(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        result = validator.validate(
            "SELECT UNKNOWN_FUNC(name) FROM users", sample_schema
        )

        assert result.valid is False
        assert "函数不在允许列表中" in (result.reason or "")

    def test_blacklisted_function_always_rejected_even_in_whitelist(
        self, validator: SqlValidator, sample_schema: DatabaseSchema
    ) -> None:
        result = validator.validate(
            "SELECT pg_sleep(100) FROM users", sample_schema
        )

        assert result.valid is False
        assert "不允许的高风险函数" in (result.reason or "")

    def test_no_whitelist_allows_all_non_blacklisted_functions(
        self, validator: SqlValidator
    ) -> None:
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            allowed_functions=set(),
        )
        result = validator.validate("SELECT SOME_FUNC(id) FROM users", schema)

        assert result.valid is False


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_string_rejected(self, validator: SqlValidator) -> None:
        result = validator.validate("")

        assert result.valid is False
        assert result.code == "E_SQL_UNSAFE"

    def test_single_statement_allowed(self, validator: SqlValidator) -> None:
        result = validator.validate("SELECT 1")

        assert result.valid is True

    def test_multiple_statements_rejected(
        self, validator: SqlValidator
    ) -> None:
        result = validator.validate("SELECT 1; SELECT 2")

        assert result.valid is False
        assert "仅允许单条语句" in (result.reason or "")

    def test_explain_analyze_rejected(self, validator: SqlValidator) -> None:
        result = validator.validate("EXPLAIN ANALYZE SELECT * FROM users")

        assert result.valid is False
        assert "不允许 EXPLAIN ANALYZE" in (result.reason or "")

    def test_nested_dml_in_cte_rejected(
        self, validator: SqlValidator
    ) -> None:
        sql = """
            WITH cte AS (INSERT INTO logs VALUES (1) RETURNING id)
            SELECT * FROM cte
        """
        result = validator.validate(sql)

        assert result.valid is False
        assert "不允许的语句类型" in (result.reason or "")

    def test_deny_list_function_case_insensitive(
        self, validator: SqlValidator
    ) -> None:
        result = validator.validate("SELECT PG_SLEEP(100)")

        assert result.valid is False
        assert "pg_sleep" in (result.reason or "").lower()

    def test_copy_command_rejected(self, validator: SqlValidator) -> None:
        result = validator.validate("COPY users TO '/tmp/dump'")

        assert result.valid is False
        assert result.code == "E_SQL_UNSAFE"
