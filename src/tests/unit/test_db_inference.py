"""Unit tests for database inference.

Covers:
- Single hit (unambiguous match)
- Ambiguous match
- No match
- Cross-database detection
- Partial ready (some schemas loading)
- Keyword extraction
- Summary building
"""

from __future__ import annotations

import pytest

from pg_mcp.config import Settings
from pg_mcp.engine.db_inference import DbInference, DbSummary
from pg_mcp.models.errors import (
    CrossDbUnsupportedError,
    DbInferAmbiguousError,
    DbInferNoMatchError,
    SchemaNotReadyError,
)
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo


class MockSchemaCache:
    """Mock SchemaCacheProtocol implementation for inference tests."""

    def __init__(
        self,
        databases: list[str],
        schemas: Optional[dict[str, DatabaseSchema]] = None,
        raise_on_get: Optional[Exception] = None,
    ) -> None:
        self._databases = databases
        self._schemas = schemas or {}
        self._raise_on_get = raise_on_get

    def discovered_databases(self) -> list[str]:
        return list(self._databases)

    async def get_schema(self, database: str) -> DatabaseSchema:
        if self._raise_on_get:
            raise self._raise_on_get
        if database in self._schemas:
            return self._schemas[database]
        raise SchemaNotReadyError(f"Schema for {database} not ready")

    async def refresh(self, database: Optional[str] = None) -> None:
        pass


def _make_settings() -> Settings:
    return Settings(
        pg_user="test",
        pg_password="test",
        pg_databases="",
    )


def _make_schema(database: str, tables: list[TableInfo]) -> DatabaseSchema:
    return DatabaseSchema(database=database, tables=tables)


@pytest.fixture
def settings() -> Settings:
    return _make_settings()


class TestSingleHit:
    """Tests for unambiguous single-database matches."""

    @pytest.mark.asyncio
    async def test_infer_single_hit_exact_table_name_returns_db(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "sales_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(["sales_db"], {"sales_db": schema})
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        result = await inference.infer("show me all orders")

        assert result == "sales_db"

    @pytest.mark.asyncio
    async def test_infer_single_hit_column_name_returns_db(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "hr_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="employees",
                    columns=[
                        ColumnInfo(name="id", type="integer", nullable=False),
                        ColumnInfo(name="salary", type="numeric", nullable=False),
                    ],
                ),
            ],
        )
        cache = MockSchemaCache(["hr_db"], {"hr_db": schema})
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        result = await inference.infer("what is the average salary")

        assert result == "hr_db"

    @pytest.mark.asyncio
    async def test_infer_single_hit_comment_match_returns_db(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "inventory_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="items",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                    comment="Inventory tracking table for warehouse stock",
                ),
            ],
        )
        cache = MockSchemaCache(["inventory_db"], {"inventory_db": schema})
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        result = await inference.infer("warehouse stock levels")

        assert result == "inventory_db"


class TestAmbiguous:
    """Tests for ambiguous matches between multiple databases."""

    @pytest.mark.asyncio
    async def test_infer_ambiguous_match_raises_error(
        self, settings: Settings
    ) -> None:
        schema_a = _make_schema(
            "db_a",
            [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        schema_b = _make_schema(
            "db_b",
            [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(
            ["db_a", "db_b"], {"db_a": schema_a, "db_b": schema_b}
        )
        inference = DbInference(cache, settings)
        inference.build_summary(schema_a)
        inference.build_summary(schema_b)

        with pytest.raises(DbInferAmbiguousError) as exc_info:
            await inference.infer("show me all users")

        assert "db_a" in exc_info.value.candidates
        assert "db_b" in exc_info.value.candidates


class TestNoMatch:
    """Tests for queries that match no database."""

    @pytest.mark.asyncio
    async def test_infer_no_match_raises_error(self, settings: Settings) -> None:
        schema = _make_schema(
            "sales_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(["sales_db"], {"sales_db": schema})
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        with pytest.raises(DbInferNoMatchError):
            await inference.infer("show me all astronauts")

    @pytest.mark.asyncio
    async def test_infer_only_stopwords_raises_no_match(
        self, settings: Settings
    ) -> None:
        cache = MockSchemaCache(["sales_db"])
        inference = DbInference(cache, settings)

        with pytest.raises(DbInferNoMatchError):
            await inference.infer("the and or")

    @pytest.mark.asyncio
    async def test_infer_empty_query_raises_no_match(
        self, settings: Settings
    ) -> None:
        cache = MockSchemaCache(["sales_db"])
        inference = DbInference(cache, settings)

        with pytest.raises(DbInferNoMatchError):
            await inference.infer("")


class TestCrossDb:
    """Tests for cross-database query detection."""

    @pytest.mark.asyncio
    async def test_infer_cross_db_raises_error(self, settings: Settings) -> None:
        schema_a = _make_schema(
            "sales_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        schema_b = _make_schema(
            "hr_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="employees",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(
            ["sales_db", "hr_db"], {"sales_db": schema_a, "hr_db": schema_b}
        )
        inference = DbInference(cache, settings)
        inference.build_summary(schema_a)
        inference.build_summary(schema_b)

        with pytest.raises(CrossDbUnsupportedError) as exc_info:
            await inference.infer("orders and employees")

        assert "sales_db" in str(exc_info.value)
        assert "hr_db" in str(exc_info.value)


class TestPartialReady:
    """Tests when some databases are still loading."""

    @pytest.mark.asyncio
    async def test_infer_some_not_ready_others_match_returns_match(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "ready_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(
            ["ready_db", "loading_db"], {"ready_db": schema}
        )
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        result = await inference.infer("show me all users")

        assert result == "ready_db"

    @pytest.mark.asyncio
    async def test_infer_all_not_ready_raises_schema_not_ready(
        self, settings: Settings
    ) -> None:
        cache = MockSchemaCache(["loading_db"])
        inference = DbInference(cache, settings)

        with pytest.raises(SchemaNotReadyError) as exc_info:
            await inference.infer("show me all users")

        assert exc_info.value.retry_after_ms == 3000

    @pytest.mark.asyncio
    async def test_infer_no_scored_and_some_not_ready_raises_not_ready(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "sales_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(
            ["sales_db", "loading_db"], {"sales_db": schema}
        )
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        with pytest.raises(SchemaNotReadyError):
            await inference.infer("show me all astronauts")


class TestKeywordExtraction:
    """Tests for the keyword extraction logic."""

    def test_extract_keywords_filters_stopwords(self, settings: Settings) -> None:
        cache = MockSchemaCache(["db"])
        inference = DbInference(cache, settings)

        keywords = inference._extract_keywords("show me all the users")

        assert "users" in keywords
        assert "the" not in keywords
        assert "me" not in keywords

    def test_extract_keywords_filters_short_tokens(self, settings: Settings) -> None:
        cache = MockSchemaCache(["db"])
        inference = DbInference(cache, settings)

        keywords = inference._extract_keywords("a b cd user")

        assert "user" in keywords
        assert "a" not in keywords
        assert "b" not in keywords
        assert "cd" in keywords

    def test_extract_keywords_lowercases(self, settings: Settings) -> None:
        cache = MockSchemaCache(["db"])
        inference = DbInference(cache, settings)

        keywords = inference._extract_keywords("USERS Orders")

        assert "users" in keywords
        assert "orders" in keywords

    def test_extract_keywords_expands_chinese_synonyms(
        self, settings: Settings
    ) -> None:
        # Chinese tokens are not split by the regex tokenizer; the synonym
        # map enriches them with English equivalents that can match the
        # English schema entity names actually present in pg-mcp fixtures.
        cache = MockSchemaCache(["db"])
        inference = DbInference(cache, settings)

        keywords = inference._extract_keywords("查询所有已发布的博客文章")

        # Should expand 博客 / 文章 / 已发布 into English synonyms.
        assert "blog" in keywords
        assert "post" in keywords
        assert "article" in keywords
        assert "published" in keywords

    def test_extract_keywords_chinese_partial_match_in_long_token(
        self, settings: Settings
    ) -> None:
        # Even when many Chinese characters fuse into a single token,
        # substring matches in the synonym map should still trigger.
        cache = MockSchemaCache(["db"])
        inference = DbInference(cache, settings)

        keywords = inference._extract_keywords("近30天每个品牌的总销售额")

        assert "brand" in keywords
        assert "sale" in keywords or "sales" in keywords
        assert "revenue" in keywords

class TestSummaryBuilding:
    """Tests for DbSummary construction."""

    def test_build_summary_collects_all_terms(self) -> None:
        schema = _make_schema(
            "test_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(name="id", type="integer", nullable=False),
                        ColumnInfo(name="email", type="text", nullable=True),
                    ],
                    comment="User accounts",
                ),
            ],
        )

        summary = DbSummary.from_schema(schema)

        assert summary.database == "test_db"
        assert "users" in summary.table_names
        assert "id" in summary.column_names
        assert "email" in summary.column_names
        assert summary.table_count == 1

    def test_remove_summary_clears_entry(self, settings: Settings) -> None:
        schema = _make_schema(
            "test_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(["test_db"])
        inference = DbInference(cache, settings)
        inference.build_summary(schema)

        inference.remove_summary("test_db")

        assert "test_db" not in inference._summaries


class TestOnDemandLoading:
    """Tests for on-demand schema loading during inference."""

    @pytest.mark.asyncio
    async def test_infer_triggers_on_demand_schema_load(
        self, settings: Settings
    ) -> None:
        schema = _make_schema(
            "lazy_db",
            [
                TableInfo(
                    schema_name="public",
                    table_name="products",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
        )
        cache = MockSchemaCache(["lazy_db"], {"lazy_db": schema})
        inference = DbInference(cache, settings)
        # No build_summary called -- should load on demand

        result = await inference.infer("show me all products")

        assert result == "lazy_db"
        assert "lazy_db" in inference._summaries
