"""Unit tests for schema retrieval with keyword matching.

Covers:
- Keyword extraction and tokenization
- Table scoring by index
- Retrieval for large schemas
- Fallback when no positive scores
- Related foreign key inclusion
- Context building formatting
"""

from __future__ import annotations

import pytest

from pg_mcp.models.schema import (
    ColumnInfo,
    DatabaseSchema,
    EnumTypeInfo,
    ForeignKeyInfo,
    TableInfo,
)
from pg_mcp.schema.retriever import SchemaRetriever, TableIndex


@pytest.fixture
def retriever() -> SchemaRetriever:
    """Return a fresh SchemaRetriever instance."""
    return SchemaRetriever(max_tables_for_full=50)


@pytest.fixture
def small_schema() -> DatabaseSchema:
    """Return a small schema (under the retrieval threshold)."""
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
                comment="User accounts",
            ),
            TableInfo(
                schema_name="public",
                table_name="orders",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                    ColumnInfo(name="user_id", type="integer", nullable=False),
                    ColumnInfo(name="total", type="numeric", nullable=False),
                ],
            ),
        ],
        foreign_keys=[
            ForeignKeyInfo(
                constraint_name="fk_orders_user_id",
                source_schema="public",
                source_table="orders",
                source_columns=["user_id"],
                target_schema="public",
                target_table="users",
                target_columns=["id"],
            ),
        ],
        enum_types=[
            EnumTypeInfo(
                schema_name="public",
                type_name="order_status",
                values=["pending", "shipped"],
            ),
        ],
    )


@pytest.fixture
def large_schema() -> DatabaseSchema:
    """Return a large schema (over the retrieval threshold)."""
    tables: list[TableInfo] = []
    for i in range(60):
        tables.append(
            TableInfo(
                schema_name="public",
                table_name=f"table_{i:02d}",
                columns=[
                    ColumnInfo(name="id", type="integer", nullable=False),
                    ColumnInfo(name="name", type="text", nullable=False),
                ],
            )
        )
    return DatabaseSchema(
        database="big_db",
        tables=tables,
    )


class TestShouldUseRetrieval:
    """Tests for the retrieval threshold check."""

    def test_small_schema_does_not_use_retrieval(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        result = retriever.should_use_retrieval(small_schema)

        assert result is False

    def test_large_schema_uses_retrieval(
        self, retriever: SchemaRetriever, large_schema: DatabaseSchema
    ) -> None:
        result = retriever.should_use_retrieval(large_schema)

        assert result is True

    def test_threshold_boundary_exact_count(
        self, small_schema: DatabaseSchema
    ) -> None:
        retriever = SchemaRetriever(max_tables_for_full=2)

        result = retriever.should_use_retrieval(small_schema)

        assert result is True


class TestBuildIndex:
    """Tests for index building."""

    def test_build_index_creates_one_per_table(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        indices = retriever.build_index(small_schema)

        assert len(indices) == 2
        assert indices[0].table_name == "users"
        assert indices[1].table_name == "orders"

    def test_build_index_includes_column_names(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        indices = retriever.build_index(small_schema)

        users_idx = indices[0]
        assert "id" in users_idx.all_terms
        assert "name" in users_idx.all_terms
        assert "email" in users_idx.all_terms

    def test_build_index_includes_comments(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        indices = retriever.build_index(small_schema)

        users_idx = indices[0]
        assert "user" in users_idx.all_terms
        assert "accounts" in users_idx.all_terms


class TestScoreByIndex:
    """Tests for table scoring against keywords."""

    def test_exact_table_name_match_scores_highest(
        self, retriever: SchemaRetriever
    ) -> None:
        idx = TableIndex(
            schema_name="public", table_name="users", all_terms=frozenset()
        )
        score = retriever._score_by_index(idx, {"users"})

        assert score == 10.0

    def test_partial_table_name_match_scores_medium(
        self, retriever: SchemaRetriever
    ) -> None:
        idx = TableIndex(
            schema_name="public",
            table_name="user_accounts",
            all_terms=frozenset(),
        )
        score = retriever._score_by_index(idx, {"users"})

        assert score == 5.0

    def test_column_name_match_scores_low(
        self, retriever: SchemaRetriever
    ) -> None:
        idx = TableIndex(
            schema_name="public",
            table_name="users",
            all_terms=frozenset({"id", "name", "email"}),
        )
        score = retriever._score_by_index(idx, {"email"})

        assert score == 3.0

    def test_no_match_scores_zero(
        self, retriever: SchemaRetriever
    ) -> None:
        idx = TableIndex(
            schema_name="public",
            table_name="users",
            all_terms=frozenset({"id", "name"}),
        )
        score = retriever._score_by_index(idx, {"astronauts"})

        assert score == 0.0

    def test_multiple_keywords_accumulate(
        self, retriever: SchemaRetriever
    ) -> None:
        idx = TableIndex(
            schema_name="public",
            table_name="users",
            all_terms=frozenset({"id", "name", "email"}),
        )
        score = retriever._score_by_index(idx, {"users", "email"})

        assert score == 13.0  # 10 for table + 3 for column


class TestRetrieve:
    """Tests for the full retrieve pipeline."""

    def test_retrieve_returns_relevant_tables(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        context = retriever.retrieve("show me all users", small_schema)

        assert "users" in context
        assert "Database: test_db" in context

    def test_retrieve_includes_foreign_keys(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        context = retriever.retrieve("orders", small_schema)

        assert "Foreign Keys" in context
        assert "fk_orders_user_id" in context

    def test_retrieve_includes_enum_types(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        context = retriever.retrieve("orders", small_schema)

        assert "Enum Types" in context
        assert "order_status" in context

    def test_retrieve_fallback_when_no_positive_scores(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        context = retriever.retrieve("astronauts on mars", small_schema)

        # Should still return some tables (fallback to first N)
        assert "Database: test_db" in context
        assert "TABLE" in context

    def test_retrieve_uses_precomputed_index(
        self, retriever: SchemaRetriever, small_schema: DatabaseSchema
    ) -> None:
        # Precompute index and attach to schema
        index = retriever.build_index(small_schema)
        object.__setattr__(  # bypass frozen dataclass / pydantic
            small_schema, "_retrieval_index", index
        )

        context = retriever.retrieve("users", small_schema)

        assert "users" in context


class TestTokenize:
    """Tests for the tokenization logic."""

    def test_tokenize_filters_stopwords(self, retriever: SchemaRetriever) -> None:
        tokens = retriever._tokenize("the quick brown fox")

        assert "the" not in tokens
        assert "quick" in tokens
        assert "brown" in tokens
        assert "fox" in tokens

    def test_tokenize_filters_short_tokens(self, retriever: SchemaRetriever) -> None:
        tokens = retriever._tokenize("a bb ccc dddd")

        assert "a" not in tokens
        assert "bb" not in tokens
        assert "ccc" in tokens
        assert "dddd" in tokens

    def test_tokenize_extracts_alphanumeric(self, retriever: SchemaRetriever) -> None:
        tokens = retriever._tokenize("user_id, email-address")

        assert "user_id" in tokens
        assert "email" in tokens
        assert "address" in tokens


class TestGetRelatedForeignKeys:
    """Tests for foreign key relationship inclusion."""

    def test_fk_where_source_included(self, retriever: SchemaRetriever) -> None:
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            foreign_keys=[
                ForeignKeyInfo(
                    constraint_name="fk_orders_user",
                    source_schema="public",
                    source_table="orders",
                    source_columns=["user_id"],
                    target_schema="public",
                    target_table="users",
                    target_columns=["id"],
                ),
            ],
        )
        selected = [schema.tables[0]]  # orders
        related = retriever._get_related_foreign_keys(selected, schema)

        assert len(related) == 1
        assert related[0].constraint_name == "fk_orders_user"

    def test_fk_where_target_included(self, retriever: SchemaRetriever) -> None:
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="orders",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            foreign_keys=[
                ForeignKeyInfo(
                    constraint_name="fk_orders_user",
                    source_schema="public",
                    source_table="orders",
                    source_columns=["user_id"],
                    target_schema="public",
                    target_table="users",
                    target_columns=["id"],
                ),
            ],
        )
        selected = [schema.tables[1]]  # users
        related = retriever._get_related_foreign_keys(selected, schema)

        assert len(related) == 1

    def test_fk_unrelated_not_included(self, retriever: SchemaRetriever) -> None:
        schema = DatabaseSchema(
            database="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="products",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
                TableInfo(
                    schema_name="public",
                    table_name="categories",
                    columns=[ColumnInfo(name="id", type="integer", nullable=False)],
                ),
            ],
            foreign_keys=[
                ForeignKeyInfo(
                    constraint_name="fk_orders_user",
                    source_schema="public",
                    source_table="orders",
                    source_columns=["user_id"],
                    target_schema="public",
                    target_table="users",
                    target_columns=["id"],
                ),
            ],
        )
        selected = [schema.tables[0]]  # products
        related = retriever._get_related_foreign_keys(selected, schema)

        assert len(related) == 0
