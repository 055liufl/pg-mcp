"""Integration tests for schema discovery.

Covers:
- load_schema orchestration
- _assemble with all metadata types
- Index definition parsing
- Foreign key grouping
- Enum and composite type handling

Note: These tests mock asyncpg connections to avoid requiring a real PostgreSQL instance.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.schema.discovery import SchemaDiscovery

pytestmark = pytest.mark.integration


@pytest.fixture
def settings() -> Settings:
    return Settings(pg_user="test", pg_password="test")


@pytest.fixture
def mock_pool_manager(settings: Settings) -> ConnectionPoolManager:
    return ConnectionPoolManager(settings)


@pytest.fixture
def discovery(mock_pool_manager: ConnectionPoolManager) -> SchemaDiscovery:
    return SchemaDiscovery(mock_pool_manager, mock_pool_manager._settings)


class MockConnection:
    """A mock asyncpg Connection for testing schema discovery queries."""

    def __init__(self, records: dict[str, list[dict]]) -> None:
        self._records = records

    async def fetch(self, query: str, *args: object) -> list[MagicMock]:
        # Match by first significant word after SELECT
        key = self._guess_key(query)
        rows = self._records.get(key, [])
        result: list[MagicMock] = []
        for row in rows:
            mock_row = MagicMock()
            mock_row.__getitem__ = lambda self, k, row=row: row[k]  # type: ignore[misc]
            mock_row.keys = lambda row=row: list(row.keys())  # type: ignore[misc]
            mock_row.get = lambda k, default=None, row=row: row.get(k, default)  # type: ignore[misc]
            # Support dict-like access
            for k, v in row.items():
                setattr(mock_row, k, v)
            result.append(mock_row)
        return result

    def _guess_key(self, query: str) -> str:
        query_lower = query.lower()
        if "foreign_tables" in query_lower:
            return "tables_and_columns"
        if "key_column_usage" in query_lower and "primary key" in query_lower:
            return "primary_keys"
        if "pg_indexes" in query_lower:
            return "indexes"
        if "foreign key" in query_lower:
            return "foreign_keys"
        if "check_constraints" in query_lower:
            return "constraints"
        if "pg_enum" in query_lower:
            return "enum_types"
        if "typtype = 'c'" in query_lower:
            return "composite_types"
        if "information_schema.views" in query_lower or "pg_matviews" in query_lower:
            return "views"
        if "pg_proc" in query_lower:
            return "allowed_functions"
        return "tables_and_columns"


class TestLoadSchema:
    """Tests for the full schema loading pipeline."""

    @pytest.mark.asyncio
    async def test_load_schema_returns_database_schema(self, discovery: SchemaDiscovery) -> None:
        mock_conn = MockConnection(
            {
                "tables_and_columns": [
                    {
                        "table_schema": "public",
                        "table_name": "users",
                        "column_name": "id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                        "ordinal_position": 1,
                        "column_comment": None,
                        "table_comment": "User accounts",
                        "table_type": "BASE TABLE",
                        "is_foreign": False,
                    },
                    {
                        "table_schema": "public",
                        "table_name": "users",
                        "column_name": "name",
                        "data_type": "text",
                        "is_nullable": "NO",
                        "column_default": None,
                        "ordinal_position": 2,
                        "column_comment": None,
                        "table_comment": "User accounts",
                        "table_type": "BASE TABLE",
                        "is_foreign": False,
                    },
                ],
                "primary_keys": [
                    {"table_schema": "public", "table_name": "users", "column_name": "id"},
                ],
                "indexes": [],
                "foreign_keys": [],
                "constraints": [],
                "enum_types": [],
                "composite_types": [],
                "views": [],
                "allowed_functions": [
                    {"proname": "upper"},
                    {"proname": "lower"},
                ],
            }
        )
        mock_pool = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)

        with patch.object(
            discovery._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            schema = await discovery.load_schema("test_db")

            assert isinstance(schema, DatabaseSchema)
            assert schema.database == "test_db"
            assert len(schema.tables) == 1
            assert schema.tables[0].table_name == "users"
            assert len(schema.tables[0].columns) == 2
            assert schema.tables[0].columns[0].is_primary_key is True
            assert schema.tables[0].columns[1].is_primary_key is False

    @pytest.mark.asyncio
    async def test_load_schema_with_foreign_keys(self, discovery: SchemaDiscovery) -> None:
        mock_conn = MockConnection(
            {
                "tables_and_columns": [
                    {
                        "table_schema": "public",
                        "table_name": "orders",
                        "column_name": "id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                        "ordinal_position": 1,
                        "column_comment": None,
                        "table_comment": None,
                        "table_type": "BASE TABLE",
                        "is_foreign": False,
                    },
                    {
                        "table_schema": "public",
                        "table_name": "orders",
                        "column_name": "user_id",
                        "data_type": "integer",
                        "is_nullable": "NO",
                        "column_default": None,
                        "ordinal_position": 2,
                        "column_comment": None,
                        "table_comment": None,
                        "table_type": "BASE TABLE",
                        "is_foreign": False,
                    },
                ],
                "primary_keys": [
                    {"table_schema": "public", "table_name": "orders", "column_name": "id"},
                ],
                "indexes": [],
                "foreign_keys": [
                    {
                        "constraint_name": "fk_orders_user_id",
                        "source_schema": "public",
                        "source_table": "orders",
                        "source_column": "user_id",
                        "target_schema": "public",
                        "target_table": "users",
                        "target_column": "id",
                    },
                ],
                "constraints": [],
                "enum_types": [],
                "composite_types": [],
                "views": [],
                "allowed_functions": [],
            }
        )
        mock_pool = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)

        with patch.object(
            discovery._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            schema = await discovery.load_schema("test_db")

            assert len(schema.foreign_keys) == 1
            fk = schema.foreign_keys[0]
            assert fk.constraint_name == "fk_orders_user_id"
            assert fk.source_columns == ["user_id"]
            assert fk.target_columns == ["id"]

    @pytest.mark.asyncio
    async def test_load_schema_with_enum_types(self, discovery: SchemaDiscovery) -> None:
        mock_conn = MockConnection(
            {
                "tables_and_columns": [
                    {
                        "table_schema": "public",
                        "table_name": "orders",
                        "column_name": "status",
                        "data_type": "order_status",
                        "is_nullable": "NO",
                        "column_default": None,
                        "ordinal_position": 1,
                        "column_comment": None,
                        "table_comment": None,
                        "table_type": "BASE TABLE",
                        "is_foreign": False,
                    },
                ],
                "primary_keys": [],
                "indexes": [],
                "foreign_keys": [],
                "constraints": [],
                "enum_types": [
                    {
                        "schema_name": "public",
                        "type_name": "order_status",
                        "values": ["pending", "processing", "shipped"],
                    },
                ],
                "composite_types": [],
                "views": [],
                "allowed_functions": [],
            }
        )
        mock_pool = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)

        with patch.object(
            discovery._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            schema = await discovery.load_schema("test_db")

            assert len(schema.enum_types) == 1
            assert schema.enum_types[0].type_name == "order_status"
            assert schema.enum_types[0].values == ["pending", "processing", "shipped"]

    @pytest.mark.asyncio
    async def test_load_schema_with_views(self, discovery: SchemaDiscovery) -> None:
        mock_conn = MockConnection(
            {
                "tables_and_columns": [],
                "primary_keys": [],
                "indexes": [],
                "foreign_keys": [],
                "constraints": [],
                "enum_types": [],
                "composite_types": [],
                "views": [
                    {
                        "schema_name": "public",
                        "view_name": "active_users",
                        "definition": "SELECT id, name FROM users WHERE active = true",
                        "is_materialized": False,
                    },
                ],
                "allowed_functions": [],
            }
        )
        mock_pool = MagicMock()
        mock_acquire_cm = AsyncMock()
        mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)

        with patch.object(
            discovery._pool_mgr, "get_pool", new_callable=AsyncMock, return_value=mock_pool
        ):
            schema = await discovery.load_schema("test_db")

            assert len(schema.views) == 1
            assert schema.views[0].view_name == "active_users"
            assert schema.views[0].is_materialized is False


class TestIndexParsing:
    """Tests for index definition parsing."""

    def test_parse_index_def_btree(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING btree (email)"
        )

        assert columns == ["email"]
        assert index_type == "btree"
        assert is_unique is False

    def test_parse_index_def_unique(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE UNIQUE INDEX idx ON users USING btree (email)"
        )

        assert is_unique is True

    def test_parse_index_def_hash(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING hash (name)"
        )

        assert index_type == "hash"

    def test_parse_index_def_gin(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING gin (data)"
        )

        assert index_type == "gin"

    def test_parse_index_def_multi_column(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING btree (last_name, first_name)"
        )

        assert columns == ["last_name", "first_name"]

    def test_parse_index_def_with_asc_desc(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING btree (created_at DESC)"
        )

        assert columns == ["created_at"]

    def test_parse_index_def_with_nulls(self, discovery: SchemaDiscovery) -> None:
        columns, index_type, is_unique = discovery._parse_index_def(
            "CREATE INDEX idx ON users USING btree (name NULLS FIRST)"
        )

        assert columns == ["name"]


class TestAssembleEdgeCases:
    """Tests for _assemble edge cases."""

    def test_assemble_empty_tables(self, discovery: SchemaDiscovery) -> None:
        schema = discovery._assemble(
            database="empty_db",
            tables_and_cols=[],
            pks=[],
            indexes=[],
            fks=[],
            constraints=[],
            enums=[],
            composites=[],
            views=[],
            allowed_functions=set(),
        )

        assert schema.database == "empty_db"
        assert schema.tables == []
        assert schema.table_count() == 0

    def test_assemble_foreign_table_flag(self, discovery: SchemaDiscovery) -> None:
        schema = discovery._assemble(
            database="test_db",
            tables_and_cols=[
                {
                    "table_schema": "public",
                    "table_name": "remote_data",
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": "NO",
                    "column_default": None,
                    "ordinal_position": 1,
                    "column_comment": None,
                    "table_comment": None,
                    "table_type": "FOREIGN",
                    "is_foreign": True,
                },
            ],
            pks=[],
            indexes=[],
            fks=[],
            constraints=[],
            enums=[],
            composites=[],
            views=[],
            allowed_functions=set(),
        )

        assert schema.tables[0].is_foreign is True
        assert "public.remote_data" in schema.foreign_table_ids()
