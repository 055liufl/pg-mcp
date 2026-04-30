"""Schema discovery using batch SQL queries against pg_catalog.

Avoids N+1 queries by fetching all metadata in a small number of
batched SQL statements per database.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.models.schema import (
    ColumnInfo,
    CompositeTypeInfo,
    ConstraintInfo,
    DatabaseSchema,
    EnumTypeInfo,
    ForeignKeyInfo,
    IndexInfo,
    TableInfo,
    ViewInfo,
)

log = structlog.get_logger()

# Functions explicitly denied regardless of volatility classification
_DENY_FUNCTIONS = frozenset({
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "lo_import",
    "lo_export",
    "lo_get",
    "lo_put",
    "pg_sleep",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "pg_advisory_unlock",
    "pg_advisory_unlock_all",
    "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock",
    "pg_notify",
    "pg_listening_channels",
    "dblink",
    "dblink_exec",
    "dblink_connect",
    "dblink_disconnect",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "set_config",
    "pg_switch_wal",
    "pg_create_restore_point",
})


class SchemaDiscovery:
    """Discovers database schema metadata via batched pg_catalog queries."""

    def __init__(
        self,
        pool_mgr: ConnectionPoolManager,
        settings: Settings,
    ) -> None:
        self._pool_mgr = pool_mgr
        self._settings = settings

    async def load_schema(self, database: str) -> DatabaseSchema:
        """Load complete schema metadata for a database.

        Executes a small number of batched SQL queries to fetch all
        tables, columns, indexes, constraints, foreign keys, views,
        enum types, composite types, and allowed functions.

        Args:
            database: Target database name.

        Returns:
            Fully populated ``DatabaseSchema`` instance.
        """
        pool = await self._pool_mgr.get_pool(database)
        async with pool.acquire() as conn:
            tables_and_cols = await self._fetch_tables_and_columns(conn)
            pks = await self._fetch_primary_keys(conn)
            indexes = await self._fetch_indexes(conn)
            fks = await self._fetch_foreign_keys(conn)
            constraints = await self._fetch_constraints(conn)
            enums = await self._fetch_enum_types(conn)
            composites = await self._fetch_composite_types(conn)
            views = await self._fetch_views(conn)
            allowed_functions = await self._load_allowed_functions(conn)

        return self._assemble(
            database=database,
            tables_and_cols=tables_and_cols,
            pks=pks,
            indexes=indexes,
            fks=fks,
            constraints=constraints,
            enums=enums,
            composites=composites,
            views=views,
            allowed_functions=allowed_functions,
        )

    async def _fetch_tables_and_columns(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                col_description(
                    (c.table_schema || '.' || c.table_name)::regclass::oid,
                    c.ordinal_position
                ) AS column_comment,
                obj_description(
                    (c.table_schema || '.' || c.table_name)::regclass::oid
                ) AS table_comment,
                t.table_type,
                ft.foreign_table_name IS NOT NULL AS is_foreign
            FROM information_schema.columns c
            JOIN information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            LEFT JOIN information_schema.foreign_tables ft
                ON c.table_schema = ft.foreign_table_schema
                AND c.table_name = ft.foreign_table_name
            WHERE c.table_schema NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast'
            )
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
            """
        )

    async def _fetch_primary_keys(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                kcu.table_schema,
                kcu.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                USING (constraint_name, table_schema)
            WHERE tc.constraint_type = 'PRIMARY KEY'
            """
        )

    async def _fetch_indexes(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                schemaname AS schema_name,
                tablename AS table_name,
                indexname AS index_name,
                indexdef AS index_def
            FROM pg_indexes
            WHERE schemaname NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast'
            )
            """
        )

    async def _fetch_foreign_keys(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                tc.constraint_name,
                tc.table_schema AS source_schema,
                tc.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_schema AS target_schema,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """
        )

    async def _fetch_constraints(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                tc.constraint_type,
                cc.check_clause AS definition
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.constraint_type IN ('CHECK', 'UNIQUE', 'EXCLUSION')
              AND tc.table_schema NOT IN (
                  'pg_catalog', 'information_schema', 'pg_toast'
              )
            """
        )

    async def _fetch_enum_types(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                n.nspname AS schema_name,
                t.typname AS type_name,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE n.nspname NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast'
            )
            GROUP BY n.nspname, t.typname
            """
        )

    async def _fetch_composite_types(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                n.nspname AS schema_name,
                t.typname AS type_name,
                a.attname AS attr_name,
                format_type(a.atttypid, a.atttypmod) AS attr_type,
                a.attnotnull AS attr_notnull
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_attribute a ON t.oid = a.attrelid
            WHERE t.typtype = 'c'
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname NOT IN (
                  'pg_catalog', 'information_schema', 'pg_toast'
              )
            ORDER BY n.nspname, t.typname, a.attnum
            """
        )

    async def _fetch_views(
        self, conn: asyncpg.Connection
    ) -> list[asyncpg.Record]:
        return await conn.fetch(
            """
            SELECT
                v.table_schema AS schema_name,
                v.table_name AS view_name,
                v.view_definition AS definition,
                FALSE AS is_materialized
            FROM information_schema.views v
            WHERE v.table_schema NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast'
            )
            UNION ALL
            SELECT
                schemaname AS schema_name,
                matviewname AS view_name,
                definition,
                TRUE AS is_materialized
            FROM pg_matviews
            WHERE schemaname NOT IN (
                'pg_catalog', 'information_schema', 'pg_toast'
            )
            """
        )

    async def _load_allowed_functions(
        self, conn: asyncpg.Connection
    ) -> set[str]:
        """Load the set of allowed (IMMUTABLE/STABLE) functions from pg_proc.

        Excludes functions in the explicit deny-list regardless of their
        volatility classification.
        """
        rows = await conn.fetch(
            """
            SELECT p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname IN ('pg_catalog', 'public')
              AND p.provolatile IN ('i', 's')
            """
        )
        return {r["proname"] for r in rows} - _DENY_FUNCTIONS

    def _assemble(
        self,
        database: str,
        tables_and_cols: list[asyncpg.Record],
        pks: list[asyncpg.Record],
        indexes: list[asyncpg.Record],
        fks: list[asyncpg.Record],
        constraints: list[asyncpg.Record],
        enums: list[asyncpg.Record],
        composites: list[asyncpg.Record],
        views: list[asyncpg.Record],
        allowed_functions: set[str],
    ) -> DatabaseSchema:
        """Assemble batched query results into a ``DatabaseSchema``."""
        # Build primary key lookup: (schema, table) -> set of column names
        pk_lookup: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in pks:
            pk_lookup[(row["table_schema"], row["table_name"])].add(
                row["column_name"]
            )

        # Group columns by (schema, table)
        col_groups: dict[
            tuple[str, str], list[tuple[asyncpg.Record, bool]]
        ] = defaultdict(list)
        for row in tables_and_cols:
            key = (row["table_schema"], row["table_name"])
            is_pk = row["column_name"] in pk_lookup.get(key, set())
            col_groups[key].append((row, is_pk))

        # Build TableInfo list
        tables: list[TableInfo] = []
        for (schema_name, table_name), cols in col_groups.items():
            columns: list[ColumnInfo] = []
            table_comment: str | None = None
            is_foreign = False
            for row, is_pk in cols:
                columns.append(
                    ColumnInfo(
                        name=row["column_name"],
                        type=row["data_type"],
                        nullable=row["is_nullable"] == "YES",
                        default=row["column_default"],
                        comment=row["column_comment"],
                        is_primary_key=is_pk,
                    )
                )
                if table_comment is None:
                    table_comment = row["table_comment"]
                is_foreign = row["is_foreign"]

            tables.append(
                TableInfo(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=columns,
                    comment=table_comment,
                    is_foreign=is_foreign,
                )
            )

        # Build IndexInfo list
        index_list: list[IndexInfo] = []
        for row in indexes:
            index_def: str = row["index_def"]
            columns, index_type, is_unique = self._parse_index_def(index_def)
            index_list.append(
                IndexInfo(
                    schema_name=row["schema_name"],
                    table_name=row["table_name"],
                    index_name=row["index_name"],
                    columns=columns,
                    index_type=index_type,
                    is_unique=is_unique,
                )
            )

        # Build ForeignKeyInfo list
        fk_list: list[ForeignKeyInfo] = []
        fk_groups: dict[
            str, dict[str, Any]
        ] = defaultdict(lambda: {"source_columns": [], "target_columns": []})
        for row in fks:
            key = row["constraint_name"]
            entry = fk_groups[key]
            if not entry.get("constraint_name"):
                entry["constraint_name"] = row["constraint_name"]
                entry["source_schema"] = row["source_schema"]
                entry["source_table"] = row["source_table"]
                entry["target_schema"] = row["target_schema"]
                entry["target_table"] = row["target_table"]
            entry["source_columns"].append(row["source_column"])
            entry["target_columns"].append(row["target_column"])

        for entry in fk_groups.values():
            fk_list.append(
                ForeignKeyInfo(
                    constraint_name=entry["constraint_name"],
                    source_schema=entry["source_schema"],
                    source_table=entry["source_table"],
                    source_columns=entry["source_columns"],
                    target_schema=entry["target_schema"],
                    target_table=entry["target_table"],
                    target_columns=entry["target_columns"],
                )
            )

        # Build ConstraintInfo list
        constraint_list: list[ConstraintInfo] = []
        for row in constraints:
            constraint_list.append(
                ConstraintInfo(
                    schema_name=row["table_schema"],
                    table_name=row["table_name"],
                    constraint_name=row["constraint_name"],
                    constraint_type=row["constraint_type"],
                    definition=row["definition"] or "",
                )
            )

        # Build EnumTypeInfo list
        enum_list: list[EnumTypeInfo] = []
        for row in enums:
            enum_list.append(
                EnumTypeInfo(
                    schema_name=row["schema_name"],
                    type_name=row["type_name"],
                    values=row["values"],
                )
            )

        # Build CompositeTypeInfo list
        composite_groups: dict[
            tuple[str, str], list[ColumnInfo]
        ] = defaultdict(list)
        for row in composites:
            key = (row["schema_name"], row["type_name"])
            composite_groups[key].append(
                ColumnInfo(
                    name=row["attr_name"],
                    type=row["attr_type"],
                    nullable=not row["attr_notnull"],
                )
            )
        composite_list: list[CompositeTypeInfo] = [
            CompositeTypeInfo(
                schema_name=schema_name,
                type_name=type_name,
                attributes=attrs,
            )
            for (schema_name, type_name), attrs in composite_groups.items()
        ]

        # Build ViewInfo list
        view_list: list[ViewInfo] = []
        for row in views:
            view_list.append(
                ViewInfo(
                    schema_name=row["schema_name"],
                    view_name=row["view_name"],
                    columns=[],  # Simplified: columns not fetched for views
                    definition=row["definition"],
                    is_materialized=row["is_materialized"],
                )
            )

        return DatabaseSchema(
            database=database,
            tables=tables,
            views=view_list,
            indexes=index_list,
            foreign_keys=fk_list,
            constraints=constraint_list,
            enum_types=enum_list,
            composite_types=composite_list,
            allowed_functions=allowed_functions,
            loaded_at=datetime.now(timezone.utc),
        )

    def _parse_index_def(
        self, index_def: str
    ) -> tuple[list[str], str, bool]:
        """Parse index definition string to extract columns, type, and uniqueness.

        This is a best-effort parser for common PostgreSQL index definitions.
        """
        columns: list[str] = []
        index_type = "btree"
        is_unique = "UNIQUE" in index_def.upper()

        # Try to extract USING clause
        upper_def = index_def.upper()
        if "USING HASH" in upper_def:
            index_type = "hash"
        elif "USING GIN" in upper_def:
            index_type = "gin"
        elif "USING GIST" in upper_def:
            index_type = "gist"
        elif "USING SPGIST" in upper_def:
            index_type = "spgist"
        elif "USING BRIN" in upper_def:
            index_type = "brin"

        # Try to extract column list from parentheses
        # Example: CREATE INDEX idx ON table USING btree (col1, col2)
        start = index_def.find("(")
        end = index_def.rfind(")")
        if start != -1 and end != -1 and end > start:
            cols_str = index_def[start + 1:end]
            # Split by comma, but be careful with function expressions
            raw_cols = cols_str.split(",")
            for c in raw_cols:
                c = c.strip()
                # Take just the column name if there's ASC/DESC or other modifiers
                for suffix in (" ASC", " DESC", " NULLS FIRST", " NULLS LAST"):
                    if c.upper().endswith(suffix.upper()):
                        c = c[: -len(suffix)].strip()
                        break
                if c:
                    columns.append(c)

        return columns, index_type, is_unique
