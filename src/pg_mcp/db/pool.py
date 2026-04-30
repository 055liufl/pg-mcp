"""asyncpg connection pool management with retry/backoff and read-only checks."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import asyncpg
import structlog

from pg_mcp.config import Settings
from pg_mcp.models.errors import DbConnectError

log = structlog.get_logger()


class ConnectionPoolManager:
    """Manages per-database asyncpg connection pools with retry logic.

    Features:
    - Lazy pool creation per database (singleton per db)
    - Exponential backoff with jitter on connection failures
    - Database discovery with PG_DATABASES override support
    - Read-only permission assertion for security
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pools: dict[str, asyncpg.Pool] = {}
        self._lock = asyncio.Lock()

    async def get_pool(self, database: str) -> asyncpg.Pool:
        """Get or create a connection pool for the given database.

        Pool creation is guarded by an asyncio.Lock to prevent concurrent
        creation of multiple pools for the same database.

        Args:
            database: Target database name.

        Returns:
            An asyncpg connection pool.

        Raises:
            DbConnectError: If pool creation fails after all retries.
        """
        if database in self._pools:
            return self._pools[database]

        async with self._lock:
            if database in self._pools:
                return self._pools[database]

            dsn = self._build_dsn(database)
            pool = await self._create_pool_with_retry(dsn)
            self._pools[database] = pool
            return pool

    async def _create_pool_with_retry(self, dsn: str) -> asyncpg.Pool:
        """Create a connection pool with exponential backoff retry.

        Args:
            dsn: PostgreSQL connection string.

        Returns:
            An asyncpg connection pool.

        Raises:
            DbConnectError: If all retry attempts are exhausted.
        """
        max_retries = 5
        base_delay = 0.1
        max_delay = 3.0
        last_error: Exception | None = None

        for attempt in range(max_retries):
            try:
                return await asyncpg.create_pool(
                    dsn,
                    min_size=1,
                    max_size=self._settings.db_pool_size,
                    command_timeout=self._settings.query_timeout,
                )
            except (asyncpg.PostgresError, OSError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt == max_retries - 1:
                    break
                delay = min(base_delay * (2**attempt), max_delay)
                jitter = random.uniform(0, delay * 0.1)
                await asyncio.sleep(delay + jitter)

        raise DbConnectError(
            f"连接池创建失败（{max_retries} 次重试）: {last_error}"
        )

    def _build_dsn(self, database: str) -> str:
        """Build a PostgreSQL DSN from settings.

        Args:
            database: Target database name.

        Returns:
            PostgreSQL connection string.
        """
        sslmode = self._settings.pg_sslmode.value
        password = self._settings.pg_password.get_secret_value()
        dsn = (
            f"postgresql://{self._settings.pg_user}:{password}"
            f"@{self._settings.pg_host}:{self._settings.pg_port}"
            f"/{database}?sslmode={sslmode}"
        )
        if self._settings.pg_sslrootcert:
            dsn += f"&sslrootcert={self._settings.pg_sslrootcert}"
        return dsn

    async def discover_databases(self) -> list[str]:
        """Discover accessible databases.

        If ``pg_databases`` is configured in settings, returns that list
        directly and skips automatic discovery.

        Returns:
            Sorted list of accessible database names.
        """
        if self._settings.pg_databases_list:
            return list(self._settings.pg_databases_list)

        pool = await self.get_pool("postgres")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT datname FROM pg_database
                WHERE datallowconn = true
                  AND datname NOT IN (
                      SELECT unnest($1::text[])
                  )
                ORDER BY datname
                """,
                self._settings.pg_exclude_databases_list,
            )
            return [r["datname"] for r in rows]

    async def assert_readonly(self) -> None:
        """Assert that the database user has limited (read-only) privileges.

        Checks for superuser, createrole, createdb privileges, and table-level
        write permissions (INSERT, UPDATE, DELETE, TRUNCATE).

        If ``strict_readonly`` is enabled in settings, raises an error on
        any detected write capability. Otherwise logs a warning.

        Raises:
            RuntimeError: If write permissions are detected and
                ``strict_readonly`` is ``True``.
        """
        pool = await self.get_pool("postgres")
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT rolsuper, rolcreaterole, rolcreatedb
                FROM pg_roles WHERE rolname = current_user
                """
            )
            if row is None:
                log.warning("readonly_check_failed", msg="无法获取当前用户角色信息")
                return

            if row["rolsuper"] or row["rolcreaterole"] or row["rolcreatedb"]:
                if self._settings.strict_readonly:
                    raise RuntimeError(
                        "STRICT_READONLY: 用户拥有管理权限，拒绝启动"
                    )
                log.warning(
                    "readonly_check_failed",
                    msg="数据库用户拥有管理权限，强烈建议使用只读用户",
                )

            has_write = await conn.fetchval(
                """
                SELECT EXISTS(
                  SELECT 1 FROM information_schema.role_table_grants
                  WHERE grantee = current_user
                  AND privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE')
                  LIMIT 1
                )
                """
            )
            if has_write:
                if self._settings.strict_readonly:
                    raise RuntimeError(
                        "STRICT_READONLY: 用户拥有表写权限，拒绝启动"
                    )
                log.warning(
                    "readonly_check_failed",
                    msg="用户拥有表写权限，SQL 执行依赖只读事务保护",
                )

    async def close_all(self) -> None:
        """Close all managed connection pools and clear the registry."""
        for pool in self._pools.values():
            await pool.close()
        self._pools.clear()

    def __repr__(self) -> str:
        return f"ConnectionPoolManager(pools={list(self._pools.keys())})"
