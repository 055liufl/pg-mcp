"""Schema cache with Redis storage, singleflight loading, and periodic refresh."""

from __future__ import annotations

import asyncio
import gzip
from typing import Any

import redis.asyncio as redis
import structlog

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.models.errors import SchemaNotReadyError
from pg_mcp.models.schema import DatabaseSchema
from pg_mcp.protocols import RefreshResult
from pg_mcp.schema.discovery import SchemaDiscovery
from pg_mcp.schema.state import SchemaState

log = structlog.get_logger()


class SchemaCache:
    """Redis-backed schema cache with singleflight loading.

    Features:
    - Gzip-compressed schema storage in Redis with TTL
    - Singleflight: at most one loading task per database
    - Periodic background refresh
    - State machine tracking (UNLOADED -> LOADING -> READY/FAILED)
    """

    PREFIX = "pg_mcp"

    def __init__(
        self,
        redis_client: redis.Redis,
        pool_mgr: ConnectionPoolManager,
        settings: Settings,
    ) -> None:
        self._redis = redis_client
        self._discovery = SchemaDiscovery(pool_mgr, settings)
        self._settings = settings
        self._databases: list[str] = []
        # Singleflight: at most one loading task per database
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._inflight_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[Any] | None = None

    def set_discovered_databases(self, databases: list[str]) -> None:
        """Set the list of databases managed by this cache.

        Typically called once at startup after database discovery.
        """
        self._databases = list(databases)

    def discovered_databases(self) -> list[str]:
        """Return the list of discovered/registered databases."""
        return list(self._databases)

    async def get_schema(self, database: str) -> DatabaseSchema:
        """Get schema for a database, triggering load if needed.

        If the schema is already cached and READY, returns it immediately.
        If loading is needed or in progress, raises ``SchemaNotReadyError``.

        Args:
            database: Target database name.

        Returns:
            Deserialized ``DatabaseSchema``.

        Raises:
            SchemaNotReadyError: If schema is loading or not yet loaded.
        """
        state = await self._get_state(database)

        if state == SchemaState.READY:
            cached = await self._redis.get(
                f"{self.PREFIX}:schema:{database}"
            )
            if cached:
                try:
                    decompressed = gzip.decompress(cached)
                    return DatabaseSchema.model_validate_json(decompressed)
                except Exception:
                    log.warning(
                        "schema_cache_corrupted",
                        database=database,
                        msg="缓存数据损坏，触发重新加载",
                    )
                    await self._set_state(database, SchemaState.UNLOADED)
            else:
                # State says READY but no data — consistency issue
                await self._set_state(database, SchemaState.UNLOADED)

        if state in (
            SchemaState.UNLOADED,
            SchemaState.LOADING,
            SchemaState.FAILED,
            None,
        ):
            await self._ensure_loading(database)

        raise SchemaNotReadyError(
            f"Schema for {database} is loading",
            retry_after_ms=2000,
        )

    async def _ensure_loading(self, database: str) -> None:
        """Singleflight: ensure at most one loading task per database."""
        async with self._inflight_lock:
            if database in self._inflight:
                task = self._inflight[database]
                if not task.done():
                    return
            self._inflight[database] = asyncio.create_task(
                self._do_load(database)
            )

    async def _do_load(self, database: str) -> None:
        """Load schema from database and store in Redis.

        Updates state machine and persists error details on failure.
        """
        await self._set_state(database, SchemaState.LOADING)
        try:
            schema = await self._discovery.load_schema(database)
            compressed = gzip.compress(
                schema.model_dump_json().encode("utf-8")
            )
            ex = self._settings.schema_refresh_interval or None
            if ex is not None and ex <= 0:
                ex = None
            await self._redis.set(
                f"{self.PREFIX}:schema:{database}",
                compressed,
                ex=ex,
            )
            await self._redis.delete(
                f"{self.PREFIX}:error:{database}"
            )
            await self._set_state(database, SchemaState.READY)
            log.info(
                "schema_loaded",
                database=database,
                table_count=schema.table_count(),
            )
        except Exception as e:
            await self._set_state(database, SchemaState.FAILED)
            await self._redis.set(
                f"{self.PREFIX}:error:{database}",
                str(e),
                ex=3600,
            )
            log.error("schema_load_failed", database=database, error=str(e))
            raise
        finally:
            async with self._inflight_lock:
                self._inflight.pop(database, None)

    async def refresh(self, database: str | None = None) -> RefreshResult:
        """Refresh schema cache for one or all databases.

        Cancels any in-flight loading tasks, resets state to UNLOADED,
        and triggers fresh loads via singleflight.

        Args:
            database: Specific database to refresh, or ``None`` for all.

        Returns:
            ``RefreshResult`` with succeeded and failed database lists.
        """
        targets = [database] if database else list(self._databases)

        # Cancel old tasks and reset state
        for db in targets:
            await self._set_state(db, SchemaState.UNLOADED)
            async with self._inflight_lock:
                old_task = self._inflight.pop(db, None)
            if old_task is not None and not old_task.done():
                old_task.cancel()
                try:
                    await old_task
                except asyncio.CancelledError:
                    pass

        # Trigger fresh loads via singleflight
        tasks = [self._ensure_loading(db) for db in targets]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results based on final state
        succeeded: list[str] = []
        failed: list[dict[str, str]] = []
        for db in targets:
            state = await self._get_state(db)
            if state == SchemaState.READY:
                succeeded.append(db)
            else:
                err = await self._redis.get(
                    f"{self.PREFIX}:error:{db}"
                )
                failed.append(
                    {"database": db, "error": err or "unknown"}
                )

        return RefreshResult(succeeded=succeeded, failed=failed)

    async def warmup_all(self) -> None:
        """Trigger background loading for all discovered databases.

        Non-blocking: fires off loading tasks and returns immediately.
        """
        for db in self._databases:
            await self._ensure_loading(db)

    async def run_periodic_refresh(self) -> None:
        """Run periodic schema refresh in a background loop.

        This coroutine is intended to be run as an asyncio Task.
        It sleeps for ``schema_refresh_interval`` seconds between refreshes.
        """
        interval = self._settings.schema_refresh_interval
        if interval <= 0:
            log.info("periodic_refresh_disabled")
            return

        while True:
            try:
                await asyncio.sleep(interval)
                log.info("periodic_refresh_start")
                result = await self.refresh()
                log.info(
                    "periodic_refresh_complete",
                    succeeded=len(result.succeeded),
                    failed=len(result.failed),
                )
            except asyncio.CancelledError:
                log.info("periodic_refresh_cancelled")
                raise
            except Exception as e:
                log.error("periodic_refresh_error", error=str(e))

    async def _get_state(self, database: str) -> SchemaState | None:
        """Get schema state from Redis.

        Returns ``None`` if no state is recorded.
        """
        raw = await self._redis.get(f"{self.PREFIX}:state:{database}")
        if raw is None:
            return None
        try:
            return SchemaState(raw)
        except ValueError:
            return None

    async def _set_state(
        self, database: str, state: SchemaState
    ) -> None:
        """Persist schema state to Redis."""
        await self._redis.set(
            f"{self.PREFIX}:state:{database}", state.value
        )

    async def close(self) -> None:
        """Cancel any in-flight tasks and clean up."""
        async with self._inflight_lock:
            tasks = list(self._inflight.values())
            self._inflight.clear()

        for task in tasks:
            if not task.done():
                task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
