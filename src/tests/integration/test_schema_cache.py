"""Integration tests for schema cache.

Covers:
- Singleflight: at most one loading task per database
- Cache hit returns schema immediately
- Cache miss triggers loading
- Refresh cancels old tasks and resets state
- State machine transitions
- Gzip compression/decompression
- Error persistence

Note: These tests mock Redis and SchemaDiscovery to avoid external dependencies.
"""

from __future__ import annotations

import asyncio
import gzip
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config import Settings
from pg_mcp.models.errors import SchemaNotReadyError
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.schema.cache import SchemaCache
from pg_mcp.schema.state import SchemaState

pytestmark = pytest.mark.integration


@pytest.fixture
def settings() -> Settings:
    return Settings(
        pg_user="test",
        pg_password="test",
        schema_refresh_interval=600,
    )


@pytest.fixture
def mock_redis() -> AsyncMock:
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    return client


@pytest.fixture
def sample_schema() -> DatabaseSchema:
    return DatabaseSchema(
        database="test_db",
        tables=[
            TableInfo(
                schema_name="public",
                table_name="users",
                columns=[ColumnInfo(name="id", type="integer", nullable=False)],
            ),
        ],
        loaded_at=datetime.now(timezone.utc),
    )


@pytest.fixture
def cache(mock_redis: AsyncMock, settings: Settings) -> SchemaCache:
    from pg_mcp.db.pool import ConnectionPoolManager

    pool_mgr = ConnectionPoolManager(settings)
    return SchemaCache(mock_redis, pool_mgr, settings)


class TestSingleflight:
    """Tests for singleflight loading behavior."""

    @pytest.mark.asyncio
    async def test_concurrent_get_schema_triggers_single_load(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        load_count = 0

        async def slow_load(database: str) -> DatabaseSchema:
            nonlocal load_count
            load_count += 1
            await asyncio.sleep(0.1)
            return sample_schema

        with patch.object(
            cache._discovery, "load_schema", new=slow_load
        ):
            cache.set_discovered_databases(["test_db"])
            # Fire two concurrent requests
            tasks = [
                asyncio.create_task(cache.get_schema("test_db")),
                asyncio.create_task(cache.get_schema("test_db")),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Both should get SchemaNotReadyError since loading takes time
            assert all(isinstance(r, SchemaNotReadyError) for r in results)
            # But load_schema should only be called once
            assert load_count == 1

    @pytest.mark.asyncio
    async def test_get_schema_after_ready_returns_schema(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        compressed = gzip.compress(
            sample_schema.model_dump_json().encode("utf-8")
        )
        # First call: state=READY, data present
        async def mock_get(key: str) -> Optional[bytes]:
            if "state" in key:
                return b"ready"
            if "schema" in key:
                return compressed
            return None

        mock_redis.get = mock_get
        cache.set_discovered_databases(["test_db"])

        result = await cache.get_schema("test_db")

        assert result.database == "test_db"
        assert result.table_count() == 1


class TestCacheMiss:
    """Tests for cache miss behavior."""

    @pytest.mark.asyncio
    async def test_cache_miss_raises_not_ready_and_triggers_load(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        mock_redis.get = AsyncMock(return_value=None)
        load_called = False

        async def mock_load(database: str) -> DatabaseSchema:
            nonlocal load_called
            load_called = True
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            assert load_called is True

    @pytest.mark.asyncio
    async def test_loading_state_persists_until_complete(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        states: list[Optional[str]] = [None]

        async def track_state(key: str) -> Optional[bytes]:
            if "state" in key:
                return states[0].encode() if states[0] else None
            return None

        mock_redis.get = track_state

        async def slow_load(database: str) -> DatabaseSchema:
            states[0] = "loading"
            await asyncio.sleep(0.05)
            states[0] = "ready"
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=slow_load):
            cache.set_discovered_databases(["test_db"])

            # First call triggers load
            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")


class TestRefresh:
    """Tests for schema refresh."""

    @pytest.mark.asyncio
    async def test_refresh_resets_state_and_triggers_reload(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        compressed = gzip.compress(
            sample_schema.model_dump_json().encode("utf-8")
        )
        # Start with READY state and cached data
        async def mock_get(key: str) -> Optional[bytes]:
            if "state" in key:
                return b"ready"
            if "schema" in key:
                return compressed
            return None

        mock_redis.get = mock_get
        cache.set_discovered_databases(["test_db"])

        # First, get schema successfully
        result = await cache.get_schema("test_db")
        assert result.database == "test_db"

        # Now refresh
        refresh_result = await cache.refresh("test_db")

        # State should be reset to UNLOADED and then re-triggered
        assert mock_redis.set.call_count >= 1

    @pytest.mark.asyncio
    async def test_refresh_cancels_inflight_task(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        cancel_detected = False

        async def slow_load(database: str) -> DatabaseSchema:
            try:
                await asyncio.sleep(10)
                return sample_schema
            except asyncio.CancelledError:
                nonlocal cancel_detected
                cancel_detected = True
                raise

        with patch.object(cache._discovery, "load_schema", new=slow_load):
            cache.set_discovered_databases(["test_db"])
            # Trigger a load
            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            # Refresh should cancel the in-flight task
            await cache.refresh("test_db")

            # Give a moment for cancellation to propagate
            await asyncio.sleep(0.05)
            assert cancel_detected is True

    @pytest.mark.asyncio
    async def test_refresh_all_databases(self, cache: SchemaCache) -> None:
        cache.set_discovered_databases(["db1", "db2"])

        result = await cache.refresh()

        # Should attempt to refresh all databases
        assert result is not None


class TestStateMachine:
    """Tests for schema state transitions."""

    @pytest.mark.asyncio
    async def test_state_unloaded_to_loading_to_ready(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        state_log: list[Optional[str]] = []

        async def track_set(key: str, value: bytes | str, **kwargs: object) -> bool:
            if "state" in key:
                state_log.append(value if isinstance(value, str) else value.decode())
            return True

        mock_redis.set = track_set

        async def mock_load(database: str) -> DatabaseSchema:
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            # Wait for the background load to complete
            await asyncio.sleep(0.05)

        assert "loading" in state_log
        assert "ready" in state_log

    @pytest.mark.asyncio
    async def test_failed_load_sets_failed_state(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
    ) -> None:
        state_log: list[Optional[str]] = []

        async def track_set(key: str, value: bytes | str, **kwargs: object) -> bool:
            if "state" in key:
                state_log.append(value if isinstance(value, str) else value.decode())
            return True

        mock_redis.set = track_set

        async def failing_load(database: str) -> DatabaseSchema:
            raise RuntimeError("Connection failed")

        with patch.object(cache._discovery, "load_schema", new=failing_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            # Wait for the background load to complete
            await asyncio.sleep(0.05)

        assert "loading" in state_log
        assert "failed" in state_log

    @pytest.mark.asyncio
    async def test_error_persisted_on_failure(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
    ) -> None:
        error_messages: list[str] = []

        async def track_error_set(key: str, value: str, **kwargs: object) -> bool:
            if "error" in key:
                error_messages.append(value)
            return True

        mock_redis.set = track_error_set

        async def failing_load(database: str) -> DatabaseSchema:
            raise RuntimeError("Connection failed")

        with patch.object(cache._discovery, "load_schema", new=failing_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            await asyncio.sleep(0.05)

        assert any("Connection failed" in msg for msg in error_messages)


class TestCompression:
    """Tests for gzip compression of cached schemas."""

    @pytest.mark.asyncio
    async def test_schema_compressed_before_storage(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        stored_data: bytes = b""

        async def capture_set(key: str, value: bytes, **kwargs: object) -> bool:
            nonlocal stored_data
            if "schema" in key:
                stored_data = value
            return True

        mock_redis.set = capture_set

        async def mock_load(database: str) -> DatabaseSchema:
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            await asyncio.sleep(0.05)

        assert len(stored_data) > 0
        # Should be valid gzip
        decompressed = gzip.decompress(stored_data)
        parsed = DatabaseSchema.model_validate_json(decompressed)
        assert parsed.database == "test_db"

    @pytest.mark.asyncio
    async def test_corrupted_cache_triggers_reload(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        load_count = 0

        async def mock_get(key: str) -> Optional[bytes]:
            if "state" in key:
                return b"ready"
            if "schema" in key:
                return b"not-valid-gzip-data"
            return None

        mock_redis.get = mock_get

        async def mock_load(database: str) -> DatabaseSchema:
            nonlocal load_count
            load_count += 1
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            await asyncio.sleep(0.05)

        assert load_count == 1


class TestWarmup:
    """Tests for warmup_all."""

    @pytest.mark.asyncio
    async def test_warmup_all_triggers_loading_for_all(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        loaded_dbs: list[str] = []

        async def mock_load(database: str) -> DatabaseSchema:
            loaded_dbs.append(database)
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["db1", "db2", "db3"])
            await cache.warmup_all()

            await asyncio.sleep(0.05)

        assert set(loaded_dbs) == {"db1", "db2", "db3"}


class TestClose:
    """Tests for cache cleanup."""

    @pytest.mark.asyncio
    async def test_close_cancels_inflight_tasks(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        cancel_detected = False

        async def slow_load(database: str) -> DatabaseSchema:
            try:
                await asyncio.sleep(10)
                return sample_schema
            except asyncio.CancelledError:
                nonlocal cancel_detected
                cancel_detected = True
                raise

        with patch.object(cache._discovery, "load_schema", new=slow_load):
            cache.set_discovered_databases(["test_db"])
            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            await cache.close()

            await asyncio.sleep(0.05)
            assert cancel_detected is True
