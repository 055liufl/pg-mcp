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
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from pg_mcp.config import Settings
from pg_mcp.models.errors import SchemaNotReadyError
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.schema.cache import SchemaCache

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
        loaded_at=datetime.now(UTC),
    )


@pytest_asyncio.fixture
async def cache(mock_redis: AsyncMock, settings: Settings) -> AsyncGenerator[SchemaCache, None]:
    from pg_mcp.db.pool import ConnectionPoolManager

    pool_mgr = ConnectionPoolManager(settings)
    cache = SchemaCache(mock_redis, pool_mgr, settings)
    yield cache
    await cache.close()


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

        with patch.object(cache._discovery, "load_schema", new=slow_load):
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
        compressed = gzip.compress(sample_schema.model_dump_json().encode("utf-8"))

        # First call: state=READY, data present
        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                return "ready"
            if "schema" in key:
                return compressed
            return None

        mock_redis.get.side_effect = mock_get
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

            # Wait for background task to start
            await asyncio.sleep(0.05)

            assert load_called is True

    @pytest.mark.asyncio
    async def test_loading_state_persists_until_complete(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        states: list[str | None] = [None]

        async def track_state(key: str) -> bytes | None:
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

            # Wait for background task to start
            await asyncio.sleep(0.05)


class TestRefresh:
    """Tests for schema refresh."""

    @pytest.mark.asyncio
    async def test_refresh_resets_state_and_triggers_reload(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        compressed = gzip.compress(sample_schema.model_dump_json().encode("utf-8"))

        # Start with READY state and cached data
        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                return "ready"
            if "schema" in key:
                return compressed
            return None

        mock_redis.get.side_effect = mock_get
        cache.set_discovered_databases(["test_db"])

        # First, get schema successfully
        result = await cache.get_schema("test_db")
        assert result.database == "test_db"

        # Now refresh
        await cache.refresh("test_db")

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

            # Wait for background task to start
            await asyncio.sleep(0.05)

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
        state_log: list[str] = []

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

            # Wait for background task to start
            await asyncio.sleep(0.05)

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
        state_log: list[str] = []

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

            # Wait for background task to start
            await asyncio.sleep(0.05)

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

            # Wait for background task to complete
            await asyncio.sleep(0.1)

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

            # Wait for background task to complete
            await asyncio.sleep(0.1)

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

        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                return b"ready"
            if "schema" in key:
                return b"not-valid-gzip-data"
            return None

        mock_redis.get.side_effect = mock_get

        async def mock_load(database: str) -> DatabaseSchema:
            nonlocal load_count
            load_count += 1
            return sample_schema

        with patch.object(cache._discovery, "load_schema", new=mock_load):
            cache.set_discovered_databases(["test_db"])

            with pytest.raises(SchemaNotReadyError):
                await cache.get_schema("test_db")

            # Wait for background task to complete
            await asyncio.sleep(0.1)

        # Corrupted blob should reset state and trigger a reload
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

            # Wait for background task to start
            await asyncio.sleep(0.05)

            await cache.close()

            await asyncio.sleep(0.05)
            assert cancel_detected is True


class TestRedisBytesRegression:
    """Regressions for Redis returning bytes vs str values."""

    @pytest.mark.asyncio
    async def test_get_state_decodes_bytes_responses(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        """SchemaState lookup must succeed when Redis returns bytes."""
        compressed = gzip.compress(sample_schema.model_dump_json().encode("utf-8"))

        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                return b"ready"  # canonical bytes payload from redis-py
            if "schema" in key:
                return compressed
            return None

        mock_redis.get.side_effect = mock_get
        cache.set_discovered_databases(["test_db"])

        result = await cache.get_schema("test_db")

        assert result.database == "test_db"

    @pytest.mark.asyncio
    async def test_refresh_reports_bytes_error_value(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
    ) -> None:
        """Errors stored as bytes should be decoded into the RefreshResult."""

        # Mock state lookup to always return FAILED so refresh observes it
        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                return b"failed"
            if "error" in key:
                return b"connection refused"
            return None

        mock_redis.get.side_effect = mock_get

        async def failing_load(database: str) -> DatabaseSchema:
            raise RuntimeError("connection refused")

        with patch.object(cache._discovery, "load_schema", new=failing_load):
            cache.set_discovered_databases(["test_db"])

            result = await cache.refresh("test_db")

        assert result.failed
        assert result.failed[0]["database"] == "test_db"
        # Error string must be decoded, not a repr like ``b'...'``.
        assert "connection refused" in result.failed[0]["error"]
        assert "b'" not in result.failed[0]["error"]


class TestRefreshCompletion:
    """Regression: refresh() must wait for the actual loads to settle."""

    @pytest.mark.asyncio
    async def test_refresh_awaits_load_to_completion(
        self,
        cache: SchemaCache,
        mock_redis: AsyncMock,
        sample_schema: DatabaseSchema,
    ) -> None:
        """refresh() returns only after each load task has finished."""
        load_started = asyncio.Event()
        load_completed = False

        async def slow_load(database: str) -> DatabaseSchema:
            nonlocal load_completed
            load_started.set()
            await asyncio.sleep(0.1)
            load_completed = True
            return sample_schema

        # Track state writes so refresh sees the right transitions
        state_store: dict[str, str] = {}

        async def mock_set(key: str, value: bytes | str, **_: object) -> bool:
            if "state" in key:
                state_store[key] = value.decode() if isinstance(value, bytes) else value
            return True

        async def mock_get(key: str) -> bytes | None:
            if "state" in key:
                state_val = state_store.get(key)
                return state_val.encode() if state_val else None
            return None

        mock_redis.set = mock_set
        mock_redis.get.side_effect = mock_get

        with patch.object(cache._discovery, "load_schema", new=slow_load):
            cache.set_discovered_databases(["test_db"])
            result = await cache.refresh("test_db")

        # refresh() must not return until the load body has actually run.
        assert load_started.is_set()
        assert load_completed is True
        assert "test_db" in result.succeeded
