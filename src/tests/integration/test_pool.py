"""Integration tests for connection pool management.

Covers:
- Pool creation and reuse
- Retry logic with backoff
- PG_DATABASES override for discover_databases
- DSN building
- assert_readonly behavior

Note: These tests mock asyncpg to avoid requiring a real PostgreSQL instance.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.models.errors import DbConnectError


@pytest.fixture
def settings() -> Settings:
    return Settings(
        pg_user="testuser",
        pg_password="testpass",
        pg_host="localhost",
        pg_port=5432,
        pg_databases="",
        db_pool_size=5,
    )


def _make_mock_pool(mock_conn: AsyncMock) -> MagicMock:
    """Create a mock pool with a callable acquire that returns an async context manager."""
    mock_pool = MagicMock()
    mock_acquire_cm = AsyncMock()
    mock_acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire_cm.__aexit__ = AsyncMock(return_value=False)
    mock_pool.acquire = MagicMock(return_value=mock_acquire_cm)
    return mock_pool


class TestPoolCreation:
    """Tests for connection pool lifecycle."""

    @pytest.mark.asyncio
    async def test_get_pool_creates_new_pool(self, settings: Settings) -> None:
        mock_pool = MagicMock()
        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(settings)
            pool = await mgr.get_pool("test_db")

            assert pool is mock_pool

    @pytest.mark.asyncio
    async def test_get_pool_reuses_existing_pool(self, settings: Settings) -> None:
        mock_pool = MagicMock()
        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(settings)
            pool1 = await mgr.get_pool("test_db")
            pool2 = await mgr.get_pool("test_db")

            assert pool1 is pool2
            assert asyncpg.create_pool.call_count == 1  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_get_pool_different_databases(self, settings: Settings) -> None:
        mock_pool_a = MagicMock()
        mock_pool_b = MagicMock()
        call_count = 0

        async def mock_create_pool(dsn: str, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            return mock_pool_a if call_count == 1 else mock_pool_b

        with patch("asyncpg.create_pool", new=mock_create_pool):
            mgr = ConnectionPoolManager(settings)
            pool_a = await mgr.get_pool("db_a")
            pool_b = await mgr.get_pool("db_b")

            assert pool_a is mock_pool_a
            assert pool_b is mock_pool_b
            assert call_count == 2


class TestRetryLogic:
    """Tests for exponential backoff retry on connection failures."""

    @pytest.mark.asyncio
    async def test_retry_on_postgres_error(self, settings: Settings) -> None:
        mock_pool = MagicMock()
        call_count = 0

        async def mock_create_pool(dsn: str, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                import asyncpg

                raise asyncpg.PostgresError("connection refused")
            return mock_pool

        with patch("asyncpg.create_pool", new=mock_create_pool):
            mgr = ConnectionPoolManager(settings)
            pool = await mgr.get_pool("test_db")

            assert pool is mock_pool
            assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_on_os_error(self, settings: Settings) -> None:
        mock_pool = MagicMock()
        call_count = 0

        async def mock_create_pool(dsn: str, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise OSError("Network unreachable")
            return mock_pool

        with patch("asyncpg.create_pool", new=mock_create_pool):
            mgr = ConnectionPoolManager(settings)
            pool = await mgr.get_pool("test_db")

            assert pool is mock_pool
            assert call_count == 2

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_raises_db_connect_error(self, settings: Settings) -> None:
        async def mock_create_pool(dsn: str, **kwargs: object) -> MagicMock:
            import asyncpg

            raise asyncpg.PostgresError("connection refused")

        with patch("asyncpg.create_pool", new=mock_create_pool):
            mgr = ConnectionPoolManager(settings)

            with pytest.raises(DbConnectError) as exc_info:
                await mgr.get_pool("test_db")

            assert "connection refused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_retry_with_backoff_increases_delay(self, settings: Settings) -> None:
        delays: list[float] = []

        async def mock_create_pool(dsn: str, **kwargs: object) -> MagicMock:
            import asyncpg

            raise asyncpg.PostgresError("fail")

        async def tracking_sleep(delay: float) -> None:
            delays.append(delay)

        with (
            patch("asyncpg.create_pool", new=mock_create_pool),
            patch("asyncio.sleep", new=tracking_sleep),
        ):
            mgr = ConnectionPoolManager(settings)
            with pytest.raises(DbConnectError):
                await mgr.get_pool("test_db")

        # Should have increasing delays (with jitter)
        assert len(delays) > 0
        for i in range(1, len(delays)):
            # Delays generally increase (allowing for jitter)
            assert delays[i] >= delays[i - 1] * 0.5


class TestDsnBuilding:
    """Tests for DSN construction."""

    def test_build_dsn_includes_all_components(self, settings: Settings) -> None:
        mgr = ConnectionPoolManager(settings)
        dsn = mgr._build_dsn("mydb")

        assert dsn.startswith("postgresql://")
        assert "testuser" in dsn
        assert "testpass" in dsn
        assert "localhost:5432" in dsn
        assert "mydb" in dsn
        assert "sslmode=prefer" in dsn

    def test_build_dsn_with_sslrootcert(self) -> None:
        settings_with_cert = Settings(
            pg_user="test",
            pg_password="test",
            pg_sslrootcert="/path/to/cert.pem",
        )
        mgr = ConnectionPoolManager(settings_with_cert)
        dsn = mgr._build_dsn("mydb")

        assert "sslrootcert=/path/to/cert.pem" in dsn

    def test_build_dsn_password_not_in_repr(self, settings: Settings) -> None:
        mgr = ConnectionPoolManager(settings)
        repr_str = repr(mgr)

        assert "testpass" not in repr_str


class TestDiscoverDatabases:
    """Tests for database discovery."""

    @pytest.mark.asyncio
    async def test_pg_databases_override_skips_discovery(self, settings: Settings) -> None:
        settings_override = Settings(
            pg_user="test",
            pg_password="test",
            pg_databases="db1,db2,db3",
        )
        mgr = ConnectionPoolManager(settings_override)
        result = await mgr.discover_databases()

        assert result == ["db1", "db2", "db3"]

    @pytest.mark.asyncio
    async def test_discover_databases_queries_postgres(self, settings: Settings) -> None:
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"datname": "db1"},
                {"datname": "db2"},
            ]
        )
        mock_pool = _make_mock_pool(mock_conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(settings)
            result = await mgr.discover_databases()

            assert result == ["db1", "db2"]
            mock_conn.fetch.assert_called_once()


class TestAssertReadonly:
    """Tests for read-only permission assertion."""

    @pytest.mark.asyncio
    async def test_assert_readonly_warns_on_superuser(self, settings: Settings) -> None:
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(
            return_value={
                "rolsuper": True,
                "rolcreaterole": False,
                "rolcreatedb": False,
            }
        )
        mock_conn.fetchval = AsyncMock(return_value=False)
        mock_pool = _make_mock_pool(mock_conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(settings)
            # Should not raise when strict_readonly is False (default)
            await mgr.assert_readonly()

    @pytest.mark.asyncio
    async def test_assert_readonly_raises_on_superuser_when_strict(
        self, settings: Settings
    ) -> None:
        strict_settings = Settings(
            pg_user="test",
            pg_password="test",
            strict_readonly=True,
        )
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(
            return_value={
                "rolsuper": True,
                "rolcreaterole": False,
                "rolcreatedb": False,
            }
        )
        mock_pool = _make_mock_pool(mock_conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(strict_settings)

            with pytest.raises(RuntimeError) as exc_info:
                await mgr.assert_readonly()

            assert "STRICT_READONLY" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_assert_readonly_raises_on_write_permissions_when_strict(
        self, settings: Settings
    ) -> None:
        strict_settings = Settings(
            pg_user="test",
            pg_password="test",
            strict_readonly=True,
        )
        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(
            return_value={
                "rolsuper": False,
                "rolcreaterole": False,
                "rolcreatedb": False,
            }
        )
        mock_conn.fetchval = AsyncMock(return_value=True)
        mock_pool = _make_mock_pool(mock_conn)

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(strict_settings)

            with pytest.raises(RuntimeError) as exc_info:
                await mgr.assert_readonly()

            assert "strict_readonly" in str(exc_info.value).lower()


class TestCloseAll:
    """Tests for pool cleanup."""

    @pytest.mark.asyncio
    async def test_close_all_closes_pools(self, settings: Settings) -> None:
        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()

        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            mgr = ConnectionPoolManager(settings)
            await mgr.get_pool("test_db")
            await mgr.close_all()

            mock_pool.close.assert_called_once()
            assert len(mgr._pools) == 0
