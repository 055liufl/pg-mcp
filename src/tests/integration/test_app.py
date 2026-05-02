"""Integration tests for the FastAPI SSE app.

Covers:
- ``GET /health`` returns ``{"status": "ok"}``
- ``POST /admin/refresh`` calls ``SchemaCache.refresh()`` and returns
  the structured RefreshResult JSON
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from pg_mcp.app import create_app
from pg_mcp.protocols import RefreshResult

pytestmark = pytest.mark.integration


class _StubCache:
    """Minimal cache double exposing ``refresh`` for the admin endpoint."""

    def __init__(self) -> None:
        self.refresh_calls: list[str | None] = []

    async def refresh(self, database: str | None = None) -> RefreshResult:
        self.refresh_calls.append(database)
        return RefreshResult(
            succeeded=["alpha", "beta"],
            failed=[{"database": "gamma", "error": "boom"}],
        )


@pytest.fixture
def stub_cache() -> _StubCache:
    return _StubCache()


@pytest.fixture
def app_client(stub_cache: _StubCache) -> TestClient:
    server = MagicMock()
    server._server = MagicMock()
    app = create_app(server, stub_cache)  # type: ignore[arg-type]
    return TestClient(app)


def test_health_endpoint_returns_ok(app_client: TestClient) -> None:
    response = app_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_admin_refresh_endpoint_returns_refresh_result(
    app_client: TestClient, stub_cache: _StubCache
) -> None:
    response = app_client.post("/admin/refresh")

    assert response.status_code == 200
    payload = response.json()
    assert payload["succeeded"] == ["alpha", "beta"]
    assert payload["failed"] == [{"database": "gamma", "error": "boom"}]
    # Refresh must have been delegated to the shared cache
    assert stub_cache.refresh_calls == [None]


def test_admin_refresh_only_accepts_post(app_client: TestClient) -> None:
    response = app_client.get("/admin/refresh")

    # Starlette returns 405 for the wrong verb on a registered route
    assert response.status_code == 405
