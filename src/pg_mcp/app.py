"""FastAPI application for SSE transport and health checks."""

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import Response
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount, Route

from pg_mcp.schema.cache import SchemaCache
from pg_mcp.server import PgMcpServer


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:  # noqa: ARG001
    """FastAPI lifespan: startup/shutdown hooks.

    All long-lived resources (connection pools, Redis, schema cache) are
    created and destroyed in ``cli._run_server`` to ensure a single event
    loop owns every async object. The lifespan context therefore does not
    perform additional init/cleanup, avoiding double-close races.
    """
    yield


def create_app(server: PgMcpServer, cache: SchemaCache) -> FastAPI:
    """Create a FastAPI application with SSE transport and health endpoint.

    The MCP SSE transport is mounted directly via Starlette ``Route`` and
    ``Mount`` to avoid interference with FastAPI's request/response
    validation and serialization flows.

    Routes:
        - ``GET /health``  -> JSON status response.
        - ``GET /sse``     -> SSE connection endpoint for MCP sessions.
        - ``POST /messages`` -> MCP message POST handler (mounted from
          ``SseServerTransport``).

    Args:
        server: The initialized ``PgMcpServer`` instance.
        cache: The ``SchemaCache`` instance (retained for future admin
            endpoints and to keep the reference alive).

    Returns:
        A configured ``FastAPI`` application.
    """
    sse_transport = SseServerTransport("/messages")

    async def handle_sse(request: Request) -> Response:
        """Establish an SSE connection and run the MCP server session.

        Args:
            request: The incoming HTTP request.

        Returns:
            An empty 200 response after the SSE session ends.
        """
        async with sse_transport.connect_sse(
            request.scope,
            request.receive,
            request._send,  # type: ignore[attr-defined]
        ) as (read_stream, write_stream):
            await server._server.run(
                read_stream,
                write_stream,
                server._server.create_initialization_options(),
            )
        return Response(status_code=200)

    async def handle_refresh(request: Request) -> Response:
        """Trigger schema refresh and return the result.

        Args:
            request: The incoming HTTP request.

        Returns:
            JSON response with succeeded/failed database lists.
        """
        result = await cache.refresh()
        return Response(
            content=result.model_dump_json(),
            media_type="application/json",
            status_code=200,
        )

    routes: list[Route | Mount] = [
        Route(
            "/health",
            endpoint=lambda _: Response(
                '{"status":"ok"}',
                media_type="application/json",
            ),
        ),
        Route("/sse", endpoint=handle_sse),
        Route("/admin/refresh", endpoint=handle_refresh, methods=["POST"]),
        Mount("/messages", app=sse_transport.handle_post_message),
    ]

    app = FastAPI(
        title="pg-mcp",
        lifespan=lifespan,
        routes=routes,  # type: ignore[arg-type]
    )

    # Keep cache reference alive on the app instance for potential
    # future admin endpoints (e.g. /admin/refresh).
    app.state.cache = cache  # type: ignore[attr-defined]

    return app
