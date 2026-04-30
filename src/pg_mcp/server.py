"""MCP Server layer: tool registration, error translation, and transport runners."""

from mcp.server import Server
from mcp.server.models import INVALID_PARAMS
from mcp.types import TextContent, Tool
from mcp import McpError
from pydantic import ValidationError

from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.models.errors import PgMcpError
from pg_mcp.models.request import QueryRequest
from pg_mcp.models.response import ErrorDetail, QueryResponse


class PgMcpServer:
    """MCP Server for pg-mcp with tool registration and error conversion.

    Wraps the lower-level ``mcp.server.Server`` and exposes a single ``query``
    tool that routes natural-language requests through ``QueryEngine``.

    Protocol-level errors (bad arguments, unknown tools) are raised as
    ``McpError`` so the transport can serialize them per the MCP spec.
    Business-level errors (SQL unsafe, DB not found, etc.) are caught and
    converted into a ``QueryResponse`` with the ``error`` field populated,
    which is then returned as a ``TextContent`` JSON payload.
    """

    def __init__(self, query_engine: QueryEngine) -> None:
        """Initialize the server with a ``QueryEngine`` instance.

        Args:
            query_engine: The orchestrator that executes query requests.
        """
        self._engine = query_engine
        self._server = Server("pg-mcp")
        self._setup_tools()

    def _setup_tools(self) -> None:
        """Register MCP tools and their handlers."""

        @self._server.list_tools()
        async def list_tools() -> list[Tool]:
            """Return the list of available tools."""
            return [
                Tool(
                    name="query",
                    description=(
                        "Execute natural language queries against PostgreSQL databases. "
                        "Returns generated SQL and optionally query results."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": (
                                    "Natural language query (required unless admin_action is set)"
                                ),
                            },
                            "database": {
                                "type": "string",
                                "description": "Target database name",
                            },
                            "return_type": {
                                "type": "string",
                                "enum": ["sql", "result"],
                                "description": (
                                    "'sql' returns only the generated SQL; "
                                    "'result' executes and returns rows"
                                ),
                            },
                            "admin_action": {
                                "type": "string",
                                "enum": ["refresh_schema"],
                                "description": "Administrative action to perform",
                            },
                        },
                        "required": [],
                    },
                )
            ]

        @self._server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[TextContent]:
            """Handle a tool invocation from the MCP client.

            Args:
                name: The tool name (must be ``query``).
                arguments: JSON-object arguments passed by the client.

            Returns:
                A list containing a single ``TextContent`` with the JSON
                serialized ``QueryResponse``.

            Raises:
                McpError: For protocol-level errors (unknown tool, invalid params).
            """
            if name != "query":
                raise McpError(INVALID_PARAMS, f"Unknown tool: {name}")

            try:
                request = QueryRequest(**arguments)
            except ValidationError as exc:
                raise McpError(INVALID_PARAMS, str(exc)) from exc

            try:
                response = await self._engine.execute(request)
            except PgMcpError as exc:
                response = QueryResponse(
                    error=ErrorDetail(
                        code=exc.code.value,
                        message=str(exc),
                        retry_after_ms=exc.retry_after_ms,
                        candidates=exc.candidates,
                    )
                )

            return [TextContent(type="text", text=response.model_dump_json())]

    async def run_stdio(self) -> None:
        """Run the server over stdio transport.

        This is the default mode for Claude Desktop and similar MCP clients.
        """
        from mcp.server.stdio import stdio_server

        async with stdio_server() as (read_stream, write_stream):
            await self._server.run(
                read_stream,
                write_stream,
                self._server.create_initialization_options(),
            )

    async def run_sse(self, host: str, port: int) -> None:
        """Run the server over SSE transport.

        In practice the SSE transport is handled by ``app.py`` via FastAPI.
        This method exists for symmetry and potential future direct use.

        Args:
            host: Bind host.
            port: Bind port.
        """
        # SSE transport is managed by FastAPI in app.py.
        # This method is intentionally a no-op; the CLI wires SSE through
        # ``create_app`` instead.
        pass
