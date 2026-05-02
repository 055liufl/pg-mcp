"""MCP Server layer: tool registration, error translation, and transport runners."""

from typing import Any

from mcp import McpError
from mcp.server import Server
from mcp.types import INVALID_PARAMS, ErrorData, TextContent, Tool
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

        @self._server.list_tools()  # type: ignore[untyped-decorator, no-untyped-call]
        async def list_tools() -> list[Tool]:
            """Return the list of available tools."""
            return [
                Tool(
                    name="query",
                    description=(
                        "对 PostgreSQL 数据库执行自然语言查询。返回生成的 SQL 以及可选的查询结果。"
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": ("自然语言查询（未设置 admin_action 时必填）"),
                            },
                            "database": {
                                "type": "string",
                                "description": "目标数据库名称",
                            },
                            "return_type": {
                                "type": "string",
                                "enum": ["sql", "result"],
                                "description": (
                                    "'sql' 仅返回生成的 SQL；'result' 执行并返回结果行"
                                ),
                            },
                            "admin_action": {
                                "type": "string",
                                "enum": ["refresh_schema"],
                                "description": "要执行的管理操作",
                            },
                        },
                        "required": [],
                    },
                )
            ]

        @self._server.call_tool()  # type: ignore[untyped-decorator]
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
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
                raise McpError(ErrorData(code=INVALID_PARAMS, message=f"未知工具: {name}"))

            try:
                request = QueryRequest(**arguments)
            except ValidationError as exc:
                raise McpError(ErrorData(code=INVALID_PARAMS, message=str(exc))) from exc

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
