"""Click CLI with unified async lifecycle for pg-mcp."""

import asyncio

import click
import structlog

from pg_mcp.config import Settings
from pg_mcp.db.pool import ConnectionPoolManager
from pg_mcp.engine.db_inference import DbInference
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.engine.result_validator import ResultValidator
from pg_mcp.engine.sql_executor import SqlExecutor
from pg_mcp.engine.sql_generator import SqlGenerator
from pg_mcp.engine.sql_rewriter import SqlRewriter
from pg_mcp.engine.sql_validator import SqlValidator
from pg_mcp.observability.logging import configure_logging
from pg_mcp.schema.cache import SchemaCache
from pg_mcp.schema.retriever import SchemaRetriever
from pg_mcp.server import PgMcpServer


@click.command()
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    show_default=True,
    help="MCP transport protocol.",
)
def main(transport: str) -> None:
    """Start the pg-mcp server.

    Supports stdio (default) for Claude Desktop and SSE for HTTP-based
    MCP clients.
    """
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)
    asyncio.run(_run_server(transport, settings))


async def _run_server(transport: str, settings: Settings) -> None:
    """Unified async lifecycle: create, run, and tear down all resources.

    All async objects (connection pools, Redis client, background tasks)
    are created and destroyed within the same event loop to avoid
    ``asyncio.run`` re-entrancy issues and ensure clean shutdown.

    Args:
        transport: ``"stdio"`` or ``"sse"``.
        settings: Application configuration.
    """
    log = structlog.get_logger()
    pool_mgr = ConnectionPoolManager(settings)

    import redis.asyncio as redis

    redis_client = redis.from_url(settings.redis_url)
    cache = SchemaCache(redis_client, pool_mgr, settings)
    retriever = SchemaRetriever(
        max_tables_for_full=settings.schema_max_tables_for_full_context
    )

    # Track background tasks so we can cancel + await them on shutdown.
    bg_tasks: set[asyncio.Task] = set()

    try:
        # 1. Discover databases (PG_DATABASES overrides auto-discovery).
        databases = await pool_mgr.discover_databases()
        cache.set_discovered_databases(databases)
        log.info(
            "databases_discovered",
            count=len(databases),
            databases=databases,
        )

        # 2. Read-only sanity check (non-blocking; warns or exits).
        await pool_mgr.assert_readonly()

        # 3. Build engine components.
        from openai import AsyncOpenAI

        openai_client = AsyncOpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            base_url=settings.openai_base_url,
        )

        sql_generator = SqlGenerator(openai_client, settings)
        sql_rewriter = SqlRewriter()
        sql_validator = SqlValidator()
        sql_executor = SqlExecutor(pool_mgr, settings)
        db_inference = DbInference(cache, settings)
        result_validator = ResultValidator(openai_client, settings)

        # Wire schema-load observers so that downstream caches (DB
        # inference summaries, retrieval indices) are rebuilt on each
        # successful schema load and dropped on invalidation/refresh.
        cache.add_loaded_hook(
            lambda db, schema: db_inference.build_summary(schema)
        )
        cache.add_loaded_hook(
            lambda db, schema: retriever.install_index(db, schema)
        )
        cache.add_invalidated_hook(db_inference.remove_summary)
        cache.add_invalidated_hook(retriever.invalidate_index)

        engine = QueryEngine(
            sql_generator=sql_generator,
            sql_rewriter=sql_rewriter,
            sql_validator=sql_validator,
            sql_executor=sql_executor,
            schema_cache=cache,
            db_inference=db_inference,
            result_validator=result_validator,
            retriever=retriever,
            settings=settings,
        )
        server = PgMcpServer(engine)

        # 4. Background warmup.
        t = asyncio.create_task(cache.warmup_all())
        bg_tasks.add(t)
        t.add_done_callback(bg_tasks.discard)

        # 5. Periodic refresh task.
        if settings.schema_refresh_interval > 0:
            t = asyncio.create_task(_periodic_refresh(cache, settings.schema_refresh_interval))
            bg_tasks.add(t)
            t.add_done_callback(bg_tasks.discard)

        # 6. Start transport.
        if transport == "stdio":
            log.info("starting_stdio_transport")
            await server.run_stdio()
        else:
            log.info("starting_sse_transport")
            await _run_sse(server, cache, settings)

    finally:
        # Cancel background tasks and wait for graceful termination.
        for task in bg_tasks:
            if not task.done():
                task.cancel()
        if bg_tasks:
            await asyncio.gather(*bg_tasks, return_exceptions=True)

        # Close long-lived resources.
        await pool_mgr.close_all()
        await redis_client.aclose()
        log.info("shutdown_complete")


async def _run_sse(server: PgMcpServer, cache: SchemaCache, settings: Settings) -> None:
    """Run the SSE transport via Uvicorn.

    Args:
        server: Initialized ``PgMcpServer``.
        cache: Initialized ``SchemaCache``.
        settings: Application configuration.
    """
    import uvicorn

    from pg_mcp.app import create_app

    app = create_app(server, cache)
    config = uvicorn.Config(
        app,
        host=settings.sse_host,
        port=settings.sse_port,
        log_level=settings.log_level.lower(),
    )
    uvicorn_server = uvicorn.Server(config)
    await uvicorn_server.serve()


async def _periodic_refresh(cache: SchemaCache, interval: int) -> None:
    """Background coroutine that refreshes all schemas on a timer.

    Args:
        cache: The schema cache instance.
        interval: Refresh interval in seconds.
    """
    log = structlog.get_logger()
    while True:
        try:
            await asyncio.sleep(interval)
            result = await cache.refresh()
            log.info(
                "periodic_refresh_complete",
                succeeded=len(result.succeeded),
                failed=len(result.failed),
            )
        except asyncio.CancelledError:
            log.debug("periodic_refresh_cancelled")
            raise
        except OSError as e:
            log.error("periodic_refresh_failed", error=str(e))
        except Exception:
            log.exception("periodic_refresh_failed")
