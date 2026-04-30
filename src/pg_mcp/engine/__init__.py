"""Core engine layer for pg-mcp.

Provides the natural-language-to-SQL pipeline components:
- QueryEngine: main orchestrator
- SqlValidator: AST-level safety validation
- SqlGenerator: LLM-based SQL generation
- SqlExecutor: read-only SQL execution
- DbInference: database auto-selection
- ResultValidator: AI result validation
"""

from pg_mcp.engine.db_inference import DbInference, DbSummary
from pg_mcp.engine.orchestrator import QueryEngine
from pg_mcp.engine.result_validator import ResultValidator
from pg_mcp.engine.sql_executor import SqlExecutor
from pg_mcp.engine.sql_generator import SqlGenerator
from pg_mcp.engine.sql_validator import SqlValidator

__all__ = [
    "DbInference",
    "DbSummary",
    "QueryEngine",
    "ResultValidator",
    "SqlExecutor",
    "SqlGenerator",
    "SqlValidator",
]
