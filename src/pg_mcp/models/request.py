"""Request data models for pg-mcp queries.

The :class:`QueryRequest` model validates incoming MCP tool arguments.
It supports both natural-language queries and administrative actions such
as schema refresh.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class QueryRequest(BaseModel):
    """Incoming query request from an MCP client.

    Either *query* or *admin_action* must be provided.  When *admin_action*
    is set, *query* may be empty.  Otherwise *query* must contain non-empty
    text after stripping whitespace.
    """

    query: str = Field(default="", min_length=0, max_length=2000)
    database: str | None = None
    return_type: Literal["sql", "result"] = "result"
    admin_action: Literal["refresh_schema"] | None = None

    @model_validator(mode="after")
    def _check_query_or_admin(self) -> QueryRequest:
        """Ensure query is present unless an admin action is requested."""
        if not self.admin_action:
            stripped = self.query.strip()
            if not stripped:
                raise ValueError("query is required when admin_action is not set")
            self.query = stripped
        return self
