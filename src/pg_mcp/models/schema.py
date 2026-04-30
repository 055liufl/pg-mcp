"""Schema data models for PostgreSQL database metadata.

These Pydantic models represent the full structure of a PostgreSQL database
schema, including tables, views, indexes, foreign keys, constraints, and
custom types.  The :class:`DatabaseSchema` root model provides methods to
serialize itself into formats suitable for LLM prompts or lightweight
inference.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ColumnInfo(BaseModel):
    """Metadata for a single table or view column."""

    name: str
    type: str
    nullable: bool
    default: str | None = None
    comment: str | None = None
    is_primary_key: bool = False


class TableInfo(BaseModel):
    """Metadata for a single table, including its columns."""

    schema_name: str
    table_name: str
    columns: list[ColumnInfo]
    comment: str | None = None
    is_foreign: bool = False


class ViewInfo(BaseModel):
    """Metadata for a view or materialized view."""

    schema_name: str
    view_name: str
    columns: list[ColumnInfo]
    definition: str | None = None
    is_materialized: bool = False


class IndexInfo(BaseModel):
    """Metadata for a database index."""

    schema_name: str
    table_name: str
    index_name: str
    columns: list[str]
    index_type: str
    is_unique: bool


class ForeignKeyInfo(BaseModel):
    """Metadata for a foreign-key constraint."""

    constraint_name: str
    source_schema: str
    source_table: str
    source_columns: list[str]
    target_schema: str
    target_table: str
    target_columns: list[str]


class ConstraintInfo(BaseModel):
    """Metadata for a table constraint (CHECK, UNIQUE, EXCLUSION)."""

    schema_name: str
    table_name: str
    constraint_name: str
    constraint_type: str
    definition: str


class EnumTypeInfo(BaseModel):
    """Metadata for a PostgreSQL enum type."""

    schema_name: str
    type_name: str
    values: list[str]


class CompositeTypeInfo(BaseModel):
    """Metadata for a PostgreSQL composite type."""

    schema_name: str
    type_name: str
    attributes: list[ColumnInfo]


class DatabaseSchema(BaseModel):
    """Complete schema metadata for a single PostgreSQL database.

    This model is the root container returned by :class:`SchemaDiscovery`
    and cached by :class:`SchemaCache`.  It can be serialized to a
    human-readable text format for LLM prompts, or to a compressed summary
    for lightweight database inference.
    """

    database: str
    tables: list[TableInfo] = Field(default_factory=list)
    views: list[ViewInfo] = Field(default_factory=list)
    indexes: list[IndexInfo] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list)
    constraints: list[ConstraintInfo] = Field(default_factory=list)
    enum_types: list[EnumTypeInfo] = Field(default_factory=list)
    composite_types: list[CompositeTypeInfo] = Field(default_factory=list)
    allowed_functions: set[str] = Field(default_factory=set)
    loaded_at: datetime = Field(default_factory=datetime.utcnow)

    def table_count(self) -> int:
        """Return the number of tables in the schema."""
        return len(self.tables)

    def foreign_table_ids(self) -> set[str]:
        """Return a set of foreign table identifiers in ``schema.table`` form."""
        return {
            f"{t.schema_name}.{t.table_name}"
            for t in self.tables
            if t.is_foreign
        }

    def to_prompt_text(self) -> str:
        """Serialize the schema into a plain-text format suitable for LLM prompts.

        The output includes:
        - Each table with columns, types, PK markers, and comments.
        - Foreign-key relationships between tables.
        - Enum types defined in the database.
        """
        lines: list[str] = []

        # Tables
        for table in self.tables:
            col_parts: list[str] = []
            for col in table.columns:
                parts = [f"{col.name}: {col.type}"]
                if col.is_primary_key:
                    parts.append("[PK]")
                if col.comment:
                    parts.append(f"# {col.comment}")
                col_parts.append(" ".join(parts))
            table_line = f"{table.schema_name}.{table.table_name}(" + ", ".join(col_parts) + ")"
            if table.comment:
                table_line += f" # {table.comment}"
            lines.append(table_line)

        # Views
        for view in self.views:
            col_parts = [f"{col.name}: {col.type}" for col in view.columns]
            view_line = (
                f"{view.schema_name}.{view.view_name}("
                + ", ".join(col_parts)
                + ")"
            )
            if view.is_materialized:
                view_line += " [MATERIALIZED VIEW]"
            else:
                view_line += " [VIEW]"
            lines.append(view_line)

        # Foreign keys
        if self.foreign_keys:
            lines.append("")
            lines.append("# Foreign Keys")
            for fk in self.foreign_keys:
                lines.append(
                    f"{fk.source_schema}.{fk.source_table}"
                    f"({','.join(fk.source_columns)})"
                    f" -> {fk.target_schema}.{fk.target_table}"
                    f"({','.join(fk.target_columns)})"
                )

        # Enum types
        if self.enum_types:
            lines.append("")
            lines.append("# Enum Types")
            for enum in self.enum_types:
                lines.append(f"{enum.schema_name}.{enum.type_name}: {enum.values}")

        return "\n".join(lines)

    def to_summary_text(self) -> str:
        """Return a compressed summary containing only table and column names.

        This format minimises token usage and is intended for the database
        inference stage, where only structural keywords matter.
        """
        lines: list[str] = []
        for table in self.tables:
            cols = ", ".join(col.name for col in table.columns)
            lines.append(f"{table.schema_name}.{table.table_name}({cols})")
        return "\n".join(lines)
