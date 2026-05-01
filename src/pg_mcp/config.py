"""Application configuration using pydantic-settings.

All settings are loaded from environment variables (with optional ``.env``
file support).  Sensitive fields such as passwords and API keys use
:class:`pydantic.SecretStr` so that they are automatically masked in logs
and repr output.
"""

from __future__ import annotations

from enum import Enum

from pydantic import Field, SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class SslMode(str, Enum):
    """PostgreSQL SSL mode enumeration."""

    DISABLE = "disable"
    ALLOW = "allow"
    PREFER = "prefer"
    REQUIRE = "require"
    VERIFY_CA = "verify-ca"
    VERIFY_FULL = "verify-full"


class ValidationDataPolicy(str, Enum):
    """Policy controlling how much result data is sent to the validation LLM."""

    METADATA_ONLY = "metadata_only"
    MASKED = "masked"
    FULL = "full"


class Settings(BaseSettings):
    """pg-mcp application settings.

    Values are read from environment variables (no prefix) and optionally
    from a ``.env`` file in the working directory.  Comma-separated string
    fields are parsed into lists via :func:`computed_field` properties.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # PostgreSQL
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_user: str = Field(default="")
    pg_password: SecretStr = Field(default=SecretStr(""))
    pg_databases: str = ""
    pg_exclude_databases: str = "template0,template1,postgres"
    pg_sslmode: SslMode = SslMode.PREFER
    pg_sslrootcert: str = ""
    db_pool_size: int = 5
    strict_readonly: bool = False

    # OpenAI
    openai_api_key: SecretStr = Field(default=SecretStr(""))
    openai_model: str = "gpt-5-mini"
    openai_base_url: str | None = None
    openai_timeout: int = 60

    # Query limits
    query_timeout: int = 30
    idle_in_transaction_session_timeout: int = 60
    max_rows: int = 1000
    max_cell_bytes: int = 4096
    max_result_bytes: int = 10 * 1024 * 1024
    max_result_bytes_hard: int = 50 * 1024 * 1024
    session_work_mem: str = "64MB"
    session_temp_file_limit: str = "256MB"
    max_concurrent_requests: int = 20

    # AI validation
    enable_validation: bool = False
    validation_sample_rows: int = 10
    validation_data_policy: ValidationDataPolicy = ValidationDataPolicy.METADATA_ONLY
    validation_deny_list: str = ""
    validation_confidence_threshold: float = -1.0

    # Schema
    max_retries: int = 2
    schema_refresh_interval: int = 600
    schema_max_tables_for_full_context: int = 50

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Logging & transport
    log_level: str = "INFO"
    log_format: str = "json"  # "json" | "console"
    transport: str = "stdio"
    sse_host: str = "0.0.0.0"
    sse_port: int = 8000

    @field_validator("pg_user")
    @classmethod
    def _pg_user_not_empty(cls, value: str) -> str:
        """Reject empty pg_user values."""
        if not value or not value.strip():
            raise ValueError("pg_user must not be empty")
        return value

    @field_validator("pg_password")
    @classmethod
    def _pg_password_not_empty(cls, value: SecretStr) -> SecretStr:
        """Reject empty pg_password values."""
        if not value.get_secret_value():
            raise ValueError("pg_password must not be empty")
        return value

    @computed_field
    @property
    def pg_databases_list(self) -> list[str]:
        """Parse *pg_databases* into a list of trimmed database names."""
        if not self.pg_databases:
            return []
        return [item.strip() for item in self.pg_databases.split(",") if item.strip()]

    @computed_field
    @property
    def pg_exclude_databases_list(self) -> list[str]:
        """Parse *pg_exclude_databases* into a list of trimmed database names."""
        return [
            item.strip()
            for item in self.pg_exclude_databases.split(",")
            if item.strip()
        ]

    @computed_field
    @property
    def validation_deny_list_items(self) -> list[str]:
        """Parse *validation_deny_list* into a list of trimmed rules."""
        if not self.validation_deny_list:
            return []
        return [
            item.strip()
            for item in self.validation_deny_list.split(",")
            if item.strip()
        ]
