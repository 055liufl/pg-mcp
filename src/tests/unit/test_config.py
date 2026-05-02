"""Unit tests for Settings configuration.

Covers:
- Default values
- Environment variable override
- SecretStr masking in repr
- Comma-list parsing for pg_databases, pg_exclude_databases, validation_deny_list
- Validation errors (empty pg_user, empty pg_password)
"""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from pydantic import ValidationError

from pg_mcp.config import Settings, SslMode, ValidationDataPolicy


@pytest.fixture(autouse=True)
def clear_env_vars() -> Generator[None, None, None]:
    """Clear relevant environment variables before each test."""
    keys = [
        "PG_USER",
        "PG_PASSWORD",
        "PG_DATABASES",
        "PG_EXCLUDE_DATABASES",
        "VALIDATION_DENY_LIST",
        "OPENAI_API_KEY",
        "PG_HOST",
        "PG_PORT",
        "LOG_LEVEL",
    ]
    original: dict[str, str | None] = {}
    for key in keys:
        original[key] = os.environ.get(key)
        os.environ.pop(key, None)

    yield

    for key, val in original.items():
        if val is not None:
            os.environ[key] = val
        else:
            os.environ.pop(key, None)


class TestDefaults:
    """Tests for default configuration values."""

    def test_default_pg_host(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.pg_host == "localhost"

    def test_default_pg_port(self) -> None:
        # Read the field default directly from the model definition so that
        # a local ``.env`` file (which pydantic-settings loads automatically)
        # does not interfere with the assertion.
        default = Settings.model_fields["pg_port"].default

        assert default == 5432

    def test_default_pg_sslmode(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.pg_sslmode == SslMode.PREFER

    def test_default_exclude_databases(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.pg_exclude_databases == "template0,template1,postgres"

    def test_default_openai_model(self) -> None:
        # Read the field default directly from the model definition so that
        # a local ``.env`` file does not interfere with the assertion.
        default = Settings.model_fields["openai_model"].default

        assert default == "gpt-5-mini"

    def test_default_validation_disabled(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.enable_validation is False

    def test_default_validation_data_policy(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.validation_data_policy == ValidationDataPolicy.METADATA_ONLY

    def test_default_max_concurrent_requests(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.max_concurrent_requests == 20


class TestEnvVarOverride:
    """Tests for environment variable-based configuration."""

    def test_pg_host_override(self) -> None:
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASSWORD"] = "test"
        os.environ["PG_HOST"] = "db.example.com"
        settings = Settings()

        assert settings.pg_host == "db.example.com"

    def test_pg_port_override(self) -> None:
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASSWORD"] = "test"
        os.environ["PG_PORT"] = "5433"
        settings = Settings()

        assert settings.pg_port == 5433

    def test_log_level_override(self) -> None:
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASSWORD"] = "test"
        os.environ["LOG_LEVEL"] = "DEBUG"
        settings = Settings()

        assert settings.log_level == "DEBUG"

    def test_openai_api_key_from_env(self) -> None:
        os.environ["PG_USER"] = "test"
        os.environ["PG_PASSWORD"] = "test"
        os.environ["OPENAI_API_KEY"] = "sk-test-key"
        settings = Settings()

        assert settings.openai_api_key.get_secret_value() == "sk-test-key"


class TestSecretStrMasking:
    """Tests that sensitive fields are masked in repr/str."""

    def test_pg_password_masked_in_repr(self) -> None:
        settings = Settings(pg_user="test", pg_password="secret123")

        repr_str = repr(settings)
        assert "secret123" not in repr_str

    def test_openai_api_key_masked_in_repr(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", openai_api_key="sk-secret")

        repr_str = repr(settings)
        assert "sk-secret" not in repr_str

    def test_pg_password_accessible_via_get_secret_value(self) -> None:
        settings = Settings(pg_user="test", pg_password="secret123")

        assert settings.pg_password.get_secret_value() == "secret123"


class TestCommaListParsing:
    """Tests for comma-separated list field parsing."""

    def test_pg_databases_list_empty_when_blank(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", pg_databases="")

        assert settings.pg_databases_list == []

    def test_pg_databases_list_single_item(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", pg_databases="mydb")

        assert settings.pg_databases_list == ["mydb"]

    def test_pg_databases_list_multiple_items(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", pg_databases="db1,db2,db3")

        assert settings.pg_databases_list == ["db1", "db2", "db3"]

    def test_pg_databases_list_trims_whitespace(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", pg_databases=" db1 , db2 , db3 ")

        assert settings.pg_databases_list == ["db1", "db2", "db3"]

    def test_pg_databases_list_ignores_empty_items(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", pg_databases="db1,,db3")

        assert settings.pg_databases_list == ["db1", "db3"]

    def test_pg_exclude_databases_list_default(self) -> None:
        settings = Settings(pg_user="test", pg_password="test")

        assert settings.pg_exclude_databases_list == [
            "template0",
            "template1",
            "postgres",
        ]

    def test_pg_exclude_databases_list_override(self) -> None:
        settings = Settings(
            pg_user="test",
            pg_password="test",
            pg_exclude_databases="old_db,backup_db",
        )

        assert settings.pg_exclude_databases_list == ["old_db", "backup_db"]

    def test_validation_deny_list_empty_when_blank(self) -> None:
        settings = Settings(pg_user="test", pg_password="test", validation_deny_list="")

        assert settings.validation_deny_list_items == []

    def test_validation_deny_list_multiple_items(self) -> None:
        settings = Settings(
            pg_user="test",
            pg_password="test",
            validation_deny_list="prod_db, staging_db",
        )

        assert settings.validation_deny_list_items == ["prod_db", "staging_db"]


class TestValidation:
    """Tests for field validation errors."""

    def test_empty_pg_user_raises_error(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Settings(pg_user="", pg_password="test")

        assert "pg_user" in str(exc_info.value)

    def test_whitespace_only_pg_user_raises_error(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Settings(pg_user="   ", pg_password="test")

        assert "pg_user" in str(exc_info.value)

    def test_empty_pg_password_raises_error(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            Settings(pg_user="test", pg_password="")

        assert "pg_password" in str(exc_info.value)

    def test_valid_settings_construct_successfully(self) -> None:
        settings = Settings(pg_user="admin", pg_password="secure")

        assert settings.pg_user == "admin"
        assert settings.pg_password.get_secret_value() == "secure"


class TestSslMode:
    """Tests for SSL mode enum."""

    def test_ssl_mode_values(self) -> None:
        assert SslMode.DISABLE == "disable"
        assert SslMode.ALLOW == "allow"
        assert SslMode.PREFER == "prefer"
        assert SslMode.REQUIRE == "require"
        assert SslMode.VERIFY_CA == "verify-ca"
        assert SslMode.VERIFY_FULL == "verify-full"


class TestValidationDataPolicy:
    """Tests for validation data policy enum."""

    def test_policy_values(self) -> None:
        assert ValidationDataPolicy.METADATA_ONLY == "metadata_only"
        assert ValidationDataPolicy.MASKED == "masked"
        assert ValidationDataPolicy.FULL == "full"
