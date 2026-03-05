"""Tests for CLI environment-variable precedence and parsing."""

from pathlib import Path

import pytest
import typer

import dbslice.cli as cli
from dbslice.config import OutputFormat, TraversalDirection


@pytest.fixture
def capture_extract(monkeypatch):
    captured: dict[str, object] = {}

    def fake_execute(config, _console):
        captured["extract_config"] = config
        return object(), object(), object()

    def fake_handle_output(**kwargs):
        captured["output_format"] = kwargs["output_format"]

    monkeypatch.setattr(cli, "_execute_extraction", fake_execute)
    monkeypatch.setattr(cli, "_handle_output_format", fake_handle_output)

    return captured


def test_extract_uses_database_url_from_env(monkeypatch, capture_extract):
    env_url = "postgresql://env_user:env_pass@localhost:5432/envdb"
    monkeypatch.setenv("DATABASE_URL", env_url)

    cli.extract(
        seed=["users.id=1"],
        no_progress=True,
    )

    extract_config = capture_extract["extract_config"]
    assert extract_config.database_url == env_url


def test_extract_cli_flags_override_env(monkeypatch, capture_extract):
    monkeypatch.setenv("DBSLICE_DEPTH", "9")
    monkeypatch.setenv("DBSLICE_DIRECTION", "down")
    monkeypatch.setenv("DBSLICE_OUTPUT_FORMAT", "json")
    monkeypatch.setenv("DBSLICE_ANONYMIZE", "false")
    monkeypatch.setenv("DBSLICE_REDACT_FIELDS", "users.email,users.phone")
    monkeypatch.setenv("DBSLICE_ALLOW_UNSAFE_WHERE", "false")

    cli.extract(
        database_url="postgresql://cli_user:cli_pass@localhost:5432/clidb",
        seed=["users.id=1"],
        depth=2,
        direction="up",
        output="sql",
        anonymize=True,
        redact=["users.custom_field"],
        allow_unsafe_where=True,
        no_progress=True,
    )

    extract_config = capture_extract["extract_config"]
    assert extract_config.depth == 2
    assert extract_config.direction == TraversalDirection.UP
    assert extract_config.output_format == OutputFormat.SQL
    assert extract_config.anonymize is True
    assert extract_config.redact_fields == ["users.custom_field"]
    assert extract_config.allow_unsafe_where is True


def test_extract_env_overrides_config_when_cli_missing(monkeypatch, tmp_path, capture_extract):
    config_path = tmp_path / "dbslice.yaml"
    config_path.write_text(
        "\n".join(
            [
                "database:",
                "  url: postgresql://config_user:config_pass@localhost:5432/configdb",
                "extraction:",
                "  default_depth: 7",
                "  direction: down",
                "  allow_unsafe_where: false",
                "anonymization:",
                "  enabled: false",
                "output:",
                "  format: sql",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("DATABASE_URL", "postgresql://env_user:env_pass@localhost:5432/envdb")
    monkeypatch.setenv("DBSLICE_DEPTH", "4")
    monkeypatch.setenv("DBSLICE_DIRECTION", "up")
    monkeypatch.setenv("DBSLICE_OUTPUT_FORMAT", "json")
    monkeypatch.setenv("DBSLICE_ANONYMIZE", "true")
    monkeypatch.setenv("DBSLICE_REDACT_FIELDS", "users.ssn,users.passport")
    monkeypatch.setenv("DBSLICE_ALLOW_UNSAFE_WHERE", "true")

    cli.extract(
        config=Path(config_path),
        seed=["users.id=1"],
        no_progress=True,
    )

    extract_config = capture_extract["extract_config"]
    assert extract_config.database_url == "postgresql://env_user:env_pass@localhost:5432/envdb"
    assert extract_config.depth == 4
    assert extract_config.direction == TraversalDirection.UP
    assert extract_config.output_format == OutputFormat.JSON
    assert extract_config.anonymize is True
    assert extract_config.redact_fields == ["users.ssn", "users.passport"]
    assert extract_config.allow_unsafe_where is True


def test_extract_invalid_env_value_fails_fast(monkeypatch, capsys):
    monkeypatch.setenv("DATABASE_URL", "postgresql://env_user:env_pass@localhost:5432/envdb")
    monkeypatch.setenv("DBSLICE_DIRECTION", "sideways")

    with pytest.raises(typer.Exit) as exc_info:
        cli.extract(seed=["users.id=1"], no_progress=True)

    assert exc_info.value.exit_code == 1
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Validation Error" in combined
    assert "DBSLICE_DIRECTION" in combined
