"""
Integration tests for CLI with real PostgreSQL database.

These tests verify the CLI works end-to-end with subprocess execution,
testing all major flags and error handling.
"""

import json
import os
import re
import subprocess
import tempfile

import pytest


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


pytestmark = pytest.mark.integration


class TestCLIBasicExtraction:
    """Test basic CLI extraction commands."""

    def test_cli_extract_to_stdout(self, ecommerce_schema: dict, test_db_url: str):
        """Test extracting to stdout."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout
        assert "orders" in result.stdout

    def test_cli_extract_to_file(self, ecommerce_schema: dict, test_db_url: str):
        """Test extracting to file with --out-file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "dbslice.cli",
                    "extract",
                    test_db_url,
                    "--seed",
                    "orders.id=1",
                    "--out-file",
                    output_file,
                ],
                capture_output=True,
                text=True,
            )

            assert result.returncode == 0
            assert os.path.exists(output_file)

            with open(output_file) as f:
                content = f.read()
                assert "INSERT INTO" in content
                assert len(content) > 100

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_cli_with_verbose(self, ecommerce_schema: dict, test_db_url: str):
        """Test verbose output."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--verbose",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        # Verbose mode shows extraction settings and traversal path
        # (written to stderr with --no-progress)
        combined_output = result.stdout + result.stderr
        assert "orders" in combined_output


class TestCLIDirectionAndDepth:
    """Test direction and depth flags."""

    def test_cli_direction_up(self, ecommerce_schema: dict, test_db_url: str):
        """Test --direction up."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--direction",
                "up",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout

    def test_cli_direction_down(self, ecommerce_schema: dict, test_db_url: str):
        """Test --direction down."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "users.id=1",
                "--direction",
                "down",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout

    def test_cli_direction_both(self, ecommerce_schema: dict, test_db_url: str):
        """Test --direction both (default)."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--direction",
                "both",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout

    def test_cli_custom_depth(self, ecommerce_schema: dict, test_db_url: str):
        """Test --depth flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--depth",
                "3",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout


class TestCLIMultipleSeeds:
    """Test multiple seed specifications."""

    def test_cli_multiple_seeds(self, ecommerce_schema: dict, test_db_url: str):
        """Test multiple --seed flags."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--seed",
                "orders.id=2",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout

    def test_cli_where_clause_seed(self, ecommerce_schema: dict, test_db_url: str):
        """Test WHERE clause seed."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders:status='completed'",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout


class TestCLIOutputFormats:
    """Test different output formats."""

    def test_cli_sql_output(self, ecommerce_schema: dict, test_db_url: str):
        """Test SQL output (default)."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--output",
                "sql",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "INSERT INTO" in result.stdout

    def test_cli_json_output(self, ecommerce_schema: dict, test_db_url: str):
        """Test JSON output."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--output",
                "json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        # Should be valid JSON
        data = json.loads(result.stdout)
        assert "tables" in data
        assert isinstance(data["tables"], dict)

    def test_cli_json_pretty(self, ecommerce_schema: dict, test_db_url: str):
        """Test pretty JSON output."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--output",
                "json",
                "--json-pretty",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "\n" in result.stdout  # Pretty print has newlines


class TestCLIAnonymization:
    """Test anonymization flags."""

    def test_cli_anonymize(self, ecommerce_schema: dict, test_db_url: str):
        """Test --anonymize flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "users.id=1",
                "--anonymize",
                "--output",
                "json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        data = json.loads(result.stdout)
        users = data["tables"]["users"]

        # Email should be anonymized
        assert users[0]["email"] != "alice@example.com"

    def test_cli_redact_fields(self, ecommerce_schema: dict, test_db_url: str):
        """Test --redact flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "users.id=1",
                "--anonymize",
                "--redact",
                "users.address",
                "--output",
                "json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        data = json.loads(result.stdout)
        users = data["tables"]["users"]

        # Address should be redacted
        assert users[0]["address"] != "123 Main St"


class TestCLIExclude:
    """Test table exclusion."""

    def test_cli_exclude_table(self, ecommerce_schema: dict, test_db_url: str):
        """Test --exclude flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "users.id=1",
                "--exclude",
                "reviews",
                "--output",
                "json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        data = json.loads(result.stdout)
        assert "reviews" not in data["tables"]

    def test_cli_exclude_multiple(self, ecommerce_schema: dict, test_db_url: str):
        """Test excluding multiple tables."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--exclude",
                "reviews",
                "--exclude",
                "products",
                "--output",
                "json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

        data = json.loads(result.stdout)
        assert "reviews" not in data["tables"]
        assert "products" not in data["tables"]


class TestCLIValidation:
    """Test validation flags."""

    def test_cli_validation_enabled(self, ecommerce_schema: dict, test_db_url: str):
        """Test that validation is enabled by default."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--verbose",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0

    def test_cli_no_validate(self, ecommerce_schema: dict, test_db_url: str):
        """Test --no-validate flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--no-validate",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0


class TestCLIProfiling:
    """Test profiling flags."""

    def test_cli_profile(self, ecommerce_schema: dict, test_db_url: str):
        """Test --profile flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=1",
                "--profile",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        # Profile summary is printed to stderr via Rich console as part of
        # _show_extraction_summary (only when progress output is enabled).
        # format_summary() outputs "QUERY PERFORMANCE PROFILE" header.
        assert (
            "QUERY PERFORMANCE PROFILE" in result.stderr
            or "Total queries" in result.stderr
            or "queries" in result.stderr.lower()
        )


class TestCLIStreaming:
    """Test streaming mode flags."""

    def test_cli_stream_mode(self, ecommerce_schema: dict, test_db_url: str):
        """Test --stream flag."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "dbslice.cli",
                    "extract",
                    test_db_url,
                    "--seed",
                    "orders.id=1",
                    "--stream",
                    "--out-file",
                    output_file,
                ],
                capture_output=True,
                text=True,
            )

            assert result.returncode == 0
            assert os.path.exists(output_file)

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)

    def test_cli_stream_threshold(self, ecommerce_schema: dict, test_db_url: str):
        """Test --stream-threshold flag."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            output_file = f.name

        try:
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "dbslice.cli",
                    "extract",
                    test_db_url,
                    "--seed",
                    "users:id <= 10",
                    "--stream-threshold",
                    "10",  # Low threshold
                    "--out-file",
                    output_file,
                ],
                capture_output=True,
                text=True,
            )

            assert result.returncode == 0
            assert os.path.exists(output_file)

        finally:
            if os.path.exists(output_file):
                os.unlink(output_file)


class TestCLIErrorHandling:
    """Test error handling and exit codes."""

    def test_cli_invalid_seed_format(self, ecommerce_schema: dict, test_db_url: str):
        """Test invalid seed format."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "invalid-seed-format",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "Invalid seed" in result.stderr or "Error" in result.stderr

    def test_cli_table_not_found(self, ecommerce_schema: dict, test_db_url: str):
        """Test non-existent table."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "nonexistent_table.id=1",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "not found" in result.stderr.lower()

    def test_cli_no_rows_found(self, ecommerce_schema: dict, test_db_url: str):
        """Test seed that matches no rows."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                test_db_url,
                "--seed",
                "orders.id=999999",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "No rows" in result.stderr or "not found" in result.stderr.lower()

    def test_cli_invalid_database_url(self, ecommerce_schema: dict):
        """Test invalid database URL."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "extract",
                "postgresql://invalid:invalid@localhost:9999/invalid",
                "--seed",
                "orders.id=1",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "Connection" in result.stderr or "failed" in result.stderr.lower()

    def test_cli_missing_required_args(self):
        """Test missing required arguments."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice",
                "extract",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0


class TestCLIInspect:
    """Test the inspect command."""

    def test_cli_inspect_database(self, ecommerce_schema: dict, test_db_url: str):
        """Test inspecting database schema."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "inspect",
                test_db_url,
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        # Should show tables
        combined = result.stdout + result.stderr
        assert "users" in combined
        assert "orders" in combined
        assert "products" in combined

    def test_cli_inspect_specific_table(self, ecommerce_schema: dict, test_db_url: str):
        """Test inspecting a specific table."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice.cli",
                "inspect",
                test_db_url,
                "--table",
                "orders",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        combined = result.stdout + result.stderr
        assert "orders" in combined
        assert "user_id" in combined  # FK column


class TestCLIVersion:
    """Test version flag."""

    def test_cli_version(self):
        """Test --version flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice",
                "--version",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "dbslice" in result.stdout
        # Should show version number


class TestCLIHelp:
    """Test help output."""

    def test_cli_help(self):
        """Test --help flag."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice",
                "--help",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        combined = result.stdout + result.stderr
        assert "extract" in combined
        assert "inspect" in combined

    def test_cli_extract_help(self):
        """Test extract --help."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "dbslice",
                "extract",
                "--help",
            ],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        # Rich adds ANSI escape sequences that split flags like --seed;
        # strip them before checking
        combined = strip_ansi(result.stdout + result.stderr)
        assert "--seed" in combined
        assert "--depth" in combined
        assert "--direction" in combined
