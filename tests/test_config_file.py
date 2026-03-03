"""Tests for YAML configuration file support."""

import tempfile
from pathlib import Path

import pytest
import yaml

from dbslice.config import SeedSpec, TraversalDirection
from dbslice.config_file import (
    AnonymizationConfig,
    ConfigFileError,
    DatabaseConfig,
    DbsliceConfig,
    ExtractionConfig,
    OutputConfig,
    PerformanceConfig,
    TableOverride,
    load_config,
)


class TestDatabaseConfig:
    """Tests for DatabaseConfig dataclass."""

    def test_default_values(self):
        config = DatabaseConfig()
        assert config.url is None
        assert config.schema is None
        assert config.options == {}

    def test_with_url(self):
        config = DatabaseConfig(url="postgres://localhost/test")
        assert config.url == "postgres://localhost/test"

    def test_with_schema(self):
        config = DatabaseConfig(url="postgres://localhost/test", schema="myschema")
        assert config.url == "postgres://localhost/test"
        assert config.schema == "myschema"

    def test_with_options(self):
        config = DatabaseConfig(
            url="postgres://localhost/test",
            options={"sslmode": "require", "connect_timeout": "10"},
        )
        assert config.options == {"sslmode": "require", "connect_timeout": "10"}


class TestExtractionConfig:
    """Tests for ExtractionConfig dataclass."""

    def test_default_values(self):
        config = ExtractionConfig()
        assert config.default_depth == 3
        assert config.direction == "both"
        assert config.exclude_tables == []
        assert config.validate is True
        assert config.fail_on_validation_error is False
        assert config.max_rows_per_table is None

    def test_custom_values(self):
        config = ExtractionConfig(
            default_depth=5,
            direction="up",
            exclude_tables=["logs", "audit"],
            max_rows_per_table=1000,
        )
        assert config.default_depth == 5
        assert config.direction == "up"
        assert config.exclude_tables == ["logs", "audit"]
        assert config.max_rows_per_table == 1000


class TestAnonymizationConfig:
    """Tests for AnonymizationConfig dataclass."""

    def test_default_values(self):
        config = AnonymizationConfig()
        assert config.enabled is False
        assert config.seed is None
        assert config.fields == {}
        assert config.patterns == {}
        assert config.security_null_fields == []

    def test_with_fields(self):
        config = AnonymizationConfig(
            enabled=True,
            seed="test_seed",
            fields={"users.email": "email", "users.phone": "phone_number"},
            patterns={"users.*_email": "email"},
            security_null_fields=["users.password*"],
        )
        assert config.enabled is True
        assert config.seed == "test_seed"
        assert config.fields == {"users.email": "email", "users.phone": "phone_number"}
        assert config.patterns == {"users.*_email": "email"}
        assert config.security_null_fields == ["users.password*"]


class TestOutputConfig:
    """Tests for OutputConfig dataclass."""

    def test_default_values(self):
        config = OutputConfig()
        assert config.format == "sql"
        assert config.include_transaction is True
        assert config.include_truncate is False
        assert config.disable_fk_checks is False
        assert config.file_mode == 0o600

    def test_custom_values(self):
        config = OutputConfig(
            format="json",
            include_transaction=False,
            include_truncate=True,
            disable_fk_checks=True,
            file_mode=0o640,
        )
        assert config.format == "json"
        assert config.include_transaction is False
        assert config.include_truncate is True
        assert config.disable_fk_checks is True
        assert config.file_mode == 0o640


class TestTableOverride:
    """Tests for TableOverride dataclass."""

    def test_default_values(self):
        override = TableOverride()
        assert override.skip is False
        assert override.depth is None
        assert override.direction is None
        assert override.max_rows is None
        assert override.anonymize_fields == {}

    def test_skip_table(self):
        override = TableOverride(skip=True)
        assert override.skip is True

    def test_max_rows(self):
        override = TableOverride(max_rows=100)
        assert override.max_rows == 100

    def test_legacy_fields(self):
        override = TableOverride(
            depth=2,
            direction="up",
            anonymize_fields={"email": "email"},
        )
        assert override.depth == 2
        assert override.direction == "up"
        assert override.anonymize_fields == {"email": "email"}


class TestDbsliceConfig:
    """Tests for DbsliceConfig dataclass and YAML loading."""

    def test_default_config(self):
        config = DbsliceConfig()
        assert config.database.url is None
        assert config.extraction.default_depth == 3
        assert config.anonymization.enabled is False
        assert config.output.format == "sql"
        assert config.tables == {}

    def test_from_yaml_minimal(self):
        yaml_content = """
database:
  url: postgres://localhost/test

extraction:
  default_depth: 3
  direction: both

anonymization:
  enabled: false

output:
  format: sql
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            assert config.database.url == "postgres://localhost/test"
            assert config.extraction.default_depth == 3
            assert config.extraction.direction == "both"
            assert config.anonymization.enabled is False
            assert config.output.format == "sql"
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_with_schema(self):
        yaml_content = """
database:
  url: postgres://localhost/test
  schema: myschema

extraction:
  default_depth: 3
  direction: both
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            assert config.database.url == "postgres://localhost/test"
            assert config.database.schema == "myschema"
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_full_config(self):
        yaml_content = """
database:
  url: postgres://user:pass@localhost:5432/myapp
  options:
    sslmode: require
    connect_timeout: 10

extraction:
  default_depth: 5
  direction: up
  exclude_tables:
    - logs
    - audit_trail
  max_rows_per_table: 10000

anonymization:
  enabled: true
  seed: my_custom_seed
  fields:
    users.email: email
    users.phone: phone_number
    customers.address: address
  patterns:
    users.*_name: name
    "*.phone*": phone_number
  security_null_fields:
    - users.password*
    - "*.api_key"

output:
  format: json
  include_transaction: false
  include_drop_tables: true

tables:
  sessions:
    skip: true
  large_table:
    max_rows: 500
  users:
    depth: 2
    direction: up
    anonymize_fields:
      email: email

performance:
  batch_size: 250
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)

            # Database
            assert config.database.url == "postgres://user:pass@localhost:5432/myapp"
            assert config.database.options == {
                "sslmode": "require",
                "connect_timeout": "10",
            }

            # Extraction
            assert config.extraction.default_depth == 5
            assert config.extraction.direction == "up"
            assert config.extraction.exclude_tables == ["logs", "audit_trail"]
            assert config.extraction.max_rows_per_table == 10000

            # Anonymization
            assert config.anonymization.enabled is True
            assert config.anonymization.seed == "my_custom_seed"
            assert config.anonymization.fields == {
                "users.email": "email",
                "users.phone": "phone_number",
                "customers.address": "address",
            }
            assert config.anonymization.patterns == {
                "users.*_name": "name",
                "*.phone*": "phone_number",
            }
            assert config.anonymization.security_null_fields == [
                "users.password*",
                "*.api_key",
            ]

            # Output
            assert config.output.format == "json"
            assert config.output.include_transaction is False
            assert config.output.include_truncate is True

            # Tables
            assert "sessions" in config.tables
            assert config.tables["sessions"].skip is True
            assert "large_table" in config.tables
            assert config.tables["large_table"].max_rows == 500
            assert config.tables["users"].depth == 2
            assert config.tables["users"].direction == "up"
            assert config.tables["users"].anonymize_fields == {"email": "email"}

            # Performance
            assert config.performance.batch_size == 250
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_empty_file(self):
        yaml_content = ""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            # Should create config with defaults
            assert config.database.url is None
            assert config.extraction.default_depth == 3
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigFileError) as exc_info:
            DbsliceConfig.from_yaml("/nonexistent/path/to/config.yaml")
        assert "File does not exist" in str(exc_info.value)

    def test_from_yaml_invalid_yaml(self):
        yaml_content = """
database:
  url: postgres://localhost/test
  invalid: [this is broken
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "Invalid YAML" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_structure(self):
        yaml_content = """
- this is a list
- not a dictionary
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "must contain a YAML mapping" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_depth(self):
        yaml_content = """
extraction:
  default_depth: -1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "must be a positive integer" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_direction(self):
        yaml_content = """
extraction:
  direction: sideways
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "direction" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_output_format(self):
        yaml_content = """
output:
  format: xml
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "format" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_performance_settings(self):
        yaml_content = """
database:
  url: postgres://localhost/test
performance:
  profile: true
  streaming:
    enabled: true
    threshold: 123
    chunk_size: 7
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            assert config.performance.profile is True
            assert config.performance.streaming.enabled is True
            assert config.performance.streaming.threshold == 123
            assert config.performance.streaming.chunk_size == 7
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_database_options_type(self):
        yaml_content = """
database:
  url: postgres://localhost/test
  options:
    - bad
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "database.options" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_performance_batch_size(self):
        yaml_content = """
database:
  url: postgres://localhost/test
performance:
  batch_size: 0
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "batch_size" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_legacy_table_exclude_alias(self):
        yaml_content = """
tables:
  logs:
    exclude: true
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            assert config.tables["logs"].skip is True
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_legacy_table_exclude_conflict(self):
        yaml_content = """
tables:
  logs:
    skip: true
    exclude: false
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "disagree" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_unknown_key_fails(self):
        yaml_content = """
unknown_section:
  x: 1
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "unknown key" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_custom_anonymization_rules(self):
        yaml_content = """
anonymization:
  enabled: true
  patterns:
    users.*_email: email
  security_null_fields:
    - users.password*
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = DbsliceConfig.from_yaml(temp_path)
            assert config.anonymization.patterns == {"users.*_email": "email"}
            assert config.anonymization.security_null_fields == ["users.password*"]
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_anonymization_provider_fails(self):
        yaml_content = """
anonymization:
  enabled: true
  patterns:
    users.*_email: no_such_provider
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "unknown faker provider" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_anonymization_field_key_fails(self):
        yaml_content = """
anonymization:
  enabled: true
  fields:
    users_*_email: email
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "table.column" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()

    def test_from_yaml_invalid_security_null_field_pattern_fails(self):
        yaml_content = """
anonymization:
  enabled: true
  security_null_fields:
    - password*
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            with pytest.raises(ConfigFileError) as exc_info:
                DbsliceConfig.from_yaml(temp_path)
            assert "table.column" in str(exc_info.value).lower()
        finally:
            Path(temp_path).unlink()


class TestToExtractConfig:
    """Tests for converting DbsliceConfig to ExtractConfig."""

    def test_basic_conversion(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(default_depth=3, direction="both"),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)

        assert extract_config.database_url == "postgres://localhost/test"
        assert extract_config.seeds == seeds
        assert extract_config.depth == 3
        assert extract_config.direction == TraversalDirection.BOTH

    def test_cli_overrides(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(default_depth=3, direction="both"),
            anonymization=AnonymizationConfig(enabled=True),
        )

        seeds = [SeedSpec.parse("users.id=1")]

        # CLI overrides
        extract_config = config.to_extract_config(
            seeds=seeds,
            database_url="postgres://localhost/override",
            depth=5,
            direction=TraversalDirection.UP,
            anonymize=False,
        )

        assert extract_config.database_url == "postgres://localhost/override"
        assert extract_config.depth == 5
        assert extract_config.direction == TraversalDirection.UP
        assert extract_config.anonymize is False

    def test_exclude_tables_from_config(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(exclude_tables=["logs", "audit"]),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)

        assert "logs" in extract_config.exclude_tables
        assert "audit" in extract_config.exclude_tables

    def test_exclude_tables_from_cli(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(exclude_tables=["logs"]),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        # CLI exclude overrides config (doesn't merge)
        extract_config = config.to_extract_config(seeds=seeds, exclude=["sessions"])

        assert "sessions" in extract_config.exclude_tables
        assert "logs" not in extract_config.exclude_tables

    def test_skip_tables_added_to_exclude(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            tables={"sessions": TableOverride(skip=True), "audit": TableOverride(skip=True)},
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)

        assert "sessions" in extract_config.exclude_tables
        assert "audit" in extract_config.exclude_tables

    def test_anonymization_fields_merged(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=True, fields={"users.email": "email", "users.phone": "phone_number"}
            ),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(
            seeds=seeds, redact=["customers.address", "customers.ssn"]
        )

        # Should merge config fields with CLI redact
        assert "users.email" in extract_config.redact_fields
        assert "users.phone" in extract_config.redact_fields
        assert "customers.address" in extract_config.redact_fields
        assert "customers.ssn" in extract_config.redact_fields

    def test_anonymization_rules_ignored_when_disabled(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=False,
                fields={"users.email": "email"},
                patterns={"users.*_name": "name"},
                security_null_fields=["users.password*"],
            ),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)

        assert extract_config.anonymize is False
        assert extract_config.redact_fields == []
        assert extract_config.anonymization_field_providers == {}
        assert extract_config.anonymization_patterns == {}
        assert extract_config.security_null_fields == []

    def test_cli_redact_still_works_when_anonymization_disabled(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=False,
                fields={"users.email": "email"},
                patterns={"users.*_name": "name"},
            ),
        )

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(
            seeds=seeds,
            redact=["users.custom_note"],
        )

        assert extract_config.anonymize is False
        assert extract_config.redact_fields == ["users.custom_note"]
        assert extract_config.anonymization_field_providers == {}
        assert extract_config.anonymization_patterns == {}

    def test_missing_database_url(self):
        config = DbsliceConfig()  # No database URL
        seeds = [SeedSpec.parse("users.id=1")]

        with pytest.raises(ValueError) as exc_info:
            config.to_extract_config(seeds=seeds)
        assert "Database URL is required" in str(exc_info.value)

    def test_database_url_from_config(self):
        config = DbsliceConfig(database=DatabaseConfig(url="postgres://localhost/test"))

        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds, database_url=None)

        assert extract_config.database_url == "postgres://localhost/test"

    def test_schema_from_config(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test", schema="myschema")
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.schema == "myschema"

    def test_schema_cli_overrides_config(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test", schema="config_schema")
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds, schema="cli_schema")
        assert extract_config.schema == "cli_schema"

    def test_schema_none_when_not_set(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test")
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.schema is None

    def test_row_limit_settings_propagate(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(max_rows_per_table=1000),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.row_limit_global == 1000

    def test_table_row_limit_settings_propagate(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            tables={"users": TableOverride(max_rows=100)},
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.row_limit_per_table == {"users": 100}

    def test_database_options_merged_into_url(self):
        config = DbsliceConfig(
            database=DatabaseConfig(
                url="postgres://localhost/test?sslmode=disable",
                options={"sslmode": "require", "application_name": "dbslice"},
            ),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert "sslmode=require" in extract_config.database_url
        assert "application_name=dbslice" in extract_config.database_url

    def test_database_options_ignored_when_cli_url_provided(self):
        config = DbsliceConfig(
            database=DatabaseConfig(
                url="postgres://localhost/test",
                options={"sslmode": "require"},
            ),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(
            seeds=seeds, database_url="postgres://localhost/override"
        )
        assert extract_config.database_url == "postgres://localhost/override"
        assert "sslmode" not in extract_config.database_url

    def test_performance_batch_size_propagates(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            performance=PerformanceConfig(batch_size=321),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.db_batch_size == 321

    def test_table_depth_and_direction_overrides_propagate(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            tables={
                "orders": TableOverride(depth=1, direction="up"),
                "users": TableOverride(direction="both"),
            },
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.table_depth_overrides == {"orders": 1}
        assert extract_config.table_direction_overrides == {
            "orders": TraversalDirection.UP,
            "users": TraversalDirection.BOTH,
        }

    def test_legacy_table_anonymize_fields_merged(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=True,
                fields={"users.email": "safe_email"},
            ),
            tables={
                "users": TableOverride(
                    anonymize_fields={"email": "email", "name": "name"},
                )
            },
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.anonymization_field_providers == {
            "users.email": "safe_email",
            "users.name": "name",
        }
        assert "users.email" in extract_config.redact_fields
        assert "users.name" in extract_config.redact_fields

    def test_runtime_flags_propagate_in_to_extract_config(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=True,
                seed="abc123",
                fields={"users.email": "email"},
                patterns={"users.*_name": "name"},
                security_null_fields=["users.password*"],
            ),
            output=OutputConfig(
                include_transaction=False,
                include_truncate=True,
                disable_fk_checks=True,
                file_mode=0o640,
            ),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(
            seeds=seeds,
            validate=False,
            fail_on_validation_error=True,
            profile=True,
            stream=True,
            stream_threshold=100,
            stream_chunk_size=5,
            output_file_mode=0o660,
        )

        assert extract_config.validate is False
        assert extract_config.fail_on_validation_error is True
        assert extract_config.profile is True
        assert extract_config.stream is True
        assert extract_config.streaming_threshold == 100
        assert extract_config.streaming_chunk_size == 5
        assert extract_config.include_transaction is False
        assert extract_config.include_truncate is True
        assert extract_config.disable_fk_checks is True
        assert extract_config.output_file_mode == 0o660
        assert extract_config.anonymization_seed == "abc123"
        assert extract_config.anonymization_field_providers == {"users.email": "email"}
        assert extract_config.anonymization_patterns == {"users.*_name": "name"}
        assert extract_config.security_null_fields == ["users.password*"]

    def test_extraction_validation_defaults_come_from_config(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(validate=False, fail_on_validation_error=True),
        )
        seeds = [SeedSpec.parse("users.id=1")]
        extract_config = config.to_extract_config(seeds=seeds)
        assert extract_config.validate is False
        assert extract_config.fail_on_validation_error is True


class TestToYaml:
    """Tests for exporting config to YAML."""

    def test_minimal_yaml(self):
        config = DbsliceConfig(database=DatabaseConfig(url="postgres://localhost/test"))
        yaml_str = config.to_yaml(include_comments=False)

        # Parse it back
        data = yaml.safe_load(yaml_str)
        assert data["database"]["url"] == "postgres://localhost/test"
        assert data["extraction"]["default_depth"] == 3
        assert data["output"]["format"] == "sql"

    def test_full_yaml(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(default_depth=5, direction="up", exclude_tables=["logs"]),
            anonymization=AnonymizationConfig(
                enabled=True,
                fields={"users.email": "email"},
                patterns={"users.*_email": "email"},
                security_null_fields=["users.password*"],
            ),
            output=OutputConfig(format="json", include_transaction=False),
            tables={"sessions": TableOverride(skip=True)},
        )

        yaml_str = config.to_yaml(include_comments=False)

        # Parse it back
        data = yaml.safe_load(yaml_str)
        assert data["database"]["url"] == "postgres://localhost/test"
        assert data["extraction"]["default_depth"] == 5
        assert data["extraction"]["direction"] == "up"
        assert data["anonymization"]["enabled"] is True
        assert data["anonymization"]["patterns"]["users.*_email"] == "email"
        assert data["anonymization"]["security_null_fields"] == ["users.password*"]
        assert data["output"]["format"] == "json"
        assert data["output"]["include_truncate"] is False
        assert data["tables"]["sessions"]["skip"] is True

    def test_yaml_with_schema(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test", schema="myschema")
        )
        yaml_str = config.to_yaml(include_comments=False)
        data = yaml.safe_load(yaml_str)
        assert data["database"]["schema"] == "myschema"

    def test_yaml_with_comments(self):
        config = DbsliceConfig()
        yaml_str = config.to_yaml(include_comments=True)

        # Should contain comments
        assert "# dbslice configuration file" in yaml_str
        assert "# Database connection settings" in yaml_str
        assert "# Extraction behavior" in yaml_str
        assert "# schema: public" in yaml_str

    def test_yaml_roundtrip_with_leading_wildcards(self):
        config = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            anonymization=AnonymizationConfig(
                enabled=True,
                patterns={"*.api_key": "pystr"},
                security_null_fields=["*.token"],
            ),
        )
        yaml_str = config.to_yaml(include_comments=False)
        data = yaml.safe_load(yaml_str)
        assert data["anonymization"]["patterns"]["*.api_key"] == "pystr"
        assert data["anonymization"]["security_null_fields"] == ["*.token"]


class TestLoadConfig:
    """Tests for load_config convenience function."""

    def test_load_config_success(self):
        yaml_content = """
database:
  url: postgres://localhost/test
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            temp_path = f.name

        try:
            config = load_config(temp_path)
            assert config.database.url == "postgres://localhost/test"
        finally:
            Path(temp_path).unlink()

    def test_load_config_file_not_found(self):
        with pytest.raises(ConfigFileError):
            load_config("/nonexistent/config.yaml")


class TestConfigFileRoundtrip:
    """Test that config can be written and read back."""

    def test_roundtrip(self):
        original = DbsliceConfig(
            database=DatabaseConfig(url="postgres://localhost/test"),
            extraction=ExtractionConfig(default_depth=5, direction="up"),
            anonymization=AnonymizationConfig(
                enabled=True,
                fields={"users.email": "email"},
                patterns={"users.*_email": "email"},
                security_null_fields=["users.password*"],
            ),
            output=OutputConfig(format="json"),
            tables={"logs": TableOverride(skip=True)},
        )

        # Write to file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml_str = original.to_yaml(include_comments=False)
            f.write(yaml_str)
            f.flush()
            temp_path = f.name

        try:
            # Read back
            loaded = DbsliceConfig.from_yaml(temp_path)

            # Compare
            assert loaded.database.url == original.database.url
            assert loaded.extraction.default_depth == original.extraction.default_depth
            assert loaded.extraction.direction == original.extraction.direction
            assert loaded.anonymization.enabled == original.anonymization.enabled
            assert loaded.anonymization.fields == original.anonymization.fields
            assert loaded.anonymization.patterns == original.anonymization.patterns
            assert (
                loaded.anonymization.security_null_fields
                == original.anonymization.security_null_fields
            )
            assert loaded.output.format == original.output.format
            assert "logs" in loaded.tables
            assert loaded.tables["logs"].skip is True
        finally:
            Path(temp_path).unlink()
