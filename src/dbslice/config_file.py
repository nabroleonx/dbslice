from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import yaml

from dbslice.config import ExtractConfig, OutputFormat, SeedSpec, TraversalDirection
from dbslice.constants import DEFAULT_TRAVERSAL_DEPTH
from dbslice.exceptions import DbsliceError
from dbslice.logging import get_logger
from dbslice.models import VirtualForeignKey

logger = get_logger(__name__)


def _mask_url_password(url: str) -> str:
    """Replace the password in a database URL with $DB_PASSWORD."""
    try:
        parsed = urlparse(url)
        if parsed.password:
            # Rebuild netloc with placeholder instead of password
            userinfo = f"{parsed.username}:$DB_PASSWORD" if parsed.username else "$DB_PASSWORD"
            host = parsed.hostname or ""
            port = f":{parsed.port}" if parsed.port else ""
            netloc = f"{userinfo}@{host}{port}"
            return urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass
    return url


__all__ = [
    "DbsliceConfig",
    "DatabaseConfig",
    "ExtractionConfig",
    "AnonymizationConfig",
    "OutputConfig",
    "TableOverride",
    "VirtualForeignKeyConfig",
    "ConfigFileError",
    "load_config",
]


class ConfigFileError(DbsliceError):
    """Error loading or parsing configuration file."""

    def __init__(self, path: str, reason: str):
        self.path = path
        self.reason = reason
        super().__init__(f"Failed to load config from '{path}': {reason}")


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    url: str | None = None
    """Database connection URL (can be overridden by CLI)."""


@dataclass
class ExtractionConfig:
    """Extraction behavior configuration."""

    default_depth: int = DEFAULT_TRAVERSAL_DEPTH
    """Default maximum FK traversal depth."""

    direction: str = "both"
    """Default traversal direction: 'up', 'down', or 'both'."""

    exclude_tables: list[str] = field(default_factory=list)
    """Tables to exclude from extraction."""

    passthrough_tables: list[str] = field(default_factory=list)
    """Tables to include in full, regardless of FK relationships (e.g., lookup tables, config tables)."""

    max_rows_per_table: int | None = None
    """Global limit on rows per table (None = unlimited)."""


@dataclass
class AnonymizationConfig:
    """Anonymization configuration."""

    enabled: bool = False
    """Enable automatic anonymization of detected sensitive fields."""

    seed: str | None = None
    """Seed for deterministic anonymization (None = use default)."""

    fields: dict[str, str] = field(default_factory=dict)
    """
    Field-specific anonymization mappings.
    Format: {"table.column": "faker_provider"}
    Example: {"users.email": "email", "users.phone": "phone_number"}
    """


@dataclass
class OutputConfig:
    """Output format configuration."""

    format: str = "sql"
    """Output format: 'sql', 'json', or 'csv'."""

    include_transaction: bool = True
    """Wrap SQL output in transaction block."""

    include_drop_tables: bool = False
    """Include DROP TABLE statements in SQL output."""


@dataclass
class TableOverride:
    """Per-table extraction overrides."""

    skip: bool = False
    """Skip this table entirely."""

    max_rows: int | None = None
    """Limit rows extracted from this table."""


@dataclass
class VirtualForeignKeyConfig:
    """Configuration for a single virtual foreign key."""

    source_table: str
    """Table containing the foreign key columns."""

    source_columns: list[str]
    """Column names that form the foreign key."""

    target_table: str
    """Table being referenced."""

    target_columns: list[str] | None = None
    """Target column names (defaults to target table's primary key)."""

    description: str = ""
    """Description of the relationship."""

    name: str | None = None
    """Custom FK name (auto-generated if not provided)."""

    is_nullable: bool = True
    """Whether the FK can be NULL."""


@dataclass
class DbsliceConfig:
    """
    Complete dbslice configuration loaded from YAML file.

    This configuration can be loaded from a YAML file and merged with
    CLI arguments, with CLI arguments taking precedence.
    """

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    anonymization: AnonymizationConfig = field(default_factory=AnonymizationConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    tables: dict[str, TableOverride] = field(default_factory=dict)
    """Per-table overrides keyed by table name."""

    virtual_foreign_keys: list[VirtualForeignKeyConfig] = field(default_factory=list)
    """Virtual foreign key relationships not defined in the database."""

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DbsliceConfig":
        """
        Load configuration from a YAML file.

        Args:
            path: Path to YAML configuration file

        Returns:
            Loaded DbsliceConfig object

        Raises:
            ConfigFileError: If file cannot be read or parsed
        """
        path = Path(path)

        if not path.exists():
            raise ConfigFileError(str(path), "File does not exist")

        if not path.is_file():
            raise ConfigFileError(str(path), "Path is not a file")

        try:
            with open(path) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigFileError(str(path), f"Invalid YAML: {e}")
        except OSError as e:
            raise ConfigFileError(str(path), f"Cannot read file: {e}")

        if data is None:
            data = {}

        if not isinstance(data, dict):
            raise ConfigFileError(str(path), "Config file must contain a YAML mapping (dictionary)")

        logger.info("Loaded config file", path=str(path))

        try:
            return cls._from_dict(data)
        except (ValueError, TypeError) as e:
            raise ConfigFileError(str(path), f"Invalid configuration: {e}")

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> "DbsliceConfig":
        """
        Parse configuration from dictionary.

        Args:
            data: Dictionary loaded from YAML

        Returns:
            DbsliceConfig object
        """
        database_data = data.get("database", {})
        if not isinstance(database_data, dict):
            raise ValueError("'database' section must be a mapping")

        database = DatabaseConfig(
            url=database_data.get("url"),
        )

        extraction_data = data.get("extraction", {})
        if not isinstance(extraction_data, dict):
            raise ValueError("'extraction' section must be a mapping")

        extraction = ExtractionConfig(
            default_depth=extraction_data.get("default_depth", DEFAULT_TRAVERSAL_DEPTH),
            direction=extraction_data.get("direction", "both"),
            exclude_tables=extraction_data.get("exclude_tables", []),
            passthrough_tables=extraction_data.get("passthrough_tables", []),
            max_rows_per_table=extraction_data.get("max_rows_per_table"),
        )

        if not isinstance(extraction.default_depth, int) or extraction.default_depth < 1:
            raise ValueError("'extraction.default_depth' must be a positive integer")

        valid_directions = {"up", "down", "both"}
        if extraction.direction not in valid_directions:
            raise ValueError(
                f"'extraction.direction' must be one of: {', '.join(valid_directions)}"
            )

        if not isinstance(extraction.exclude_tables, list):
            raise ValueError("'extraction.exclude_tables' must be a list")

        if not isinstance(extraction.passthrough_tables, list):
            raise ValueError("'extraction.passthrough_tables' must be a list")

        anon_data = data.get("anonymization", {})
        if not isinstance(anon_data, dict):
            raise ValueError("'anonymization' section must be a mapping")

        fields = anon_data.get("fields", {})
        if not isinstance(fields, dict):
            raise ValueError("'anonymization.fields' must be a mapping")

        anonymization = AnonymizationConfig(
            enabled=anon_data.get("enabled", False),
            seed=anon_data.get("seed"),
            fields=fields,
        )

        output_data = data.get("output", {})
        if not isinstance(output_data, dict):
            raise ValueError("'output' section must be a mapping")

        output = OutputConfig(
            format=output_data.get("format", "sql"),
            include_transaction=output_data.get("include_transaction", True),
            include_drop_tables=output_data.get("include_drop_tables", False),
        )

        valid_formats = {"sql", "json", "csv"}
        if output.format not in valid_formats:
            raise ValueError(f"'output.format' must be one of: {', '.join(valid_formats)}")

        tables_data = data.get("tables", {})
        if not isinstance(tables_data, dict):
            raise ValueError("'tables' section must be a mapping")

        tables = {}
        for table_name, table_config in tables_data.items():
            if not isinstance(table_config, dict):
                raise ValueError(f"Configuration for table '{table_name}' must be a mapping")

            tables[table_name] = TableOverride(
                skip=table_config.get("skip", False),
                max_rows=table_config.get("max_rows"),
            )

        vfk_data = data.get("virtual_foreign_keys", [])
        if not isinstance(vfk_data, list):
            raise ValueError("'virtual_foreign_keys' section must be a list")

        virtual_fks = []
        for i, vfk_config in enumerate(vfk_data):
            if not isinstance(vfk_config, dict):
                raise ValueError(f"Virtual FK #{i + 1} must be a mapping")

            if "source_table" not in vfk_config:
                raise ValueError(f"Virtual FK #{i + 1}: 'source_table' is required")
            if "source_columns" not in vfk_config:
                raise ValueError(f"Virtual FK #{i + 1}: 'source_columns' is required")
            if "target_table" not in vfk_config:
                raise ValueError(f"Virtual FK #{i + 1}: 'target_table' is required")

            source_columns = vfk_config["source_columns"]
            if not isinstance(source_columns, list):
                raise ValueError(
                    f"Virtual FK #{i + 1}: 'source_columns' must be a list of column names"
                )

            target_columns = vfk_config.get("target_columns")
            if target_columns is not None and not isinstance(target_columns, list):
                raise ValueError(
                    f"Virtual FK #{i + 1}: 'target_columns' must be a list of column names"
                )

            virtual_fks.append(
                VirtualForeignKeyConfig(
                    source_table=vfk_config["source_table"],
                    source_columns=source_columns,
                    target_table=vfk_config["target_table"],
                    target_columns=target_columns,
                    description=vfk_config.get("description", ""),
                    name=vfk_config.get("name"),
                    is_nullable=vfk_config.get("is_nullable", True),
                )
            )

        return cls(
            database=database,
            extraction=extraction,
            anonymization=anonymization,
            output=output,
            tables=tables,
            virtual_foreign_keys=virtual_fks,
        )

    def to_extract_config(
        self,
        seeds: list[SeedSpec],
        database_url: str | None = None,
        depth: int | None = None,
        direction: TraversalDirection | None = None,
        output_format: OutputFormat | None = None,
        output_file: str | None = None,
        exclude: list[str] | None = None,
        passthrough: list[str] | None = None,
        anonymize: bool | None = None,
        redact: list[str] | None = None,
        verbose: bool = False,
        dry_run: bool = False,
        no_progress: bool = False,
        validate: bool = True,
        fail_on_validation_error: bool = False,
        profile: bool = False,
    ) -> ExtractConfig:
        """
        Convert to ExtractConfig for use by the extraction engine.

        CLI arguments override config file values.

        Args:
            seeds: Seed specifications (required, from CLI)
            database_url: Database URL override (from CLI)
            depth: Depth override (from CLI)
            direction: Direction override (from CLI)
            output_format: Output format override (from CLI)
            output_file: Output file path (from CLI)
            exclude: Exclude tables override (from CLI)
            passthrough: Passthrough tables override (from CLI)
            anonymize: Anonymize flag override (from CLI)
            redact: Redact fields override (from CLI)
            verbose: Verbose flag (from CLI)
            dry_run: Dry run flag (from CLI)
            no_progress: No progress flag (from CLI)
            validate: Enable validation (from CLI)
            fail_on_validation_error: Fail on validation errors (from CLI)
            profile: Enable profiling (from CLI)

        Returns:
            ExtractConfig ready for extraction

        Raises:
            ValueError: If required fields are missing
        """
        final_url = database_url or self.database.url
        if not final_url:
            raise ValueError(
                "Database URL is required. Provide it via --database-url or in config file under 'database.url'"
            )

        final_depth = depth if depth is not None else self.extraction.default_depth

        if direction is not None:
            final_direction = direction
        else:
            final_direction = TraversalDirection(self.extraction.direction)

        if output_format is not None:
            final_output_format = output_format
        else:
            final_output_format = OutputFormat(self.output.format)

        # Exclude tables: CLI overrides (doesn't merge)
        if exclude is not None:
            final_exclude = set(exclude)
        else:
            final_exclude = set(self.extraction.exclude_tables)

        for table_name, override in self.tables.items():
            if override.skip:
                final_exclude.add(table_name)

        # Passthrough tables: CLI overrides (doesn't merge)
        if passthrough is not None:
            final_passthrough = set(passthrough)
        else:
            final_passthrough = set(self.extraction.passthrough_tables)

        final_anonymize = anonymize if anonymize is not None else self.anonymization.enabled

        # Redact fields: merge CLI with config file
        final_redact: list[str] = []
        if self.anonymization.fields:
            final_redact.extend(self.anonymization.fields.keys())
        if redact:
            final_redact.extend(redact)

        logger.debug(
            "Merged config with CLI args",
            url_source="CLI" if database_url else "config",
            depth_source="CLI" if depth is not None else "config",
            final_depth=final_depth,
            final_anonymize=final_anonymize,
        )

        virtual_fks = []
        for vfk_config in self.virtual_foreign_keys:
            target_columns = tuple(vfk_config.target_columns) if vfk_config.target_columns else ()
            name = vfk_config.name or (f"vfk_{vfk_config.source_table}_{vfk_config.target_table}")
            virtual_fks.append(
                VirtualForeignKey(
                    name=name,
                    source_table=vfk_config.source_table,
                    source_columns=tuple(vfk_config.source_columns),
                    target_table=vfk_config.target_table,
                    target_columns=target_columns,
                    description=vfk_config.description,
                    is_nullable=vfk_config.is_nullable,
                )
            )

        return ExtractConfig(
            database_url=final_url,
            seeds=seeds,
            depth=final_depth,
            direction=final_direction,
            output_format=final_output_format,
            output_file=output_file,
            exclude_tables=final_exclude,
            passthrough_tables=final_passthrough,
            anonymize=final_anonymize,
            redact_fields=final_redact,
            verbose=verbose,
            dry_run=dry_run,
            no_progress=no_progress,
            validate=validate,
            fail_on_validation_error=fail_on_validation_error,
            profile=profile,
            virtual_foreign_keys=virtual_fks,
        )

    def to_yaml(self, include_comments: bool = True) -> str:
        """
        Export configuration to YAML string.

        Args:
            include_comments: Whether to include helpful comments

        Returns:
            YAML string representation
        """
        output = []

        if include_comments:
            output.append("# dbslice configuration file")
            output.append("# https://github.com/nabroleonx/dbslice")
            output.append("")

        if include_comments:
            output.append("# Database connection settings")
        output.append("database:")
        if self.database.url:
            safe_url = _mask_url_password(self.database.url)
            output.append(f"  url: {safe_url}")
            if safe_url != self.database.url:
                output.append(
                    "  # WARNING: Password replaced with $DB_PASSWORD. "
                    "Set this environment variable or edit the URL above."
                )
        else:
            output.append("  # url: postgres://user:pass@localhost:5432/myapp")
        output.append("")

        if include_comments:
            output.append("# Extraction behavior")
        output.append("extraction:")
        output.append(f"  default_depth: {self.extraction.default_depth}")
        output.append(f"  direction: {self.extraction.direction}  # up, down, or both")
        if self.extraction.exclude_tables:
            output.append("  exclude_tables:")
            for table in self.extraction.exclude_tables:
                output.append(f"    - {table}")
        if self.extraction.max_rows_per_table:
            output.append(f"  max_rows_per_table: {self.extraction.max_rows_per_table}")
        output.append("")

        if include_comments:
            output.append("# Anonymization settings")
        output.append("anonymization:")
        output.append(f"  enabled: {str(self.anonymization.enabled).lower()}")
        if self.anonymization.seed:
            output.append(f"  seed: {self.anonymization.seed}")
        if self.anonymization.fields:
            output.append("  fields:")
            for field, provider in self.anonymization.fields.items():
                output.append(f"    {field}: {provider}")
        output.append("")

        if include_comments:
            output.append("# Output format settings")
        output.append("output:")
        output.append(f"  format: {self.output.format}  # sql, json, or csv")
        output.append(f"  include_transaction: {str(self.output.include_transaction).lower()}")
        output.append(f"  include_drop_tables: {str(self.output.include_drop_tables).lower()}")
        output.append("")

        if self.tables:
            if include_comments:
                output.append("# Per-table overrides")
            output.append("tables:")
            for table_name, override in self.tables.items():
                output.append(f"  {table_name}:")
                if override.skip:
                    output.append("    skip: true")
                if override.max_rows is not None:
                    output.append(f"    max_rows: {override.max_rows}")
            output.append("")

        if self.virtual_foreign_keys:
            if include_comments:
                output.append("# Virtual foreign keys for relationships not in the database schema")
                output.append(
                    "# Useful for Django GenericForeignKeys, implicit relationships, etc."
                )
            output.append("virtual_foreign_keys:")
            for vfk in self.virtual_foreign_keys:
                output.append("  - source_table: " + vfk.source_table)
                output.append("    source_columns:")
                for col in vfk.source_columns:
                    output.append(f"      - {col}")
                output.append("    target_table: " + vfk.target_table)
                if vfk.target_columns:
                    output.append("    target_columns:")
                    for col in vfk.target_columns:
                        output.append(f"      - {col}")
                if vfk.description:
                    output.append(f'    description: "{vfk.description}"')
                if vfk.name:
                    output.append(f"    name: {vfk.name}")
                if not vfk.is_nullable:
                    output.append("    is_nullable: false")

        return "\n".join(output)


def load_config(path: str | Path) -> DbsliceConfig:
    """
    Load configuration from a YAML file.

    Args:
        path: Path to YAML configuration file

    Returns:
        Loaded DbsliceConfig object

    Raises:
        ConfigFileError: If file cannot be read or parsed
    """
    return DbsliceConfig.from_yaml(path)
