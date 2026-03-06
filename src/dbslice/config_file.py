import inspect
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import yaml

from dbslice.config import ExtractConfig, OutputFormat, SeedSpec, TraversalDirection
from dbslice.constants import (
    DEFAULT_OUTPUT_FILE_MODE,
    DEFAULT_STREAMING_CHUNK_SIZE,
    DEFAULT_STREAMING_THRESHOLD,
    DEFAULT_TRAVERSAL_DEPTH,
)
from dbslice.exceptions import DbsliceError
from dbslice.logging import get_logger
from dbslice.models import VirtualForeignKey

logger = get_logger(__name__)

_TOP_LEVEL_KEYS = {
    "version",
    "database",
    "extraction",
    "anonymization",
    "output",
    "performance",
    "tables",
    "virtual_foreign_keys",
    "compliance",
}
_DATABASE_KEYS = {"url", "schema", "options"}
_EXTRACTION_KEYS = {
    "default_depth",
    "direction",
    "exclude_tables",
    "passthrough_tables",
    "validate",
    "fail_on_validation_error",
    "max_rows_per_table",
    "allow_unsafe_where",
}
_ANONYMIZATION_KEYS = {
    "enabled",
    "seed",
    "fields",
    "patterns",
    "security_null_fields",
    "deterministic",
}
_COMPLIANCE_KEYS = {
    "profiles",
    "strict",
    "generate_manifest",
    "policy_mode",
    "allow_url_patterns",
    "deny_url_patterns",
    "required_sslmode",
    "require_ci",
    "sign_manifest",
    "manifest_key_env",
}
_OUTPUT_KEYS = {
    "format",
    "include_transaction",
    "include_truncate",
    "include_drop_tables",
    "disable_fk_checks",
    "file_mode",
    "json_mode",
    "json_pretty",
    "csv_mode",
    "csv_delimiter",
}
_PERFORMANCE_KEYS = {"profile", "streaming", "batch_size"}
_STREAMING_KEYS = {"enabled", "threshold", "chunk_size"}
_TABLE_OVERRIDE_KEYS = {"skip", "max_rows", "depth", "direction", "exclude", "anonymize_fields"}
_VIRTUAL_FK_KEYS = {
    "source_table",
    "source_columns",
    "target_table",
    "target_columns",
    "description",
    "name",
    "is_nullable",
}
_DATABASE_URL_ENV_PATTERN = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _validate_unknown_keys(section_name: str, data: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(data.keys()) - allowed)
    if unknown:
        raise ValueError(
            f"Unknown key(s) in '{section_name}': {', '.join(unknown)}. "
            "Remove unsupported fields or update your config."
        )


def _validate_exact_field_key(key: str, section_name: str) -> None:
    """Validate exact table.column keys (no wildcards)."""
    if not isinstance(key, str):
        raise ValueError(f"{section_name} keys must be strings")
    if "*" in key or "?" in key:
        raise ValueError(
            f"{section_name} key '{key}' must be an exact table.column without wildcards"
        )
    parts = key.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"{section_name} key '{key}' must be in table.column format")


def _validate_glob_field_pattern(pattern: str, section_name: str) -> None:
    """Validate wildcard table.column glob patterns."""
    if not isinstance(pattern, str):
        raise ValueError(f"{section_name} entries must be strings")
    parts = pattern.split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"{section_name} pattern '{pattern}' must be in table.column format (glob allowed)"
        )


def _validate_faker_provider(provider: str) -> None:
    """Validate that a Faker provider exists and is callable without required args."""
    if not isinstance(provider, str) or not provider:
        raise ValueError("Faker provider name must be a non-empty string")

    try:
        from faker import Faker
    except ImportError as e:
        raise ValueError("Faker is required to validate anonymization providers") from e

    fake = Faker()
    method = getattr(fake, provider, None)
    if method is None or not callable(method):
        raise ValueError(f"Unknown Faker provider '{provider}'")

    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        # If we cannot introspect, accept callable as valid.
        return

    for param in signature.parameters.values():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.default is inspect.Parameter.empty:
            raise ValueError(
                f"Faker provider '{provider}' requires parameter '{param.name}' and "
                "cannot be used without arguments"
            )


def _normalize_database_options(raw_options: Any) -> dict[str, str]:
    """Validate and normalize database options to string values."""
    if raw_options is None:
        return {}
    if not isinstance(raw_options, dict):
        raise ValueError("'database.options' must be a mapping")

    normalized: dict[str, str] = {}
    for key, value in raw_options.items():
        if not isinstance(key, str) or not key:
            raise ValueError("'database.options' keys must be non-empty strings")
        if value is None or not isinstance(value, (str, int, float, bool)):
            raise ValueError(
                "'database.options' values must be scalar (string, number, or boolean)"
            )
        normalized[key] = str(value).lower() if isinstance(value, bool) else str(value)
    return normalized


def _merge_database_url_options(url: str, options: dict[str, str]) -> str:
    """Merge/override query options into a database URL."""
    if not options:
        return url
    parsed = urlparse(url)
    existing = parse_qs(parsed.query, keep_blank_values=True)
    for key, value in options.items():
        existing[key] = [value]
    merged_query = urlencode(existing, doseq=True)
    return urlunparse(parsed._replace(query=merged_query))


def _resolve_database_url(raw_url: Any) -> str | None:
    """Resolve database.url, supporting exact-match ${VAR} and ${VAR_FILE} placeholders."""
    if raw_url is None:
        return None
    if not isinstance(raw_url, str):
        raise ValueError("'database.url' must be a string")

    match = _DATABASE_URL_ENV_PATTERN.fullmatch(raw_url)
    if not match:
        return raw_url

    env_key = match.group(1)
    env_value = os.environ.get(env_key)
    if env_value is None:
        raise ValueError(
            f"'database.url' references environment variable '{env_key}', but it is not set"
        )

    if env_key.endswith("_FILE"):
        file_path = env_value
        try:
            resolved = Path(file_path).read_text(encoding="utf-8")
        except OSError as e:
            raise ValueError(
                f"'database.url' references '{env_key}' -> '{file_path}', but file could not be read: {e}"
            ) from e
        return resolved.strip()

    return env_value


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


def _yaml_quote(value: str) -> str:
    """Quote a YAML scalar deterministically for safe inline emission."""
    return yaml.safe_dump(
        value,
        default_style='"',
        default_flow_style=True,
        allow_unicode=False,
    ).strip()


__all__ = [
    "DbsliceConfig",
    "DatabaseConfig",
    "ExtractionConfig",
    "AnonymizationConfig",
    "ComplianceConfig",
    "OutputConfig",
    "PerformanceConfig",
    "StreamingConfig",
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

    schema: str | None = None
    """PostgreSQL schema name (default: 'public')."""

    options: dict[str, str] = field(default_factory=dict)
    """Additional connection options merged into URL query parameters."""


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

    validate: bool = True
    """Validate extraction for referential integrity."""

    fail_on_validation_error: bool = False
    """Fail extraction when validation errors are detected."""

    max_rows_per_table: int | None = None
    """Global limit on rows per table (None = unlimited)."""

    allow_unsafe_where: bool = False
    """Allow seed WHERE clauses with subqueries (trusted inputs only)."""


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

    patterns: dict[str, str] = field(default_factory=dict)
    """
    Wildcard anonymization mappings.
    Format: {"table_glob.column_glob": "faker_provider"}
    Example: {"users.*_email": "email", "*.phone*": "phone_number"}
    """

    security_null_fields: list[str] = field(default_factory=list)
    """
    Wildcard field list to force NULL for sensitive values.
    Format: ["table_glob.column_glob"]
    Example: ["users.password*", "*.api_key"]
    """

    deterministic: bool = True
    """Use deterministic anonymization (same input → same output). Set to false for stronger privacy."""


@dataclass
class ComplianceConfig:
    """Compliance configuration."""

    profiles: list[str] = field(default_factory=list)
    """Compliance profiles to apply (e.g., ['gdpr', 'hipaa', 'pci-dss'])."""

    strict: bool = False
    """Fail extraction if uncovered PII is detected by value scanning."""

    generate_manifest: bool = False
    """Generate an audit manifest alongside extraction output."""

    policy_mode: str = "off"
    """Policy gate mode: off, standard, or strict."""

    allow_url_patterns: list[str] = field(default_factory=list)
    """Allow-list regex patterns for source database URLs."""

    deny_url_patterns: list[str] = field(default_factory=list)
    """Deny-list regex patterns for source database URLs."""

    required_sslmode: str | None = None
    """Required PostgreSQL sslmode query parameter value."""

    require_ci: bool = False
    """Require CI environment for compliance-active extraction."""

    sign_manifest: bool = False
    """Sign compliance manifests with HMAC."""

    manifest_key_env: str = "DBSLICE_MANIFEST_SIGNING_KEY"
    """Environment variable name containing HMAC signing key."""


@dataclass
class OutputConfig:
    """Output format configuration."""

    format: str = "sql"
    """Output format: 'sql', 'json', or 'csv'."""

    include_transaction: bool = True
    """Wrap SQL output in transaction block."""

    include_truncate: bool = False
    """Include TRUNCATE TABLE statements before inserts."""

    disable_fk_checks: bool = False
    """Disable FK checks while importing generated SQL."""

    file_mode: int = DEFAULT_OUTPUT_FILE_MODE
    """Permissions mode for output files (octal)."""

    json_mode: str = "auto"
    """JSON output mode: auto, single, or per-table."""

    json_pretty: bool = True
    """Enable pretty-printed JSON output."""

    csv_mode: str = "auto"
    """CSV output mode: auto, single, or per-table."""

    csv_delimiter: str = ","
    """CSV delimiter character."""


@dataclass
class StreamingConfig:
    """Streaming performance configuration."""

    enabled: bool = False
    threshold: int = DEFAULT_STREAMING_THRESHOLD
    chunk_size: int = DEFAULT_STREAMING_CHUNK_SIZE


@dataclass
class PerformanceConfig:
    """Performance-related configuration."""

    profile: bool = False
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    batch_size: int | None = None


@dataclass
class TableOverride:
    """Per-table extraction overrides."""

    skip: bool = False
    """Skip this table entirely."""

    depth: int | None = None
    """Per-table max depth override for downward traversal."""

    direction: str | None = None
    """Per-table traversal direction override: up, down, or both."""

    max_rows: int | None = None
    """Limit rows extracted from this table."""

    anonymize_fields: dict[str, str] = field(default_factory=dict)
    """Deprecated per-table anonymization mappings (column -> provider)."""


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

    version: str | None = None
    """Optional config schema version tag (currently informational)."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    anonymization: AnonymizationConfig = field(default_factory=AnonymizationConfig)
    compliance: ComplianceConfig = field(default_factory=ComplianceConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
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
        _validate_unknown_keys("root", data, _TOP_LEVEL_KEYS)

        database_data = data.get("database", {})
        if not isinstance(database_data, dict):
            raise ValueError("'database' section must be a mapping")
        _validate_unknown_keys("database", database_data, _DATABASE_KEYS)

        database_options = _normalize_database_options(database_data.get("options"))
        database_url = _resolve_database_url(database_data.get("url"))

        database = DatabaseConfig(
            url=database_url,
            schema=database_data.get("schema"),
            options=database_options,
        )

        extraction_data = data.get("extraction", {})
        if not isinstance(extraction_data, dict):
            raise ValueError("'extraction' section must be a mapping")
        _validate_unknown_keys("extraction", extraction_data, _EXTRACTION_KEYS)

        extraction = ExtractionConfig(
            default_depth=extraction_data.get("default_depth", DEFAULT_TRAVERSAL_DEPTH),
            direction=extraction_data.get("direction", "both"),
            exclude_tables=extraction_data.get("exclude_tables", []),
            passthrough_tables=extraction_data.get("passthrough_tables", []),
            validate=extraction_data.get("validate", True),
            fail_on_validation_error=extraction_data.get("fail_on_validation_error", False),
            max_rows_per_table=extraction_data.get("max_rows_per_table"),
            allow_unsafe_where=extraction_data.get("allow_unsafe_where", False),
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
        if not isinstance(extraction.validate, bool):
            raise ValueError("'extraction.validate' must be true or false")
        if not isinstance(extraction.fail_on_validation_error, bool):
            raise ValueError("'extraction.fail_on_validation_error' must be true or false")
        if not isinstance(extraction.allow_unsafe_where, bool):
            raise ValueError("'extraction.allow_unsafe_where' must be true or false")
        if extraction.max_rows_per_table is not None and (
            not isinstance(extraction.max_rows_per_table, int) or extraction.max_rows_per_table <= 0
        ):
            raise ValueError("'extraction.max_rows_per_table' must be a positive integer")

        anon_data = data.get("anonymization", {})
        if not isinstance(anon_data, dict):
            raise ValueError("'anonymization' section must be a mapping")

        _validate_unknown_keys("anonymization", anon_data, _ANONYMIZATION_KEYS)

        fields = anon_data.get("fields", {})
        if not isinstance(fields, dict):
            raise ValueError("'anonymization.fields' must be a mapping")
        for field_name, provider in fields.items():
            _validate_exact_field_key(field_name, "'anonymization.fields'")
            _validate_faker_provider(provider)

        patterns = anon_data.get("patterns", {})
        if not isinstance(patterns, dict):
            raise ValueError("'anonymization.patterns' must be a mapping")
        for pattern, provider in patterns.items():
            _validate_glob_field_pattern(pattern, "'anonymization.patterns'")
            _validate_faker_provider(provider)

        security_null_fields = anon_data.get("security_null_fields", [])
        if not isinstance(security_null_fields, list):
            raise ValueError("'anonymization.security_null_fields' must be a list")
        for pattern in security_null_fields:
            _validate_glob_field_pattern(pattern, "'anonymization.security_null_fields'")

        deterministic_val = anon_data.get("deterministic", True)
        if not isinstance(deterministic_val, bool):
            raise ValueError("'anonymization.deterministic' must be true or false")

        anonymization = AnonymizationConfig(
            enabled=anon_data.get("enabled", False),
            seed=anon_data.get("seed"),
            fields=fields,
            patterns=patterns,
            security_null_fields=security_null_fields,
            deterministic=deterministic_val,
        )

        compliance_data = data.get("compliance", {})
        if not isinstance(compliance_data, dict):
            raise ValueError("'compliance' section must be a mapping")
        _validate_unknown_keys("compliance", compliance_data, _COMPLIANCE_KEYS)

        compliance_profiles_raw = compliance_data.get("profiles", [])
        if not isinstance(compliance_profiles_raw, list):
            raise ValueError("'compliance.profiles' must be a list")

        # Validate profile names
        from dbslice.compliance.profiles import get_profile
        for profile_name in compliance_profiles_raw:
            if not isinstance(profile_name, str):
                raise ValueError("'compliance.profiles' entries must be strings")
            get_profile(profile_name)  # Raises ValueError if unknown

        compliance_strict = compliance_data.get("strict", False)
        if not isinstance(compliance_strict, bool):
            raise ValueError("'compliance.strict' must be true or false")
        compliance_manifest = compliance_data.get("generate_manifest", False)
        if not isinstance(compliance_manifest, bool):
            raise ValueError("'compliance.generate_manifest' must be true or false")
        compliance_policy_mode = compliance_data.get("policy_mode", "off")
        if compliance_policy_mode not in {"off", "standard", "strict"}:
            raise ValueError("'compliance.policy_mode' must be one of: off, standard, strict")

        allow_url_patterns = compliance_data.get("allow_url_patterns", [])
        if not isinstance(allow_url_patterns, list) or not all(
            isinstance(item, str) for item in allow_url_patterns
        ):
            raise ValueError("'compliance.allow_url_patterns' must be a list of strings")
        for pattern in allow_url_patterns:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(
                    f"'compliance.allow_url_patterns' contains invalid regex '{pattern}': {e}"
                ) from e

        deny_url_patterns = compliance_data.get("deny_url_patterns", [])
        if not isinstance(deny_url_patterns, list) or not all(
            isinstance(item, str) for item in deny_url_patterns
        ):
            raise ValueError("'compliance.deny_url_patterns' must be a list of strings")
        for pattern in deny_url_patterns:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(
                    f"'compliance.deny_url_patterns' contains invalid regex '{pattern}': {e}"
                ) from e

        required_sslmode = compliance_data.get("required_sslmode")
        if required_sslmode is not None and (
            not isinstance(required_sslmode, str) or not required_sslmode.strip()
        ):
            raise ValueError("'compliance.required_sslmode' must be a non-empty string when set")

        require_ci = compliance_data.get("require_ci", False)
        if not isinstance(require_ci, bool):
            raise ValueError("'compliance.require_ci' must be true or false")

        sign_manifest = compliance_data.get("sign_manifest", False)
        if not isinstance(sign_manifest, bool):
            raise ValueError("'compliance.sign_manifest' must be true or false")

        manifest_key_env = compliance_data.get("manifest_key_env", "DBSLICE_MANIFEST_SIGNING_KEY")
        if not isinstance(manifest_key_env, str) or not manifest_key_env:
            raise ValueError("'compliance.manifest_key_env' must be a non-empty string")

        compliance = ComplianceConfig(
            profiles=compliance_profiles_raw,
            strict=compliance_strict,
            generate_manifest=compliance_manifest,
            policy_mode=compliance_policy_mode,
            allow_url_patterns=allow_url_patterns,
            deny_url_patterns=deny_url_patterns,
            required_sslmode=required_sslmode,
            require_ci=require_ci,
            sign_manifest=sign_manifest,
            manifest_key_env=manifest_key_env,
        )

        output_data = data.get("output", {})
        if not isinstance(output_data, dict):
            raise ValueError("'output' section must be a mapping")

        _validate_unknown_keys("output", output_data, _OUTPUT_KEYS)

        include_drop_tables_alias = output_data.get("include_drop_tables")
        include_truncate = output_data.get("include_truncate")

        if include_drop_tables_alias is not None:
            logger.warning(
                "'output.include_drop_tables' is deprecated and treated as "
                "'output.include_truncate'. This alias will be removed in a future release."
            )
            if include_truncate is not None and bool(include_truncate) != bool(include_drop_tables_alias):
                raise ValueError(
                    "'output.include_drop_tables' and 'output.include_truncate' disagree. "
                    "Use only 'output.include_truncate'."
                )
            include_truncate = include_drop_tables_alias

        file_mode = output_data.get("file_mode", DEFAULT_OUTPUT_FILE_MODE)
        from dbslice.utils.fileio import parse_file_mode

        try:
            parsed_mode = parse_file_mode(file_mode)
        except ValueError as e:
            raise ValueError(f"'output.file_mode' is invalid: {e}")

        include_truncate_value = include_truncate if include_truncate is not None else False

        output = OutputConfig(
            format=output_data.get("format", "sql"),
            include_transaction=output_data.get("include_transaction", True),
            include_truncate=include_truncate_value,
            disable_fk_checks=output_data.get("disable_fk_checks", False),
            file_mode=parsed_mode,
            json_mode=output_data.get("json_mode", "auto"),
            json_pretty=output_data.get("json_pretty", True),
            csv_mode=output_data.get("csv_mode", "auto"),
            csv_delimiter=output_data.get("csv_delimiter", ","),
        )

        valid_formats = {"sql", "json", "csv"}
        if output.format not in valid_formats:
            raise ValueError(f"'output.format' must be one of: {', '.join(valid_formats)}")
        if not isinstance(output.include_truncate, bool):
            raise ValueError("'output.include_truncate' must be true or false")
        if not isinstance(output.include_transaction, bool):
            raise ValueError("'output.include_transaction' must be true or false")
        if not isinstance(output.disable_fk_checks, bool):
            raise ValueError("'output.disable_fk_checks' must be true or false")
        if output.json_mode not in {"auto", "single", "per-table"}:
            raise ValueError("'output.json_mode' must be one of: auto, single, per-table")
        if not isinstance(output.json_pretty, bool):
            raise ValueError("'output.json_pretty' must be true or false")
        if output.csv_mode not in {"auto", "single", "per-table"}:
            raise ValueError("'output.csv_mode' must be one of: auto, single, per-table")
        if not isinstance(output.csv_delimiter, str) or len(output.csv_delimiter) != 1:
            raise ValueError("'output.csv_delimiter' must be a single-character string")

        performance_data = data.get("performance", {})
        if not isinstance(performance_data, dict):
            raise ValueError("'performance' section must be a mapping")
        _validate_unknown_keys("performance", performance_data, _PERFORMANCE_KEYS)

        streaming_data = performance_data.get("streaming", {})
        if not isinstance(streaming_data, dict):
            raise ValueError("'performance.streaming' section must be a mapping")
        _validate_unknown_keys("performance.streaming", streaming_data, _STREAMING_KEYS)

        performance = PerformanceConfig(
            profile=performance_data.get("profile", False),
            streaming=StreamingConfig(
                enabled=streaming_data.get("enabled", False),
                threshold=streaming_data.get("threshold", DEFAULT_STREAMING_THRESHOLD),
                chunk_size=streaming_data.get("chunk_size", DEFAULT_STREAMING_CHUNK_SIZE),
            ),
            batch_size=performance_data.get("batch_size"),
        )
        if not isinstance(performance.profile, bool):
            raise ValueError("'performance.profile' must be true or false")
        if not isinstance(performance.streaming.enabled, bool):
            raise ValueError("'performance.streaming.enabled' must be true or false")
        if not isinstance(performance.streaming.threshold, int) or performance.streaming.threshold <= 0:
            raise ValueError("'performance.streaming.threshold' must be a positive integer")
        if not isinstance(performance.streaming.chunk_size, int) or performance.streaming.chunk_size <= 0:
            raise ValueError("'performance.streaming.chunk_size' must be a positive integer")
        if performance.batch_size is not None and (
            not isinstance(performance.batch_size, int) or performance.batch_size <= 0
        ):
            raise ValueError("'performance.batch_size' must be a positive integer")

        tables_data = data.get("tables", {})
        if not isinstance(tables_data, dict):
            raise ValueError("'tables' section must be a mapping")

        tables = {}
        valid_directions = {"up", "down", "both"}
        for table_name, table_config in tables_data.items():
            if not isinstance(table_config, dict):
                raise ValueError(f"Configuration for table '{table_name}' must be a mapping")
            _validate_unknown_keys(f"tables.{table_name}", table_config, _TABLE_OVERRIDE_KEYS)

            skip_value = table_config.get("skip")
            exclude_alias = table_config.get("exclude")
            if skip_value is not None and not isinstance(skip_value, bool):
                raise ValueError(f"'tables.{table_name}.skip' must be true or false")
            if exclude_alias is not None and not isinstance(exclude_alias, bool):
                raise ValueError(f"'tables.{table_name}.exclude' must be true or false")
            if (
                skip_value is not None
                and exclude_alias is not None
                and bool(skip_value) != bool(exclude_alias)
            ):
                raise ValueError(
                    f"'tables.{table_name}.skip' and 'tables.{table_name}.exclude' disagree. "
                    "Use only 'skip'."
                )
            if exclude_alias is not None:
                logger.warning(
                    f"'tables.{table_name}.exclude' is deprecated and treated as "
                    f"'tables.{table_name}.skip'. This alias will be removed in a future release."
                )
            final_skip = bool(skip_value) if skip_value is not None else bool(exclude_alias)

            table_depth = table_config.get("depth")
            if table_depth is not None and (not isinstance(table_depth, int) or table_depth <= 0):
                raise ValueError(f"'tables.{table_name}.depth' must be a positive integer")

            table_direction = table_config.get("direction")
            if table_direction is not None and table_direction not in valid_directions:
                raise ValueError(
                    f"'tables.{table_name}.direction' must be one of: "
                    f"{', '.join(sorted(valid_directions))}"
                )

            max_rows = table_config.get("max_rows")
            if max_rows is not None and (not isinstance(max_rows, int) or max_rows <= 0):
                raise ValueError(f"'tables.{table_name}.max_rows' must be a positive integer")

            anonymize_fields = table_config.get("anonymize_fields", {})
            if not isinstance(anonymize_fields, dict):
                raise ValueError(f"'tables.{table_name}.anonymize_fields' must be a mapping")
            for column_name, provider in anonymize_fields.items():
                if not isinstance(column_name, str) or not column_name:
                    raise ValueError(
                        f"'tables.{table_name}.anonymize_fields' keys must be non-empty strings"
                    )
                if "." in column_name or "*" in column_name or "?" in column_name:
                    raise ValueError(
                        f"'tables.{table_name}.anonymize_fields' key '{column_name}' must be "
                        "a bare column name without wildcards"
                    )
                _validate_faker_provider(provider)

            tables[table_name] = TableOverride(
                skip=final_skip,
                depth=table_depth,
                direction=table_direction,
                max_rows=max_rows,
                anonymize_fields=anonymize_fields,
            )

        vfk_data = data.get("virtual_foreign_keys", [])
        if not isinstance(vfk_data, list):
            raise ValueError("'virtual_foreign_keys' section must be a list")

        virtual_fks = []
        for i, vfk_config in enumerate(vfk_data):
            if not isinstance(vfk_config, dict):
                raise ValueError(f"Virtual FK #{i + 1} must be a mapping")
            _validate_unknown_keys(
                f"virtual_foreign_keys[{i}]",
                vfk_config,
                _VIRTUAL_FK_KEYS,
            )

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
            version=data.get("version"),
            database=database,
            extraction=extraction,
            anonymization=anonymization,
            compliance=compliance,
            output=output,
            performance=performance,
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
        validate: bool | None = None,
        fail_on_validation_error: bool | None = None,
        profile: bool | None = None,
        stream: bool | None = None,
        stream_threshold: int | None = None,
        stream_chunk_size: int | None = None,
        output_file_mode: int | None = None,
        schema: str | None = None,
        allow_unsafe_where: bool | None = None,
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
            stream: Force streaming mode (from CLI)
            stream_threshold: Auto-stream threshold override (from CLI)
            stream_chunk_size: Streaming chunk size override (from CLI)
            output_file_mode: Output file permissions override (from CLI)
            schema: PostgreSQL schema name override (from CLI)
            allow_unsafe_where: Unsafe WHERE override (from CLI/env)

        Returns:
            ExtractConfig ready for extraction

        Raises:
            ValueError: If required fields are missing
        """
        final_url: str | None
        if database_url is not None:
            final_url = database_url
            if self.database.options:
                logger.warning(
                    "Ignoring 'database.options' because database URL was provided via CLI"
                )
        else:
            final_url = self.database.url
            if final_url and self.database.options:
                final_url = _merge_database_url_options(final_url, self.database.options)

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
        final_validate = validate if validate is not None else self.extraction.validate
        final_fail_on_validation_error = (
            fail_on_validation_error
            if fail_on_validation_error is not None
            else self.extraction.fail_on_validation_error
        )
        final_profile = profile if profile is not None else self.performance.profile
        final_stream = stream if stream is not None else self.performance.streaming.enabled
        final_stream_threshold = (
            stream_threshold
            if stream_threshold is not None
            else self.performance.streaming.threshold
        )
        final_stream_chunk_size = (
            stream_chunk_size
            if stream_chunk_size is not None
            else self.performance.streaming.chunk_size
        )
        final_allow_unsafe_where = (
            allow_unsafe_where
            if allow_unsafe_where is not None
            else self.extraction.allow_unsafe_where
        )
        final_output_file_mode = (
            output_file_mode if output_file_mode is not None else self.output.file_mode
        )

        table_depth_overrides: dict[str, int] = {}
        table_direction_overrides: dict[str, TraversalDirection] = {}
        row_limit_per_table: dict[str, int] = {}
        legacy_table_field_providers: dict[str, str] = {}

        for table_name, override in self.tables.items():
            if override.depth is not None:
                table_depth_overrides[table_name] = override.depth
            if override.direction is not None:
                table_direction_overrides[table_name] = TraversalDirection(override.direction)
            if override.max_rows is not None:
                row_limit_per_table[table_name] = override.max_rows

            if override.anonymize_fields:
                logger.warning(
                    f"'tables.{table_name}.anonymize_fields' is deprecated. "
                    "Use 'anonymization.fields' instead."
                )
                for column_name, provider in override.anonymize_fields.items():
                    field_name = f"{table_name}.{column_name}"
                    legacy_table_field_providers[field_name] = provider

        effective_field_providers: dict[str, str] = {}
        if final_anonymize:
            effective_field_providers.update(legacy_table_field_providers)
            for field_name, provider in self.anonymization.fields.items():
                if field_name in effective_field_providers:
                    logger.warning(
                        f"Both legacy table anonymization and 'anonymization.fields' define "
                        f"'{field_name}'. Using 'anonymization.fields'."
                    )
                effective_field_providers[field_name] = provider

        effective_patterns = dict(self.anonymization.patterns) if final_anonymize else {}
        effective_security_null_fields = (
            list(self.anonymization.security_null_fields) if final_anonymize else []
        )

        # Redact fields: merge config and CLI (only when anonymization is enabled)
        final_redact: list[str] = []
        if final_anonymize and effective_field_providers:
            final_redact.extend(effective_field_providers.keys())
        if redact:
            final_redact.extend(redact)
        final_redact = list(dict.fromkeys(final_redact))

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

        final_schema = schema or self.database.schema

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
            validate=final_validate,
            fail_on_validation_error=final_fail_on_validation_error,
            profile=final_profile,
            stream=final_stream,
            streaming_threshold=final_stream_threshold,
            streaming_chunk_size=final_stream_chunk_size,
            db_batch_size=self.performance.batch_size,
            include_transaction=self.output.include_transaction,
            include_truncate=self.output.include_truncate,
            disable_fk_checks=self.output.disable_fk_checks,
            output_file_mode=final_output_file_mode,
            table_depth_overrides=table_depth_overrides,
            table_direction_overrides=table_direction_overrides,
            row_limit_global=self.extraction.max_rows_per_table,
            row_limit_per_table=row_limit_per_table,
            anonymization_seed=self.anonymization.seed,
            anonymization_field_providers=effective_field_providers,
            anonymization_patterns=effective_patterns,
            security_null_fields=effective_security_null_fields,
            virtual_foreign_keys=virtual_fks,
            schema=final_schema,
            allow_unsafe_where=final_allow_unsafe_where,
            compliance_profiles=self.compliance.profiles,
            compliance_strict=self.compliance.strict,
            generate_manifest=self.compliance.generate_manifest
            or bool(self.compliance.profiles),
            deterministic=self.anonymization.deterministic,
            compliance_policy_mode=self.compliance.policy_mode,
            compliance_allowed_url_patterns=list(self.compliance.allow_url_patterns),
            compliance_denied_url_patterns=list(self.compliance.deny_url_patterns),
            compliance_required_sslmode=self.compliance.required_sslmode,
            compliance_require_ci=self.compliance.require_ci,
            compliance_manifest_sign=self.compliance.sign_manifest,
            compliance_manifest_key_env=self.compliance.manifest_key_env,
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

        if self.version:
            output.append(f'version: "{self.version}"')
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
        if self.database.schema:
            output.append(f"  schema: {self.database.schema}")
        elif include_comments:
            output.append("  # schema: public  # PostgreSQL schema (default: public)")
        if self.database.options:
            output.append("  options:")
            for key, value in self.database.options.items():
                output.append(f"    {key}: {_yaml_quote(value)}")
        output.append("")

        if include_comments:
            output.append("# Extraction behavior")
        output.append("extraction:")
        output.append(f"  default_depth: {self.extraction.default_depth}")
        output.append(f"  direction: {self.extraction.direction}  # up, down, or both")
        output.append(f"  validate: {str(self.extraction.validate).lower()}")
        output.append(
            "  fail_on_validation_error: "
            f"{str(self.extraction.fail_on_validation_error).lower()}"
        )
        if self.extraction.allow_unsafe_where:
            output.append("  allow_unsafe_where: true")
        if self.extraction.exclude_tables:
            output.append("  exclude_tables:")
            for table in self.extraction.exclude_tables:
                output.append(f"    - {table}")
        if self.extraction.max_rows_per_table is not None:
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
        if self.anonymization.patterns:
            output.append("  patterns:")
            for pattern, provider in self.anonymization.patterns.items():
                output.append(f"    {_yaml_quote(pattern)}: {_yaml_quote(provider)}")
        if self.anonymization.security_null_fields:
            output.append("  security_null_fields:")
            for pattern in self.anonymization.security_null_fields:
                output.append(f"    - {_yaml_quote(pattern)}")
        output.append(f"  deterministic: {str(self.anonymization.deterministic).lower()}")
        if include_comments:
            output.append(
                "  # deterministic=false increases privacy but may reduce repeatability"
            )
        output.append("")

        if include_comments:
            output.append("# Compliance settings")
        output.append("compliance:")
        if self.compliance.profiles:
            output.append("  profiles:")
            for profile in self.compliance.profiles:
                output.append(f"    - {profile}")
        else:
            output.append("  profiles: []")
        output.append(f"  strict: {str(self.compliance.strict).lower()}")
        output.append(f"  generate_manifest: {str(self.compliance.generate_manifest).lower()}")
        output.append(f"  policy_mode: {_yaml_quote(self.compliance.policy_mode)}")
        if self.compliance.allow_url_patterns:
            output.append("  allow_url_patterns:")
            for pattern in self.compliance.allow_url_patterns:
                output.append(f"    - {_yaml_quote(pattern)}")
        if self.compliance.deny_url_patterns:
            output.append("  deny_url_patterns:")
            for pattern in self.compliance.deny_url_patterns:
                output.append(f"    - {_yaml_quote(pattern)}")
        if self.compliance.required_sslmode:
            output.append(f"  required_sslmode: {self.compliance.required_sslmode}")
        output.append(f"  require_ci: {str(self.compliance.require_ci).lower()}")
        output.append(f"  sign_manifest: {str(self.compliance.sign_manifest).lower()}")
        output.append(f"  manifest_key_env: {self.compliance.manifest_key_env}")
        output.append("")

        if include_comments:
            output.append("# Output format settings")
        output.append("output:")
        output.append(f"  format: {self.output.format}  # sql, json, or csv")
        output.append(f"  include_transaction: {str(self.output.include_transaction).lower()}")
        output.append(f"  include_truncate: {str(self.output.include_truncate).lower()}")
        output.append(f"  disable_fk_checks: {str(self.output.disable_fk_checks).lower()}")
        output.append(f"  file_mode: \"{self.output.file_mode:o}\"")
        output.append(f"  json_mode: {self.output.json_mode}")
        output.append(f"  json_pretty: {str(self.output.json_pretty).lower()}")
        output.append(f"  csv_mode: {self.output.csv_mode}")
        output.append(f"  csv_delimiter: \"{self.output.csv_delimiter}\"")
        if include_comments:
            output.append(
                "  # Backward-compatible alias accepted: include_drop_tables (deprecated)"
            )
        output.append("")

        if include_comments:
            output.append("# Performance settings")
        output.append("performance:")
        output.append(f"  profile: {str(self.performance.profile).lower()}")
        output.append("  streaming:")
        output.append(f"    enabled: {str(self.performance.streaming.enabled).lower()}")
        output.append(f"    threshold: {self.performance.streaming.threshold}")
        output.append(f"    chunk_size: {self.performance.streaming.chunk_size}")
        if self.performance.batch_size is not None:
            output.append(f"  batch_size: {self.performance.batch_size}")
        output.append("")

        if self.tables:
            if include_comments:
                output.append("# Per-table overrides")
            output.append("tables:")
            for table_name, override in self.tables.items():
                output.append(f"  {table_name}:")
                if override.skip:
                    output.append("    skip: true")
                if override.depth is not None:
                    output.append(f"    depth: {override.depth}")
                if override.direction is not None:
                    output.append(f"    direction: {override.direction}")
                if override.max_rows is not None:
                    output.append(f"    max_rows: {override.max_rows}")
                if override.anonymize_fields:
                    output.append("    anonymize_fields:")
                    for column_name, provider in override.anonymize_fields.items():
                        output.append(f"      {column_name}: {provider}")
                    if include_comments:
                        output.append(
                            "    # Deprecated: prefer top-level anonymization.fields"
                        )
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
