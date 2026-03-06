import hashlib
import secrets
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any

from dbslice.constants import DEFAULT_ANONYMIZATION_SEED
from dbslice.logging import get_logger

logger = get_logger(__name__)

try:
    from faker import Faker

    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    Faker = None  # type: ignore

if TYPE_CHECKING:
    from dbslice.compliance.manifest import ComplianceManifest
    from dbslice.models import SchemaGraph


_DEFAULT_ANONYMIZATION_PATTERNS: dict[str, str] = {
    # Contact information
    "email": "email",
    "phone": "phone_number",
    "mobile": "phone_number",
    "fax": "phone_number",
    "landline": "phone_number",
    # Personal names
    "name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "firstname": "first_name",
    "lastname": "last_name",
    "full_name": "name",
    "fullname": "name",
    # Address fields
    "address": "address",
    "street": "street_address",
    "city": "city",
    "zip": "zipcode",
    "zipcode": "zipcode",
    "postal": "zipcode",
    # Identity documents
    "ssn": "ssn",
    "credit_card": "credit_card_number",
    "card_number": "credit_card_number",
    "card": "credit_card_number",
    "passport": "passport_number",
    "driver_license": "license_plate",
    "driverlicense": "license_plate",
    "license_number": "license_plate",
    # Financial
    "iban": "iban",
    "bank_account": "bban",
    "account_number": "bban",
    "routing_number": "aba",
    "swift": "swift",
    # Network
    "ip_address": "ipv4",
    "ipaddress": "ipv4",
    "ip": "ipv4",
    "ipv6": "ipv6",
    "mac_address": "mac_address",
    # User identity
    "username": "user_name",
    "user_name": "user_name",
    # Personal data
    "dob": "date_of_birth",
    "date_of_birth": "date_of_birth",
    "birthdate": "date_of_birth",
    "birth_date": "date_of_birth",
    # Professional/organizational
    "company": "company",
    "organization": "company",
    "employer": "company",
    "job_title": "job",
    "salary": "random_int",
    "compensation": "random_int",
    "wage": "random_int",
    # Web/URLs
    "url": "url",
    "website": "url",
    "domain": "domain_name",
}

# Fields to NULL instead of fake (security-sensitive)
_SECURITY_NULL_PATTERNS: list[str] = [
    # Authentication
    "password",
    "passwd",
    "pwd",
    "hash",
    "salt",
    # Tokens and secrets
    "token",
    "secret",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "oauth_token",
    "csrf_token",
    "session_id",
    # Keys
    "private_key",
    "privatekey",
    "public_key",
    "publickey",
    "encryption_key",
    "decrypt_key",
    # Cryptographic
    "nonce",
    "signature",
    "certificate",
    "client_secret",
    "oauth_secret",
]


class DeterministicAnonymizer:
    """
    Anonymizes values deterministically - same input always produces same output.

    This preserves referential integrity when the same value appears in multiple
    tables or rows. Uses Faker with deterministic seeding based on input values.
    """

    def __init__(
        self,
        seed: str = DEFAULT_ANONYMIZATION_SEED,
        schema: "SchemaGraph | None" = None,
        deterministic: bool = True,
        manifest: "ComplianceManifest | None" = None,
    ):
        """
        Initialize the anonymizer with a global seed.

        Args:
            seed: Global seed for deterministic anonymization
            schema: Optional schema graph for FK detection (prevents anonymizing FK columns)
            deterministic: If False, use random seeds per value (stronger privacy, no cross-table consistency)
            manifest: Optional compliance manifest to record anonymization actions

        Raises:
            ImportError: If Faker is not installed
        """
        if not FAKER_AVAILABLE:
            raise ImportError(
                "Faker is required for anonymization. Install it with: pip install faker"
            )

        mode = "deterministic" if deterministic else "non-deterministic"
        logger.info("Initializing anonymizer", seed=seed[:20] + "...", mode=mode)
        self.global_seed = seed
        self.deterministic = deterministic
        self.fake = Faker()
        self._cache: dict[tuple, Any] = {}
        self.redact_fields: set[str] = set()  # Set of normalized "table.column"
        self.field_providers: dict[str, str] = {}
        self.custom_patterns: list[tuple[str, str]] = []
        self.fallback_patterns: list[tuple[str, str]] = []
        self.security_null_fields: list[str] = []
        self.schema = schema
        self._fk_columns_cache: dict[str, set[str]] = {}  # Cache of FK columns per table
        self.manifest = manifest
        self._manifest_recorded: set[tuple[str, str]] = set()  # Track which fields we've recorded

    def _normalize_field(self, table: str, column: str) -> str:
        """Return normalized table.column field name for matching."""
        return f"{table}.{column}".lower()

    def _match_glob(self, pattern: str, field: str) -> bool:
        """Case-insensitive shell-style glob match for table.column patterns."""
        return fnmatchcase(field, pattern.lower())

    def _resolve_pattern_provider(
        self, table: str, column: str, patterns: list[tuple[str, str]]
    ) -> str | None:
        """
        Resolve provider from wildcard patterns.

        Resolution policy:
        - Most specific pattern wins (longest non-wildcard literal).
        - Ties are resolved by declaration order (first wins).
        """
        field = self._normalize_field(table, column)
        best_provider: str | None = None
        best_specificity = -1

        for pattern, provider in patterns:
            if not self._match_glob(pattern, field):
                continue

            specificity = sum(1 for ch in pattern if ch not in {"*", "?"})
            if specificity > best_specificity:
                best_provider = provider
                best_specificity = specificity

        return best_provider

    def _resolve_custom_pattern_provider(self, table: str, column: str) -> str | None:
        """Resolve provider from user-defined wildcard patterns."""
        return self._resolve_pattern_provider(table, column, self.custom_patterns)

    def _resolve_fallback_pattern_provider(self, table: str, column: str) -> str | None:
        """Resolve provider from fallback wildcard patterns (e.g., compliance profiles)."""
        return self._resolve_pattern_provider(table, column, self.fallback_patterns)

    def _resolve_exact_field_provider(self, table: str, column: str) -> str | None:
        """Resolve provider from exact field mappings."""
        return self.field_providers.get(self._normalize_field(table, column))

    def _resolve_faker_method(self, table: str, column: str) -> str:
        """
        Resolve faker method with precedence:
        1. Exact field provider mapping
        2. User wildcard pattern mapping
        3. Fallback wildcard pattern mapping
        4. Built-in column substring mapping
        5. pystr fallback
        """
        exact_provider = self._resolve_exact_field_provider(table, column)
        if exact_provider:
            return exact_provider

        pattern_provider = self._resolve_custom_pattern_provider(table, column)
        if pattern_provider:
            return pattern_provider

        fallback_pattern_provider = self._resolve_fallback_pattern_provider(table, column)
        if fallback_pattern_provider:
            return fallback_pattern_provider

        return self.get_faker_method(column)

    def configure(
        self,
        redact_fields: list[str],
        field_providers: dict[str, str] | None = None,
        patterns: dict[str, str] | None = None,
        fallback_patterns: dict[str, str] | None = None,
        security_null_fields: list[str] | None = None,
    ):
        """
        Configure custom anonymization behavior.

        Args:
            redact_fields: List of exact fields in "table.column" format.
            field_providers: Exact field to faker-provider mappings.
            patterns: User wildcard table.column glob to faker-provider mappings.
            fallback_patterns: Lower-priority wildcard mappings (e.g., compliance profiles).
            security_null_fields: Wildcard table.column globs to force NULL.
        """
        self.redact_fields = {field.lower() for field in redact_fields}
        self.field_providers = {
            field.lower(): provider for field, provider in (field_providers or {}).items()
        }
        self.custom_patterns = [
            (pattern.lower(), provider) for pattern, provider in (patterns or {}).items()
        ]
        self.fallback_patterns = [
            (pattern.lower(), provider) for pattern, provider in (fallback_patterns or {}).items()
        ]
        self.security_null_fields = [pattern.lower() for pattern in (security_null_fields or [])]

        logger.info(
            "Anonymizer configured",
            redact_field_count=len(self.redact_fields),
            exact_provider_count=len(self.field_providers),
            user_pattern_count=len(self.custom_patterns),
            fallback_pattern_count=len(self.fallback_patterns),
            security_null_pattern_count=len(self.security_null_fields),
        )

    def _is_foreign_key_column(self, table: str, column: str) -> bool:
        """
        Check if a column is part of a foreign key.

        CRITICAL: Foreign key columns should NEVER be anonymized as this would
        break referential integrity.

        Args:
            table: Table name
            column: Column name

        Returns:
            True if column is part of a foreign key
        """
        if not self.schema:
            return False

        if table not in self._fk_columns_cache:
            fk_columns: set[str] = set()
            for _, fk in self.schema.get_parents(table):
                fk_columns.update(fk.source_columns)
            self._fk_columns_cache[table] = fk_columns

        return column in self._fk_columns_cache[table]

    def should_anonymize(self, table: str, column: str) -> bool:
        """
        Determine if a column should be anonymized.

        Checks explicit redact list and pattern matching.

        CRITICAL: Never anonymizes foreign key columns to preserve referential integrity.

        Args:
            table: Table name
            column: Column name

        Returns:
            True if column should be anonymized
        """
        # NEVER anonymize foreign key columns
        if self._is_foreign_key_column(table, column):
            return False

        full_name = self._normalize_field(table, column)

        # Explicitly marked for redaction
        if full_name in self.redact_fields:
            return True

        # Exact field mapping always enables anonymization
        if full_name in self.field_providers:
            return True

        # Custom wildcard patterns
        if self._resolve_custom_pattern_provider(table, column):
            return True

        # Fallback wildcard patterns (e.g., compliance profiles)
        if self._resolve_fallback_pattern_provider(table, column):
            return True

        # Pattern matching on column name
        col_lower = column.lower()
        for pattern in _DEFAULT_ANONYMIZATION_PATTERNS:
            if pattern in col_lower:
                return True

        return False

    def should_null(self, table: str, column: str) -> bool:
        """
        Determine if a column should be set to NULL (for security-sensitive fields).

        Args:
            table: Table name
            column: Column name

        Returns:
            True if column should be NULLed
        """
        # FK columns are never nulled to preserve referential integrity
        if self._is_foreign_key_column(table, column):
            return False

        field = self._normalize_field(table, column)
        for pattern in self.security_null_fields:
            if self._match_glob(pattern, field):
                return True

        col_lower = column.lower()
        for pattern in _SECURITY_NULL_PATTERNS:
            if pattern in col_lower:
                return True
        return False

    def get_faker_method(self, column: str) -> str:
        """
        Get the appropriate Faker method for a column based on its name.

        Args:
            column: Column name

        Returns:
            Faker method name (e.g., "email", "phone_number")
        """
        col_lower = column.lower()
        for pattern, method in _DEFAULT_ANONYMIZATION_PATTERNS.items():
            if pattern in col_lower:
                return method
        # Default to random string
        return "pystr"

    def anonymize_value(self, value: Any, table: str, column: str) -> Any:
        """
        Anonymize a single value deterministically with caching.

        The same input value will always produce the same anonymized output,
        ensuring referential integrity is preserved. This method uses an
        in-memory cache keyed by (value, column) to ensure consistency.

        Cache Behavior:
            - Cache key: (str(value), column, resolved_faker_method)
            - Same value in same column type gets identical output across tables
            - Example: "john@example.com" in any "email" column → same fake email
            - This preserves referential integrity when values appear multiple times

        Determinism:
            - Uses SHA-256 hash of (global_seed:column:method:value) as Faker seed
            - Column name is included to differentiate same values in different contexts
            - Example: "john" as first_name vs last_name may produce different outputs

        Args:
            value: The value to anonymize
            table: Table name (for pattern detection, not used in cache key)
            column: Column name (for pattern detection and cache key)

        Returns:
            Anonymized value (same output for same input), or original if not sensitive
        """
        if value is None:
            return None

        # FK integrity has highest priority over nulling/anonymization rules.
        if self._is_foreign_key_column(table, column):
            self._record_manifest_fk(table, column)
            return value

        if self.should_null(table, column):
            self._record_manifest_null(table, column)
            return None

        if not self.should_anonymize(table, column):
            self._record_manifest_unmasked(table, column)
            return value

        faker_method = self._resolve_faker_method(table, column)
        self._record_manifest_masked(table, column, faker_method)

        # Check for custom compliance transformers first (these take the value as input)
        custom_fn = self._get_custom_transformer(faker_method)
        if custom_fn is not None:
            return custom_fn(value)

        if self.deterministic:
            cache_key = (str(value), column, faker_method)
            if cache_key in self._cache:
                return self._cache[cache_key]

            # Generate deterministic seed from global seed + column/provider + original value
            # Including column name ensures same value in different column types gets different output
            hash_input = f"{self.global_seed}:{column}:{faker_method}:{value}".encode()
            seed_int = int.from_bytes(hashlib.sha256(hash_input).digest()[:8], "big")
            self.fake.seed_instance(seed_int)

            try:
                anonymized = getattr(self.fake, faker_method)()
            except (AttributeError, TypeError):
                anonymized = self.fake.pystr()

            self._cache[cache_key] = anonymized
            return anonymized
        else:
            seed_int = int.from_bytes(secrets.token_bytes(8), "big")
            self.fake.seed_instance(seed_int)

            try:
                anonymized = getattr(self.fake, faker_method)()
            except (AttributeError, TypeError):
                anonymized = self.fake.pystr()

            return anonymized

    def anonymize_row(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        """
        Anonymize all sensitive fields in a row.

        Args:
            table: Table name
            row: Dictionary of column name -> value

        Returns:
            New dictionary with sensitive fields anonymized
        """
        anonymized_count = 0
        result = {}

        for column, value in row.items():
            anonymized_value = self.anonymize_value(value, table, column)
            if anonymized_value != value:
                anonymized_count += 1
            result[column] = anonymized_value

        if anonymized_count > 0:
            logger.debug(
                "Anonymized row",
                table=table,
                anonymized_fields=anonymized_count,
                total_fields=len(row),
            )

        return result

    def get_statistics(self) -> dict[str, int]:
        """
        Get anonymization statistics.

        Returns:
            Dictionary with cache size and other stats
        """
        return {
            "cache_size": len(self._cache),
            "redact_fields_count": len(self.redact_fields),
            "exact_provider_count": len(self.field_providers),
            "pattern_count": len(self.custom_patterns),
            "fallback_pattern_count": len(self.fallback_patterns),
            "security_null_pattern_count": len(self.security_null_fields),
        }

    @staticmethod
    def _get_custom_transformer(method_name: str) -> Any | None:
        """Look up a custom compliance transformer function by name."""
        from dbslice.compliance.transformers import CUSTOM_TRANSFORMERS

        return CUSTOM_TRANSFORMERS.get(method_name)

    def _record_manifest_masked(self, table: str, column: str, method: str) -> None:
        """Record a masked field in the manifest (once per table.column)."""
        if not self.manifest:
            return
        key = (table, column)
        if key not in self._manifest_recorded:
            self._manifest_recorded.add(key)
            self.manifest.record_masked_field(table, column, method)

    def _record_manifest_null(self, table: str, column: str) -> None:
        """Record a NULLed field in the manifest (once per table.column)."""
        if not self.manifest:
            return
        key = (table, column)
        if key not in self._manifest_recorded:
            self._manifest_recorded.add(key)
            self.manifest.record_nulled_field(table, column, "security_null_pattern")

    def _record_manifest_fk(self, table: str, column: str) -> None:
        """Record a preserved FK field in the manifest (once per table.column)."""
        if not self.manifest:
            return
        key = (table, column)
        if key not in self._manifest_recorded:
            self._manifest_recorded.add(key)
            self.manifest.record_fk_preserved(table, column)

    def _record_manifest_unmasked(self, table: str, column: str) -> None:
        """Record an unmasked field in the manifest (once per table.column)."""
        if not self.manifest:
            return
        key = (table, column)
        if key not in self._manifest_recorded:
            self._manifest_recorded.add(key)
            self.manifest.record_unmasked_field(table, column)
