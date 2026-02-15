import hashlib
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
    "driver_license": "driver_license",
    "driverlicense": "driver_license",
    "license_number": "driver_license",
    # Financial
    "iban": "iban",
    "bank_account": "bban",
    "account_number": "bban",
    "routing_number": "routing_number",
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

    def __init__(self, seed: str = DEFAULT_ANONYMIZATION_SEED, schema: "SchemaGraph | None" = None):
        """
        Initialize the anonymizer with a global seed.

        Args:
            seed: Global seed for deterministic anonymization
            schema: Optional schema graph for FK detection (prevents anonymizing FK columns)

        Raises:
            ImportError: If Faker is not installed
        """
        if not FAKER_AVAILABLE:
            raise ImportError(
                "Faker is required for anonymization. Install it with: pip install faker"
            )

        logger.info("Initializing anonymizer", seed=seed[:20] + "...")  # Truncate seed in logs
        self.global_seed = seed
        self.fake = Faker()
        self._cache: dict[tuple, Any] = {}
        self.redact_fields: set[str] = set()  # Set of "table.column"
        self.schema = schema
        self._fk_columns_cache: dict[str, set[str]] = {}  # Cache of FK columns per table

    def configure(self, redact_fields: list[str]):
        """
        Configure additional fields to redact.

        Args:
            redact_fields: List of fields in "table.column" format
        """
        self.redact_fields = set(redact_fields)
        logger.info("Anonymizer configured", redact_field_count=len(redact_fields))

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
            table_info = self.schema.get_table(table)
            if table_info:
                for fk in table_info.foreign_keys:
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

        full_name = f"{table}.{column}"

        # Explicitly marked for redaction
        if full_name in self.redact_fields:
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
            - Cache key: (str(value), column)
            - Same value in same column type gets identical output across tables
            - Example: "john@example.com" in any "email" column → same fake email
            - This preserves referential integrity when values appear multiple times

        Determinism:
            - Uses SHA-256 hash of (global_seed:column:value) as Faker seed
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

        if self.should_null(table, column):
            return None

        if not self.should_anonymize(table, column):
            return value

        cache_key = (str(value), column)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Generate deterministic seed from global seed + column name + original value
        # Including column name ensures same value in different column types gets different output
        hash_input = f"{self.global_seed}:{column}:{value}".encode()
        seed_int = int.from_bytes(hashlib.sha256(hash_input).digest()[:8], "big")

        self.fake.seed_instance(seed_int)
        faker_method = self.get_faker_method(column)

        try:
            anonymized = getattr(self.fake, faker_method)()
        except (AttributeError, TypeError):
            # Fallback if Faker method doesn't exist or fails
            anonymized = self.fake.pystr()

        self._cache[cache_key] = anonymized
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
        }
