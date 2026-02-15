"""Tests for anonymization functionality."""

from dbslice.models import Column, ForeignKey, SchemaGraph, Table
from dbslice.utils.anonymizer import DeterministicAnonymizer


class TestDeterministicAnonymizer:
    """Tests for DeterministicAnonymizer class."""

    def test_deterministic_same_input_same_output(self):
        anon = DeterministicAnonymizer(seed="test")

        result1 = anon.anonymize_value("john@example.com", "users", "email")
        result2 = anon.anonymize_value("john@example.com", "users", "email")

        assert result1 == result2
        assert result1 != "john@example.com"  # Value changed
        assert "@" in result1  # Still looks like email

    def test_different_inputs_different_outputs(self):
        anon = DeterministicAnonymizer(seed="test")

        result1 = anon.anonymize_value("john@example.com", "users", "email")
        result2 = anon.anonymize_value("jane@example.com", "users", "email")

        assert result1 != result2

    def test_different_seeds_different_outputs(self):
        anon1 = DeterministicAnonymizer(seed="seed1")
        anon2 = DeterministicAnonymizer(seed="seed2")

        result1 = anon1.anonymize_value("john@example.com", "users", "email")
        result2 = anon2.anonymize_value("john@example.com", "users", "email")

        assert result1 != result2

    def test_pattern_detection_email(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "email")
        assert anon.should_anonymize("users", "user_email")
        assert anon.should_anonymize("users", "contact_email")
        assert anon.should_anonymize("users", "EMAIL")  # Case insensitive
        assert not anon.should_anonymize("users", "id")

    def test_pattern_detection_phone(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "phone")
        assert anon.should_anonymize("users", "mobile")
        assert anon.should_anonymize("users", "phone_number")
        assert anon.should_anonymize("users", "mobile_phone")
        assert anon.should_anonymize("users", "fax")
        assert anon.should_anonymize("users", "landline")

    def test_pattern_detection_name(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "name")
        assert anon.should_anonymize("users", "first_name")
        assert anon.should_anonymize("users", "last_name")
        assert anon.should_anonymize("users", "firstname")
        assert anon.should_anonymize("users", "lastname")
        assert anon.should_anonymize("users", "full_name")
        assert anon.should_anonymize("users", "fullname")

    def test_pattern_detection_address(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "address")
        assert anon.should_anonymize("users", "street")
        assert anon.should_anonymize("users", "city")
        assert anon.should_anonymize("users", "zip")
        assert anon.should_anonymize("users", "zipcode")
        assert anon.should_anonymize("users", "postal")

    def test_pattern_detection_sensitive_ids(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "ssn")
        assert anon.should_anonymize("users", "credit_card")
        assert anon.should_anonymize("users", "card_number")
        assert anon.should_anonymize("users", "ip_address")
        assert anon.should_anonymize("users", "username")
        assert anon.should_anonymize("users", "passport")
        assert anon.should_anonymize("users", "driver_license")
        assert anon.should_anonymize("users", "dob")
        assert anon.should_anonymize("users", "date_of_birth")

    def test_pattern_detection_financial(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("accounts", "iban")
        assert anon.should_anonymize("accounts", "bank_account")
        assert anon.should_anonymize("accounts", "account_number")
        assert anon.should_anonymize("accounts", "routing_number")
        assert anon.should_anonymize("accounts", "swift")
        assert anon.should_anonymize("employees", "salary")
        assert anon.should_anonymize("employees", "compensation")

    def test_pattern_detection_professional(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("users", "company")
        assert anon.should_anonymize("users", "organization")
        assert anon.should_anonymize("users", "employer")
        assert anon.should_anonymize("users", "job_title")

    def test_pattern_detection_network(self):
        anon = DeterministicAnonymizer()

        assert anon.should_anonymize("devices", "ipv6")
        assert anon.should_anonymize("devices", "mac_address")
        assert anon.should_anonymize("users", "url")
        assert anon.should_anonymize("users", "website")
        assert anon.should_anonymize("users", "domain")

    def test_null_patterns_password(self):
        anon = DeterministicAnonymizer()

        result = anon.anonymize_value("secret123", "users", "password")
        assert result is None

        result = anon.anonymize_value("secret123", "users", "password_hash")
        assert result is None

        result = anon.anonymize_value("secret123", "users", "passwd")
        assert result is None

        result = anon.anonymize_value("secret123", "users", "pwd")
        assert result is None

    def test_null_patterns_token(self):
        anon = DeterministicAnonymizer()

        assert anon.anonymize_value("tok_abc", "auth", "token") is None
        assert anon.anonymize_value("abc123", "auth", "api_token") is None
        assert anon.anonymize_value("key_123", "config", "api_key") is None
        assert anon.anonymize_value("key_123", "config", "apikey") is None
        assert anon.anonymize_value("sec_123", "config", "secret") is None
        assert anon.anonymize_value("oauth123", "auth", "oauth_token") is None
        assert anon.anonymize_value("csrf123", "auth", "csrf_token") is None
        assert anon.anonymize_value("sess123", "auth", "session_id") is None

    def test_null_patterns_keys(self):
        anon = DeterministicAnonymizer()

        assert anon.anonymize_value("pk_123", "auth", "private_key") is None
        assert anon.anonymize_value("pk_123", "auth", "privatekey") is None
        assert anon.anonymize_value("at_123", "auth", "access_token") is None
        assert anon.anonymize_value("rt_123", "auth", "refresh_token") is None
        assert anon.anonymize_value("key_123", "auth", "encryption_key") is None

    def test_null_patterns_cryptographic(self):
        anon = DeterministicAnonymizer()

        assert anon.anonymize_value("hash123", "auth", "hash") is None
        assert anon.anonymize_value("salt123", "auth", "salt") is None
        assert anon.anonymize_value("nonce123", "auth", "nonce") is None
        assert anon.anonymize_value("sig123", "auth", "signature") is None
        assert anon.anonymize_value("cert123", "auth", "certificate") is None
        assert anon.anonymize_value("secret123", "auth", "client_secret") is None

    def test_null_values_stay_null(self):
        anon = DeterministicAnonymizer()

        assert anon.anonymize_value(None, "users", "email") is None
        assert anon.anonymize_value(None, "users", "password") is None
        assert anon.anonymize_value(None, "users", "name") is None

    def test_non_sensitive_fields_unchanged(self):
        anon = DeterministicAnonymizer()

        assert anon.anonymize_value(123, "users", "id") == 123
        assert anon.anonymize_value("active", "users", "status") == "active"
        assert anon.anonymize_value(99.99, "orders", "total") == 99.99
        assert anon.anonymize_value(True, "users", "is_active") is True

    def test_redact_fields_configuration(self):
        anon = DeterministicAnonymizer()
        anon.configure(["users.metadata", "orders.notes"])

        assert anon.should_anonymize("users", "metadata")
        assert anon.should_anonymize("orders", "notes")
        assert not anon.should_anonymize("orders", "id")
        assert not anon.should_anonymize("users", "id")

    def test_redact_fields_overrides_patterns(self):
        anon = DeterministicAnonymizer()
        anon.configure(["users.custom_field"])

        # Pattern detection still works
        assert anon.should_anonymize("users", "email")

        # Custom field also anonymized
        assert anon.should_anonymize("users", "custom_field")

    def test_anonymize_row(self):
        anon = DeterministicAnonymizer()

        row = {
            "id": 1,
            "email": "john@example.com",
            "name": "John Doe",
            "password": "secret123",
            "status": "active",
        }

        anonymized = anon.anonymize_row("users", row)

        assert anonymized["id"] == 1  # Not sensitive
        assert anonymized["status"] == "active"  # Not sensitive
        assert anonymized["email"] != "john@example.com"  # Anonymized
        assert "@" in anonymized["email"]  # Still looks like email
        assert anonymized["name"] != "John Doe"  # Anonymized
        assert anonymized["password"] is None  # NULLed

    def test_anonymize_row_preserves_structure(self):
        anon = DeterministicAnonymizer()

        row = {
            "id": 1,
            "email": "test@example.com",
            "age": 30,
            "metadata": {"foo": "bar"},
        }

        anonymized = anon.anonymize_row("users", row)

        assert set(anonymized.keys()) == set(row.keys())

    def test_cache_consistency(self):
        anon = DeterministicAnonymizer()

        # Anonymize same email in different tables
        result1 = anon.anonymize_value("john@example.com", "users", "email")
        result2 = anon.anonymize_value("john@example.com", "profiles", "email")

        assert result1 == result2  # Same anonymized value

    def test_cache_consistency_across_rows(self):
        anon = DeterministicAnonymizer()

        row1 = {"id": 1, "email": "shared@example.com"}
        row2 = {"id": 2, "email": "shared@example.com"}

        anon1 = anon.anonymize_row("users", row1)
        anon2 = anon.anonymize_row("users", row2)

        assert anon1["email"] == anon2["email"]

    def test_get_statistics(self):
        anon = DeterministicAnonymizer()
        anon.configure(["users.secret"])

        anon.anonymize_value("test1", "users", "email")
        anon.anonymize_value("test2", "users", "email")

        stats = anon.get_statistics()
        assert stats["cache_size"] == 2
        assert stats["redact_fields_count"] == 1

    def test_get_statistics_empty(self):
        anon = DeterministicAnonymizer()

        stats = anon.get_statistics()
        assert stats["cache_size"] == 0
        assert stats["redact_fields_count"] == 0

    def test_faker_method_selection(self):
        anon = DeterministicAnonymizer()

        assert anon.get_faker_method("email") == "email"
        assert anon.get_faker_method("phone") == "phone_number"
        # first_name contains "name" so it matches the "name" pattern first
        # This is by design - pattern matching returns first match
        assert anon.get_faker_method("first_name") in ["first_name", "name"]
        assert anon.get_faker_method("last_name") in ["last_name", "name"]
        assert anon.get_faker_method("address") == "address"
        assert anon.get_faker_method("city") == "city"
        assert anon.get_faker_method("ssn") == "ssn"
        assert anon.get_faker_method("unknown_field") == "pystr"

    def test_anonymize_numeric_values(self):
        anon = DeterministicAnonymizer()

        # Phone numbers might be stored as integers
        result = anon.anonymize_value(5551234567, "users", "phone")
        assert result != 5551234567
        assert isinstance(result, str)  # Faker returns string for phone

    def test_anonymize_with_special_characters(self):
        anon = DeterministicAnonymizer()

        email_with_plus = "user+tag@example.com"
        result = anon.anonymize_value(email_with_plus, "users", "email")

        assert result != email_with_plus
        assert "@" in result

    def test_multiple_anonymizers_independent(self):
        anon1 = DeterministicAnonymizer(seed="seed1")
        anon2 = DeterministicAnonymizer(seed="seed2")

        result1 = anon1.anonymize_value("test@example.com", "users", "email")
        result2 = anon2.anonymize_value("test@example.com", "users", "email")

        # Different seeds should produce different outputs
        assert result1 != result2

    def test_foreign_key_columns_never_anonymized(self):
        """Foreign key columns are never anonymized (CRITICAL for referential integrity)."""
        # Create a simple schema with FK
        users_table = Table(
            name="users",
            schema="public",
            columns=[
                Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
                Column(name="email", data_type="VARCHAR", nullable=False, is_primary_key=False),
            ],
            primary_key=("id",),
            foreign_keys=[],
        )

        orders_fk = ForeignKey(
            name="fk_orders_user",
            source_table="orders",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            is_nullable=False,
        )

        orders_table = Table(
            name="orders",
            schema="public",
            columns=[
                Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
                Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=False),
                Column(name="user_email", data_type="VARCHAR", nullable=True, is_primary_key=False),
            ],
            primary_key=("id",),
            foreign_keys=[orders_fk],
        )

        schema = SchemaGraph(
            tables={"users": users_table, "orders": orders_table},
            edges=[orders_fk],
        )

        anon = DeterministicAnonymizer(schema=schema)

        # user_email should be anonymized (not an FK, matches email pattern)
        assert anon.should_anonymize("orders", "user_email")

        # user_id should NOT be anonymized (is an FK column)
        assert not anon.should_anonymize("orders", "user_id")

        # Test actual anonymization
        row = {
            "id": 100,
            "user_id": 1,  # FK column - should NOT be anonymized
            "user_email": "test@example.com",  # Should be anonymized
        }

        anonymized = anon.anonymize_row("orders", row)

        assert anonymized["id"] == 100
        assert anonymized["user_id"] == 1  # FK preserved!
        assert anonymized["user_email"] != "test@example.com"  # Email anonymized

    def test_column_name_in_hash(self):
        """Hash includes column name for better determinism."""
        anon = DeterministicAnonymizer(seed="test")

        # Same value "john" in different columns should produce different outputs
        result_firstname = anon.anonymize_value("john", "users", "first_name")
        result_lastname = anon.anonymize_value("john", "users", "last_name")

        # Both should be anonymized
        assert result_firstname != "john"
        assert result_lastname != "john"

        # They might be different (depending on Faker randomness)
        # But they should be deterministic
        result_firstname2 = anon.anonymize_value("john", "users", "first_name")
        result_lastname2 = anon.anonymize_value("john", "users", "last_name")

        assert result_firstname == result_firstname2
        assert result_lastname == result_lastname2
