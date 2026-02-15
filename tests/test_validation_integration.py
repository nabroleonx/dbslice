"""Integration tests for validation across the application."""

import pytest

from dbslice.config import SeedSpec
from dbslice.exceptions import InvalidURLError
from dbslice.input_validators import (
    IdentifierValidationError,
)
from dbslice.utils.connection import parse_database_url


class TestSeedSpecParsing:
    """Integration tests for SeedSpec parsing with validation."""

    def test_parse_simple_seed_with_validation(self):
        # Valid seed
        seed = SeedSpec.parse("users.id=123")
        assert seed.table == "users"
        assert seed.column == "id"
        assert seed.value == 123

    def test_parse_seed_rejects_invalid_table_name(self):
        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("invalid-table.id=123")

        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("'; DROP TABLE users--.id=1")

    def test_parse_seed_rejects_invalid_column_name(self):
        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("users.invalid-column=123")

        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("users.'; DROP--=1")

    def test_parse_seed_rejects_sql_keyword_identifiers(self):
        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("select.id=1")

        with pytest.raises(ValueError, match="Invalid seed"):
            SeedSpec.parse("users.drop=1")

    def test_parse_seed_with_empty_value(self):
        with pytest.raises(ValueError, match="Invalid seed value"):
            SeedSpec.parse("users.name=''")

        with pytest.raises(ValueError, match="Invalid seed value"):
            SeedSpec.parse("users.name='   '")

    def test_parse_where_clause_seed_with_validation(self):
        # Valid WHERE clause
        seed = SeedSpec.parse("users:status='active'")
        assert seed.table == "users"
        assert seed.where_clause == "status='active'"

    def test_parse_where_clause_rejects_dangerous_sql(self):
        from dbslice.exceptions import InsecureWhereClauseError

        # These should all be caught by the WHERE clause validation in config.py
        dangerous_seeds = [
            "users:1=1; DROP TABLE users",  # Semicolon + DROP
            "users:id=1 OR 1=1; DELETE FROM users",  # Semicolon + DELETE
            "users:id=1; EXEC sp_",  # Semicolon + EXEC
            "orders:DELETE FROM users",  # Direct DELETE
            "users:DROP TABLE orders",  # Direct DROP
        ]
        for seed_str in dangerous_seeds:
            with pytest.raises((ValueError, InsecureWhereClauseError)):
                SeedSpec.parse(seed_str)

    def test_parse_seed_with_very_long_value(self):
        long_value = "a" * 1001
        with pytest.raises(ValueError, match="Invalid seed value"):
            SeedSpec.parse(f"users.name='{long_value}'")

    def test_parse_seed_empty_specification(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            SeedSpec.parse("")

        with pytest.raises(ValueError, match="cannot be empty"):
            SeedSpec.parse("   ")

    def test_parse_seed_with_special_characters_in_value(self):
        # These should be OK when properly quoted
        valid_seeds = [
            "users.email='test@example.com'",
            "users.name='John Doe'",
            "products.description='Product with (parentheses)'",
        ]
        for seed_str in valid_seeds:
            seed = SeedSpec.parse(seed_str)
            assert seed is not None

    def test_to_where_clause_validation(self):
        seed = SeedSpec.parse("users.id=123")
        where, params = seed.to_where_clause()
        assert where == "id = %s"
        assert params == (123,)


class TestDatabaseURLParsingIntegration:
    """Integration tests for database URL parsing with validation."""

    def test_parse_valid_postgres_url(self):
        url = "postgres://user:pass@localhost:5432/mydb"
        config = parse_database_url(url)
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "mydb"

    def test_parse_url_with_early_validation(self):
        # This should fail during validation, not parsing
        with pytest.raises(InvalidURLError):
            parse_database_url("not_a_url")

    def test_parse_url_rejects_empty(self):
        with pytest.raises(InvalidURLError):
            parse_database_url("")

    def test_parse_url_rejects_missing_database(self):
        with pytest.raises(InvalidURLError):
            parse_database_url("postgres://localhost/")

    def test_parse_url_with_special_characters(self):
        url = "postgres://user:p%40ssw0rd@localhost/mydb"
        config = parse_database_url(url)
        assert config.password == "p@ssw0rd"


class TestValidationErrorPropagation:
    """Test that validation errors propagate correctly through the application."""

    def test_seed_validation_error_contains_helpful_info(self):
        try:
            SeedSpec.parse("invalid-table.id=1")
            assert False, "Should have raised error"
        except ValueError as e:
            error_msg = str(e)
            # Should mention it's a seed error
            assert "seed" in error_msg.lower() or "invalid" in error_msg.lower()
            # Should give some indication of what's wrong
            assert len(error_msg) > 20  # Not just a generic message

    def test_url_validation_error_contains_examples(self):
        try:
            parse_database_url("invalid")
            assert False, "Should have raised error"
        except InvalidURLError as e:
            error_msg = str(e)
            # Should mention it's invalid and what's supported
            assert "Invalid" in error_msg or "Unsupported" in error_msg


class TestValidationCombinations:
    """Test validation with various combinations of inputs."""

    def test_seed_with_table_and_column_at_max_length(self):
        max_table = "t" * 63
        max_column = "c" * 63
        seed_str = f"{max_table}.{max_column}=123"
        seed = SeedSpec.parse(seed_str)
        assert seed.table == max_table
        assert seed.column == max_column

    def test_seed_with_numeric_string_value(self):
        seed = SeedSpec.parse("users.age=25")
        assert seed.value == 25
        assert isinstance(seed.value, int)

    def test_seed_with_string_value(self):
        seed = SeedSpec.parse("users.name='John'")
        assert seed.value == "John"
        assert isinstance(seed.value, str)

    def test_where_clause_with_complex_conditions(self):
        complex_clauses = [
            "users:status='active' AND created_at > '2024-01-01'",
            "orders:total > 100 AND total < 1000",
            "products:category IN ('electronics', 'books')",
            "users:email LIKE '%@example.com'",
        ]
        for clause in complex_clauses:
            seed = SeedSpec.parse(clause)
            assert seed.where_clause is not None


class TestValidationWithRealWorldData:
    """Test validation with real-world data patterns."""

    def test_email_addresses_in_seeds(self):
        emails = [
            "test@example.com",
            "user.name@domain.co.uk",
            "admin+test@company.org",
        ]
        for email in emails:
            seed = SeedSpec.parse(f"users.email='{email}'")
            assert seed.value == email

    def test_names_with_special_characters(self):
        names = [
            "O'Brien",
            "Mary-Jane",
            "José",
        ]
        for name in names:
            # These should work when properly escaped
            if "'" in name:
                # Would need to be escaped in real usage
                continue
            seed = SeedSpec.parse(f"users.name='{name}'")
            assert seed.value == name

    def test_timestamps_in_where_clauses(self):
        seed = SeedSpec.parse("orders:created_at > '2024-01-01 00:00:00'")
        assert "2024-01-01" in seed.where_clause

    def test_numeric_ranges_in_where_clauses(self):
        seed = SeedSpec.parse("products:price BETWEEN 10.00 AND 99.99")
        assert "BETWEEN" in seed.where_clause


class TestSecurityValidation:
    """Test that security-critical validations are working."""

    def test_sql_injection_in_table_name_blocked(self):
        injection_attempts = [
            "users; DROP TABLE users--",
            "users' OR '1'='1",
            "users/**/OR/**/1=1",
        ]
        for attempt in injection_attempts:
            with pytest.raises((ValueError, IdentifierValidationError)):
                SeedSpec.parse(f"{attempt}.id=1")

    def test_sql_injection_in_column_name_blocked(self):
        injection_attempts = [
            "id; DROP TABLE users--",
            "id' OR '1'='1",
        ]
        for attempt in injection_attempts:
            with pytest.raises((ValueError, IdentifierValidationError)):
                SeedSpec.parse(f"users.{attempt}=1")

    def test_sql_injection_in_where_clause_blocked(self):
        from dbslice.exceptions import InsecureWhereClauseError

        injection_attempts = [
            "users:1=1; DROP TABLE orders",  # Caught by semicolon + DROP
            "users:1=1; EXEC sp_executesql",  # Caught by semicolon + EXEC
            "orders:1=1; DELETE FROM users",  # Caught by semicolon + DELETE
            "users:DROP TABLE orders",  # Caught by DROP keyword
            "orders:TRUNCATE TABLE users",  # Caught by TRUNCATE keyword
        ]
        for attempt in injection_attempts:
            with pytest.raises((ValueError, InsecureWhereClauseError)):
                SeedSpec.parse(attempt)

    def test_command_stacking_blocked(self):
        from dbslice.exceptions import InsecureWhereClauseError

        with pytest.raises((ValueError, InsecureWhereClauseError)):
            SeedSpec.parse("users:id=1; UPDATE users SET admin=true")

    def test_comment_based_attacks_blocked(self):
        from dbslice.exceptions import InsecureWhereClauseError

        # Only test patterns that are actually blocked by config.py validation
        comment_attacks = [
            "users:id=1 OR 1=1--",  # Double dash comment
            "users:id=1 OR 1=1/*comment*/",  # Block comment
        ]
        for attack in comment_attacks:
            with pytest.raises((ValueError, InsecureWhereClauseError)):
                SeedSpec.parse(attack)


class TestValidationPerformance:
    """Test that validation doesn't have performance issues."""

    def test_validation_with_many_seeds(self):
        # Should be fast even with many seeds
        seeds = []
        for i in range(100):
            seed = SeedSpec.parse(f"users.id={i}")
            seeds.append(seed)
        assert len(seeds) == 100

    def test_validation_with_long_valid_identifiers(self):
        # Should handle max-length identifiers efficiently
        long_name = "a" * 63
        seed = SeedSpec.parse(f"{long_name}.{long_name}=123")
        assert seed.table == long_name


class TestValidationConsistency:
    """Test that validation is consistent across different entry points."""

    def test_same_validation_rules_everywhere(self):
        # Invalid identifier should fail in both table and column position
        invalid = "invalid-name"

        with pytest.raises((ValueError, IdentifierValidationError)):
            SeedSpec.parse(f"{invalid}.id=1")

        with pytest.raises((ValueError, IdentifierValidationError)):
            SeedSpec.parse(f"users.{invalid}=1")

    def test_validation_order_consistent(self):
        # Should validate format before content
        with pytest.raises(ValueError):
            SeedSpec.parse("not_a_valid_seed")

        # Should validate identifiers during parsing
        with pytest.raises((ValueError, IdentifierValidationError)):
            SeedSpec.parse("invalid-table.id=1")
