"""Comprehensive tests for input validation."""

import pytest

from dbslice.input_validators import (
    DatabaseURLValidationError,
    DepthValidationError,
    FilePathValidationError,
    IdentifierValidationError,
    SeedValidationError,
    ValidationError,
    validate_column_name,
    validate_database_url,
    validate_depth,
    validate_exclude_tables,
    validate_identifier,
    validate_output_file_path,
    validate_redact_fields,
    validate_seed_value,
    validate_table_name,
    validate_where_clause,
)


class TestIdentifierValidation:
    """Tests for SQL identifier validation."""

    def test_valid_identifiers(self):
        valid_identifiers = [
            "users",
            "order_items",
            "User_Profile",
            "_private",
            "table123",
            "a",
            "A",
            "_",
            "user_id_2",
            "table$name",  # Dollar sign allowed in some DBs
        ]
        for identifier in valid_identifiers:
            validate_identifier(identifier, "test")

    def test_empty_identifier(self):
        with pytest.raises(IdentifierValidationError, match="cannot be empty"):
            validate_identifier("", "test")

    def test_too_long_identifier(self):
        long_name = "a" * 64  # Max is 63
        with pytest.raises(IdentifierValidationError, match="too long"):
            validate_identifier(long_name, "test")

    def test_invalid_characters(self):
        invalid_identifiers = [
            "user-name",  # Hyphen not allowed
            "user.name",  # Dot not allowed (except in qualified names)
            "user name",  # Space not allowed
            "user@domain",  # @ not allowed
            "user#1",  # Hash not allowed
            "user%",  # Percent not allowed
            "123user",  # Cannot start with digit
            "user;",  # Semicolon not allowed
            "'; DROP TABLE users--",  # SQL injection attempt
        ]
        for identifier in invalid_identifiers:
            with pytest.raises(IdentifierValidationError):
                validate_identifier(identifier, "test")

    def test_sql_keywords_rejected(self):
        keywords = [
            "select",
            "SELECT",
            "drop",
            "DROP",
            "delete",
            "insert",
            "update",
            "alter",
            "create",
            "truncate",
        ]
        for keyword in keywords:
            with pytest.raises(IdentifierValidationError, match="SQL keyword"):
                validate_identifier(keyword, "test")

    def test_table_name_validation(self):
        validate_table_name("users")
        validate_table_name("order_items")

        with pytest.raises(IdentifierValidationError, match="table name"):
            validate_table_name("'; DROP TABLE")

    def test_column_name_validation(self):
        validate_column_name("user_id")
        validate_column_name("email")

        with pytest.raises(IdentifierValidationError, match="column name"):
            validate_column_name("invalid-name")


class TestWhereClauseValidation:
    """Tests for WHERE clause validation."""

    def test_valid_where_clauses(self):
        valid_clauses = [
            "status = 'active'",
            "id = 123",
            "email = 'test@example.com'",
            "created_at > '2024-01-01'",
            "id IN (1, 2, 3)",
            "name LIKE '%test%'",
            "age BETWEEN 18 AND 65",
            "active = true AND verified = true",
            "category = 'DELETE'",  # DELETE as a value is OK
        ]
        for clause in valid_clauses:
            validate_where_clause(clause)

    def test_empty_where_clause(self):
        with pytest.raises(SeedValidationError, match="cannot be empty"):
            validate_where_clause("")

        with pytest.raises(SeedValidationError, match="cannot be empty"):
            validate_where_clause("   ")

    def test_too_long_where_clause(self):
        long_clause = "id = " + ("1" * 10000)
        with pytest.raises(SeedValidationError, match="too long"):
            validate_where_clause(long_clause)

    def test_dangerous_sql_patterns(self):
        dangerous_clauses = [
            "1=1; DROP TABLE users",
            "id=1 OR 1=1; DELETE FROM users",
            "id=1; TRUNCATE TABLE users",
            "id=1; ALTER TABLE users",
            "1=1 UNION SELECT * FROM passwords",
            "1=1; EXEC sp_executesql",
            "1=1; EXECUTE('DROP TABLE')",
        ]
        for clause in dangerous_clauses:
            with pytest.raises(SeedValidationError, match="dangerous SQL patterns"):
                validate_where_clause(clause)


class TestSeedValueValidation:
    """Tests for seed value validation."""

    def test_valid_seed_values(self):
        valid_values = [
            123,
            "test@example.com",
            "John Doe",
            "active",
            42,
            "a",
        ]
        for value in valid_values:
            validate_seed_value(value)

    def test_none_value_rejected(self):
        with pytest.raises(SeedValidationError, match="cannot be None"):
            validate_seed_value(None)

    def test_empty_string_rejected(self):
        with pytest.raises(SeedValidationError, match="cannot be empty or whitespace"):
            validate_seed_value("")

        with pytest.raises(SeedValidationError, match="cannot be empty or whitespace"):
            validate_seed_value("   ")

    def test_too_long_string_rejected(self):
        long_value = "a" * 1001
        with pytest.raises(SeedValidationError, match="too long"):
            validate_seed_value(long_value)


class TestDepthValidation:
    """Tests for traversal depth validation."""

    def test_valid_depths(self):
        valid_depths = [1, 2, 3, 5, 10]
        for depth in valid_depths:
            validate_depth(depth)

    def test_zero_depth_rejected(self):
        with pytest.raises(DepthValidationError, match="at least 1"):
            validate_depth(0)

    def test_negative_depth_rejected(self):
        with pytest.raises(DepthValidationError, match="at least 1"):
            validate_depth(-1)

    def test_excessive_depth_rejected(self):
        with pytest.raises(DepthValidationError, match="cannot exceed 10"):
            validate_depth(11)

        with pytest.raises(DepthValidationError, match="cannot exceed 10"):
            validate_depth(100)

        with pytest.raises(DepthValidationError, match="cannot exceed 10"):
            validate_depth(1000)

    def test_non_integer_depth_rejected(self):
        with pytest.raises(DepthValidationError, match="must be an integer"):
            validate_depth("3")

        with pytest.raises(DepthValidationError, match="must be an integer"):
            validate_depth(3.5)


class TestDatabaseURLValidation:
    """Tests for database URL validation."""

    def test_valid_postgres_urls(self):
        valid_urls = [
            "postgres://user:pass@localhost:5432/mydb",
            "postgresql://user:pass@localhost/mydb",
            "postgres://localhost/mydb",
            "postgres://user@localhost:5432/mydb",
            "postgres://user:pass@localhost:5432/mydb?sslmode=require",
        ]
        for url in valid_urls:
            validate_database_url(url)

    def test_valid_mysql_urls(self):
        valid_urls = [
            "mysql://user:pass@localhost:3306/mydb",
            "mysql://localhost/mydb",
            "mysql://user:pass@localhost/mydb",
        ]
        for url in valid_urls:
            validate_database_url(url)

    def test_valid_sqlite_urls(self):
        valid_urls = [
            "sqlite:///./test.db",
            "sqlite:////var/data/test.db",
            "sqlite:///:memory:",
        ]
        for url in valid_urls:
            validate_database_url(url)

    def test_empty_url_rejected(self):
        with pytest.raises(DatabaseURLValidationError, match="cannot be empty"):
            validate_database_url("")

        with pytest.raises(DatabaseURLValidationError, match="cannot be empty"):
            validate_database_url("   ")

    def test_missing_scheme_rejected(self):
        with pytest.raises(DatabaseURLValidationError):
            validate_database_url("localhost/mydb")

    def test_unsupported_database_type_rejected(self):
        with pytest.raises(DatabaseURLValidationError, match="Unsupported"):
            validate_database_url("oracle://localhost/mydb")

        with pytest.raises(DatabaseURLValidationError, match="Unsupported"):
            validate_database_url("mongodb://localhost/mydb")

    def test_missing_database_name_rejected(self):
        # These URLs are malformed and caught by the pattern check
        with pytest.raises(DatabaseURLValidationError):
            validate_database_url("postgres://localhost/")

        with pytest.raises(DatabaseURLValidationError):
            validate_database_url("postgres://localhost")

    def test_malformed_url_rejected(self):
        malformed_urls = [
            "not_a_url",
            "postgres:/incomplete",
            "postgres://",
        ]
        for url in malformed_urls:
            with pytest.raises(DatabaseURLValidationError):
                validate_database_url(url)


class TestFilePathValidation:
    """Tests for output file path validation."""

    def test_valid_file_paths(self, tmp_path):
        # Test with existing directory
        valid_path = tmp_path / "output.sql"
        validate_output_file_path(valid_path)

        # Test with current directory
        validate_output_file_path("./output.sql")
        validate_output_file_path("output.sql")

    def test_empty_path_rejected(self):
        with pytest.raises(FilePathValidationError, match="cannot be empty"):
            validate_output_file_path("")

    def test_nonexistent_parent_directory_rejected(self):
        with pytest.raises(FilePathValidationError, match="does not exist"):
            validate_output_file_path("/nonexistent/directory/output.sql")

    def test_parent_not_directory_rejected(self, tmp_path):
        file_path = tmp_path / "file.txt"
        file_path.write_text("test")

        invalid_path = file_path / "output.sql"
        with pytest.raises(FilePathValidationError, match="not a directory"):
            validate_output_file_path(invalid_path)

    def test_system_directory_rejected(self):
        # Note: This test only works if the system directories exist
        # We'll test the logic without actually needing write permissions
        import os

        if os.path.exists("/bin"):
            with pytest.raises(FilePathValidationError, match="system directory"):
                validate_output_file_path("/bin/output.sql")


class TestExcludeTablesValidation:
    """Tests for exclude tables validation."""

    def test_valid_exclude_tables(self):
        validate_exclude_tables(["audit_log", "temp_data"])
        validate_exclude_tables(["users", "orders", "products"])
        validate_exclude_tables([])

    def test_invalid_table_names_rejected(self):
        with pytest.raises(IdentifierValidationError):
            validate_exclude_tables(["valid_table", "'; DROP TABLE"])

        with pytest.raises(IdentifierValidationError):
            validate_exclude_tables(["invalid-name"])


class TestRedactFieldsValidation:
    """Tests for redact fields validation."""

    def test_valid_redact_fields(self):
        validate_redact_fields(["users.email", "orders.notes"])
        validate_redact_fields(["table1.col1", "table2.col2", "table3.col3"])
        validate_redact_fields([])

    def test_missing_dot_rejected(self):
        with pytest.raises(ValidationError, match="must be 'table.column'"):
            validate_redact_fields(["invalid"])

    def test_multiple_dots_rejected(self):
        with pytest.raises(ValidationError, match="must be 'table.column'"):
            validate_redact_fields(["schema.table.column"])

    def test_invalid_identifiers_rejected(self):
        with pytest.raises(IdentifierValidationError):
            validate_redact_fields(["valid_table.invalid-column"])

        with pytest.raises(IdentifierValidationError):
            validate_redact_fields(["invalid-table.valid_column"])


class TestValidationErrorMessages:
    """Tests for validation error message quality."""

    def test_identifier_error_messages(self):
        try:
            validate_identifier("'; DROP TABLE", "table")
            assert False, "Should have raised exception"
        except IdentifierValidationError as e:
            assert "table" in str(e)
            assert "alphanumeric" in str(e) or "underscore" in str(e)

    def test_depth_error_messages(self):
        try:
            validate_depth(100)
            assert False, "Should have raised exception"
        except DepthValidationError as e:
            assert "10" in str(e)
            assert "DoS" in str(e) or "prevent" in str(e)

    def test_url_error_messages(self):
        try:
            validate_database_url("invalid")
            assert False, "Should have raised exception"
        except DatabaseURLValidationError as e:
            error_msg = str(e)
            # Should mention it's an unsupported type or format issue
            assert "Unsupported" in error_msg or "format" in error_msg or "scheme" in error_msg

    def test_file_path_error_messages(self):
        try:
            validate_output_file_path("/nonexistent/dir/file.sql")
            assert False, "Should have raised exception"
        except FilePathValidationError as e:
            assert "does not exist" in str(e) or "directory" in str(e)
            assert "create" in str(e) or "existing" in str(e)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_identifier_at_max_length(self):
        max_length_name = "a" * 63
        validate_identifier(max_length_name, "test")

    def test_depth_at_boundaries(self):
        validate_depth(1)  # Min
        validate_depth(10)  # Max

    def test_unicode_in_identifiers(self):
        # Unicode should be rejected (not alphanumeric ASCII)
        with pytest.raises(IdentifierValidationError):
            validate_identifier("café", "test")

    def test_where_clause_with_quotes(self):
        # Single quotes
        validate_where_clause("status = 'active'")
        # Double quotes
        validate_where_clause('name = "test"')
        # Mixed quotes
        validate_where_clause("name = 'O''Brien'")

    def test_case_sensitivity(self):
        # SQL keywords should be caught regardless of case
        for case_variant in ["DROP", "drop", "Drop", "dRoP"]:
            with pytest.raises(IdentifierValidationError):
                validate_identifier(case_variant, "test")
