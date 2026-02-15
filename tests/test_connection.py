"""Tests for database URL parsing."""

import pytest

from dbslice.config import DatabaseType
from dbslice.exceptions import InvalidURLError, UnsupportedDatabaseError
from dbslice.utils.connection import DatabaseConfig, parse_database_url


class TestParsePostgresURL:
    """Tests for PostgreSQL URL parsing."""

    def test_full_url(self):
        url = "postgres://user:pass@localhost:5432/mydb"
        config = parse_database_url(url)

        assert config.db_type == DatabaseType.POSTGRESQL
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.user == "user"
        assert config.password == "pass"
        assert config.database == "mydb"

    def test_postgresql_scheme(self):
        url = "postgresql://user:pass@localhost/mydb"
        config = parse_database_url(url)
        assert config.db_type == DatabaseType.POSTGRESQL

    def test_default_port(self):
        url = "postgres://user:pass@localhost/mydb"
        config = parse_database_url(url)
        assert config.port == 5432

    def test_no_password(self):
        url = "postgres://user@localhost/mydb"
        config = parse_database_url(url)
        assert config.user == "user"
        assert config.password is None

    def test_with_query_params(self):
        url = "postgres://user:pass@localhost/mydb?sslmode=require"
        config = parse_database_url(url)
        assert config.options.get("sslmode") == "require"

    def test_url_encoded_password(self):
        url = "postgres://user:p%40ssw0rd@localhost/mydb"
        config = parse_database_url(url)
        assert config.password == "p@ssw0rd"


class TestParseMySQLURL:
    """Tests for MySQL URL parsing."""

    def test_full_url(self):
        url = "mysql://user:pass@localhost:3306/mydb"
        config = parse_database_url(url)

        assert config.db_type == DatabaseType.MYSQL
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.database == "mydb"

    def test_default_port(self):
        url = "mysql://user:pass@localhost/mydb"
        config = parse_database_url(url)
        assert config.port == 3306


class TestParseSQLiteURL:
    """Tests for SQLite URL parsing."""

    def test_absolute_path(self):
        url = "sqlite:////var/data/test.db"
        config = parse_database_url(url)

        assert config.db_type == DatabaseType.SQLITE
        assert config.database == "/var/data/test.db"
        assert config.host is None
        assert config.port is None

    def test_relative_path(self):
        url = "sqlite:///./test.db"
        config = parse_database_url(url)

        assert config.db_type == DatabaseType.SQLITE
        assert config.database == "./test.db"

    def test_memory_database(self):
        url = "sqlite:///:memory:"
        config = parse_database_url(url)
        assert config.database == ":memory:"


class TestParseURLErrors:
    """Tests for URL parsing error cases."""

    def test_empty_url(self):
        with pytest.raises(InvalidURLError, match="cannot be empty"):
            parse_database_url("")

    def test_missing_scheme(self):
        with pytest.raises(InvalidURLError, match="Missing URL scheme"):
            parse_database_url("localhost/mydb")

    def test_unsupported_database(self):
        with pytest.raises(UnsupportedDatabaseError, match="oracle"):
            parse_database_url("oracle://localhost/mydb")

    def test_missing_database_name(self):
        with pytest.raises(InvalidURLError, match="Database name is required"):
            parse_database_url("postgres://localhost/")


class TestDatabaseConfig:
    """Tests for DatabaseConfig."""

    def test_to_dsn_postgres(self):
        config = DatabaseConfig(
            db_type=DatabaseType.POSTGRESQL,
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            database="testdb",
            options={},
            original_url="postgres://...",
        )

        dsn = config.to_dsn()
        assert "host=localhost" in dsn
        assert "port=5432" in dsn
        assert "user=testuser" in dsn
        assert "password=testpass" in dsn
        assert "dbname=testdb" in dsn

    def test_to_dsn_sqlite(self):
        config = DatabaseConfig(
            db_type=DatabaseType.SQLITE,
            host=None,
            port=None,
            user=None,
            password=None,
            database="/path/to/db.sqlite",
            options={},
            original_url="sqlite:///...",
        )

        dsn = config.to_dsn()
        assert dsn == "/path/to/db.sqlite"
