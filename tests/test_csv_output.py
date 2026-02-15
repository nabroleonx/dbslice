"""Tests for CSV output generation."""

import csv
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from dbslice.models import Column, Table
from dbslice.output.csv_out import CSVGenerator, generate_csv


class TestCSVGenerator:
    """Tests for CSVGenerator."""

    @pytest.fixture
    def generator(self):
        """Create a CSV generator in single mode."""
        return CSVGenerator(mode="single")

    @pytest.fixture
    def per_table_generator(self):
        """Create a CSV generator in per-table mode."""
        return CSVGenerator(mode="per-table")

    @pytest.fixture
    def sample_tables_schema(self):
        """Sample table schemas for testing."""
        return {
            "users": Table(
                name="users",
                schema="public",
                columns=[
                    Column("id", "INTEGER", False, True),
                    Column("email", "TEXT", False, False),
                    Column("name", "TEXT", True, False),
                ],
                primary_key=("id",),
                foreign_keys=[],
            ),
            "orders": Table(
                name="orders",
                schema="public",
                columns=[
                    Column("id", "INTEGER", False, True),
                    Column("user_id", "INTEGER", False, False),
                    Column("total", "DECIMAL", True, False),
                ],
                primary_key=("id",),
                foreign_keys=[],
            ),
        }

    def test_init_valid_modes(self):
        single = CSVGenerator(mode="single")
        assert single.mode == "single"

        per_table = CSVGenerator(mode="per-table")
        assert per_table.mode == "per-table"

    def test_init_invalid_mode(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            CSVGenerator(mode="invalid")

    def test_init_custom_delimiter(self):
        generator = CSVGenerator(delimiter=";")
        assert generator.delimiter == ";"

    def test_generate_single_mode_basic(self, generator, sample_tables_schema):
        tables_data = {
            "users": [{"id": 1, "email": "test@example.com", "name": "Test"}],
            "orders": [{"id": 1, "user_id": 1, "total": 99.99}],
        }
        insert_order = ["users", "orders"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        assert isinstance(csv_output, str)
        assert "table_name" in csv_output
        assert "users" in csv_output
        assert "orders" in csv_output
        assert "test@example.com" in csv_output

    def test_generate_single_mode_has_headers(self, generator, sample_tables_schema):
        tables_data = {
            "users": [{"id": 1, "email": "a@b.com", "name": None}],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        lines = csv_output.strip().split("\n")
        assert len(lines) >= 2  # Header + at least one data row
        header = lines[0]
        assert "table_name" in header
        assert "id" in header
        assert "email" in header
        assert "name" in header

    def test_generate_per_table_mode_basic(self, per_table_generator, sample_tables_schema):
        tables_data = {
            "users": [{"id": 1, "email": "test@example.com", "name": "Test"}],
            "orders": [{"id": 1, "user_id": 1, "total": 99.99}],
        }
        insert_order = ["users", "orders"]

        csv_output = per_table_generator.generate(tables_data, insert_order, sample_tables_schema)

        assert isinstance(csv_output, dict)
        assert "users" in csv_output
        assert "orders" in csv_output
        assert "test@example.com" in csv_output["users"]
        assert "99.99" in csv_output["orders"]

    def test_generate_per_table_separate_headers(self, per_table_generator, sample_tables_schema):
        tables_data = {
            "users": [{"id": 1, "email": "a@b.com", "name": "Test"}],
            "orders": [{"id": 1, "user_id": 1, "total": 50.0}],
        }
        insert_order = ["users", "orders"]

        csv_output = per_table_generator.generate(tables_data, insert_order, sample_tables_schema)

        # Check users CSV
        users_lines = csv_output["users"].strip().split("\n")
        assert "id,email,name" in users_lines[0]
        assert "table_name" not in users_lines[0]  # Should not have table_name column

        # Check orders CSV
        orders_lines = csv_output["orders"].strip().split("\n")
        assert "id,user_id,total" in orders_lines[0]

    def test_format_value_none(self, generator):
        assert generator._format_value(None) == ""

    def test_format_value_bool(self, generator):
        assert generator._format_value(True) == "true"
        assert generator._format_value(False) == "false"

    def test_format_value_numbers(self, generator):
        assert generator._format_value(42) == "42"
        assert generator._format_value(3.14) == "3.14"
        assert generator._format_value(Decimal("99.99")) == "99.99"

    def test_format_value_string(self, generator):
        assert generator._format_value("hello") == "hello"
        assert generator._format_value("hello, world") == "hello, world"

    def test_format_value_datetime(self, generator):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = generator._format_value(dt)
        assert "2024-01-15" in result
        assert "10:30:00" in result

    def test_format_value_date(self, generator):
        d = date(2024, 1, 15)
        result = generator._format_value(d)
        assert result == "2024-01-15"

    def test_format_value_time(self, generator):
        t = time(14, 30, 45)
        result = generator._format_value(t)
        assert "14:30:45" in result

    def test_format_value_timedelta(self, generator):
        td = timedelta(hours=1, minutes=30)
        result = generator._format_value(td)
        assert result == "5400.0"  # 1.5 hours in seconds

    def test_format_value_uuid(self, generator):
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = generator._format_value(u)
        assert result == "12345678-1234-5678-1234-567812345678"

    def test_format_value_bytes(self, generator):
        data = b"\x00\x01\x02\xff"
        result = generator._format_value(data)
        assert result == "000102ff"

    def test_format_value_dict(self, generator):
        data = {"key": "value", "num": 42}
        result = generator._format_value(data)
        parsed = json.loads(result)
        assert parsed["key"] == "value"
        assert parsed["num"] == 42

    def test_format_value_list(self, generator):
        data = [1, 2, 3, "test"]
        result = generator._format_value(data)
        parsed = json.loads(result)
        assert parsed == [1, 2, 3, "test"]

    def test_format_value_nested_json(self, generator):
        data = {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
        result = generator._format_value(data)
        parsed = json.loads(result)
        assert len(parsed["users"]) == 2
        assert parsed["users"][0]["name"] == "Alice"

    def test_single_mode_preserves_insert_order(self, generator, sample_tables_schema):
        tables_data = {
            "orders": [{"id": 1, "user_id": 1, "total": 100}],
            "users": [{"id": 1, "email": "a@b.com", "name": "A"}],
        }
        insert_order = ["users", "orders"]  # Users first

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        lines = csv_output.strip().split("\n")
        # Find data rows (skip header)
        data_lines = [line for line in lines[1:] if line.strip()]

        # First data row should be from users table
        assert data_lines[0].startswith("users,")
        # Second data row should be from orders table
        assert data_lines[1].startswith("orders,")

    def test_csv_quoting_with_special_chars(self, generator, sample_tables_schema):
        tables_data = {
            "users": [
                {"id": 1, "email": "test@example.com", "name": "O'Reilly"},
                {"id": 2, "email": "test2@example.com", "name": "Smith, John"},
            ],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        # Parse CSV to verify it's valid
        reader = csv.DictReader(csv_output.strip().split("\n"))
        rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "O'Reilly"
        assert rows[1]["name"] == "Smith, John"

    def test_custom_delimiter(self, sample_tables_schema):
        generator = CSVGenerator(mode="single", delimiter="|")
        tables_data = {
            "users": [{"id": 1, "email": "a@b.com", "name": "Test"}],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        assert "|" in csv_output
        # Header should use custom delimiter
        lines = csv_output.strip().split("\n")
        assert lines[0].count("|") >= 3  # At least 3 delimiters in header

    def test_empty_tables(self, generator, sample_tables_schema):
        tables_data = {
            "users": [],
            "orders": [],
        }
        insert_order = ["users", "orders"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        # Should just have headers or be empty
        lines = csv_output.strip().split("\n")
        # If there are any lines, first should be header
        if lines and lines[0]:
            assert "table_name" in lines[0]

    def test_per_table_empty_table(self, per_table_generator, sample_tables_schema):
        tables_data = {
            "users": [],
        }
        insert_order = ["users"]

        csv_output = per_table_generator.generate(tables_data, insert_order, sample_tables_schema)

        assert "users" in csv_output
        assert csv_output["users"] == ""

    def test_multiple_rows_same_table(self, generator, sample_tables_schema):
        tables_data = {
            "users": [
                {"id": 1, "email": "alice@test.com", "name": "Alice"},
                {"id": 2, "email": "bob@test.com", "name": "Bob"},
                {"id": 3, "email": "charlie@test.com", "name": "Charlie"},
            ],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        lines = csv_output.strip().split("\n")
        # 1 header + 3 data rows
        assert len(lines) == 4
        assert "alice@test.com" in csv_output
        assert "bob@test.com" in csv_output
        assert "charlie@test.com" in csv_output

    def test_null_value_in_csv(self, generator, sample_tables_schema):
        tables_data = {
            "users": [{"id": 1, "email": "test@example.com", "name": None}],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        # Parse CSV and check NULL handling
        reader = csv.DictReader(csv_output.strip().split("\n"))
        rows = list(reader)

        assert rows[0]["name"] == ""  # NULL becomes empty string in CSV

    def test_write_to_file_single_mode(self, generator, sample_tables_schema, tmp_path):
        tables_data = {
            "users": [{"id": 1, "email": "test@test.com", "name": "Test"}],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        output_file = tmp_path / "output.csv"
        generator.write_to_file(csv_output, output_file)

        assert output_file.exists()
        content = output_file.read_text()
        assert "test@test.com" in content

    def test_write_to_file_per_table_mode(
        self, per_table_generator, sample_tables_schema, tmp_path
    ):
        tables_data = {
            "users": [{"id": 1, "email": "test@test.com", "name": "Test"}],
            "orders": [{"id": 1, "user_id": 1, "total": 50.0}],
        }
        insert_order = ["users", "orders"]

        csv_output = per_table_generator.generate(tables_data, insert_order, sample_tables_schema)

        output_dir = tmp_path / "output"
        per_table_generator.write_to_file(csv_output, output_dir)

        assert output_dir.exists()
        assert (output_dir / "users.csv").exists()
        assert (output_dir / "orders.csv").exists()

        users_content = (output_dir / "users.csv").read_text()
        assert "test@test.com" in users_content

    def test_single_mode_mixed_columns(self, generator):
        tables_data = {
            "users": [{"id": 1, "email": "a@b.com"}],
            "products": [{"id": 1, "sku": "ABC123", "price": 19.99}],
        }
        insert_order = ["users", "products"]
        schema = {}

        csv_output = generator.generate(tables_data, insert_order, schema)

        # Should have all columns from both tables
        lines = csv_output.strip().split("\n")
        header = lines[0]
        assert "email" in header
        assert "sku" in header
        assert "price" in header

    def test_rfc4180_compliance(self, generator, sample_tables_schema):
        """Test RFC 4180 compliance with various edge cases."""
        tables_data = {
            "users": [
                # Comma in field
                {"id": 1, "email": "test@example.com", "name": "Doe, John"},
                # Quotes in field
                {"id": 2, "email": "test2@example.com", "name": 'Say "Hello"'},
                # Newline in field
                {"id": 3, "email": "test3@example.com", "name": "Line1\nLine2"},
            ],
        }
        insert_order = ["users"]

        csv_output = generator.generate(tables_data, insert_order, sample_tables_schema)

        # Parse it back using StringIO to properly handle multiline fields
        import io

        reader = csv.DictReader(io.StringIO(csv_output))
        rows = list(reader)

        assert len(rows) == 3
        assert rows[0]["name"] == "Doe, John"
        assert rows[1]["name"] == 'Say "Hello"'
        assert rows[2]["name"] == "Line1\nLine2"

    def test_generate_with_broken_fks(self, generator, sample_tables_schema):
        """Test CSV generation with broken FKs (should not affect output)."""
        tables_data = {
            "users": [{"id": 1, "email": "test@example.com", "name": "Test"}],
        }
        insert_order = ["users"]

        # Mock broken FKs and deferred updates
        broken_fks = [object()]
        deferred_updates = [object()]

        csv_output = generator.generate(
            tables_data,
            insert_order,
            sample_tables_schema,
            broken_fks=broken_fks,
            deferred_updates=deferred_updates,
        )

        # Should still generate valid CSV
        assert isinstance(csv_output, str)
        assert "test@example.com" in csv_output


class TestGenerateCsvFunction:
    """Tests for the generate_csv convenience function."""

    def test_basic_usage(self):
        """Test basic usage of generate_csv function."""
        tables_data = {"users": [{"id": 1, "email": "test@test.com"}]}
        tables_schema = {
            "users": Table(
                name="users",
                schema="public",
                columns=[
                    Column("id", "INTEGER", False, True),
                    Column("email", "TEXT", False, False),
                ],
                primary_key=("id",),
                foreign_keys=[],
            )
        }

        csv = generate_csv(
            tables_data,
            ["users"],
            tables_schema,
            mode="single",
        )

        assert "test@test.com" in csv

    def test_per_table_mode(self):
        """Test generate_csv in per-table mode."""
        tables_data = {
            "users": [{"id": 1, "email": "test@test.com"}],
            "orders": [{"id": 1, "total": 100}],
        }
        tables_schema = {}

        csv = generate_csv(
            tables_data,
            ["users", "orders"],
            tables_schema,
            mode="per-table",
        )

        assert isinstance(csv, dict)
        assert "users" in csv
        assert "orders" in csv

    def test_custom_delimiter(self):
        """Test generate_csv with custom delimiter."""
        tables_data = {"users": [{"id": 1, "email": "test@test.com"}]}
        tables_schema = {}

        csv = generate_csv(
            tables_data,
            ["users"],
            tables_schema,
            delimiter=";",
        )

        assert ";" in csv
