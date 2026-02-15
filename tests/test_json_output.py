"""Tests for JSON output generation."""

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest

from dbslice.models import Column, Table
from dbslice.output.json_out import DatabaseTypeEncoder, JSONGenerator, generate_json


class TestDatabaseTypeEncoder:
    """Tests for custom JSON encoder."""

    def test_encode_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 45)
        result = json.dumps(dt, cls=DatabaseTypeEncoder)
        assert "2024-01-15" in result
        assert "10:30:45" in result

    def test_encode_date(self):
        d = date(2024, 1, 15)
        result = json.dumps(d, cls=DatabaseTypeEncoder)
        assert result == '"2024-01-15"'

    def test_encode_time(self):
        t = time(10, 30, 45)
        result = json.dumps(t, cls=DatabaseTypeEncoder)
        assert "10:30:45" in result

    def test_encode_timedelta(self):
        td = timedelta(hours=2, minutes=30, seconds=45)
        result = json.dumps(td, cls=DatabaseTypeEncoder)
        # Should convert to total seconds
        expected_seconds = 2 * 3600 + 30 * 60 + 45
        assert str(expected_seconds) in result

    def test_encode_decimal(self):
        d = Decimal("99.99")
        result = json.dumps(d, cls=DatabaseTypeEncoder)
        assert "99.99" in result

    def test_encode_uuid(self):
        u = UUID("12345678-1234-5678-1234-567812345678")
        result = json.dumps(u, cls=DatabaseTypeEncoder)
        assert "12345678-1234-5678-1234-567812345678" in result

    def test_encode_bytes(self):
        b = b"\x00\x01\x02\xff"
        result = json.dumps(b, cls=DatabaseTypeEncoder)
        assert "000102ff" in result

    def test_encode_dict(self):
        data = {"key": "value", "number": 42}
        result = json.dumps(data, cls=DatabaseTypeEncoder)
        parsed = json.loads(result)
        assert parsed == data

    def test_encode_list(self):
        data = [1, 2, 3, "test"]
        result = json.dumps(data, cls=DatabaseTypeEncoder)
        parsed = json.loads(result)
        assert parsed == data

    def test_encode_mixed_types(self):
        data = {
            "datetime": datetime(2024, 1, 15, 10, 30),
            "date": date(2024, 1, 15),
            "decimal": Decimal("99.99"),
            "uuid": UUID("12345678-1234-5678-1234-567812345678"),
            "bytes": b"\x00\x01",
            "string": "test",
            "number": 42,
            "bool": True,
            "null": None,
        }
        result = json.dumps(data, cls=DatabaseTypeEncoder)
        parsed = json.loads(result)

        # Verify all types were encoded correctly
        assert "2024-01-15" in parsed["datetime"]
        assert parsed["date"] == "2024-01-15"
        assert parsed["decimal"] == 99.99
        assert "12345678-1234-5678" in parsed["uuid"]
        assert parsed["bytes"] == "0001"
        assert parsed["string"] == "test"
        assert parsed["number"] == 42
        assert parsed["bool"] is True
        assert parsed["null"] is None


class TestJSONGenerator:
    """Tests for JSONGenerator class."""

    @pytest.fixture
    def sample_tables_schema(self):
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

    @pytest.fixture
    def sample_tables_data(self):
        return {
            "users": [
                {"id": 1, "email": "test@example.com", "name": "Test User"},
                {"id": 2, "email": "admin@example.com", "name": "Admin"},
            ],
            "orders": [
                {"id": 1, "user_id": 1, "total": Decimal("99.99")},
                {"id": 2, "user_id": 2, "total": Decimal("149.99")},
            ],
        }

    def test_init_single_mode(self):
        generator = JSONGenerator(mode="single")
        assert generator.mode == "single"
        assert generator.pretty is True
        assert generator.indent == 2

    def test_init_per_table_mode(self):
        generator = JSONGenerator(mode="per-table")
        assert generator.mode == "per-table"

    def test_init_invalid_mode(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            JSONGenerator(mode="invalid")

    def test_init_compact_mode(self):
        generator = JSONGenerator(pretty=False)
        assert generator.indent is None

    def test_generate_single_mode_basic(self, sample_tables_data, sample_tables_schema):
        generator = JSONGenerator(mode="single")
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        assert isinstance(result, str)
        parsed = json.loads(result)

        # Check metadata
        assert "metadata" in parsed
        assert parsed["metadata"]["generated_by"] == "dbslice"
        assert parsed["metadata"]["table_count"] == 2
        assert parsed["metadata"]["total_rows"] == 4
        assert parsed["metadata"]["insert_order"] == ["users", "orders"]
        assert parsed["metadata"]["has_cycles"] is False

        # Check tables
        assert "tables" in parsed
        assert "users" in parsed["tables"]
        assert "orders" in parsed["tables"]
        assert len(parsed["tables"]["users"]) == 2
        assert len(parsed["tables"]["orders"]) == 2

    def test_generate_single_mode_with_cycles(self, sample_tables_data, sample_tables_schema):
        generator = JSONGenerator(mode="single")
        insert_order = ["users", "orders"]

        # Mock broken FKs and deferred updates
        class MockFK:
            pass

        class MockUpdate:
            pass

        broken_fks = [MockFK()]
        deferred_updates = [MockUpdate(), MockUpdate()]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
            broken_fks=broken_fks,
            deferred_updates=deferred_updates,
        )

        parsed = json.loads(result)

        # Check cycle metadata
        assert parsed["metadata"]["has_cycles"] is True
        assert parsed["metadata"]["broken_fks_count"] == 1
        assert parsed["metadata"]["deferred_updates_count"] == 2

    def test_generate_single_mode_compact(self, sample_tables_data, sample_tables_schema):
        generator = JSONGenerator(mode="single", pretty=False)
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        # Compact mode should not have newlines (except possibly one at the end)
        assert result.count("\n") <= 1

    def test_generate_per_table_mode(self, sample_tables_data, sample_tables_schema):
        generator = JSONGenerator(mode="per-table")
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        assert isinstance(result, dict)
        assert "users" in result
        assert "orders" in result

        # Check users table
        users_parsed = json.loads(result["users"])
        assert users_parsed["table"] == "users"
        assert users_parsed["row_count"] == 2
        assert len(users_parsed["rows"]) == 2

        # Check orders table
        orders_parsed = json.loads(result["orders"])
        assert orders_parsed["table"] == "orders"
        assert orders_parsed["row_count"] == 2
        assert len(orders_parsed["rows"]) == 2

    def test_generate_with_special_types(self, sample_tables_schema):
        generator = JSONGenerator(mode="single")

        tables_data = {
            "users": [
                {
                    "id": 1,
                    "created_at": datetime(2024, 1, 15, 10, 30),
                    "birthday": date(1990, 5, 20),
                    "balance": Decimal("1234.56"),
                    "uuid": UUID("12345678-1234-5678-1234-567812345678"),
                    "avatar": b"\x89PNG",
                }
            ]
        }
        insert_order = ["users"]

        result = generator.generate(tables_data, insert_order, sample_tables_schema)
        parsed = json.loads(result)

        user = parsed["tables"]["users"][0]
        assert "2024-01-15" in user["created_at"]
        assert user["birthday"] == "1990-05-20"
        assert user["balance"] == 1234.56
        assert "12345678-1234-5678" in user["uuid"]
        assert user["avatar"] == "89504e47"  # hex of b"\x89PNG"

    def test_generate_empty_tables(self, sample_tables_schema):
        generator = JSONGenerator(mode="single")
        tables_data = {}
        insert_order = []

        result = generator.generate(tables_data, insert_order, sample_tables_schema)
        parsed = json.loads(result)

        assert parsed["metadata"]["table_count"] == 0
        assert parsed["metadata"]["total_rows"] == 0
        assert parsed["tables"] == {}

    def test_write_to_file_single_mode(self, sample_tables_data, sample_tables_schema, tmp_path):
        generator = JSONGenerator(mode="single")
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        output_file = tmp_path / "output.json"
        generator.write_to_file(result, output_file)

        assert output_file.exists()
        content = output_file.read_text(encoding="utf-8")
        parsed = json.loads(content)
        assert parsed["metadata"]["table_count"] == 2

    def test_write_to_file_per_table_mode(self, sample_tables_data, sample_tables_schema, tmp_path):
        generator = JSONGenerator(mode="per-table")
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        output_dir = tmp_path / "output"
        generator.write_to_file(result, output_dir)

        assert output_dir.exists()
        assert (output_dir / "users.json").exists()
        assert (output_dir / "orders.json").exists()

        users_content = (output_dir / "users.json").read_text(encoding="utf-8")
        users_parsed = json.loads(users_content)
        assert users_parsed["table"] == "users"
        assert users_parsed["row_count"] == 2

    def test_write_to_file_creates_parent_dirs(
        self, sample_tables_data, sample_tables_schema, tmp_path
    ):
        generator = JSONGenerator(mode="single")
        insert_order = ["users", "orders"]

        result = generator.generate(
            sample_tables_data,
            insert_order,
            sample_tables_schema,
        )

        # Create nested path that doesn't exist yet
        output_file = tmp_path / "nested" / "dir" / "output.json"
        generator.write_to_file(result, output_file)

        assert output_file.exists()

    def test_write_to_file_invalid_type_single_mode(self, sample_tables_schema):
        generator = JSONGenerator(mode="single")

        with pytest.raises(ValueError, match="Single mode output must be a string"):
            generator.write_to_file({}, Path("/tmp/test.json"))

    def test_write_to_file_invalid_type_per_table_mode(self, sample_tables_schema):
        generator = JSONGenerator(mode="per-table")

        with pytest.raises(ValueError, match="Per-table mode output must be a dict"):
            generator.write_to_file("string", Path("/tmp/test"))

    def test_unicode_handling(self, sample_tables_schema):
        generator = JSONGenerator(mode="single")

        tables_data = {
            "users": [
                {
                    "id": 1,
                    "name": "Test User 中文测试",
                    "emoji": "Hello 😀 World",
                }
            ]
        }
        insert_order = ["users"]

        result = generator.generate(tables_data, insert_order, sample_tables_schema)

        # ensure_ascii=False means unicode characters are preserved
        assert "中文测试" in result
        assert "😀" in result

        # Verify it's still valid JSON
        parsed = json.loads(result)
        assert parsed["tables"]["users"][0]["name"] == "Test User 中文测试"
        assert parsed["tables"]["users"][0]["emoji"] == "Hello 😀 World"

    def test_null_values(self, sample_tables_schema):
        generator = JSONGenerator(mode="single")

        tables_data = {
            "users": [
                {"id": 1, "email": "test@example.com", "name": None},
                {"id": 2, "email": None, "name": "Test"},
            ]
        }
        insert_order = ["users"]

        result = generator.generate(tables_data, insert_order, sample_tables_schema)
        parsed = json.loads(result)

        assert parsed["tables"]["users"][0]["name"] is None
        assert parsed["tables"]["users"][1]["email"] is None


class TestGenerateJsonFunction:
    """Tests for the generate_json convenience function."""

    @pytest.fixture
    def sample_tables_schema(self):
        return {
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

    def test_basic_usage_single_mode(self, sample_tables_schema):
        tables_data = {"users": [{"id": 1, "email": "test@example.com"}]}
        insert_order = ["users"]

        result = generate_json(
            tables_data,
            insert_order,
            sample_tables_schema,
            mode="single",
        )

        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "metadata" in parsed
        assert "tables" in parsed

    def test_basic_usage_per_table_mode(self, sample_tables_schema):
        tables_data = {"users": [{"id": 1, "email": "test@example.com"}]}
        insert_order = ["users"]

        result = generate_json(
            tables_data,
            insert_order,
            sample_tables_schema,
            mode="per-table",
        )

        assert isinstance(result, dict)
        assert "users" in result

    def test_pretty_false(self, sample_tables_schema):
        tables_data = {"users": [{"id": 1, "email": "test@example.com"}]}
        insert_order = ["users"]

        result = generate_json(
            tables_data,
            insert_order,
            sample_tables_schema,
            pretty=False,
        )

        # Compact output should have minimal newlines
        assert result.count("\n") <= 1


class TestIntegration:
    """Integration tests for JSON output."""

    def test_full_extraction_workflow(self, tmp_path):
        """Test a complete extraction workflow with JSON output."""
        # Simulate extraction result
        tables_schema = {
            "users": Table(
                name="users",
                schema="public",
                columns=[
                    Column("id", "INTEGER", False, True),
                    Column("email", "TEXT", False, False),
                    Column("created_at", "TIMESTAMP", False, False),
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
                    Column("amount", "DECIMAL", False, False),
                    Column("created_at", "TIMESTAMP", False, False),
                ],
                primary_key=("id",),
                foreign_keys=[],
            ),
        }

        tables_data = {
            "users": [
                {
                    "id": 1,
                    "email": "user1@example.com",
                    "created_at": datetime(2024, 1, 15, 10, 30),
                },
                {
                    "id": 2,
                    "email": "user2@example.com",
                    "created_at": datetime(2024, 1, 16, 11, 45),
                },
            ],
            "orders": [
                {
                    "id": 1,
                    "user_id": 1,
                    "amount": Decimal("99.99"),
                    "created_at": datetime(2024, 1, 17, 14, 20),
                },
                {
                    "id": 2,
                    "user_id": 1,
                    "amount": Decimal("149.99"),
                    "created_at": datetime(2024, 1, 18, 9, 15),
                },
                {
                    "id": 3,
                    "user_id": 2,
                    "amount": Decimal("79.99"),
                    "created_at": datetime(2024, 1, 19, 16, 30),
                },
            ],
        }

        insert_order = ["users", "orders"]

        # Test single file output
        generator = JSONGenerator(mode="single", pretty=True)
        json_output = generator.generate(tables_data, insert_order, tables_schema)

        output_file = tmp_path / "extraction.json"
        generator.write_to_file(json_output, output_file)

        # Verify file was created and is valid JSON
        assert output_file.exists()
        content = output_file.read_text(encoding="utf-8")
        parsed = json.loads(content)

        # Verify metadata
        assert parsed["metadata"]["table_count"] == 2
        assert parsed["metadata"]["total_rows"] == 5
        assert parsed["metadata"]["insert_order"] == ["users", "orders"]

        # Verify data
        assert len(parsed["tables"]["users"]) == 2
        assert len(parsed["tables"]["orders"]) == 3
        assert parsed["tables"]["users"][0]["email"] == "user1@example.com"
        assert parsed["tables"]["orders"][0]["amount"] == 99.99
