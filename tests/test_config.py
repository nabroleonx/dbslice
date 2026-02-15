"""Tests for configuration dataclasses."""

import pytest

from dbslice.config import (
    DatabaseType,
    OutputFormat,
    SeedSpec,
    TraversalDirection,
)
from dbslice.models import Column, ForeignKey, Table


class TestDatabaseType:
    """Tests for DatabaseType enum."""

    def test_values(self):
        assert DatabaseType.POSTGRESQL.value == "postgresql"
        assert DatabaseType.MYSQL.value == "mysql"
        assert DatabaseType.SQLITE.value == "sqlite"


class TestColumn:
    """Tests for Column dataclass."""

    def test_create_column(self):
        col = Column(
            name="id",
            data_type="INTEGER",
            nullable=False,
            is_primary_key=True,
        )
        assert col.name == "id"
        assert col.data_type == "INTEGER"
        assert col.nullable is False
        assert col.is_primary_key is True
        assert col.default is None

    def test_column_with_default(self):
        col = Column(
            name="status",
            data_type="TEXT",
            nullable=False,
            is_primary_key=False,
            default="'active'",
        )
        assert col.default == "'active'"

    def test_column_hash(self):
        col1 = Column("id", "INTEGER", False, True)
        col2 = Column("id", "INTEGER", True, False)  # Same name/type
        col3 = Column("name", "TEXT", True, False)

        # Same name/type should hash the same
        assert hash(col1) == hash(col2)
        # Different name should hash differently
        assert hash(col1) != hash(col3)


class TestForeignKey:
    """Tests for ForeignKey dataclass."""

    def test_create_fk(self):
        fk = ForeignKey(
            name="fk_orders_users",
            source_table="orders",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            is_nullable=False,
        )
        assert fk.name == "fk_orders_users"
        assert fk.source_table == "orders"
        assert fk.target_table == "users"
        assert fk.is_nullable is False

    def test_as_edge(self):
        fk = ForeignKey(
            name="fk_orders_users",
            source_table="orders",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            is_nullable=False,
        )
        assert fk.as_edge() == ("orders", "users")

    def test_is_self_referential(self):
        # Not self-referential
        fk1 = ForeignKey(
            name="fk",
            source_table="orders",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            is_nullable=False,
        )
        assert fk1.is_self_referential is False

        # Self-referential
        fk2 = ForeignKey(
            name="fk",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            is_nullable=True,
        )
        assert fk2.is_self_referential is True


class TestTable:
    """Tests for Table dataclass."""

    def test_create_table(self):
        table = Table(
            name="users",
            schema="public",
            columns=[
                Column("id", "INTEGER", False, True),
                Column("email", "TEXT", False, False),
            ],
            primary_key=("id",),
            foreign_keys=[],
        )
        assert table.name == "users"
        assert table.schema == "public"
        assert len(table.columns) == 2
        assert table.primary_key == ("id",)

    def test_get_pk_columns(self):
        table = Table(
            name="order_items",
            schema="public",
            columns=[],
            primary_key=("order_id", "product_id"),
            foreign_keys=[],
        )
        assert table.get_pk_columns() == ("order_id", "product_id")

    def test_get_column(self):
        col = Column("email", "TEXT", False, False)
        table = Table(
            name="users",
            schema="public",
            columns=[Column("id", "INTEGER", False, True), col],
            primary_key=("id",),
            foreign_keys=[],
        )
        assert table.get_column("email") == col
        assert table.get_column("nonexistent") is None

    def test_get_column_names(self):
        table = Table(
            name="users",
            schema="public",
            columns=[
                Column("id", "INTEGER", False, True),
                Column("email", "TEXT", False, False),
                Column("name", "TEXT", True, False),
            ],
            primary_key=("id",),
            foreign_keys=[],
        )
        assert table.get_column_names() == ["id", "email", "name"]


class TestSchemaGraph:
    """Tests for SchemaGraph dataclass."""

    def test_get_parents(self, sample_schema):
        parents = sample_schema.get_parents("orders")
        assert len(parents) == 1
        parent_table, fk = parents[0]
        assert parent_table == "users"
        assert fk.source_columns == ("user_id",)

    def test_get_children(self, sample_schema):
        children = sample_schema.get_children("orders")
        assert len(children) == 1
        child_table, fk = children[0]
        assert child_table == "order_items"
        assert fk.source_columns == ("order_id",)

    def test_get_table(self, sample_schema):
        table = sample_schema.get_table("users")
        assert table is not None
        assert table.name == "users"

        assert sample_schema.get_table("nonexistent") is None

    def test_has_table(self, sample_schema):
        assert sample_schema.has_table("users") is True
        assert sample_schema.has_table("nonexistent") is False

    def test_get_table_names(self, sample_schema):
        names = sample_schema.get_table_names()
        assert set(names) == {"users", "products", "orders", "order_items"}


class TestSeedSpec:
    """Tests for SeedSpec parsing."""

    def test_parse_simple_equality(self):
        seed = SeedSpec.parse("orders.id=12345")
        assert seed.table == "orders"
        assert seed.column == "id"
        assert seed.value == 12345
        assert seed.where_clause is None

    def test_parse_string_value(self):
        seed = SeedSpec.parse("users.email='test@example.com'")
        assert seed.table == "users"
        assert seed.column == "email"
        assert seed.value == "test@example.com"

    def test_parse_where_clause(self):
        seed = SeedSpec.parse("orders:status='failed' AND total > 100")
        assert seed.table == "orders"
        assert seed.column is None
        assert seed.value is None
        assert seed.where_clause == "status='failed' AND total > 100"

    def test_parse_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid seed format"):
            SeedSpec.parse("invalid")

    def test_to_where_clause_simple(self):
        seed = SeedSpec.parse("orders.id=123")
        where, params = seed.to_where_clause()
        assert where == "id = %s"
        assert params == (123,)

    def test_to_where_clause_raw(self):
        seed = SeedSpec.parse("orders:status='failed'")
        where, params = seed.to_where_clause()
        assert where == "status='failed'"
        assert params == ()


class TestTraversalDirection:
    """Tests for TraversalDirection enum."""

    def test_values(self):
        assert TraversalDirection.UP.value == "up"
        assert TraversalDirection.DOWN.value == "down"
        assert TraversalDirection.BOTH.value == "both"


class TestOutputFormat:
    """Tests for OutputFormat enum."""

    def test_values(self):
        assert OutputFormat.SQL.value == "sql"
        assert OutputFormat.JSON.value == "json"
        assert OutputFormat.CSV.value == "csv"
