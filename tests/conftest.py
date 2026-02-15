"""Shared pytest fixtures for dbslice tests."""

import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest

from dbslice.adapters.base import DatabaseAdapter
from dbslice.models import Column, ForeignKey, SchemaGraph, Table


@pytest.fixture
def sample_schema() -> SchemaGraph:
    """Create a sample schema for testing."""
    users_table = Table(
        name="users",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="email", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="name", data_type="TEXT", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    products_table = Table(
        name="products",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="sku", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="name", data_type="TEXT", nullable=True, is_primary_key=False),
            Column(name="price", data_type="REAL", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    orders_table = Table(
        name="orders",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(
                name="user_id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
            Column(name="total", data_type="REAL", nullable=True, is_primary_key=False),
            Column(name="status", data_type="TEXT", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    order_items_table = Table(
        name="order_items",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(
                name="order_id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
            Column(
                name="product_id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
            Column(
                name="quantity",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    # FK: orders.user_id -> users.id
    fk_orders_users = ForeignKey(
        name="fk_orders_users",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )

    # FK: order_items.order_id -> orders.id
    fk_items_orders = ForeignKey(
        name="fk_items_orders",
        source_table="order_items",
        source_columns=("order_id",),
        target_table="orders",
        target_columns=("id",),
        is_nullable=False,
    )

    # FK: order_items.product_id -> products.id
    fk_items_products = ForeignKey(
        name="fk_items_products",
        source_table="order_items",
        source_columns=("product_id",),
        target_table="products",
        target_columns=("id",),
        is_nullable=False,
    )

    return SchemaGraph(
        tables={
            "users": users_table,
            "products": products_table,
            "orders": orders_table,
            "order_items": order_items_table,
        },
        edges=[fk_orders_users, fk_items_orders, fk_items_products],
    )


@pytest.fixture
def self_referential_schema() -> SchemaGraph:
    """Create a schema with self-referential FK (employees.manager_id)."""
    employees_table = Table(
        name="employees",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="name", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(
                name="manager_id",
                data_type="INTEGER",
                nullable=True,
                is_primary_key=False,
            ),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    # Self-referential FK
    fk_manager = ForeignKey(
        name="fk_employees_manager",
        source_table="employees",
        source_columns=("manager_id",),
        target_table="employees",
        target_columns=("id",),
        is_nullable=True,
    )

    return SchemaGraph(
        tables={"employees": employees_table},
        edges=[fk_manager],
    )


@pytest.fixture
def sqlite_db() -> Iterator[sqlite3.Connection]:
    """Create an in-memory SQLite database with test data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create tables
    conn.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            name TEXT
        );

        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT,
            price REAL
        );

        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            total REAL,
            status TEXT
        );

        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(id),
            product_id INTEGER NOT NULL REFERENCES products(id),
            quantity INTEGER NOT NULL
        );

        -- Insert test data
        INSERT INTO users (id, email, name) VALUES
            (1, 'alice@example.com', 'Alice'),
            (2, 'bob@example.com', 'Bob'),
            (3, 'charlie@example.com', 'Charlie');

        INSERT INTO products (id, sku, name, price) VALUES
            (1, 'WIDGET-001', 'Widget', 19.99),
            (2, 'GADGET-001', 'Gadget', 49.99),
            (3, 'GIZMO-001', 'Gizmo', 29.99);

        INSERT INTO orders (id, user_id, total, status) VALUES
            (1, 1, 69.98, 'completed'),
            (2, 1, 49.99, 'pending'),
            (3, 2, 19.99, 'completed');

        INSERT INTO order_items (id, order_id, product_id, quantity) VALUES
            (1, 1, 1, 2),
            (2, 1, 2, 1),
            (3, 2, 2, 1),
            (4, 3, 1, 1);
    """
    )

    yield conn
    conn.close()


class MockAdapter(DatabaseAdapter):
    """Mock database adapter for testing without a real database."""

    def __init__(self, schema: SchemaGraph, data: dict[str, list[dict[str, Any]]]):
        self.schema = schema
        self.data = data
        self._connected = False

    def connect(self, url: str) -> None:
        self._connected = True

    def close(self) -> None:
        self._connected = False

    def get_schema(self, schema_name: str | None = None) -> SchemaGraph:
        return self.schema

    def fetch_rows(
        self,
        table: str,
        where_clause: str,
        params: tuple[Any, ...],
    ) -> Iterator[dict[str, Any]]:
        # Simple implementation: just return all rows for the table
        # In real tests, you'd want to filter based on where_clause
        yield from self.data.get(table, [])

    def fetch_by_pk(
        self,
        table: str,
        pk_columns: tuple[str, ...],
        pk_values: set[tuple[Any, ...]],
    ) -> Iterator[dict[str, Any]]:
        table_data = self.data.get(table, [])
        for row in table_data:
            pk_tuple = tuple(row[col] for col in pk_columns)
            if pk_tuple in pk_values:
                yield row

    def fetch_fk_values(
        self,
        table: str,
        fk: ForeignKey,
        source_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        result = set()
        table_info = self.schema.get_table(table)
        if not table_info:
            return result

        pk_cols = table_info.primary_key
        fk_cols = fk.source_columns

        for row in self.data.get(table, []):
            pk_tuple = tuple(row[col] for col in pk_cols)
            if pk_tuple in source_pk_values:
                fk_tuple = tuple(row[col] for col in fk_cols)
                if None not in fk_tuple:
                    result.add(fk_tuple)

        return result

    def fetch_referencing_pks(
        self,
        fk: ForeignKey,
        target_pk_values: set[tuple[Any, ...]],
    ) -> set[tuple[Any, ...]]:
        result = set()
        source_table = fk.source_table
        table_info = self.schema.get_table(source_table)
        if not table_info:
            return result

        pk_cols = table_info.primary_key
        fk_cols = fk.source_columns

        for row in self.data.get(source_table, []):
            fk_tuple = tuple(row[col] for col in fk_cols)
            if fk_tuple in target_pk_values:
                pk_tuple = tuple(row[col] for col in pk_cols)
                result.add(pk_tuple)

        return result

    def fetch_all_pks(
        self,
        table: str,
        pk_columns: tuple[str, ...],
    ) -> set[tuple[Any, ...]]:
        """Fetch ALL primary keys from a table."""
        result = set()
        for row in self.data.get(table, []):
            pk_tuple = tuple(row[col] for col in pk_columns)
            result.add(pk_tuple)
        return result

    def get_table_pk_columns(self, table: str) -> tuple[str, ...]:
        table_info = self.schema.get_table(table)
        return table_info.primary_key if table_info else ()

    def begin_snapshot(self) -> None:
        pass

    def end_snapshot(self) -> None:
        pass


@pytest.fixture
def mock_adapter(sample_schema: SchemaGraph) -> MockAdapter:
    """Create a mock adapter with sample data."""
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "name": "Alice"},
            {"id": 2, "email": "bob@example.com", "name": "Bob"},
        ],
        "products": [
            {"id": 1, "sku": "WIDGET-001", "name": "Widget", "price": 19.99},
            {"id": 2, "sku": "GADGET-001", "name": "Gadget", "price": 49.99},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 69.98, "status": "completed"},
            {"id": 2, "user_id": 2, "total": 49.99, "status": "pending"},
        ],
        "order_items": [
            {"id": 1, "order_id": 1, "product_id": 1, "quantity": 2},
            {"id": 2, "order_id": 1, "product_id": 2, "quantity": 1},
            {"id": 3, "order_id": 2, "product_id": 2, "quantity": 1},
        ],
    }
    return MockAdapter(sample_schema, data)
