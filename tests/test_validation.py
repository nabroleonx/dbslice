"""Tests for extraction validation."""

import pytest

from dbslice.models import Column, ForeignKey, SchemaGraph, Table
from dbslice.validation import ExtractionValidator, OrphanedRecord, ValidationResult


@pytest.fixture
def simple_schema() -> SchemaGraph:
    """Create a simple schema with users -> orders relationship."""
    users_table = Table(
        name="users",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="email", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    orders_table = Table(
        name="orders",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=False),
            Column(name="total", data_type="REAL", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    fk_orders_users = ForeignKey(
        name="fk_orders_users",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )

    return SchemaGraph(
        tables={"users": users_table, "orders": orders_table},
        edges=[fk_orders_users],
    )


@pytest.fixture
def complex_schema() -> SchemaGraph:
    """Create a more complex schema: users -> orders -> order_items -> products."""
    users_table = Table(
        name="users",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="email", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    products_table = Table(
        name="products",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="name", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    orders_table = Table(
        name="orders",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    order_items_table = Table(
        name="order_items",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="order_id", data_type="INTEGER", nullable=False, is_primary_key=False),
            Column(name="product_id", data_type="INTEGER", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    fk_orders_users = ForeignKey(
        name="fk_orders_users",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )

    fk_items_orders = ForeignKey(
        name="fk_items_orders",
        source_table="order_items",
        source_columns=("order_id",),
        target_table="orders",
        target_columns=("id",),
        is_nullable=False,
    )

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
def nullable_fk_schema() -> SchemaGraph:
    """Create a schema with nullable foreign key."""
    users_table = Table(
        name="users",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="name", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    orders_table = Table(
        name="orders",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="INTEGER", nullable=True, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    fk_orders_users_nullable = ForeignKey(
        name="fk_orders_users_nullable",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=True,
    )

    return SchemaGraph(
        tables={"users": users_table, "orders": orders_table},
        edges=[fk_orders_users_nullable],
    )


class TestValidationResult:
    """Tests for ValidationResult."""

    def test_default_valid(self):
        result = ValidationResult()
        assert result.is_valid is True
        assert result.orphaned_records == []
        assert result.total_records_checked == 0
        assert result.total_fk_checks == 0

    def test_add_orphan_sets_invalid(self):
        result = ValidationResult()
        orphan = OrphanedRecord(
            table="orders",
            pk_values=(1,),
            fk_name="fk_orders_users",
            fk_columns=("user_id",),
            fk_values=(999,),
            parent_table="users",
            parent_pk_columns=("id",),
        )
        result.add_orphan(orphan)
        assert result.is_valid is False
        assert len(result.orphaned_records) == 1

    def test_format_report_valid(self):
        result = ValidationResult(
            is_valid=True,
            total_records_checked=100,
            total_fk_checks=50,
        )
        report = result.format_report()
        assert "Status: VALID" in report
        assert "Records checked: 100" in report
        assert "Foreign key checks performed: 50" in report

    def test_format_report_invalid(self):
        result = ValidationResult(total_records_checked=10, total_fk_checks=5)
        orphan = OrphanedRecord(
            table="orders",
            pk_values=(1,),
            fk_name="fk_orders_users",
            fk_columns=("user_id",),
            fk_values=(999,),
            parent_table="users",
            parent_pk_columns=("id",),
        )
        result.add_orphan(orphan)

        report = result.format_report()
        assert "Status: INVALID" in report
        assert "1 orphaned record(s)" in report
        assert "orders" in report

    def test_format_report_with_broken_fks(self):
        broken_fk = ForeignKey(
            name="fk_self_ref",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            is_nullable=True,
        )
        result = ValidationResult(broken_fks=[broken_fk])
        report = result.format_report()
        assert "Intentionally broken FKs" in report
        assert "fk_self_ref" in report


class TestExtractionValidator:
    """Tests for ExtractionValidator."""

    def test_valid_extraction(self, simple_schema):
        """Test validation of a valid extraction with all FKs satisfied."""
        validator = ExtractionValidator(simple_schema)

        tables = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
                {"id": 2, "email": "bob@example.com"},
            ],
            "orders": [
                {"id": 1, "user_id": 1, "total": 100.0},
                {"id": 2, "user_id": 2, "total": 200.0},
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is True
        assert len(result.orphaned_records) == 0
        assert result.total_records_checked == 4
        assert result.total_fk_checks == 2

    def test_orphaned_record_detected(self, simple_schema):
        """Test detection of orphaned record (missing parent)."""
        validator = ExtractionValidator(simple_schema)

        tables = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
            ],
            "orders": [
                {"id": 1, "user_id": 1, "total": 100.0},
                {"id": 2, "user_id": 999, "total": 200.0},  # user_id 999 doesn't exist
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is False
        assert len(result.orphaned_records) == 1
        orphan = result.orphaned_records[0]
        assert orphan.table == "orders"
        assert orphan.pk_values == (2,)
        assert orphan.fk_values == (999,)
        assert orphan.parent_table == "users"

    def test_multiple_orphans(self, simple_schema):
        """Test detection of multiple orphaned records."""
        validator = ExtractionValidator(simple_schema)

        tables = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
            ],
            "orders": [
                {"id": 1, "user_id": 999, "total": 100.0},  # Missing parent
                {"id": 2, "user_id": 888, "total": 200.0},  # Missing parent
                {"id": 3, "user_id": 1, "total": 300.0},  # Valid
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is False
        assert len(result.orphaned_records) == 2
        assert result.total_records_checked == 4

    def test_null_fk_allowed(self, nullable_fk_schema):
        """Test that NULL FK values are allowed for nullable FKs."""
        validator = ExtractionValidator(nullable_fk_schema)

        tables = {
            "users": [
                {"id": 1, "name": "Alice"},
            ],
            "orders": [
                {"id": 1, "user_id": None},  # NULL FK is valid
                {"id": 2, "user_id": 1},  # Valid FK
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is True
        assert len(result.orphaned_records) == 0

    def test_broken_fks_skipped(self, simple_schema):
        """Test that broken FKs (for cycles) are skipped during validation."""
        validator = ExtractionValidator(simple_schema)

        # Get the FK from schema
        broken_fk = simple_schema.edges[0]

        tables = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
            ],
            "orders": [
                {"id": 1, "user_id": 999, "total": 100.0},  # Would be orphaned
            ],
        }

        # Validate with broken FK list
        result = validator.validate(tables, broken_fks=[broken_fk])

        # Should be valid because the FK is broken (intentionally for cycles)
        assert result.is_valid is True
        assert len(result.orphaned_records) == 0
        assert len(result.broken_fks) == 1

    def test_complex_chain_valid(self, complex_schema):
        """Test validation of a complex FK chain with all references satisfied."""
        validator = ExtractionValidator(complex_schema)

        tables = {
            "users": [{"id": 1, "email": "alice@example.com"}],
            "products": [{"id": 10, "name": "Widget"}],
            "orders": [{"id": 100, "user_id": 1}],
            "order_items": [{"id": 1000, "order_id": 100, "product_id": 10}],
        }

        result = validator.validate(tables)

        assert result.is_valid is True
        assert len(result.orphaned_records) == 0
        assert result.total_fk_checks == 3  # order->user, item->order, item->product

    def test_complex_chain_missing_intermediate(self, complex_schema):
        """Test detection of orphaned record in middle of FK chain."""
        validator = ExtractionValidator(complex_schema)

        tables = {
            "users": [{"id": 1, "email": "alice@example.com"}],
            "products": [{"id": 10, "name": "Widget"}],
            "orders": [{"id": 100, "user_id": 1}],
            "order_items": [
                {"id": 1000, "order_id": 999, "product_id": 10}  # order 999 missing
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is False
        assert len(result.orphaned_records) == 1
        orphan = result.orphaned_records[0]
        assert orphan.table == "order_items"
        assert orphan.parent_table == "orders"

    def test_empty_extraction(self, simple_schema):
        """Test validation of empty extraction."""
        validator = ExtractionValidator(simple_schema)
        result = validator.validate({})

        assert result.is_valid is True
        assert len(result.orphaned_records) == 0
        assert result.total_records_checked == 0

    def test_single_table_no_fks(self, simple_schema):
        """Test validation of extraction with only parent table (no FKs to check)."""
        validator = ExtractionValidator(simple_schema)

        tables = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
                {"id": 2, "email": "bob@example.com"},
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is True
        assert result.total_records_checked == 2
        assert result.total_fk_checks == 0

    def test_orphan_with_composite_pk(self):
        """Test orphan detection with composite primary keys."""
        # Create schema with composite PK
        users_table = Table(
            name="users",
            schema="main",
            columns=[
                Column(name="org_id", data_type="INTEGER", nullable=False, is_primary_key=True),
                Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=True),
            ],
            primary_key=("org_id", "user_id"),
            foreign_keys=[],
        )

        orders_table = Table(
            name="orders",
            schema="main",
            columns=[
                Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
                Column(name="org_id", data_type="INTEGER", nullable=False, is_primary_key=False),
                Column(name="user_id", data_type="INTEGER", nullable=False, is_primary_key=False),
            ],
            primary_key=("id",),
            foreign_keys=[],
        )

        fk_orders_users = ForeignKey(
            name="fk_orders_users",
            source_table="orders",
            source_columns=("org_id", "user_id"),
            target_table="users",
            target_columns=("org_id", "user_id"),
            is_nullable=False,
        )

        schema = SchemaGraph(
            tables={"users": users_table, "orders": orders_table},
            edges=[fk_orders_users],
        )

        validator = ExtractionValidator(schema)

        tables = {
            "users": [
                {"org_id": 1, "user_id": 100},
            ],
            "orders": [
                {"id": 1, "org_id": 1, "user_id": 999},  # user 999 doesn't exist
            ],
        }

        result = validator.validate(tables)

        assert result.is_valid is False
        assert len(result.orphaned_records) == 1
        orphan = result.orphaned_records[0]
        assert orphan.fk_values == (1, 999)


class TestOrphanedRecord:
    """Tests for OrphanedRecord dataclass."""

    def test_orphaned_record_string(self):
        orphan = OrphanedRecord(
            table="orders",
            pk_values=(123,),
            fk_name="fk_orders_users",
            fk_columns=("user_id",),
            fk_values=(999,),
            parent_table="users",
            parent_pk_columns=("id",),
        )

        s = str(orphan)
        assert "orders" in s
        assert "users" in s
        assert "fk_orders_users" in s
        assert "parent not found" in s
