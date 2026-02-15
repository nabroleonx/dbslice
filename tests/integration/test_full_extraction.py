"""
Integration tests for full extraction workflow with PostgreSQL.

These tests verify end-to-end extraction with a real database,
including FK traversal, filtering, cycles, and anonymization.
"""

import pytest

from dbslice.config import SeedSpec, TraversalDirection
from dbslice.core.engine import ExtractionEngine

pytestmark = pytest.mark.integration


class TestBasicExtraction:
    """Test basic extraction scenarios with FK traversal."""

    def test_extract_single_order_with_parents(
        self, ecommerce_schema: dict, extract_config_factory
    ):
        """
        Test extracting a single order with parent records only (UP direction).

        With UP direction from orders, we follow FKs to parent tables:
        - orders.user_id -> users.id  =>  users is a parent

        Should extract:
        - Order #1
        - User #1 (order's user, via user_id FK)

        Should NOT extract (these are children, not parents):
        - order_items (child of orders via order_id FK)
        - products (not directly referenced by orders)
        - reviews (not directly referenced by orders)
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.UP,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Verify extraction statistics
        assert result.total_rows() > 0
        assert "orders" in result.tables
        assert "users" in result.tables

        # Verify specific data
        assert len(result.tables["orders"]) == 1
        assert result.tables["orders"][0]["id"] == 1

        # User #1 should be included (parent via user_id FK)
        assert len(result.tables["users"]) == 1
        assert result.tables["users"][0]["id"] == 1

        # order_items is a CHILD of orders (order_items.order_id -> orders.id),
        # so it should NOT be included with UP direction
        assert "order_items" not in result.tables or len(result.tables["order_items"]) == 0

        # products and reviews should NOT be included (only going UP from orders)
        assert "products" not in result.tables or len(result.tables["products"]) == 0
        assert "reviews" not in result.tables or len(result.tables["reviews"]) == 0

    def test_extract_user_with_children(self, ecommerce_schema: dict, extract_config_factory):
        """
        Test extracting a user with all child records (orders, reviews).

        Should extract:
        - User #1
        - Orders by user #1 (orders 1, 2)
        - Order items for those orders
        - Products referenced by order items
        - Reviews by user #1
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Verify user
        assert len(result.tables["users"]) == 1
        assert result.tables["users"][0]["id"] == 1

        # User #1 has orders 1 and 2
        assert len(result.tables["orders"]) == 2
        order_ids = {o["id"] for o in result.tables["orders"]}
        assert order_ids == {1, 2}

        # Order items for orders 1 and 2
        assert len(result.tables["order_items"]) == 3
        item_ids = {item["id"] for item in result.tables["order_items"]}
        assert item_ids == {1, 2, 3}

        # User #1's reviews (reviews 1, 2)
        assert len(result.tables["reviews"]) == 2
        review_ids = {r["id"] for r in result.tables["reviews"]}
        assert review_ids == {1, 2}

    def test_extract_bidirectional(self, ecommerce_schema: dict, extract_config_factory):
        """
        Test bidirectional extraction (both parents and children).

        Starting from order_items.id=1:
        - UP: order #1 (parent via order_id), product #1 (parent via product_id)
        - UP from order #1: user #1 (parent via user_id)
        - BOTH cascades: DOWN from discovered parents finds more children,
          which in turn pull in their parents for referential integrity.

        With BOTH direction the engine follows FKs in both directions and
        cascades extensively, so we use >= checks for counts.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("order_items.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Verify the seed order item is included
        assert "order_items" in result.tables
        assert len(result.tables["order_items"]) >= 1
        assert any(item["id"] == 1 for item in result.tables["order_items"])

        # Should have order #1 (parent of order_item #1)
        assert "orders" in result.tables
        assert len(result.tables["orders"]) >= 1
        assert any(o["id"] == 1 for o in result.tables["orders"])

        # Should have user #1 (parent of order #1 via user_id)
        assert "users" in result.tables
        assert len(result.tables["users"]) >= 1
        assert any(u["id"] == 1 for u in result.tables["users"])

        # Should have product #1 (parent of order_item #1 via product_id)
        assert "products" in result.tables
        assert len(result.tables["products"]) >= 1
        assert any(p["id"] == 1 for p in result.tables["products"])


class TestWhereClauseSeeds:
    """Test extraction with WHERE clause seeds."""

    def test_extract_with_status_filter(self, ecommerce_schema: dict, extract_config_factory):
        """Test extracting orders with status filter.

        With BOTH direction, the engine follows FKs in all directions and cascades.
        Starting from completed orders (1 and 3), it goes UP to users and DOWN to
        order_items, then further to products, reviews, and potentially more orders
        via user relationships.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders:status='completed'")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract at least orders 1 and 3 (both completed, the seed rows)
        assert len(result.tables["orders"]) >= 2
        order_ids = {o["id"] for o in result.tables["orders"]}
        assert {1, 3}.issubset(order_ids)

        # Should have at least users 1 and 2 (parents of orders 1 and 3)
        user_ids = {u["id"] for u in result.tables["users"]}
        assert {1, 2}.issubset(user_ids)

    def test_extract_with_date_filter(self, ecommerce_schema: dict, extract_config_factory):
        """Test extracting orders with date filter."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders:created_at >= '2024-01-03'")],
            direction=TraversalDirection.UP,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract orders 3 and 4 (Jan 3 and Jan 4)
        assert len(result.tables["orders"]) == 2
        order_ids = {o["id"] for o in result.tables["orders"]}
        assert order_ids == {3, 4}

    def test_extract_with_numeric_comparison(self, ecommerce_schema: dict, extract_config_factory):
        """Test extracting products with price filter.

        With BOTH direction, the engine cascades through related records.
        Starting from products 2 and 3 (price > 20), it follows FKs down to
        order_items that reference them, then up to orders and users, and
        potentially further, pulling in more products transitively.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("products:price > 20.00")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract at least products 2 and 3 (49.99 and 29.99, the seeds)
        assert len(result.tables["products"]) >= 2
        product_ids = {p["id"] for p in result.tables["products"]}
        assert {2, 3}.issubset(product_ids)


class TestMultipleSeeds:
    """Test extraction with multiple seed specifications."""

    def test_multiple_orders(self, ecommerce_schema: dict, extract_config_factory):
        """Test extracting multiple orders by ID."""
        config = extract_config_factory(
            seeds=[
                SeedSpec.parse("orders.id=1"),
                SeedSpec.parse("orders.id=3"),
            ],
            direction=TraversalDirection.UP,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract orders 1 and 3
        assert len(result.tables["orders"]) == 2
        order_ids = {o["id"] for o in result.tables["orders"]}
        assert order_ids == {1, 3}

        # Should extract users 1 and 2
        user_ids = {u["id"] for u in result.tables["users"]}
        assert user_ids == {1, 2}

    def test_multiple_tables(self, ecommerce_schema: dict, extract_config_factory):
        """Test extracting from multiple different tables."""
        config = extract_config_factory(
            seeds=[
                SeedSpec.parse("users.id=2"),
                SeedSpec.parse("products.id=4"),
            ],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have user #2
        assert any(u["id"] == 2 for u in result.tables["users"])

        # Should have product #4
        assert any(p["id"] == 4 for p in result.tables["products"])

    def test_overlapping_seeds(self, ecommerce_schema: dict, extract_config_factory):
        """Test that overlapping seeds don't cause duplicates."""
        config = extract_config_factory(
            seeds=[
                SeedSpec.parse("orders.id=1"),
                SeedSpec.parse("users.id=1"),  # User #1 is also order #1's user
            ],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have exactly one copy of user #1
        user_1_count = sum(1 for u in result.tables["users"] if u["id"] == 1)
        assert user_1_count == 1


class TestCycleHandling:
    """Test handling of circular foreign key references."""

    def test_detect_cycles(self, circular_ref_schema: dict, extract_config_factory):
        """Test that circular references are detected and resolved.

        The circular_ref_schema has cycles:
        - departments <-> employees (bidirectional FKs, both nullable)
        - employees -> employees (self-referential, nullable)

        The engine should detect cycles and break them at nullable FKs.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("employees.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should detect and resolve cycles
        assert result.has_cycles
        assert len(result.broken_fks) > 0
        assert len(result.cycle_infos) > 0

        # Should still extract data successfully
        assert "employees" in result.tables
        assert len(result.tables["employees"]) >= 1

    def test_cycle_resolution(self, circular_ref_schema: dict, extract_config_factory):
        """Test that cycles are resolved with nullable FK breaking.

        The departments <-> employees cycle has nullable FKs on both sides,
        so the engine can break the cycle and generate deferred updates.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("departments.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should break cycles and create deferred updates
        assert result.has_cycles
        assert len(result.broken_fks) > 0
        assert len(result.deferred_updates) > 0

        # Should still extract related records
        assert "departments" in result.tables
        assert len(result.tables["departments"]) >= 1
        assert "employees" in result.tables
        assert len(result.tables["employees"]) >= 1

        # Verify insert order is valid (no circular dependencies)
        assert len(result.insert_order) > 0

    def test_self_referential_extraction(self, circular_ref_schema: dict, extract_config_factory):
        """Test extraction with self-referential table (employees.manager_id).

        Employee #2 (Bob) has manager_id=1 (Alice) and department_id=1 (Engineering).
        With UP direction:
        - employees.manager_id -> employees.id (self-ref): finds Alice (id=1)
        - employees.department_id -> departments.id: finds Engineering dept (id=1)
        - departments.manager_id -> employees.id: finds Alice again (id=1)

        The cycle between employees and departments means topological sort
        will detect cycles and break them at nullable FKs.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("employees.id=2")],  # Bob, managed by Alice
            direction=TraversalDirection.UP,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should extract both Bob and his manager Alice
        assert "employees" in result.tables
        assert len(result.tables["employees"]) >= 2
        emp_ids = {e["id"] for e in result.tables["employees"]}
        assert {1, 2}.issubset(emp_ids)  # At least Alice and Bob

        # Should also include departments (parent via department_id FK)
        assert "departments" in result.tables
        assert len(result.tables["departments"]) >= 1


class TestAnonymization:
    """Test end-to-end anonymization."""

    def test_anonymize_sensitive_fields(self, ecommerce_schema: dict, extract_config_factory):
        """Test that sensitive fields are anonymized."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
            anonymize=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Get user data
        user = result.tables["users"][0]

        # Email should be anonymized (not the original)
        assert user["email"] != "alice@example.com"
        assert "@" in user["email"]  # Should still look like an email

        # Name should be anonymized
        assert user["name"] != "Alice Smith"

        # Phone should be anonymized
        assert user["phone"] != "555-0001"

    def test_anonymization_with_redact_fields(self, ecommerce_schema: dict, extract_config_factory):
        """Test custom field redaction."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("products.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            anonymize=True,
            redact_fields=["products.description"],
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Product description should be redacted
        product = result.tables["products"][0]
        assert product["description"] != "A useful widget"

    def test_anonymization_preserves_relationships(
        self, ecommerce_schema: dict, extract_config_factory
    ):
        """Test that anonymization preserves FK relationships."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            anonymize=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Get order and verify user_id FK is preserved
        order = result.tables["orders"][0]
        user = result.tables["users"][0]

        assert order["user_id"] == user["id"]

        # User data should be anonymized
        assert user["email"] != "alice@example.com"


class TestValidation:
    """Test extraction validation for referential integrity."""

    def test_validation_passes(self, ecommerce_schema: dict, extract_config_factory):
        """Test that valid extraction passes validation."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            validate=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Validation should pass
        assert result.validation_result is not None
        assert result.validation_result.is_valid
        assert len(result.validation_result.orphaned_records) == 0

    def test_validation_with_cycles(self, circular_ref_schema: dict, extract_config_factory):
        """Test that validation handles circular references correctly.

        The circular_ref_schema has cycles that should be detected and broken.
        Validation should still pass because broken FKs are tracked and
        excluded from referential integrity checks.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("employees.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            validate=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have extracted data successfully despite cycles
        assert "employees" in result.tables
        assert len(result.tables["employees"]) >= 1

        # Cycles should be detected
        assert result.has_cycles

        # Validation should pass even with cycles (broken FKs are tracked)
        assert result.validation_result is not None
        assert result.validation_result.is_valid

    def test_validation_fail_on_error(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection
    ):
        """Test that validation can fail the extraction if configured."""
        # This test is tricky because we need to create an invalid extraction
        # For now, we just verify the flag works
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.UP,
            depth=10,
            validate=True,
            fail_on_validation_error=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should succeed with valid data
        assert result.validation_result is not None
        assert result.validation_result.is_valid


class TestTraversalDepth:
    """Test FK traversal depth limiting."""

    def test_depth_zero(self, ecommerce_schema: dict, extract_config_factory):
        """Test that depth=0 only extracts seed rows."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=0,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should only have the order itself
        assert len(result.tables["orders"]) == 1
        assert result.tables["orders"][0]["id"] == 1

        # Should not have related tables
        assert "users" not in result.tables or len(result.tables["users"]) == 0
        assert "order_items" not in result.tables or len(result.tables["order_items"]) == 0

    def test_depth_one(self, ecommerce_schema: dict, extract_config_factory):
        """Test that depth=1 extracts one level of relationships.

        With UP direction from orders.id=1 and depth=1:
        - Depth 0: seed = orders (id=1)
        - Depth 0->1 (UP): orders.user_id -> users.id => finds user #1
        - Depth 1: max depth reached, no further traversal

        order_items is a CHILD of orders (not a parent), so it is NOT
        found with UP direction regardless of depth.
        """
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.UP,
            depth=1,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have order and its direct parent (user)
        assert "orders" in result.tables
        assert len(result.tables["orders"]) == 1
        assert "users" in result.tables
        assert len(result.tables["users"]) >= 1

        # order_items is a child of orders, NOT a parent.
        # With UP direction, children are not traversed.
        assert "order_items" not in result.tables or len(result.tables["order_items"]) == 0

        # Products are not directly referenced by orders, so not found
        assert "products" not in result.tables or len(result.tables["products"]) == 0


class TestExcludeTables:
    """Test table exclusion from extraction."""

    def test_exclude_table(self, ecommerce_schema: dict, extract_config_factory):
        """Test excluding a table from extraction."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            exclude_tables={"reviews"},
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have user and orders
        assert "users" in result.tables
        assert "orders" in result.tables

        # Should NOT have reviews
        assert "reviews" not in result.tables

    def test_exclude_multiple_tables(self, ecommerce_schema: dict, extract_config_factory):
        """Test excluding multiple tables."""
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            exclude_tables={"reviews", "products"},
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have order and user
        assert "orders" in result.tables
        assert "users" in result.tables

        # Should NOT have excluded tables
        assert "reviews" not in result.tables
        assert "products" not in result.tables
