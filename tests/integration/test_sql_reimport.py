"""
Integration tests for SQL reimport verification.

These tests verify that generated SQL can be successfully re-imported
into a fresh database and that referential integrity is preserved.
"""

import pytest

from dbslice.config import DatabaseType, SeedSpec, TraversalDirection
from dbslice.core.engine import ExtractionEngine
from dbslice.output.sql import SQLGenerator

from .conftest import count_rows, execute_sql_file, fetch_all_rows

pytestmark = pytest.mark.integration


class TestSQLReimport:
    """Test that generated SQL can be re-imported successfully."""

    def test_reimport_basic_extraction(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test reimporting a basic extraction."""
        # Extract data
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        # Clear database (TRUNCATE to keep table structure for reimport)
        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        # Import SQL
        execute_sql_file(pg_connection, sql)

        # Verify data was imported — BOTH direction pulls in the full
        # connected component, so assert minimum expected counts
        assert count_rows(pg_connection, "orders") >= 1
        assert count_rows(pg_connection, "users") >= 1
        assert count_rows(pg_connection, "order_items") >= 2
        assert count_rows(pg_connection, "products") >= 2

    def test_reimport_preserves_data(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that reimported data matches original."""
        # Extract user 1 with BOTH direction. Needs sufficient depth so the
        # BFS can follow children (orders, reviews) AND then go back up to
        # fetch their parents (products) for referential integrity.
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.BOTH,
            depth=5,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Store original data
        original_user = next(u for u in result.tables["users"] if u["id"] == 1)

        # Generate and reimport SQL (include broken_fks/deferred_updates for safety)
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        # Clear all tables and reimport the full extraction
        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify user data matches
        reimported = fetch_all_rows(pg_connection, "users")
        reimported_user = next(u for u in reimported if u["id"] == 1)
        assert reimported_user["id"] == original_user["id"]
        assert reimported_user["email"] == original_user["email"]
        assert reimported_user["name"] == original_user["name"]


class TestReferentialIntegrity:
    """Test that referential integrity is preserved after reimport."""

    def test_foreign_keys_valid(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that all foreign keys are valid after reimport."""
        # Extract order with all relationships
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        # Clear and reimport
        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify FK constraints (will fail if FKs are invalid)
        with pg_connection.cursor() as cur:
            # Verify orders.user_id references valid user
            cur.execute("""
                SELECT COUNT(*) FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                WHERE u.id IS NULL
            """)
            assert cur.fetchone()[0] == 0

            # Verify order_items.order_id references valid order
            cur.execute("""
                SELECT COUNT(*) FROM order_items oi
                LEFT JOIN orders o ON oi.order_id = o.id
                WHERE o.id IS NULL
            """)
            assert cur.fetchone()[0] == 0

            # Verify order_items.product_id references valid product
            cur.execute("""
                SELECT COUNT(*) FROM order_items oi
                LEFT JOIN products p ON oi.product_id = p.id
                WHERE p.id IS NULL
            """)
            assert cur.fetchone()[0] == 0

    def test_insert_order_respected(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that INSERT order respects dependencies."""
        # Extract data
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Verify insert order is safe (parents before children)
        # users and products should come before orders
        users_idx = result.insert_order.index("users")
        products_idx = result.insert_order.index("products")
        orders_idx = result.insert_order.index("orders")
        items_idx = result.insert_order.index("order_items")

        assert users_idx < orders_idx
        assert products_idx < items_idx
        assert orders_idx < items_idx

    def test_no_orphaned_records(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that reimport has no orphaned records."""
        # Extract with validation
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            validate=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should pass validation
        assert result.validation_result.is_valid

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Manually verify no orphaned records
        with pg_connection.cursor() as cur:
            # Check all FKs are valid
            cur.execute("""
                SELECT
                    'orders.user_id' as fk,
                    COUNT(*) as orphaned
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                WHERE u.id IS NULL

                UNION ALL

                SELECT
                    'order_items.order_id' as fk,
                    COUNT(*) as orphaned
                FROM order_items oi
                LEFT JOIN orders o ON oi.order_id = o.id
                WHERE o.id IS NULL

                UNION ALL

                SELECT
                    'order_items.product_id' as fk,
                    COUNT(*) as orphaned
                FROM order_items oi
                LEFT JOIN products p ON oi.product_id = p.id
                WHERE p.id IS NULL
            """)

            for row in cur.fetchall():
                fk_name, orphaned_count = row
                assert orphaned_count == 0, f"{fk_name} has {orphaned_count} orphaned records"


class TestCycleResolution:
    """Test that circular references are resolved correctly on reimport."""

    def test_cycle_broken_fks(
        self, circular_ref_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that broken FKs are NULL in initial INSERT."""
        # Extract with cycles
        config = extract_config_factory(
            seeds=[SeedSpec.parse("employees.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have cycles
        assert result.has_cycles
        assert len(result.broken_fks) > 0

        # Generate SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        # SQL should contain both INSERT and UPDATE statements
        assert "INSERT INTO" in sql
        assert "UPDATE" in sql

        # Clear and reimport
        with pg_connection.cursor() as cur:
            for table in ["project_assignments", "projects", "employees", "departments"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify data was imported
        assert count_rows(pg_connection, "employees") > 0
        assert count_rows(pg_connection, "departments") > 0

    def test_deferred_updates_applied(
        self, circular_ref_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that deferred UPDATEs restore broken FK values."""
        # Extract with cycles
        config = extract_config_factory(
            seeds=[SeedSpec.parse("departments.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Should have deferred updates
        assert len(result.deferred_updates) > 0

        # Get original data before reimport
        original_dept = next(d for d in result.tables["departments"] if d["id"] == 1)
        original_manager_id = original_dept["manager_id"]

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        with pg_connection.cursor() as cur:
            for table in ["project_assignments", "projects", "employees", "departments"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify deferred updates were applied (FK values restored)
        reimported = fetch_all_rows(pg_connection, "departments")
        dept_1 = next(d for d in reimported if d["id"] == 1)

        # Manager FK should be restored to original value
        assert dept_1["manager_id"] == original_manager_id

    def test_cycle_resolution_preserves_integrity(
        self, circular_ref_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that cycle resolution doesn't break referential integrity."""
        # Extract with cycles
        config = extract_config_factory(
            seeds=[SeedSpec.parse("employees.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            validate=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        with pg_connection.cursor() as cur:
            for table in ["project_assignments", "projects", "employees", "departments"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify all FKs are valid after reimport
        with pg_connection.cursor() as cur:
            # Check departments.manager_id references valid employee
            cur.execute("""
                SELECT COUNT(*) FROM departments d
                LEFT JOIN employees e ON d.manager_id = e.id
                WHERE d.manager_id IS NOT NULL AND e.id IS NULL
            """)
            assert cur.fetchone()[0] == 0

            # Check employees.department_id references valid department
            cur.execute("""
                SELECT COUNT(*) FROM employees e
                LEFT JOIN departments d ON e.department_id = d.id
                WHERE e.department_id IS NOT NULL AND d.id IS NULL
            """)
            assert cur.fetchone()[0] == 0

            # Check employees.manager_id references valid employee
            cur.execute("""
                SELECT COUNT(*) FROM employees e1
                LEFT JOIN employees e2 ON e1.manager_id = e2.id
                WHERE e1.manager_id IS NOT NULL AND e2.id IS NULL
            """)
            assert cur.fetchone()[0] == 0


class TestAnonymizedReimport:
    """Test that anonymized data can be reimported while preserving structure."""

    def test_anonymized_data_reimports(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that anonymized data can be successfully reimported."""
        # Extract with anonymization
        config = extract_config_factory(
            seeds=[SeedSpec.parse("users.id=1")],
            direction=TraversalDirection.DOWN,
            depth=10,
            anonymize=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
        )

        # Clear and reimport
        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify data was imported
        assert count_rows(pg_connection, "users") > 0

        # Verify data is anonymized
        users = fetch_all_rows(pg_connection, "users")
        user = users[0]
        assert user["email"] != "alice@example.com"
        assert user["name"] != "Alice Smith"

    def test_anonymized_fks_preserved(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test that FK relationships are preserved with anonymization."""
        # Extract with anonymization
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            direction=TraversalDirection.BOTH,
            depth=10,
            anonymize=True,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
        )

        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify FK relationships are intact
        with pg_connection.cursor() as cur:
            # All order.user_id should reference valid users
            cur.execute("""
                SELECT COUNT(*) FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
                WHERE u.id IS NULL
            """)
            assert cur.fetchone()[0] == 0


class TestComplexReimport:
    """Test reimport of complex extraction scenarios."""

    def test_reimport_multiple_seeds(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test reimporting extraction from multiple seeds."""
        # Extract from multiple seeds
        config = extract_config_factory(
            seeds=[
                SeedSpec.parse("orders.id=1"),
                SeedSpec.parse("orders.id=3"),
            ],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify both orders were imported
        orders = fetch_all_rows(pg_connection, "orders")
        order_ids = {o["id"] for o in orders}
        assert {1, 3}.issubset(order_ids)

    def test_reimport_with_where_clause(
        self, ecommerce_schema: dict, extract_config_factory, pg_connection, clean_database
    ):
        """Test reimporting extraction with WHERE clause seed."""
        # Extract with WHERE clause
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders:status='completed'")],
            direction=TraversalDirection.BOTH,
            depth=10,
        )

        engine = ExtractionEngine(config)
        result, schema = engine.extract()

        # Generate and reimport SQL
        generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
        sql = generator.generate(
            result.tables,
            result.insert_order,
            schema.tables,
            result.broken_fks,
            result.deferred_updates,
        )

        with pg_connection.cursor() as cur:
            for table in ["reviews", "order_items", "orders", "products", "users"]:
                cur.execute(f'TRUNCATE TABLE "{table}" CASCADE')

        execute_sql_file(pg_connection, sql)

        # Verify that the seed completed orders were imported.
        # With BOTH direction, related orders (via shared users) may also be
        # included to maintain referential integrity, so we verify that at
        # least the completed orders are present rather than asserting all
        # orders are completed.
        orders = fetch_all_rows(pg_connection, "orders")
        completed_orders = [o for o in orders if o["status"] == "completed"]
        assert len(completed_orders) >= 2  # orders 1 and 3 are completed
