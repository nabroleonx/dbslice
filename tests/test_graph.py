"""Tests for FK graph traversal."""

from dbslice.config import TraversalDirection
from dbslice.core.graph import GraphTraverser, TraversalConfig, TraversalResult


class TestTraversalResult:
    """Tests for TraversalResult."""

    def test_add_records_new(self):
        result = TraversalResult()
        new = result.add_records("users", {(1,), (2,)})
        assert new == {(1,), (2,)}
        assert result.records["users"] == {(1,), (2,)}

    def test_add_records_existing(self):
        result = TraversalResult()
        result.add_records("users", {(1,), (2,)})
        new = result.add_records("users", {(2,), (3,)})
        # Only (3,) is new
        assert new == {(3,)}
        assert result.records["users"] == {(1,), (2,), (3,)}

    def test_get_records(self):
        result = TraversalResult()
        result.add_records("users", {(1,)})
        assert result.get_records("users") == {(1,)}
        assert result.get_records("nonexistent") == set()

    def test_total_records(self):
        result = TraversalResult()
        result.add_records("users", {(1,), (2,)})
        result.add_records("orders", {(10,)})
        assert result.total_records() == 3

    def test_table_count(self):
        result = TraversalResult()
        result.add_records("users", {(1,)})
        result.add_records("orders", {(10,)})
        assert result.table_count() == 2


class TestGraphTraverser:
    """Tests for GraphTraverser."""

    def test_traverse_up_single_level(self, sample_schema, mock_adapter):
        """Traversing up from orders should find users."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=1,
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("orders", {(1,)}, config)

        # Should have orders and users
        assert "orders" in result.records
        assert "users" in result.records
        # Order 1 has user_id=1
        assert (1,) in result.records["users"]

    def test_traverse_up_multi_level(self, sample_schema, mock_adapter):
        """Traversing up from order_items should find orders, users, products."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=3,
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("order_items", {(1,)}, config)

        # Should have order_items, orders, users, products
        assert "order_items" in result.records
        assert "orders" in result.records
        assert "users" in result.records
        assert "products" in result.records

    def test_traverse_down_single_level(self, sample_schema, mock_adapter):
        """Traversing down from users should find orders."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=1,
            direction=TraversalDirection.DOWN,
        )

        result = traverser.traverse("users", {(1,)}, config)

        # Should have users and orders
        assert "users" in result.records
        assert "orders" in result.records
        # User 1 has order 1
        assert (1,) in result.records["orders"]

    def test_traverse_both_directions(self, sample_schema, mock_adapter):
        """Traversing both directions from orders."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=2,
            direction=TraversalDirection.BOTH,
        )

        result = traverser.traverse("orders", {(1,)}, config)

        # Should have multiple tables
        assert "orders" in result.records
        assert "users" in result.records  # parent
        assert "order_items" in result.records  # children

    def test_depth_limit(self, sample_schema, mock_adapter):
        """Depth limit should prevent deep traversal."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=0,  # Only seed, no traversal
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("orders", {(1,)}, config)

        # Should only have orders (seed)
        assert "orders" in result.records
        # Should NOT have users (would require depth 1)
        assert "users" not in result.records

    def test_exclude_tables(self, sample_schema, mock_adapter):
        """Excluded tables should be skipped."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=3,
            direction=TraversalDirection.UP,
            exclude_tables={"products"},
        )

        result = traverser.traverse("order_items", {(1,)}, config)

        # Should NOT include products
        assert "products" not in result.records
        # But should include others
        assert "orders" in result.records
        assert "users" in result.records

    def test_no_duplicate_visits(self, sample_schema, mock_adapter):
        """Same record shouldn't be fetched twice."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=3,
            direction=TraversalDirection.UP,
        )

        # Start with multiple order_items that reference the same order
        result = traverser.traverse("order_items", {(1,), (2,)}, config)

        # Order 1 should only appear once (both items reference it)
        assert result.records["orders"] == {(1,)}

    def test_traversal_path_tracking(self, sample_schema, mock_adapter):
        """Traversal path should be recorded."""
        traverser = GraphTraverser(sample_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=2,
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("orders", {(1,)}, config)

        # Should have path entries
        assert len(result.traversal_path) > 0
        assert any("seed" in path for path in result.traversal_path)


class TestTraversalConfig:
    """Tests for TraversalConfig."""

    def test_defaults(self):
        config = TraversalConfig()
        assert config.max_depth == 3
        assert config.direction == TraversalDirection.BOTH
        assert config.exclude_tables == set()

    def test_custom_values(self):
        config = TraversalConfig(
            max_depth=5,
            direction=TraversalDirection.UP,
            exclude_tables={"audit_logs"},
        )
        assert config.max_depth == 5
        assert config.direction == TraversalDirection.UP
        assert "audit_logs" in config.exclude_tables
