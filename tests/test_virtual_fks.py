"""Tests for virtual foreign key functionality."""

import pytest

from dbslice.config import TraversalDirection
from dbslice.config_file import DbsliceConfig, VirtualForeignKeyConfig
from dbslice.core.graph import GraphTraverser, TraversalConfig
from dbslice.models import Column, ForeignKey, SchemaGraph, Table, VirtualForeignKey


@pytest.fixture
def django_generic_fk_schema():
    """
    Create a schema simulating Django's GenericForeignKey pattern.

    This includes:
    - ContentType table (django_content_type)
    - Multiple target tables (orders, users)
    - A notifications table with generic FK (content_type_id + object_id)
    """
    # ContentType table
    content_type_table = Table(
        name="django_content_type",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="app_label", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="model", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    # Users table
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

    # Orders table
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
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    # Notifications table with GenericFK
    notifications_table = Table(
        name="notifications",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(
                name="content_type_id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
            Column(
                name="object_id",
                data_type="INTEGER",
                nullable=False,
                is_primary_key=False,
            ),
            Column(name="message", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    # Real FK: orders -> users
    fk_orders_users = ForeignKey(
        name="fk_orders_users",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )

    # Real FK: notifications -> content_type
    fk_notifications_content_type = ForeignKey(
        name="fk_notifications_content_type",
        source_table="notifications",
        source_columns=("content_type_id",),
        target_table="django_content_type",
        target_columns=("id",),
        is_nullable=False,
    )

    schema = SchemaGraph(
        tables={
            "django_content_type": content_type_table,
            "users": users_table,
            "orders": orders_table,
            "notifications": notifications_table,
        },
        edges=[fk_orders_users, fk_notifications_content_type],
    )

    # Add virtual FKs for GenericForeignKey relationships
    # Virtual FK: notifications -> orders (when content_type points to orders)
    vfk_notifications_orders = VirtualForeignKey(
        name="vfk_notifications_orders",
        source_table="notifications",
        source_columns=("object_id",),
        target_table="orders",
        target_columns=("id",),
        description="Generic FK to orders via ContentType",
        is_nullable=False,
    )

    # Virtual FK: notifications -> users (when content_type points to users)
    vfk_notifications_users = VirtualForeignKey(
        name="vfk_notifications_users",
        source_table="notifications",
        source_columns=("object_id",),
        target_table="users",
        target_columns=("id",),
        description="Generic FK to users via ContentType",
        is_nullable=False,
    )

    schema.add_virtual_fk(vfk_notifications_orders)
    schema.add_virtual_fk(vfk_notifications_users)

    return schema


@pytest.fixture
def implicit_fk_schema():
    """
    Create a schema with implicit FKs (relationships without DB constraints).

    This simulates legacy databases or cross-database relationships.
    """
    # Users table
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

    # Audit log table (no real FK to users)
    audit_log_table = Table(
        name="audit_log",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="INTEGER", nullable=True, is_primary_key=False),
            Column(name="action", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="timestamp", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("id",),
        foreign_keys=[],
    )

    schema = SchemaGraph(
        tables={
            "users": users_table,
            "audit_log": audit_log_table,
        },
        edges=[],  # No real FKs
    )

    # Add virtual FK for implicit relationship
    vfk_audit_users = VirtualForeignKey(
        name="vfk_audit_users",
        source_table="audit_log",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        description="Implicit FK without DB constraint",
        is_nullable=True,
    )

    schema.add_virtual_fk(vfk_audit_users)

    return schema


class TestVirtualForeignKey:
    """Tests for VirtualForeignKey model."""

    def test_virtual_fk_creation(self):
        vfk = VirtualForeignKey(
            name="vfk_test",
            source_table="source",
            source_columns=("col1", "col2"),
            target_table="target",
            target_columns=("id",),
            description="Test virtual FK",
        )

        assert vfk.name == "vfk_test"
        assert vfk.source_table == "source"
        assert vfk.source_columns == ("col1", "col2")
        assert vfk.target_table == "target"
        assert vfk.description == "Test virtual FK"
        assert vfk.is_nullable is True  # Default

    def test_virtual_fk_to_foreign_key(self):
        vfk = VirtualForeignKey(
            name="vfk_test",
            source_table="source",
            source_columns=("col1",),
            target_table="target",
            target_columns=("id",),
            description="Test",
            is_nullable=False,
        )

        fk = vfk.to_foreign_key()

        assert fk.name == vfk.name
        assert fk.source_table == vfk.source_table
        assert fk.source_columns == vfk.source_columns
        assert fk.target_table == vfk.target_table
        assert fk.target_columns == vfk.target_columns
        assert fk.is_nullable == vfk.is_nullable

    def test_virtual_fk_hash(self):
        vfk1 = VirtualForeignKey(
            name="vfk_test",
            source_table="source",
            source_columns=("col1",),
            target_table="target",
            target_columns=("id",),
            description="Test",
        )

        vfk2 = VirtualForeignKey(
            name="vfk_test",
            source_table="source",
            source_columns=("col1",),
            target_table="target",
            target_columns=("id",),
            description="Different description",  # Should still hash the same
        )

        # Same name, source, target should hash the same
        assert hash(vfk1) == hash(vfk2)

    def test_virtual_fk_self_referential(self):
        vfk = VirtualForeignKey(
            name="vfk_self",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            description="Self-referential",
        )

        assert vfk.is_self_referential


class TestSchemaGraphVirtualFKs:
    """Tests for SchemaGraph virtual FK support."""

    def test_add_virtual_fk(self, implicit_fk_schema):
        assert len(implicit_fk_schema.virtual_edges) == 1
        assert implicit_fk_schema.virtual_edges[0].name == "vfk_audit_users"

    def test_get_virtual_fks_all(self, django_generic_fk_schema):
        vfks = django_generic_fk_schema.get_virtual_fks()
        assert len(vfks) == 2
        assert any(vfk.name == "vfk_notifications_orders" for vfk in vfks)
        assert any(vfk.name == "vfk_notifications_users" for vfk in vfks)

    def test_get_virtual_fks_by_table(self, django_generic_fk_schema):
        # Get virtual FKs involving notifications table
        vfks = django_generic_fk_schema.get_virtual_fks("notifications")
        assert len(vfks) == 2

        # Get virtual FKs involving orders table
        vfks = django_generic_fk_schema.get_virtual_fks("orders")
        assert len(vfks) == 1
        assert vfks[0].target_table == "orders"

    def test_get_parents_includes_virtual(self, implicit_fk_schema):
        parents = implicit_fk_schema.get_parents("audit_log")
        assert len(parents) == 1
        parent_table, fk = parents[0]
        assert parent_table == "users"
        assert fk.name == "vfk_audit_users"

    def test_get_children_includes_virtual(self, implicit_fk_schema):
        children = implicit_fk_schema.get_children("users")
        assert len(children) == 1
        child_table, fk = children[0]
        assert child_table == "audit_log"
        assert fk.name == "vfk_audit_users"

    def test_is_virtual_fk(self, implicit_fk_schema):
        # Get the FK (converted from virtual)
        children = implicit_fk_schema.get_children("users")
        _, fk = children[0]

        # Check if it's a virtual FK
        assert implicit_fk_schema.is_virtual_fk(fk)

    def test_real_and_virtual_fks_combined(self, django_generic_fk_schema):
        # Notifications has both real FK (to content_type) and virtual FKs (to orders/users)
        parents = django_generic_fk_schema.get_parents("notifications")
        assert len(parents) == 3  # 1 real + 2 virtual

        parent_tables = {parent_table for parent_table, _ in parents}
        assert "django_content_type" in parent_tables  # Real FK
        assert "orders" in parent_tables  # Virtual FK
        assert "users" in parent_tables  # Virtual FK


class TestVirtualFKTraversal:
    """Tests for graph traversal with virtual FKs."""

    def test_traverse_up_with_virtual_fk(self, implicit_fk_schema, mock_adapter):
        """Traversing up from audit_log should find users via virtual FK."""
        # Mock data
        mock_adapter.schema = implicit_fk_schema
        mock_adapter.data = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
                {"id": 2, "email": "bob@example.com"},
            ],
            "audit_log": [
                {"id": 1, "user_id": 1, "action": "login", "timestamp": "2024-01-01"},
                {"id": 2, "user_id": 1, "action": "logout", "timestamp": "2024-01-02"},
                {"id": 3, "user_id": 2, "action": "login", "timestamp": "2024-01-03"},
            ],
        }

        traverser = GraphTraverser(implicit_fk_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=1,
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("audit_log", {(1,)}, config)

        # Should have audit_log and users
        assert "audit_log" in result.records
        assert "users" in result.records
        # Audit log 1 has user_id=1
        assert (1,) in result.records["users"]

    def test_traverse_down_with_virtual_fk(self, implicit_fk_schema, mock_adapter):
        """Traversing down from users should find audit_log via virtual FK."""
        # Mock data
        mock_adapter.schema = implicit_fk_schema
        mock_adapter.data = {
            "users": [
                {"id": 1, "email": "alice@example.com"},
            ],
            "audit_log": [
                {"id": 1, "user_id": 1, "action": "login", "timestamp": "2024-01-01"},
                {"id": 2, "user_id": 1, "action": "logout", "timestamp": "2024-01-02"},
            ],
        }

        traverser = GraphTraverser(implicit_fk_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=1,
            direction=TraversalDirection.DOWN,
        )

        result = traverser.traverse("users", {(1,)}, config)

        # Should have users and audit_log
        assert "users" in result.records
        assert "audit_log" in result.records
        # User 1 has two audit log entries
        assert (1,) in result.records["audit_log"]
        assert (2,) in result.records["audit_log"]

    def test_virtual_fk_marked_in_traversal_path(self, implicit_fk_schema, mock_adapter):
        """Virtual FKs should be marked as 'virtual' in traversal path."""
        # Mock data
        mock_adapter.schema = implicit_fk_schema
        mock_adapter.data = {
            "users": [{"id": 1, "email": "alice@example.com"}],
            "audit_log": [{"id": 1, "user_id": 1, "action": "login", "timestamp": "2024-01-01"}],
        }

        traverser = GraphTraverser(implicit_fk_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=1,
            direction=TraversalDirection.UP,
        )

        result = traverser.traverse("audit_log", {(1,)}, config)

        # Check traversal path contains "virtual"
        path_str = "\n".join(result.traversal_path)
        assert "virtual" in path_str
        assert "vfk_audit_users" in path_str

    def test_django_generic_fk_traversal(self, django_generic_fk_schema, mock_adapter):
        """Test traversal with Django GenericForeignKey pattern."""
        # Mock data
        mock_adapter.schema = django_generic_fk_schema
        mock_adapter.data = {
            "django_content_type": [
                {"id": 1, "app_label": "app", "model": "user"},
                {"id": 2, "app_label": "app", "model": "order"},
            ],
            "users": [
                {"id": 1, "email": "alice@example.com", "name": "Alice"},
            ],
            "orders": [
                {"id": 10, "user_id": 1, "total": 100.0},
            ],
            "notifications": [
                {
                    "id": 1,
                    "content_type_id": 2,
                    "object_id": 10,
                    "message": "Order created",
                },
                {
                    "id": 2,
                    "content_type_id": 1,
                    "object_id": 1,
                    "message": "User registered",
                },
            ],
        }

        traverser = GraphTraverser(django_generic_fk_schema, mock_adapter)
        config = TraversalConfig(
            max_depth=2,
            direction=TraversalDirection.UP,
        )

        # Start from notifications
        result = traverser.traverse("notifications", {(1,), (2,)}, config)

        # Should traverse through virtual FKs
        assert "notifications" in result.records
        assert "django_content_type" in result.records  # Real FK
        assert "orders" in result.records  # Virtual FK
        assert "users" in result.records  # Virtual FK + real FK from orders


class TestVirtualFKConfigFile:
    """Tests for virtual FK configuration file parsing."""

    def test_parse_virtual_fks_from_yaml(self, tmp_path):
        """Test parsing virtual FKs from YAML config."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
database:
  url: postgres://localhost/test

extraction:
  default_depth: 3

virtual_foreign_keys:
  - source_table: notifications
    source_columns:
      - object_id
    target_table: orders
    description: "Generic FK to orders via ContentType"
    name: vfk_notifications_orders

  - source_table: audit_log
    source_columns:
      - entity_id
    target_table: users
    target_columns:
      - id
    description: "Implicit FK without constraint"
    is_nullable: true
"""
        )

        config = DbsliceConfig.from_yaml(config_file)

        assert len(config.virtual_foreign_keys) == 2

        # Check first virtual FK
        vfk1 = config.virtual_foreign_keys[0]
        assert vfk1.source_table == "notifications"
        assert vfk1.source_columns == ["object_id"]
        assert vfk1.target_table == "orders"
        assert vfk1.description == "Generic FK to orders via ContentType"
        assert vfk1.name == "vfk_notifications_orders"

        # Check second virtual FK
        vfk2 = config.virtual_foreign_keys[1]
        assert vfk2.source_table == "audit_log"
        assert vfk2.source_columns == ["entity_id"]
        assert vfk2.target_table == "users"
        assert vfk2.target_columns == ["id"]
        assert vfk2.is_nullable is True

    def test_virtual_fk_config_validation(self, tmp_path):
        """Test validation of virtual FK configuration."""
        config_file = tmp_path / "config.yaml"

        # Missing required field
        config_file.write_text(
            """
virtual_foreign_keys:
  - source_table: notifications
    target_table: orders
    # Missing source_columns
"""
        )

        with pytest.raises(Exception) as exc_info:
            DbsliceConfig.from_yaml(config_file)

        assert "source_columns" in str(exc_info.value)

    def test_virtual_fk_yaml_export(self):
        """Test exporting virtual FKs to YAML."""
        vfk_config = VirtualForeignKeyConfig(
            source_table="notifications",
            source_columns=["object_id"],
            target_table="orders",
            description="Generic FK",
        )

        config = DbsliceConfig(virtual_foreign_keys=[vfk_config])
        yaml_str = config.to_yaml(include_comments=True)

        assert "virtual_foreign_keys:" in yaml_str
        assert "source_table: notifications" in yaml_str
        assert "source_columns:" in yaml_str
        assert "- object_id" in yaml_str
        assert "target_table: orders" in yaml_str
        assert 'description: "Generic FK"' in yaml_str

    def test_empty_virtual_fks(self):
        """Test config with no virtual FKs."""
        config = DbsliceConfig()
        assert len(config.virtual_foreign_keys) == 0

        yaml_str = config.to_yaml()
        # Should not include virtual_foreign_keys section if empty
        assert "virtual_foreign_keys:" not in yaml_str


class TestVirtualFKEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_virtual_fk_with_composite_keys(self):
        """Test virtual FKs with composite keys."""
        vfk = VirtualForeignKey(
            name="vfk_composite",
            source_table="order_items",
            source_columns=("order_id", "product_id"),
            target_table="composite_table",
            target_columns=("id1", "id2"),
            description="Composite key virtual FK",
        )

        assert len(vfk.source_columns) == 2
        assert len(vfk.target_columns) == 2

        fk = vfk.to_foreign_key()
        assert fk.source_columns == ("order_id", "product_id")
        assert fk.target_columns == ("id1", "id2")

    def test_virtual_fk_nullable_handling(self):
        """Test nullable virtual FK handling."""
        vfk_nullable = VirtualForeignKey(
            name="vfk_nullable",
            source_table="audit_log",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            description="Nullable FK",
            is_nullable=True,
        )

        vfk_not_null = VirtualForeignKey(
            name="vfk_not_null",
            source_table="orders",
            source_columns=("user_id",),
            target_table="users",
            target_columns=("id",),
            description="Not nullable FK",
            is_nullable=False,
        )

        assert vfk_nullable.is_nullable is True
        assert vfk_not_null.is_nullable is False

    def test_multiple_virtual_fks_same_tables(self, django_generic_fk_schema):
        """Test multiple virtual FKs between the same pair of tables."""
        # notifications -> orders and notifications -> users
        # Both use object_id column but point to different tables
        notifications_parents = django_generic_fk_schema.get_parents("notifications")

        # Should have 3 parents (content_type + orders + users)
        assert len(notifications_parents) == 3

        virtual_parents = [
            (table, fk)
            for table, fk in notifications_parents
            if django_generic_fk_schema.is_virtual_fk(fk)
        ]
        assert len(virtual_parents) == 2
