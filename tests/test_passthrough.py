"""Tests for passthrough tables feature."""

import pytest

from dbslice.core.graph import GraphTraverser, TraversalConfig
from dbslice.models import Column, ForeignKey, SchemaGraph, Table
from tests.conftest import MockAdapter


@pytest.fixture
def passthrough_schema() -> SchemaGraph:
    """
    Create a schema with regular tables and passthrough tables.

    Schema:
    - users (regular table)
    - orders (regular table, FK to users)
    - countries (passthrough - lookup table)
    - site_config (passthrough - config table)
    """
    users_table = Table(
        name="users",
        schema="main",
        columns=[
            Column(name="id", data_type="INTEGER", nullable=False, is_primary_key=True),
            Column(name="email", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="country_code", data_type="TEXT", nullable=True, is_primary_key=False),
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

    countries_table = Table(
        name="countries",
        schema="main",
        columns=[
            Column(name="code", data_type="TEXT", nullable=False, is_primary_key=True),
            Column(name="name", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=("code",),
        foreign_keys=[],
    )

    site_config_table = Table(
        name="site_config",
        schema="main",
        columns=[
            Column(name="key", data_type="TEXT", nullable=False, is_primary_key=True),
            Column(name="value", data_type="TEXT", nullable=True, is_primary_key=False),
        ],
        primary_key=("key",),
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

    return SchemaGraph(
        tables={
            "users": users_table,
            "orders": orders_table,
            "countries": countries_table,
            "site_config": site_config_table,
        },
        edges=[fk_orders_users],
    )


@pytest.fixture
def passthrough_adapter(passthrough_schema: SchemaGraph) -> MockAdapter:
    """Create a mock adapter with passthrough test data."""
    data = {
        "users": [
            {"id": 1, "email": "alice@example.com", "country_code": "US"},
            {"id": 2, "email": "bob@example.com", "country_code": "UK"},
        ],
        "orders": [
            {"id": 1, "user_id": 1, "total": 100.0},
            {"id": 2, "user_id": 2, "total": 200.0},
        ],
        "countries": [
            {"code": "US", "name": "United States"},
            {"code": "UK", "name": "United Kingdom"},
            {"code": "CA", "name": "Canada"},
            {"code": "FR", "name": "France"},
        ],
        "site_config": [
            {"key": "site_name", "value": "My Site"},
            {"key": "maintenance_mode", "value": "false"},
            {"key": "max_upload_size", "value": "10MB"},
        ],
    }
    return MockAdapter(passthrough_schema, data)


def test_passthrough_includes_all_rows(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test that passthrough tables include ALL rows."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    # Seed: extract just one user
    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries", "site_config"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Check that seed user is included
    assert "users" in result.records
    assert (1,) in result.records["users"]

    # Check that ALL countries are included (not just US)
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4
    assert ("US",) in result.records["countries"]
    assert ("UK",) in result.records["countries"]
    assert ("CA",) in result.records["countries"]
    assert ("FR",) in result.records["countries"]

    # Check that ALL site_config rows are included
    assert "site_config" in result.records
    assert len(result.records["site_config"]) == 3
    assert ("site_name",) in result.records["site_config"]
    assert ("maintenance_mode",) in result.records["site_config"]
    assert ("max_upload_size",) in result.records["site_config"]


def test_passthrough_works_with_fk_traversal(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test that passthrough tables work alongside normal FK traversal."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    # Seed: extract one order
    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries"},
    )

    result = traverser.traverse("orders", seed_pks, config)

    # Check FK traversal: order -> user
    assert "orders" in result.records
    assert (1,) in result.records["orders"]

    assert "users" in result.records
    assert (1,) in result.records["users"]  # User 1 referenced by order 1

    # Check passthrough: ALL countries, not just US (user 1's country)
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4


def test_passthrough_with_no_fk_traversal(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test passthrough tables with depth=0 (no FK traversal)."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=0,  # No FK traversal
        passthrough_tables={"countries", "site_config"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Only seed user
    assert "users" in result.records
    assert len(result.records["users"]) == 1

    # But ALL passthrough tables
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4

    assert "site_config" in result.records
    assert len(result.records["site_config"]) == 3


def test_multiple_passthrough_tables(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test multiple passthrough tables are all included."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries", "site_config"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Both passthrough tables should be fully included
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4

    assert "site_config" in result.records
    assert len(result.records["site_config"]) == 3


def test_passthrough_nonexistent_table(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test that nonexistent passthrough tables are skipped gracefully."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries", "nonexistent_table"},
    )

    # Should not raise an error
    result = traverser.traverse("users", seed_pks, config)

    # Valid passthrough table should be included
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4

    # Nonexistent table should not be in results
    assert "nonexistent_table" not in result.records


def test_passthrough_empty_table(passthrough_schema: SchemaGraph):
    """Test passthrough with an empty table."""
    # Create adapter with empty countries table
    data = {
        "users": [{"id": 1, "email": "alice@example.com", "country_code": "US"}],
        "orders": [],
        "countries": [],  # Empty
        "site_config": [{"key": "site_name", "value": "My Site"}],
    }
    adapter = MockAdapter(passthrough_schema, data)

    traverser = GraphTraverser(passthrough_schema, adapter)
    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries", "site_config"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Empty passthrough table should not appear in results
    assert "countries" not in result.records

    # Non-empty passthrough table should appear
    assert "site_config" in result.records
    assert len(result.records["site_config"]) == 1


def test_passthrough_traversal_path_documentation(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test that passthrough tables are documented in traversal path."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Check that traversal path includes passthrough entry
    passthrough_entries = [
        path for path in result.traversal_path if path.startswith("passthrough:")
    ]
    assert len(passthrough_entries) == 1
    assert "countries" in passthrough_entries[0]
    assert "4 rows total" in passthrough_entries[0]


def test_passthrough_dedupe_with_fk_traversal(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """
    Test that passthrough tables correctly deduplicate when some rows
    might have been included via FK traversal.
    """
    # Add a FK from users to countries (optional)
    fk_users_countries = ForeignKey(
        name="fk_users_countries",
        source_table="users",
        source_columns=("country_code",),
        target_table="countries",
        target_columns=("code",),
        is_nullable=True,
    )

    schema_with_fk = SchemaGraph(
        tables=passthrough_schema.tables,
        edges=passthrough_schema.edges + [fk_users_countries],
    )

    adapter = MockAdapter(schema_with_fk, passthrough_adapter.data)
    traverser = GraphTraverser(schema_with_fk, adapter)

    # Seed: extract one user (which references US)
    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"countries"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # All countries should be included (via passthrough)
    # Even though US was already included via FK traversal
    assert "countries" in result.records
    assert len(result.records["countries"]) == 4

    # Check traversal path shows both FK traversal and passthrough
    path_str = " ".join(result.traversal_path)
    assert "countries" in path_str
    # Should show passthrough even though some rows were from FK
    assert "passthrough:" in path_str


def test_passthrough_with_exclude_tables(
    passthrough_adapter: MockAdapter,
    passthrough_schema: SchemaGraph,
):
    """Test that excluded tables are not included even if marked as passthrough."""
    traverser = GraphTraverser(passthrough_schema, passthrough_adapter)

    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        exclude_tables={"countries"},  # Exclude countries
        passthrough_tables={"countries", "site_config"},  # But also mark as passthrough
    )

    result = traverser.traverse("users", seed_pks, config)

    # Countries should NOT be included (excluded takes precedence)
    assert "countries" not in result.records

    # site_config should be included (not excluded)
    assert "site_config" in result.records
    assert len(result.records["site_config"]) == 3


def test_passthrough_table_without_pk(passthrough_schema: SchemaGraph):
    """Test that passthrough tables without primary keys are skipped."""
    # Add a table without a primary key
    no_pk_table = Table(
        name="logs",
        schema="main",
        columns=[
            Column(name="timestamp", data_type="TEXT", nullable=False, is_primary_key=False),
            Column(name="message", data_type="TEXT", nullable=False, is_primary_key=False),
        ],
        primary_key=(),  # No PK
        foreign_keys=[],
    )

    schema_with_no_pk = SchemaGraph(
        tables={**passthrough_schema.tables, "logs": no_pk_table},
        edges=passthrough_schema.edges,
    )

    data = {
        "users": [{"id": 1, "email": "alice@example.com", "country_code": "US"}],
        "orders": [],
        "countries": [{"code": "US", "name": "United States"}],
        "site_config": [],
        "logs": [{"timestamp": "2023-01-01", "message": "Test"}],
    }
    adapter = MockAdapter(schema_with_no_pk, data)

    traverser = GraphTraverser(schema_with_no_pk, adapter)
    seed_pks = {(1,)}

    config = TraversalConfig(
        max_depth=3,
        passthrough_tables={"logs", "countries"},
    )

    result = traverser.traverse("users", seed_pks, config)

    # Table without PK should not be included
    assert "logs" not in result.records

    # Table with PK should be included
    assert "countries" in result.records
