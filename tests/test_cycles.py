"""Tests for cycle detection and breaking algorithms."""

import pytest

from dbslice.core.cycles import (
    CycleInfo,
    DeferredUpdate,
    break_cycles_at_nullable_fks,
    build_deferred_updates,
    find_cycles_dfs,
    identify_cycle_fks,
    select_nullable_fk_to_break,
)
from dbslice.models import Column, ForeignKey, SchemaGraph, Table


class TestFindCyclesDFS:
    """Tests for DFS-based cycle detection."""

    def test_no_cycles(self):
        """Test graph with no cycles."""
        dependencies = {
            "orders": {"users"},
            "order_items": {"orders"},
            "users": set(),
        }
        cycles = find_cycles_dfs(dependencies)
        assert cycles == []

    def test_self_referential_cycle(self):
        """Test self-referential table (most common cycle)."""
        dependencies = {
            "employees": {"employees"},
        }
        cycles = find_cycles_dfs(dependencies)
        assert len(cycles) == 1
        assert "employees" in cycles[0]

    def test_two_table_cycle(self):
        """Test cycle between two tables."""
        dependencies = {
            "orders": {"shipments"},
            "shipments": {"orders"},
        }
        cycles = find_cycles_dfs(dependencies)
        assert len(cycles) >= 1
        # Should find a cycle involving both tables
        cycle = cycles[0]
        assert "orders" in cycle or "shipments" in cycle

    def test_three_table_cycle(self):
        """Test cycle through three tables."""
        dependencies = {
            "a": {"b"},
            "b": {"c"},
            "c": {"a"},
        }
        cycles = find_cycles_dfs(dependencies)
        assert len(cycles) >= 1


class TestIdentifyCycleFKs:
    """Tests for identifying FKs in a cycle."""

    def test_single_fk_in_cycle(self):
        """Test cycle with one FK."""
        fks = [
            ForeignKey(
                name="fk1",
                source_table="employees",
                source_columns=("manager_id",),
                target_table="employees",
                target_columns=("id",),
                is_nullable=True,
            ),
        ]
        schema = SchemaGraph(tables={}, edges=fks)
        cycle = ["employees"]

        cycle_fks = identify_cycle_fks(schema, cycle)
        assert len(cycle_fks) == 1
        assert cycle_fks[0].name == "fk1"

    def test_multiple_fks_in_cycle(self):
        """Test cycle with multiple FKs."""
        fks = [
            ForeignKey(
                name="fk1",
                source_table="orders",
                source_columns=("shipment_id",),
                target_table="shipments",
                target_columns=("id",),
                is_nullable=True,
            ),
            ForeignKey(
                name="fk2",
                source_table="shipments",
                source_columns=("order_id",),
                target_table="orders",
                target_columns=("id",),
                is_nullable=False,
            ),
        ]
        schema = SchemaGraph(tables={}, edges=fks)
        cycle = ["orders", "shipments"]

        cycle_fks = identify_cycle_fks(schema, cycle)
        assert len(cycle_fks) == 2


class TestSelectNullableFKToBreak:
    """Tests for selecting the best nullable FK to break."""

    def test_prefer_self_referential_for_self_loop(self):
        """Should prefer self-referential FKs for single-table (self-loop) cycles."""
        fks = [
            ForeignKey(
                name="fk_manager",
                source_table="employees",
                source_columns=("manager_id",),
                target_table="employees",
                target_columns=("id",),
                is_nullable=True,
            ),
            ForeignKey(
                name="fk_dept",
                source_table="employees",
                source_columns=("dept_id",),
                target_table="departments",
                target_columns=("id",),
                is_nullable=True,
            ),
        ]

        # For a self-loop cycle [employees], self-referential FKs are preferred
        selected = select_nullable_fk_to_break(fks, cycle=["employees"])
        assert selected is not None
        assert selected.name == "fk_manager"
        assert selected.is_self_referential

    def test_prefer_inter_table_for_multi_table_cycle(self):
        """Should prefer inter-table FKs for multi-table cycles.

        Self-referential FKs don't break inter-table cycles, so they
        should not be selected when the cycle involves multiple tables.
        """
        fks = [
            ForeignKey(
                name="fk_manager",
                source_table="employees",
                source_columns=("manager_id",),
                target_table="employees",
                target_columns=("id",),
                is_nullable=True,
            ),
            ForeignKey(
                name="fk_dept",
                source_table="employees",
                source_columns=("dept_id",),
                target_table="departments",
                target_columns=("id",),
                is_nullable=True,
            ),
        ]

        # For a multi-table cycle [departments, employees], inter-table FKs are preferred
        selected = select_nullable_fk_to_break(fks, cycle=["departments", "employees"])
        assert selected is not None
        assert selected.name == "fk_dept"
        assert not selected.is_self_referential

    def test_prefer_single_column(self):
        """Should prefer single-column FKs over composite."""
        fks = [
            ForeignKey(
                name="fk_single",
                source_table="orders",
                source_columns=("user_id",),
                target_table="users",
                target_columns=("id",),
                is_nullable=True,
            ),
            ForeignKey(
                name="fk_composite",
                source_table="orders",
                source_columns=("tenant_id", "customer_id"),
                target_table="customers",
                target_columns=("tenant_id", "id"),
                is_nullable=True,
            ),
        ]

        selected = select_nullable_fk_to_break(fks)
        assert selected.name == "fk_single"
        assert len(selected.source_columns) == 1

    def test_no_nullable_fk(self):
        """Should return None if no nullable FKs exist."""
        fks = [
            ForeignKey(
                name="fk1",
                source_table="orders",
                source_columns=("user_id",),
                target_table="users",
                target_columns=("id",),
                is_nullable=False,
            ),
        ]

        selected = select_nullable_fk_to_break(fks)
        assert selected is None

    def test_empty_list(self):
        """Should return None for empty FK list."""
        selected = select_nullable_fk_to_break([])
        assert selected is None


class TestBreakCyclesAtNullableFKs:
    """Tests for the main cycle-breaking algorithm."""

    def test_break_simple_self_reference(self):
        """Test breaking a simple self-referential cycle."""
        # Create schema with self-referential employees table
        employees_table = Table(
            name="employees",
            schema="public",
            columns=[
                Column(name="id", data_type="int", nullable=False, is_primary_key=True),
                Column(name="manager_id", data_type="int", nullable=True, is_primary_key=False),
            ],
            primary_key=("id",),
            foreign_keys=[],
        )

        fk = ForeignKey(
            name="fk_manager",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            is_nullable=True,
        )

        schema = SchemaGraph(
            tables={"employees": employees_table},
            edges=[fk],
        )

        dependencies = {"employees": {"employees"}}
        tables = {"employees"}

        fks_to_break, cycle_infos = break_cycles_at_nullable_fks(schema, tables, dependencies)

        assert len(fks_to_break) == 1
        assert fks_to_break[0].name == "fk_manager"
        assert len(cycle_infos) == 1
        assert "employees" in cycle_infos[0].tables

    def test_no_nullable_fk_raises_error(self):
        """Test that ValueError is raised when no nullable FK exists."""
        # Create cycle with only non-nullable FK
        fk = ForeignKey(
            name="fk_required",
            source_table="orders",
            source_columns=("shipment_id",),
            target_table="shipments",
            target_columns=("id",),
            is_nullable=False,
        )

        schema = SchemaGraph(tables={}, edges=[fk])
        dependencies = {"orders": {"shipments"}, "shipments": {"orders"}}
        tables = {"orders", "shipments"}

        with pytest.raises(ValueError, match="no nullable foreign key"):
            break_cycles_at_nullable_fks(schema, tables, dependencies)

    def test_no_cycles_returns_empty(self):
        """Test that no cycles returns empty lists."""
        schema = SchemaGraph(tables={}, edges=[])
        dependencies = {"orders": {"users"}, "users": set()}
        tables = {"orders", "users"}

        fks_to_break, cycle_infos = break_cycles_at_nullable_fks(schema, tables, dependencies)

        assert fks_to_break == []
        assert cycle_infos == []


class TestBuildDeferredUpdates:
    """Tests for building deferred UPDATE statements."""

    def test_build_single_update(self):
        """Test building UPDATE for single FK."""
        fk = ForeignKey(
            name="fk_manager",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            is_nullable=True,
        )

        employees_table = Table(
            name="employees",
            schema="public",
            columns=[],
            primary_key=("id",),
            foreign_keys=[],
        )

        schema = SchemaGraph(tables={"employees": employees_table}, edges=[fk])

        tables_data = {
            "employees": [
                {"id": 1, "name": "Alice", "manager_id": 2},
                {"id": 2, "name": "Bob", "manager_id": 1},
            ]
        }

        updates = build_deferred_updates([fk], tables_data, schema)

        assert len(updates) == 2
        assert updates[0].table == "employees"
        assert updates[0].fk_column == "manager_id"
        assert updates[0].fk_value == 2
        assert updates[1].fk_value == 1

    def test_skip_null_fk_values(self):
        """Test that NULL FK values don't generate UPDATEs."""
        fk = ForeignKey(
            name="fk_manager",
            source_table="employees",
            source_columns=("manager_id",),
            target_table="employees",
            target_columns=("id",),
            is_nullable=True,
        )

        employees_table = Table(
            name="employees",
            schema="public",
            columns=[],
            primary_key=("id",),
            foreign_keys=[],
        )

        schema = SchemaGraph(tables={"employees": employees_table}, edges=[fk])

        tables_data = {
            "employees": [
                {"id": 1, "name": "Alice", "manager_id": None},  # NULL - no UPDATE
                {"id": 2, "name": "Bob", "manager_id": 1},  # Has value - UPDATE
            ]
        }

        updates = build_deferred_updates([fk], tables_data, schema)

        assert len(updates) == 1
        assert updates[0].pk_values == (2,)
        assert updates[0].fk_value == 1

    def test_composite_pk(self):
        """Test building UPDATE for table with composite PK."""
        fk = ForeignKey(
            name="fk_parent",
            source_table="items",
            source_columns=("parent_id",),
            target_table="items",
            target_columns=("id",),
            is_nullable=True,
        )

        items_table = Table(
            name="items",
            schema="public",
            columns=[],
            primary_key=("tenant_id", "id"),
            foreign_keys=[],
        )

        schema = SchemaGraph(tables={"items": items_table}, edges=[fk])

        tables_data = {
            "items": [
                {"tenant_id": 1, "id": 100, "parent_id": 200},
            ]
        }

        updates = build_deferred_updates([fk], tables_data, schema)

        assert len(updates) == 1
        assert updates[0].pk_columns == ("tenant_id", "id")
        assert updates[0].pk_values == (1, 100)


class TestDeferredUpdate:
    """Tests for DeferredUpdate dataclass."""

    def test_format_where_clause_simple(self):
        """Test WHERE clause formatting for single PK."""
        update = DeferredUpdate(
            table="employees",
            pk_columns=("id",),
            pk_values=(1,),
            fk_column="manager_id",
            fk_value=2,
        )

        where_clause = update.format_where_clause()
        assert where_clause == "id = 1"

    def test_format_where_clause_composite(self):
        """Test WHERE clause formatting for composite PK."""
        update = DeferredUpdate(
            table="items",
            pk_columns=("tenant_id", "id"),
            pk_values=(1, 100),
            fk_column="parent_id",
            fk_value=200,
        )

        where_clause = update.format_where_clause()
        assert "tenant_id = 1" in where_clause
        assert "id = 100" in where_clause
        assert " AND " in where_clause


class TestCycleInfo:
    """Tests for CycleInfo dataclass."""

    def test_str_representation(self):
        """Test string representation shows cycle path."""
        cycle_info = CycleInfo(
            tables=["a", "b", "c"],
            fks_in_cycle=[],
        )

        cycle_str = str(cycle_info)
        assert "a" in cycle_str
        assert "b" in cycle_str
        assert "c" in cycle_str
        assert "→" in cycle_str
