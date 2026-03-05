"""Tests for inspect helper heuristics."""

from dbslice.cli import _detect_potential_implicit_fks
from dbslice.models import Column, ForeignKey, SchemaGraph, Table


def _table(name: str, columns: list[Column], pk: tuple[str, ...] = ("id",)) -> Table:
    return Table(
        name=name,
        schema="public",
        columns=columns,
        primary_key=pk,
        foreign_keys=[],
    )


def test_detect_potential_implicit_fks_suggests_missing_user_id_relationship():
    users = _table(
        "users",
        [Column(name="id", data_type="integer", nullable=False, is_primary_key=True)],
    )
    audit_log = _table(
        "audit_log",
        [
            Column(name="id", data_type="integer", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="integer", nullable=False, is_primary_key=False),
        ],
    )
    schema = SchemaGraph(tables={"users": users, "audit_log": audit_log}, edges=[])

    candidates = _detect_potential_implicit_fks(schema)

    assert ("audit_log", "user_id", "users") in candidates


def test_detect_potential_implicit_fks_skips_columns_with_real_fk():
    users = _table(
        "users",
        [Column(name="id", data_type="integer", nullable=False, is_primary_key=True)],
    )
    orders = _table(
        "orders",
        [
            Column(name="id", data_type="integer", nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="integer", nullable=False, is_primary_key=False),
        ],
    )
    fk = ForeignKey(
        name="fk_orders_users",
        source_table="orders",
        source_columns=("user_id",),
        target_table="users",
        target_columns=("id",),
        is_nullable=False,
    )
    orders.foreign_keys.append(fk)
    schema = SchemaGraph(tables={"users": users, "orders": orders}, edges=[fk])

    candidates = _detect_potential_implicit_fks(schema)

    assert ("orders", "user_id", "users") not in candidates
