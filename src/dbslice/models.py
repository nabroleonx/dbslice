from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    """Represents a database column."""

    name: str
    data_type: str
    nullable: bool
    is_primary_key: bool
    default: str | None = None

    def __hash__(self) -> int:
        """Hash for use in sets and as dict keys."""
        return hash((self.name, self.data_type))


@dataclass(frozen=True)
class ForeignKey:
    """Represents a foreign key relationship."""

    name: str
    source_table: str
    source_columns: tuple[str, ...]
    target_table: str
    target_columns: tuple[str, ...]
    is_nullable: bool

    def __hash__(self) -> int:
        """Hash for use in sets and as dict keys."""
        return hash((self.name, self.source_table, self.target_table))

    def as_edge(self) -> tuple[str, str]:
        """Return as directed edge (child -> parent)."""
        return (self.source_table, self.target_table)

    @property
    def is_self_referential(self) -> bool:
        """Check if this FK references the same table."""
        return self.source_table == self.target_table


@dataclass(frozen=True)
class VirtualForeignKey:
    """
    Represents an implicit/virtual FK relationship not defined in the database schema.

    Virtual FKs are useful for:
    - Django GenericForeignKeys (ContentType framework)
    - Implicit relationships via JSON fields or application logic
    - Relationships across databases
    - Legacy schemas with missing FK constraints

    Unlike real FKs, virtual FKs:
    - Are not enforced by the database
    - Must be configured manually
    - May be conditional or complex
    - Are marked as "virtual" in traversal output
    """

    name: str
    source_table: str
    source_columns: tuple[str, ...]
    target_table: str
    target_columns: tuple[str, ...]
    description: str
    is_nullable: bool = True  # Virtual FKs are often nullable

    def __hash__(self) -> int:
        """Hash for use in sets and as dict keys."""
        return hash((self.name, self.source_table, self.target_table, "virtual"))

    def as_edge(self) -> tuple[str, str]:
        """Return as directed edge (child -> parent)."""
        return (self.source_table, self.target_table)

    @property
    def is_self_referential(self) -> bool:
        """Check if this FK references the same table."""
        return self.source_table == self.target_table

    def to_foreign_key(self) -> ForeignKey:
        """Convert to a regular ForeignKey for compatibility."""
        return ForeignKey(
            name=self.name,
            source_table=self.source_table,
            source_columns=self.source_columns,
            target_table=self.target_table,
            target_columns=self.target_columns,
            is_nullable=self.is_nullable,
        )


@dataclass
class Table:
    """Represents a database table."""

    name: str
    schema: str  # 'public' for postgres, database name for mysql
    columns: list[Column]
    primary_key: tuple[str, ...]
    foreign_keys: list[ForeignKey]

    def __hash__(self) -> int:
        """Hash for use in sets and as dict keys."""
        return hash((self.name, self.schema))

    def get_pk_columns(self) -> tuple[str, ...]:
        """Get primary key column names."""
        return self.primary_key

    def get_column(self, name: str) -> Column | None:
        """Get a column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_column_names(self) -> list[str]:
        """Get all column names."""
        return [col.name for col in self.columns]


@dataclass
class SchemaGraph:
    """Complete schema representation as a graph."""

    tables: dict[str, Table]
    edges: list[ForeignKey]  # All FK relationships
    virtual_edges: list["VirtualForeignKey"] = field(
        default_factory=list
    )  # Virtual FK relationships

    def get_table(self, name: str) -> Table | None:
        """Get a table by name."""
        return self.tables.get(name)

    def get_parents(self, table: str) -> list[tuple[str, ForeignKey]]:
        """
        Get tables this table depends on (FK targets).

        Includes both real and virtual FKs.

        Returns:
            List of (parent_table_name, foreign_key) tuples
        """
        parents = [(fk.target_table, fk) for fk in self.edges if fk.source_table == table]
        virtual_parents = [
            (vfk.target_table, vfk.to_foreign_key())
            for vfk in self.virtual_edges
            if vfk.source_table == table
        ]
        return parents + virtual_parents

    def get_children(self, table: str) -> list[tuple[str, ForeignKey]]:
        """
        Get tables that depend on this table (FK sources).

        Includes both real and virtual FKs.

        Returns:
            List of (child_table_name, foreign_key) tuples
        """
        children = [(fk.source_table, fk) for fk in self.edges if fk.target_table == table]
        virtual_children = [
            (vfk.source_table, vfk.to_foreign_key())
            for vfk in self.virtual_edges
            if vfk.target_table == table
        ]
        return children + virtual_children

    def get_virtual_fks(self, table: str | None = None) -> list["VirtualForeignKey"]:
        """
        Get virtual foreign keys.

        Args:
            table: If provided, only return virtual FKs involving this table

        Returns:
            List of VirtualForeignKey objects
        """
        if table is None:
            return self.virtual_edges
        return [
            vfk
            for vfk in self.virtual_edges
            if vfk.source_table == table or vfk.target_table == table
        ]

    def add_virtual_fk(self, vfk: "VirtualForeignKey") -> None:
        """
        Add a virtual foreign key to the schema graph.

        Args:
            vfk: VirtualForeignKey to add
        """
        self.virtual_edges.append(vfk)

    def is_virtual_fk(self, fk: ForeignKey) -> bool:
        """
        Check if a FK was originally a virtual FK.

        Args:
            fk: ForeignKey to check

        Returns:
            True if this FK originated from a virtual FK
        """
        return any(
            vfk.name == fk.name
            and vfk.source_table == fk.source_table
            and vfk.target_table == fk.target_table
            for vfk in self.virtual_edges
        )

    def get_table_names(self) -> list[str]:
        """Get all table names."""
        return list(self.tables.keys())

    def has_table(self, name: str) -> bool:
        """Check if a table exists."""
        return name in self.tables
