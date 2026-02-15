"""Shared fixtures for integration tests with PostgreSQL."""

import os
from collections.abc import Iterator
from typing import Any

import psycopg2
import psycopg2.extras
import pytest

from dbslice.config import ExtractConfig, TraversalDirection


def get_test_db_url() -> str | None:
    """
    Get test database URL from environment variable.

    Returns:
        Database URL if DBSLICE_TEST_DB is set, None otherwise
    """
    return os.environ.get("DBSLICE_TEST_DB")


def is_postgres_available() -> bool:
    """Check if PostgreSQL test database is available."""
    url = get_test_db_url()
    if not url:
        return False

    try:
        conn = psycopg2.connect(url)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_available() -> bool:
    """Check if PostgreSQL is available for testing."""
    return is_postgres_available()


@pytest.fixture(scope="session")
def test_db_url(postgres_available: bool) -> str:
    """
    Get test database URL or skip tests if not available.

    To run integration tests, set DBSLICE_TEST_DB environment variable:
        export DBSLICE_TEST_DB="postgresql://user:pass@localhost:5432/test_db"
        pytest tests/integration/
    """
    if not postgres_available:
        pytest.skip(
            "PostgreSQL test database not available. "
            "Set DBSLICE_TEST_DB environment variable to run integration tests."
        )
    return get_test_db_url()


@pytest.fixture
def pg_connection(test_db_url: str) -> Iterator[psycopg2.extensions.connection]:
    """Create a PostgreSQL connection for testing."""
    conn = psycopg2.connect(test_db_url)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def clean_database(pg_connection: psycopg2.extensions.connection) -> Iterator[None]:
    """
    Clean up all tables before and after each test.

    This ensures each test starts with a clean slate.
    """

    def cleanup():
        # Reset connection state in case a test left it in a failed transaction
        if pg_connection.info.transaction_status != 0:  # IDLE = 0
            pg_connection.rollback()
        with pg_connection.cursor() as cur:
            # Drop all tables in public schema
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]

            if tables:
                for table in tables:
                    cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')

    cleanup()
    yield
    cleanup()


@pytest.fixture
def ecommerce_schema(
    pg_connection: psycopg2.extensions.connection, clean_database: None
) -> dict[str, Any]:
    """
    Create e-commerce schema for testing.

    Schema:
        users (id, email, name, address, phone)
        products (id, sku, name, price, description)
        orders (id, user_id, total, status, created_at)
        order_items (id, order_id, product_id, quantity, price)
        reviews (id, product_id, user_id, rating, comment)

    Returns:
        Dict with inserted data for verification
    """
    with pg_connection.cursor() as cur:
        # Create tables
        cur.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                name VARCHAR(255),
                address TEXT,
                phone VARCHAR(50)
            )
        """)

        cur.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                sku VARCHAR(100) NOT NULL UNIQUE,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10, 2),
                description TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                total DECIMAL(10, 2),
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER NOT NULL REFERENCES orders(id),
                product_id INTEGER NOT NULL REFERENCES products(id),
                quantity INTEGER NOT NULL,
                price DECIMAL(10, 2)
            )
        """)

        cur.execute("""
            CREATE TABLE reviews (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES products(id),
                user_id INTEGER NOT NULL REFERENCES users(id),
                rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert test data
        # Users
        cur.execute("""
            INSERT INTO users (id, email, name, address, phone) VALUES
            (1, 'alice@example.com', 'Alice Smith', '123 Main St', '555-0001'),
            (2, 'bob@example.com', 'Bob Jones', '456 Oak Ave', '555-0002'),
            (3, 'charlie@example.com', 'Charlie Brown', '789 Elm St', '555-0003'),
            (4, 'diana@example.com', 'Diana Prince', '321 Pine Rd', '555-0004')
        """)

        # Products
        cur.execute("""
            INSERT INTO products (id, sku, name, price, description) VALUES
            (1, 'WIDGET-001', 'Widget', 19.99, 'A useful widget'),
            (2, 'GADGET-001', 'Gadget', 49.99, 'An amazing gadget'),
            (3, 'GIZMO-001', 'Gizmo', 29.99, 'A cool gizmo'),
            (4, 'DOOHICKEY-001', 'Doohickey', 9.99, 'A handy doohickey')
        """)

        # Orders
        cur.execute("""
            INSERT INTO orders (id, user_id, total, status, created_at) VALUES
            (1, 1, 69.98, 'completed', '2024-01-01 10:00:00'),
            (2, 1, 49.99, 'pending', '2024-01-02 11:00:00'),
            (3, 2, 19.99, 'completed', '2024-01-03 12:00:00'),
            (4, 3, 39.98, 'cancelled', '2024-01-04 13:00:00')
        """)

        # Order items
        cur.execute("""
            INSERT INTO order_items (id, order_id, product_id, quantity, price) VALUES
            (1, 1, 1, 2, 19.99),
            (2, 1, 2, 1, 29.99),
            (3, 2, 2, 1, 49.99),
            (4, 3, 1, 1, 19.99),
            (5, 4, 3, 1, 29.99),
            (6, 4, 4, 1, 9.99)
        """)

        # Reviews
        cur.execute("""
            INSERT INTO reviews (id, product_id, user_id, rating, comment, created_at) VALUES
            (1, 1, 1, 5, 'Great product!', '2024-01-05 10:00:00'),
            (2, 2, 1, 4, 'Pretty good', '2024-01-05 11:00:00'),
            (3, 1, 2, 5, 'Love it!', '2024-01-05 12:00:00'),
            (4, 3, 3, 3, 'It is okay', '2024-01-05 13:00:00')
        """)

    return {
        "users": [1, 2, 3, 4],
        "products": [1, 2, 3, 4],
        "orders": [1, 2, 3, 4],
        "order_items": [1, 2, 3, 4, 5, 6],
        "reviews": [1, 2, 3, 4],
    }


@pytest.fixture
def circular_ref_schema(
    pg_connection: psycopg2.extensions.connection, clean_database: None
) -> dict[str, Any]:
    """
    Create schema with circular references for cycle detection testing.

    Schema:
        departments (id, name, manager_id -> employees.id)
        employees (id, name, department_id -> departments.id, manager_id -> employees.id)
        projects (id, name, lead_employee_id -> employees.id)
        project_assignments (id, project_id, employee_id)

    This schema has multiple cycles:
    1. departments <-> employees (bidirectional FK)
    2. employees -> employees (self-reference)

    Returns:
        Dict with inserted data for verification
    """
    with pg_connection.cursor() as cur:
        # Create tables with nullable FKs to allow cycle resolution
        cur.execute("""
            CREATE TABLE departments (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                manager_id INTEGER  -- Nullable FK, will reference employees.id
            )
        """)

        cur.execute("""
            CREATE TABLE employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                department_id INTEGER,  -- Nullable FK
                manager_id INTEGER  -- Self-reference, nullable
            )
        """)

        cur.execute("""
            CREATE TABLE projects (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                lead_employee_id INTEGER  -- Nullable FK
            )
        """)

        cur.execute("""
            CREATE TABLE project_assignments (
                id SERIAL PRIMARY KEY,
                project_id INTEGER NOT NULL,
                employee_id INTEGER NOT NULL
            )
        """)

        # Add foreign keys after table creation
        cur.execute("""
            ALTER TABLE departments
            ADD CONSTRAINT fk_dept_manager
            FOREIGN KEY (manager_id) REFERENCES employees(id)
        """)

        cur.execute("""
            ALTER TABLE employees
            ADD CONSTRAINT fk_emp_dept
            FOREIGN KEY (department_id) REFERENCES departments(id)
        """)

        cur.execute("""
            ALTER TABLE employees
            ADD CONSTRAINT fk_emp_manager
            FOREIGN KEY (manager_id) REFERENCES employees(id)
        """)

        cur.execute("""
            ALTER TABLE projects
            ADD CONSTRAINT fk_proj_lead
            FOREIGN KEY (lead_employee_id) REFERENCES employees(id)
        """)

        cur.execute("""
            ALTER TABLE project_assignments
            ADD CONSTRAINT fk_pa_project
            FOREIGN KEY (project_id) REFERENCES projects(id)
        """)

        cur.execute("""
            ALTER TABLE project_assignments
            ADD CONSTRAINT fk_pa_employee
            FOREIGN KEY (employee_id) REFERENCES employees(id)
        """)

        # Insert test data (carefully to avoid FK violations)
        # First insert employees and departments without FKs
        cur.execute("""
            INSERT INTO departments (id, name, manager_id) VALUES
            (1, 'Engineering', NULL),
            (2, 'Sales', NULL)
        """)

        cur.execute("""
            INSERT INTO employees (id, name, department_id, manager_id) VALUES
            (1, 'Alice Manager', 1, NULL),
            (2, 'Bob Developer', 1, 1),
            (3, 'Charlie Sales Lead', 2, NULL),
            (4, 'Diana Sales Rep', 2, 3)
        """)

        # Now update departments with manager FKs (creates cycle)
        cur.execute("""
            UPDATE departments SET manager_id = 1 WHERE id = 1
        """)
        cur.execute("""
            UPDATE departments SET manager_id = 3 WHERE id = 2
        """)

        # Insert projects
        cur.execute("""
            INSERT INTO projects (id, name, lead_employee_id) VALUES
            (1, 'Project Alpha', 1),
            (2, 'Project Beta', 2)
        """)

        # Insert project assignments
        cur.execute("""
            INSERT INTO project_assignments (id, project_id, employee_id) VALUES
            (1, 1, 1),
            (2, 1, 2),
            (3, 2, 2)
        """)

    return {
        "departments": [1, 2],
        "employees": [1, 2, 3, 4],
        "projects": [1, 2],
        "project_assignments": [1, 2, 3],
    }


def execute_sql_file(pg_connection: psycopg2.extensions.connection, sql_content: str) -> None:
    """
    Execute SQL statements from a string.

    This helper is used to reimport extracted SQL for validation.

    Args:
        pg_connection: Database connection
        sql_content: SQL statements to execute
    """
    with pg_connection.cursor() as cur:
        # Split on semicolons and execute each statement
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]
        for statement in statements:
            if statement:
                cur.execute(statement)


def count_rows(pg_connection: psycopg2.extensions.connection, table: str) -> int:
    """Count rows in a table."""
    with pg_connection.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]


def fetch_all_rows(
    pg_connection: psycopg2.extensions.connection, table: str
) -> list[dict[str, Any]]:
    """Fetch all rows from a table as dictionaries."""
    with pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f'SELECT * FROM "{table}"')
        return [dict(row) for row in cur.fetchall()]


def table_exists(pg_connection: psycopg2.extensions.connection, table: str) -> bool:
    """Check if a table exists in the database."""
    with pg_connection.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """,
            (table,),
        )
        return cur.fetchone()[0]


@pytest.fixture
def extract_config_factory(test_db_url: str):
    """
    Factory fixture for creating ExtractConfig objects with test database URL.

    Usage:
        config = extract_config_factory(
            seeds=[SeedSpec.parse("orders.id=1")],
            depth=5
        )
    """

    def factory(**kwargs) -> ExtractConfig:
        defaults = {
            "database_url": test_db_url,
            "seeds": [],
            "depth": 5,
            "direction": TraversalDirection.BOTH,
            "validate": True,
        }
        defaults.update(kwargs)
        return ExtractConfig(**defaults)

    return factory
