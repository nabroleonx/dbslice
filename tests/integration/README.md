# Integration Tests

Comprehensive integration tests for dbslice that verify the complete extraction workflow with a real PostgreSQL database.

## Prerequisites

### PostgreSQL Database

Integration tests require a PostgreSQL database. You can set one up using:

#### Option 1: Docker (Recommended)

```bash
docker run --name dbslice-test-postgres \
  -e POSTGRES_USER=dbslice_test \
  -e POSTGRES_PASSWORD=test_password \
  -e POSTGRES_DB=dbslice_test \
  -p 5432:5432 \
  -d postgres:15
```

#### Option 2: Local PostgreSQL

Create a test database in your local PostgreSQL instance:

```sql
CREATE DATABASE dbslice_test;
CREATE USER dbslice_test WITH PASSWORD 'test_password';
GRANT ALL PRIVILEGES ON DATABASE dbslice_test TO dbslice_test;
```

## Running Tests

### Set Environment Variable

Export the database URL before running tests:

```bash
export DBSLICE_TEST_DB="postgresql://dbslice_test:test_password@localhost:5432/dbslice_test"
```

### Run All Integration Tests

```bash
pytest tests/integration/ -v
```

### Run Specific Test Suites

```bash
# Full extraction workflow tests
pytest tests/integration/test_full_extraction.py -v

# SQL reimport and referential integrity tests
pytest tests/integration/test_sql_reimport.py -v

# Performance and benchmarking tests
pytest tests/integration/test_performance.py -v

# CLI integration tests
pytest tests/integration/test_cli_integration.py -v
```

### Run with Coverage

```bash
pytest tests/integration/ --cov=dbslice --cov-report=html --cov-report=term
```

## Test Organization

### `test_full_extraction.py`

Tests the complete extraction workflow with various scenarios:

- **Basic extraction**: Single seed with FK traversal
- **WHERE clause seeds**: Filtering with SQL conditions
- **Multiple seeds**: Extracting from multiple starting points
- **Cycle handling**: Circular foreign key references
- **Anonymization**: End-to-end data anonymization
- **Validation**: Referential integrity checking
- **Depth limiting**: FK traversal depth control
- **Table exclusion**: Excluding tables from extraction

### `test_sql_reimport.py`

Tests that generated SQL can be successfully re-imported:

- **Basic reimport**: Verify SQL can be executed
- **Data preservation**: Verify data matches after reimport
- **Referential integrity**: All FKs remain valid
- **Insert order**: Dependencies are respected
- **Cycle resolution**: Broken FKs and deferred updates work
- **Anonymized data**: Anonymized extractions reimport correctly

### `test_performance.py`

Tests performance characteristics with realistic datasets:

- **Large datasets**: Extraction with thousands of records
- **Streaming mode**: Memory-efficient streaming activation
- **Query batching**: Batching effectiveness with many FKs
- **Benchmarks**: Speed measurements and profiling

### `test_cli_integration.py`

Tests CLI functionality end-to-end with subprocess:

- **Basic commands**: Extract to stdout and files
- **All flags**: Direction, depth, seeds, output formats
- **Anonymization**: --anonymize and --redact flags
- **Validation**: --validate and --no-validate
- **Profiling**: --profile flag
- **Streaming**: --stream and --stream-threshold
- **Error handling**: Invalid inputs and exit codes
- **Inspect command**: Schema inspection
- **Help/version**: Documentation flags

## Test Fixtures

### Database Fixtures

- `pg_connection`: PostgreSQL connection for tests
- `clean_database`: Cleans up tables before/after each test
- `ecommerce_schema`: Realistic e-commerce schema with test data
- `circular_ref_schema`: Schema with circular references for cycle testing

### Schema Details

#### E-commerce Schema

```
users (id, email, name, address, phone)
  └── orders (id, user_id, total, status)
        ├── order_items (id, order_id, product_id, quantity, price)
        │     └── products (id, sku, name, price, description)
        └── reviews (id, product_id, user_id, rating, comment)
```

Sample data includes:
- 4 users
- 4 orders (various statuses)
- 6 order items
- 4 products
- 4 reviews

#### Circular Reference Schema

```
departments (id, name, manager_id -> employees)
     ↓ (bidirectional)
employees (id, name, department_id, manager_id -> employees)
     └── projects (id, name, lead_employee_id)
           └── project_assignments (id, project_id, employee_id)
```

This schema tests cycle detection and resolution.

### Helper Functions

- `execute_sql_file()`: Execute SQL statements for reimport testing
- `count_rows()`: Count rows in a table
- `fetch_all_rows()`: Fetch all rows as dictionaries
- `table_exists()`: Check if a table exists

## Skipping Tests

Tests automatically skip if PostgreSQL is not available:

```
SKIPPED [1] tests/integration/conftest.py:45: PostgreSQL test database not available.
Set DBSLICE_TEST_DB environment variable to run integration tests.
```

## CI/CD Integration

Integration tests run automatically in GitHub Actions on push/PR. See `.github/workflows/integration-tests.yml`.

The workflow:
1. Starts PostgreSQL service container
2. Runs tests across Python 3.10, 3.11, 3.12
3. Generates coverage reports
4. Runs performance benchmarks
5. Tests CLI functionality

## Debugging Tests

### Verbose Output

```bash
pytest tests/integration/ -v --tb=long
```

### Run Single Test

```bash
pytest tests/integration/test_full_extraction.py::TestBasicExtraction::test_extract_single_order_with_parents -v
```

### Keep Test Database

To inspect the database after test failures, modify the `clean_database` fixture to skip cleanup.

### SQL Logging

Set verbose logging to see SQL queries:

```bash
pytest tests/integration/ -v --log-cli-level=DEBUG
```

## Performance Benchmarking

Performance tests include benchmark fixtures that print metrics:

```bash
pytest tests/integration/test_performance.py::TestBenchmarks -v -s
```

Example output:
```
Single user extraction:
  Time: 0.35s
  Rows extracted: 25
  Rows/second: 71
```

## Writing New Integration Tests

### Basic Test Structure

```python
def test_my_feature(
    ecommerce_schema: dict,
    extract_config_factory,
    pg_connection
):
    """Test description."""
    # Create config
    config = extract_config_factory(
        seeds=[SeedSpec.parse("orders.id=1")],
        depth=10,
    )

    # Run extraction
    engine = ExtractionEngine(config)
    result, schema = engine.extract()

    # Verify results
    assert result.total_rows() > 0
    assert "orders" in result.tables
```

### Testing SQL Reimport

```python
def test_reimport(
    ecommerce_schema: dict,
    extract_config_factory,
    pg_connection,
    clean_database
):
    """Test SQL reimport."""
    # Extract
    config = extract_config_factory(seeds=[SeedSpec.parse("orders.id=1")])
    engine = ExtractionEngine(config)
    result, schema = engine.extract()

    # Generate SQL
    generator = SQLGenerator(db_type=DatabaseType.POSTGRESQL)
    sql = generator.generate(result.tables, result.insert_order, schema.tables)

    # Clear and reimport
    execute_sql_file(pg_connection, "TRUNCATE TABLE orders CASCADE")
    execute_sql_file(pg_connection, sql)

    # Verify
    assert count_rows(pg_connection, "orders") == 1
```

## Troubleshooting

### Connection Refused

```
psycopg2.OperationalError: could not connect to server: Connection refused
```

**Solution**: Ensure PostgreSQL is running and accepting connections on the specified port.

### Permission Denied

```
psycopg2.ProgrammingError: permission denied for schema public
```

**Solution**: Grant privileges to the test user:
```sql
GRANT ALL PRIVILEGES ON DATABASE dbslice_test TO dbslice_test;
GRANT ALL PRIVILEGES ON SCHEMA public TO dbslice_test;
```

### Database Already Exists

```
psycopg2.errors.DuplicateDatabase: database "dbslice_test" already exists
```

**Solution**: Drop and recreate the database:
```bash
dropdb dbslice_test
createdb dbslice_test
```

### Tests Timing Out

**Solution**: Increase PostgreSQL connection limits or reduce test dataset sizes.

## Best Practices

1. **Test isolation**: Each test should be independent and not rely on other tests
2. **Clean database**: Use `clean_database` fixture to ensure clean state
3. **Realistic data**: Use fixtures that mirror production schemas
4. **Performance**: Keep test datasets small enough for fast execution
5. **Documentation**: Add clear docstrings explaining what each test verifies
6. **Assertions**: Use specific assertions with helpful error messages

## Resources

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [pytest Documentation](https://docs.pytest.org/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
