# dbslice development commands

# Show available commands
default:
    @just --list

# Install dependencies
install:
    uv sync --dev

# Run unit tests
test *args='':
    uv run pytest tests/ -m "not integration" -v {{args}}

# Run integration tests (requires DBSLICE_TEST_DB)
test-integration:
    #!/usr/bin/env bash
    if [ -z "$DBSLICE_TEST_DB" ]; then
        echo "Error: DBSLICE_TEST_DB not set"
        echo "Example: export DBSLICE_TEST_DB='postgresql://user:pass@localhost:5432/dbslice_test'"
        exit 1
    fi
    uv run pytest tests/integration/ -v

# Run all tests
test-all:
    uv run pytest tests/ -v

# Run tests with coverage
test-coverage:
    uv run pytest tests/ -m "not integration" --cov=dbslice --cov-report=html --cov-report=term

# Run linter
lint:
    uv run ruff check src/ tests/

# Format code
format:
    uv run ruff format src/ tests/

# Run type checker
typecheck:
    uv run mypy src/dbslice/

# Run all checks (lint + typecheck + test)
check: lint typecheck test

# Serve docs locally
docs:
    uv run mkdocs serve -a localhost:9006

# Build and prepare a release
release:
    #!/usr/bin/env bash
    version=$(grep 'version' pyproject.toml | head -1 | cut -d'"' -f2)
    echo "Current version: $version"
    rm -rf dist/ build/
    uv build
    echo ""
    echo "Built artifacts:"
    ls -la dist/
    echo ""
    echo "To publish: uv publish --token YOUR_PYPI_TOKEN"
    echo "Or create a GitHub Release to auto-publish via CI"

# Clean build artifacts
clean:
    rm -rf build/ dist/ .pytest_cache .coverage htmlcov/ .ruff_cache site/
    find . -type d -name __pycache__ -exec rm -rf {} +
    find . -type f -name "*.pyc" -delete

# Start PostgreSQL container for integration tests
docker-postgres:
    docker run --name dbslice-test-postgres \
        -e POSTGRES_USER=dbslice_test \
        -e POSTGRES_PASSWORD=test_password \
        -e POSTGRES_DB=dbslice_test \
        -p 5432:5432 \
        -d postgres:15
    @echo "Waiting for PostgreSQL..."
    @sleep 5
    @echo "Ready. Run: export DBSLICE_TEST_DB='postgresql://dbslice_test:test_password@localhost:5432/dbslice_test'"

# Stop PostgreSQL container
docker-stop:
    docker stop dbslice-test-postgres || true
    docker rm dbslice-test-postgres || true

# Run integration tests with Docker PostgreSQL
docker-test: docker-stop docker-postgres
    #!/usr/bin/env bash
    sleep 5
    export DBSLICE_TEST_DB='postgresql://dbslice_test:test_password@localhost:5432/dbslice_test'
    uv run pytest tests/integration/ -v
    just docker-stop
