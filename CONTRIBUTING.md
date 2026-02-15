# Contributing to dbslice

Thank you for your interest in contributing to dbslice!

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL (for integration tests)
- Git

### Development Setup

```bash
# Clone the repository
git clone https://github.com/nabroleonx/dbslice.git
cd dbslice

# Install development dependencies
uv sync --dev

# Run tests
uv run pytest
```

## Making Changes

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

### 2. Make Your Changes

- Follow the existing code style
- Add tests for new functionality
- Update documentation as needed

### 3. Run Quality Checks

```bash
# Run tests
uv run pytest

# Type checking
uv run mypy src/dbslice

# Linting
uv run ruff check src/

# Auto-fix linting issues
uv run ruff check src/ --fix
```

### 4. Commit Your Changes

```bash
git add .
git commit -m "feat: add new feature description"
```

Follow [Conventional Commits](https://www.conventionalcommits.org/):
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `refactor:` - Code change that neither fixes a bug nor adds a feature
- `test:` - Adding missing tests

### 5. Submit a Pull Request

- Push your branch to GitHub
- Open a pull request against `main`
- Fill out the PR template
- Wait for review

## Code Style

- Use type hints for all function signatures
- Follow PEP 8 with 100 character line limit
- Write tests for new functionality using pytest fixtures from `conftest.py`
- Run `uv run ruff check src/` and `uv run mypy src/dbslice` before submitting

## Questions?

- Open a [GitHub Discussion](https://github.com/nabroleonx/dbslice/discussions)
- Check existing [Issues](https://github.com/nabroleonx/dbslice/issues)
