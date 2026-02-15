# Configuration File Reference

Complete reference for dbslice YAML configuration files.

## Table of Contents

- [Overview](#overview)
- [File Location](#file-location)
- [Configuration Schema](#configuration-schema)
- [Sections](#sections)
  - [version](#version)
  - [database](#database)
  - [extraction](#extraction)
  - [anonymization](#anonymization)
  - [output](#output)
  - [tables](#tables)
  - [performance](#performance)
- [CLI Override Behavior](#cli-override-behavior)
- [Validation Rules](#validation-rules)
- [Complete Examples](#complete-examples)
- [Best Practices](#best-practices)

---

## Overview

dbslice supports YAML configuration files for managing complex extraction scenarios. Configuration files are useful for:

- **Repeatable Extractions**: Save extraction settings for consistent results
- **Team Sharing**: Share extraction configs with team members
- **Complex Configurations**: Manage multi-seed, multi-table extractions
- **CI/CD Integration**: Version-controlled extraction configurations
- **Security**: Keep sensitive settings (database URLs) out of command history

---

## File Location

### Default Locations

dbslice looks for configuration files in these locations (in order):

1. File specified with `--config` flag
2. `dbslice.yaml` in current directory
3. `.dbslice.yaml` in current directory
4. `~/.config/dbslice/config.yaml` in user home directory

### Generating Configuration Files

```bash
# Generate default configuration
dbslice init postgresql://localhost/mydb

# Generate to specific location
dbslice init postgresql://localhost/mydb -f config/production.yaml

# Generate without sensitive field detection
dbslice init postgresql://localhost/mydb --no-detect-sensitive
```

---

## Configuration Schema

The configuration file uses YAML format with the following top-level structure:

```yaml
version: "1.0"           # Config file format version
database:                # Database connection settings
extraction:              # Extraction behavior settings
anonymization:           # Anonymization configuration
output:                  # Output format settings
tables:                  # Per-table configuration (optional)
performance:             # Performance tuning (optional)
```

---

## Sections

### version

**Type:** String
**Required:** Yes
**Default:** `"1.0"`

Specifies the configuration file format version. Currently only `"1.0"` is supported.

```yaml
version: "1.0"
```

---

### database

Database connection configuration.

#### Schema

```yaml
database:
  url: string              # Database connection URL (required)
  schema: string           # Schema name (optional, default: "public" for PostgreSQL)
  options: object          # Additional connection options (optional)
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `url` | String | Yes | - | Database connection URL |
| `schema` | String | No | `"public"` | Schema name for PostgreSQL |
| `options` | Object | No | `{}` | Additional driver-specific options |

#### Examples

```yaml
# Basic PostgreSQL connection
database:
  url: postgresql://user:pass@localhost:5432/mydb

# With schema specification
database:
  url: postgresql://user:pass@localhost:5432/mydb
  schema: public

# With connection options
database:
  url: postgresql://user:pass@localhost:5432/mydb
  options:
    connect_timeout: 10
    application_name: dbslice

# Environment variable (recommended for security)
database:
  url: ${DATABASE_URL}

# Read from file
database:
  url: ${DATABASE_URL_FILE}
```

---

### extraction

Extraction behavior configuration.

#### Schema

```yaml
extraction:
  default_depth: integer           # Default traversal depth
  direction: string                # Traversal direction (up/down/both)
  exclude_tables: list[string]     # Tables to exclude
  validate: boolean                # Enable validation
  fail_on_validation_error: boolean  # Stop on validation errors
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `default_depth` | Integer | No | `3` | Maximum FK traversal depth |
| `direction` | String | No | `"both"` | Traversal direction: `up`, `down`, or `both` |
| `exclude_tables` | List[String] | No | `[]` | Tables to exclude from extraction |
| `validate` | Boolean | No | `true` | Validate extraction for referential integrity |
| `fail_on_validation_error` | Boolean | No | `false` | Stop execution if validation finds issues |

#### Examples

```yaml
# Basic extraction config
extraction:
  default_depth: 3
  direction: both

# Exclude audit tables
extraction:
  default_depth: 5
  direction: both
  exclude_tables:
    - audit_logs
    - sessions
    - temp_data
    - migration_history

# With validation
extraction:
  default_depth: 3
  direction: both
  validate: true
  fail_on_validation_error: false

# Parents only (dependencies)
extraction:
  default_depth: 10
  direction: up
  validate: true
```

---

### anonymization

Anonymization and data redaction configuration.

#### Schema

```yaml
anonymization:
  enabled: boolean              # Enable anonymization
  seed: string                  # Deterministic seed
  fields: object                # Field-specific anonymization
  patterns: object              # Custom pattern matching (optional)
  security_null_fields: list    # Fields to set to NULL (optional)
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | Boolean | No | `false` | Enable automatic anonymization |
| `seed` | String | No | Generated | Deterministic seed for consistent anonymization |
| `fields` | Object | No | `{}` | Map of `table.column` to Faker method |
| `patterns` | Object | No | Built-in | Custom pattern matching rules |
| `security_null_fields` | List[String] | No | Built-in | Fields to set to NULL (passwords, tokens) |

#### Field Anonymization Methods

Common Faker methods for the `fields` mapping:

| Method | Description | Example Output |
|--------|-------------|----------------|
| `email` | Email address | `john@example.com` |
| `phone_number` | Phone number | `+1-555-0123` |
| `first_name` | First name | `John` |
| `last_name` | Last name | `Doe` |
| `name` | Full name | `John Doe` |
| `address` | Street address | `123 Main St` |
| `city` | City name | `New York` |
| `zipcode` | ZIP/postal code | `12345` |
| `ssn` | Social Security Number | `123-45-6789` |
| `credit_card_number` | Credit card number | `4532-1234-5678-9010` |
| `ipv4` | IPv4 address | `192.168.1.1` |
| `company` | Company name | `Acme Corp` |
| `url` | URL | `https://example.com` |

See [Faker documentation](https://faker.readthedocs.io/) for complete list.

#### Examples

```yaml
# Basic anonymization
anonymization:
  enabled: true

# With custom seed (for deterministic output)
anonymization:
  enabled: true
  seed: "my-secret-seed-12345"

# Field-specific anonymization
anonymization:
  enabled: true
  fields:
    users.email: email
    users.phone: phone_number
    users.first_name: first_name
    users.last_name: last_name
    users.ssn: ssn
    customers.company: company
    payments.card_number: credit_card_number
    logs.ip_address: ipv4

# Complete anonymization config
anonymization:
  enabled: true
  seed: "production-to-dev-2023"
  fields:
    # User PII
    users.email: email
    users.phone: phone_number
    users.first_name: first_name
    users.last_name: last_name
    users.date_of_birth: date_of_birth

    # Identity documents
    users.ssn: ssn
    users.passport: passport_number
    users.driver_license: license_plate

    # Financial data
    payments.card_number: credit_card_number
    payments.routing_number: routing_number
    payments.account_number: bban

    # Contact information
    customers.company: company
    customers.address: address
    customers.city: city
    customers.postal_code: postcode

    # Network data
    logs.ip_address: ipv4
    sessions.user_agent: user_agent

# Disable anonymization for specific environments
anonymization:
  enabled: false  # For non-production to non-production transfers
```

---

### output

Output format and generation configuration.

#### Schema

```yaml
output:
  format: string                   # Output format (sql/json/csv)
  include_transaction: boolean     # Wrap in BEGIN/COMMIT
  include_drop_tables: boolean     # Include DROP TABLE statements
  disable_fk_checks: boolean       # Disable FK checks during import
  json_mode: string                # JSON mode (single/per-table)
  json_pretty: boolean             # Pretty-print JSON
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `format` | String | No | `"sql"` | Output format: `sql`, `json`, or `csv` |
| `include_transaction` | Boolean | No | `true` | Wrap SQL in BEGIN/COMMIT |
| `include_drop_tables` | Boolean | No | `false` | Include DROP TABLE statements |
| `disable_fk_checks` | Boolean | No | `false` | Disable FK checks during import |
| `json_mode` | String | No | `"single"` | JSON mode: `single` or `per-table` |
| `json_pretty` | Boolean | No | `true` | Pretty-print JSON output |

#### Examples

```yaml
# Basic SQL output
output:
  format: sql

# SQL with transactions
output:
  format: sql
  include_transaction: true
  include_drop_tables: false

# SQL for test fixtures (destructive)
output:
  format: sql
  include_transaction: true
  include_drop_tables: true  # Drops tables before inserting
  disable_fk_checks: true    # Disables FK checks during import

# JSON output (single file)
output:
  format: json
  json_mode: single
  json_pretty: true

# JSON output (per-table files)
output:
  format: json
  json_mode: per-table
  json_pretty: true

# Compact JSON for APIs
output:
  format: json
  json_mode: single
  json_pretty: false
```

---

### tables

Per-table configuration (optional advanced feature).

#### Schema

```yaml
tables:
  table_name:
    depth: integer               # Override default depth for this table
    direction: string            # Override direction for this table
    exclude: boolean             # Exclude this table
    anonymize_fields: object     # Table-specific anonymization
```

#### Examples

```yaml
# Per-table overrides
tables:
  # Deep traversal for critical table
  orders:
    depth: 10
    direction: both

  # Shallow traversal for large table
  audit_logs:
    depth: 1
    direction: down

  # Exclude table entirely
  temp_data:
    exclude: true

  # Table-specific anonymization
  users:
    anonymize_fields:
      ssn: ssn
      passport: passport_number
      tax_id: ssn

# Complete table configuration
tables:
  orders:
    depth: 5
    direction: both
    anonymize_fields:
      customer_note: text

  products:
    depth: 2
    direction: up

  sessions:
    exclude: true

  audit_logs:
    exclude: true
```

---

### performance

Performance tuning configuration (optional).

#### Schema

```yaml
performance:
  profile: boolean                    # Enable query profiling
  streaming:
    enabled: boolean                  # Force streaming mode
    threshold: integer                # Auto-enable threshold (rows)
    chunk_size: integer               # Rows per chunk
  batch_size: integer                 # Database batch size
```

#### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `profile` | Boolean | No | `false` | Enable query profiling |
| `streaming.enabled` | Boolean | No | `false` | Force streaming mode |
| `streaming.threshold` | Integer | No | `50000` | Auto-enable streaming above this row count |
| `streaming.chunk_size` | Integer | No | `1000` | Rows per chunk in streaming mode |
| `batch_size` | Integer | No | `1000` | Database batch size for bulk operations |

#### Examples

```yaml
# Basic performance config
performance:
  profile: true

# Streaming configuration
performance:
  streaming:
    enabled: false           # Auto-enable based on threshold
    threshold: 100000        # Enable streaming at 100K rows
    chunk_size: 1000         # Process 1K rows at a time

# Aggressive performance tuning
performance:
  profile: true
  streaming:
    enabled: false
    threshold: 50000
    chunk_size: 2000
  batch_size: 2000           # Larger batches for faster queries

# Memory-constrained environment
performance:
  streaming:
    enabled: true            # Always stream
    threshold: 10000         # Low threshold
    chunk_size: 500          # Small chunks
  batch_size: 500
```

---

## CLI Override Behavior

Command-line arguments take precedence over configuration file settings. This allows you to:
- Use a base configuration file
- Override specific settings via CLI for one-off extractions

### Override Rules

1. **CLI always wins**: CLI arguments override config file settings
2. **Merge behavior**: Some options (like seeds, exclude tables) are merged
3. **Complete replacement**: Other options (like depth, direction) are replaced

### Override Examples

**Config file (`dbslice.yaml`):**
```yaml
version: "1.0"
database:
  url: postgresql://localhost/mydb
extraction:
  default_depth: 3
  direction: both
  exclude_tables:
    - audit_logs
    - sessions
anonymization:
  enabled: true
```

**CLI overrides:**

```bash
# Override depth
dbslice extract --config dbslice.yaml --seed "orders.id=1" --depth 5
# Result: depth=5 (CLI wins)

# Override direction
dbslice extract --config dbslice.yaml --seed "orders.id=1" --direction up
# Result: direction=up (CLI wins)

# Add excluded tables (merged)
dbslice extract --config dbslice.yaml --seed "orders.id=1" --exclude temp_data
# Result: exclude_tables = [audit_logs, sessions, temp_data] (merged)

# Disable anonymization
dbslice extract --config dbslice.yaml --seed "orders.id=1" --no-anonymize
# Result: anonymization disabled (CLI wins)

# Override database URL
dbslice extract postgresql://other-host/db --config dbslice.yaml --seed "orders.id=1"
# Result: Uses postgresql://other-host/db (CLI wins)
```

---

## Validation Rules

Configuration files are validated when loaded. Common validation errors:

### Schema Validation

```yaml
# ❌ Invalid: Missing version
database:
  url: postgresql://localhost/mydb

# ✅ Valid: Version specified
version: "1.0"
database:
  url: postgresql://localhost/mydb
```

### Database URL Validation

```yaml
# ❌ Invalid: Unsupported protocol
database:
  url: mysql://localhost/mydb  # MySQL not yet supported

# ❌ Invalid: Malformed URL
database:
  url: not-a-valid-url

# ✅ Valid: PostgreSQL URL
database:
  url: postgresql://localhost/mydb
```

### Direction Validation

```yaml
# ❌ Invalid: Unknown direction
extraction:
  direction: sideways

# ✅ Valid: Known directions
extraction:
  direction: up     # or "down", "both"
```

### Depth Validation

```yaml
# ❌ Invalid: Negative depth
extraction:
  default_depth: -1

# ❌ Invalid: Zero depth
extraction:
  default_depth: 0

# ✅ Valid: Positive depth
extraction:
  default_depth: 3
```

### Output Format Validation

```yaml
# ❌ Invalid: Unknown format
output:
  format: xml

# ✅ Valid: Supported formats
output:
  format: sql   # or "json"
```

---

## Complete Examples

### Development Environment

**config/development.yaml:**
```yaml
version: "1.0"

database:
  url: postgresql://localhost:5432/myapp_dev

extraction:
  default_depth: 3
  direction: both
  exclude_tables:
    - audit_logs
    - sessions
    - temp_data
  validate: true
  fail_on_validation_error: false

anonymization:
  enabled: false  # No need to anonymize dev-to-dev

output:
  format: sql
  include_transaction: true
  include_drop_tables: false

performance:
  profile: false
  streaming:
    enabled: false
    threshold: 50000
```

**Usage:**
```bash
dbslice extract --config config/development.yaml --seed "orders.id=12345"
```

---

### Production to Staging

**config/prod_to_staging.yaml:**
```yaml
version: "1.0"

database:
  url: ${PRODUCTION_DATABASE_URL}  # From environment

extraction:
  default_depth: 5
  direction: both
  exclude_tables:
    - audit_logs
    - sessions
    - analytics_events
    - email_logs
  validate: true
  fail_on_validation_error: true

anonymization:
  enabled: true
  seed: "prod-to-staging-2024"
  fields:
    # User PII
    users.email: email
    users.phone: phone_number
    users.first_name: first_name
    users.last_name: last_name
    users.ssn: ssn
    users.passport: passport_number

    # Financial data
    payments.card_number: credit_card_number
    payments.routing_number: routing_number
    payments.cvv: random_int

    # Contact info
    customers.company: company
    customers.address: address
    customers.city: city

output:
  format: sql
  include_transaction: true
  include_drop_tables: false

performance:
  profile: true
  streaming:
    enabled: false
    threshold: 100000
    chunk_size: 1000
```

**Usage:**
```bash
export PRODUCTION_DATABASE_URL="postgresql://prod.example.com/myapp"

dbslice extract \
  --config config/prod_to_staging.yaml \
  --seed "users:created_at >= '2024-01-01' AND status='active'" \
  --out-file staging_subset.sql \
  --verbose
```

---

### Test Fixture Generation

**config/test_fixtures.yaml:**
```yaml
version: "1.0"

database:
  url: postgresql://localhost/myapp_dev

extraction:
  default_depth: 10  # Deep traversal for complete fixtures
  direction: both
  validate: true
  fail_on_validation_error: true

anonymization:
  enabled: true
  seed: "test-fixtures-stable"  # Stable seed for reproducible tests
  fields:
    users.email: email
    users.phone: phone_number

output:
  format: sql
  include_transaction: true
  include_drop_tables: true      # Destructive - for test DB
  disable_fk_checks: false        # Keep FK validation

performance:
  profile: false
  streaming:
    enabled: false
```

**Usage:**
```bash
dbslice extract \
  --config config/test_fixtures.yaml \
  --seed "users.email='test@example.com'" \
  --seed "products:is_test_product=true" \
  --out-file tests/fixtures/baseline.sql
```

---

### CI/CD Integration

**config/ci.yaml:**
```yaml
version: "1.0"

database:
  url: ${CI_DATABASE_URL}

extraction:
  default_depth: 3
  direction: both
  exclude_tables:
    - audit_logs
    - sessions
  validate: true
  fail_on_validation_error: true  # Fail CI on validation errors

anonymization:
  enabled: true
  seed: ${CI_ANONYMIZATION_SEED}  # From CI secrets
  fields:
    users.email: email
    users.ssn: ssn

output:
  format: sql
  include_transaction: true

performance:
  profile: false
  streaming:
    enabled: false
    threshold: 10000  # Lower threshold for CI
```

**CI Pipeline:**
```yaml
# .github/workflows/test.yml
steps:
  - name: Generate test data
    env:
      CI_DATABASE_URL: ${{ secrets.TEST_DB_URL }}
      CI_ANONYMIZATION_SEED: ${{ secrets.ANONYMIZATION_SEED }}
    run: |
      dbslice extract \
        --config config/ci.yaml \
        --seed "users:is_test_user=true" \
        --out-file test_data.sql

  - name: Load test data
    run: |
      psql $CI_DATABASE_URL < test_data.sql
```

---

### Large Dataset Migration

**config/migration.yaml:**
```yaml
version: "1.0"

database:
  url: ${SOURCE_DATABASE_URL}

extraction:
  default_depth: 3
  direction: both
  validate: true
  fail_on_validation_error: false  # Don't fail on orphaned records

anonymization:
  enabled: false  # Disable for migration

output:
  format: sql
  include_transaction: true
  include_drop_tables: false

performance:
  profile: true
  streaming:
    enabled: true           # Always stream
    threshold: 10000        # Low threshold
    chunk_size: 1000
  batch_size: 1000
```

**Usage:**
```bash
export SOURCE_DATABASE_URL="postgresql://source.example.com/myapp"

dbslice extract \
  --config config/migration.yaml \
  --seed "orders:created_at >= '2024-01-01'" \
  --out-file migration_2024.sql \
  --verbose
```

---

## Best Practices

### 1. Version Control Configuration Files

```bash
# Commit config files to version control
git add config/*.yaml
git commit -m "Add dbslice extraction configs"

# Use .gitignore for environment-specific files
echo "config/local.yaml" >> .gitignore
```

### 2. Use Environment Variables for Secrets

```yaml
# ❌ Bad: Hardcoded credentials
database:
  url: postgresql://user:password123@prod.example.com/myapp

# ✅ Good: Environment variable
database:
  url: ${DATABASE_URL}
```

### 3. Document Configuration Files

```yaml
version: "1.0"

# Production to Staging configuration
# Purpose: Extract anonymized subset for staging environment
# Updated: 2024-01-15
# Owner: DevOps Team

database:
  url: ${PRODUCTION_DATABASE_URL}

extraction:
  # Depth of 5 captures full order history
  default_depth: 5
  direction: both

  # Exclude high-volume tables
  exclude_tables:
    - audit_logs      # 500M+ rows
    - analytics_events  # 1B+ rows
```

### 4. Separate Configs by Environment

```
config/
├── development.yaml      # Local development
├── staging.yaml          # Staging environment
├── production.yaml       # Production reads
├── ci.yaml              # CI/CD pipeline
└── migration.yaml       # Data migration
```

### 5. Test Configuration Files

```bash
# Validate config file
dbslice extract --config config/production.yaml --dry-run --seed "orders.id=1"

# Test with small dataset first
dbslice extract --config config/production.yaml --seed "orders.id=12345" --depth 1
```

### 6. Use Profiles for Different Scenarios

```yaml
# Base configuration
version: "1.0"

database:
  url: ${DATABASE_URL}

extraction:
  default_depth: 3
  direction: both

# Override for specific scenarios via CLI
# Bug reproduction: --depth 10 --profile
# Quick test: --depth 1 --no-validate
# Large dataset: --stream --stream-threshold 10000
```

---

## See Also

- [CLI Reference](cli-reference.md) -- Command-line interface
- [Advanced Usage](advanced-usage.md) -- Anonymization, streaming, virtual FKs
