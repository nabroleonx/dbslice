# Advanced Usage

This guide covers anonymization, streaming for large datasets, and virtual foreign keys.

---

## Anonymizing Sensitive Data

### Basic Anonymization

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --anonymize \
  --out-file user_safe.sql
```

**What gets anonymized** (pattern-matched, case-insensitive):

| Category | Column patterns | Faker method |
|----------|----------------|--------------|
| Email | `email`, `contact_email` | `faker.email()` |
| Phone | `phone`, `mobile`, `fax` | `faker.phone_number()` |
| Names | `name`, `first_name`, `last_name` | `faker.name()` / variants |
| Address | `address`, `street`, `city`, `zip` | `faker.address()` / variants |
| Identity | `ssn`, `credit_card`, `passport`, `driver_license` | Matching Faker methods |
| Financial | `iban`, `bank_account`, `routing_number` | Matching Faker methods |
| Network | `ip_address`, `mac_address` | `faker.ipv4()` / `faker.mac_address()` |
| Personal | `dob`, `date_of_birth`, `username` | Matching Faker methods |
| Professional | `company`, `job_title` | `faker.company()` / `faker.job()` |
| Web | `url`, `domain` | `faker.url()` / `faker.domain_name()` |

**What gets NULLed** (security-sensitive, never faked):

- Authentication: `password`, `hash`, `salt`
- Tokens: `token`, `secret`, `api_key`, `access_token`, `refresh_token`, `csrf_token`
- Cryptographic: `private_key`, `encryption_key`, `nonce`, `signature`, `certificate`

**What stays original**: Foreign keys, IDs, timestamps, enums, booleans, and non-sensitive data.

### Example Transformation

**Original**:
```sql
INSERT INTO users (id, email, name, password_hash, country_id)
VALUES (1, 'alice@example.com', 'Alice Smith', '$2b$12$...', 1);
```

**Anonymized**:
```sql
INSERT INTO users (id, email, name, password_hash, country_id)
VALUES (1, 'john.doe@example.org', 'John Smith', NULL, 1);  -- FK preserved
```

### Custom Field Redaction

For fields not caught by built-in patterns:

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --anonymize \
  --redact "users.employee_id" \
  --redact "orders.internal_notes" \
  --out-file user_custom_redact.sql
```

You can also define custom patterns and redact lists in your config file. See [Configuration](configuration.md) for the `anonymization` section.

### Deterministic Anonymization

Anonymization is deterministic: the same input value always produces the same fake output. This preserves data relationships across tables.

```
Original:
  users.email = 'alice@example.com'
  profiles.contact_email = 'alice@example.com'

Anonymized (both become the same fake email):
  users.email = 'john@example.org'
  profiles.contact_email = 'john@example.org'
```

### Verify Integrity After Anonymization

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --anonymize \
  --validate \
  --out-file user_safe_validated.sql
```

Validation confirms all FK references remain intact after anonymization.

### Compliance Use Cases

**GDPR Right to Erasure** -- extract and anonymize before deletion:

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=12345" \
  --anonymize \
  --out-file gdpr_erasure_backup.sql
```

**HIPAA De-identification** -- anonymize plus redact clinical fields:

```bash
dbslice extract \
  postgres://medical-db:5432/ehr \
  --seed "patients.mrn='12345'" \
  --anonymize \
  --redact "patients.social_security" \
  --redact "visits.notes" \
  --out-file patient_deidentified.sql
```

---

## Streaming Large Datasets

When extracting large result sets (100K+ rows), use streaming mode to avoid out-of-memory errors.

### Force Streaming

```bash
dbslice extract \
  postgres://localhost/db \
  --seed "users:created_at > '2020-01-01'" \
  --stream \
  --out-file large_extract.sql
```

**How it works**: Data is fetched in chunks (default 1000 rows) and written directly to the file, avoiding loading everything into memory.

`--out-file` is required for streaming (cannot stream to stdout).

### Auto-Streaming

dbslice automatically enables streaming when the result set exceeds the threshold (default 50K rows):

```bash
dbslice extract \
  postgres://localhost/db \
  --seed "orders:created_at > '2020-01-01'" \
  --out-file huge_extract.sql
```

### Tune Streaming Behaviour

```bash
# Lower the auto-streaming threshold
dbslice extract ... --streaming-threshold 10000 --out-file extract.sql

# Adjust chunk size (larger = faster throughput, more memory)
dbslice extract ... --stream --streaming-chunk-size 5000 --out-file extract.sql

# Smaller chunks for memory-constrained environments
dbslice extract ... --stream --streaming-chunk-size 500 --out-file extract.sql
```

Default chunk size is 1000 rows, which is a good balance for most cases.

### Other Performance Tips

- **Reduce depth**: `--depth 1` is much faster than the default `--depth 3`.
- **Exclude large tables**: `--exclude audit_logs --exclude analytics_events`.
- **Profile slow extractions**: `--profile` shows per-query timing and suggests missing indexes.
- **Add indexes on FK columns** before large extractions:
  ```sql
  CREATE INDEX idx_orders_user_id ON orders(user_id);
  ```

See [CLI Reference](cli-reference.md) for full streaming and performance options.

---

## Virtual Foreign Keys

Virtual foreign keys let you define relationships between tables that are **not enforced by database constraints**. This is useful for:

- Django apps using `GenericForeignKey` (ContentType framework)
- Legacy schemas with missing FK constraints
- Implicit relationships via application logic
- Cross-database references

### Configuration

Define virtual FKs in your `dbslice.yaml` config file:

```yaml
virtual_foreign_keys:
  # Django GenericFK to orders
  - source_table: notifications
    source_columns:
      - object_id
    target_table: orders
    target_columns:
      - id
    description: "Generic FK to orders via ContentType"
    name: vfk_notifications_orders
    is_nullable: false

  # Implicit relationship
  - source_table: audit_log
    source_columns:
      - user_id
    target_table: users
    description: "Implicit FK without DB constraint"
    is_nullable: true
```

### Configuration Fields

| Field | Required | Description |
|-------|----------|-------------|
| `source_table` | Yes | Table containing the FK columns |
| `source_columns` | Yes | Column names forming the FK |
| `target_table` | Yes | Table being referenced |
| `target_columns` | No | Target columns (defaults to target table's PK) |
| `description` | No | Human-readable description |
| `name` | No | Custom FK name (auto-generated if omitted) |
| `is_nullable` | No | Whether the FK can be NULL (defaults to `true`) |

### How It Works

Virtual FKs are added to the schema graph and traversed just like real FKs during extraction. They appear in verbose output with a `(virtual:...)` marker:

```
seed: notifications (1 rows)
notifications --(up:fk_notifications_content_type)--> django_content_type (1 rows)
notifications --(virtual:vfk_notifications_orders)--> orders (1 rows)
```

### Example: Django GenericForeignKey

**Config** (`dbslice.yaml`):
```yaml
database:
  url: postgres://localhost:5432/django_app

extraction:
  default_depth: 3

virtual_foreign_keys:
  - source_table: notifications
    source_columns: [object_id]
    target_table: users
    description: "Generic FK to users"

  - source_table: notifications
    source_columns: [object_id]
    target_table: orders
    description: "Generic FK to orders"
```

**Extract**:
```bash
dbslice extract \
  --config dbslice.yaml \
  --seed "users.id=1" \
  --out-file user_subset.sql
```

This extracts user 1, their orders, and all notifications pointing to them via the virtual FK.

### Example: Composite Virtual FK

Multi-column virtual foreign key for multi-tenant schemas:

```yaml
virtual_foreign_keys:
  - source_table: order_events
    source_columns:
      - tenant_id
      - order_id
    target_table: orders
    target_columns:
      - tenant_id
      - id
    description: "Multi-tenant composite FK"
    is_nullable: false
```

### Ambiguous References (GenericForeignKey)

When multiple virtual FKs share the same source column (e.g., `object_id`), dbslice tries all of them. Use WHERE clauses to filter:

```bash
dbslice extract \
  --config dbslice.yaml \
  --seed "notifications:content_type_id = 12 AND object_id = 1" \
  --out-file extract.sql
```

### Performance Considerations

Virtual FKs cannot use the same database-level optimizations as real FKs. Add indexes on virtual FK columns for better performance:

```sql
CREATE INDEX idx_notifications_object_id ON notifications(object_id);
CREATE INDEX idx_audit_log_user_id ON audit_log(user_id);
```

### Testing Virtual FKs

Use dry-run with verbose output to verify virtual FKs are being followed:

```bash
dbslice extract \
  --config dbslice.yaml \
  --seed "notifications.id=1" \
  --dry-run \
  --verbose
```

Look for `(virtual:...)` entries in the traversal path.

---

## See Also

- [CLI Reference](cli-reference.md) -- All command options
- [Configuration](configuration.md) -- YAML config file reference
- [Best Practices](../help/best-practices.md) -- Quick tips
