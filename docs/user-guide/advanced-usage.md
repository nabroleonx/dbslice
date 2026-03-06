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

You can also define explicit redact field mappings in your config file (`anonymization.fields`).  
You can also define wildcard anonymization rules in config via `anonymization.patterns`
and wildcard NULL-forcing rules via `anonymization.security_null_fields`.

Example:

```yaml
anonymization:
  enabled: true
  fields:
    users.email: email                # exact field provider
  patterns:
    users.*_name: name                # wildcard field provider
    "*.phone*": phone_number
  security_null_fields:
    - users.password*                 # force NULL
    - "*.api_key"
```

Rule precedence is: exact `fields` > wildcard `patterns` > built-in pattern mapping.
For wildcard conflicts, the most specific rule wins (ties use first-defined order).
Foreign-key columns are never anonymized or nulled.

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

### Non-Deterministic Mode

For stronger privacy guarantees, use non-deterministic mode where each value gets a random Faker seed instead of a deterministic one:

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --anonymize \
  --non-deterministic \
  --out-file strong_privacy.sql
```

Or in config:

```yaml
anonymization:
  enabled: true
  deterministic: false
```

**Trade-off**: Same value in different tables may produce different fake values (e.g., "alice@example.com" might become "john@foo.com" in one table and "jane@bar.org" in another). Use deterministic mode when cross-table consistency matters.

**Legal note**: Deterministic anonymization is technically **pseudonymization** under GDPR (same seed + input = same output = reversible). Non-deterministic mode is closer to true anonymization but structural linkage may still allow re-identification.

---

## Compliance Profiles

dbslice includes built-in compliance profiles for GDPR, HIPAA Safe Harbor, and PCI-DSS v4.0. Profiles auto-configure anonymization patterns, run value-based PII scanning, and generate audit manifests.

### Using Compliance Profiles

```bash
# HIPAA-compliant extraction
dbslice extract \
  postgres://medical-db:5432/ehr \
  --seed "patients.id=1" \
  --compliance hipaa \
  --out-file patient_subset.sql

# Multiple profiles
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --compliance gdpr \
  --compliance pci-dss \
  --out-file compliant_subset.sql
```

Or in config:

```yaml
compliance:
  profiles: [hipaa, gdpr]
  strict: true
  generate_manifest: true
```

### Available Profiles

| Profile | Description | Key Coverage |
|---------|-------------|-------------|
| `gdpr` | EU General Data Protection Regulation | Names, email, phone, address, IP, DOB, SSN, financial IDs, online identifiers |
| `hipaa` | HIPAA Safe Harbor de-identification | All 18 Safe Harbor identifiers: names, dates, geographic data, phone, fax, email, SSN, medical record numbers, health plan IDs, account numbers, license numbers, vehicle/device IDs, URLs, IPs, biometrics, photos, unique IDs |
| `pci-dss` | PCI-DSS v4.0 | PAN (credit card), cardholder name, expiration date, service code; CVV/PIN NULLed (never faked) |

### What Compliance Profiles Do

When a profile is active:

1. **Auto-enable anonymization** -- no need for `--anonymize`
2. **Merge column patterns** -- profile-defined patterns are added to your anonymization config
3. **Apply security NULL rules** -- profile-specific fields are forced to NULL (e.g., CVV for PCI-DSS)
4. **Run value-based PII scanning** -- regex patterns scan actual data values (not just column names) for email, SSN, phone numbers, IP addresses, and credit card numbers (with Luhn validation)
5. **Flag free-text columns** -- columns like `notes`, `comments`, `description` are flagged as potential PII containers
6. **Generate audit manifest** -- a JSON manifest documenting what was anonymized

### Strict Mode

In strict mode, extraction fails if the PII scanner detects unmasked PII in the output:

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=1" \
  --compliance hipaa \
  --compliance-strict \
  --out-file subset.sql
```

This ensures no PII slips through to dev/test environments.

### Audit Manifest

When compliance profiles are active (or `--manifest` is passed), dbslice writes a `*.manifest.json` file alongside the output:

```json
{
  "extraction_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-03-06T10:30:00Z",
  "dbslice_version": "0.5.0",
  "masking_type": "deterministic_pseudonymization",
  "compliance_profiles": ["hipaa"],
  "seed_hash": "sha256:a1b2c3d4e5f6...",
  "tables": {
    "patients": {
      "rows_extracted": 1,
      "fields_masked": [
        {"column": "email", "method": "email", "category": ""},
        {"column": "ssn", "method": "ssn", "category": ""}
      ],
      "fields_nulled": [
        {"column": "password_hash", "reason": "security_null_pattern"}
      ],
      "fields_preserved_fk": ["id", "doctor_id"],
      "fields_unmasked": ["created_at", "status"]
    }
  },
  "pii_scan_results": [],
  "output_file_hashes": {
    "subset.sql": "sha256:a1b2c3..."
  },
  "breakglass": {},
  "signature_algorithm": "",
  "signature": "",
  "warnings": [
    {"table": "visits", "column": "notes", "reason": "Free-text column may contain embedded PII", "severity": "warning"}
  ]
}
```

This manifest provides structured evidence for audit reviews. It documents what dbslice did but is not a substitute for infrastructure-level audit logging.

You can verify output file integrity later:

```bash
# Verify output file hashes match
dbslice verify-manifest subset.manifest.json --no-verify-signature

# Verify hashes + HMAC signature (if signing was enabled)
export DBSLICE_MANIFEST_SIGNING_KEY="your-key"
dbslice verify-manifest subset.manifest.json
```

Note: HMAC signing uses a shared symmetric key. It provides tamper detection (was the manifest modified after creation?) but not non-repudiation (it cannot prove *who* created it). For provable origin, wrap with an external signing tool (e.g., cosign, GPG) in your CI pipeline.

### Compliance Use Cases

**GDPR Right to Erasure** -- extract and anonymize before deletion:

```bash
dbslice extract \
  postgres://prod:5432/app \
  --seed "users.id=12345" \
  --compliance gdpr \
  --out-file gdpr_erasure_backup.sql
```

**HIPAA Safe Harbor De-identification**:

```bash
dbslice extract \
  postgres://medical-db:5432/ehr \
  --seed "patients.mrn='12345'" \
  --compliance hipaa \
  --compliance-strict \
  --out-file patient_deidentified.sql
```

**PCI-DSS: No Real PANs in Dev/Test** (Requirement 6.5.6):

```bash
dbslice extract \
  postgres://billing:5432/payments \
  --seed "transactions.id=999" \
  --compliance pci-dss \
  --out-file test_transactions.sql
```

---

## Column Mapping UI

Instead of manually writing anonymization config, use the built-in browser UI to visually map columns.

### Launch

```bash
dbslice map postgresql://localhost/myapp

# Custom port
dbslice map postgresql://localhost/myapp --port 8888
```

This opens a local server on `127.0.0.1:9473` with a session token for security. No data leaves your machine — the browser connects to the local `dbslice` process, which connects to the database.

### Workflow

1. **Introspect** -- Enter your database URL, click Introspect Schema. Only metadata is read.
2. **Apply profiles** -- Click GDPR, HIPAA, or PCI-DSS to auto-map columns matching the profile's rules.
3. **Review** -- For each column, set action to Keep, Anonymize, or NULL. Pick a provider from the dropdown.
4. **Export** -- Click Generate Config to produce a `dbslice.yaml`. Download it.
5. **Use** -- `dbslice extract --config dbslice.yaml --seed "table.column=value"`

### What the UI shows

- **Table list** with progress bars showing how many columns are mapped per table
- **Compliance profile chips** that overlay suggested mappings with one click
- **Provider dropdown** with descriptions (not a raw text input)
- **Summary panel** at the bottom: click "14 masked" to see all masked fields across all tables, grouped by table
- **Live YAML preview** that updates as you change mappings
- **Bulk actions** per table: Anonymize all, NULL all, Reset

### Security

- Server binds to `127.0.0.1` only (not `0.0.0.0`)
- Random session token generated at startup, required on all API requests
- No persistent state, no cookies, no external requests (except Tailwind CSS CDN for styling)

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
