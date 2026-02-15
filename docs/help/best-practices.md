# Best Practices

Top tips for effective use of dbslice.

---

## 1. Start with Low Depth, Increase If Needed

```bash
dbslice extract postgres://host/db --seed "orders.id=123" --depth 1
```

Default depth is 3. Reduce for faster, smaller extractions. Increase (up to 5-10) only if you are missing related data.

## 2. Always Anonymize Production Data

```bash
dbslice extract postgres://prod/db --seed "users.id=1" --anonymize
```

Never extract production data without `--anonymize`. Foreign keys are preserved automatically.

## 3. Validate Extractions

```bash
dbslice extract postgres://host/db --seed "orders.id=123" --validate --fail-on-validation-error
```

Validation checks that all FK references point to included records. Enable `--fail-on-validation-error` in scripts and CI.

## 4. Use Config Files for Repeatable Extractions

```bash
dbslice init postgres://localhost/myapp --out-file dbslice.yaml
dbslice extract --config dbslice.yaml --seed "orders.id=123"
```

Store settings in YAML for consistency. Use environment variables for credentials (`url: ${DATABASE_URL}`).

## 5. Exclude Large Non-Essential Tables

```bash
dbslice extract postgres://host/db --seed "orders.id=123" \
  --exclude audit_logs \
  --exclude analytics_events
```

Audit logs, analytics, and session tables are often huge and unnecessary for debugging.

## 6. Use Streaming for Large Datasets

```bash
dbslice extract postgres://host/db \
  --seed "orders:created_at > '2023-01-01'" \
  --stream --out-file large.sql
```

Streaming writes directly to file, avoiding out-of-memory errors. Required for datasets over 100K rows.

## 7. Use Read-Only Accounts on Production

```sql
CREATE USER dbslice_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE myapp TO dbslice_readonly;
GRANT USAGE ON SCHEMA public TO dbslice_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbslice_readonly;
```

Prefer read replicas over primary servers.

## 8. Profile Slow Extractions

```bash
dbslice extract postgres://host/db --seed "orders.id=123" --profile --verbose
```

Profiling identifies which queries are slow and suggests missing indexes.

## 9. Use JSON for Cross-Database Workflows

```bash
dbslice extract postgres://host/db --seed "orders.id=1" --output json --out-file fixtures/
```

JSON is database-agnostic and works well for test fixtures in any language.

## 10. Test Import Before Using

```bash
createdb test_import
psql -d test_import < schema.sql
psql -d test_import < subset.sql
psql -d test_import -c "SELECT COUNT(*) FROM orders;"
dropdb test_import
```

Always verify extracted data loads cleanly into an isolated database before relying on it.

---

## See Also

- [Advanced Usage](../user-guide/advanced-usage.md) -- Anonymization, streaming, virtual FKs
- [CLI Reference](../user-guide/cli-reference.md) -- All command options
- [Configuration](../user-guide/configuration.md) -- YAML config file reference
