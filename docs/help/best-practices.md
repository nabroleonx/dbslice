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

## 2b. Use Compliance Profiles for Regulated Data

```bash
dbslice extract postgres://prod/db --seed "users.id=1" \
  --compliance hipaa --compliance-strict
```

Compliance profiles (GDPR, HIPAA, PCI-DSS) auto-configure anonymization, run value-based PII scanning, and generate audit manifests. Use `--compliance-strict` to fail if unmasked PII is detected.

## 2c. Treat Output as Pseudonymized Data

Deterministic mode is **pseudonymization**, not full anonymization. For higher privacy, set `anonymization.deterministic: false` and still keep operational controls (least privilege DB account, restricted output location, and manifest review).

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

## 11. Use a Compliance Runbook in CI

Suggested CI flow:
1. `dbslice inspect --compliance-check ... --compliance-output json` on target schema.
2. `dbslice extract ... --out-file ...` with compliance profiles.
3. `dbslice verify-manifest ...` to confirm output file hashes.
4. Optionally sign manifest + output with an external tool (cosign, GPG) for non-repudiation.
5. Archive artifacts to immutable storage (S3 Object Lock, GCS retention, etc.).

## 12. Compliance Controls (Quick Reference)

These are **runtime CLI checks**, not an IAM or governance system. They reduce accidental mistakes but are not a substitute for network-level controls, access policies, or encryption at rest.

| Risk | Control | Limitation |
|------|---------|------------|
| Unmasked PII reaches dev/test | `--compliance ... --compliance-strict`, profile rules, residual scan | Pattern-based detection only; may miss PII in unusual column names or embedded in binary data |
| Unsafe ad-hoc extraction | `compliance.policy_mode: standard`, breakglass override with reason + ticket | CLI flags can be bypassed by not using the config file |
| Unknown data source used | `compliance.allow_url_patterns` / `deny_url_patterns` | Regex on URL string; does not prevent DNS aliasing or network-level bypass |
| Non-TLS DB connection | `compliance.required_sslmode` | Checks URL query param only; does not verify actual TLS handshake |
| Non-CI execution | `compliance.require_ci: true` | Checks `CI=true` env var, which can be set manually |
| Output tampering | Manifest `output_file_hashes` + `dbslice verify-manifest` | SHA256 file hashes detect changes after the fact |
| Manifest tampering | `compliance.sign_manifest: true` with HMAC-SHA256 | Symmetric key — tamper detection only, **not** non-repudiation. For provable origin, wrap with external signing (cosign, GPG) |

---

## See Also

- [Advanced Usage](../user-guide/advanced-usage.md) -- Anonymization, streaming, virtual FKs
- [CLI Reference](../user-guide/cli-reference.md) -- All command options
- [Configuration](../user-guide/configuration.md) -- YAML config file reference
