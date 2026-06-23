# CHANGELOG


## v1.0.1 (2026-06-23)

### Bug Fixes

- Streaming output not clobbered by post-extraction output handler
  ([#9](https://github.com/nabroleonx/dbslice/pull/9),
  [`e5f99d8`](https://github.com/nabroleonx/dbslice/commit/e5f99d889941c38fcfffbabd8a9b2ed9f1862f63))

* fix: streaming output not clobbered by post-extraction output handler

In streaming mode, `_do_streaming_extract` writes data directly to the output file and returns an
  `ExtractionResult` with an intentionally empty `tables` dict (data lives in the file, not in
  memory). The CLI then called `_generate_and_output_sql` unconditionally, which regenerated SQL
  from the empty `tables` and wrote the resulting `BEGIN; ... COMMIT;` shell back to the same path —
  clobbering the streamed content.

The bug is silent: dbslice logs `Wrote <N> rows to <path>` and exits 0, but the file is left as a
  ~400-byte empty shell.

Fix: add `was_streamed: bool` to `ExtractionResult`, set it to True in `_do_streaming_extract`, and
  short-circuit the SQL/JSON/CSV output handlers when the flag is set. The streamed file is
  preserved as-is.

Closes #8.

* fix: address review — reject non-SQL streaming up front, single output guard

Streaming writes SQL directly to disk; it has no JSON/CSV path. Instead of defensively
  short-circuiting every output handler (which would silently preserve SQL content under a
  .json/.csv name), reject the unsupported combo:

- _should_use_streaming() returns False for any non-SQL output_format, so the auto-threshold path
  never streams SQL into a JSON/CSV file. (This was a latent bug: the gate keyed only on row count +
  output_file, not format.) - CLI rejects an explicit --stream for non-SQL output before extraction.
  - Consolidate the three per-handler streamed-result skips into one guard in
  _handle_output_format(); only SQL reaches it now. - Set was_streamed=True at the source of truth
  (StreamingExtractionEngine .stream_to_file) so direct callers and the CLI see consistent metadata;
  drop the redundant wrapper assignment. - Regression test now drives the real streaming engine
  through _handle_output_format and asserts the streamed file is byte-for-byte unchanged; add
  _should_use_streaming format-gate test; fix devnull leak.


## v1.0.0 (2026-04-06)

### Bug Fixes

- Improve extraction engine and validation mechanisms
  ([#7](https://github.com/nabroleonx/dbslice/pull/7),
  [`18c1545`](https://github.com/nabroleonx/dbslice/commit/18c1545f8b9f4450a6e2020edea5d9e6276093c5))

Consolidate duplicate WHERE clause validation, fix NULL filtering inconsistency in FK traversal, add
  seed cardinality limits, fix Decimal precision loss in JSON output, type ExtractionResult fields,
  and refactor streaming deferred updates to use chunked fetching.

Additionally, it addresses five previously unimplemented review findings:

- Use \N sentinel for NULL in CSV output to distinguish from empty string - Raise error when
  passthrough table has no primary key (was silently skipped) - Add --statement-timeout CLI flag for
  PostgreSQL query timeout - Validate anonymizer provider names at configure time (catch typos) -
  Add post-extraction compliance manifest validation

BREAKING CHANGE: - JSON output now serializes Decimal values as strings (e.g., "99.99") instead of
  floats to preserve exact precision. - CSV output now uses \N for NULL values instead of an empty
  string. - Passthrough tables without a primary key now raise an error instead of being silently
  skipped.

### Breaking Changes

- - JSON output now serializes Decimal values as strings (e.g., "99.99") instead of floats to
  preserve exact precision.


## v0.5.0 (2026-03-06)

### Features

- Add compliance profiles, PII scanning, and column mapping UI
  ([#6](https://github.com/nabroleonx/dbslice/pull/6),
  [`243efba`](https://github.com/nabroleonx/dbslice/commit/243efbae718272badb344aa2a7f9edef29f3681a))

- GDPR, HIPAA Safe Harbor, and PCI-DSS profiles that auto-configure masking - Two-phase PII value
  scanning (pre-mask coverage + post-mask residual) - Custom transformers for Safe Harbor
  (year_only, zip3, age_bucket) - Free-text redaction, binary column handling, k-anonymity checks -
  Non-deterministic anonymization mode - Audit manifests with file hashes and optional HMAC signing
  - Policy gates, source guardrails, breakglass workflow - Local browser UI for column mapping and
  config export (dbslice map) - inspect `--compliance-check` and verify-manifest commands - 160+ new
  tests


## v0.4.0 (2026-03-05)

### Features

- Add support for unsafe WHERE subqueries in seed specifications
  ([#5](https://github.com/nabroleonx/dbslice/pull/5),
  [`d246247`](https://github.com/nabroleonx/dbslice/commit/d246247e7bd401298721b21e964f7ede66e0a030))

- Introduced `--allow-unsafe-where` option in CLI to enable subqueries in seed WHERE clauses. -
  Updated configuration to include `allow_unsafe_where` field for extraction settings. - Enhanced
  PostgreSQL adapter to handle unsafe WHERE clauses based on configuration. - Modified validation
  logic to allow subqueries when explicitly opted in. - Added tests for implicit foreign key
  detection and cycle fallback behavior. - Updated existing tests to cover new functionality and
  ensure security measures are in place.


## v0.3.0 (2026-03-03)

### Features

- Add env-driven config/CLI precedence and PK-less safety guards
  ([#4](https://github.com/nabroleonx/dbslice/pull/4),
  [`895058c`](https://github.com/nabroleonx/dbslice/commit/895058c56019c4d737a48a4eaffc951bd5fa98c1))

* feat: add env-driven config/CLI precedence and PK-less safety guards

- Support `database.url` placeholders `${VAR}` and `${VAR_FILE}` in YAML config - Fail fast for
  missing env vars and unreadable `_FILE` targets - Add extract env defaults: - `DATABASE_URL` -
  `DBSLICE_DEPTH` - `DBSLICE_DIRECTION` - `DBSLICE_OUTPUT_FORMAT` - `DBSLICE_ANONYMIZE` -
  `DBSLICE_REDACT_FIELDS` - Enforce precedence: `CLI > Env > Config` - Allow `init` and `inspect` to
  fallback to `DATABASE_URL` when URL arg is omitted - Harden PK-less behavior: - error on PK-less
  seed tables - skip non-seed PK-less PK/FK fetch paths safely with warnings - Update docs for
  placeholder semantics, env formats, and precedence - Add unit/integration coverage for env
  resolution/fallback and PK-less safety

* test: clarify assertion for PK-less parent table in extraction test


## v0.2.0 (2026-02-28)

### Features

- Add schema selection support for PostgreSQL adapter
  ([#2](https://github.com/nabroleonx/dbslice/pull/2),
  [`25b483c`](https://github.com/nabroleonx/dbslice/commit/25b483cbe80f041033330f3e0d9aa8fdcb488cc5))


## v0.1.3 (2026-02-15)

### Bug Fixes

- Remove depth limit on up traversal to ensure referential integrity
  ([`36035ab`](https://github.com/nabroleonx/dbslice/commit/36035ab7a0d646e08bc25906b7fe42dae6076247))


## v0.1.2 (2026-02-15)

### Bug Fixes

- Clean up redundant test runs in release
  ([`86e320b`](https://github.com/nabroleonx/dbslice/commit/86e320b481767a1b2704596f13f5d4b7790be5b2))


## v0.1.1 (2026-02-15)
