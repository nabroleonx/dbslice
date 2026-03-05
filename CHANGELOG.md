# CHANGELOG


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
