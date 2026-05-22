---
title: TraceDB Platform Contract v0
tags:
  - tracedb
  - platform-contract
  - sdk
status: contract-freeze-draft
type: platform-contract
updated: 2026-05-22
---

# TraceDB Platform Contract v0

This is the DX-facing contract that SDKs and future adapters must converge on.
It sits above the current HTTP route inventory and below language-specific SDK
ergonomics:

```text
TraceDB SDK Contract
  -> canonical HTTP/wire contract
  -> shared TraceQuery/result/error model
  -> Rust reference SDK
  -> TypeScript platform SDK
  -> Python AI/data SDK
  -> TraceQL / SQL-ish adapter
  -> GraphQL adapter
```

MCP is optional glue later. It does not drive this architecture.

The companion machine-readable manifest is `docs/platform-contract-v0.json`.
The current wire contract is `docs/api/v1-http.md`; the current generated route
artifact is `docs/api/v1-openapi.json`.
The initial executable conformance runner is `scripts/platform_conformance.py`.

## Boundaries

- Status is `contract-freeze-draft`. This is a source-of-truth checklist for
  SDK/adaptor work, not a managed-cloud SLA.
- SQL compatibility is not implemented.
- TraceDB is not PostgreSQL-compatible. Future SQL-ish work must compile into
  TraceDB's native query model instead of becoming a PostgreSQL emulation layer.
- GraphQL is planned after SDK/query contract stabilization and must compile
  into the same query model instead of creating a resolver-specific database
  model.
- Exported performance claims still require an external control and a number to
  beat. Internal TraceDB-only runs are development evidence only.

## Developer Model

Every product surface must map to these contract components:

| Component | Contract |
| --- | --- |
| `connection_config` | URL, token, timeout, retry policy, and optional local/admin paths belong in connection configuration, not per-call reinvention. |
| `database_branch_config` | Managed database and branch routing use `database_id` and `branch_id` metadata. Direct local-engine calls can omit them. |
| `table_handles` | SDKs should expose table-scoped handles so application code can bind a table once before writes and queries. |
| `schema_migrations` | v0 requires schema apply. Migration planning/versioning is future but must remain part of the contract vocabulary. |
| `record_writes` | Single-record put/patch/delete must share record identity, tenant identity, and field semantics across surfaces. |
| `batch_ingest` | Batch writes are first-class and should preserve `record_count`, epoch, and optional write timing where exposed. |
| `query_builder` | SDK builders should compile into the same `HybridQuery`/TraceQuery model as direct JSON calls. |
| `traceql_string_execution` | Native TraceQL v0 strings execute through `POST /v1/traceql` after compiling into `HybridQuery`; SQL-ish syntax remains a future adapter over the same query model, not a separate engine. |
| `result_envelope` | Success responses are route-specific JSON; errors preserve the current `{ "error": string, "code"?: string }` envelope plus SDK context. |
| `explain_provenance_freshness_jobs` | Query/explain surfaces share `HybridExplain` fields for access paths, planner candidates, counters, timings, and freshness/provenance evidence as they mature. |
| `errors_retries_idempotency` | Safe retries stay read-only. Mutation/admin retries require caller-provided `Idempotency-Key`; same key/body replays, body mismatch returns `409`. |
| `pagination_cursors` | Pagination/cursors are a named future contract. Today scan returns `records` and `returned_count` with no cursor metadata. |
| `admin_compact_snapshot_restore` | Compact, snapshot, restore, and admin-jobs visibility are part of the platform path, with local filesystem boundaries explicit. |

## Current Surface Matrix

| Surface | ID | Current status | Contract role |
| --- | --- | --- | --- |
| HTTP direct | `http_direct` | Current | Canonical wire contract. |
| Rust SDK | `rust_sdk` | Reference candidate with env config | Ergonomic reference SDK over the wire contract while preserving raw HTTP methods. |
| TypeScript SDK | `typescript_sdk` | Public wrapper conformance checked with env config, safe retries, and idempotency retries | Hand-written `TraceDB` table/query wrapper over the generated transport. |
| Python SDK | `python_sdk` | Sync HTTP smoked from installed package with safe and idempotency retries | Sync-first AI/data/notebook SDK over the canonical HTTP contract. |
| TraceQL / SQL-ish | `traceql_sqlish` | Native TraceQL HTTP execution checked; SQL-ish syntax parked | Future adapter into the same TraceQuery/query model. |
| GraphQL | `graphql` | Planned after contract | Future schema-generated adapter into the same TraceQuery/query model. |

Maintenance mode means a platform project can use TraceDB through Rust, TypeScript, Python, TraceQL/SQL-ish, or GraphQL and receive the same behavior, same errors, same result shape, and same explain/freshness semantics.

## Conformance Harness v0

The first harness should use the machine-readable manifest as the shared
scenario list. Each surface reports pass/fail/skipped for the same scenario IDs
and must not invent surface-specific semantics.

| Scenario | ID | Current wire path | Required behavior |
| --- | --- | --- | --- |
| Schema apply | `schema_apply` | `POST /v1/schema/apply` | Applies `TableSchema` and returns an epoch. |
| Put | `put` | `POST /v1/records/put` | Writes or replaces one record and returns an epoch. |
| Batch ingest | `batch` | `POST /v1/records/put-batch` | Writes multiple records and returns `record_count` plus epoch. |
| Patch | `patch` | `POST /v1/records/patch` | Updates selected fields while preserving untouched fields. |
| Get | `get` | `POST /v1/records/get` | Returns `RecordOutput` or `null`. |
| Scan | `scan` | `POST /v1/records/scan` | Returns `records` and `returned_count`; cursor metadata is future. |
| Query | `query` | `POST /v1/query` | Returns `HybridQueryRow` results with typed score components. |
| TraceQL string execution | `traceql_string_execution` | `POST /v1/traceql` | Parses native TraceQL strings into the shared `HybridQuery` model and preserves query result/error envelope behavior. |
| Explain | `explain` | `POST /v1/explain` | Returns `HybridExplain` access paths, planner candidates, counters, and timings. |
| Delete | `delete` | `POST /v1/records/delete` | Hides deleted records from get, scan, query, and explain materialization. |
| Idempotency | `idempotency` | `Idempotency-Key` on mutation/admin routes | Same key plus same method/path/body replays; same key with different body returns `409`. |
| Errors | `errors` | Current JSON error envelope | SDKs expose status/method/path/body plus parsed `error` and optional `code`. |
| Snapshot/restore | `snapshot_restore` | `POST /v1/admin/snapshot`, `POST /v1/admin/restore` | Snapshot local state, restore into a separate target, and preserve visible state. |

Future harness lanes: `pagination_cursors` and `durable_jobs`. These stay out
of the v0 pass/fail contract until the engine exposes concrete behavior.

Run the current executable lanes with:

```bash
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
```

The `http_direct` lane uses raw stdlib HTTP requests against `tracedb-server`
and now checks all 13 current v0 scenario IDs, including native
`traceql_string_execution` through `POST /v1/traceql`. The `rust_sdk` lane maps
the existing Rust SDK quickstart product path into the same manifest scenario
IDs and now checks all 13 current IDs, including `traceql_string_execution`
through `TraceDbClient::traceql_typed`. The `typescript_sdk` lane runs the
public TypeScript SDK smoke through `npm run public-http-smoke --
--summary-json ...` and maps schema apply, put, batch, patch, get, scan, query,
TraceQL string execution, explain, delete, idempotency, errors, and
snapshot/restore into the same scenario IDs. The `python_sdk` lane first
installs a copied `clients/python` package into an isolated temporary pip
`--target`, then runs `clients/python/http_smoke.py` with source-path imports
disabled. It maps schema apply, put, batch, patch, get, scan, query, explain,
delete, idempotency, errors, and snapshot/restore into the same scenario IDs and
still reports `traceql_string_execution` as `not_checked` until the sync SDK
exposes native TraceQL execution. Future surfaces must report unimplemented
scenarios as `not_checked` rather than silently treating them as success.

Current verified checkpoint: Modal workspace run `ap-7dKR46BWCsRmjRBCNctWhn`
passed in 82.426s. Its `platform-conformance-quick` command reported
`http_direct` 13/13 and `rust_sdk` 13/13, including
`traceql_string_execution`; its `typescript-sdk-conformance` command reported
`typescript_sdk` 13/13 with native TraceQL covered by public SDK result and
explain evidence.

The Rust SDK also has a first ergonomic reference layer over the same wire
contract: `TraceDb::connect(config)?` returns the reference client, and
`db.table("docs").tenant("tenant-a")` returns a `TableHandle`. Handles can
execute table insert, batch insert, patch, get, scan, and delete calls, then
enter the query builder with `query()` or the direct chaining helpers
`where_eq`, `match_text`, `near`, `with_explain`, `limit`, `all()`, and
`explain_plan()`. `TraceDbClient::traceql_typed` and `traceql_request_typed`
send native TraceQL strings to `POST /v1/traceql` and decode the same
`QueryResponse` envelope as `query_typed`. These helpers compile into or reuse
the existing `RecordInput`, `RecordPutBatchRequest`, record request,
`TraceQlQueryRequest`, and `HybridQuery` models; the raw HTTP methods remain
available.
`TraceDbClientConfig::from_env()` now reads `TRACEDB_URL`, optional
`TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`,
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES` so Rust callers can share the same connection and
routing config boundary as the other SDK lanes.

The TypeScript package now starts the public SDK layer in
`clients/typescript/src/index.ts` and `clients/typescript/src/sdk.ts`.
`@tracedb/sdk` exposes `TraceDB`; `@tracedb/sdk/transport` exposes the generated
`TraceDbClient` transport subpath through built `dist` JS/declaration outputs.
`new TraceDB({ url, token })` or
`TraceDB.fromEnv()` wraps that transport and exposes table handles with
`insert`, `insertBatch`, `patch`, `get`, `scan`, `delete`, admin
compact/snapshot/restore/jobs, `where`, `match`, `near`, `with`, `limit`, `all`,
`explainPlan`, `traceql`, and `traceqlRequest`. `TraceDB.fromEnv()` reads
`TRACEDB_URL`, optional `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`,
`TRACEDB_BRANCH_ID`, and `TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES` so the TypeScript public SDK shares the same
connection, routing, read-only retry, and keyed mutation/admin retry boundary as
Rust. `safeRetries` only retries transient 5xx responses for health/ready, get,
scan, query, native TraceQL, and explain. `idempotencyRetries` is default-off and retries
transient 5xx responses for mutation/admin routes only when the request carries
a caller-provided `Idempotency-Key`. The wrapper is fake-fetch,
build/pack, packed temp-consumer install, package-entry, and typecheck guarded
and now has real local HTTP and gateway smokes through `npm run
public-http-smoke` and `npm run gateway-smoke`.
The
public HTTP smoke now emits machine-readable
idempotency, TraceQL result/explain, and error-envelope evidence for
`scripts/platform_conformance.py --surface typescript_sdk`; the generated
transport remains available and remains the source of route methods.

The Python package now starts the sync-first AI/data SDK lane in
`clients/python/tracedb/client.py`. `TraceDB(url, token="dev-token")` exposes
table handles and a query builder with `insert`, `insert_batch`, `patch`, `get`,
`scan`, `delete`, `where`, `match_text`, `near`, `with_options`, `limit`,
`all`, and `explain_plan`, plus health/catalog/metrics/admin helpers. The
stdlib-only SDK also exposes `TraceDB.from_env()` for `TRACEDB_URL`,
`TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`,
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES`. `safe_retries` only retries transient 5xx
responses for health, ready, get, scan, query, and explain.
`idempotency_retries` is default-off and retries transient 5xx responses for
mutation/admin routes only when that request carries a caller-provided
`Idempotency-Key`; unkeyed writes and 4xx/conflict responses are not retried.
The local package/unit lane is `python3 -m unittest discover -s
clients/python/tests`; `python3 clients/python/install_smoke.py` prefers a
temporary venv, installs `clients/python` with pip `--no-deps`, and runs a
consumer from outside the repo to prove the installed `tracedb` package exports
the public DX. When a remote image can run Python but lacks working `ensurepip`,
the same smoke falls back to an isolated temporary pip `--target` install.
Modal workspace verification runs both package lanes before the Python
conformance smoke. The stdlib-only smoke `python3
clients/python/http_smoke.py` starts a local
`tracedb-server` and proves all required v0 contract scenarios through the
Python surface. It is sync SDK contract evidence, not package publishing
readiness, async support, managed-cloud proof, SQL compatibility, or GraphQL
support. The smoke is also promoted into the local product gate as
`product-regression --only python_sdk_smoke`, so `product-quickstart
--skip-typescript` now retains Python SDK contract evidence while omitting only
the TypeScript lanes.

Native TraceQL v0 now executes through `POST /v1/traceql` with a
`TraceQlQueryRequest` body of `{ "query": string }`. The server parses the
line-oriented string with `traceql_query_from_str`, accepts directives such as
`FROM`, `TENANT`, `WHERE`, `MATCH`, `NEAR`, `FRESHNESS`, `LIMIT`, and
`EXPLAIN`, and compiles them into the existing `HybridQuery` model before using
the same query execution and response shaping as `POST /v1/query`. This is
native TraceQL evidence only. It is not SQL compatibility, PostgreSQL
compatibility, GraphQL support, or a separate query engine.

## Surface Implementation Rules

- HTTP direct remains the source of wire truth.
- Rust SDK is the reference ergonomic SDK. It may add table handles and builder
  APIs, but raw HTTP methods must remain available.
- TypeScript's generated client is a transport layer, not the final public
  product shape.
- Python starts sync-first for ingestion, AI workflows, notebooks, and platform
  conformance tests. Async can follow after sync parity.
- TraceQL / SQL-ish must parse into the same query model. Do not claim SQL
  compatibility or PostgreSQL compatibility. The current native TraceQL HTTP
  surface is `/v1/traceql`; SQL-ish syntax remains parked.
- GraphQL must be schema-generated from TraceDB schema and compile into the same
  query model. It should not own database semantics.

## Verification Ladder

Use the smallest ladder that proves the touched surface:

```bash
cargo test -p tracedb-testkit --test usability_acceptance platform_contract_v0_declares_sdk_conformance_harness -- --exact
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 -m unittest discover -s clients/python/tests
python3 clients/python/install_smoke.py
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/generate_openapi_v1.py --check
python3 scripts/generate_typescript_client.py --check
cargo run -p tracedb-cli -- product-quickstart --skip-typescript
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

Local macOS executable-policy failures are local environment evidence, not a
contract failure. Use Modal for remote Linux product verification when the local
machine cannot execute Rust binaries.
