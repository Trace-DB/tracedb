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

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

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
TraceField is the memory/runtime research program that informs future runtime
directions, not the current product or an implemented runtime in this repo.
Agent Memory Flight Recorder is a concrete local demo wedge built on TraceDB
records, query/explain output, and replayable receipts; it is not a conformance
surface yet. Tensor artifacts are future governed derived-artifact/module work,
not current tensor compute or tensor storage services.
`crates/tracedb-memory-runtime` is placeholder/scaffolding only; memory calculus
is not implemented.

The companion machine-readable manifest is `docs/platform-contract-v0.json`.
The current wire contract is `docs/api/v1-http.md`; the current generated route
artifact is `docs/api/v1-openapi.json`.
The local durability boundary is `docs/durability-semantics-v0.md`.
The initial executable conformance runner is `scripts/platform_conformance.py`.

## Boundaries

- Status is `contract-freeze-draft`. This is a source-of-truth checklist for
  SDK/adaptor work, not a managed-cloud SLA.
- SQL compatibility is not implemented; the bounded SQL-ish `SELECT` adapter
  under `/v1/traceql` is not a SQL engine.
- TraceDB is not PostgreSQL-compatible. Future SQL-ish work must compile into
  TraceDB's native query model instead of becoming a PostgreSQL emulation layer.
- GraphQL has a native `data`/`errors` envelope through `POST /v1/graphql`
  for TraceDB query, mutation, and admin operations. The bounded GraphQL query
  adapter remains compatibility-only at `POST /v1/graphql/bounded`.
  Subscriptions remain unsupported.
- Exported performance claims still require an external control and a number to
  beat. Internal TraceDB-only runs are development evidence only.
- Local WAL/checkpoint/snapshot behavior is governed by
  `docs/durability-semantics-v0.md`; it is not a managed-cloud SLA, not
  cross-replica idempotency, and not crash-atomic exactly-once semantics.
- The current HTTP stack boundary is local/development product proof:
  `tracedb-server` and `tracedb-gateway` default to Tokio/Axum product paths
  with Tower body limits, timeouts, load shedding, concurrency limits, graceful
  shutdown, structured JSON tracing, and private engine-token enforcement where
  configured. Legacy stdlib listener helpers remain for compatibility tests and
  local harnesses. The HTTP routes remain the canonical wire contract, but the
  server implementation does not provide TLS or HTTP/2 and is not a complete
  managed-service runtime.

## Developer Model

Every product surface must map to these contract components:

| Component | Contract |
| --- | --- |
| `connection_config` | URL, token, timeout, retry policy, and optional local/admin paths belong in connection configuration, not per-call reinvention. |
| `database_branch_config` | Managed database and branch routing use `database_id` and `branch_id` metadata. Direct local-engine calls can omit them. SDKs default `branch_id` to `<database_id>:main` on copied object-shaped POST bodies when a configured database omits an explicit branch. |
| `table_handles` | SDKs should expose table-scoped handles so application code can bind a table once before writes and queries. |
| `schema_migrations` | v0 requires schema apply. Migration planning/versioning is future but must remain part of the contract vocabulary. |
| `record_writes` | Single-record put/patch/delete must share record identity, tenant identity, and field semantics across surfaces. |
| `batch_ingest` | Batch writes are first-class and should preserve `record_count`, epoch, and optional write timing where exposed. |
| `query_builder` | SDK builders should compile into the same `HybridQuery`/TraceQuery model as direct JSON calls, including preserving text/vector field selection as `text_field` and `vector_field` instead of treating SDK field arguments as placeholders. They also canonicalize `strict`, `lazy`, and `allow_dirty` freshness inputs to the `Strict`, `Lazy`, and `AllowDirty` wire modes used by direct HTTP, TraceQL, native GraphQL, and the bounded GraphQL adapter. |
| `traceql_string_execution` | Native TraceQL strings, TraceDB command statements, and the bounded SQL-ish `SELECT * FROM ... WHERE tenant_id = ... [AND field = value]* [LIMIT n]` adapter execute through `POST /v1/traceql` after compiling into TraceDB's operation/query model; neither form is a separate SQL engine. |
| `result_envelope` | Success responses are route-specific JSON; errors preserve the current `{ "error": string, "code"?: string }` envelope plus SDK context. |
| `explain_provenance_freshness_jobs` | Query/explain surfaces share `HybridExplain` fields for access paths, planner candidates, counters, timings, and freshness/provenance evidence as they mature. |
| `errors_retries_idempotency` | Safe retries stay read-only. Mutation/admin retries require caller-provided `Idempotency-Key`; same key/body replays, body mismatch returns `409`. |
| `pagination_cursors` | Scan/query pagination returns `next_cursor` when additional results exist. Production routes can use opaque signed actor/query-bound cursors when cursor signing is configured, with legacy offset parsing retained for compatibility. |
| `admin_compact_snapshot_restore` | Compact, snapshot, restore, and admin-jobs visibility are part of the platform path, with local filesystem boundaries explicit. |

## Current Surface Matrix

| Surface | ID | Current status | Contract role |
| --- | --- | --- | --- |
| HTTP direct | `http_direct` | Current | Canonical wire contract. |
| Rust SDK | `rust_sdk` | Reference candidate with env config and row batch ingestion | Ergonomic reference SDK over the wire contract while preserving raw HTTP methods. |
| TypeScript SDK | `typescript_sdk` | Public wrapper conformance checked with raw-contract and row batch ingestion, env config, safe retries, and idempotency retries | Hand-written `TraceDB` table/query wrapper over the generated transport. |
| Python SDK | `python_sdk` | Sync HTTP smoked from installed package with row batch ingestion, native TraceQL, safe retries, and idempotency retries | Sync-first AI/data/notebook SDK over the canonical HTTP contract. |
| TraceQL | `traceql` | Native TraceDB command/query statements checked for every blocking scenario | Native command/query surface over the canonical operation model. |
| TraceQL / SQL-ish | `traceql_sqlish` | Bounded SQL-ish `SELECT` adapter checked as compatibility lane | Adapter into the same TraceQuery/query model, not SQL compatibility. |
| GraphQL | `graphql` | Native `POST /v1/graphql` data/errors surface checked for every blocking scenario | Native production API over the canonical operation model. |

Maintenance mode means a platform project can use TraceDB through Rust, TypeScript, Python, TraceQL/SQL-ish, or GraphQL and receive the same behavior, same errors, same result shape, and same explain/freshness semantics.

## Conformance Harness v0

The first harness should use the machine-readable manifest as the shared
scenario list. Each surface reports pass/fail/skipped for the same scenario IDs
and must not invent surface-specific semantics.

| Scenario | ID | Current wire path | Required behavior |
| --- | --- | --- | --- |
| Schema apply | `schema_apply` | `POST /v1/schema/apply` | Applies `TableSchema` and returns an epoch. Schema validation rejects non-GraphQL-safe identifiers, duplicate columns, overlapping scalar/text/vector columns, reserved TraceDB result metadata fields, and invalid vector source columns before WAL append. |
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
| Snapshot/restore | `snapshot_restore` | `POST /v1/admin/snapshot`, `POST /v1/admin/restore` | Snapshot local state, restore into a separate target, and optionally verify a restored record with `verify_record`. |

Future harness lanes: `pagination_cursors` and `durable_jobs`. These stay out
of the v0 pass/fail contract until the engine exposes concrete behavior.

Run the current executable lanes with:

```bash
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/platform_conformance.py --surface traceql --summary-json /tmp/tracedb-traceql-conformance.json
python3 scripts/platform_conformance.py --surface traceql_sqlish --summary-json /tmp/tracedb-traceql-sqlish-conformance.json
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
```

The `http_direct` lane uses raw stdlib HTTP requests against `tracedb-server`
and now checks all 13 current v0 scenario IDs, including native
`traceql_string_execution` through `POST /v1/traceql`. The `rust_sdk` lane maps
the existing Rust SDK quickstart product path into the same manifest scenario
IDs and now checks all 13 current IDs, including `traceql_string_execution`
through `TraceDbClient::traceql_typed`; the quickstart batch scenario now uses
the table-handle `insert_rows` helper while still posting the canonical
`/v1/records/put-batch` body. The `typescript_sdk` lane runs the public
TypeScript SDK smoke through `npm run public-http-smoke --
--summary-json ...` and maps schema apply, put, batch, patch, get, scan, query,
TraceQL string execution, explain, delete, idempotency, errors, and
snapshot/restore into the same scenario IDs. The `python_sdk` lane first
installs a copied `clients/python` package into an isolated temporary pip
`--target`, then runs `clients/python/http_smoke.py` with source-path imports
disabled. It maps schema apply, put, batch, patch, get, scan, query, explain,
TraceQL string execution, delete, idempotency, errors, and snapshot/restore
into the same scenario IDs through the installed sync SDK, and the smoke also
checks native GraphQL `data`/`errors` result/explain calls through
`TraceDB.graphql()`. The `traceql` lane starts the HTTP server and exercises
native TraceDB command statements for all 13 scenario IDs. The `graphql` lane
starts the HTTP server and exercises native GraphQL operations for all 13
scenario IDs. The `traceql_sqlish` lane starts the HTTP server, seeds minimal records through
canonical wire calls, and checks the bounded SQL-ish adapter through
`/v1/traceql`. It reports
`query`, `traceql_string_execution`, `explain`, and `errors` as passed, while
schema/write/admin scenarios remain explicit `not_checked` results. Future
surfaces must report unimplemented scenarios as `not_checked` rather than
silently treating them as success.
The bounded GraphQL compatibility lane remains available through
`POST /v1/graphql/bounded`, but it is no longer the production GraphQL gate.

Current verified checkpoint: Modal workspace run `ap-YBjqjv9hV5dHkVb2AgJSud`
passed 20/20 commands in 96.9s. Its `platform-conformance-quick` command
reported `http_direct` 13/13 and `rust_sdk` 13/13, including
`traceql_string_execution`; its `typescript-sdk-conformance` command reported
`typescript_sdk` 13/13; and its `python-sdk-conformance` command reported
`python_sdk` 13/13 with native TraceQL covered by installed-package smoke result
and explain evidence. Its `python-sdk-conformance` command also exercised the
installed Python SDK's generated GraphQL SDL export through
`TraceDB.graphql_schema()` plus GraphQL result/explain calls through
`TraceDB.graphql()`. Its `traceql-sqlish-conformance` command reported
`traceql_sqlish` as `ok: true`, `complete: false`, with 4/13 scenarios passed
and 9/13 intentionally `not_checked`. The current API parity branch promotes
native `traceql` and native `graphql` to 13/13 conformance lanes; bounded
GraphQL and SQL-ish SELECT remain compatibility evidence. Its
`typescript-npm-public-http-smoke` command exercised the TypeScript public SDK
through schema/write/read/query/admin flows plus `TraceDB.graphqlSchema()` over
real HTTP, reporting `graphql_schema_export: true`, `docs` as the exported
GraphQL schema table, `type DocsRow` as a schema token, native TraceQL, and
GraphQL result/explain calls. Its
`cargo test --workspace --all-targets` command included the generated GraphQL
SDL unit test, HTTP GraphQL schema export test, Rust SDK `GraphQlQueryRequest`,
sync `graphql_typed`, safe retry, async `graphql_typed`, and generated schema
helper coverage; the Rust SDK `http_client` suite reported 49/49 tests passed,
including `graphql_schema_typed_gets_generated_schema_response`,
`graphql_schema_typed_retries_transient_read_failures_when_safe_retries_enabled`,
and `async_client_graphql_schema_typed_gets_generated_schema_response`.

The Rust SDK also has a first ergonomic reference layer over the same wire
contract: `TraceDb::connect(config)?` returns the reference client, and
`db.table("docs").tenant("tenant-a")` returns a `TableHandle`. Handles can
execute table insert, batch insert, patch, get, scan, and delete calls, then
enter the query builder with `query()` or the direct chaining helpers
`where_eq`, `match_text`, `near`, `with_explain`, `limit`, `all()`, and
`explain_plan()`. `TraceDbClient::traceql_typed` and `traceql_request_typed`
send native TraceQL strings to `POST /v1/traceql` and decode the same
`QueryResponse` envelope as `query_typed`. `TraceDbClient::graphql_schema`,
`TraceDbClient::graphql_schema_typed`, and
`TraceDbAsyncClient::graphql_schema_typed` read the generated SDL from
`GET /v1/graphql/schema` and decode the same `GraphQlSchemaResponse` contract
as HTTP direct. `TraceDbClient::graphql_typed` and `graphql_request_typed` do
the same for the bounded `POST /v1/graphql` adapter with
`GraphQlQueryRequest`. These helpers compile into or reuse the existing
`RecordInput`, `RecordPutBatchRequest`, record request, `TraceQlQueryRequest`,
`GraphQlQueryRequest`, `GraphQlSchemaResponse`, and `HybridQuery` models; the
raw HTTP methods remain available. `GraphQlSchemaResponse.execution` is the
canonical wire field for the execution note; the Rust SDK accepts the older
`execution_caveat` spelling as a backward-compatible alias but emits the current
field in typed examples and tests.

The TypeScript public SDK mirrors the generated GraphQL schema route through
`TraceDB.graphqlSchema()` over the generated transport's `/v1/graphql/schema`
method, then mirrors the same bounded GraphQL wire route through
`TraceDB.graphql(query)` and `graphqlRequest({ query })` over the generated
transport's `/v1/graphql` method. This is public SDK access to generated SDL
export and the bounded adapter, not GraphQL mutation support, resolver runtime,
GraphQL data-envelope execution, or full GraphQL adapter parity.

The Python sync SDK mirrors generated GraphQL SDL export through
`TraceDB.graphql_schema()` over `GET /v1/graphql/schema`, then mirrors the same
bounded GraphQL wire route through `TraceDB.graphql(query)` and
`graphql_request({"query": query})` over `POST /v1/graphql`, reusing the same
dictionary-shaped `GraphQlSchemaResponse`, `GraphQlQueryRequest`, and
`QueryResponse` envelopes as the raw HTTP contract. This is sync SDK access to
generated SDL export and the bounded adapter, not async Python support, GraphQL
mutation support, resolver runtime, GraphQL data-envelope execution, or full
GraphQL adapter parity.
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
`insert`, raw-contract `insertBatch`, row-oriented `insertRows`, `patch`, `get`,
`scan`, `delete`, admin compact/snapshot/restore/jobs, `where`, `match`, `near`,
`with`, `limit`, `all`, `explainPlan`, `traceql`, and `traceqlRequest`.
`TraceDB.fromEnv()` reads
`TRACEDB_URL`, optional `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`,
`TRACEDB_BRANCH_ID`, and `TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES` so the TypeScript public SDK shares the same
connection, routing, read-only retry, and keyed mutation/admin retry boundary as
Rust. `safeRetries` only retries transient 5xx responses for health/ready, get,
scan, query, native TraceQL, bounded GraphQL, and explain. `idempotencyRetries`
is default-off and retries transient 5xx responses for mutation/admin routes
only when the request carries a caller-provided `Idempotency-Key`. The wrapper is fake-fetch,
build/pack, packed temp-consumer install, package-entry, and typecheck guarded
and now has real local HTTP and gateway smokes through `npm run
public-http-smoke` and `npm run gateway-smoke`.
The public HTTP smoke now emits machine-readable idempotency, TraceQL
result/explain, bounded GraphQL result/explain, raw-contract batch ingestion,
row batch ingestion, and error-envelope evidence for
`scripts/platform_conformance.py --surface typescript_sdk`; the generated
transport remains available and remains the source of route methods.

The Rust SDK reference layer now exposes row-oriented table ingestion alongside
the raw typed methods. `TraceDbClient::table("docs").tenant("tenant-a")` and
`TraceDb::connect(config)?.table("docs").tenant("tenant-a")` expose
`insert_rows`, `insert_rows_with_options`, `insert_rows_with_id_field`, and
`insert_rows_with_id_field_and_options`. These helpers accept normal
`serde_json::Map` row dictionaries, copy them into the existing
`RecordPutBatchRequest` shape, inject `tenant` and `id` fields consistently with
raw `insert_batch`, and preserve `Idempotency-Key` behavior through
`TraceDbRequestOptions`.

The Python package now starts the sync-first AI/data SDK lane in
`clients/python/tracedb/client.py`. `TraceDB(url, token="dev-token")` exposes
table handles and a query builder with `insert`, raw-contract `insert_batch`,
row-oriented `insert_rows`, `patch`, `get`, `scan`, `delete`, `where`,
`match_text`, `near`, `with_options`, `limit`,
`all`, and `explain_plan`, plus health/catalog/metrics/admin helpers. The
stdlib-only SDK also exposes `TraceDB.from_env()` for `TRACEDB_URL`,
`TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`,
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES`. `safe_retries` only retries transient 5xx
responses for health, ready, get, scan, query, native TraceQL, bounded GraphQL,
and explain.
`idempotency_retries` is default-off and retries transient 5xx responses for
mutation/admin routes only when that request carries a caller-provided
`Idempotency-Key`; unkeyed writes and 4xx/conflict responses are not retried.
`TraceDB.traceql(query)` and `traceql_request({"query": query})` execute native
TraceQL strings through the canonical `POST /v1/traceql` route.
`TraceDB.graphql(query)` and `graphql_request({"query": query})` execute bounded
GraphQL query-adapter strings through the canonical `POST /v1/graphql` route.
`insert_rows` is intentionally SDK-side ergonomics for AI/data rows; it copies
row dictionaries into the existing `POST /v1/records/put-batch` request shape
and supports `id_field` for custom row ids.
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
readiness, async support, managed-cloud proof, SQL compatibility, or full
GraphQL adapter parity. The smoke is also promoted into the local product gate as
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
compatibility, GraphQL execution/runtime evidence, or a separate query engine.
`MATCH <field> "..."` preserves `<field>` as `HybridQuery.text_field`; `NEAR
<field> [...]` preserves `<field>` as `HybridQuery.vector_field`.

The same `/v1/traceql` parser now accepts a deliberately bounded SQL-ish form:
`EXPLAIN? SELECT * FROM <table> WHERE tenant_id = <value> [AND field = value]*
[LIMIT n]`. It maps `tenant_id`/`tenant` to `HybridQuery.tenant_id`, additional
equality predicates to `scalar_eq`, `LIMIT` to `top_k`, and `EXPLAIN` to the
shared explain flag. Unsupported SQL constructs such as `JOIN`, `GROUP`,
`ORDER`, `UNION`, mutation DDL/DML, and PostgreSQL-specific behavior fail as
`invalid SQL-ish` bad-request responses rather than implying compatibility.

The query crate exposes `graphql_schema_sdl_from_tables` to generate SDL from
applied `TableSchema` definitions and the bounded GraphQL adapter compiler
primitive `graphql_query_from_str`. The HTTP server exposes the generated SDL
through `GET /v1/graphql/schema` with `GraphQlSchemaResponse`, including the
adapter marker, table names, and the `execution` note that `POST /v1/graphql`
returns TraceDB's `QueryResponse` rather than a GraphQL data envelope.
`graphql_query_from_str` accepts one root selection whose field name is the
table, with arguments such as `tenant_id`, `where`/`filter`, `match`/`text`,
`near`/`vector`, `limit`, `freshness`, and `explain`, then compiles directly
into `HybridQuery`. The HTTP server exposes that path through
`POST /v1/graphql` with `GraphQlQueryRequest`, returning the same
`QueryResponse` shape as `/v1/query`. The Rust SDK mirrors the schema route
with `TraceDbClient::graphql_schema`, `TraceDbClient::graphql_schema_typed`,
and `TraceDbAsyncClient::graphql_schema_typed`, and mirrors the bounded query
route with `GraphQlQueryRequest`, `TraceDbClient::graphql_typed`, and
`TraceDbAsyncClient::graphql_typed`; the TypeScript SDK mirrors schema export
with `TraceDB.graphqlSchema()` and bounded execution with `TraceDB.graphql()`
and `TraceDB.graphqlRequest()`; the Python SDK mirrors schema export with
`TraceDB.graphql_schema()` and native execution with `TraceDB.graphql()` and
`graphql_request({"query": query})`. Bounded adapter execution is explicit
through `/v1/graphql/bounded` helpers. Subscriptions remain unsupported.

## Surface Implementation Rules

- HTTP direct remains the source of wire truth.
- Rust SDK is the reference ergonomic SDK. It may add table handles and builder
  APIs, but raw HTTP methods must remain available.
- TypeScript's generated client is a transport layer, not the final public
  product shape.
- Python starts sync-first for ingestion, AI workflows, notebooks, and platform
  conformance tests. Async can follow after sync parity.
- TraceQL / SQL-ish must parse into the same query model. Do not claim SQL
  compatibility or PostgreSQL compatibility. The current HTTP surface is
  `/v1/traceql`; the SQL-ish surface is limited to the checked bounded
  `SELECT` adapter until a broader TraceQL adapter is explicitly designed.
- GraphQL schema export is generated from TraceDB schema, native execution uses
  GraphQL `data`/`errors`, and all operations still compile into TraceDB's
  canonical operation/query model. The bounded `graphql_query_from_str`
  adapter lives at `POST /v1/graphql/bounded` for compatibility only.

## Verification Ladder

Use the smallest ladder that proves the touched surface:

```bash
cargo test -p tracedb-testkit --test usability_acceptance platform_contract_v0_declares_sdk_conformance_harness -- --exact
cargo test -p tracedb-query graphql_schema_sdl_is_generated_from_table_schema --no-run
cargo test -p tracedb-testkit --test usability_acceptance http_graphql_endpoint_executes_bounded_query_through_hybrid_query --no-run
cargo test -p tracedb-testkit --test usability_acceptance http_graphql_schema_exports_sdl_from_applied_table_schema --no-run
cargo test -p tracedb-sdk --test http_client graphql_schema_typed_gets_generated_schema_response --no-run
cargo test -p tracedb-sdk --test http_client graphql_schema_typed_retries_transient_read_failures_when_safe_retries_enabled --no-run
cargo test -p tracedb-sdk --test http_client graphql_request_typed_posts_native_query_string --no-run
cargo test -p tracedb-sdk --test http_client graphql_typed_retries_transient_read_failures_when_safe_retries_enabled --no-run
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 -m unittest discover -s clients/python/tests
python3 clients/python/install_smoke.py
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/generate_openapi_v1.py --check
python3 scripts/generate_typescript_client.py --check
cargo run -p tracedb-cli -- product-quickstart --skip-typescript
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
modal run scripts/modal_product_verify.py --mode workspace --only typescript_gateway_smoke --summary-json /tmp/tracedb-modal-gateway-smoke.json
```

Local macOS executable-policy failures are local environment evidence, not a
contract failure. Use Modal for remote Linux product verification when the local
machine cannot execute Rust binaries.
