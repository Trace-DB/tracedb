---
title: TraceDB
aliases:
  - TraceDB Home
tags:
  - tracedb
  - docs
status: stub
type: repo-handoff
updated: 2026-05-19
---

# TraceDB

Canonical TraceDB architecture, strategy, benchmark, implementation, operations, and decision notes now live in the Grogan Development Vault:

```text
/Users/zgrogan/Repos/grogan-development-vault/10_Projects/TraceField Suite/TraceDB/
```

This repo-local file is intentionally a stub so code, tests, and handoff paths do not depend on the vault checkout.

## Local Product Smoke

Run from the repo root:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

The demo exercises schema apply, batch ingest, query/explain, scan, delete,
compact, snapshot, and restore through the embedded engine. SQL compatibility is
not implemented.

The consolidated local product regression gate is:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
```

It emits one machine-readable `local-product-regression` summary for the
embedded demo/verify path, `http-demo`, local `doctor http`, Rust SDK
quickstart, Python sync SDK smoke, and generated TypeScript check/http/gateway
smoke paths, with a compact top-level `human_summary` for quick operator
scanning. It is local
product regression evidence only: SQL remains not implemented, managed-cloud is
not checked, and benchmarks are not checked. The local product regression gate
also has test-only `--inject-failure STEP` coverage for nonzero process status
and machine-readable failed-step JSON. Use
`--report-file PATH` to write the same product-regression JSON summary to a
predictable file while preserving JSON stdout; this applies to full runs,
`--only`, `--inject-failure`, and `--list-steps`, and creates parent
directories. Use
`--list-steps` to print the machine-readable list of product-regression step
names with `human_summary` and `only_supported` metadata for operators and CI
wiring without executing any product checks. `--skip-typescript` is for the full
product gate and non-TypeScript selectors; a TypeScript `--only` selector
conflicts with --skip-typescript. `product-quickstart` runs the same local
product gate with a default report file at
`target/tracedb/product-quickstart.json`; it accepts the same
product-regression options, still writes JSON to stdout, and includes the
resolved artifact path in the top-level `report_file` field. Treat that artifact
as the local quickstart receipt: it should report `ok: true`, `mode:
"local-product-regression"`, `scope: "local_only"`, `human_summary.status:
"passed"`, `claims.sql_module: "not_implemented"`,
`claims.managed_cloud: "not_checked"`, and `claims.benchmark: "not_checked"`.
`product-quickstart --skip-typescript` is the reduced fallback receipt for
machines without Node tooling: it still writes
`target/tracedb/product-quickstart.json`, keeps `report_file`, reports
`typescript_enabled: false`, passes the six non-TypeScript local steps including
`python_sdk_smoke`, and omits `typescript_check`, `typescript_http_smoke`, and
`typescript_gateway_smoke`. Treat it as a reduced local evidence path, not the
full product gate.
`product-quickstart --inject-failure embedded_demo` is the quick failure receipt
check: it exits nonzero, writes the same default report artifact, keeps
`report_file`, reports `human_summary.status: "failed"`, and records
`failure_injection: "embedded_demo"` with an injected failed `embedded_demo`
step.
`--only embedded_demo` currently runs just the embedded demo step and emits the
normal one-step product-regression JSON summary. `--only embedded_verify`
verifies an existing embedded demo data root, usually with the same
`--data-root` used by `--only embedded_demo`. `--only http_demo` runs the
self-contained local HTTP demo step and emits the normal one-step
`local-product-regression` JSON summary. It does not run local `doctor http`,
the Rust SDK quickstart, generated TypeScript smoke steps, managed-cloud
checks, benchmark controls, or SQL compatibility checks.
`--only local_doctor` starts a managed-style local loopback `tracedb-server`
child process and runs only the existing local `doctor http` product-regression
step with readiness wait, `database_id`, and `branch_id` metadata. It emits the
normal one-step `local-product-regression` JSON summary with `only_step:
"local_doctor"`. This is read-only local endpoint diagnostics against a managed
local server, not a mutating product smoke, not full local product gate
evidence, not managed-cloud proof, not benchmark evidence, and not SQL
compatibility.
`--only rust_sdk_quickstart` starts a managed-style local loopback
`tracedb-server`, creates/uses the quickstart admin dir, runs only the existing
Rust SDK quickstart product-regression step, and emits one-step
`local-product-regression` JSON with `only_step: "rust_sdk_quickstart"`. This
is local Rust SDK quickstart evidence only, not full product gate coverage, not
`http_demo`, not local `doctor http` diagnostics, not generated TypeScript
smoke, not managed-cloud proof, not benchmark evidence, and not SQL
compatibility.
Failed Rust SDK child runs that still emit quickstart JSON are preserved under
`steps.rust_sdk_quickstart.summary`, with stdout/stderr tails retained on the
failed step.
`--only python_sdk_smoke` runs only `python3 clients/python/http_smoke.py` from
the workspace root. The smoke starts its own local `tracedb-server` child
process and exercises the sync Python SDK through ready, catalog, schema apply,
insert, batch ingest, patch, get, scan, query, explain, delete, idempotency,
error envelopes, compact, snapshot, restore, and jobs. It emits one-step
`local-product-regression` JSON with `only_step: "python_sdk_smoke"`. This is
local sync Python SDK HTTP smoke evidence only, not full product gate coverage,
not embedded demo/verify, not `http_demo`, not local `doctor http`
diagnostics, not Rust SDK quickstart evidence, not TypeScript smoke, not
managed-cloud proof, not benchmark evidence, and not SQL compatibility.
`--only typescript_check` runs only `(cd clients/typescript && npm run check)`,
which currently performs the package typecheck plus dependency-free
generated-client, public SDK, package build, package-entry smoke, and pack
dry-run checks, and emits one-step
`local-product-regression` JSON with `only_step: "typescript_check"`. This is
TypeScript package boundary evidence only, not full product gate coverage, not
`http_demo`, not local `doctor http` diagnostics, not Rust SDK quickstart
evidence, not TypeScript HTTP smoke, not TypeScript gateway smoke, not
managed-cloud proof, not benchmark evidence, and not SQL compatibility.
`--only typescript_http_smoke` runs only `(cd clients/typescript && npm run
public-http-smoke)`, which starts its own local `tracedb-server` child process
and exercises the public TypeScript SDK wrapper over the generated transport,
and emits one-step `local-product-regression` JSON with `only_step:
"typescript_http_smoke"`. The smoke now includes idempotency replay/conflict
and parsed error-envelope evidence for the shared conformance harness. This is
local public TypeScript SDK HTTP smoke evidence only, not full product
gate coverage, not embedded demo/verify, not `http_demo`, not local
`doctor http` diagnostics, not Rust SDK quickstart evidence, not
`typescript_check`, not generated-transport `http-smoke`, not TypeScript gateway
smoke, not managed-cloud proof, not benchmark evidence, and not SQL
compatibility.
`--only typescript_gateway_smoke` runs only `(cd clients/typescript && npm run
gateway-smoke)`, which starts a local engine plus gateway-mode
`tracedb-server`, requires bearer auth, checks missing-token and bad-branch
rejection, and runs the public TypeScript SDK wrapper through the gateway with
managed routing metadata plus a local admin scratch dir. It emits
one-step `local-product-regression` JSON with `only_step:
"typescript_gateway_smoke"`. This is local public TypeScript SDK gateway
auth/routing evidence only, not full product gate coverage, not embedded
demo/verify, not `http_demo`, not local `doctor http` diagnostics, not Rust SDK
quickstart evidence, not `typescript_check`, not TypeScript HTTP smoke, not
managed-cloud proof, not benchmark evidence, and not SQL compatibility.

Current platform conformance checkpoint: `scripts/platform_conformance.py` maps
HTTP direct, Rust SDK, TypeScript SDK, and Python SDK into all 13 Platform
Contract v0 scenario IDs, plus partial TraceQL/SQL-ish and GraphQL adapter
lanes. Modal workspace run `ap-RPGPKFDjFK13bpAOn4x9m0` passed 20/20 commands in
93.2s, including `platform-conformance-quick`,
`traceql-sqlish-conformance`, `graphql-http-conformance`,
`typescript-sdk-conformance`, `python-sdk-conformance`, Python unit/install
smokes, TypeScript package/HTTP/gateway lanes, TypeScript public SDK GraphQL
schema export evidence, TypeScript and Python public SDK GraphQL result/explain
smoke coverage, and
`cargo test --workspace --all-targets`. The GraphQL lane reported schema apply,
query, explain, and error behavior as passed with 9/13 scenarios intentionally
`not_checked`, and the workspace tests included the generated GraphQL SDL unit
test, HTTP GraphQL schema export test, Rust SDK `GraphQlQueryRequest`,
sync/async `graphql_typed`, generated schema helper, and GraphQL safe retry
coverage; the TypeScript public SDK smoke now also checks
`TraceDB.graphqlSchema()` against the generated SDL route. The Rust SDK
`http_client` suite reported 49/49 passed. This is platform conformance
evidence, not managed-cloud proof, SQL
compatibility, full GraphQL adapter parity, or benchmark evidence.

The local HTTP plus SDK smoke is also available as one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

It starts a loopback `tracedb-server` child process, drives the Rust SDK over
HTTP through ready, schema apply, batch ingest, scan, query, explain, delete,
compact, snapshot, and restore, uses keyed SDK write/admin retries for
mutation/admin steps, and reports `sql_module: not_implemented`. This proves the
local HTTP SDK product path; it does not claim managed-cloud deployment health,
backup/DR semantics, cross-replica idempotency, or SQL compatibility.

The SDK quickstart is also runnable against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo/data TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000 --safe-retries 1 --idempotency-retries 1 --admin-dir /tmp/tracedb-sdk-demo/admin
```

Endpoint diagnostics are available without mutating data:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --timeout-ms 1000 --safe-retries 1 --wait-ready-ms 5000 --database-id db_local --branch-id db_local:main
```

The HTTP doctor checks health, readiness, catalog, public-safe metrics, and
admin-jobs routes and reports `sql_module: not_implemented`. Failed checks
include parsed `server_error` and `server_error_code` fields when an endpoint
returns the current coded JSON error shape. Optional `--database-id` and
`--branch-id` add managed-routing metadata for gateway diagnostics, including
the bodyless admin-jobs route. Optional `--wait-ready-ms` polls readiness before
the normal checks and reports the readiness wait in the JSON summary. The
command exits non-zero when any check fails while keeping the JSON summary on
stdout. It is a local/managed-style endpoint diagnostic, not a SQL probe,
benchmark, or managed deployment proof.

For CI or deployed endpoint checks, the doctor can read the same endpoint
configuration from `TRACEDB_URL`, `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`,
`TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_WAIT_READY_MS`:

```bash
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 TRACEDB_WAIT_READY_MS=5000 cargo run -p tracedb-cli -- doctor http
```

The SDK quickstart uses typed convenience response methods, including typed
health, catalog, metrics, admin-jobs, single-record put, batch ingest, patch,
and query rows, over the current HTTP JSON shapes. It accepts `--timeout-ms` for
the blocking SDK request timeout, `--safe-retries` for bounded health/read route
retries, `--idempotency-retries` / `TRACEDB_IDEMPOTENCY_RETRIES` for
idempotency-key-gated write/admin retries, and optional `--admin-dir` to run
compact/snapshot/restore against an absolute server-side local scratch
directory. The quickstart checks diagnostics before the write path, writes one
record through typed `put`, batch-ingests two more records, patches a record,
verifies patched visibility before scan/query/delete, and generates per-run
idempotency keys for those mutations when idempotency retries are enabled. It
also intentionally exercises one non-2xx server response and reports the parsed
`error_envelope` so SDK error behavior is conformance-visible. The admin path is
interpreted by the
`tracedb-server` process, and restore creates a separate database directory
instead of replacing the running server.
`TraceDbRequestOptions` can manually attach `Idempotency-Key` to individual
write/admin requests. The SDK also exposes typed local admin helpers for
compact, snapshot, and restore. The quickstart JSON summary now uses the same
operator-facing envelope shape as the TypeScript endpoint quickstart: `mode`,
`server_url`, optional `database_id` / `branch_id`, `table`, `tenant_id`, and a
structured `admin` object for requested/skipped compact, snapshot, and restore.
Invalid quickstart configuration, including malformed URLs, invalid retry counts,
and non-absolute admin scratch paths, exits nonzero but still emits a parseable
`ok: false` JSON summary on stdout with `error.kind`, `error.message`, false
step statuses, and `sql_module: not_implemented`.
SQL compatibility remains unimplemented.

The current versioned HTTP API route reference is `docs/api/v1-http.md`. The
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`, generated by
`scripts/generate_openapi_v1.py`.
The Rust SDK exposes `TraceDbAsyncClient` as a first async facade over the
current HTTP API. It uses a background thread per request and preserves the
blocking client's timeout, retry, managed-routing, and error semantics. It now
exposes async typed write/admin helpers for schema apply, record
put/batch/patch/delete, compact, snapshot, and restore, including option-aware
idempotency helpers. This is an async integration surface for the current
product path, not a final runtime-native Tokio/async-std transport.
The blocking Rust SDK now also exposes a first ergonomic reference layer through
`TraceDb::connect(config)?` and `db.table("docs").tenant("tenant-a")`. That
`TableHandle` can insert single records, batch insert records, patch records,
get records, scan, delete, enter query mode with `query()`, add scalar equality
predicates, add text/vector query clauses, request explain output, set a limit,
execute `all()` against `/v1/query`, and execute `explain_plan()` against
`/v1/explain` using the canonical `HybridQuery` shape.
`TraceDbClientConfig::from_env()` now builds Rust SDK
connection config from `TRACEDB_URL`, optional `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`,
`TRACEDB_SAFE_RETRIES`, and `TRACEDB_IDEMPOTENCY_RETRIES`. Raw HTTP methods
remain available.
The checked generated TypeScript `fetch` client artifact is
`clients/typescript/src/client.ts`, generated by
`scripts/generate_typescript_client.py` from the OpenAPI artifact. It covers the
current v1 HTTP routes, can add configured `database_id` and `branch_id` to
copied JSON POST bodies when absent, and can send caller-supplied
`Idempotency-Key` through `TraceDbRequestOptions`. It now includes
OpenAPI-derived schema aliases and typed method signatures for the current HTTP
surface while preserving the permissive `additionalProperties` boundary: known
fields are optional, unknown JSON fields remain allowed, and runtime validation
stays server-side. The generated `RecordPutBody` alias matches the current
server route by allowing either direct `RecordInput` or `{ record: RecordInput
}`, and `GetRecordResponse.record` is now typed as `RecordOutput | null` with
the serialized `version_id` field. `HybridQuery` now explicitly includes
`scalar_eq`, `graph_seed`, and `temporal_as_of` request fields. `RecordScanOutput`,
`QueryResponse`, `HybridQueryRow`, `HybridScoreComponents`, and `HybridExplain`
now expose the current server response shape for scan/query/explain, including
access-path, candidate, counter, and timing explain fields. Health, readiness,
catalog, metrics, and admin-jobs responses now have concrete generated aliases
as well; fields stay optional where local-engine and gateway shapes differ. It
is not a published npm package, not a hand-maintained managed SDK, not a strict
runtime validator, and not a SQL compatibility claim.
The TypeScript package now also starts the public platform SDK layer at
`clients/typescript/src/sdk.ts`. `TraceDB` wraps the generated `TraceDbClient`
transport and exposes `TraceDB.fromEnv()` for `TRACEDB_URL`, optional
`TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, and
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES`, plus table handles for single insert, batch
insert, patch, get, scan, delete, admin compact/snapshot/restore/jobs, and
query-builder chaining through `where({ tenant_id })`, `match`, `near`, `with`,
`limit`, `all`, and `explainPlan`. The public wrapper retries transient 5xx
responses only for health/ready, get, scan, query, and explain through
`safeRetries`; keyed mutation/admin retry is default-off through
`idempotencyRetries` and only applies when the individual request carries a
caller-provided idempotency key.
`npm run public-smoke` verifies this wrapper with a fake transport, env config,
safe retry behavior, idempotency retry behavior, and missing-tenant request validation; `npm run public-http-smoke`
verifies it against a real local `tracedb-server` with idempotency and
error-envelope evidence, and `scripts/platform_conformance.py --surface
typescript_sdk` now maps it into the shared Platform Contract v0 scenario IDs.
This is public-DX conformance evidence, not npm publishing readiness or
managed-cloud proof.
`node --experimental-strip-types clients/typescript/smoke.ts` verifies the
artifact imports and executes in the local Node runtime with fake-fetch coverage
for representative generated aliases, GET no-body behavior, POST routing
metadata injection, explicit routing field precedence, idempotency headers, and
HTTP error shape, including parsed `{ "error": string, "code"?: string }`
envelopes on `TraceDbHttpError`. Stable machine-readable error `code` values
are preserved when present. It rejects empty or CR/LF-containing
`idempotencyKey` request options before `fetchImpl` as `TraceDbRequestError`.
`cd clients/typescript && npm ci && npm run check` installs the locked package
tooling, typechecks `@tracedb/sdk`, and runs generated-client, public SDK, and
package build/entrypoint smokes. The package now exposes `@tracedb/sdk` from
`dist/index.js` / `dist/index.d.ts` and `@tracedb/sdk/transport` from
`dist/client.js` / `dist/client.d.ts`; `npm run pack-dry-run` proves tarball
contents without publishing, and `npm run consumer-smoke` installs the packed
tarball into a clean temporary project and imports both package entrypoints.
This is package-ready build/pack/install boundary coverage, not an npm release
or publication pipeline. `cd clients/typescript && npm run http-smoke` starts a local
`tracedb-server` child process and drives the
generated TypeScript transport over real HTTP routes; `npm run public-http-smoke`
drives the public `TraceDB` wrapper over the same generated transport through
ready, health, catalog, metrics, schema apply, insert, batch ingest, patch, get,
scan, query, explain, delete, idempotency replay/conflict, parsed error
envelopes, compact, snapshot, restore, and admin jobs.
`TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run
quickstart` runs the generated TypeScript client against an existing HTTP
endpoint through readiness, health, catalog, metrics, schema apply, batch ingest,
patch, patched visibility, scan, query, explain, delete, and admin jobs.
Optional `TRACEDB_DATABASE_ID` / `TRACEDB_BRANCH_ID` add managed routing
metadata, and optional absolute `TRACEDB_ADMIN_DIR` enables local
compact/snapshot/restore against server-side scratch paths. The quickstart
reports `sql_module: not_implemented` and is endpoint example evidence, not a
package publishing claim, SQL compatibility, managed-cloud backup/DR, or
benchmark evidence. `cd clients/typescript && npm run gateway-smoke` starts a
local engine plus gateway-mode server with `TRACEDB_REQUIRE_API_KEY=true`,
`TRACEDB_API_TOKEN=dev-token`, and `TRACEDB_ENGINE_URL` pointing at the engine,
proves missing-token `401` and bad-branch `400` enforcement, then runs the
public TypeScript `TraceDB` wrapper through the gateway with
`databaseId=db_local` and `branchId=db_local:main`. This is local gateway
auth/routing evidence for the public SDK over the generated transport, not
managed-cloud proof or benchmark evidence.
The Python SDK now starts the sync-first AI/data SDK lane under
`clients/python/tracedb`. `TraceDB(url, token="dev-token")` and
`TraceDB.from_env()` expose table
handles and a query builder with `insert`, `insert_batch`, `patch`, `get`,
`scan`, `delete`, `where`, `match_text`, `near`, `with_options`, `limit`,
`all`, and `explain_plan`, plus health/catalog/metrics/admin helpers,
managed `database_id` / `branch_id` routing metadata injection,
`Idempotency-Key` support, parsed HTTP error envelopes, and read-only
`safe_retries` for health, ready, get, scan, query, and explain.
`idempotency_retries` is default-off and retries transient 5xx responses for
mutation/admin routes only when that request carries a caller-provided
`Idempotency-Key`; unkeyed writes and 4xx/conflict responses are not retried.
The env helper reads `TRACEDB_URL`, optional `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`,
`TRACEDB_SAFE_RETRIES`, and `TRACEDB_IDEMPOTENCY_RETRIES`;
`python3 -m unittest discover -s clients/python/tests` now checks the local
package shape and config helper.
`python3 clients/python/install_smoke.py` prefers a temporary venv, installs
`clients/python`, and runs a consumer from outside the repo so packaging drift
is visible; on images without working `ensurepip`, it falls back to an isolated
temporary pip `--target` install. The Python conformance lane now installs a
copied package into an isolated temporary pip `--target` before running
`clients/python/http_smoke.py` with source-path imports disabled. Modal
workspace verification runs both package lanes before Python conformance.
`python3 clients/python/http_smoke.py --summary-json
/tmp/tracedb-python-sdk-smoke.json` starts a local `tracedb-server` and drives
the Python surface through schema apply, put, batch ingest, patch, get, scan,
query, explain, delete, idempotency replay/conflict, error-envelope parsing,
compact, snapshot, restore, and admin jobs. `python3
scripts/platform_conformance.py --surface python_sdk --summary-json
/tmp/tracedb-python-sdk-conformance.json` maps that smoke into all required v0
contract scenarios. This is sync SDK contract evidence, not PyPI readiness,
async support, managed-cloud proof, SQL compatibility, or full GraphQL adapter
parity.

Native TraceQL now executes through the canonical HTTP surface:
`POST /v1/traceql` accepts `{ "query": string }`, parses line-oriented `FROM`,
`TENANT`, `WHERE`, `MATCH`, `NEAR`, `FRESHNESS`, `LIMIT`, and `EXPLAIN`
directives with `traceql_query_from_str`, and compiles them into the existing
`HybridQuery` model before returning the same result shape as `POST /v1/query`.
The same parser also accepts the bounded SQL-ish adapter form
`EXPLAIN? SELECT * FROM <table> WHERE tenant_id = <value> [AND field = value]*
[LIMIT n]`, compiling it into the same `HybridQuery` path and returning
`invalid SQL-ish` bad-request errors for unsupported constructs such as `JOIN`.
The conformance harness now includes `traceql_string_execution`; HTTP direct
passes that scenario through `/v1/traceql`, the Rust SDK lane passes it through
`TraceDbClient::traceql_typed`, the TypeScript SDK lane passes it through
`TraceDB.traceql()`, and the Python SDK lane passes it through the sync
`TraceDB.traceql()` helper. It also has a dedicated partial `traceql_sqlish`
lane that checks bounded SQL-ish query, explain, and error behavior while
leaving schema/write/admin scenarios `not_checked`. This is
TraceQL/query-adapter execution evidence only; SQL compatibility and PostgreSQL
compatibility remain unimplemented. `GET /v1/graphql/schema` now exports
generated SDL from applied TraceDB table schema, and `POST /v1/graphql` exposes
a bounded GraphQL query adapter over the same `HybridQuery` model. The GraphQL
conformance lane checks schema export, query, explain, and error behavior while
leaving write/admin scenarios `not_checked`. The Rust SDK exposes the generated
schema route through `TraceDbClient::graphql_schema`,
`TraceDbClient::graphql_schema_typed`, and
`TraceDbAsyncClient::graphql_schema_typed`, then exposes the bounded adapter
through `TraceDbClient::graphql_typed`, `graphql_request_typed`, and
`GraphQlQueryRequest`. The TypeScript SDK exposes schema export through
`TraceDB.graphqlSchema()` and bounded execution through `TraceDB.graphql()` and
`graphqlRequest({ query })`; the Python SDK exposes bounded execution through
`TraceDB.graphql()` and `graphql_request({"query": query})`. GraphQL
mutation support, subscription support, resolver runtime, GraphQL data-envelope
execution, and full adapter parity remain unimplemented.
Mutation and admin routes accept optional `Idempotency-Key` for local
data-dir-backed engine replay, and the gateway forwards that header. Replay
survives a clean engine reopen from the same data directory after a successful
local cache write; filesystem cache-write failures are logged and do not roll
back the original mutation. Cross-replica idempotency, crash-atomic exactly-once
semantics, and managed-cloud exactly-once guarantees remain future work. SDK
write/admin retries are opt-in, bounded, transient-only, and require an
idempotency key.
