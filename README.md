# TraceDB

TraceDB is a development-stage transactional candidate-stream database. The
current product surface is an embedded/local engine with CLI and HTTP paths.

Canonical architecture, benchmark, and roadmap notes live in the Grogan
Development vault:

```text
/Users/zgrogan/Repos/grogan-development-vault/10_Projects/TraceField Suite/TraceDB/
```

The DX-facing platform contract starts at `docs/platform-contract-v0.md`, with
the machine-readable conformance manifest in `docs/platform-contract-v0.json`.
This is the checklist future SDKs and adapters must pass before TraceDB claims
maintenance-mode parity across Rust, TypeScript, Python, TraceQL/SQL-ish, and
GraphQL.

Run the initial executable contract harness with:

```bash
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/platform_conformance.py --surface traceql_sqlish --summary-json /tmp/tracedb-traceql-sqlish-conformance.json
python3 scripts/platform_conformance.py --surface graphql --summary-json /tmp/tracedb-graphql-conformance.json
```

It reads `docs/platform-contract-v0.json`, drives a raw HTTP `http_direct` lane
against `tracedb-server`, reuses the Rust SDK quickstart product path for the
`rust_sdk` lane, maps the public TypeScript SDK HTTP smoke for the
`typescript_sdk` lane, installs the Python SDK package before running the sync
Python SDK smoke for the `python_sdk` lane,
and runs a bounded SQL-ish `/v1/traceql` adapter lane for `traceql_sqlish`.
The current HTTP direct, Rust SDK, TypeScript SDK, and Python SDK lanes cover
all required v0 scenarios. The SQL-ish lane is intentionally partial: it checks
query, TraceQL string execution, explain, and error-envelope behavior, while
schema/write/admin scenarios remain explicit `not_checked` results until that
adapter surface owns more semantics.
GraphQL has a generated SDL export from applied `TableSchema` definitions
through `GET /v1/graphql/schema`, plus a bounded `graphql_query_from_str`
compiler primitive in `tracedb-query` and a bounded `POST /v1/graphql` HTTP
adapter that compiles the query string into the same `HybridQuery` path as
`/v1/query`. The `graphql` conformance lane checks schema export, query,
explain, and error-envelope behavior, while write/admin scenarios remain
explicit `not_checked` results. This is not GraphQL mutation support,
subscription support, resolver runtime, GraphQL data-envelope execution, or
full adapter parity. Rust SDK callers can use `TraceDbClient::graphql_typed` or
`graphql_request_typed` with `GraphQlQueryRequest`, and TypeScript SDK callers
can use `TraceDB.graphql()` or `graphqlRequest({ query })`; Python SDK callers
can use `TraceDB.graphql()` or `graphql_request({"query": query})` to exercise
the same bounded wire contract.

## Quickstart

Run the local product demo from the repo root:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo demo
```

The command exercises the embedded engine path end to end:

- opens a local TraceDB data directory
- applies a demo schema
- batch-ingests records
- runs hybrid query and explain
- scans and deletes records
- compacts
- creates and restores a snapshot
- reports that SQL is not implemented

Verify the same data directory afterward:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-demo verify
```

Run the local HTTP plus SDK product smoke with one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

The command starts a local loopback `tracedb-server` child process, drives the
Rust SDK over HTTP through ready, schema apply, batch ingest, scan, query,
explain, delete, compact, snapshot, and restore, then emits a JSON summary. It
uses keyed SDK write/admin retries for its mutation/admin steps and still
reports `sql_module: not_implemented`. This is local product-path evidence, not
managed-cloud deployment or backup/DR evidence.

Run the consolidated local product regression gate:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
```

The gate emits one JSON summary with `mode: "local-product-regression"`,
`scope: "local_only"`, a compact top-level `human_summary`, and explicit
`not_checked` markers for managed-cloud and benchmark claims. It orchestrates
the embedded demo/verify path, `http-demo`, local `doctor http`, the Rust SDK
quickstart, the Python sync SDK smoke, and generated TypeScript
check/http/gateway smoke paths. It is
local product regression evidence, not SQL compatibility, managed-cloud proof,
or benchmark evidence. Use
`--skip-typescript` when the local Node tooling is not installed. For CI
failure-path coverage, use test-only `--inject-failure STEP` to verify that the
gate exits nonzero while still emitting the failed-step JSON summary. Use
`--report-file PATH` to write the same JSON summary to a predictable file while
preserving JSON stdout for automation; this applies to full runs, `--only`,
`--inject-failure`, and `--list-steps`, and creates parent directories. Use
`--list-steps` to print JSON step metadata, including `human_summary` and
`only_supported`, for operator and CI wiring without running demo, HTTP, SDK,
or TypeScript smoke steps. `--skip-typescript` is for the full product gate and
non-TypeScript selectors; a TypeScript `--only` selector conflicts with --skip-typescript
for steps such as `typescript_check`, `typescript_http_smoke`, or
`typescript_gateway_smoke`. `product-quickstart` is the same local product gate
with a default report file at `target/tracedb/product-quickstart.json`; it
accepts the same product-regression options, including `--only` and
`--skip-typescript`, still writes JSON to stdout, and includes a top-level
`report_file` field when a report artifact is configured. A copy-paste local
receipt check is:

```bash
cargo run -q -p tracedb-cli -- product-quickstart
python3 - <<'PY'
import json
from pathlib import Path

receipt = Path("target/tracedb/product-quickstart.json")
summary = json.loads(receipt.read_text())
assert summary["ok"] is True
assert summary["mode"] == "local-product-regression"
assert summary["scope"] == "local_only"
assert summary["report_file"] == str(receipt.resolve())
assert summary["human_summary"]["status"] == "passed"
assert summary["claims"] == {
    "sql_module": "not_implemented",
    "managed_cloud": "not_checked",
    "benchmark": "not_checked",
}
print(summary["human_summary"]["message"])
print(summary["report_file"])
PY
```

Use `--skip-typescript` only when local Node tooling is unavailable; that is a
reduced local evidence path because the TypeScript check/http/gateway smoke
steps are skipped. For the reduced fallback receipt, use:

```bash
cargo run -q -p tracedb-cli -- product-quickstart --skip-typescript
```

The receipt still writes `target/tracedb/product-quickstart.json`, preserves
`report_file`, reports `typescript_enabled: false`, passes the six
non-TypeScript local steps including `python_sdk_smoke`, and omits `typescript_check`,
`typescript_http_smoke`, and `typescript_gateway_smoke`. Treat this as a
reduced local evidence path for machines without Node tooling, not the full
product gate.

When local executable policy or machine resources block product verification,
run the same reduced quickstart path on Modal as remote Linux product verification:

```bash
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

The Modal runner uploads the current checkout with `.git`, `target/`, local
env files, benchmark reports, caches, and Node modules excluded, then runs
`cargo fmt --all -- --check`, the focused quickstart receipt test, the docs
contract test, and `product-quickstart --skip-typescript`. It validates
`target/tracedb/product-quickstart.json` against stdout and confirms the
reduced receipt still has `typescript_enabled: false`, six non-TypeScript
steps including `python_sdk_smoke`, SQL as `not_implemented`, and managed-cloud/benchmark claims as
`not_checked`. Use `--mode workspace` for the heavier remote lane that also
runs the full CLI demo test file, usability acceptance test file, and
`cargo test --workspace --all-targets`. This is remote Linux product verification;
it does not replace a final macOS-local quickstart check when the question is
whether this workstation can execute binaries.

To validate the failure receipt path without waiting for the full gate, use:

```bash
cargo run -q -p tracedb-cli -- product-quickstart --inject-failure embedded_demo
```

The command exits nonzero, still writes
`target/tracedb/product-quickstart.json`, preserves the top-level `report_file`
field, reports `human_summary.status: "failed"`, and records
`failure_injection: "embedded_demo"` with an injected failed `embedded_demo`
step. For narrow local iteration,
`--only embedded_demo` currently runs just the embedded demo step and emits the
normal one-step `local-product-regression` JSON summary. After that, use the
same `--data-root` with `--only embedded_verify` to verify the existing
embedded demo data without running HTTP, SDK, or TypeScript steps.
`--only http_demo` runs the self-contained local HTTP demo step and emits the
normal one-step `local-product-regression` JSON summary. It does not run local
`doctor http`, the Rust SDK quickstart, generated TypeScript smoke steps,
managed-cloud checks, benchmark controls, or SQL compatibility checks.
`--only local_doctor` starts a managed-style local loopback `tracedb-server`
child process and runs only the existing local `doctor http` product-regression
step with readiness wait, `database_id`, and `branch_id` metadata. It emits the
normal one-step `local-product-regression` JSON summary with `only_step:
"local_doctor"`. This is local endpoint diagnostics evidence only; it does not
run `http_demo`, the Rust SDK quickstart, generated TypeScript smoke steps,
managed-cloud checks, benchmark controls, or SQL compatibility checks.
`--only rust_sdk_quickstart` starts a managed-style local loopback
`tracedb-server` child process, creates the SDK admin directory, and runs only
the existing Rust SDK quickstart product-regression step with idempotency
retries plus compact/snapshot/restore admin coverage. It emits the normal
one-step `local-product-regression` JSON summary with `only_step:
"rust_sdk_quickstart"`. This is local Rust SDK quickstart evidence only; it
does not run `http_demo`, local `doctor http`, generated TypeScript smoke
steps, managed-cloud checks, benchmark controls, or SQL compatibility checks.
When the Rust SDK child exits nonzero after emitting quickstart JSON, the
wrapper preserves that nested child object under
`steps.rust_sdk_quickstart.summary` while also retaining stdout/stderr tails for
operator debugging.
`--only python_sdk_smoke` runs only `python3 clients/python/http_smoke.py` from
the workspace root. The smoke starts its own local `tracedb-server` child
process and exercises the sync Python SDK through ready, catalog, schema apply,
insert, batch ingest, patch, get, scan, query, explain, delete, idempotency,
error envelopes, compact, snapshot, restore, and jobs. It emits the normal
one-step `local-product-regression` JSON summary with `only_step:
"python_sdk_smoke"`. This is local sync Python SDK HTTP smoke evidence only;
it does not run embedded demo/verify, `http_demo`, local `doctor http`, the
Rust SDK quickstart, generated TypeScript smoke steps, managed-cloud checks,
benchmark controls, or SQL compatibility checks.
`--only typescript_check` runs only `(cd clients/typescript && npm run check)`,
which currently performs the package typecheck plus dependency-free
generated-client, public SDK, package build, package-entry smoke, and pack
dry-run checks. It emits the normal one-step
`local-product-regression` JSON summary with `only_step: "typescript_check"`.
This is TypeScript package boundary evidence only; it does not run `http_demo`,
local `doctor http`, the Rust SDK quickstart, TypeScript HTTP smoke,
TypeScript gateway smoke, managed-cloud checks, benchmark controls, or SQL
compatibility checks.
`--only typescript_http_smoke` runs only
`(cd clients/typescript && npm run public-http-smoke)`, which starts its own
local `tracedb-server` child process and exercises the public TypeScript SDK
wrapper through ready, catalog, schema apply, insert, batch ingest, patch, get,
scan, query, explain, delete, compact, snapshot, restore, and jobs. It emits
the normal one-step `local-product-regression` JSON summary with `only_step:
"typescript_http_smoke"`. This is local public TypeScript SDK HTTP smoke
evidence only; it does not run embedded demo/verify, `http_demo`, local
`doctor http`, the Rust SDK quickstart, `typescript_check`, TypeScript gateway
smoke, managed-cloud checks, benchmark controls, or SQL compatibility checks.
`--only typescript_gateway_smoke` runs only
`(cd clients/typescript && npm run gateway-smoke)`, which starts a local engine
plus gateway-mode `tracedb-server`, requires bearer auth, verifies bad-token and
bad-branch rejection, and exercises the public TypeScript SDK wrapper through
the gateway with `databaseId=db_local`, `branchId=db_local:main`, and a local
admin scratch directory. It emits the normal one-step
`local-product-regression` JSON summary with `only_step:
"typescript_gateway_smoke"`. This is local public TypeScript SDK gateway
auth/routing evidence only; it does not run embedded demo/verify, `http_demo`,
local `doctor http`, the Rust SDK quickstart, `typescript_check`, TypeScript
HTTP smoke, managed-cloud checks, benchmark controls, or SQL compatibility
checks.

Run the SDK quickstart against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo/data TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000 --safe-retries 1 --idempotency-retries 1 --admin-dir /tmp/tracedb-sdk-demo/admin
```

For a read-only endpoint diagnostic against that running server:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --timeout-ms 1000 --safe-retries 1 --wait-ready-ms 5000 --database-id db_local --branch-id db_local:main
```

The HTTP doctor checks the current health, readiness, catalog, public-safe
metrics, and admin-jobs routes, returns a JSON summary with per-route responses
or parsed error envelopes, including `server_error` and `server_error_code`
when an endpoint returns the current coded JSON error shape, and reports
`sql_module: not_implemented`. Optional `--database-id` and `--branch-id` add
managed-routing metadata for gateway diagnostics, including the bodyless
admin-jobs route. Optional `--wait-ready-ms` polls readiness before the normal
checks, reports `ready_wait_timeout_ms` and `ready_wait`, and keeps immediate
post-start local checks scriptable. The command exits non-zero when any check
fails while keeping the JSON summary on stdout. It is a local/managed-style
endpoint diagnostic, not a SQL probe or benchmark.

For CI or deployed endpoint checks, the same command can read endpoint config
from environment variables instead of flags:

```bash
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 TRACEDB_WAIT_READY_MS=5000 cargo run -p tracedb-cli -- doctor http
```

The SDK example checks readiness, health, catalog, public-safe metrics, and
admin jobs, then applies schema, batch-ingests records, patches a record,
verifies patched visibility, scans, queries, explains, deletes, verifies
deleted-record hiding, optionally compacts/snapshots/restores when `--admin-dir`
points at an absolute server-side local scratch directory, and reports
`sql_module: not_implemented`. The admin path is interpreted by the
`tracedb-server` process, and restore creates a separate database directory
instead of replacing the running server. The example uses typed SDK convenience
methods over the current HTTP response shapes and accepts a configurable SDK
request timeout; the original raw `serde_json::Value` methods remain available.
The blocking SDK now also exposes a first ergonomic table/query layer through
`TraceDb::connect(config)?` and `db.table("docs").tenant("tenant-a")`.
`TableHandle` values can `insert`, `insert_batch`, `patch_record`,
`get_record`, `scan_typed`, and `delete_record`, then enter the query builder
with `query()` or the direct chaining helpers `where_eq`, `match_text`, `near`,
`with_explain`, `limit`, `all()`, and `explain_plan()`. The table helpers post
the canonical record/batch/patch wire shapes, the builder posts the canonical
`HybridQuery` wire shape to `/v1/query` or `/v1/explain`, and both paths are
covered by request-shape and real loopback-server SDK tests.
Rust SDK connection config can also be built directly from environment
variables with `TraceDbClientConfig::from_env()`. It reads `TRACEDB_URL`,
optional `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`,
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, and
`TRACEDB_IDEMPOTENCY_RETRIES`; tests use `TraceDbClientConfig::from_env_vars`
to prove the same parser without mutating process environment.
Bounded safe retries are available for SDK health/read routes only. Callers can
manually attach `Idempotency-Key` per write/admin request with
`TraceDbRequestOptions`; `TraceDbClientConfig::with_idempotency_retries` can then
opt into bounded transient retries for those keyed write/admin requests. The SDK
quickstart demonstrates that path with `--idempotency-retries` /
`TRACEDB_IDEMPOTENCY_RETRIES`, generating per-run keys for its write/admin
steps. The SDK also exposes typed local admin helpers for compact, snapshot, and
restore. Its JSON summary uses a stable operator envelope with
`mode: "rust-sdk-quickstart"`, `server_url`, optional `database_id` /
`branch_id`, `table`, `tenant_id`, and a structured `admin` object that reports
whether compact, snapshot, and restore were requested or skipped. The quickstart
also reports `records_put`, `records_batched`, and an `error_envelope` sample so
the platform conformance runner can prove single-record put and SDK error
parsing. Configuration failures such as an invalid `--url`, invalid retry count,
or relative `--admin-dir` also emit a parseable `ok: false` JSON summary on
stdout with the same mode, endpoint metadata, `error.kind`, `error.message`,
false step statuses, and `sql_module: not_implemented`, while stderr keeps the
short human-readable error line.

The current versioned HTTP route reference is in `docs/api/v1-http.md`; the
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`. A checked
generated TypeScript `fetch` client artifact lives at
`clients/typescript/src/client.ts` and is regenerated from the OpenAPI artifact
with `python3 scripts/generate_typescript_client.py`.
The Rust SDK also exposes `TraceDbAsyncClient`, a minimal async facade over the
same HTTP contract. It runs the existing transport on a background thread per
request, preserving timeout, retry, routing metadata, and error behavior while
making the current typed read, write, and admin helpers awaitable. Async typed
write/admin helpers include schema apply, record put/batch/patch/delete,
compact, snapshot, and restore, including the same option-aware idempotency
helpers as the blocking client. This is not yet a runtime-native
Tokio/async-std transport.
The TypeScript client smoke runs with local Node type stripping:

```bash
node --experimental-strip-types clients/typescript/smoke.ts
```

The TypeScript package now also has a first public SDK wrapper over the
generated transport:

```ts
import { TraceDB } from "./src/sdk";

const db = TraceDB.fromEnv();

const result = await db
  .table("docs")
  .where({ tenant_id: "tenant-a", status: "published" })
  .match("body", "rust sdk")
  .near("embedding", [1, 0, 0])
  .with({ explain: true, freshness: "lazy" })
  .limit(20)
  .all();
```

The wrapper lives in `clients/typescript/src/sdk.ts`, uses the generated
`TraceDbClient` as its transport, and is covered by `npm run public-smoke`.
`TraceDB.fromEnv()` reads `TRACEDB_URL`, optional `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, and
`TRACEDB_SAFE_RETRIES`, and `TRACEDB_IDEMPOTENCY_RETRIES` so the public
TypeScript SDK shares the same connection, routing, read-only retry, and keyed
mutation/admin retry config boundary as Rust. `safeRetries` retries transient
5xx responses only for health/ready, get, scan, query, and explain.
`idempotencyRetries` is default-off and retries transient 5xx responses for
mutation/admin routes only when the individual request carries a caller-provided
idempotency key.
`npm run public-http-smoke` starts a local server and drives the public wrapper
through schema apply, insert, batch ingest, patch, get, scan, query, explain,
delete, idempotency replay/conflict, parsed error envelopes, compact, snapshot,
restore, and jobs. `npm run public-http-smoke -- --summary-json
/tmp/tracedb-typescript-sdk-smoke.json` writes the machine-readable summary
that `python3 scripts/platform_conformance.py --surface typescript_sdk` maps
onto the Platform Contract v0 scenario IDs. It is TypeScript public-DX parity
evidence, not a published npm package claim.

The TypeScript SDK now has package-ready metadata for the public SDK entrypoint
while keeping the generated client as the transport subpath:

```bash
cd clients/typescript
npm ci
npm run check
```

The package is named `@tracedb/sdk`, builds `.` from `src/index.ts` into
`dist/index.js` / `dist/index.d.ts`, exports the generated transport as
`@tracedb/sdk/transport` from `dist/client.js` / `dist/client.d.ts`, and
includes `package-smoke`, `pack-dry-run`, and `consumer-smoke` scripts that
import both paths through the package `exports` map, verify tarball contents,
install the packed tarball into a clean temporary project, and import both
entrypoints from that installed package without publishing. This is local
build/pack/install boundary evidence, not evidence of an npm release or
publication pipeline.

Run the generated TypeScript client against a real local HTTP server with:

```bash
cd clients/typescript
npm run http-smoke
npm run public-http-smoke
```

`http-smoke` drives the generated transport artifact. `public-http-smoke` drives
the public `TraceDB` wrapper over that same generated transport. Both start a
loopback `tracedb-server` child process with isolated temporary data and remain
local product-path evidence, not managed-cloud health or package publishing
evidence.

Run the generated TypeScript endpoint quickstart against an existing HTTP
endpoint with:

```bash
cd clients/typescript
TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run quickstart
```

Set `TRACEDB_DATABASE_ID` and `TRACEDB_BRANCH_ID` to exercise managed-routing
metadata. Set `TRACEDB_ADMIN_DIR` to an absolute server-side local scratch path
to include compact, snapshot, and restore; without it, the quickstart stays on
readiness, health, catalog, metrics, schema apply, batch ingest, patch, patched
visibility, scan, query, explain, delete, and admin jobs. It emits
`sql_module: not_implemented` and is
an endpoint example for the generated artifact, not package publishing or
benchmark evidence.

Run the public TypeScript SDK gateway smoke with:

```bash
cd clients/typescript
npm run gateway-smoke
```

This starts a local engine plus a gateway-mode `tracedb-server` with
`TRACEDB_REQUIRE_API_KEY=true`, `TRACEDB_API_TOKEN=dev-token`, and
`TRACEDB_ENGINE_URL` pointing at the engine. It then runs the public `TraceDB`
wrapper through the gateway with `databaseId=db_local`, `branchId=db_local:main`,
and a local admin scratch directory, covering schema apply, insert, batch,
patch, get, scan, query, explain, delete, compact, snapshot, restore, jobs,
token rejection, and bad-branch rejection. It is local gateway auth/routing
evidence for the public TypeScript SDK over the generated transport, not
managed-cloud proof, package publishing readiness, or benchmark evidence.

The Python SDK now has a sync-first package under `clients/python/tracedb`:

```python
from tracedb import TraceDB

db = TraceDB.from_env()
rows = (
    db.table("docs")
    .where({"tenant_id": "tenant-a", "status": "published"})
    .match_text("body", "TraceDB Python")
    .near("embedding", [1, 0, 0])
    .with_options(explain=True, freshness="lazy")
    .limit(20)
    .all()
)
```

Run its local HTTP smoke with:

```bash
python3 -m unittest discover -s clients/python/tests
python3 clients/python/install_smoke.py
python3 clients/python/http_smoke.py --summary-json /tmp/tracedb-python-sdk-smoke.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
```

The client is stdlib-only for now, exposes table handles, a query builder,
health/catalog/metrics/admin helpers, managed `database_id` / `branch_id`
routing metadata injection, `TraceDB.from_env()` for `TRACEDB_URL`,
`TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, and
`TRACEDB_TIMEOUT_MS`, `TRACEDB_SAFE_RETRIES`, `TRACEDB_IDEMPOTENCY_RETRIES`,
`Idempotency-Key` support, and parsed HTTP error envelopes. `safe_retries`
retries transient HTTP 5xx responses only for health/read routes: health, ready,
get, scan, query, and explain. `idempotency_retries` is default-off and retries
transient HTTP 5xx responses for mutation/admin routes only when that request
carries a caller-provided `Idempotency-Key`; unkeyed writes and 4xx/conflict
responses are not retried. The unit lane checks the local package shape and env
config helper.
The install smoke prefers a temporary venv, installs `clients/python`, and runs
a consumer from outside the repo so source-path imports cannot mask packaging
drift; on images without working `ensurepip`, it falls back to an isolated
temporary pip `--target` install. The HTTP smoke starts a local `tracedb-server`
and proves all required v0 contract scenarios for the Python surface. The
`python_sdk` conformance lane runs that smoke with the package installed into an
isolated temporary pip `--target`, so source-tree imports cannot mask SDK drift.
This is sync SDK contract evidence, not PyPI readiness, async support,
managed-cloud proof, SQL compatibility, or full GraphQL adapter parity.

Native TraceQL now executes through the canonical HTTP surface:
`POST /v1/traceql` accepts `{ "query": string }`, parses line-oriented `FROM`,
`TENANT`, `WHERE`, `MATCH`, `NEAR`, `FRESHNESS`, `LIMIT`, and `EXPLAIN`
directives with `traceql_query_from_str`, and compiles them into the existing
`HybridQuery` model before returning the same result shape as `POST /v1/query`.
The same parser also accepts the bounded SQL-ish adapter form
`EXPLAIN? SELECT * FROM <table> WHERE tenant_id = <value> [AND field = value]*
[LIMIT n]`, compiling it into the same `HybridQuery` path and returning
`invalid SQL-ish` bad-request errors for unsupported constructs such as `JOIN`.
The platform conformance harness now includes `traceql_string_execution`; HTTP
direct passes that scenario through `/v1/traceql`, the Rust SDK lane passes it
through `TraceDbClient::traceql_typed`, the TypeScript SDK lane passes it
through the public `TraceDB.traceql()` helper, and the Python SDK lane passes it
through the sync `TraceDB.traceql()` helper. The dedicated `traceql_sqlish`
lane checks the bounded SQL-ish adapter against the same scenario manifest as a
partial surface. `GET /v1/graphql/schema` now exports generated SDL from
applied TraceDB table schema, and `POST /v1/graphql` exposes a bounded GraphQL
query adapter over the same `HybridQuery` model. The GraphQL conformance lane
checks schema export, query, explain, and error behavior. The Rust SDK exposes
this bounded HTTP adapter through `TraceDbClient::graphql_typed`,
`graphql_request_typed`, and `GraphQlQueryRequest`; the TypeScript SDK exposes
it through `TraceDB.graphql()` and `graphqlRequest({ query })`; the Python SDK
exposes it through `TraceDB.graphql()` and
`graphql_request({"query": query})`. This is TraceQL/query-adapter, GraphQL SDL
export, and bounded GraphQL query-adapter evidence only; SQL compatibility,
PostgreSQL compatibility, GraphQL mutation support, subscription support,
resolver runtime, GraphQL data-envelope execution, and full adapter parity
remain unimplemented.

The generated TypeScript artifact includes OpenAPI-derived schema aliases such
as `TableSchema`, `RecordPutBatchRequest`, `HybridQuery`, and
`SnapshotRequest`, and its route methods return OpenAPI response aliases such as
`ReadyResponse`, `PutBatchResponse`, `RecordScanOutput`, `QueryResponse`, and
`HybridExplain`. The query/explain aliases include current response-shape
helpers such as `HybridQueryRow`, `HybridScoreComponents`, `AccessPathExplain`,
`Candidate`, and timing entries. These are permissive
compile-time helpers: known fields are optional, unknown JSON fields are still
allowed, and server-side runtime validation remains authoritative. The OpenAPI
artifact and generated client now also expose `HybridQuery.scalar_eq`,
`HybridQuery.graph_seed`, and `HybridQuery.temporal_as_of` request fields, the
server's `/v1/records/put` direct-or-wrapper body as `RecordPutBody`, and
`getRecord` responses type `record` as `RecordOutput | null` with the serialized
`version_id` field.

## Current Boundaries

- SQL compatibility is not implemented.
- The Rust `tracedb-sdk` crate now includes a minimal HTTP client for the
  current engine API plus the original request-builder helpers. It can attach
  managed `database_id` and `branch_id` routing metadata to JSON POST bodies,
  includes typed convenience response methods and typed query rows for the
  current product path, includes typed health/catalog/metrics/admin-jobs
  helpers, includes typed local admin helpers for compact, snapshot, and
  restore, supports a configurable blocking socket request timeout, supports
  bounded safe retries for health/read routes, supports opt-in
  idempotency-key-gated transient retries for mutation/admin routes, and non-2xx
  SDK errors include request method, request path, HTTP status, and response
  body. When the response body is the current
  `{ "error": string, "code"?: string }` envelope, the SDK also exposes that
  parsed error through `error_response()`, `server_error()`, and
  `server_error_code()`. It is not yet a full managed/cloud SDK.
- `clients/typescript/src/client.ts` is a generated dependency-free transport
  artifact for the current HTTP API. It now includes OpenAPI-derived schema
  aliases and typed method signatures, including concrete health, readiness,
  catalog, metrics, and admin-jobs response aliases, while preserving the API's
  permissive additional-properties boundary. It is not a hand-maintained managed
  SDK, not a strict runtime validator, and not a broader SDK compatibility
  claim. Its current runtime smoke uses Node's experimental TypeScript strip
  support. The `clients/typescript` package now declares `@tracedb/sdk` package
  metadata, builds JS/declaration outputs into `dist`, and exports the generated
  transport as `@tracedb/sdk/transport`, but this is not evidence of an npm
  release or publication pipeline. It rejects
  empty or CR/LF-containing `idempotencyKey` request options before `fetchImpl`
  is called. `TraceDbHttpError` preserves the raw response
  body and exposes parsed `responseJson`, `errorResponse`, `responseError`, and
  `responseCode` when the server or gateway returns the current JSON error
  envelope; stable machine-readable error `code` is preserved when present.
- HTTP mutation and admin routes accept optional `Idempotency-Key` for local
  data-dir-backed replay on the engine, and the gateway forwards that header.
  After a successful local cache write, replay survives a clean engine reopen
  from the same data directory; filesystem cache-write failures are logged and
  do not roll back the original mutation. This is not cross-replica, not
  crash-atomic exactly-once, and not a managed-cloud exactly-once guarantee. The
  Rust SDK can manually send the header per request through
  `TraceDbRequestOptions`, and opt-in SDK idempotent retries require that
  header.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require external controls and a number to beat.
