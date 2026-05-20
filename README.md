# TraceDB

TraceDB is a development-stage transactional candidate-stream database. The
current product surface is an embedded/local engine with CLI and HTTP paths.

Canonical architecture, benchmark, and roadmap notes live in the Grogan
Development vault:

```text
/Users/zgrogan/Repos/grogan-development-vault/10_Projects/TraceField Suite/TraceDB/
```

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
```

The gate emits one JSON summary with `mode: "local-product-regression"`,
`scope: "local_only"`, and explicit `not_checked` markers for managed-cloud and
benchmark claims. It orchestrates the embedded demo/verify path, `http-demo`,
local `doctor http`, the Rust SDK quickstart, and generated TypeScript
check/http/gateway smoke paths. It is local product regression evidence, not
SQL compatibility, managed-cloud proof, or benchmark evidence. Use
`--skip-typescript` when the local Node tooling is not installed. For CI
failure-path coverage, use test-only `--inject-failure STEP` to verify that the
gate exits nonzero while still emitting the failed-step JSON summary. Use
`--list-steps` to print JSON step metadata for operator and CI wiring without
running demo, HTTP, SDK, or TypeScript smoke steps. For narrow local iteration,
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
`--only typescript_check` runs only `(cd clients/typescript && npm run check)`,
which currently performs the private package typecheck plus dependency-free
generated-client smoke. It emits the normal one-step
`local-product-regression` JSON summary with `only_step: "typescript_check"`.
This is generated TypeScript check evidence only; it does not run `http_demo`,
local `doctor http`, the Rust SDK quickstart, TypeScript HTTP smoke,
TypeScript gateway smoke, managed-cloud checks, benchmark controls, or SQL
compatibility checks.
`--only typescript_http_smoke` runs only
`(cd clients/typescript && npm run http-smoke)`, which starts its own local
`tracedb-server` child process and exercises the generated TypeScript client
HTTP product path through ready, catalog, schema apply, batch ingest, get,
scan, query, explain, delete, compact, snapshot, restore, and jobs. It emits
the normal one-step `local-product-regression` JSON summary with `only_step:
"typescript_http_smoke"`. This is local generated TypeScript HTTP smoke
evidence only; it does not run embedded demo/verify, `http_demo`, local
`doctor http`, the Rust SDK quickstart, `typescript_check`, TypeScript gateway
smoke, managed-cloud checks, benchmark controls, or SQL compatibility checks.
`--only typescript_gateway_smoke` runs only
`(cd clients/typescript && npm run gateway-smoke)`, which starts a local engine
plus gateway-mode `tracedb-server`, requires bearer auth, verifies bad-token and
bad-branch rejection, and runs the endpoint quickstart through the gateway with
`TRACEDB_DATABASE_ID=db_local`, `TRACEDB_BRANCH_ID=db_local:main`, and a local
admin scratch directory. It emits the normal one-step
`local-product-regression` JSON summary with `only_step:
"typescript_gateway_smoke"`. This is local generated TypeScript gateway
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

The SDK example applies schema, batch-ingests records, scans, queries, explains,
deletes, verifies deleted-record hiding, optionally compacts/snapshots/restores
when `--admin-dir` points at an absolute server-side local scratch directory,
and reports `sql_module: not_implemented`. The admin path is interpreted by the
`tracedb-server` process, and restore creates a separate database directory
instead of replacing the running server. The example uses typed SDK convenience
methods over the current HTTP response shapes and accepts a configurable SDK
request timeout; the original raw `serde_json::Value` methods remain available.
Bounded safe retries are available for SDK health/read routes only. Callers can
manually attach `Idempotency-Key` per write/admin request with
`TraceDbRequestOptions`; `TraceDbClientConfig::with_idempotency_retries` can then
opt into bounded transient retries for those keyed write/admin requests. The SDK
quickstart demonstrates that path with `--idempotency-retries` /
`TRACEDB_IDEMPOTENCY_RETRIES`, generating per-run keys for its write/admin
steps. The SDK also exposes typed local admin helpers for compact, snapshot, and
restore.

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

The generated TypeScript client also has a private local package boundary for
typechecking the artifact and smoke script:

```bash
cd clients/typescript
npm ci
npm run check
```

Run the generated TypeScript client against a real local HTTP server with:

```bash
cd clients/typescript
npm run http-smoke
```

This starts a loopback `tracedb-server` child process with an isolated temporary
data directory and drives the generated client through ready, schema apply,
direct put, batch ingest, get, scan, query, explain, delete, compact, snapshot,
restore, and admin jobs. It is local product-path evidence for the generated
transport artifact, not managed-cloud health or package publishing evidence.

Run the generated TypeScript endpoint quickstart against an existing HTTP
endpoint with:

```bash
cd clients/typescript
TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run quickstart
```

Set `TRACEDB_DATABASE_ID` and `TRACEDB_BRANCH_ID` to exercise managed-routing
metadata. Set `TRACEDB_ADMIN_DIR` to an absolute server-side local scratch path
to include compact, snapshot, and restore; without it, the quickstart stays on
readiness, health, catalog, metrics, schema apply, batch ingest, scan, query,
explain, delete, and admin jobs. It emits `sql_module: not_implemented` and is
an endpoint example for the generated artifact, not package publishing or
benchmark evidence.

Run the generated TypeScript gateway smoke with:

```bash
cd clients/typescript
npm run gateway-smoke
```

This starts a local engine plus a gateway-mode `tracedb-server` with
`TRACEDB_REQUIRE_API_KEY=true`, `TRACEDB_API_TOKEN=dev-token`, and
`TRACEDB_ENGINE_URL` pointing at the engine. It then runs the endpoint
quickstart through the gateway with `TRACEDB_DATABASE_ID=db_local`,
`TRACEDB_BRANCH_ID=db_local:main`, and a local admin scratch directory. It is
local gateway auth/routing evidence for the generated TypeScript artifact, not
managed-cloud proof, package publishing readiness, or benchmark evidence.

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
  permissive additional-properties boundary. It is not a published npm package,
  not a hand-maintained managed SDK, not a strict runtime validator, and not a
  broader SDK compatibility claim. Its current runtime smoke uses Node's
  experimental TypeScript strip support. The private package under
  `clients/typescript` exists only for local typechecking plus fake-fetch and
  real local HTTP smoke validation; it does not declare package publishing
  fields. It rejects empty or CR/LF-containing `idempotencyKey` request options
  before `fetchImpl` is called. `TraceDbHttpError` preserves the raw response
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
