---
title: TraceDB v1 HTTP API Reference
tags:
  - tracedb
  - api
  - http
status: current-product-surface
type: api-reference
updated: 2026-05-19
---

# TraceDB v1 HTTP API Reference

This document tracks the current TraceDB `v1` HTTP product surface exposed by
`tracedb-server` and allowed through `tracedb-gateway`. It is a working local
API reference, not a managed-cloud SLA, not a benchmark claim, and not a SQL
compatibility claim.

The companion machine-readable artifact is `docs/api/v1-openapi.json`. Regenerate
or check it from the repo root with:

```bash
python3 scripts/generate_openapi_v1.py
python3 scripts/generate_openapi_v1.py --check
```

The checked generated TypeScript transport artifact is
`clients/typescript/src/client.ts`. It is generated from the OpenAPI artifact,
not hand-maintained as a second route manifest:

```bash
python3 scripts/generate_typescript_client.py
python3 scripts/generate_typescript_client.py --check
node --experimental-strip-types clients/typescript/smoke.ts
(cd clients/typescript && npm ci && npm run check)
(cd clients/typescript && npm run http-smoke)
```

## Boundaries

- SQL compatibility is not implemented.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require an external control and a number to beat.
- The current Rust SDK is a minimal blocking HTTP client for this API surface.
  It now also has a first ergonomic table/query layer over the same wire
  contract through `TraceDbClient::table("docs").tenant("tenant-a")`, including
  table insert, batch insert, get, scan, and delete helpers plus a query builder
  that posts the canonical `HybridQuery` shape.
  It also exposes `TraceDbAsyncClient` as a minimal async facade over the same
  HTTP contract. This first async surface runs the existing transport on a
  background thread per request so callers can await typed read, write, and
  admin helpers without blocking the first Future poll on socket I/O; it is not
  yet a runtime-native Tokio/async-std transport.
- The TypeScript client under `clients/typescript/src/client.ts` is a generated
  dependency-free `fetch` client artifact for this API surface. It is not a
  published npm package, not a managed-cloud SDK promise, and not a SQL
  compatibility claim. It includes OpenAPI-derived schema aliases and typed
  method signatures while keeping known fields optional and unknown JSON fields
  allowed. Runtime validation remains server-side. Scan/query/explain response
  aliases expose current server fields, including record scan counts, query
  rows, score components, access-path explain entries, planner candidates, and
  timing entries. Its local runtime smoke uses
  Node's experimental TypeScript strip support. The private package under
  `clients/typescript` exists only for local typechecking plus fake-fetch and
  real local HTTP smoke validation; it does not declare package publishing
  fields. It rejects empty or CR/LF-containing `idempotencyKey` request options
  before `fetchImpl` is called.
- The TypeScript public SDK wrapper under `clients/typescript/src/sdk.ts` is the
  first hand-written platform SDK layer over that generated transport. It
  exposes `TraceDB`, table handles, single and batch inserts, patch,
  scan/get/delete, admin compact/snapshot/restore/jobs, and query-builder
  chaining through `where({ tenant_id })`, `match`, `near`, `with`, `limit`,
  `all`, and `explainPlan`. It is smoke-tested with fake fetch through
  `npm run public-smoke` and with real local HTTP through
  `npm run public-http-smoke`; `npm run gateway-smoke` now drives the same
  public wrapper through the local gateway auth/routing lane.
- The Python SDK under `clients/python/tracedb` is the first sync AI/data SDK
  lane over this API surface. It is stdlib-only for now and exposes `TraceDB`,
  table handles, single and batch inserts, patch, get, scan, delete,
  health/catalog/metrics/admin helpers, managed `database_id` / `branch_id`
  routing metadata injection, `Idempotency-Key` support, parsed HTTP error
  envelopes, and query-builder chaining through `where`, `match_text`, `near`,
  `with_options`, `limit`, `all`, and `explain_plan`. It is covered by
  `python3 clients/python/http_smoke.py` and
  `python3 scripts/platform_conformance.py --surface python_sdk`. This is sync
  SDK contract evidence, not PyPI readiness, async support, managed-cloud
  proof, SQL compatibility, or GraphQL support.
- SDK safe retries apply only to health/read routes that do not mutate TraceDB
  data state: `GET /v1/health`, `GET /v1/ready`, `POST /v1/records/get`,
  `POST /v1/records/scan`, `POST /v1/query`, and `POST /v1/explain`.
- Idempotency-Key supports local data-dir-backed replay for mutation and admin
  routes. Same key plus same method/path/raw body replays the first successful
  response; same key with a different body returns `409 Conflict`.
- The idempotency cache is local-engine-only and scoped to the same data
  directory. It survives a clean engine reopen from that data directory, but it
  is not cross-replica, not crash-atomic exactly-once, and not a managed-cloud
  exactly-once guarantee.
- Filesystem cache-write failures are logged and do not roll back the original
  successful mutation; clean-reopen replay requires the local cache write to
  have succeeded.
- The generated TypeScript client rejects empty or CR/LF-containing
  `idempotencyKey` options before network I/O as `TraceDbRequestError`.
- The Rust SDK can manually send `Idempotency-Key` with
  `TraceDbRequestOptions` on individual requests. `safe_retries` still applies
  only to health/read routes. `idempotency_retries` is a separate opt-in policy
  for transient 5xx/timeout retries on mutation/admin routes and is only active
  when the individual request includes an `Idempotency-Key`.
- Gateway metering, request logging, and rate limiting may still observe each
  HTTP attempt.

## Transport

Requests and responses are JSON over HTTP/1.1. POST routes expect
`Content-Type: application/json`. The local engine accepts requests directly;
the gateway accepts the same product routes and forwards authorized requests to
the engine.

The minimal SDK can add `database_id` and `branch_id` fields to object-shaped
POST bodies when configured for managed routing. Direct engine-local requests
can omit those fields.

`TraceDbAsyncClient` wraps the same Rust SDK configuration and exposes
awaitable `ready`, `health`, catalog, metrics, admin-jobs, get, scan, query,
explain, and generic JSON request methods. It also exposes async typed write/admin helpers
for schema apply, record put/batch/patch/delete, compact,
snapshot, and restore, including the same option-aware idempotency helpers as
the blocking client. It preserves the same timeout, retry, error-envelope, and
managed-routing behavior as the blocking client. The implementation uses a
background thread per request, so it is suitable for basic async integration
tests and scripts but not a final high-concurrency runtime-native transport.

The generated TypeScript client follows the same routing metadata boundary:
configured `databaseId` and `branchId` are added only to absent root
`database_id` and `branch_id` fields on copied JSON POST bodies. Explicit
request fields win, the caller's object is not mutated, and GET routes send no
JSON body.

The Python SDK follows the same routing metadata boundary: configured
`database_id` and `branch_id` are added only to absent root `database_id` and
`branch_id` fields on copied JSON POST bodies. Explicit request fields win, the
caller-provided object is not mutated, and GET routes send no JSON body.

Generated TypeScript aliases are sourced from the OpenAPI component schemas and
are used in route method signatures. They are intentionally loose: the current
HTTP API accepts additive JSON fields in several request and response shapes, so
the generated aliases extend `JsonObject`, mark known fields optional, and leave
domain enforcement to the server. The OpenAPI schema for
`POST /v1/records/put` uses `RecordPutBody`, a `oneOf` union matching current
server behavior: callers may send `RecordInput` directly or the wrapper
`RecordPutRequest`. `GetRecordResponse.record` references `RecordOutput | null`;
`RecordOutput` includes the serialized `version_id` field. `HybridQuery`
includes the existing JSON request knobs for `scalar_eq`, `graph_seed`, and
`temporal_as_of`; these are native API fields, not SQL compatibility.

## Health And Catalog

| Route | Request | Response |
| --- | --- | --- |
| `GET /v1/health` | No body. | Engine or gateway health JSON with `ok` and `service`. |
| `GET /v1/ready` | No body. | Readiness JSON with `ready`, `service`, and engine epoch or gateway health-check context. |
| `GET /v1/databases` | No body. | Local mode returns a `local` database; gateway mode returns catalog databases. |
| `GET /v1/branches` | No body. | Local mode returns the active branch; gateway mode returns catalog branches. |
| `GET /v1/metrics/public-safe` | No body. | Public-safe service, epoch, segment/index/module/schema, recovery, request, or rate-limit counters. |

The generated OpenAPI and TypeScript artifacts expose concrete but permissive
aliases for these read-only responses: `HealthResponse`, `ReadyResponse`,
`DatabasesResponse`, `BranchesResponse`, `MetricsResponse`, and `JobsResponse`.
Fields remain optional where local-engine and gateway shapes differ.

Local compatibility aliases also exist for development: `GET /health`,
`GET /ready`, and `GET /metrics`.

The CLI can run a read-only diagnostic against a running local engine or
managed-style endpoint:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --timeout-ms 1000 --safe-retries 1 --wait-ready-ms 5000 --database-id db_local --branch-id db_local:main
```

The diagnostic checks `GET /v1/health`, `GET /v1/ready`, `GET /v1/databases`,
`GET /v1/branches`, `GET /v1/metrics/public-safe`, and `GET /v1/admin/jobs`.
It emits a single JSON summary with per-route responses or SDK error details,
including parsed `server_error` and `server_error_code` fields when an endpoint
returns the current coded JSON error shape. Optional `--database-id` and
`--branch-id` add managed-routing metadata to gateway diagnostics; for the
bodyless admin-jobs route, the gateway receives those IDs as query metadata
before proxying `/v1/admin/jobs` to the engine. Optional `--wait-ready-ms`
polls `GET /v1/ready` before the normal checks and reports
`ready_wait_timeout_ms` plus a `ready_wait` object in the summary. The command
exits non-zero when any check fails while preserving the JSON summary on
stdout. It does not mutate data, does not probe SQL compatibility, and is not
benchmark evidence.

The same diagnostic can be run from CI or deployment scripts with endpoint
configuration supplied by environment variables:

```bash
TRACEDB_URL=https://<endpoint> TRACEDB_TOKEN=$TRACEDB_TOKEN TRACEDB_DATABASE_ID=db_local TRACEDB_BRANCH_ID=db_local:main TRACEDB_TIMEOUT_MS=1000 TRACEDB_SAFE_RETRIES=1 TRACEDB_WAIT_READY_MS=5000 cargo run -p tracedb-cli -- doctor http
```

Error responses use the current JSON envelope
`{ "error": string, "code"?: string }` for server and gateway failures such as
validation errors, not found routes, idempotency conflicts, unauthorized gateway
calls, gateway rate limits, and upstream unavailability. The `error` string
remains the compatibility field; `code` is a stable machine-readable value when
the server or gateway can classify the failure. The Rust SDK and generated
TypeScript transport preserve the raw response body and expose parsed
error-envelope helpers. This is current-envelope ergonomics, not a broader RFC
7807/problem-details contract.

## Schema And Writes

| Route | Request | Response |
| --- | --- | --- |
| `POST /v1/schema/apply` | `TableSchema`: `name`, `primary_id_column`, `tenant_id_column`, scalar columns, text-indexed columns, and vector columns. | `{ "epoch": number }`. |
| `POST /v1/insert` | `RecordInput`: `table`, `id`, `tenant_id`, and `fields`. | `{ "epoch": number }`. Kept for compatibility; prefer records routes for product examples. |
| `POST /v1/records/put` | Either `RecordInput` directly or `{ "record": RecordInput }`. Full replacement write. | `{ "epoch": number }`. |
| `POST /v1/records/put-batch` | `RecordPutBatchRequest`: `records` plus optional `include_write_timing`. | `{ "epoch": number, "record_count": number }`; includes `write_timing` when requested. |
| `POST /v1/records/patch` | `RecordPatchRequest`: `table`, `tenant_id`, `id`, and patch `fields`. | `{ "epoch": number }`. |
| `POST /v1/records/delete` | `RecordDeleteRequest`: `table`, `tenant_id`, `id`, and optional `tombstone`. | `{ "deleted": true, "epoch": number }`. |

Write routes allocate epochs and mutate TraceDB state. They accept optional
`Idempotency-Key` for local data-dir-backed replay. The SDK only retries writes when
`TraceDbClientConfig::with_idempotency_retries` is enabled and the individual
request includes an `Idempotency-Key`; `safe_retries` alone never retries
mutating writes.

## Reads And Retrieval

| Route | Request | Response |
| --- | --- | --- |
| `POST /v1/records/get` | `RecordGetRequest`: `table`, `tenant_id`, and `id`. | `{ "record": RecordOutput \| null }`. |
| `POST /v1/records/scan` | `RecordScanRequest`: `table`, `tenant_id`, and optional `limit`. | `RecordScanOutput` with `records: RecordOutput[]` and `returned_count`. No cursor metadata is emitted today. |
| `POST /v1/query` | `HybridQuery`: `table`, `tenant_id`, optional `text`, optional `vector`, optional `scalar_eq`, optional `graph_seed`, optional `temporal_as_of`, `top_k`, `freshness`, and `explain`. | With `explain: false`, returns `{ "results": HybridQueryRow[] }`; with `explain: true`, returns results plus `HybridExplain` metadata. |
| `POST /v1/explain` | Same query shape as `POST /v1/query`; the server forces explain mode. | `HybridExplain` only, including current access-path, candidate, counter, and timing fields. |

Query responses include `Server-Timing` phase attribution for read, parse,
lock wait, engine, explain build, materialization, response shaping, and encode
costs. These timings are development instrumentation, not exported benchmark
claims.

## Admin

| Route | Request | Response |
| --- | --- | --- |
| `POST /v1/admin/compact` | Empty JSON object. | `{ "compacted": true }`. |
| `POST /v1/admin/snapshot` | `{ "target": "/path/to/snapshot" }`. | `{ "snapshot": true, "target": string }`. |
| `POST /v1/admin/restore` | `{ "source": "/path/to/snapshot", "target": "/path/to/restore" }`. | `{ "restored": true, "source": string, "target": string }`. |
| `GET /v1/admin/jobs` | No body. | Idle job queue state for segment compaction, snapshot creation, and feature indexing. |

Admin routes can mutate durable files or create out-of-band filesystem state.
They accept optional `Idempotency-Key` for local data-dir-backed replay. Replay
survives a clean engine reopen from the same data directory, but the contract is
not cross-replica or crash-atomic exactly-once, and filesystem cache-write
failures are logged without rolling back the original successful operation. The
SDK only retries admin requests when
`TraceDbClientConfig::with_idempotency_retries` is enabled and the individual
request includes an `Idempotency-Key`; `safe_retries` alone never retries admin
requests.

The Rust SDK provides typed `SnapshotRequest`, `SnapshotResponse`,
`RestoreRequest`, and `RestoreResponse` wrappers plus raw/typed snapshot and
restore helpers over these local admin routes. The wire contract remains JSON
string paths.

## Minimal Product Path

Run the consolidated local product regression gate with one command:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
```

The gate emits one `local-product-regression` JSON summary for the embedded
demo/verify path, local HTTP SDK demo, endpoint doctor, Rust SDK quickstart,
Python sync SDK smoke, and generated TypeScript check/http/gateway smoke paths,
with a compact
top-level `human_summary` for quick operator scanning. It is local product
regression evidence only: SQL remains not implemented, managed-cloud is not
checked, and benchmarks are not checked. Failure ergonomics for the consolidated
local gate are covered by test-only `--inject-failure STEP`, which preserves the
JSON summary on stdout and exits nonzero. Operators can pass
`--report-file PATH` to write that same JSON summary to a predictable file while
preserving JSON stdout; parent directories are created. Operators can run
`product-regression --list-steps` to discover valid gate steps for failure
injection and CI orchestration; it emits JSON step metadata including
`human_summary` and `only_supported` and does not run demo, HTTP, SDK, or
TypeScript smoke steps.
`product-quickstart` runs the same local product gate with a default report file
at `target/tracedb/product-quickstart.json`, preserves JSON stdout, and includes
the resolved artifact path in the top-level `report_file` field. Operators can
validate the local quickstart receipt by checking that artifact for `ok: true`,
`mode: "local-product-regression"`, `scope: "local_only"`,
`human_summary.status: "passed"`, `claims.sql_module: "not_implemented"`,
`claims.managed_cloud: "not_checked"`, and `claims.benchmark: "not_checked"`.
`product-quickstart --skip-typescript` is the reduced fallback receipt for
machines without Node tooling: it still writes
`target/tracedb/product-quickstart.json`, keeps `report_file`, reports
`typescript_enabled: false`, passes the six non-TypeScript local steps including
`python_sdk_smoke`, and omits `typescript_check`, `typescript_http_smoke`, and
`typescript_gateway_smoke`. Treat it as a reduced local evidence path, not the
full product gate.
`product-quickstart --inject-failure embedded_demo` validates the failure
receipt path without running later product steps: the command exits nonzero,
writes the same default report artifact, keeps `report_file`, reports
`human_summary.status: "failed"`, and records the injected `embedded_demo`
failure.
`--skip-typescript` is for the full product gate and non-TypeScript selectors; a
TypeScript `--only` selector conflicts with --skip-typescript.
`product-regression --only embedded_demo` runs only
the embedded demo step and emits one-step `local-product-regression` JSON.
`product-regression --only http_demo` runs the self-contained local HTTP demo
step and emits the normal one-step `local-product-regression` JSON summary. It
does not run local `doctor http`, the Rust SDK quickstart, generated
TypeScript smoke steps, managed-cloud checks, benchmark controls, or SQL
compatibility checks.
`product-regression --only local_doctor` starts a managed-style local loopback
`tracedb-server` child process and runs only the existing local `doctor http`
product-regression step with readiness wait, `database_id`, and `branch_id`
metadata. It emits the normal one-step `local-product-regression` JSON summary
with `only_step: "local_doctor"`. This is local endpoint diagnostics evidence
only; it does not run `http_demo`, the Rust SDK quickstart, generated
TypeScript smoke steps, managed-cloud checks, benchmark controls, or SQL
compatibility checks. `product-regression --only rust_sdk_quickstart` starts a
managed-style local loopback `tracedb-server`, creates/uses the quickstart admin
dir, runs only the existing Rust SDK quickstart product-regression step, and
emits one-step `local-product-regression` JSON with `only_step:
"rust_sdk_quickstart"`. This is local Rust SDK quickstart evidence only, not
full product gate coverage, not `http_demo`, not local `doctor http`
diagnostics, not generated TypeScript smoke, not managed-cloud proof, not
benchmark evidence, and not SQL compatibility. If the Rust SDK child exits
nonzero after writing quickstart JSON, product-regression preserves that nested
object under `steps.rust_sdk_quickstart.summary` and keeps stdout/stderr tails
on the failed step for debugging.
`product-regression --only python_sdk_smoke` runs only
`python3 clients/python/http_smoke.py` from the workspace root. The smoke starts
its own local `tracedb-server` child process and exercises the sync Python SDK
through ready, catalog, schema apply, insert, batch ingest, patch, get, scan,
query, explain, delete, idempotency, error envelopes, compact, snapshot,
restore, and jobs. It emits one-step `local-product-regression` JSON with
`only_step: "python_sdk_smoke"`. This is local sync Python SDK HTTP smoke
evidence only, not full product gate coverage, not `http_demo`, not local
`doctor http`, not Rust SDK quickstart, not TypeScript smoke, not managed-cloud
proof, not benchmark evidence, and not SQL compatibility.
`product-regression --only typescript_check` runs only `npm run check` in
`clients/typescript`, which currently performs the private package typecheck
plus dependency-free generated-client smoke, and emits one-step
`local-product-regression` JSON with `only_step: "typescript_check"`. This is
generated TypeScript check evidence only, not full product gate coverage, not
`http_demo`, not local `doctor http`, not Rust SDK quickstart, not TypeScript
HTTP smoke, not TypeScript gateway smoke, not managed-cloud proof, not
benchmark evidence, and not SQL compatibility.
`product-regression --only typescript_http_smoke` runs only `npm run
public-http-smoke` in `clients/typescript`, which starts its own local
`tracedb-server` child process and exercises the public TypeScript SDK wrapper
over the generated transport, and emits one-step `local-product-regression` JSON
with `only_step: "typescript_http_smoke"`. This is local public TypeScript SDK HTTP
smoke evidence only, not full product gate coverage, not embedded demo/verify,
not `http_demo`, not local `doctor http`, not Rust SDK quickstart, not
`typescript_check`, not generated-transport `http-smoke`, not TypeScript gateway
smoke, not managed-cloud proof, not benchmark evidence, and not SQL
compatibility.
`product-regression --only typescript_gateway_smoke` runs only `npm run
gateway-smoke` in `clients/typescript`, which starts a local engine plus
gateway-mode `tracedb-server`, requires bearer auth, checks missing-token and
bad-branch rejection, and runs the public TypeScript SDK wrapper through the
gateway with managed routing metadata plus a local admin scratch dir. It emits
one-step `local-product-regression` JSON with `only_step:
"typescript_gateway_smoke"`. This is local public TypeScript SDK gateway
auth/routing evidence only, not full product gate coverage, not embedded
demo/verify, not `http_demo`, not local `doctor http`, not Rust SDK quickstart,
not `typescript_check`, not TypeScript HTTP smoke, not managed-cloud proof, not
benchmark evidence, and not SQL compatibility.
`product-regression --only embedded_verify`
verifies an existing embedded demo data root and should be run with the same
`--data-root` used for `--only embedded_demo`.

Run the complete local HTTP plus Rust SDK smoke with one command:

```bash
cargo run -p tracedb-cli -- --data /tmp/tracedb-http-demo http-demo
```

The command starts a loopback `tracedb-server` child process, drives the current
typed Rust SDK over HTTP, and reports `sql_module: not_implemented`. It covers
ready, schema apply, batch ingest, scan, query, explain, delete, compact,
snapshot, restore, and keyed mutation/admin retries. The child server's output
is not mixed into the command JSON summary.

The current runnable HTTP product path is:

1. Start the local engine with `tracedb-server`.
2. Check `GET /v1/ready`.
3. Apply schema with `POST /v1/schema/apply`.
4. Batch ingest with `POST /v1/records/put-batch`.
5. Read with `POST /v1/records/get` or `POST /v1/records/scan`.
6. Retrieve with `POST /v1/query` and inspect with `POST /v1/explain`.
7. Delete with `POST /v1/records/delete`.
8. Optionally compact, snapshot, and restore through the admin routes when
   using explicit server-side local paths.

For endpoint diagnostics before or after the product path, run:

```bash
cargo run -p tracedb-cli -- doctor http --url http://127.0.0.1:8090 --token dev-token --database-id db_local --branch-id db_local:main
```

The doctor can also read `TRACEDB_URL`, `TRACEDB_TOKEN`,
`TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, `TRACEDB_TIMEOUT_MS`, and
`TRACEDB_SAFE_RETRIES`, plus `TRACEDB_WAIT_READY_MS`, which lets
secret-bearing deployed checks avoid command line token arguments and lets
freshly started local endpoints settle before the full diagnostic run.

The SDK quickstart exercises this path and reports `sql_module:
not_implemented`. It uses typed SDK calls for readiness, health, catalog,
public-safe metrics, admin jobs, schema apply, batch ingest, patch, scan, query,
explain, delete, and deleted-record hiding; the patch step verifies patched
visibility before later reads and delete. Passing `--admin-dir SERVER_SIDE_DIR`
also exercises compact, snapshot, and restore with typed SDK admin helpers. The
argument must be an absolute path interpreted by the server process and is
intended as local scratch space. Passing `--idempotency-retries N`
or `TRACEDB_IDEMPOTENCY_RETRIES=N` demonstrates the keyed write/admin retry path
by generating per-run `Idempotency-Key` values for the quickstart's
mutation/admin steps. Restore creates a separate database directory; it does not
replace the running server's data directory and is not managed-cloud backup/DR
semantics. The JSON summary reports the active envelope fields operators need to
compare quickstarts across clients: `mode: "rust-sdk-quickstart"`,
`server_url`, optional `database_id` / `branch_id`, `table`, `tenant_id`, and a
structured `admin` object where skipped admin work is explicit instead of
collapsed into a failure-shaped boolean. Invalid quickstart configuration exits
nonzero but preserves stdout as JSON, reporting `ok: false`, `phase: "config"`,
`error.kind`, `error.message`, false step statuses, and `sql_module:
not_implemented` for automation that parses product-regression child summaries.

The generated TypeScript client has its own local HTTP smoke:

```bash
cd clients/typescript
npm run http-smoke
npm run public-http-smoke
```

`http-smoke` starts `tracedb-server` with an isolated temporary data directory
and uses the generated client against real HTTP routes. `public-http-smoke`
starts the same kind of local server and uses the public `TraceDB` wrapper for
schema apply, insert, batch ingest, patch, get, scan, query, explain, delete,
compact, snapshot, restore, and admin jobs.

It also has an endpoint quickstart for an already-running local or
managed-style HTTP endpoint:

```bash
cd clients/typescript
TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run quickstart
```

`TRACEDB_DATABASE_ID` and `TRACEDB_BRANCH_ID` add managed-routing metadata to
JSON POST bodies. `TRACEDB_ADMIN_DIR=/absolute/server/side/path` enables
compact, snapshot, and restore against a server-side local scratch directory;
without it, the quickstart avoids path-based admin writes while still covering
readiness, health, catalog, metrics, schema apply, batch ingest, patch, patched
visibility, scan, query, explain, delete, and admin jobs. The quickstart emits
`sql_module: not_implemented` and remains endpoint example evidence, not SQL
compatibility, managed-cloud backup/DR, or benchmark evidence.

The public TypeScript SDK also has a local gateway smoke:

```bash
cd clients/typescript
npm run gateway-smoke
```

That smoke starts an engine plus a gateway-mode server with
`TRACEDB_REQUIRE_API_KEY=true`, `TRACEDB_API_TOKEN=dev-token`, and
`TRACEDB_ENGINE_URL` pointing at the engine. It runs the public `TraceDB`
wrapper through the gateway with `databaseId=db_local`, `branchId=db_local:main`,
and a local admin scratch path. This is local gateway bearer-auth and
managed-routing evidence for the public TypeScript SDK over the generated
transport, not managed-cloud proof or benchmark evidence.
