---
title: TraceDB v1 HTTP API Reference
tags:
  - tracedb
  - api
  - http
status: current-product-surface
type: api-reference
updated: 2026-05-25
---

# TraceDB v1 HTTP API Reference

TraceDB is an AI-native transactional candidate-stream database.
One logical record. One commit epoch. Many native views. No external sync
drift. Explain every candidate.

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

SDK transport artifacts generated from this OpenAPI contract are maintained in
the sibling standalone SDK repositories, especially `../tracedb-js` for the
TypeScript/JavaScript transport and public SDK. The authoritative HTTP
contract and OpenAPI artifact live in `../tracedb-protocol`; this core mirror
validates against `tracedb-protocol.lock` and the protocol artifacts.
SDK package checks and SDK smokes run outside the core product gate.

## Boundaries

- SQL compatibility is not implemented.
- TraceField is the memory/runtime research program and future runtime context,
  not a current HTTP product surface or implemented runtime in this repo.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require an external control and a number to beat.
- SDK implementations are external to this core repo. Rust SDK work lives in
  `../tracedb-rust`, Python SDK work lives in `../tracedb-python`, and
  TypeScript/JavaScript SDK work lives in `../tracedb-js`. Those repos own
  package metadata, generated/client artifacts, SDK quickstarts, SDK smokes,
  and SDK conformance against this HTTP contract.
- SDK safe retries apply only to routes or operation payloads that are provably
  read-only. Blanket safe-retry routes are `GET /v1/health`, `GET /v1/ready`,
  `GET /v1/graphql/schema`, `POST /v1/records/get`,
  `POST /v1/records/scan`, `POST /v1/query`, `POST /v1/explain`, and
  `POST /v1/graphql/bounded`. Native `POST /v1/traceql` and
  `POST /v1/graphql` are polymorphic operation routes: retry only when the
  payload is provably read-only, or when the caller supplies `Idempotency-Key`
  and idempotency retry is enabled.
- Idempotency-Key supports local data-dir replay from WAL/checkpoint-backed
  idempotency receipts for mutation, admin, and polymorphic native operation
  routes. Same key plus same
  method/path/raw body replays the first successful response; same key with a
  different body returns `409 Conflict`.
- The idempotency authority is local-engine-only and scoped to the same data
  directory. It survives a clean engine reopen from that data directory, but it
  is not cross-replica, not crash-atomic exactly-once, and not a managed-cloud
  exactly-once guarantee. The full local WAL, manifest, checkpoint,
  snapshot/restore, lock-file, TDE, and idempotency boundary is documented in
  `docs/durability-semantics-v0.md`.
- The generated TypeScript client rejects empty or CR/LF-containing
  `idempotencyKey` options before network I/O as `TraceDbRequestError`.
- The Rust SDK can manually send `Idempotency-Key` with
  `TraceDbRequestOptions` on individual requests. `safe_retries` still applies
  only to provably read-only routes or operation payloads. `idempotency_retries`
  is a separate opt-in policy for transient 5xx/timeout retries on mutating,
  admin, or polymorphic native operation routes and is only active when the
  individual request includes an `Idempotency-Key`.
- Gateway metering, request logging, and rate limiting may still observe each
  HTTP attempt.

## Current HTTP Stack Boundary

The current HTTP stack boundary is explicit: `tracedb-server` and
`tracedb-gateway` default to Tokio/Axum product paths with Tower body limits,
timeouts, load shedding, concurrency limits, graceful shutdown, structured
JSON tracing, and private engine-token enforcement where configured. Engine
mode uses an async handle with serialized writes/admin work and cheap read
snapshots, so health, readiness, and public-safe metrics do not wait behind
long query execution.

Requests are bounded by the configured Axum/Tower layers. Legacy stdlib
listener helpers remain for compatibility tests and local harnesses, but they
are not the production `serve()` path. The current server path does not provide
TLS or HTTP/2, does not implement a full public-internet proxy feature set, and
is not a complete managed-service runtime.

## Transport

Requests and responses are JSON over HTTP/1.1. POST routes expect
`Content-Type: application/json`. The local engine accepts requests directly;
the gateway accepts the same product routes and forwards authorized requests to
the engine.

The minimal SDK can add `database_id` and `branch_id` fields to object-shaped
POST bodies when configured for managed routing. If only `database_id` is
configured, SDKs default the copied request's `branch_id` to
`<database_id>:main`. Direct engine-local requests can omit those fields.

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
`database_id` and `branch_id` fields on copied JSON POST bodies. If
`databaseId` is configured and `branchId` is not, the copied POST body defaults
`branch_id` to `<database_id>:main`. Explicit request fields win, the caller's
object is not mutated, and GET routes send no JSON body.

The Python SDK follows the same routing metadata boundary: configured
`database_id` and `branch_id` are added only to absent root `database_id` and
`branch_id` fields on copied JSON POST bodies. If `database_id` is configured
and `branch_id` is not, the copied POST body defaults `branch_id` to
`<database_id>:main`. Explicit request fields win, the caller-provided object is
not mutated, and GET routes send no JSON body.

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
| `POST /v1/schema/apply` | `TableSchema`: `name`, `primary_id_column`, `tenant_id_column`, scalar columns, text-indexed columns, and vector columns. Names must be GraphQL-safe identifiers. The server rejects duplicate columns, overlapping scalar/text/vector columns, reserved TraceDB result metadata fields, zero-dimension vectors, and undeclared or duplicate vector source columns before WAL append. | `{ "epoch": number }`. |
| `POST /v1/insert` | `RecordInput`: `table`, `id`, `tenant_id`, and `fields`. | `{ "epoch": number }`. Kept for compatibility; prefer records routes for product examples. |
| `POST /v1/records/put` | Either `RecordInput` directly or `{ "record": RecordInput }`. Full replacement write. | `{ "epoch": number }`. |
| `POST /v1/records/put-batch` | `RecordPutBatchRequest`: `records` plus optional `include_write_timing`. | `{ "epoch": number, "record_count": number }`; includes `write_timing` when requested. |
| `POST /v1/records/patch` | `RecordPatchRequest`: `table`, `tenant_id`, `id`, and patch `fields`. | `{ "epoch": number }`. |
| `POST /v1/records/delete` | `RecordDeleteRequest`: `table`, `tenant_id`, `id`, and optional `tombstone`. | `{ "deleted": true, "epoch": number }`. |

Write routes allocate epochs and mutate TraceDB state. They accept optional
`Idempotency-Key` for local data-dir-backed replay. SDK idempotency retry
options are default-off and only retry writes when the individual request
includes an `Idempotency-Key`; `safe_retries` alone never retries mutating
writes.

## Reads And Retrieval

| Route | Request | Response |
| --- | --- | --- |
| `POST /v1/records/get` | `RecordGetRequest`: `table`, `tenant_id`, and `id`. | `{ "record": RecordOutput \| null }`. |
| `POST /v1/records/scan` | `RecordScanRequest`: `table`, `tenant_id`, optional `limit`, and optional opaque `cursor` from the prior page. | `RecordScanOutput` with `records: RecordOutput[]`, `returned_count`, and optional `next_cursor`. Records are ordered by `record_id`; omit `cursor` for the first page. |
| `POST /v1/query` | `HybridQuery`: `table`, `tenant_id`, optional opaque `cursor`, optional `text_field`, optional `text`, optional `vector_field`, optional `vector`, optional `scalar_eq`, optional `graph_seed`, optional `temporal_as_of`, `top_k`, `freshness`, and `explain`. `freshness` accepts `Strict`, `Lazy`, or `AllowDirty`; SDKs also canonicalize lowercase forms such as `allow_dirty`. `text_field` selects one schema text-indexed column; if omitted, text search spans all text-indexed columns. `vector_field` selects one schema vector column; if omitted, vector scoring uses the first vector column for backwards-compatible fieldless queries. | With `explain: false`, returns `{ "results": HybridQueryRow[], "next_cursor"?: string }`; with `explain: true`, returns results plus `HybridExplain` metadata and optional `next_cursor`. |
| `POST /v1/traceql` | `TraceQlQueryRequest`: `{ "query": string }`, where `query` is either read-only native line-oriented TraceQL or a TraceDB command statement. `GET`, `SCAN`, `QUERY`, `EXPLAIN`, and `JOBS LIST` are read-only; `SCHEMA APPLY`, `PUT`, `BATCH`, `PATCH`, `DELETE`, `SNAPSHOT`, and `RESTORE` mutate data or admin state. | Query strings return the same result shape as `POST /v1/query`; command statements return the canonical route-specific JSON body for the operation. This polymorphic route is not blanket safe-retry; mutating/admin commands should use `Idempotency-Key` for idempotency retries. This is not SQL or PostgreSQL compatibility. |
| `GET /v1/graphql/schema` | No body. Generates SDL from currently applied `TableSchema` definitions. Rust SDK callers can use `TraceDbClient::graphql_schema`, `TraceDbClient::graphql_schema_typed`, or `TraceDbAsyncClient::graphql_schema_typed`; TypeScript SDK callers can use `TraceDB.graphqlSchema()`; Python SDK callers can use `TraceDB.graphql_schema()`. | `GraphQlSchemaResponse` with `adapter`, generated `schema` SDL, `tables`, and compatibility execution notes. This SDL export is retained for the bounded adapter and schema discovery. |
| `POST /v1/graphql` | `GraphQlQueryRequest`: `{ "query": string, "variables"?: object, "operationName"?: string }`. Native TraceDB operations use one root field. `get`, `scan`, `query`, `explain`, and `jobs` are read-only; `schemaApply`, `put`, `batch`, `patch`, `delete`, `compact`, `snapshot`, `restore`, and `jobRun` mutate data or admin state. Fields that need an operation body take an `input` JSON string argument. Rust SDK callers can use `TraceDbClient::graphql_typed` or `graphql_request_typed`; TypeScript SDK callers can use `TraceDB.graphql()` or `graphqlRequest({ query })`; Python SDK callers can use `TraceDB.graphql()` or `graphql_request({"query": query})`. | Standard GraphQL-style `{ "data": ..., "errors": ... }` response envelope. This polymorphic route is not blanket safe-retry; mutating/admin root fields should use `Idempotency-Key` for idempotency retries. Unsupported fields and operation failures return `errors` with `extensions.code = "TRACEDB_GRAPHQL_ERROR"`. Subscriptions remain unsupported. |
| `POST /v1/graphql/bounded` | `GraphQlQueryRequest`: `{ "query": string }`, where `query` is the bounded GraphQL adapter form with one root table field and arguments such as `tenant_id`, `where`, `match_field`, `match`, `near_field`, `near`, `limit`, `freshness`, and `explain`. Rust SDK callers can use `bounded_graphql_typed`; TypeScript SDK callers can use `TraceDB.boundedGraphql()`; Python SDK callers can use `TraceDB.bounded_graphql()`. | Same result shape as `POST /v1/query`. This route is compatibility-only and does not satisfy native GraphQL production gates. |
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
| `POST /v1/admin/restore` | `{ "source": "/path/to/snapshot", "target": "/path/to/restore" }`, optionally with `verify_record` as a `RecordGetRequest`. | `{ "restored": true, "source": string, "target": string }`, optionally with `verification: { "status": "passed"|"failed", "record_visible": boolean, "request": RecordGetRequest, "record": RecordOutput|null }`. |
| `GET /v1/admin/jobs` | No body. | Idle job queue state for segment compaction, snapshot creation, and feature indexing. |

Admin routes can mutate durable files or create out-of-band filesystem state.
They accept optional `Idempotency-Key` for local data-dir replay from
WAL/checkpoint-backed idempotency receipts. Replay survives a clean engine
reopen from the same data directory, but the contract is not cross-replica,
not managed-cloud exactly-once, and not crash-atomic exactly-once. The SDK
idempotency retry options are default-off and only retry admin requests when
the individual request includes an `Idempotency-Key`; `safe_retries` alone never
retries admin requests.

The Rust SDK provides typed `SnapshotRequest`, `SnapshotResponse`,
`RestoreRequest`, `RestoreResponse`, and optional `RestoreVerification` wrappers
plus raw/typed snapshot and restore helpers over these local admin routes. The
wire contract remains JSON string paths plus an optional restored-record check.

## Minimal Product Path

Run the consolidated local product regression gate with one command:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
cargo run -p tracedb-cli -- durability-faults
```

The gate emits one `local-product-regression` JSON summary for the embedded
demo/verify path, local HTTP demo, and endpoint doctor, with a compact top-level
`human_summary` for quick operator scanning. It is local core product regression
evidence only: SQL remains not implemented, managed-cloud is not checked,
benchmarks are not checked, and SDK conformance is externally owned. Failure
ergonomics for the consolidated local gate are covered by test-only
`--inject-failure STEP`, which preserves the JSON summary on stdout and exits
nonzero. Operators can pass `--report-file PATH` to write that same JSON summary
to a predictable file while preserving JSON stdout; parent directories are
created. Operators can run `product-regression --list-steps` to discover valid
core gate steps for failure injection and CI orchestration; it emits JSON step
metadata including `human_summary` and `only_supported` and does not run demo,
HTTP, or SDK smoke steps.
`product-quickstart` runs the same local core product gate with a default report
file at `target/tracedb/product-quickstart.json`, preserves JSON stdout, and
includes the resolved artifact path in the top-level `report_file` field.
Operators can validate the local quickstart receipt by checking that artifact
for `ok: true`, `mode: "local-product-regression"`, `scope: "local_only"`,
`human_summary.status: "passed"`, `claims.sql_module: "not_implemented"`,
`claims.managed_cloud: "not_checked"`, and `claims.benchmark: "not_checked"`.
`product-quickstart --inject-failure embedded_demo` validates the failure
receipt path without running later product steps: the command exits nonzero,
writes the same default report artifact, keeps `report_file`, reports
`human_summary.status: "failed"`, and records the injected `embedded_demo`
failure.
`durability-faults` writes `target/tracedb/durability-faults.json` and emits
`mode: "local-durability-faults"` with `claims.tde_scope:
"local_artifacts_when_configured"`. It covers wrong/missing master key, torn
WAL tail, manifest/checkpoint corruption, stale-lock recovery, encrypted
snapshot restore, and WAL idempotency replay after reopen. This is local
durability evidence, not managed-cloud backup/DR evidence.
`product-regression --only embedded_demo` runs only the embedded demo step and
emits one-step `local-product-regression` JSON. `product-regression --only embedded_verify`
verifies an existing embedded demo data root and should be run with the same
`--data-root` used for `--only embedded_demo`.
`product-regression --only http_demo` runs the self-contained local HTTP demo
step and emits the normal one-step `local-product-regression` JSON summary. It
does not run local `doctor http`, SDK conformance, managed-cloud checks,
benchmark controls, or SQL compatibility checks.
`product-regression --only local_doctor` starts a managed-style local loopback
`tracedb-server` child process and runs only the existing local `doctor http`
product-regression step with readiness wait, `database_id`, and `branch_id`
metadata. It emits the normal one-step `local-product-regression` JSON summary
with `only_step: "local_doctor"`. This is local endpoint diagnostics evidence
only; it does not run `http_demo`, SDK conformance, managed-cloud checks,
benchmark controls, or SQL compatibility checks.

SDK conformance is owned by `../tracedb-rust`, `../tracedb-python`, and
`../tracedb-js`. Run SDK quickstarts, SDK HTTP smokes, package checks, and SDK
Platform Contract evidence from those standalone repos.

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

SDK quickstarts and language-specific endpoint examples live in the sibling
standalone repositories: `../tracedb-rust`, `../tracedb-python`, and
`../tracedb-js`.
