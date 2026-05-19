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
```

## Boundaries

- SQL compatibility is not implemented.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require an external control and a number to beat.
- The current Rust SDK is a minimal blocking HTTP client for this API surface.
- The TypeScript client under `clients/typescript/src/client.ts` is a generated
  dependency-free `fetch` client artifact for this API surface. It is not a
  published npm package, not a managed-cloud SDK promise, and not a SQL
  compatibility claim. Its local runtime smoke uses Node's experimental
  TypeScript strip support. The private package under `clients/typescript`
  exists only for local typechecking and smoke validation; it does not declare
  package publishing fields.
- SDK safe retries apply only to health/read routes that do not mutate TraceDB
  data state: `GET /v1/health`, `GET /v1/ready`, `POST /v1/records/get`,
  `POST /v1/records/scan`, `POST /v1/query`, and `POST /v1/explain`.
- Idempotency-Key supports local in-process replay for mutation and admin routes.
  Same key plus same method/path/raw body replays the first successful
  response; same key with a different body returns `409 Conflict`.
- The idempotency cache is local-engine-only and in-process. It is not durable
  across restart/crash, not cross-replica, and not a managed-cloud exactly-once
  guarantee.
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

The generated TypeScript client follows the same routing metadata boundary:
configured `databaseId` and `branchId` are added only to absent root
`database_id` and `branch_id` fields on copied JSON POST bodies. Explicit
request fields win, the caller's object is not mutated, and GET routes send no
JSON body.

## Health And Catalog

| Route | Request | Response |
| --- | --- | --- |
| `GET /v1/health` | No body. | Engine or gateway health JSON with `ok` and `service`. |
| `GET /v1/ready` | No body. | Readiness JSON with `ready`, `service`, and engine epoch or gateway health-check context. |
| `GET /v1/databases` | No body. | Local mode returns a `local` database; gateway mode returns catalog databases. |
| `GET /v1/branches` | No body. | Local mode returns the active branch; gateway mode returns catalog branches. |
| `GET /v1/metrics/public-safe` | No body. | Public-safe service, epoch, segment/index/module/schema, recovery, request, or rate-limit counters. |

Local compatibility aliases also exist for development: `GET /health`,
`GET /ready`, and `GET /metrics`.

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
`Idempotency-Key` for local in-process replay. The SDK only retries writes when
`TraceDbClientConfig::with_idempotency_retries` is enabled and the individual
request includes an `Idempotency-Key`; `safe_retries` alone never retries
mutating writes.

## Reads And Retrieval

| Route | Request | Response |
| --- | --- | --- |
| `POST /v1/records/get` | `RecordGetRequest`: `table`, `tenant_id`, and `id`. | `{ "record": RecordOutput \| null }`. |
| `POST /v1/records/scan` | `RecordScanRequest`: `table`, `tenant_id`, optional `limit`, and optional cursor fields. | `RecordScanOutput` with `records`, `returned_count`, and cursor metadata. |
| `POST /v1/query` | `HybridQuery`: `table`, `tenant_id`, optional `text`, optional `vector`, scalar filters, `top_k`, `freshness`, and `explain`. | With `explain: false`, returns `{ "results": [...] }`; with `explain: true`, returns results plus explain metadata. |
| `POST /v1/explain` | Same query shape as `POST /v1/query`; the server forces explain mode. | `HybridExplain` only. |

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
They accept optional `Idempotency-Key` for local in-process replay, but the
contract is not durable across restart/crash. The SDK only retries admin
requests when `TraceDbClientConfig::with_idempotency_retries` is enabled and the
individual request includes an `Idempotency-Key`; `safe_retries` alone never
retries admin requests.

The Rust SDK provides typed `SnapshotRequest`, `SnapshotResponse`,
`RestoreRequest`, and `RestoreResponse` wrappers plus raw/typed snapshot and
restore helpers over these local admin routes. The wire contract remains JSON
string paths.

## Minimal Product Path

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

The SDK quickstart exercises this path and reports `sql_module:
not_implemented`. Passing `--admin-dir SERVER_SIDE_DIR` also exercises compact,
snapshot, and restore with typed SDK admin helpers. The argument must be an
absolute path interpreted by the server process and is intended as local scratch
space. Passing `--idempotency-retries N` or `TRACEDB_IDEMPOTENCY_RETRIES=N`
demonstrates the keyed write/admin retry path by generating per-run
`Idempotency-Key` values for the quickstart's mutation/admin steps. Restore
creates a separate database directory; it does not replace the running server's
data directory and is not managed-cloud backup/DR semantics.
