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

Run the SDK quickstart against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo/data TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000 --safe-retries 1 --idempotency-retries 1 --admin-dir /tmp/tracedb-sdk-demo/admin
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
  current product path, includes typed local admin helpers for compact,
  snapshot, and restore, supports a configurable blocking socket request
  timeout, supports bounded safe retries for health/read routes, supports
  opt-in idempotency-key-gated transient retries for mutation/admin routes, and
  non-2xx SDK errors include request method, request path, HTTP status, and
  response body. It is not yet a full managed/cloud SDK.
- `clients/typescript/src/client.ts` is a generated dependency-free transport
  artifact for the current HTTP API. It now includes OpenAPI-derived schema
  aliases and typed method signatures while preserving the API's permissive
  additional-properties boundary. It is not a published npm package, not a
  hand-maintained managed SDK, not a strict runtime validator, and not a broader
  SDK compatibility claim. Its current runtime smoke uses Node's experimental
  TypeScript strip support. The private package under `clients/typescript`
  exists only for local typechecking plus fake-fetch and real local HTTP smoke
  validation; it does not declare package publishing fields. It rejects empty or
  CR/LF-containing `idempotencyKey` request options before `fetchImpl` is called.
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
