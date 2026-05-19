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

Run the SDK quickstart against a local HTTP server:

```bash
TRACEDB_DATA_DIR=/tmp/tracedb-sdk-demo TRACEDB_BIND=127.0.0.1:8090 cargo run -p tracedb-server
```

In a second terminal:

```bash
cargo run -p tracedb-sdk --example quickstart -- --url http://127.0.0.1:8090 --token dev-token --timeout-ms 5000 --safe-retries 1
```

The SDK example applies schema, batch-ingests records, scans, queries, explains,
deletes, verifies deleted-record hiding, and reports `sql_module:
not_implemented`. The example uses typed SDK convenience methods over the
current HTTP response shapes and accepts a configurable SDK request timeout; the
original raw `serde_json::Value` methods remain available. Bounded safe retries
are available for SDK health/read routes only. Callers can manually attach
`Idempotency-Key` per write/admin request with `TraceDbRequestOptions`; the SDK
does not automatically retry those routes.

The current versioned HTTP route reference is in `docs/api/v1-http.md`; the
machine-readable OpenAPI artifact is `docs/api/v1-openapi.json`.

## Current Boundaries

- SQL compatibility is not implemented.
- The Rust `tracedb-sdk` crate now includes a minimal HTTP client for the
  current engine API plus the original request-builder helpers. It can attach
  managed `database_id` and `branch_id` routing metadata to JSON POST bodies,
  includes typed convenience response methods and typed query rows for the
  current product path, supports a configurable blocking socket request timeout,
  supports bounded safe retries for health/read routes, and non-2xx SDK errors
  include request method, request path, HTTP status, and response body. It is
  not yet a full managed/cloud SDK.
- HTTP mutation and admin routes accept optional `Idempotency-Key` for local
  in-process replay on the engine, and the gateway forwards that header. This
  is not durable across restart/crash, not cross-replica, and does not enable
  automatic SDK write/admin retries yet. The Rust SDK can manually send the
  header per request through `TraceDbRequestOptions`.
- Internal TraceDB-only runs are development evidence. Exported performance
  claims still require external controls and a number to beat.
