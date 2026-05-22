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
| `traceql_string_execution` | TraceQL/string execution is a future adapter over the same query model, not a separate engine. |
| `result_envelope` | Success responses are route-specific JSON; errors preserve the current `{ "error": string, "code"?: string }` envelope plus SDK context. |
| `explain_provenance_freshness_jobs` | Query/explain surfaces share `HybridExplain` fields for access paths, planner candidates, counters, timings, and freshness/provenance evidence as they mature. |
| `errors_retries_idempotency` | Safe retries stay read-only. Mutation/admin retries require caller-provided `Idempotency-Key`; same key/body replays, body mismatch returns `409`. |
| `pagination_cursors` | Pagination/cursors are a named future contract. Today scan returns `records` and `returned_count` with no cursor metadata. |
| `admin_compact_snapshot_restore` | Compact, snapshot, restore, and admin-jobs visibility are part of the platform path, with local filesystem boundaries explicit. |

## Current Surface Matrix

| Surface | ID | Current status | Contract role |
| --- | --- | --- | --- |
| HTTP direct | `http_direct` | Current | Canonical wire contract. |
| Rust SDK | `rust_sdk` | Reference candidate | Ergonomic reference SDK over the wire contract while preserving raw HTTP methods. |
| TypeScript SDK | `typescript_sdk` | Public wrapper started | Hand-written `TraceDB` table/query wrapper over the generated transport. |
| Python SDK | `python_sdk` | Sync HTTP smoked | Sync-first AI/data/notebook SDK over the canonical HTTP contract. |
| TraceQL / SQL-ish | `traceql_sqlish` | Parked | Future adapter into the same TraceQuery/query model. |
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
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
```

The `http_direct` lane uses raw stdlib HTTP requests against `tracedb-server`.
The `rust_sdk` lane maps the existing Rust SDK quickstart product path into the
same manifest scenario IDs. The current executable lanes cover all required v0
scenarios for HTTP direct and Rust SDK, including single-record put and parsed
error-envelope evidence. The `python_sdk` lane runs the sync SDK smoke in
`clients/python/http_smoke.py` and maps schema apply, put, batch, patch, get,
scan, query, explain, delete, idempotency, errors, and snapshot/restore into
the same scenario IDs. Future surfaces must report unimplemented scenarios as
`not_checked` rather than silently treating them as success.

The Rust SDK also has a first ergonomic reference layer over the same wire
contract: `TraceDbClient::table("docs").tenant("tenant-a")` can execute table
insert, batch insert, get, scan, and delete calls and build query requests with
`where_eq`, `match_text`, `near`, `with_explain`, `limit`, and `all()`. These
helpers compile into the existing `RecordInput`, `RecordPutBatchRequest`, record
request, and `HybridQuery` models; the raw HTTP methods remain available.

The TypeScript package now starts the public SDK layer in
`clients/typescript/src/sdk.ts`. `new TraceDB({ url, token })` wraps the
generated `TraceDbClient` transport and exposes table handles with
`insert`, `insertBatch`, `patch`, `get`, `scan`, `delete`, admin
compact/snapshot/restore/jobs, `where`, `match`, `near`, `with`, `limit`, `all`,
and `explainPlan`. The wrapper is fake-fetch/typecheck guarded and now has real
local HTTP and gateway smokes through `npm run public-http-smoke` and
`npm run gateway-smoke`; the generated transport remains available and remains
the source of route methods.

The Python package now starts the sync-first AI/data SDK lane in
`clients/python/tracedb/client.py`. `TraceDB(url, token="dev-token")` exposes
table handles and a query builder with `insert`, `insert_batch`, `patch`, `get`,
`scan`, `delete`, `where`, `match_text`, `near`, `with_options`, `limit`,
`all`, and `explain_plan`, plus health/catalog/metrics/admin helpers. The
stdlib-only smoke `python3 clients/python/http_smoke.py` starts a local
`tracedb-server` and proves all required v0 contract scenarios through the
Python surface. It is sync SDK contract evidence, not package publishing
readiness, async support, managed-cloud proof, SQL compatibility, or GraphQL
support.

## Surface Implementation Rules

- HTTP direct remains the source of wire truth.
- Rust SDK is the reference ergonomic SDK. It may add table handles and builder
  APIs, but raw HTTP methods must remain available.
- TypeScript's generated client is a transport layer, not the final public
  product shape.
- Python starts sync-first for ingestion, AI workflows, notebooks, and platform
  conformance tests. Async can follow after sync parity.
- TraceQL / SQL-ish must parse into the same query model. Do not claim SQL
  compatibility or PostgreSQL compatibility.
- GraphQL must be schema-generated from TraceDB schema and compile into the same
  query model. It should not own database semantics.

## Verification Ladder

Use the smallest ladder that proves the touched surface:

```bash
cargo test -p tracedb-testkit --test usability_acceptance platform_contract_v0_declares_sdk_conformance_harness -- --exact
python3 scripts/platform_conformance.py --surface http_direct --surface rust_sdk --summary-json /tmp/tracedb-platform-conformance.json
python3 scripts/platform_conformance.py --surface python_sdk --summary-json /tmp/tracedb-python-sdk-conformance.json
python3 scripts/generate_openapi_v1.py --check
python3 scripts/generate_typescript_client.py --check
cargo run -p tracedb-cli -- product-quickstart --skip-typescript
modal run scripts/modal_product_verify.py --mode quickstart --summary-json /tmp/tracedb-modal-product-quickstart.json
```

Local macOS executable-policy failures are local environment evidence, not a
contract failure. Use Modal for remote Linux product verification when the local
machine cannot execute Rust binaries.
