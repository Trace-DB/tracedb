# TraceDB TypeScript Client And SDK

This directory contains a generated, dependency-free TypeScript `fetch` client
for the current TraceDB v1 HTTP API plus a hand-written public SDK wrapper over
that transport.

It is generated from `docs/api/v1-openapi.json` and is checked in so product
smokes and examples can import a stable artifact without waiting for a package
publishing pipeline. It is not a managed-cloud SDK promise and not a published
npm package contract.

The public wrapper lives in `src/sdk.ts`. It intentionally uses the generated
`TraceDbClient` as its transport layer instead of duplicating HTTP routes.
Current public DX starts with `new TraceDB({ url, token })` or
`TraceDB.fromEnv()`, table handles, record writes, batch ingest, patch,
scan/get/delete, admin compact/snapshot/restore, and a query builder that
compiles to the canonical `HybridQuery` body.

The generated artifact also emits TypeScript aliases from the OpenAPI component
schemas and uses them in method signatures. These aliases intentionally preserve
the API's permissive `additionalProperties` boundary: known fields are optional,
unknown JSON fields remain allowed, and runtime validation stays server-side.
They are compile-time ergonomics for the current HTTP API, not strict domain
validators. The generated `RecordPutBody` alias mirrors current server behavior:
`putRecord` accepts either `RecordInput` directly or `{ record: RecordInput }`.
`GetRecordResponse.record` is typed as `RecordOutput | null`, and
`RecordOutput` exposes the server's serialized `version_id` field.
`HybridQuery` now explicitly includes the current JSON request fields
`scalar_eq`, `graph_seed`, and `temporal_as_of`. The scan, query, and explain
response aliases now expose current server fields for
`RecordScanOutput.records`, `RecordScanOutput.returned_count`,
`QueryResponse.results`, optional `QueryResponse.explain`, `HybridQueryRow`,
`HybridScoreComponents`, access-path explain entries, planner candidates, and
timing entries.
Those fields are still optional compile-time helpers rather than strict runtime
validators.

Regenerate or check it from the repo root:

```bash
python3 scripts/generate_typescript_client.py
python3 scripts/generate_typescript_client.py --check
```

Run the local dependency-free Node smoke:

```bash
node --experimental-strip-types clients/typescript/smoke.ts
cd clients/typescript
npm run public-smoke
```

The smoke imports `src/client.ts`, uses a fake `fetchImpl`, verifies generated
schema aliases typecheck in representative schema, batch, query, and admin
calls, verifies GET routes send no body, verifies POST routing metadata is added
without mutating caller objects, verifies explicit `database_id` / `branch_id`
request fields win, checks `Idempotency-Key`, and checks `TraceDbHttpError`
method/path/status/body context plus parsed JSON error-envelope fields,
including stable machine-readable error `code` exposed as `responseCode` when
present. It also verifies the client rejects empty or CR/LF-containing
idempotency keys as
`TraceDbRequestError` before `fetchImpl` is called, and verifies current
scan/query/explain response aliases typecheck.
It also typechecks the read-only product response aliases for health,
readiness, database catalog, branch catalog, public-safe metrics, and admin
jobs.
This is runtime smoke coverage for the checked artifact, not a package
publishing pipeline.

The public SDK smoke imports `src/sdk.ts` and verifies the first table/query
wrapper layer against a fake transport. It checks `TraceDB.fromEnv()` for
`TRACEDB_URL`, `TRACEDB_TOKEN`, `TRACEDB_DATABASE_ID`, `TRACEDB_BRANCH_ID`, and
`TRACEDB_TIMEOUT_MS`, then checks table-scoped `insert`, `insertBatch`, `patch`,
`get`, `scan`, `delete`, admin
compact/snapshot/restore, and query-builder chaining via `where({ tenant_id })`,
`match`, `near`, `with`, `limit`, `all`, and `explainPlan`. It also checks
missing-tenant validation raises `TraceDbRequestError` before `fetchImpl` is
called. This is public-DX smoke coverage over the generated transport, not
publishing readiness or managed-cloud proof.

Run the real local HTTP smoke from the TypeScript package directory:

```bash
cd clients/typescript
npm run http-smoke
```

The HTTP smoke starts a local `tracedb-server` child process with an isolated
temporary data directory, waits for readiness, then drives the generated client
through health, catalog, metrics, schema apply, direct put, batch ingest, get,
scan, query, explain, delete, compact, snapshot, restore, and admin jobs. It
emits a JSON summary and `typescript client http smoke ok`. This is local
loopback product evidence for the generated artifact, not a package publishing
pipeline, managed-cloud health, or benchmark evidence.

Run the public SDK wrapper against a real local HTTP server:

```bash
cd clients/typescript
npm run public-http-smoke
npm run public-http-smoke -- --summary-json /tmp/tracedb-typescript-sdk-smoke.json
```

The public HTTP smoke starts a local `tracedb-server` child process with an
isolated temporary data directory, waits for readiness, then drives `TraceDB`
and table handles through health, catalog, metrics, schema apply, insert, batch
ingest, patch, get, scan, query, explain, delete, idempotency replay/conflict,
parsed error envelopes, compact, snapshot, restore, and admin jobs. It emits a
JSON summary and `typescript public sdk http smoke ok`. This is local loopback
product evidence for the public wrapper over the generated transport and is the
input for:

```bash
python3 scripts/platform_conformance.py --surface typescript_sdk --summary-json /tmp/tracedb-typescript-sdk-conformance.json
```

This is local loopback conformance evidence for the public wrapper over the
generated transport, not package publishing readiness, managed-cloud health, or
benchmark evidence.

Run the endpoint quickstart against an existing local or managed-style HTTP
endpoint:

```bash
cd clients/typescript
TRACEDB_URL=http://127.0.0.1:8090 TRACEDB_TOKEN=dev-token npm run quickstart
```

Optional routing metadata can be supplied with `TRACEDB_DATABASE_ID` and
`TRACEDB_BRANCH_ID`. Optional local admin coverage can be enabled with
`TRACEDB_ADMIN_DIR=/absolute/server/side/path`; that path is interpreted by the
TraceDB server process and is intended for local scratch use. Without
`TRACEDB_ADMIN_DIR`, the quickstart still checks readiness, health, catalog,
metrics, schema apply, batch ingest, patch, patched visibility, scan, query,
explain, delete, and admin jobs. With `TRACEDB_ADMIN_DIR`, it also compacts,
snapshots, and restores to a separate directory. The quickstart emits a JSON
summary and `typescript client endpoint quickstart ok`. It is a
managed-endpoint example for the generated artifact, not package publishing
readiness, not SQL compatibility, and not benchmark evidence.

Run the public TypeScript SDK wrapper through a local gateway that requires a
bearer token and routes by managed metadata:

```bash
cd clients/typescript
npm run gateway-smoke
```

The gateway smoke starts a local engine plus `tracedb-gateway` mode with
`TRACEDB_REQUIRE_API_KEY=true`, `TRACEDB_API_TOKEN=dev-token`, and
`TRACEDB_ENGINE_URL` pointing at the engine. It then runs `new TraceDB({
url, token, databaseId, branchId })` through schema apply, insert, batch,
patch, get, scan, query, explain, delete, compact, snapshot, restore, jobs,
missing-token rejection, and bad-branch rejection. It emits `typescript public
sdk gateway smoke ok`. This proves bearer-auth forwarding and managed-routing
metadata on the public wrapper against the local gateway path; it is still not
managed-cloud proof or benchmark evidence.

The repo-level local product regression gate also runs this generated client
surface:

```bash
cargo run -p tracedb-cli -- product-regression
cargo run -p tracedb-cli -- product-quickstart
```

That gate runs `npm run check`, `npm run public-http-smoke`, and
`npm run gateway-smoke` as local product regression evidence only. It does not
claim package publishing readiness, SQL compatibility, managed-cloud proof, or
benchmark results. Test-only `--inject-failure STEP` can validate failed-step
JSON, top-level `human_summary`, and nonzero gate behavior without weakening
this TypeScript smoke path. `--report-file PATH` writes the same
product-regression JSON summary to a predictable file while preserving stdout.
That option belongs to the repo-level product-regression gate, not the
TypeScript package scripts themselves. `product-quickstart` is the same
repo-level gate with a default report file at
`target/tracedb/product-quickstart.json`.
Use `product-regression --list-steps` to discover the TypeScript smoke step
names plus `human_summary` and `only_supported` metadata without invoking Node
tooling. The current `--only embedded_demo` mode and dependency-aware
`--only embedded_verify` mode do not run the TypeScript smoke steps.
`--only http_demo` also does not run the TypeScript smoke steps;
it is limited to the self-contained local HTTP demo step. `--only local_doctor`
also does not run the TypeScript smoke steps; it is limited to local endpoint
diagnostics against a managed local server.
`--only rust_sdk_quickstart` also does not run the TypeScript smoke steps; it
is limited to local Rust SDK quickstart verification against a managed local
server.
`--skip-typescript` is for the full product gate and non-TypeScript selectors;
a TypeScript `--only` selector conflicts with --skip-typescript.
`--only typescript_check` runs only `npm run check`, which currently performs
the private package typecheck plus dependency-free generated-client and public
SDK smokes. It does not run `http-smoke`, `gateway-smoke`, `http_demo`, local
`doctor http`, the Rust SDK quickstart, managed-cloud checks, benchmark
controls, or SQL compatibility checks.
`--only typescript_http_smoke` runs only `npm run public-http-smoke`, which
starts its own local `tracedb-server` child process and exercises the public
TypeScript SDK wrapper over the generated transport. It does not run embedded demo/verify,
`http_demo`, local `doctor http`, the Rust SDK quickstart, `typescript_check`,
generated-transport `http-smoke`, TypeScript gateway smoke, managed-cloud
checks, benchmark controls, or SQL compatibility checks.
`--only typescript_gateway_smoke` runs only `npm run gateway-smoke`, which
starts a local engine plus gateway-mode server, requires bearer auth, checks
missing-token and bad-branch rejection, and runs the public TypeScript SDK
wrapper through the gateway with `databaseId=db_local`, `branchId=db_local:main`,
and a local admin scratch dir. It does not run embedded demo/verify, `http_demo`,
local `doctor http`, the Rust SDK
quickstart, `typescript_check`, `http-smoke`, managed-cloud checks, benchmark
controls, or SQL compatibility checks.

Install the local private package tooling and run the typecheck boundary:

```bash
cd clients/typescript
npm ci
npm run typecheck
npm run smoke
npm run public-smoke
npm run http-smoke
npm run public-http-smoke
npm run quickstart
npm run gateway-smoke
npm run check
```

The package is marked `private: true` and exists to typecheck the generated
artifact plus smoke scripts. This is not a package publishing pipeline. It
deliberately does not declare package publishing fields such as `exports`,
`main`, `types`, `files`, or `publishConfig`.

## Local Usage

```ts
import { TraceDB, type TableSchema } from "./src/sdk";

const db = new TraceDB({
  url: "http://127.0.0.1:8090",
  token: "dev-token",
});

const schema: TableSchema = {
  name: "docs",
  primary_id_column: "id",
  tenant_id_column: "tenant",
  scalar_columns: ["status"],
  text_indexed_columns: ["body"],
  vector_columns: [{ name: "embedding", dimensions: 3, source_columns: ["body"] }],
};

await db.ready();
await db.applySchema(schema);

const docs = db.table("docs").tenant("tenant-a");
await docs.insert("a", { body: "hello", embedding: [1, 0, 0] });
await docs.insertBatch([
  { id: "b", fields: { body: "batch", embedding: [0, 1, 0] } },
]);

const result = await db
  .table("docs")
  .where({ tenant_id: "tenant-a", status: "published" })
  .match("body", "hello")
  .near("embedding", [1, 0, 0])
  .with({ explain: true, freshness: "lazy" })
  .limit(20)
  .all();

console.log(result.results);
```

The generated transport remains available for raw route access:

```ts
import { TraceDbClient } from "./src/client";
```

## Managed-Routing Metadata

When `databaseId` or `branchId` is configured, the client copies object-shaped
POST bodies and adds `database_id` and `branch_id` only when those root fields
are absent. Explicit request fields win. GET routes send no JSON body.

```ts
const managedClient = new TraceDbClient({
  baseUrl: "http://127.0.0.1:8090",
  token: "dev-token",
  databaseId: "local",
  branchId: "main",
});

await managedClient.putBatch({
  records: [
    {
      table: "docs",
      id: "a",
      tenant_id: "tenant-a",
      fields: { id: "a", tenant: "tenant-a", body: "hello" },
    },
  ],
});
```

## Idempotency Boundary

Mutation and admin methods accept `TraceDbRequestOptions.idempotencyKey`, which
sends `Idempotency-Key`. Current TraceDB support is local data-dir-backed replay
for mutation/admin routes and survives a clean engine reopen from the same data
directory after a successful local cache write. It is not cross-replica, not
crash-atomic exactly-once, and not managed-cloud exactly-once semantics. The
generated client rejects empty or CR/LF-containing idempotency keys before
network I/O with
`TraceDbRequestError`.

```ts
await client.deleteRecord(
  { table: "docs", tenant_id: "tenant-a", id: "a", tombstone: "user_delete" },
  { idempotencyKey: "delete-a-1" },
);
```

SQL compatibility is not implemented. Internal TraceDB-only runs are development
evidence; exported performance claims require an external control and a number
to beat.
