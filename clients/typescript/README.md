# TraceDB TypeScript Client Artifact

This directory contains a generated, dependency-free TypeScript `fetch` client
for the current TraceDB v1 HTTP API.

It is generated from `docs/api/v1-openapi.json` and is checked in so product
smokes and examples can import a stable artifact without waiting for a package
publishing pipeline. It is not a managed-cloud SDK promise and not a published
npm package contract.

The generated artifact also emits TypeScript aliases from the OpenAPI component
schemas and uses them in method signatures. These aliases intentionally preserve
the API's permissive `additionalProperties` boundary: known fields are optional,
unknown JSON fields remain allowed, and runtime validation stays server-side.
They are compile-time ergonomics for the current HTTP API, not strict domain
validators. The generated `RecordPutBody` alias mirrors current server behavior:
`putRecord` accepts either `RecordInput` directly or `{ record: RecordInput }`.
`GetRecordResponse.record` is typed as `RecordOutput | null`, and
`RecordOutput` exposes the server's serialized `version_id` field. The scan,
query, and explain response aliases now expose current server fields for
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
```

The smoke imports `src/client.ts`, uses a fake `fetchImpl`, verifies generated
schema aliases typecheck in representative schema, batch, query, and admin
calls, verifies GET routes send no body, verifies POST routing metadata is added
without mutating caller objects, verifies explicit `database_id` / `branch_id`
request fields win, checks `Idempotency-Key`, and checks `TraceDbHttpError`
method/path/status/body context. It also verifies the client rejects empty or
CR/LF-containing idempotency keys as `TraceDbRequestError` before `fetchImpl` is
called, and verifies current scan/query/explain response aliases typecheck.
This is runtime smoke coverage for the checked artifact, not a package
publishing pipeline.

Run the real local HTTP smoke from the TypeScript package directory:

```bash
cd clients/typescript
npm run http-smoke
```

The HTTP smoke starts a local `tracedb-server` child process with an isolated
temporary data directory, waits for readiness, then drives the generated client
through schema apply, direct put, batch ingest, get, scan, query, explain,
delete, compact, snapshot, restore, and admin jobs. It emits a JSON summary and
`typescript client http smoke ok`. This is local loopback product evidence for
the generated artifact, not a package publishing pipeline, managed-cloud health,
or benchmark evidence.

Install the local private package tooling and run the typecheck boundary:

```bash
cd clients/typescript
npm ci
npm run typecheck
npm run smoke
npm run http-smoke
npm run check
```

The package is marked `private: true` and exists to typecheck the generated
artifact plus smoke script. This is not a package publishing pipeline. It
deliberately does not declare package publishing fields such as `exports`,
`main`, `types`, `files`, or `publishConfig`.

## Local Usage

```ts
import { TraceDbClient, type TableSchema } from "./src/client";

const client = new TraceDbClient({
  baseUrl: "http://127.0.0.1:8090",
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

await client.ready();
await client.applySchema(schema);
await client.putRecord({
  table: "docs",
  id: "a",
  tenant_id: "tenant-a",
  fields: { body: "hello" },
});
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
sends `Idempotency-Key`. Current TraceDB support is local in-process replay for
mutation/admin routes. It is not durable across restart/crash, not cross-replica,
and not exactly-once managed-cloud semantics. The generated client rejects empty
or CR/LF-containing idempotency keys before network I/O with
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
