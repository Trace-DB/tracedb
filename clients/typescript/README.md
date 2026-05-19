# TraceDB TypeScript Client Artifact

This directory contains a generated, dependency-free TypeScript `fetch` client
for the current TraceDB v1 HTTP API.

It is generated from `docs/api/v1-openapi.json` and is checked in so product
smokes and examples can import a stable artifact without waiting for a package
publishing pipeline. It is not a managed-cloud SDK promise and not a published
npm package contract.

Regenerate or check it from the repo root:

```bash
python3 scripts/generate_typescript_client.py
python3 scripts/generate_typescript_client.py --check
```

Run the local dependency-free Node smoke:

```bash
node --experimental-strip-types clients/typescript/smoke.ts
```

The smoke imports `src/client.ts`, uses a fake `fetchImpl`, verifies GET routes
send no body, verifies POST routing metadata is added without mutating caller
objects, verifies explicit `database_id` / `branch_id` request fields win,
checks `Idempotency-Key`, and checks `TraceDbHttpError` method/path/status/body
context. This is runtime smoke coverage for the checked artifact, not a package
publishing pipeline.

## Local Usage

```ts
import { TraceDbClient } from "./src/client";

const client = new TraceDbClient({
  baseUrl: "http://127.0.0.1:8090",
  token: "dev-token",
});

await client.ready();
await client.applySchema({
  name: "docs",
  primary_id_column: "id",
  tenant_id_column: "tenant",
  scalar_columns: ["status"],
  text_indexed_columns: ["body"],
  vector_columns: [{ name: "embedding", dimensions: 3, source_columns: ["body"] }],
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
and not exactly-once managed-cloud semantics.

```ts
await client.deleteRecord(
  { table: "docs", tenant_id: "tenant-a", id: "a", tombstone: "user_delete" },
  { idempotencyKey: "delete-a-1" },
);
```

SQL compatibility is not implemented. Internal TraceDB-only runs are development
evidence; exported performance claims require an external control and a number
to beat.
