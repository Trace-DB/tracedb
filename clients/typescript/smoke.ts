import assert from "node:assert/strict";
import {
  TraceDbClient,
  TraceDbHttpError,
  type HybridQuery,
  type JsonObject,
  type PutBatchResponse,
  type QueryResponse,
  type RecordPutBatchRequest,
  type SnapshotRequest,
  type SnapshotResponse,
  type TableSchema,
  type TraceDbFetchInit,
} from "./src/client.ts";

type FetchCall = {
  input: string;
  init: TraceDbFetchInit;
};

function okJson(value: JsonObject) {
  return {
    ok: true,
    status: 200,
    async text(): Promise<string> {
      return JSON.stringify(value);
    },
  };
}

const calls: FetchCall[] = [];
const client = new TraceDbClient({
  baseUrl: "http://127.0.0.1:8090/",
  token: "dev-token",
  databaseId: "db-default",
  branchId: "branch-default",
  fetchImpl: async (input, init) => {
    calls.push({ input, init });
    return okJson({ ok: true });
  },
});

await client.ready();
assert.equal(calls[0].input, "http://127.0.0.1:8090/v1/ready");
assert.equal(calls[0].init.method, "GET");
assert.equal(calls[0].init.body, undefined);
assert.equal(calls[0].init.headers.Authorization, "Bearer dev-token");
assert.equal(calls[0].init.headers["Content-Type"], undefined);

const schemaBody: TableSchema = {
  name: "docs",
  primary_id_column: "id",
  tenant_id_column: "tenant",
};
await client.applySchema(schemaBody, { idempotencyKey: "schema-1" });
const schemaRequest = JSON.parse(calls[1].init.body ?? "{}");
assert.deepEqual(schemaBody, {
  name: "docs",
  primary_id_column: "id",
  tenant_id_column: "tenant",
});
assert.equal(schemaRequest.database_id, "db-default");
assert.equal(schemaRequest.branch_id, "branch-default");
assert.equal(calls[1].init.headers["Idempotency-Key"], "schema-1");
assert.equal(calls[1].init.headers["Content-Type"], "application/json");

const batchBody: RecordPutBatchRequest = {
  records: [
    {
      table: "docs",
      id: "a",
      tenant_id: "tenant-a",
      fields: { body: "hello" },
    },
  ],
};
const batchResponse: PutBatchResponse = await client.putBatch(batchBody);
assert.equal(batchResponse.ok, true);
assert.equal(JSON.parse(calls[2].init.body ?? "{}").database_id, "db-default");

const queryBody: HybridQuery = {
  table: "docs",
  tenant_id: "tenant-a",
  text: "hello",
  vector: [1, 0, 0],
  top_k: 5,
};
const queryResponse: QueryResponse = await client.query(queryBody);
assert.equal(queryResponse.ok, true);
assert.equal(JSON.parse(calls[3].init.body ?? "{}").vector.length, 3);

const snapshotBody: SnapshotRequest = { target: "/tmp/tracedb-snapshot" };
const snapshotResponse: SnapshotResponse = await client.snapshot(snapshotBody);
assert.equal(snapshotResponse.ok, true);
assert.equal(JSON.parse(calls[4].init.body ?? "{}").target, "/tmp/tracedb-snapshot");

await client.putRecord({
  database_id: "explicit-db",
  branch_id: "explicit-branch",
  record: {
    table: "docs",
    id: "a",
    tenant_id: "tenant-a",
    fields: { body: "hello" },
  },
});
const explicitRoutingRequest = JSON.parse(calls[5].init.body ?? "{}");
assert.equal(explicitRoutingRequest.database_id, "explicit-db");
assert.equal(explicitRoutingRequest.branch_id, "explicit-branch");

const failingClient = new TraceDbClient({
  baseUrl: "http://127.0.0.1:8090",
  fetchImpl: async () => ({
    ok: false,
    status: 503,
    async text(): Promise<string> {
      return "{\"error\":\"down\"}";
    },
  }),
});

await assert.rejects(
  () => failingClient.health(),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbHttpError);
    assert.equal(error.method, "GET");
    assert.equal(error.path, "/v1/health");
    assert.equal(error.status, 503);
    assert.equal(error.body, "{\"error\":\"down\"}");
    return true;
  },
);

console.log("typescript client smoke ok");
