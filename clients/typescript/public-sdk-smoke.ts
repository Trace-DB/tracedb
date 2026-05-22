import assert from "node:assert/strict";
import {
  TraceDB,
  TraceDbRequestError,
  type JsonObject,
  type TraceDbFetchInit,
} from "./src/sdk.ts";

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
const db = new TraceDB({
  url: "http://127.0.0.1:8090/",
  token: "dev-token",
  databaseId: "db-default",
  branchId: "branch-default",
  fetchImpl: async (input, init) => {
    calls.push({ input, init });
    if (input.endsWith("/v1/query")) {
      return okJson({ results: [{ record_id: "intro", tenant_id: "tenant-a" }] });
    }
    if (input.endsWith("/v1/records/scan")) {
      return okJson({ records: [], returned_count: 2 });
    }
    if (input.endsWith("/v1/records/get")) {
      return okJson({ record: { id: "intro", table: "docs", tenant_id: "tenant-a", fields: {} } });
    }
    if (input.endsWith("/v1/records/delete")) {
      return okJson({ deleted: true, epoch: 4 });
    }
    if (input.endsWith("/v1/records/put-batch")) {
      return okJson({ epoch: 3, record_count: 2 });
    }
    return okJson({ epoch: 2 });
  },
});

const fields = { body: "hello", embedding: [1, 0, 0] };
const insert = await db
  .table("docs")
  .tenant("tenant-a")
  .insert("intro", fields, { idempotencyKey: "ts-public-put-1" });
assert.equal(insert.epoch, 2);
assert.deepEqual(fields, { body: "hello", embedding: [1, 0, 0] });
assert.equal(calls[0].input, "http://127.0.0.1:8090/v1/records/put");
assert.equal(calls[0].init.headers.Authorization, "Bearer dev-token");
assert.equal(calls[0].init.headers["Idempotency-Key"], "ts-public-put-1");
const insertBody = JSON.parse(calls[0].init.body ?? "{}");
assert.equal(insertBody.database_id, "db-default");
assert.equal(insertBody.branch_id, "branch-default");
assert.equal(insertBody.table, "docs");
assert.equal(insertBody.tenant_id, "tenant-a");
assert.equal(insertBody.id, "intro");
assert.equal(insertBody.fields.id, "intro");
assert.equal(insertBody.fields.tenant, "tenant-a");

const batch = await db.table("docs").tenant("tenant-a").insertBatch([
  { id: "batch-a", fields: { body: "batch a" } },
  { id: "batch-b", fields: { body: "batch b", tenant: "explicit-tenant" } },
]);
assert.equal(batch.record_count, 2);
const batchBody = JSON.parse(calls[1].init.body ?? "{}");
assert.equal(batchBody.records[0].table, "docs");
assert.equal(batchBody.records[0].tenant_id, "tenant-a");
assert.equal(batchBody.records[0].fields.id, "batch-a");
assert.equal(batchBody.records[0].fields.tenant, "tenant-a");
assert.equal(batchBody.records[1].fields.tenant, "explicit-tenant");

const query = await db
  .table("docs")
  .where({ tenant_id: "tenant-a", status: "published" })
  .match("body", "rust sdk")
  .near("embedding", [1, 0, 0])
  .with({ explain: true, freshness: "lazy" })
  .limit(20)
  .all();
assert.equal(query.results?.[0]?.record_id, "intro");
const queryBody = JSON.parse(calls[2].init.body ?? "{}");
assert.equal(queryBody.table, "docs");
assert.equal(queryBody.tenant_id, "tenant-a");
assert.deepEqual(queryBody.scalar_eq, { status: "published" });
assert.equal(queryBody.text, "rust sdk");
assert.deepEqual(queryBody.vector, [1, 0, 0]);
assert.equal(queryBody.freshness, "Lazy");
assert.equal(queryBody.explain, true);
assert.equal(queryBody.top_k, 20);

await db.table("docs").tenant("tenant-a").get("intro");
await db.table("docs").tenant("tenant-a").limit(5).scan();
await db.table("docs").tenant("tenant-a").delete("intro", {
  idempotencyKey: "ts-public-delete-1",
  tombstone: "public_sdk_smoke",
});

const callCount = calls.length;
await assert.rejects(
  () => db.table("docs").match("body", "missing tenant").all(),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "POST");
    assert.equal(error.path, "/v1/query");
    assert.match(error.message, /tenant/);
    return true;
  },
);
assert.equal(calls.length, callCount, "missing tenant should reject before fetch");

console.log("typescript public sdk smoke ok");
