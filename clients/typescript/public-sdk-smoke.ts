import assert from "node:assert/strict";
import {
  TraceDB,
  TraceDbRequestError,
  type GraphQlQueryRequest,
  type GraphQlSchemaResponse,
  type JsonObject,
  type TraceDbFetchInit,
} from "./src/sdk.ts";

type FetchCall = {
  input: string;
  init: TraceDbFetchInit;
};

function okJson(value: JsonObject) {
  return responseJson(200, value);
}

function responseJson(status: number, value: JsonObject) {
  return {
    ok: status >= 200 && status < 300,
    status,
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
    if (input.endsWith("/v1/traceql")) {
      return okJson({
        results: [{ record_id: "intro", tenant_id: "tenant-a" }],
        explain: { returned_count: 1 },
      });
    }
    if (input.endsWith("/v1/graphql")) {
      return okJson({
        results: [{ record_id: "intro", tenant_id: "tenant-a" }],
        explain: { returned_count: 1 },
      });
    }
    if (input.endsWith("/v1/graphql/schema")) {
      return okJson({
        adapter: "bounded_graphql_query_adapter",
        schema: "type Query {\n  docs(tenant_id: String!, limit: Int): [docs!]!\n}\n",
        tables: ["docs"],
        execution: "POST /v1/graphql returns TraceDB QueryResponse, not a GraphQL data envelope",
      });
    }
    if (input.endsWith("/v1/explain")) {
      return okJson({ returned_count: 2 });
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
    if (input.endsWith("/v1/records/patch")) {
      return okJson({ epoch: 5 });
    }
    if (input.endsWith("/v1/records/put-batch")) {
      return okJson({ epoch: 3, record_count: 2 });
    }
    if (input.endsWith("/v1/admin/compact")) {
      return okJson({ compacted: true });
    }
    if (input.endsWith("/v1/admin/snapshot")) {
      return okJson({ snapshot: true, target: "/tmp/snapshot" });
    }
    if (input.endsWith("/v1/admin/restore")) {
      return okJson({ restored: true, target: "/tmp/restore" });
    }
    if (input.endsWith("/v1/admin/jobs")) {
      return okJson({ jobs: [{ queue: "tracedb.snapshot.create" }] });
    }
    return okJson({ epoch: 2 });
  },
});

const fromEnvCalls: FetchCall[] = [];
const fromEnvDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090/",
    TRACEDB_TOKEN: "env-token",
    TRACEDB_DATABASE_ID: "env-db",
    TRACEDB_BRANCH_ID: "env-branch",
    TRACEDB_TIMEOUT_MS: "2500",
  },
  fetchImpl: async (input, init) => {
    fromEnvCalls.push({ input, init });
    return okJson({ epoch: 7 });
  },
});

const fromEnvInsert = await fromEnvDb.table("docs").tenant("tenant-a").insert("env-intro", {
  body: "from env",
});
assert.equal(fromEnvInsert.epoch, 7);
assert.equal(fromEnvCalls[0].input, "http://127.0.0.1:8090/v1/records/put");
assert.equal(fromEnvCalls[0].init.headers.Authorization, "Bearer env-token");
assert.ok(fromEnvCalls[0].init.signal, "TRACEDB_TIMEOUT_MS should attach a request signal");
const fromEnvBody = JSON.parse(fromEnvCalls[0].init.body ?? "{}");
assert.equal(fromEnvBody.database_id, "env-db");
assert.equal(fromEnvBody.branch_id, "env-branch");

const retryCalls: FetchCall[] = [];
let retryAttempt = 0;
const retryDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090",
    TRACEDB_SAFE_RETRIES: "1",
  },
  fetchImpl: async (input, init) => {
    retryCalls.push({ input, init });
    retryAttempt += 1;
    if (retryAttempt === 1) {
      return responseJson(503, { error: "temporarily unavailable" });
    }
    return okJson({ record: { id: "intro", table: "docs", tenant_id: "tenant-a", fields: {} } });
  },
});
const retriedGet = await retryDb.table("docs").tenant("tenant-a").get("intro");
assert.equal(retriedGet.record?.id, "intro");
assert.equal(retryCalls.length, 2, "TRACEDB_SAFE_RETRIES should retry read-only 5xx responses");
assert.equal(retryCalls[0].input, "http://127.0.0.1:8090/v1/records/get");

const traceqlRetryCalls: FetchCall[] = [];
let traceqlRetryAttempt = 0;
const traceqlRetryDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090",
    TRACEDB_SAFE_RETRIES: "1",
  },
  fetchImpl: async (input, init) => {
    traceqlRetryCalls.push({ input, init });
    traceqlRetryAttempt += 1;
    if (traceqlRetryAttempt === 1) {
      return responseJson(503, { error: "temporarily unavailable" });
    }
    return okJson({ results: [] });
  },
});
const retriedTraceql = await traceqlRetryDb.traceql("FROM docs\nTENANT tenant-a\nLIMIT 1");
assert.equal(retriedTraceql.results?.length, 0);
assert.equal(
  traceqlRetryCalls.length,
  2,
  "TRACEDB_SAFE_RETRIES should retry native TraceQL read-only 5xx responses",
);
assert.equal(traceqlRetryCalls[0].input, "http://127.0.0.1:8090/v1/traceql");

const graphqlRetryCalls: FetchCall[] = [];
let graphqlRetryAttempt = 0;
const graphqlRetryDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090",
    TRACEDB_SAFE_RETRIES: "1",
  },
  fetchImpl: async (input, init) => {
    graphqlRetryCalls.push({ input, init });
    graphqlRetryAttempt += 1;
    if (graphqlRetryAttempt === 1) {
      return responseJson(503, { error: "temporarily unavailable" });
    }
    return okJson({ results: [] });
  },
});
const retriedGraphql = await graphqlRetryDb.graphql(
  `query { docs(tenant_id: "tenant-a", limit: 1) { record_id } }`,
);
assert.equal(retriedGraphql.results?.length, 0);
assert.equal(
  graphqlRetryCalls.length,
  2,
  "TRACEDB_SAFE_RETRIES should retry bounded GraphQL read-only 5xx responses",
);
assert.equal(graphqlRetryCalls[0].input, "http://127.0.0.1:8090/v1/graphql");

const graphqlSchemaRetryCalls: FetchCall[] = [];
let graphqlSchemaRetryAttempt = 0;
const graphqlSchemaRetryDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090",
    TRACEDB_SAFE_RETRIES: "1",
  },
  fetchImpl: async (input, init) => {
    graphqlSchemaRetryCalls.push({ input, init });
    graphqlSchemaRetryAttempt += 1;
    if (graphqlSchemaRetryAttempt === 1) {
      return responseJson(503, { error: "temporarily unavailable" });
    }
    return okJson({
      adapter: "bounded_graphql_query_adapter",
      schema: "type Query {\n  docs(tenant_id: String!, limit: Int): [docs!]!\n}\n",
      tables: ["docs"],
      execution: "POST /v1/graphql returns TraceDB QueryResponse, not a GraphQL data envelope",
    });
  },
});
const retriedGraphqlSchema = await graphqlSchemaRetryDb.graphqlSchema();
assert.equal(retriedGraphqlSchema.tables?.[0], "docs");
assert.equal(
  graphqlSchemaRetryCalls.length,
  2,
  "TRACEDB_SAFE_RETRIES should retry GraphQL schema read-only 5xx responses",
);
assert.equal(graphqlSchemaRetryCalls[0].input, "http://127.0.0.1:8090/v1/graphql/schema");
assert.equal(graphqlSchemaRetryCalls[0].init.method, "GET");

const mutationRetryCalls: FetchCall[] = [];
const mutationRetryDb = new TraceDB({
  url: "http://127.0.0.1:8090",
  safeRetries: 1,
  fetchImpl: async (input, init) => {
    mutationRetryCalls.push({ input, init });
    return responseJson(503, { error: "write unavailable" });
  },
});
await assert.rejects(
  () => mutationRetryDb.table("docs").tenant("tenant-a").insert("intro", { body: "write" }),
  (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /HTTP 503/);
    return true;
  },
);
assert.equal(mutationRetryCalls.length, 1, "safeRetries must not retry mutations");

const idempotencyRetryCalls: FetchCall[] = [];
let idempotencyRetryAttempt = 0;
const idempotencyRetryDb = TraceDB.fromEnv({
  env: {
    TRACEDB_URL: "http://127.0.0.1:8090",
    TRACEDB_IDEMPOTENCY_RETRIES: "1",
  },
  fetchImpl: async (input, init) => {
    idempotencyRetryCalls.push({ input, init });
    idempotencyRetryAttempt += 1;
    if (idempotencyRetryAttempt === 1) {
      return responseJson(503, { error: "write unavailable" });
    }
    return okJson({ epoch: 9 });
  },
});
const retriedInsert = await idempotencyRetryDb
  .table("docs")
  .tenant("tenant-a")
  .insert("idempotent-intro", { body: "retry write" }, { idempotencyKey: "ts-idem-insert-1" });
assert.equal(retriedInsert.epoch, 9);
assert.equal(
  idempotencyRetryCalls.length,
  2,
  "TRACEDB_IDEMPOTENCY_RETRIES should retry keyed mutation 5xx responses",
);
assert.equal(idempotencyRetryCalls[0].init.headers["Idempotency-Key"], "ts-idem-insert-1");

const unkeyedIdempotencyRetryCalls: FetchCall[] = [];
const unkeyedIdempotencyRetryDb = new TraceDB({
  url: "http://127.0.0.1:8090",
  idempotencyRetries: 1,
  fetchImpl: async (input, init) => {
    unkeyedIdempotencyRetryCalls.push({ input, init });
    return responseJson(503, { error: "write unavailable" });
  },
});
await assert.rejects(
  () => unkeyedIdempotencyRetryDb.table("docs").tenant("tenant-a").insert("intro", { body: "write" }),
  (error: unknown) => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /HTTP 503/);
    return true;
  },
);
assert.equal(unkeyedIdempotencyRetryCalls.length, 1, "idempotencyRetries require Idempotency-Key");

const rowBatchCalls: FetchCall[] = [];
const rowBatchDb = new TraceDB({
  url: "http://127.0.0.1:8090",
  fetchImpl: async (input, init) => {
    rowBatchCalls.push({ input, init });
    return okJson({ epoch: 10, record_count: 2 });
  },
});
const rowInputs = [
  { id: "row-a", body: "row batch a", embedding: [1, 0, 0], status: "published" },
  { id: "row-b", body: "row batch b", embedding: [0, 1, 0], status: "draft" },
];
const rowBatch = await rowBatchDb
  .table("docs")
  .tenant("tenant-a")
  .insertRows(rowInputs, { idempotencyKey: "ts-rows-1" });
assert.equal(rowBatch.record_count, 2);
assert.equal(rowBatchCalls[0].input, "http://127.0.0.1:8090/v1/records/put-batch");
assert.equal(rowBatchCalls[0].init.headers["Idempotency-Key"], "ts-rows-1");
assert.deepEqual(rowInputs[0], {
  id: "row-a",
  body: "row batch a",
  embedding: [1, 0, 0],
  status: "published",
});
assert.deepEqual(
  JSON.parse(rowBatchCalls[0].init.body ?? "{}"),
  {
    records: [
      {
        table: "docs",
        tenant_id: "tenant-a",
        id: "row-a",
        fields: {
          id: "row-a",
          tenant: "tenant-a",
          body: "row batch a",
          embedding: [1, 0, 0],
          status: "published",
        },
      },
      {
        table: "docs",
        tenant_id: "tenant-a",
        id: "row-b",
        fields: {
          id: "row-b",
          tenant: "tenant-a",
          body: "row batch b",
          embedding: [0, 1, 0],
          status: "draft",
        },
      },
    ],
  },
);

await rowBatchDb
  .table("docs")
  .tenant("tenant-a")
  .insertRows([{ doc_id: "custom-row", body: "custom id field" }], { idField: "doc_id" });
assert.deepEqual(
  JSON.parse(rowBatchCalls[1].init.body ?? "{}"),
  {
    records: [
      {
        table: "docs",
        tenant_id: "tenant-a",
        id: "custom-row",
        fields: {
          doc_id: "custom-row",
          body: "custom id field",
          id: "custom-row",
          tenant: "tenant-a",
        },
      },
    ],
  },
);

const rowBatchCallCount = rowBatchCalls.length;
await assert.rejects(
  () => rowBatchDb.table("docs").tenant("tenant-a").insertRows([{ body: "missing id" }], { idField: "doc_id" }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "POST");
    assert.equal(error.path, "/v1/records/put-batch");
    assert.match(error.message, /row 0 missing id field 'doc_id'/);
    return true;
  },
);
assert.equal(rowBatchCalls.length, rowBatchCallCount, "missing row id should reject before fetch");

assert.throws(
  () => TraceDB.fromEnv({ env: {} }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_URL");
    return true;
  },
);

assert.throws(
  () =>
    TraceDB.fromEnv({
      env: {
        TRACEDB_URL: "http://127.0.0.1:8090",
        TRACEDB_TIMEOUT_MS: "0",
      },
    }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_TIMEOUT_MS");
    return true;
  },
);

assert.throws(
  () =>
    TraceDB.fromEnv({
      env: {
        TRACEDB_URL: "http://127.0.0.1:8090",
        TRACEDB_SAFE_RETRIES: "-1",
      },
    }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_SAFE_RETRIES");
    return true;
  },
);

assert.throws(
  () =>
    TraceDB.fromEnv({
      env: {
        TRACEDB_URL: "http://127.0.0.1:8090",
        TRACEDB_IDEMPOTENCY_RETRIES: "-1",
      },
    }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_IDEMPOTENCY_RETRIES");
    return true;
  },
);

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

const patch = await db.table("docs").tenant("tenant-a").patch("batch-a", { status: "reviewed" });
assert.equal(patch.epoch, 5);
const patchBody = JSON.parse(calls[2].init.body ?? "{}");
assert.equal(patchBody.table, "docs");
assert.equal(patchBody.tenant_id, "tenant-a");
assert.equal(patchBody.id, "batch-a");
assert.deepEqual(patchBody.fields, { status: "reviewed" });

const query = await db
  .table("docs")
  .where({ tenant_id: "tenant-a", status: "published" })
  .match("body", "rust sdk")
  .near("embedding", [1, 0, 0])
  .with({ explain: true, freshness: "lazy" })
  .limit(20)
  .all();
assert.equal(query.results?.[0]?.record_id, "intro");
const queryBody = JSON.parse(calls[3].init.body ?? "{}");
assert.equal(queryBody.table, "docs");
assert.equal(queryBody.tenant_id, "tenant-a");
assert.deepEqual(queryBody.scalar_eq, { status: "published" });
assert.equal(queryBody.text_field, "body");
assert.equal(queryBody.text, "rust sdk");
assert.equal(queryBody.vector_field, "embedding");
assert.deepEqual(queryBody.vector, [1, 0, 0]);
assert.equal(queryBody.freshness, "Lazy");
assert.equal(queryBody.explain, true);
assert.equal(queryBody.top_k, 20);

const explain = await db
  .table("docs")
  .where({ tenant_id: "tenant-a", status: "published" })
  .match("body", "rust sdk")
  .near("embedding", [1, 0, 0])
  .limit(20)
  .explainPlan();
assert.equal(explain.returned_count, 2);
const explainBody = JSON.parse(calls[4].init.body ?? "{}");
assert.equal(explainBody.table, "docs");
assert.equal(explainBody.tenant_id, "tenant-a");
assert.equal(explainBody.text_field, "body");
assert.equal(explainBody.vector_field, "embedding");

const traceql = await db.traceql("FROM docs\nTENANT tenant-a\nLIMIT 1\nEXPLAIN");
assert.equal(traceql.results?.[0]?.record_id, "intro");
assert.equal(traceql.explain?.returned_count, 1);
const traceqlBody = JSON.parse(calls[5].init.body ?? "{}");
assert.equal(traceqlBody.query, "FROM docs\nTENANT tenant-a\nLIMIT 1\nEXPLAIN");

const graphqlQuery = `query { docs(tenant_id: "tenant-a", limit: 1, explain: true) { record_id } }`;
const graphql = await db.graphql(graphqlQuery);
assert.equal(graphql.results?.[0]?.record_id, "intro");
assert.equal(graphql.explain?.returned_count, 1);
const graphqlBody = JSON.parse(calls[6].init.body ?? "{}");
assert.equal(graphqlBody.query, graphqlQuery);

const graphqlRequest: GraphQlQueryRequest = { query: graphqlQuery };
const graphqlViaRequest = await db.graphqlRequest(graphqlRequest);
assert.equal(graphqlViaRequest.results?.[0]?.record_id, "intro");
const graphqlRequestBody = JSON.parse(calls[7].init.body ?? "{}");
assert.equal(graphqlRequestBody.query, graphqlQuery);

const graphqlSchema: GraphQlSchemaResponse = await db.graphqlSchema();
assert.equal(graphqlSchema.adapter, "bounded_graphql_query_adapter");
assert.equal(graphqlSchema.tables?.[0], "docs");
assert.match(graphqlSchema.schema ?? "", /type Query/);
assert.match(graphqlSchema.execution ?? "", /QueryResponse/);
assert.equal(calls[8].input, "http://127.0.0.1:8090/v1/graphql/schema");
assert.equal(calls[8].init.method, "GET");
assert.equal(calls[8].init.body, undefined);

await db.table("docs").tenant("tenant-a").get("intro");
await db.table("docs").tenant("tenant-a").limit(5).scan();
await db.table("docs").tenant("tenant-a").delete("intro", {
  idempotencyKey: "ts-public-delete-1",
  tombstone: "public_sdk_smoke",
});
await db.compact({ idempotencyKey: "ts-public-compact-1" });
await db.snapshot({ target: "/tmp/snapshot" }, { idempotencyKey: "ts-public-snapshot-1" });
await db.restore(
  { source: "/tmp/snapshot", target: "/tmp/restore" },
  { idempotencyKey: "ts-public-restore-1" },
);
const jobs = await db.listAdminJobs();
assert.equal(jobs.jobs?.[0]?.queue, "tracedb.snapshot.create");

await db
  .table("docs")
  .where({ tenant_id: "tenant-a" })
  .match("body", "dirty feature")
  .near("embedding", [1, 0, 0])
  .with({ freshness: "allow_dirty" })
  .all();
const allowDirtyCall = calls.at(-1);
if (allowDirtyCall === undefined) {
  throw new Error("allow_dirty query did not issue a request");
}
const allowDirtyBody = JSON.parse(allowDirtyCall.init.body ?? "{}");
assert.equal(allowDirtyBody.freshness, "AllowDirty");

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
