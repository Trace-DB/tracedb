import assert from "node:assert/strict";
import {
  TraceDbClient,
  TraceDbHttpError,
  TraceDbRequestError,
  type AccessPathExplain,
  type Candidate,
  type HybridExplain,
  type HybridQuery,
  type HybridQueryRow,
  type HybridScoreComponents,
  type JsonObject,
  type RecordInput,
  type PutBatchResponse,
  type QueryResponse,
  type GetRecordResponse,
  type RecordPutBatchRequest,
  type RecordScanOutput,
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
  scalar_eq: { status: "active" },
  graph_seed: "root-a",
  temporal_as_of: 123,
  top_k: 5,
};
const queryResponse: QueryResponse = await client.query(queryBody);
assert.equal(queryResponse.ok, true);
const queryRequest = JSON.parse(calls[3].init.body ?? "{}");
assert.equal(queryRequest.vector.length, 3);
assert.equal(queryRequest.scalar_eq.status, "active");
assert.equal(queryRequest.graph_seed, "root-a");
assert.equal(queryRequest.temporal_as_of, 123);

const typedScore: HybridScoreComponents = {
  final_score: 1,
  lexical: 0.7,
  vector: null,
};
const typedAccessPath: AccessPathExplain = {
  access_path_id: "LexicalPath",
  opened: true,
  visibility_checked_before_open: true,
  candidates: 1,
};
const typedCandidate: Candidate = {
  record_id: "a",
  version_id: 1,
  score_components: typedScore,
  source: "LexicalPath",
  freshness: "Ready",
  visibility_checked: true,
};
const typedRow: HybridQueryRow = {
  record_id: "a",
  version_id: 1,
  tenant_id: "tenant-a",
  fields: { body: "hello" },
  score: typedScore,
};
const typedExplain: HybridExplain = {
  read_epoch: 1,
  returned_count: 1,
  access_paths: [typedAccessPath],
  planner_candidates: [typedCandidate],
  phase_timings: [{ phase: "materialization", elapsed_ms: 0.1 }],
};
const typedQueryResponse: QueryResponse = { results: [typedRow], explain: typedExplain };
const typedScanOutput: RecordScanOutput = {
  records: [
    {
      table: "docs",
      id: "a",
      tenant_id: "tenant-a",
      version_id: 1,
      fields: { body: "hello" },
    },
  ],
  returned_count: 1,
};
assert.equal(typedQueryResponse.results?.[0]?.score?.final_score, 1);
assert.equal(typedQueryResponse.explain?.access_paths?.[0]?.access_path_id, "LexicalPath");
assert.equal(typedScanOutput.records?.[0]?.version_id, 1);

const snapshotBody: SnapshotRequest = { target: "/tmp/tracedb-snapshot" };
const snapshotResponse: SnapshotResponse = await client.snapshot(snapshotBody);
assert.equal(snapshotResponse.ok, true);
assert.equal(JSON.parse(calls[4].init.body ?? "{}").target, "/tmp/tracedb-snapshot");

const directPutBody: RecordInput = {
  table: "docs",
  id: "direct",
  tenant_id: "tenant-a",
  fields: { body: "direct" },
};
await client.putRecord(directPutBody);
assert.equal(JSON.parse(calls[5].init.body ?? "{}").id, "direct");

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
const explicitRoutingRequest = JSON.parse(calls[6].init.body ?? "{}");
assert.equal(explicitRoutingRequest.database_id, "explicit-db");
assert.equal(explicitRoutingRequest.branch_id, "explicit-branch");

const getResponse: GetRecordResponse = await client.getRecord({
  table: "docs",
  tenant_id: "tenant-a",
  id: "direct",
});
assert.equal(getResponse.ok, true);

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

for (const invalidKey of ["", "bad\nkey", "bad\rkey"]) {
  const callCount = calls.length;
  await assert.rejects(
    () => client.applySchema(schemaBody, { idempotencyKey: invalidKey }),
    (error: unknown) => {
      assert.ok(error instanceof TraceDbRequestError);
      assert.equal(error.method, "POST");
      assert.equal(error.path, "/v1/schema/apply");
      assert.match(error.message, /idempotency key must be non-empty and must not contain CR or LF/);
      return true;
    },
  );
  assert.equal(calls.length, callCount, "invalid idempotency key should reject before fetch");
}

console.log("typescript client smoke ok");
