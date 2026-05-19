import { isAbsolute, join } from "node:path";
import {
  TraceDbClient,
  type HybridQuery,
  type RecordInput,
  type RecordPutBatchRequest,
  type RestoreRequest,
  type SnapshotRequest,
  type TableSchema,
} from "./src/client.ts";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.trim() === "") {
    throw new Error(`${name} is required`);
  }
  return value;
}

function optionalEnv(name: string): string | undefined {
  const value = process.env[name];
  if (value === undefined || value.trim() === "") {
    return undefined;
  }
  return value;
}

function record(
  table: string,
  tenantId: string,
  id: string,
  body: string,
  embedding: number[],
): RecordInput {
  return {
    table,
    id,
    tenant_id: tenantId,
    fields: {
      body,
      embedding,
      id,
      status: "published",
      tenant: tenantId,
    },
  };
}

function idempotencyKey(runId: string, step: string): string {
  return `ts-quickstart-${runId}-${step}`;
}

const baseUrl = requiredEnv("TRACEDB_URL");
const token = process.env.TRACEDB_TOKEN ?? "dev-token";
const databaseId = optionalEnv("TRACEDB_DATABASE_ID");
const branchId = optionalEnv("TRACEDB_BRANCH_ID");
const adminDir = optionalEnv("TRACEDB_ADMIN_DIR");
if (adminDir !== undefined && !isAbsolute(adminDir)) {
  throw new Error("TRACEDB_ADMIN_DIR must be an absolute server-side local path");
}

const runId = `${process.pid}-${Date.now()}`;
const table = `ts_quickstart_${runId.replaceAll("-", "_")}`;
const tenantId = "typescript-quickstart";
const client = new TraceDbClient({
  baseUrl,
  token,
  databaseId,
  branchId,
});

await client.ready();
await client.health();
await client.listDatabases();
await client.listBranches();
await client.publicSafeMetrics();

const schema: TableSchema = {
  name: table,
  primary_id_column: "id",
  tenant_id_column: "tenant",
  scalar_columns: ["status"],
  text_indexed_columns: ["body"],
  vector_columns: [{ name: "embedding", dimensions: 3, source_columns: ["body"] }],
};
const schemaResponse = await client.applySchema(schema, {
  idempotencyKey: idempotencyKey(runId, "schema"),
});

const batch: RecordPutBatchRequest = {
  records: [
    record(table, tenantId, "intro", "TraceDB TypeScript endpoint quickstart", [1, 0, 0]),
    record(table, tenantId, "sdk", "TraceDB generated TypeScript client", [0.8, 0.2, 0]),
    record(table, tenantId, "ops", "TraceDB snapshot restore and WAL recovery", [0, 1, 0]),
  ],
};
const batchResponse = await client.putBatch(batch, {
  idempotencyKey: idempotencyKey(runId, "put-batch"),
});

const scanResponse = await client.scanRecords({ table, tenant_id: tenantId, limit: 10 });
const query: HybridQuery = {
  table,
  tenant_id: tenantId,
  text: "TypeScript endpoint",
  vector: [1, 0, 0],
  scalar_eq: { status: "published" },
  graph_seed: "intro",
  temporal_as_of: Number.MAX_SAFE_INTEGER,
  top_k: 3,
  freshness: "Strict",
  explain: true,
};
const queryResponse = await client.query(query);
const explainResponse = await client.explain(query);

const deleteResponse = await client.deleteRecord(
  { table, tenant_id: tenantId, id: "ops", tombstone: "typescript_endpoint_quickstart" },
  { idempotencyKey: idempotencyKey(runId, "delete") },
);
const deleted = await client.getRecord({ table, tenant_id: tenantId, id: "ops" });

let compacted = false;
let snapshotTarget: string | undefined;
let restoreTarget: string | undefined;
let restored = false;
const adminRequested = adminDir !== undefined;
if (adminRequested) {
  const compactResponse = await client.compact(
    {},
    { idempotencyKey: idempotencyKey(runId, "compact") },
  );
  compacted = compactResponse.compacted === true;
  snapshotTarget = join(adminDir, `snapshot-${runId}`);
  restoreTarget = join(adminDir, `restore-${runId}`);
  const snapshotRequest: SnapshotRequest = { target: snapshotTarget };
  await client.snapshot(snapshotRequest, { idempotencyKey: idempotencyKey(runId, "snapshot") });
  const restoreRequest: RestoreRequest = { source: snapshotTarget, target: restoreTarget };
  const restoreResponse = await client.restore(restoreRequest, {
    idempotencyKey: idempotencyKey(runId, "restore"),
  });
  restored = restoreResponse.restored === true;
}

const jobs = await client.listAdminJobs();
const steps = {
  ready: true,
  health: true,
  catalog: true,
  metrics: true,
  schema_apply: typeof schemaResponse.epoch === "number",
  batch_ingest: batchResponse.record_count === 3,
  scan: scanResponse.returned_count === 3,
  query: Array.isArray(queryResponse.results),
  explain: typeof explainResponse.returned_count === "number",
  delete: deleteResponse.deleted === true,
  jobs: Array.isArray(jobs.jobs),
};
const admin = {
  requested: adminRequested,
  compact: adminRequested ? compacted : "skipped",
  snapshot: adminRequested ? snapshotTarget !== undefined : "skipped",
  restore: adminRequested ? restored : "skipped",
};
const adminOk = !adminRequested || (compacted && snapshotTarget !== undefined && restored);
const ok = Object.values(steps).every((step) => step) && adminOk;

const summary = {
  ok,
  mode: "typescript-endpoint-quickstart",
  server_url: baseUrl,
  database_id: databaseId,
  branch_id: branchId,
  table,
  tenant_id: tenantId,
  steps,
  admin,
  records_inserted: batchResponse.record_count,
  records_scanned: scanResponse.returned_count,
  query_result_ids: queryResponse.results?.map((row) => row.record_id),
  explain_returned_count: explainResponse.returned_count,
  deleted_hidden: deleted.record === null,
  snapshot_target: snapshotTarget,
  restore_target: restoreTarget,
  sql_module: "not_implemented",
};

console.log(JSON.stringify(summary, null, 2));
if (!ok) {
  throw new Error("TraceDB TypeScript endpoint quickstart did not complete all required checks");
}
console.log("typescript client endpoint quickstart ok");
