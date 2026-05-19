import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  TraceDbClient,
  type GetRecordResponse,
  type HybridQuery,
  type QueryResponse,
  type RecordInput,
  type RecordPutBatchRequest,
  type RecordScanOutput,
  type RestoreRequest,
  type RestoreResponse,
  type SnapshotRequest,
  type SnapshotResponse,
  type TableSchema,
} from "./src/client.ts";

const sourceDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(sourceDir, "../..");

type ServerOutput = {
  stderr: string;
  stdout: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function freePort(): Promise<number> {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("failed to allocate local TCP port");
  }
  await new Promise<void>((resolveClose, rejectClose) => {
    server.close((error) => {
      if (error) {
        rejectClose(error);
      } else {
        resolveClose();
      }
    });
  });
  return address.port;
}

function startServer(dataDir: string, bind: string, output: ServerOutput): ChildProcessWithoutNullStreams {
  const child = spawn("cargo", ["run", "-q", "-p", "tracedb-server"], {
    cwd: repoRoot,
    env: {
      ...process.env,
      TRACEDB_BIND: bind,
      TRACEDB_DATA_DIR: dataDir,
      TRACEDB_SERVICE_MODE: "engine",
    },
    stdio: "pipe",
  });
  child.stdin.end();
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    output.stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    output.stderr += chunk;
  });
  return child;
}

async function stopServer(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) {
    child.kill();
    await once(child, "exit");
  }
}

async function waitForReady(
  client: TraceDbClient,
  child: ChildProcessWithoutNullStreams,
  output: ServerOutput,
): Promise<void> {
  let lastError = "server did not report ready";
  for (let attempt = 0; attempt < 300; attempt += 1) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        `tracedb-server exited before readiness; stdout=${output.stdout}; stderr=${output.stderr}`,
      );
    }
    try {
      await client.ready();
      return;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(`timed out waiting for tracedb-server readiness: ${lastError}`);
}

function record(id: string, body: string, embedding: number[]): RecordInput {
  return {
    table: "docs",
    id,
    tenant_id: "tenant-a",
    fields: {
      body,
      embedding,
      id,
      status: "published",
      tenant: "tenant-a",
    },
  };
}

const runId = `${process.pid}-${Date.now()}`;
const root = await mkdtemp(join(tmpdir(), "tracedb-ts-http-smoke-"));
const dataDir = join(root, "data");
const adminDir = join(root, "admin");
const snapshotTarget = join(adminDir, "snapshot");
const restoreTarget = join(adminDir, "restore");
const port = await freePort();
const bind = `127.0.0.1:${port}`;
const baseUrl = `http://${bind}`;
const serverOutput: ServerOutput = { stderr: "", stdout: "" };
const child = startServer(dataDir, bind, serverOutput);

try {
  await mkdir(adminDir, { recursive: true });
  const client = new TraceDbClient({ baseUrl, token: "dev-token" });
  await waitForReady(client, child, serverOutput);

  const schema: TableSchema = {
    name: "docs",
    primary_id_column: "id",
    tenant_id_column: "tenant",
    scalar_columns: ["status"],
    text_indexed_columns: ["body"],
    vector_columns: [{ dimensions: 3, name: "embedding", source_columns: ["body"] }],
  };
  const schemaResponse = await client.applySchema(schema, { idempotencyKey: `ts-${runId}-schema` });
  assert.equal(typeof schemaResponse.epoch, "number");

  const direct = record("intro", "TraceDB TypeScript HTTP quickstart", [1, 0, 0]);
  await client.putRecord(direct, { idempotencyKey: `ts-${runId}-put-direct` });

  const batch: RecordPutBatchRequest = {
    records: [
      record("sdk", "TraceDB generated TypeScript client", [0.8, 0.2, 0]),
      record("ops", "TraceDB snapshot restore and WAL recovery", [0, 1, 0]),
    ],
  };
  const batchResponse = await client.putBatch(batch, { idempotencyKey: `ts-${runId}-put-batch` });
  assert.equal(batchResponse.record_count, 2);

  const getResponse: GetRecordResponse = await client.getRecord({
    table: "docs",
    tenant_id: "tenant-a",
    id: "intro",
  });
  assert.equal(getResponse.record?.id, "intro");
  assert.equal(typeof getResponse.record?.version_id, "number");

  const scanResponse: RecordScanOutput = await client.scanRecords({
    table: "docs",
    tenant_id: "tenant-a",
    limit: 10,
  });
  assert.equal(scanResponse.returned_count, 3);

  const query: HybridQuery = {
    table: "docs",
    tenant_id: "tenant-a",
    text: "TypeScript HTTP",
    vector: [1, 0, 0],
    top_k: 3,
    freshness: "Strict",
    explain: true,
  };
  const queryResponse: QueryResponse = await client.query(query);
  assert.equal(Array.isArray(queryResponse.results), true);
  const explainResponse = await client.explain(query);
  assert.equal(typeof explainResponse.returned_count, "number");

  const deleteResponse = await client.deleteRecord(
    { table: "docs", tenant_id: "tenant-a", id: "ops", tombstone: "typescript_http_smoke" },
    { idempotencyKey: `ts-${runId}-delete` },
  );
  assert.equal(deleteResponse.deleted, true);
  const deleted = await client.getRecord({ table: "docs", tenant_id: "tenant-a", id: "ops" });
  assert.equal(deleted.record, null);

  const compactResponse = await client.compact({}, { idempotencyKey: `ts-${runId}-compact` });
  assert.equal(compactResponse.compacted, true);
  const snapshotRequest: SnapshotRequest = { target: snapshotTarget };
  const snapshotResponse: SnapshotResponse = await client.snapshot(snapshotRequest, {
    idempotencyKey: `ts-${runId}-snapshot`,
  });
  assert.equal(snapshotResponse.snapshot, true);
  const restoreRequest: RestoreRequest = { source: snapshotTarget, target: restoreTarget };
  const restoreResponse: RestoreResponse = await client.restore(restoreRequest, {
    idempotencyKey: `ts-${runId}-restore`,
  });
  assert.equal(restoreResponse.restored, true);

  const jobs = await client.listAdminJobs();
  assert.equal(typeof jobs, "object");

  console.log(JSON.stringify({
    ok: true,
    mode: "local-http-typescript-smoke",
    server_url: baseUrl,
    steps: {
      ready: true,
      schema_apply: true,
      direct_put: true,
      batch_ingest: true,
      get: true,
      scan: true,
      query: true,
      explain: true,
      delete: true,
      compact: true,
      snapshot: true,
      restore: true,
      jobs: true,
    },
    records_inserted: 3,
    records_scanned: scanResponse.returned_count,
    deleted_hidden: deleted.record === null,
    snapshot_target: snapshotResponse.target,
    restore_target: restoreResponse.target,
    sql_module: "not_implemented",
  }, null, 2));
  console.log("typescript client http smoke ok");
} finally {
  await stopServer(child);
  await rm(root, { force: true, recursive: true });
}
