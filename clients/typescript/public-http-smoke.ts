import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  TraceDB,
  TraceDbHttpError,
  type ReadyResponse,
  type TableSchema,
} from "./src/sdk.ts";

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
  db: TraceDB,
  child: ChildProcessWithoutNullStreams,
  output: ServerOutput,
): Promise<ReadyResponse> {
  let lastError = "server did not report ready";
  for (let attempt = 0; attempt < 900; attempt += 1) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        `tracedb-server exited before readiness; stdout=${output.stdout}; stderr=${output.stderr}`,
      );
    }
    try {
      return await db.ready();
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(
    `timed out waiting for tracedb-server readiness: ${lastError}; ` +
      `stdout=${output.stdout}; stderr=${output.stderr}`,
  );
}

function summaryJsonPath(argv: string[]): string | undefined {
  const index = argv.indexOf("--summary-json");
  if (index === -1) {
    return undefined;
  }
  const value = argv[index + 1];
  if (value === undefined || value.trim().length === 0) {
    throw new Error("--summary-json requires a path");
  }
  return value;
}

const runId = `${process.pid}-${Date.now()}`;
const summaryJson = summaryJsonPath(process.argv.slice(2));
const root = await mkdtemp(join(tmpdir(), "tracedb-ts-public-http-smoke-"));
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
  const db = new TraceDB({ url: baseUrl, token: "dev-token" });
  const ready = await waitForReady(db, child, serverOutput);
  assert.equal(ready.ready, true);
  const health = await db.health();
  assert.equal(health.ok, true);
  assert.equal(health.service, "tracedb-engine");
  const databases = await db.listDatabases();
  assert.equal(databases.mode, "local");
  assert.equal(databases.databases?.[0]?.database_id, "local");
  const branches = await db.listBranches();
  assert.equal(branches.branches?.length, 1);
  const metrics = await db.publicSafeMetrics();
  assert.equal(metrics.service, "tracedb-engine");
  assert.equal(typeof metrics.latest_epoch, "number");

  const schema: TableSchema = {
    name: "docs",
    primary_id_column: "id",
    tenant_id_column: "tenant",
    scalar_columns: ["status"],
    text_indexed_columns: ["body"],
    vector_columns: [{ dimensions: 3, name: "embedding", source_columns: ["body"] }],
  };
  await db.applySchema(schema, { idempotencyKey: `ts-public-${runId}-schema` });

  const docs = db.table("docs").tenant("tenant-a");
  const putFields = {
    body: "TraceDB TypeScript public SDK HTTP smoke",
    embedding: [1, 0, 0],
    status: "published",
  };
  const putIdempotencyKey = `ts-public-${runId}-put`;
  const putResponse = await docs.insert(
    "intro",
    putFields,
    { idempotencyKey: putIdempotencyKey },
  );
  const replayResponse = await docs.insert("intro", putFields, {
    idempotencyKey: putIdempotencyKey,
  });
  assert.equal(replayResponse.epoch, putResponse.epoch);
  let idempotencyConflictStatus: number | undefined;
  try {
    await docs.insert(
      "intro",
      {
        body: "TraceDB TypeScript public SDK HTTP smoke changed",
        embedding: [1, 0, 0],
        status: "published",
      },
      { idempotencyKey: putIdempotencyKey },
    );
  } catch (error) {
    assert.equal(error instanceof TraceDbHttpError, true);
    idempotencyConflictStatus = (error as TraceDbHttpError).status;
  }
  assert.equal(idempotencyConflictStatus, 409);

  const batch = await docs.insertBatch(
    [
      {
        id: "sdk",
        fields: {
          body: "TraceDB public TypeScript SDK table handle",
          embedding: [0.8, 0.2, 0],
          status: "published",
        },
      },
      {
        id: "ops",
        fields: {
          body: "TraceDB snapshot restore and WAL recovery",
          embedding: [0, 1, 0],
          status: "published",
        },
      },
    ],
    { idempotencyKey: `ts-public-${runId}-batch` },
  );
  assert.equal(batch.record_count, 2);

  await docs.patch("sdk", { status: "reviewed", reviewer: "public-http-smoke" }, {
    idempotencyKey: `ts-public-${runId}-patch`,
  });
  const patched = await docs.get("sdk");
  assert.equal(patched.record?.fields?.status, "reviewed");

  const scanResponse = await docs.limit(10).scan();
  assert.equal(scanResponse.returned_count, 3);

  const queryResponse = await db
    .table("docs")
    .where({ tenant_id: "tenant-a", status: "published" })
    .match("body", "TypeScript public SDK")
    .near("embedding", [1, 0, 0])
    .with({ explain: true, freshness: "strict" })
    .limit(3)
    .all();
  assert.equal(Array.isArray(queryResponse.results), true);
  assert.equal(typeof queryResponse.explain?.returned_count, "number");

  const explainResponse = await db
    .table("docs")
    .where({ tenant_id: "tenant-a", status: "published" })
    .match("body", "TypeScript public SDK")
    .near("embedding", [1, 0, 0])
    .limit(3)
    .explainPlan();
  assert.equal(typeof explainResponse.returned_count, "number");

  const deleteResponse = await docs.delete("ops", {
    idempotencyKey: `ts-public-${runId}-delete`,
    tombstone: "typescript_public_http_smoke",
  });
  assert.equal(deleteResponse.deleted, true);
  const deleted = await docs.get("ops");
  assert.equal(deleted.record, null);

  const compactResponse = await db.compact({ idempotencyKey: `ts-public-${runId}-compact` });
  assert.equal(compactResponse.compacted, true);
  const snapshotResponse = await db.snapshot(
    { target: snapshotTarget },
    { idempotencyKey: `ts-public-${runId}-snapshot` },
  );
  assert.equal(snapshotResponse.snapshot, true);
  const restoreResponse = await db.restore(
    { source: snapshotTarget, target: restoreTarget },
    { idempotencyKey: `ts-public-${runId}-restore` },
  );
  assert.equal(restoreResponse.restored, true);

  const jobs = await db.listAdminJobs();
  assert.equal(jobs.jobs?.some((job) => job.queue === "tracedb.snapshot.create"), true);

  let errorEnvelope:
    | {
        status: number;
        error: string | undefined;
        code: string | undefined;
        method: string;
        path: string;
      }
    | undefined;
  try {
    await db.client.getRecord({} as never);
  } catch (error) {
    assert.equal(error instanceof TraceDbHttpError, true);
    const httpError = error as TraceDbHttpError;
    errorEnvelope = {
      status: httpError.status,
      error: httpError.responseError,
      code: httpError.responseCode,
      method: httpError.method,
      path: httpError.path,
    };
  }
  assert.equal(errorEnvelope?.status, 400);
  assert.equal(typeof errorEnvelope?.error, "string");

  const summary = {
    ok: true,
    mode: "local-http-typescript-public-sdk-smoke",
    server_url: baseUrl,
    sdk_surface: "public",
    transport: "generated TraceDbClient",
    steps: {
      ready: true,
      health: true,
      catalog: true,
      metrics: true,
      schema_apply: true,
      put: true,
      insert: true,
      batch_ingest: true,
      patch: true,
      get: true,
      scan: true,
      query: true,
      explain: true,
      delete: true,
      idempotency: true,
      error_envelope: true,
      compact: true,
      snapshot: true,
      restore: true,
      jobs: true,
    },
    records_put: 1,
    records_inserted: 3,
    records_scanned: scanResponse.returned_count,
    catalog_databases: databases.databases?.length,
    put_epoch: putResponse.epoch,
    idempotency_replay_observed: replayResponse.epoch === putResponse.epoch,
    idempotency_conflict_status: idempotencyConflictStatus,
    patched_status: patched.record?.fields?.status,
    deleted_hidden: deleted.record === null,
    error_envelope: errorEnvelope,
    snapshot_target: snapshotResponse.target,
    restore_target: restoreResponse.target,
    sql_module: "not_implemented",
  };
  if (summaryJson !== undefined) {
    await mkdir(dirname(summaryJson), { recursive: true });
    await writeFile(summaryJson, `${JSON.stringify(summary, null, 2)}\n`);
  }
  console.log(JSON.stringify(summary, null, 2));
  console.log("typescript public sdk http smoke ok");
} finally {
  await stopServer(child);
  await rm(root, { force: true, recursive: true });
}
