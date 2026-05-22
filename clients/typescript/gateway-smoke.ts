import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { TraceDB, TraceDbHttpError, type TableSchema } from "./src/sdk.ts";

const sourceDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(sourceDir, "../..");
const token = "dev-token";
const databaseId = "db_local";
const branchId = "db_local:main";

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

function startTracedbServer(env: NodeJS.ProcessEnv, output: ServerOutput): ChildProcessWithoutNullStreams {
  const child = spawn("cargo", ["run", "-q", "-p", "tracedb-server"], {
    cwd: repoRoot,
    env: { ...process.env, ...env },
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

async function stopChild(child: ChildProcessWithoutNullStreams): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) {
    child.kill();
    await once(child, "exit");
  }
}

async function waitForReady(
  label: string,
  db: TraceDB,
  child: ChildProcessWithoutNullStreams,
  output: ServerOutput,
): Promise<void> {
  let lastError = `${label} did not report ready`;
  for (let attempt = 0; attempt < 900; attempt += 1) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`${label} exited before readiness; stdout=${output.stdout}; stderr=${output.stderr}`);
    }
    try {
      const ready = await db.ready();
      if (ready.ready === true) {
        return;
      }
      lastError = JSON.stringify(ready);
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(
    `timed out waiting for ${label} readiness: ${lastError}; stdout=${output.stdout}; stderr=${output.stderr}`,
  );
}

const authRoutingProbeSchema: TableSchema = {
  name: "gateway_probe",
  primary_id_column: "id",
  tenant_id_column: "tenant",
  text_indexed_columns: ["body"],
};

async function expectHttpError(
  label: string,
  action: () => Promise<unknown>,
  status: number,
  responseError: string,
): Promise<void> {
  try {
    await action();
  } catch (error) {
    assert.equal(error instanceof TraceDbHttpError, true, `${label} should throw TraceDbHttpError`);
    const httpError = error as TraceDbHttpError;
    assert.equal(httpError.status, status, `${label} status`);
    assert.equal(httpError.responseError, responseError, `${label} response error`);
    return;
  }
  throw new Error(`${label} unexpectedly succeeded`);
}

const root = await mkdtemp(join(tmpdir(), "tracedb-ts-gateway-smoke-"));
const runId = `${process.pid}-${Date.now()}`;
const engineDataDir = join(root, "engine-data");
const adminDir = join(root, "admin");
const snapshotTarget = join(adminDir, "snapshot");
const restoreTarget = join(adminDir, "restore");
const enginePort = await freePort();
const gatewayPort = await freePort();
const engineBind = `127.0.0.1:${enginePort}`;
const gatewayBind = `127.0.0.1:${gatewayPort}`;
const engineUrl = `http://${engineBind}`;
const gatewayUrl = `http://${gatewayBind}`;
const engineOutput: ServerOutput = { stderr: "", stdout: "" };
const gatewayOutput: ServerOutput = { stderr: "", stdout: "" };
const engine = startTracedbServer({
  TRACEDB_BIND: engineBind,
  TRACEDB_DATA_DIR: engineDataDir,
  TRACEDB_SERVICE_MODE: "engine",
}, engineOutput);
let gateway: ChildProcessWithoutNullStreams | undefined;

try {
  await mkdir(adminDir, { recursive: true });
  await waitForReady("tracedb engine", new TraceDB({ url: engineUrl, token }), engine, engineOutput);
  gateway = startTracedbServer({
    TRACEDB_API_TOKEN: token,
    TRACEDB_BIND: gatewayBind,
    TRACEDB_ENGINE_URL: engineUrl,
    TRACEDB_REQUIRE_API_KEY: "true",
    TRACEDB_SERVICE_MODE: "gateway",
  }, gatewayOutput);
  const db = new TraceDB({ url: gatewayUrl, token, databaseId, branchId });
  await waitForReady(
    "tracedb gateway",
    db,
    gateway,
    gatewayOutput,
  );
  const health = await db.health();
  assert.equal(health.ok, true);
  assert.equal(health.service, "tracedb-gateway");
  const databases = await db.listDatabases();
  assert.equal(databases.gateway, true);
  assert.equal(databases.databases?.some((database) => database.database_id === databaseId), true);
  const branches = await db.listBranches();
  assert.equal(branches.gateway, true);
  assert.equal(branches.branches?.some((branch) => branch.branch_id === branchId), true);
  const metrics = await db.publicSafeMetrics();
  assert.equal(metrics.service, "tracedb-gateway");

  await expectHttpError(
    "gateway bearer token enforcement",
    () => new TraceDB({ url: gatewayUrl, databaseId, branchId }).applySchema(authRoutingProbeSchema),
    401,
    "invalid api token",
  );
  await expectHttpError(
    "gateway branch routing enforcement",
    () => new TraceDB({
      url: gatewayUrl,
      token,
      databaseId,
      branchId: "db_missing:main",
    }).applySchema(authRoutingProbeSchema),
    400,
    "unknown branch db_missing:main",
  );

  const schema: TableSchema = {
    name: "docs",
    primary_id_column: "id",
    tenant_id_column: "tenant",
    scalar_columns: ["status"],
    text_indexed_columns: ["body"],
    vector_columns: [{ dimensions: 3, name: "embedding", source_columns: ["body"] }],
  };
  await db.applySchema(schema, { idempotencyKey: `ts-public-gateway-${runId}-schema` });

  const docs = db.table("docs").tenant("tenant-a");
  await docs.insert(
    "intro",
    {
      body: "TraceDB TypeScript public SDK gateway smoke",
      embedding: [1, 0, 0],
      status: "published",
    },
    { idempotencyKey: `ts-public-gateway-${runId}-put` },
  );
  const batch = await docs.insertBatch(
    [
      {
        id: "sdk",
        fields: {
          body: "TraceDB public TypeScript SDK table handle through gateway",
          embedding: [0.8, 0.2, 0],
          status: "published",
        },
      },
      {
        id: "ops",
        fields: {
          body: "TraceDB gateway snapshot restore and WAL recovery",
          embedding: [0, 1, 0],
          status: "published",
        },
      },
    ],
    { idempotencyKey: `ts-public-gateway-${runId}-batch` },
  );
  assert.equal(batch.record_count, 2);

  await docs.patch("sdk", { status: "reviewed", reviewer: "public-gateway-smoke" }, {
    idempotencyKey: `ts-public-gateway-${runId}-patch`,
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
    .with({ explain: true, freshness: "lazy" })
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
    idempotencyKey: `ts-public-gateway-${runId}-delete`,
    tombstone: "typescript_public_gateway_smoke",
  });
  assert.equal(deleteResponse.deleted, true);
  const deleted = await docs.get("ops");
  assert.equal(deleted.record, null);

  const compactResponse = await db.compact({ idempotencyKey: `ts-public-gateway-${runId}-compact` });
  assert.equal(compactResponse.compacted, true);
  const snapshotResponse = await db.snapshot(
    { target: snapshotTarget },
    { idempotencyKey: `ts-public-gateway-${runId}-snapshot` },
  );
  assert.equal(snapshotResponse.snapshot, true);
  const restoreResponse = await db.restore(
    { source: snapshotTarget, target: restoreTarget },
    { idempotencyKey: `ts-public-gateway-${runId}-restore` },
  );
  assert.equal(restoreResponse.restored, true);

  const jobs = await db.listAdminJobs();
  assert.equal(jobs.jobs?.some((job) => job.queue === "tracedb.snapshot.create"), true);

  console.log(JSON.stringify({
    ok: true,
    mode: "local-gateway-typescript-public-sdk-smoke",
    gateway_url: gatewayUrl,
    engine_url: engineUrl,
    sdk_surface: "public",
    transport: "generated TraceDbClient",
    token_required: true,
    token_enforcement: true,
    routing_enforcement: true,
    database_id: databaseId,
    branch_id: branchId,
    steps: {
      ready: true,
      health: true,
      catalog: true,
      metrics: true,
      token_enforcement: true,
      routing_enforcement: true,
      schema_apply: true,
      insert: true,
      batch_ingest: true,
      patch: true,
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
    catalog_databases: databases.databases?.length,
    patched: true,
    patched_status: patched.record?.fields?.status,
    deleted_hidden: deleted.record === null,
    snapshot_target: snapshotResponse.target,
    restore_target: restoreResponse.target,
    sql_module: "not_implemented",
  }, null, 2));
  console.log("typescript public sdk gateway smoke ok");
} finally {
  if (gateway !== undefined) {
    await stopChild(gateway);
  }
  await stopChild(engine);
  await rm(root, { force: true, recursive: true });
}
