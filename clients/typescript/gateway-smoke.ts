import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { TraceDbClient, TraceDbHttpError, type TableSchema } from "./src/client.ts";

const sourceDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(sourceDir, "../..");
const token = "dev-token";
const databaseId = "db_local";
const branchId = "db_local:main";

type ServerOutput = {
  stderr: string;
  stdout: string;
};

type ChildOutput = ServerOutput & {
  status: number | null;
  signal: NodeJS.Signals | null;
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
  client: TraceDbClient,
  child: ChildProcessWithoutNullStreams,
  output: ServerOutput,
): Promise<void> {
  let lastError = `${label} did not report ready`;
  for (let attempt = 0; attempt < 300; attempt += 1) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`${label} exited before readiness; stdout=${output.stdout}; stderr=${output.stderr}`);
    }
    try {
      const ready = await client.ready();
      if (ready.ready === true) {
        return;
      }
      lastError = JSON.stringify(ready);
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(`timed out waiting for ${label} readiness: ${lastError}`);
}

async function runQuickstart(env: NodeJS.ProcessEnv): Promise<ChildOutput> {
  const child = spawn(process.execPath, ["--experimental-strip-types", join(sourceDir, "quickstart.ts")], {
    cwd: sourceDir,
    env: { ...process.env, ...env },
    stdio: "pipe",
  });
  child.stdin.end();
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const timer = setTimeout(() => {
    child.kill();
  }, 60_000);
  const [status, signal] = await once(child, "exit") as [number | null, NodeJS.Signals | null];
  clearTimeout(timer);
  return { status, signal, stdout, stderr };
}

function parseQuickstartSummary(stdout: string): Record<string, unknown> {
  const sentinel = "typescript client endpoint quickstart ok";
  const sentinelIndex = stdout.indexOf(sentinel);
  assert.notEqual(sentinelIndex, -1, `quickstart output missing sentinel: ${stdout}`);
  const jsonText = stdout.slice(0, sentinelIndex).trim();
  return JSON.parse(jsonText) as Record<string, unknown>;
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
const engineDataDir = join(root, "engine-data");
const adminDir = join(root, "admin");
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
  await waitForReady("tracedb engine", new TraceDbClient({ baseUrl: engineUrl, token }), engine, engineOutput);
  gateway = startTracedbServer({
    TRACEDB_API_TOKEN: token,
    TRACEDB_BIND: gatewayBind,
    TRACEDB_ENGINE_URL: engineUrl,
    TRACEDB_REQUIRE_API_KEY: "true",
    TRACEDB_SERVICE_MODE: "gateway",
  }, gatewayOutput);
  await waitForReady(
    "tracedb gateway",
    new TraceDbClient({ baseUrl: gatewayUrl, token, databaseId, branchId }),
    gateway,
    gatewayOutput,
  );
  await expectHttpError(
    "gateway bearer token enforcement",
    () => new TraceDbClient({ baseUrl: gatewayUrl, databaseId, branchId })
      .applySchema(authRoutingProbeSchema),
    401,
    "invalid api token",
  );
  await expectHttpError(
    "gateway branch routing enforcement",
    () => new TraceDbClient({
      baseUrl: gatewayUrl,
      token,
      databaseId,
      branchId: "db_missing:main",
    }).applySchema(authRoutingProbeSchema),
    400,
    "unknown branch db_missing:main",
  );

  const quickstart = await runQuickstart({
    TRACEDB_ADMIN_DIR: adminDir,
    TRACEDB_BRANCH_ID: branchId,
    TRACEDB_DATABASE_ID: databaseId,
    TRACEDB_TOKEN: token,
    TRACEDB_URL: gatewayUrl,
  });
  assert.equal(
    quickstart.status,
    0,
    `gateway quickstart failed with signal=${quickstart.signal}\nstdout=${quickstart.stdout}\nstderr=${quickstart.stderr}`,
  );
  const summary = parseQuickstartSummary(quickstart.stdout);
  assert.equal(summary.ok, true);
  assert.equal(summary.mode, "typescript-endpoint-quickstart");
  assert.equal(summary.server_url, gatewayUrl);
  assert.equal(summary.database_id, databaseId);
  assert.equal(summary.branch_id, branchId);
  assert.equal(summary.deleted_hidden, true);
  assert.equal(summary.sql_module, "not_implemented");

  console.log(JSON.stringify({
    ok: true,
    mode: "local-gateway-typescript-smoke",
    gateway_url: gatewayUrl,
    engine_url: engineUrl,
    token_required: true,
    token_enforcement: true,
    routing_enforcement: true,
    database_id: databaseId,
    branch_id: branchId,
    quickstart_mode: summary.mode,
    quickstart_steps: summary.steps,
    quickstart_admin: summary.admin,
    deleted_hidden: summary.deleted_hidden,
    sql_module: "not_implemented",
  }, null, 2));
  console.log("typescript client gateway smoke ok");
} finally {
  if (gateway !== undefined) {
    await stopChild(gateway);
  }
  await stopChild(engine);
  await rm(root, { force: true, recursive: true });
}
