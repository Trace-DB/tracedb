import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const tempRoot = mkdtempSync(join(tmpdir(), "tracedb-sdk-consumer-"));

function run(command, args, cwd) {
  return execFileSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
}

try {
  const packDir = join(tempRoot, "pack");
  const consumerDir = join(tempRoot, "consumer");
  mkdirSync(packDir);
  mkdirSync(consumerDir);

  const packOutput = run(
    npm,
    ["pack", "--json", "--pack-destination", packDir],
    packageRoot,
  );
  const [packed] = JSON.parse(packOutput);
  assert.ok(packed?.filename, "npm pack should return a tarball filename");
  const tarballPath = isAbsolute(packed.filename)
    ? packed.filename
    : join(packDir, packed.filename);
  assert.ok(existsSync(tarballPath), `${tarballPath} should exist`);

  writeFileSync(
    join(consumerDir, "package.json"),
    `${JSON.stringify({ private: true, type: "module" }, null, 2)}\n`,
  );
  run(
    npm,
    ["install", "--ignore-scripts", "--no-audit", "--fund=false", tarballPath],
    consumerDir,
  );

  for (const path of [
    "node_modules/@tracedb/sdk/dist/index.js",
    "node_modules/@tracedb/sdk/dist/index.d.ts",
    "node_modules/@tracedb/sdk/dist/client.js",
    "node_modules/@tracedb/sdk/dist/client.d.ts",
    "node_modules/@tracedb/sdk/package.json",
  ]) {
    assert.ok(existsSync(join(consumerDir, path)), `${path} should install`);
  }

  writeFileSync(
    join(consumerDir, "consumer.mjs"),
    `import assert from "node:assert/strict";
import { TraceDB, TraceDbRequestError } from "@tracedb/sdk";
import { TraceDbClient } from "@tracedb/sdk/transport";

assert.equal(typeof TraceDB, "function");
assert.equal(typeof TraceDbClient, "function");
assert.throws(
  () => TraceDB.fromEnv({ env: {} }),
  (error) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_URL");
    return true;
  },
);
console.log("typescript packed consumer import ok");
`,
  );

  const consumerOutput = run(process.execPath, ["consumer.mjs"], consumerDir);
  assert.match(consumerOutput, /typescript packed consumer import ok/);
  console.log("typescript packed consumer smoke ok");
} finally {
  rmSync(tempRoot, { force: true, recursive: true });
}
