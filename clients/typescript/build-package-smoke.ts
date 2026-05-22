import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { TraceDB, TraceDbRequestError } from "@tracedb/sdk";
import { TraceDbClient } from "@tracedb/sdk/transport";

const packageJson = JSON.parse(readFileSync("package.json", "utf8"));

assert.equal(packageJson.main, "./dist/index.js");
assert.equal(packageJson.types, "./dist/index.d.ts");
assert.deepEqual(packageJson.files, ["dist", "README.md"]);
assert.equal(packageJson.exports["."].types, "./dist/index.d.ts");
assert.equal(packageJson.exports["."].default, "./dist/index.js");
assert.equal(packageJson.exports["./transport"].types, "./dist/client.d.ts");
assert.equal(packageJson.exports["./transport"].default, "./dist/client.js");

for (const path of [
  "dist/index.js",
  "dist/index.d.ts",
  "dist/sdk.js",
  "dist/sdk.d.ts",
  "dist/client.js",
  "dist/client.d.ts",
]) {
  assert.ok(existsSync(path), `${path} should exist after npm run build`);
}

for (const path of ["index.d.ts", "sdk.d.ts"]) {
  const declaration = readFileSync(join("dist", path), "utf8");
  assert.equal(
    declaration.includes('.ts"'),
    false,
    `${path} should not expose .ts import specifiers`,
  );
}

assert.equal(typeof TraceDB, "function");
assert.equal(typeof TraceDbClient, "function");
assert.throws(
  () => TraceDB.fromEnv({ env: {} }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_URL");
    return true;
  },
);

console.log("typescript build package smoke ok");
