import assert from "node:assert/strict";
import {
  TraceDB,
  TraceDbRequestError,
  type TableSchema,
} from "@tracedb/sdk";
import { TraceDbClient } from "@tracedb/sdk/transport";

assert.equal(typeof TraceDB, "function");
assert.equal(typeof TraceDbClient, "function");

const schema: TableSchema = {
  name: "docs",
  primary_id_column: "id",
  tenant_id_column: "tenant",
  scalar_columns: ["status"],
  text_indexed_columns: ["body"],
  vector_columns: [{ name: "embedding", dimension: 3 }],
};
assert.equal(schema.name, "docs");

assert.throws(
  () => TraceDB.fromEnv({ env: {} }),
  (error: unknown) => {
    assert.ok(error instanceof TraceDbRequestError);
    assert.equal(error.method, "CONFIG");
    assert.equal(error.path, "TRACEDB_URL");
    return true;
  },
);

console.log("typescript package entry smoke ok");
