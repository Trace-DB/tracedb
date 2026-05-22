import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const distDir = "dist";

for (const entry of readdirSync(distDir)) {
  if (!entry.endsWith(".d.ts")) {
    continue;
  }
  const path = join(distDir, entry);
  const original = readFileSync(path, "utf8");
  const rewritten = original.replaceAll(".ts\"", ".js\"");
  if (rewritten !== original) {
    writeFileSync(path, rewritten);
  }
}
