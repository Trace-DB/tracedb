#!/usr/bin/env python3
"""Generate the checked-in TraceDB v1 TypeScript fetch client artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OPENAPI_ARTIFACT = ROOT / "docs" / "api" / "v1-openapi.json"
CLIENT_ARTIFACT = ROOT / "clients" / "typescript" / "src" / "client.ts"

METHOD_NAME_OVERRIDES = {
    "getHealth": "health",
    "getReady": "ready",
    "getDatabases": "listDatabases",
    "getBranches": "listBranches",
    "getMetricsPublicSafe": "publicSafeMetrics",
    "postSchemaApply": "applySchema",
    "postInsert": "insert",
    "postRecordsPut": "putRecord",
    "postRecordsPutBatch": "putBatch",
    "postRecordsPatch": "patchRecord",
    "postRecordsDelete": "deleteRecord",
    "postRecordsGet": "getRecord",
    "postRecordsScan": "scanRecords",
    "postQuery": "query",
    "postExplain": "explain",
    "postAdminCompact": "compact",
    "postAdminSnapshot": "snapshot",
    "postAdminRestore": "restore",
    "getAdminJobs": "listAdminJobs",
}

OPERATION_ORDER = [
    "getHealth",
    "getReady",
    "getDatabases",
    "getBranches",
    "getMetricsPublicSafe",
    "postSchemaApply",
    "postInsert",
    "postRecordsPut",
    "postRecordsPutBatch",
    "postRecordsPatch",
    "postRecordsDelete",
    "postRecordsGet",
    "postRecordsScan",
    "postQuery",
    "postExplain",
    "postAdminCompact",
    "postAdminSnapshot",
    "postAdminRestore",
    "getAdminJobs",
]


class Operation(dict[str, Any]):
    pass


def schema_ref_name(schema: dict[str, Any] | None) -> str | None:
    if not schema:
        return None
    ref = schema.get("$ref")
    if isinstance(ref, str):
        return ref.rsplit("/", 1)[-1]
    return None


def load_operations() -> list[Operation]:
    spec = json.loads(OPENAPI_ARTIFACT.read_text())
    operations_by_id: dict[str, Operation] = {}
    paths = spec.get("paths", {})
    for path, methods in paths.items():
        for method, operation in methods.items():
            operation_id = operation["operationId"]
            request_schema = schema_ref_name(
                operation.get("requestBody", {})
                .get("content", {})
                .get("application/json", {})
                .get("schema")
            )
            response_schema = schema_ref_name(
                operation.get("responses", {})
                .get("200", {})
                .get("content", {})
                .get("application/json", {})
                .get("schema")
            )
            operations_by_id[operation_id] = Operation(
                operation_id=operation_id,
                method=method.upper(),
                path=path,
                summary=operation.get("summary", ""),
                has_body="requestBody" in operation,
                request_schema=request_schema,
                response_schema=response_schema,
                mutates_state=operation.get("x-tracedb-mutates-state", False),
                safe_retry=operation.get("x-tracedb-sdk-safe-retry", False),
                idempotency_supported=operation.get("x-tracedb-idempotency-key-supported", False),
            )

    missing = [operation_id for operation_id in OPERATION_ORDER if operation_id not in operations_by_id]
    if missing:
        raise SystemExit(f"{OPENAPI_ARTIFACT} is missing expected operationIds: {', '.join(missing)}")

    extra = sorted(set(operations_by_id) - set(OPERATION_ORDER))
    if extra:
        raise SystemExit(f"{OPENAPI_ARTIFACT} has unhandled operationIds: {', '.join(extra)}")

    return [operations_by_id[operation_id] for operation_id in OPERATION_ORDER]


def type_for_schema(schema: dict[str, Any]) -> str:
    ref_name = schema_ref_name(schema)
    if ref_name:
        return ref_name

    if "oneOf" in schema:
        return " | ".join(type_for_schema(option) for option in schema["oneOf"])

    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        return " | ".join(type_for_schema({**schema, "type": option}) for option in schema_type)

    if schema_type == "string":
        return "string"
    if schema_type in {"integer", "number"}:
        return "number"
    if schema_type == "boolean":
        return "boolean"
    if schema_type == "null":
        return "null"
    if schema_type == "array":
        item_type = type_for_schema(schema.get("items", {}))
        if "|" in item_type:
            return f"({item_type})[]"
        return f"{item_type}[]"
    if schema_type == "object":
        return "JsonObject"
    return "JsonValue"


def property_name(name: str) -> str:
    if name.replace("_", "").isalnum() and not name[0].isdigit() and "-" not in name:
        return name
    return json.dumps(name)


def render_schema_aliases() -> str:
    spec = json.loads(OPENAPI_ARTIFACT.read_text())
    schemas = spec["components"]["schemas"]
    blocks = [
        "// Generated schema aliases keep OpenAPI's permissive additionalProperties boundary.",
        "// Known fields are optional; runtime validation remains server-side.",
        "",
    ]
    for name, schema in schemas.items():
        properties = schema.get("properties", {})
        if schema.get("type") == "object" or properties:
            blocks.append(f"export interface {name} extends JsonObject {{")
            for prop_name, prop_schema in properties.items():
                blocks.append(f"  {property_name(prop_name)}?: {type_for_schema(prop_schema)};")
            blocks.append("}")
            blocks.append("")
        else:
            blocks.append(f"export type {name} = {type_for_schema(schema)};")
            blocks.append("")
    return "\n".join(blocks)


def render_header() -> str:
    return """// Generated by scripts/generate_typescript_client.py from docs/api/v1-openapi.json.
// Do not edit this file by hand; run `python3 scripts/generate_typescript_client.py`.
// SQL compatibility is not implemented.
// Internal TraceDB-only runs are development evidence; exported performance claims require an external control and a number to beat.
// This is a generated transport artifact, not a published managed SDK package.
// Idempotency-Key is caller supplied and local data-dir-backed on current mutation/admin routes.
// It survives clean engine reopen from the same data directory after successful cache writes,
// but is not cross-replica, crash-atomic exactly-once, or managed-cloud exactly-once semantics.

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonObject | JsonValue[];
export interface JsonObject {
  [key: string]: JsonValue | undefined;
}

type TraceDbMethod = "GET" | "POST";
export type TraceDbRequestContext = TraceDbMethod | "CONFIG";

export interface TraceDbFetchResponse {
  ok: boolean;
  status: number;
  text(): Promise<string>;
}

export interface TraceDbFetchInit {
  method: TraceDbMethod;
  headers: Record<string, string>;
  body?: string;
  signal?: unknown;
}

export type TraceDbFetch = (input: string, init: TraceDbFetchInit) => Promise<TraceDbFetchResponse>;

export type TraceDbRequestOptions = {
  headers?: Record<string, string>;
  idempotencyKey?: string;
  signal?: unknown;
};

export type TraceDbClientConfig = {
  baseUrl: string;
  token?: string;
  databaseId?: string;
  branchId?: string;
  fetchImpl?: TraceDbFetch;
};

function parseTraceDbJsonBody(body: string): JsonValue | undefined {
  const trimmed = body.trim();
  if (trimmed.length === 0) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed) as JsonValue;
  } catch {
    return undefined;
  }
}

function errorResponseFromJson(value: JsonValue | undefined): ErrorResponse | undefined {
  if (value === undefined || value === null || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  const error = (value as JsonObject).error;
  if (typeof error !== "string") {
    return undefined;
  }
  const code = (value as JsonObject).code;
  return typeof code === "string" ? { error, code } : { error };
}

export class TraceDbHttpError extends Error {
  readonly method: TraceDbMethod;
  readonly path: string;
  readonly status: number;
  readonly body: string;
  readonly responseJson?: JsonValue;
  readonly errorResponse?: ErrorResponse;
  readonly responseError?: string;
  readonly responseCode?: string;

  constructor(method: TraceDbMethod, path: string, status: number, body: string) {
    super(`TraceDB ${method} ${path} failed with HTTP ${status}: ${body}`);
    this.name = "TraceDbHttpError";
    this.method = method;
    this.path = path;
    this.status = status;
    this.body = body;
    this.responseJson = parseTraceDbJsonBody(body);
    this.errorResponse = errorResponseFromJson(this.responseJson);
    this.responseError = this.errorResponse?.error;
    this.responseCode = this.errorResponse?.code;
  }
}

export class TraceDbRequestError extends Error {
  readonly method: TraceDbRequestContext;
  readonly path: string;

  constructor(method: TraceDbRequestContext, path: string, message: string) {
    super(`TraceDB ${method} ${path} request invalid: ${message}`);
    this.name = "TraceDbRequestError";
    this.method = method;
    this.path = path;
  }
}

"""


def render_class_start() -> str:
    return """export class TraceDbClient {
  private readonly baseUrl: string;
  private readonly token?: string;
  private readonly databaseId?: string;
  private readonly branchId?: string;
  private readonly fetchImpl: TraceDbFetch;

  constructor(config: TraceDbClientConfig) {
    const baseUrl = config.baseUrl.replace(/\\/+$/, "");
    if (baseUrl.length === 0) {
      throw new Error("TraceDbClientConfig.baseUrl is required");
    }

    const defaultFetch = (globalThis as typeof globalThis & { fetch?: TraceDbFetch }).fetch;
    if (!config.fetchImpl && typeof defaultFetch !== "function") {
      throw new Error("TraceDbClient requires config.fetchImpl when global fetch is unavailable");
    }

    this.baseUrl = baseUrl;
    this.token = config.token;
    this.databaseId = config.databaseId;
    this.branchId = config.branchId;
    this.fetchImpl = config.fetchImpl ?? defaultFetch!;
  }

"""


def render_operation(operation: Operation) -> str:
    method_name = METHOD_NAME_OVERRIDES[operation["operation_id"]]
    method = operation["method"]
    path = operation["path"]
    operation_id = operation["operation_id"]
    summary = operation["summary"]
    request_schema = operation["request_schema"] or "JsonObject"
    response_schema = operation["response_schema"] or "JsonValue"
    mutates_note = " Mutates TraceDB state." if operation["mutates_state"] else ""
    retry_note = " Caller may provide Idempotency-Key." if operation["idempotency_supported"] else ""

    if operation["has_body"]:
        default_body = " = {}" if operation_id == "postAdminCompact" else ""
        signature = f"body: {request_schema}{default_body}, options: TraceDbRequestOptions = {{}}"
        body_arg = "body"
    else:
        signature = "options: TraceDbRequestOptions = {}"
        body_arg = "undefined"

    return f"""  // {operation_id}: {method} {path}
  /** {summary}.{mutates_note}{retry_note} */
  async {method_name}({signature}): Promise<{response_schema}> {{
    return this.request<{response_schema}>("{method}", "{path}", {body_arg}, options);
  }}

"""


def render_helpers() -> str:
    return """  private async request<TResponse extends JsonValue>(
    method: TraceDbMethod,
    path: string,
    body: JsonObject | undefined,
    options: TraceDbRequestOptions,
  ): Promise<TResponse> {
    const headers: Record<string, string> = {
      Accept: "application/json",
      ...options.headers,
    };

    if (this.token) {
      headers.Authorization = `Bearer ${this.token}`;
    }
    if (method !== "GET") {
      headers["Content-Type"] = headers["Content-Type"] ?? "application/json";
    }
    const idempotencyKey = this.validatedIdempotencyKey(method, path, options);
    if (idempotencyKey !== undefined) {
      headers["Idempotency-Key"] = idempotencyKey;
    }

    const init: TraceDbFetchInit = { method, headers };
    if (options.signal !== undefined) {
      init.signal = options.signal;
    }
    if (method !== "GET") {
      init.body = JSON.stringify(this.withRoutingMetadata(body ?? {}));
    }

    const response = await this.fetchImpl(`${this.baseUrl}${path}`, init);
    const responseBody = await response.text();
    if (!response.ok) {
      throw new TraceDbHttpError(method, path, response.status, responseBody);
    }

    const trimmed = responseBody.trim();
    if (trimmed.length === 0) {
      return null as TResponse;
    }
    return JSON.parse(trimmed) as TResponse;
  }

  private withRoutingMetadata(body: JsonObject): JsonObject {
    if (!this.databaseId && !this.branchId) {
      return body;
    }

    const routed: JsonObject = { ...body };
    if (this.databaseId && routed.database_id === undefined) {
      routed.database_id = this.databaseId;
    }
    if (this.branchId && routed.branch_id === undefined) {
      routed.branch_id = this.branchId;
    }
    return routed;
  }

  private validatedIdempotencyKey(
    method: TraceDbMethod,
    path: string,
    options: TraceDbRequestOptions,
  ): string | undefined {
    const key = options.idempotencyKey;
    if (key === undefined) {
      return undefined;
    }
    if (key.length === 0 || key.includes("\\r") || key.includes("\\n")) {
      throw new TraceDbRequestError(
        method,
        path,
        "idempotency key must be non-empty and must not contain CR or LF",
      );
    }
    return key;
  }
}
"""


def render_client() -> str:
    operations = load_operations()
    methods = "".join(render_operation(operation) for operation in operations)
    return render_header() + render_schema_aliases() + render_class_start() + methods + render_helpers()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the checked-in artifact is stale")
    args = parser.parse_args()

    rendered = render_client()
    if args.check:
        current = CLIENT_ARTIFACT.read_text() if CLIENT_ARTIFACT.exists() else ""
        if current != rendered:
            print(f"{CLIENT_ARTIFACT} is stale; run scripts/generate_typescript_client.py", flush=True)
            return 1
        return 0

    CLIENT_ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    CLIENT_ARTIFACT.write_text(rendered)
    print(CLIENT_ARTIFACT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
