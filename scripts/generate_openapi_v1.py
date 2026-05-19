#!/usr/bin/env python3
"""Generate the checked-in TraceDB v1 OpenAPI artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT = ROOT / "docs" / "api" / "v1-openapi.json"

BOUNDARIES = [
    "SQL compatibility is not implemented.",
    "Internal TraceDB-only runs are development evidence; exported performance claims require an external control and a number to beat.",
    "Idempotency-Key supports local in-process replay for mutation and admin routes; SDK idempotent retries are opt-in and require an Idempotency-Key; durable cross-restart idempotency remains future work.",
]

SAFE_RETRY_ROUTES = {
    ("get", "/v1/health"),
    ("get", "/v1/ready"),
    ("post", "/v1/records/get"),
    ("post", "/v1/records/scan"),
    ("post", "/v1/query"),
    ("post", "/v1/explain"),
}

ROUTES = [
    ("get", "/v1/health", "health", "Health check", None, "HealthResponse", False),
    ("get", "/v1/ready", "readiness", "Readiness check", None, "ReadyResponse", False),
    ("get", "/v1/databases", "catalog", "List databases", None, "DatabasesResponse", False),
    ("get", "/v1/branches", "catalog", "List branches", None, "BranchesResponse", False),
    ("get", "/v1/metrics/public-safe", "metrics", "Read public-safe metrics", None, "MetricsResponse", False),
    ("post", "/v1/schema/apply", "schema", "Apply table schema", "TableSchema", "EpochResponse", True),
    ("post", "/v1/insert", "records", "Insert record compatibility route", "RecordInput", "EpochResponse", True),
    ("post", "/v1/records/put", "records", "Put record", "RecordPutRequest", "EpochResponse", True),
    ("post", "/v1/records/put-batch", "records", "Put record batch", "RecordPutBatchRequest", "PutBatchResponse", True),
    ("post", "/v1/records/patch", "records", "Patch record", "RecordPatchRequest", "EpochResponse", True),
    ("post", "/v1/records/delete", "records", "Delete record", "RecordDeleteRequest", "DeleteResponse", True),
    ("post", "/v1/records/get", "records", "Get record", "RecordGetRequest", "GetRecordResponse", False),
    ("post", "/v1/records/scan", "records", "Scan records", "RecordScanRequest", "RecordScanOutput", False),
    ("post", "/v1/query", "query", "Run hybrid query", "HybridQuery", "QueryResponse", False),
    ("post", "/v1/explain", "query", "Explain hybrid query", "HybridQuery", "HybridExplain", False),
    ("post", "/v1/admin/compact", "admin", "Compact local engine state", "EmptyObject", "CompactResponse", True),
    ("post", "/v1/admin/snapshot", "admin", "Create snapshot", "SnapshotRequest", "SnapshotResponse", True),
    ("post", "/v1/admin/restore", "admin", "Restore snapshot", "RestoreRequest", "RestoreResponse", True),
    ("get", "/v1/admin/jobs", "admin", "List admin jobs", None, "JobsResponse", False),
]


def schema_ref(name: str) -> dict[str, Any]:
    return {"$ref": f"#/components/schemas/{name}"}


def object_schema(description: str, properties: dict[str, Any] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "description": description, "additionalProperties": True}
    if properties:
        schema["properties"] = properties
    return schema


def array_schema(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": items}


def components() -> dict[str, Any]:
    return {
        "schemas": {
            "EmptyObject": object_schema("Empty JSON object."),
            "TableSchema": object_schema("TraceDB table schema.", {
                "name": {"type": "string"},
                "primary_id_column": {"type": "string"},
                "tenant_id_column": {"type": "string"},
                "scalar_columns": array_schema({"type": "string"}),
                "text_indexed_columns": array_schema({"type": "string"}),
                "vector_columns": array_schema(object_schema("Vector column schema.")),
            }),
            "RecordInput": object_schema("TraceDB record input.", {
                "table": {"type": "string"},
                "id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "fields": object_schema("Record field map."),
            }),
            "RecordOutput": object_schema("TraceDB visible record output.", {
                "table": {"type": "string"},
                "id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "fields": object_schema("Record field map."),
                "version": {"type": "integer"},
            }),
            "RecordPutRequest": object_schema("Full replacement record write. The server also accepts RecordInput directly.", {
                "record": schema_ref("RecordInput"),
            }),
            "RecordPutBatchRequest": object_schema("Batch record write.", {
                "records": array_schema(schema_ref("RecordInput")),
                "include_write_timing": {"type": "boolean"},
            }),
            "RecordPatchRequest": object_schema("Patch record request.", {
                "table": {"type": "string"},
                "tenant_id": {"type": "string"},
                "id": {"type": "string"},
                "fields": object_schema("Patch field map."),
            }),
            "RecordDeleteRequest": object_schema("Delete/tombstone record request.", {
                "table": {"type": "string"},
                "tenant_id": {"type": "string"},
                "id": {"type": "string"},
                "tombstone": {"type": "string"},
            }),
            "RecordGetRequest": object_schema("Get record request.", {
                "table": {"type": "string"},
                "tenant_id": {"type": "string"},
                "id": {"type": "string"},
            }),
            "RecordScanRequest": object_schema("Scan records request.", {
                "table": {"type": "string"},
                "tenant_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 0},
            }),
            "HybridQuery": object_schema("Hybrid lexical/vector/scalar query.", {
                "table": {"type": "string"},
                "tenant_id": {"type": "string"},
                "text": {"type": ["string", "null"]},
                "vector": {"type": ["array", "null"], "items": {"type": "number"}},
                "top_k": {"type": "integer", "minimum": 0},
                "freshness": {"type": "string"},
                "explain": {"type": "boolean"},
            }),
            "SnapshotRequest": object_schema("Snapshot creation request.", {"target": {"type": "string"}}),
            "RestoreRequest": object_schema("Snapshot restore request.", {
                "source": {"type": "string"},
                "target": {"type": "string"},
            }),
            "HealthResponse": object_schema("Health response."),
            "ReadyResponse": object_schema("Readiness response."),
            "DatabasesResponse": object_schema("Database catalog response."),
            "BranchesResponse": object_schema("Branch catalog response."),
            "MetricsResponse": object_schema("Public-safe metrics response."),
            "EpochResponse": object_schema("Epoch allocation response.", {"epoch": {"type": "integer"}}),
            "PutBatchResponse": object_schema("Batch write response.", {
                "epoch": {"type": "integer"},
                "record_count": {"type": "integer"},
                "write_timing": object_schema("Optional write timing attribution."),
            }),
            "DeleteResponse": object_schema("Delete response.", {
                "deleted": {"type": "boolean"},
                "epoch": {"type": "integer"},
            }),
            "GetRecordResponse": object_schema("Get record response.", {
                "record": {"oneOf": [object_schema("Record output."), {"type": "null"}]},
            }),
            "RecordScanOutput": object_schema("Scan output."),
            "HybridQueryRow": object_schema("Hybrid query result row.", {
                "table": {"type": "string"},
                "record_id": {"type": "string"},
                "tenant_id": {"type": "string"},
                "fields": object_schema("Record field map."),
                "score": {"type": "number"},
            }),
            "QueryResponse": object_schema("Query response."),
            "HybridExplain": object_schema("Explain response."),
            "CompactResponse": object_schema("Compact response.", {"compacted": {"type": "boolean"}}),
            "SnapshotResponse": object_schema("Snapshot response.", {
                "snapshot": {"type": "boolean"},
                "target": {"type": "string"},
            }),
            "RestoreResponse": object_schema("Restore response.", {
                "restored": {"type": "boolean"},
                "source": {"type": "string"},
                "target": {"type": "string"},
            }),
            "JobsResponse": object_schema("Admin job queue response."),
            "ErrorResponse": object_schema("Error response.", {"error": {"type": "string"}}),
        },
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "description": "Required by gateway-managed routes; direct local engine development can use any token.",
            }
        },
    }


def operation_id(method: str, path: str) -> str:
    parts = [part for part in path.strip("/").split("/") if part != "v1"]
    return method + "".join(part.replace("-", "_").title().replace("_", "") for part in parts)


def route_operation(
    method: str,
    path: str,
    tag: str,
    summary: str,
    request_schema: str | None,
    response_schema: str,
    mutates_state: bool,
) -> dict[str, Any]:
    operation: dict[str, Any] = {
        "tags": [tag],
        "operationId": operation_id(method, path),
        "summary": summary,
        "description": (
            "Current TraceDB v1 product route. "
            "This OpenAPI artifact is generated from the checked-in route manifest."
        ),
        "responses": {
            "200": {
                "description": "Successful TraceDB response.",
                "content": {"application/json": {"schema": schema_ref(response_schema)}},
            },
            "400": {
                "description": "Bad request or validation failure.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "401": {
                "description": "Unauthorized gateway request.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "404": {
                "description": "Route not found.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "429": {
                "description": "Gateway rate limit exceeded.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "500": {
                "description": "Engine or gateway failure.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "502": {
                "description": "Gateway upstream failure.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
            "503": {
                "description": "Service unavailable.",
                "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
            },
        },
        "x-tracedb-mutates-state": mutates_state,
        "x-tracedb-sdk-safe-retry": (method, path) in SAFE_RETRY_ROUTES,
        "x-tracedb-sdk-idempotency-retry-supported": mutates_state,
        "x-tracedb-idempotency-key-supported": mutates_state,
    }
    if mutates_state:
        operation["parameters"] = [
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": False,
                "schema": {"type": "string"},
                "description": (
                    "Optional local in-process replay key scoped by method and path. "
                    "Same key plus same raw request body replays the first successful response; "
                    "same key with a different body returns 409 Conflict."
                ),
            }
        ]
        operation["x-tracedb-idempotency-durability"] = "in-process-local-only"
        operation["responses"]["409"] = {
            "description": "Idempotency key was reused with a different request body.",
            "content": {"application/json": {"schema": schema_ref("ErrorResponse")}},
        }
    if request_schema:
        operation["requestBody"] = {
            "required": True,
            "content": {"application/json": {"schema": schema_ref(request_schema)}},
        }
    return operation


def build_spec() -> dict[str, Any]:
    paths: dict[str, Any] = {}
    for method, path, tag, summary, request_schema, response_schema, mutates_state in ROUTES:
        paths.setdefault(path, {})[method] = route_operation(
            method,
            path,
            tag,
            summary,
            request_schema,
            response_schema,
            mutates_state,
        )
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "TraceDB v1 HTTP API",
            "version": "0.1.0-development",
            "description": "\n".join(BOUNDARIES),
        },
        "servers": [
            {"url": "http://127.0.0.1:8090", "description": "Local tracedb-server"},
            {"url": "https://tracedb-engine-production.up.railway.app", "description": "Current Railway alpha engine"},
        ],
        "security": [{"bearerAuth": []}],
        "tags": [
            {"name": "health"},
            {"name": "readiness"},
            {"name": "catalog"},
            {"name": "metrics"},
            {"name": "schema"},
            {"name": "records"},
            {"name": "query"},
            {"name": "admin"},
        ],
        "paths": paths,
        "components": components(),
        "x-tracedb-generated-by": "scripts/generate_openapi_v1.py",
    }


def render_spec() -> str:
    return json.dumps(build_spec(), indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the checked-in artifact is stale")
    args = parser.parse_args()

    rendered = render_spec()
    if args.check:
        current = ARTIFACT.read_text() if ARTIFACT.exists() else ""
        if current != rendered:
            print(f"{ARTIFACT} is stale; run scripts/generate_openapi_v1.py", flush=True)
            return 1
        return 0

    ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(rendered)
    print(ARTIFACT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
