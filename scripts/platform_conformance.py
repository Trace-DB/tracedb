from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = Path("docs/platform-contract-v0.json")
DEFAULT_SURFACES = ["http_direct"]
PYTHON_SDK_REPO_ENV = "TRACEDB_PYTHON_SDK_REPO"
TYPESCRIPT_SDK_REPO_ENV = "TRACEDB_TYPESCRIPT_SDK_REPO"
RUST_SDK_REPO_ENV = "TRACEDB_RUST_SDK_REPO"
TENANT_A_ACTOR_HEADERS = {
    "x-tracedb-tenant-id": "tenant-a",
    "x-tracedb-database-id": "local",
    "x-tracedb-branch-id": "main",
    "x-tracedb-token-identity": "platform-conformance",
    "x-tracedb-request-id": "platform-conformance",
}
PYTHON_SDK_CONFORMANCE_EVIDENCE = "installed standalone package + http_smoke.py"
RUST_SDK_CONFORMANCE_EVIDENCE = "standalone Rust SDK quickstart"
TRACEQL_NATIVE_CONFORMANCE_EVIDENCE = "POST /v1/traceql native TraceDB command statements"
GRAPHQL_NATIVE_CONFORMANCE_EVIDENCE = "POST /v1/graphql native GraphQL data/errors envelope"
TRACEQL_SQLISH_CONFORMANCE_EVIDENCE = "POST /v1/traceql bounded SQL-ish SELECT adapter"
TRACEQL_SQLISH_NOT_CHECKED_REASON = (
    "SQL-ish adapter surface only proves query/explain/error behavior; "
    "schema/write/admin scenarios remain HTTP/SDK surfaces"
)
GRAPHQL_HTTP_CONFORMANCE_EVIDENCE = "POST /v1/graphql/bounded bounded GraphQL query adapter"
GRAPHQL_SCHEMA_CONFORMANCE_EVIDENCE = "GET /v1/graphql/schema generated SDL from TableSchema"
GRAPHQL_HTTP_NOT_CHECKED_REASON = (
    "Current GraphQL conformance lane only proves schema export/query/explain/error behavior; "
    "native GraphQL write/admin root fields are implemented but not checked by this lane; "
    "this is not GraphQL resolver runtime or full adapter parity"
)
SERVER_READY_TIMEOUT_SECONDS = 120.0


def load_contract(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def contract_scenario_ids(manifest: dict[str, Any]) -> list[str]:
    return [scenario["id"] for scenario in manifest["conformance_scenarios"]]


def contract_surface_ids(manifest: dict[str, Any]) -> list[str]:
    return [surface["id"] for surface in manifest["surfaces"]]


def scenario_result(
    scenario_id: str,
    status: str,
    *,
    evidence: str | None = None,
    reason: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"id": scenario_id, "status": status}
    if evidence is not None:
        result["evidence"] = evidence
    if reason is not None:
        result["reason"] = reason
    if details:
        result["details"] = details
    return result


def passed(scenario_id: str, evidence: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return scenario_result(scenario_id, "passed", evidence=evidence, details=details)


def failed(scenario_id: str, error: Exception) -> dict[str, Any]:
    return scenario_result(scenario_id, "failed", reason=f"{type(error).__name__}: {error}")


def not_checked(scenario_id: str, reason: str) -> dict[str, Any]:
    return scenario_result(scenario_id, "not_checked", reason=reason)


def empty_surface_report(
    manifest: dict[str, Any],
    surface_id: str,
    status: str,
    reason: str,
) -> dict[str, Any]:
    return finalize_surface(
        surface_id,
        status,
        [scenario_result(scenario_id, status, reason=reason) for scenario_id in contract_scenario_ids(manifest)],
        evidence=[],
    )


def finalize_surface(
    surface_id: str,
    status: str,
    scenarios: list[dict[str, Any]],
    *,
    evidence: list[str],
) -> dict[str, Any]:
    failed_count = sum(1 for scenario in scenarios if scenario["status"] == "failed")
    passed_count = sum(1 for scenario in scenarios if scenario["status"] == "passed")
    complete = len(scenarios) > 0 and passed_count == len(scenarios)
    ok = failed_count == 0 and status not in {"not_run", "failed"}
    return {
        "surface": surface_id,
        "status": status,
        "ok": ok,
        "complete": complete,
        "passed": passed_count,
        "required": len(scenarios),
        "not_checked": sum(1 for scenario in scenarios if scenario["status"] == "not_checked"),
        "failed": failed_count,
        "evidence": evidence,
        "scenarios": scenarios,
    }


def ordered_surface_scenarios(
    manifest: dict[str, Any],
    scenario_map: dict[str, dict[str, Any]],
    *,
    default_reason: str,
) -> list[dict[str, Any]]:
    return [
        scenario_map.get(scenario_id, not_checked(scenario_id, default_reason))
        for scenario_id in contract_scenario_ids(manifest)
    ]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    headers: dict[str, str] | None = None,
    expected_status: int = 200,
) -> tuple[int, dict[str, Any]]:
    request_headers = {
        "Authorization": "Bearer dev-token",
        "Accept": "application/json",
    }
    data = None
    if body is not None:
        data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8")
        status = error.code
    if status != expected_status:
        raise RuntimeError(f"{method} {path} returned {status}, expected {expected_status}: {payload}")
    try:
        return status, json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{method} {path} returned non-JSON body: {payload}") from error


def wait_for_ready(base_url: str, process: subprocess.Popen[str]) -> None:
    last_error = "not ready"
    deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(
                f"tracedb-server exited before ready; stdout={stdout}; stderr={stderr}"
            )
        try:
            _, ready = request_json(base_url, "GET", "/v1/ready")
            if ready.get("ready") is True:
                return
        except Exception as error:  # noqa: BLE001 - readiness loops report the last failure.
            last_error = str(error)
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for tracedb-server readiness: {last_error}")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def table_schema() -> dict[str, Any]:
    return {
        "name": "docs",
        "primary_id_column": "id",
        "tenant_id_column": "tenant",
        "scalar_columns": ["status"],
        "text_indexed_columns": ["body"],
        "vector_columns": [
            {"name": "embedding", "dimensions": 3, "source_columns": ["body"]},
        ],
    }


def record(record_id: str, body: str, status: str, embedding: list[float]) -> dict[str, Any]:
    return {
        "table": "docs",
        "id": record_id,
        "tenant_id": "tenant-a",
        "fields": {
            "body": body,
            "embedding": embedding,
            "id": record_id,
            "status": status,
            "tenant": "tenant-a",
        },
    }


def query_body(*, explain: bool) -> dict[str, Any]:
    return {
        "table": "docs",
        "tenant_id": "tenant-a",
        "text": "TraceDB",
        "vector": [1.0, 0.0, 0.0],
        "top_k": 3,
        "freshness": "Strict",
        "explain": explain,
    }


def error_envelope_scenario(base_url: str) -> dict[str, Any]:
    status, payload = request_json(
        base_url,
        "POST",
        "/v1/records/get",
        {},
        expected_status=400,
    )
    if not isinstance(payload.get("error"), str):
        raise RuntimeError(f"expected JSON error envelope with string error field, got {payload}")
    return passed(
        "errors",
        "current JSON error envelope",
        {
            "status": status,
            "error": payload["error"],
            "code": payload.get("code"),
        },
    )


def traceql_string_execution_scenario(base_url: str) -> dict[str, Any]:
    traceql = "\n".join(
        [
            "FROM docs",
            "TENANT tenant-a",
            'WHERE status = "reviewed"',
            'MATCH body "TraceDB"',
            "NEAR embedding [1.0, 0.0, 0.0]",
            "FRESHNESS allow_dirty",
            "LIMIT 3",
        ]
    )
    _, payload = request_json(base_url, "POST", "/v1/traceql", {"query": traceql})
    results = payload.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"TraceQL response missing results list: {payload}")
    result_ids = [row.get("record_id") for row in results if isinstance(row, dict)]
    if "intro" not in result_ids:
        raise RuntimeError(f"TraceQL did not return expected record intro: {payload}")
    if "explain" in payload:
        raise RuntimeError(f"TraceQL response should be lean without EXPLAIN: {payload}")

    _, explain_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": f"{traceql}\nEXPLAIN"},
    )
    if not isinstance(explain_payload.get("results"), list) or not isinstance(
        explain_payload.get("explain"),
        dict,
    ):
        raise RuntimeError(f"TraceQL EXPLAIN response missing results or explain: {explain_payload}")

    invalid_status, invalid_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": "FROM docs\nTENANT tenant-a\nDROP TABLE docs"},
        expected_status=400,
    )
    error = invalid_payload.get("error")
    if invalid_payload.get("code") != "bad_request" or not (
        isinstance(error, str) and "invalid TraceQL" in error
    ):
        raise RuntimeError(f"invalid TraceQL did not preserve bad-request envelope: {invalid_payload}")

    return passed(
        "traceql_string_execution",
        "POST /v1/traceql",
        {
            "result_ids": result_ids,
            "explain": True,
            "invalid_status": invalid_status,
            "invalid_code": invalid_payload.get("code"),
        },
    )


def json_compact(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def traceql_command(base_url: str, command: str, payload: dict[str, Any] | None = None, *, headers: dict[str, str] | None = None, expected_status: int = 200) -> tuple[int, dict[str, Any]]:
    body = command if payload is None else f"{command} {json_compact(payload)}"
    return request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": body},
        headers=headers,
        expected_status=expected_status,
    )


def graphql_operation(
    base_url: str,
    field: str,
    payload: dict[str, Any] | None = None,
    *,
    selection: str = "{ ok }",
    headers: dict[str, str] | None = None,
    expected_status: int = 200,
) -> tuple[int, dict[str, Any]]:
    if payload is None:
        query = f"query {{ {field} {selection} }}"
    else:
        query = f"query {{ {field}(input: {json.dumps(json_compact(payload))}) {selection} }}"
    return request_json(
        base_url,
        "POST",
        "/v1/graphql",
        {"query": query},
        headers=headers,
        expected_status=expected_status,
    )


def graphql_data(payload: dict[str, Any], field: str) -> dict[str, Any]:
    errors = payload.get("errors")
    if errors:
        raise RuntimeError(f"GraphQL returned errors for {field}: {errors}")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get(field), dict):
        raise RuntimeError(f"GraphQL response missing data.{field}: {payload}")
    return data[field]


def traceql_native_surface_report(manifest: dict[str, Any], base_url: str, admin_dir: Path) -> dict[str, Any]:
    scenarios: dict[str, dict[str, Any]] = {}
    intro = record("intro", "TraceDB native TraceQL conformance", "draft", [1.0, 0.0, 0.0])
    intro_changed = record(
        "intro",
        "TraceDB native TraceQL conformance changed",
        "draft",
        [1.0, 0.0, 0.0],
    )

    def run_step(scenario_id: str, action: Any) -> None:
        try:
            scenarios[scenario_id] = action()
        except Exception as error:  # noqa: BLE001 - report per-scenario failure details.
            scenarios[scenario_id] = failed(scenario_id, error)

    run_step(
        "schema_apply",
        lambda: passed("schema_apply", "TraceQL SCHEMA APPLY", {"epoch": traceql_command(base_url, "SCHEMA APPLY", table_schema())[1]["epoch"]}),
    )
    run_step(
        "put",
        lambda: passed(
            "put",
            "TraceQL PUT",
            {"epoch": traceql_command(base_url, "PUT", intro, headers={"Idempotency-Key": "traceql-put-intro"})[1]["epoch"]},
        ),
    )
    run_step(
        "batch",
        lambda: passed(
            "batch",
            "TraceQL BATCH",
            {
                "record_count": traceql_command(
                    base_url,
                    "BATCH",
                    {
                        "records": [
                            record("sdk", "TraceDB native TraceQL SDK conformance", "published", [0.8, 0.2, 0.0]),
                            record("ops", "TraceDB native TraceQL snapshot restore", "published", [0.0, 1.0, 0.0]),
                        ]
                    },
                    headers={"Idempotency-Key": "traceql-batch"},
                )[1]["record_count"]
            },
        ),
    )
    run_step(
        "patch",
        lambda: passed(
            "patch",
            "TraceQL PATCH",
            {
                "epoch": traceql_command(
                    base_url,
                    "PATCH",
                    {"table": "docs", "tenant_id": "tenant-a", "id": "intro", "fields": {"status": "reviewed"}},
                    headers={"Idempotency-Key": "traceql-patch"},
                )[1]["epoch"]
            },
        ),
    )
    run_step(
        "get",
        lambda: passed(
            "get",
            "TraceQL GET",
            {
                "status": traceql_command(
                    base_url,
                    "GET",
                    {"table": "docs", "tenant_id": "tenant-a", "id": "intro"},
                )[1]["record"]["fields"]["status"]
            },
        ),
    )
    run_step(
        "scan",
        lambda: passed(
            "scan",
            "TraceQL SCAN",
            {
                "returned_count": traceql_command(
                    base_url,
                    "SCAN",
                    {"table": "docs", "tenant_id": "tenant-a", "limit": 10},
                )[1]["returned_count"]
            },
        ),
    )
    run_step(
        "query",
        lambda: passed(
            "query",
            "TraceQL QUERY command",
            {"result_count": len(traceql_command(base_url, "QUERY", query_body(explain=True))[1]["results"])},
        ),
    )
    run_step(
        "traceql_string_execution",
        lambda: traceql_string_execution_scenario(base_url),
    )
    run_step(
        "explain",
        lambda: passed(
            "explain",
            "TraceQL EXPLAIN command",
            {
                "returned_count": traceql_command(
                    base_url,
                    "EXPLAIN",
                    query_body(explain=True),
                )[1]["returned_count"]
            },
        ),
    )
    run_step(
        "delete",
        lambda: passed(
            "delete",
            "TraceQL DELETE",
            {
                "deleted": traceql_command(
                    base_url,
                    "DELETE",
                    {"table": "docs", "tenant_id": "tenant-a", "id": "ops", "tombstone": "platform_conformance"},
                    headers={"Idempotency-Key": "traceql-delete"},
                )[1]["deleted"],
                "hidden": traceql_command(
                    base_url,
                    "GET",
                    {"table": "docs", "tenant_id": "tenant-a", "id": "ops"},
                )[1]["record"]
                is None,
            },
        ),
    )
    run_step(
        "idempotency",
        lambda: passed(
            "idempotency",
            "TraceQL Idempotency-Key replay and conflict",
            {
                "replay_epoch": traceql_command(
                    base_url,
                    "PUT",
                    intro,
                    headers={"Idempotency-Key": "traceql-put-intro"},
                )[1]["epoch"],
                "conflict_status": traceql_command(
                    base_url,
                    "PUT",
                    intro_changed,
                    headers={"Idempotency-Key": "traceql-put-intro"},
                    expected_status=409,
                )[0],
            },
        ),
    )
    run_step(
        "errors",
        lambda: passed(
            "errors",
            "invalid TraceQL command JSON error envelope",
            {
                "status": traceql_command(
                    base_url,
                    "DROP DATABASE",
                    None,
                    expected_status=400,
                )[0]
            },
        ),
    )
    snapshot_dir = admin_dir / "traceql-snapshot"
    restore_dir = admin_dir / "traceql-restore"
    run_step(
        "snapshot_restore",
        lambda: passed(
            "snapshot_restore",
            "TraceQL SNAPSHOT and RESTORE",
            {
                "snapshot": traceql_command(
                    base_url,
                    "SNAPSHOT",
                    {"target": str(snapshot_dir)},
                    headers={"Idempotency-Key": "traceql-snapshot"},
                )[1]["snapshot"],
                "restored": traceql_command(
                    base_url,
                    "RESTORE",
                    {"source": str(snapshot_dir), "target": str(restore_dir)},
                    headers={"Idempotency-Key": "traceql-restore"},
                )[1]["restored"],
            },
        ),
    )

    return finalize_surface(
        "traceql",
        "checked",
        ordered_surface_scenarios(
            manifest,
            scenarios,
            default_reason="TraceQL native command scenario did not run",
        ),
        evidence=[TRACEQL_NATIVE_CONFORMANCE_EVIDENCE],
    )


def graphql_native_surface_report(manifest: dict[str, Any], base_url: str, admin_dir: Path) -> dict[str, Any]:
    scenarios: dict[str, dict[str, Any]] = {}
    intro = record("intro", "TraceDB native GraphQL conformance", "draft", [1.0, 0.0, 0.0])
    intro_changed = record(
        "intro",
        "TraceDB native GraphQL conformance changed",
        "draft",
        [1.0, 0.0, 0.0],
    )

    def run_step(scenario_id: str, action: Any) -> None:
        try:
            scenarios[scenario_id] = action()
        except Exception as error:  # noqa: BLE001 - report per-scenario failure details.
            scenarios[scenario_id] = failed(scenario_id, error)

    def data(field: str, payload: dict[str, Any]) -> dict[str, Any]:
        return graphql_data(payload, field)

    run_step(
        "schema_apply",
        lambda: passed("schema_apply", "GraphQL schemaApply", {"epoch": data("schemaApply", graphql_operation(base_url, "schemaApply", table_schema(), selection="{ epoch }")[1])["epoch"]}),
    )
    run_step(
        "put",
        lambda: passed(
            "put",
            "GraphQL put",
            {
                "epoch": data(
                    "put",
                    graphql_operation(
                        base_url,
                        "put",
                        intro,
                        selection="{ epoch }",
                        headers={"Idempotency-Key": "graphql-put-intro"},
                    )[1],
                )["epoch"]
            },
        ),
    )
    run_step(
        "batch",
        lambda: passed(
            "batch",
            "GraphQL batch",
            {
                "record_count": data(
                    "batch",
                    graphql_operation(
                        base_url,
                        "batch",
                        {
                            "records": [
                                record("sdk", "TraceDB native GraphQL SDK conformance", "published", [0.8, 0.2, 0.0]),
                                record("ops", "TraceDB native GraphQL snapshot restore", "published", [0.0, 1.0, 0.0]),
                            ]
                        },
                        selection="{ record_count }",
                        headers={"Idempotency-Key": "graphql-batch"},
                    )[1],
                )["record_count"]
            },
        ),
    )
    run_step(
        "patch",
        lambda: passed(
            "patch",
            "GraphQL patch",
            {
                "epoch": data(
                    "patch",
                    graphql_operation(
                        base_url,
                        "patch",
                        {"table": "docs", "tenant_id": "tenant-a", "id": "intro", "fields": {"status": "reviewed"}},
                        selection="{ epoch }",
                        headers={"Idempotency-Key": "graphql-patch"},
                    )[1],
                )["epoch"]
            },
        ),
    )
    run_step(
        "get",
        lambda: passed(
            "get",
            "GraphQL get",
            {
                "status": data(
                    "get",
                    graphql_operation(
                        base_url,
                        "get",
                        {"table": "docs", "tenant_id": "tenant-a", "id": "intro"},
                        selection="{ record }",
                    )[1],
                )["record"]["fields"]["status"]
            },
        ),
    )
    run_step(
        "scan",
        lambda: passed(
            "scan",
            "GraphQL scan",
            {
                "returned_count": data(
                    "scan",
                    graphql_operation(
                        base_url,
                        "scan",
                        {"table": "docs", "tenant_id": "tenant-a", "limit": 10},
                        selection="{ returned_count records }",
                    )[1],
                )["returned_count"]
            },
        ),
    )
    run_step(
        "query",
        lambda: passed(
            "query",
            "GraphQL query",
            {
                "result_count": len(
                    data(
                        "query",
                        graphql_operation(
                            base_url,
                            "query",
                            query_body(explain=True),
                            selection="{ results explain }",
                        )[1],
                    )["results"]
                )
            },
        ),
    )
    run_step(
        "traceql_string_execution",
        lambda: passed(
            "traceql_string_execution",
            "GraphQL query operation parity with TraceQL scenario",
            {"result_count": len(data("query", graphql_operation(base_url, "query", query_body(explain=True), selection="{ results explain }")[1])["results"])},
        ),
    )
    run_step(
        "explain",
        lambda: passed(
            "explain",
            "GraphQL explain",
            {
                "returned_count": data(
                    "explain",
                    graphql_operation(
                        base_url,
                        "explain",
                        query_body(explain=True),
                        selection="{ returned_count }",
                    )[1],
                )["returned_count"]
            },
        ),
    )
    run_step(
        "delete",
        lambda: passed(
            "delete",
            "GraphQL delete",
            {
                "deleted": data(
                    "delete",
                    graphql_operation(
                        base_url,
                        "delete",
                        {"table": "docs", "tenant_id": "tenant-a", "id": "ops", "tombstone": "platform_conformance"},
                        selection="{ deleted }",
                        headers={"Idempotency-Key": "graphql-delete"},
                    )[1],
                )["deleted"],
                "hidden": data(
                    "get",
                    graphql_operation(
                        base_url,
                        "get",
                        {"table": "docs", "tenant_id": "tenant-a", "id": "ops"},
                        selection="{ record }",
                    )[1],
                )["record"]
                is None,
            },
        ),
    )
    run_step(
        "idempotency",
        lambda: passed(
            "idempotency",
            "GraphQL Idempotency-Key replay and conflict",
            {
                "replay_epoch": data(
                    "put",
                    graphql_operation(
                        base_url,
                        "put",
                        intro,
                        selection="{ epoch }",
                        headers={"Idempotency-Key": "graphql-put-intro"},
                    )[1],
                )["epoch"],
                "conflict_status": graphql_operation(
                    base_url,
                    "put",
                    intro_changed,
                    selection="{ epoch }",
                    headers={"Idempotency-Key": "graphql-put-intro"},
                    expected_status=409,
                )[0],
            },
        ),
    )
    run_step(
        "errors",
        lambda: passed(
            "errors",
            "GraphQL data/errors envelope",
            {
                "errors": graphql_operation(
                    base_url,
                    "unsupportedTraceDbField",
                    None,
                    selection="{ ok }",
                )[1].get("errors")
            },
        ),
    )
    snapshot_dir = admin_dir / "graphql-snapshot"
    restore_dir = admin_dir / "graphql-restore"
    run_step(
        "snapshot_restore",
        lambda: passed(
            "snapshot_restore",
            "GraphQL snapshot and restore",
            {
                "snapshot": data(
                    "snapshot",
                    graphql_operation(
                        base_url,
                        "snapshot",
                        {"target": str(snapshot_dir)},
                        selection="{ snapshot target }",
                        headers={"Idempotency-Key": "graphql-snapshot"},
                    )[1],
                )["snapshot"],
                "restored": data(
                    "restore",
                    graphql_operation(
                        base_url,
                        "restore",
                        {"source": str(snapshot_dir), "target": str(restore_dir)},
                        selection="{ restored target }",
                        headers={"Idempotency-Key": "graphql-restore"},
                    )[1],
                )["restored"],
            },
        ),
    )

    return finalize_surface(
        "graphql",
        "checked",
        ordered_surface_scenarios(
            manifest,
            scenarios,
            default_reason="GraphQL native scenario did not run",
        ),
        evidence=[GRAPHQL_NATIVE_CONFORMANCE_EVIDENCE],
    )


def traceql_sqlish_conformance_summary(base_url: str) -> dict[str, Any]:
    steps = {
        "sqlish_select": False,
        "sqlish_explain": False,
        "invalid_sqlish": False,
    }
    sqlish_query = (
        'SELECT * FROM docs WHERE tenant_id = "tenant-a" '
        'AND status = "reviewed" LIMIT 3'
    )
    _, payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": sqlish_query},
        headers=TENANT_A_ACTOR_HEADERS,
    )
    results = payload.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"SQL-ish TraceQL response missing results list: {payload}")
    result_ids = [row.get("record_id") for row in results if isinstance(row, dict)]
    if "intro" not in result_ids:
        raise RuntimeError(f"SQL-ish TraceQL did not return expected record intro: {payload}")
    if "explain" in payload:
        raise RuntimeError(f"SQL-ish SELECT response should be lean without EXPLAIN: {payload}")
    steps["sqlish_select"] = True

    _, explain_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {"query": f"EXPLAIN {sqlish_query}"},
        headers=TENANT_A_ACTOR_HEADERS,
    )
    if not isinstance(explain_payload.get("results"), list) or not isinstance(
        explain_payload.get("explain"),
        dict,
    ):
        raise RuntimeError(f"SQL-ish EXPLAIN response missing results or explain: {explain_payload}")
    steps["sqlish_explain"] = True

    invalid_status, invalid_payload = request_json(
        base_url,
        "POST",
        "/v1/traceql",
        {
            "query": (
                'SELECT * FROM docs JOIN users ON docs.user_id = users.id '
                'WHERE tenant_id = "tenant-a"'
            )
        },
        headers=TENANT_A_ACTOR_HEADERS,
        expected_status=400,
    )
    invalid_error = invalid_payload.get("error")
    if invalid_payload.get("code") != "bad_request" or not (
        isinstance(invalid_error, str) and "SQL-ish" in invalid_error
    ):
        raise RuntimeError(f"invalid SQL-ish did not preserve bad-request envelope: {invalid_payload}")
    steps["invalid_sqlish"] = True

    return {
        "ok": all(steps.values()),
        "mode": "traceql-sqlish-conformance",
        "surface": "traceql_sqlish",
        "steps": steps,
        "sql_compatibility": "not_implemented",
        "postgres_compatibility": "not_compatible",
        "sqlish_result_ids": result_ids,
        "sqlish_explain": True,
        "invalid_sqlish_status": invalid_status,
        "invalid_sqlish_code": invalid_payload.get("code"),
        "invalid_sqlish_error": invalid_error,
    }


def graphql_http_conformance_summary(base_url: str) -> dict[str, Any]:
    steps = {
        "graphql_schema": False,
        "graphql_query": False,
        "graphql_explain": False,
        "invalid_graphql": False,
    }
    _, schema_payload = request_json(base_url, "GET", "/v1/graphql/schema")
    schema_sdl = schema_payload.get("schema")
    schema_tables = schema_payload.get("tables")
    schema_tokens = [
        "scalar TraceDBJSON",
        "enum TraceDBFreshness",
        "type Query",
        "docs(",
        "tenant_id: ID!",
        "where: DocsWhere",
        "type DocsRow",
        "status: TraceDBJSON",
        "body: String",
        "embedding: [Float!]",
    ]
    if schema_payload.get("adapter") != "bounded_graphql_query_adapter":
        raise RuntimeError(f"GraphQL schema adapter marker missing: {schema_payload}")
    if schema_payload.get("execution") != "POST /v1/graphql/bounded returns TraceDB QueryResponse; POST /v1/graphql returns GraphQL data/errors":
        raise RuntimeError(f"GraphQL schema execution field missing: {schema_payload}")
    if not isinstance(schema_tables, list) or "docs" not in schema_tables:
        raise RuntimeError(f"GraphQL schema response missing docs table: {schema_payload}")
    if not isinstance(schema_sdl, str) or any(token not in schema_sdl for token in schema_tokens):
        raise RuntimeError(f"GraphQL schema SDL missing required tokens: {schema_payload}")
    steps["graphql_schema"] = True

    graphql_query = (
        'query { docs(tenant_id: "tenant-a", where: {status: "reviewed"}, '
        'match: "TraceDB", near: [1.0, 0.0, 0.0], freshness: ALLOW_DIRTY, '
        "limit: 3) { record_id } }"
    )
    _, payload = request_json(base_url, "POST", "/v1/graphql/bounded", {"query": graphql_query})
    results = payload.get("results")
    if not isinstance(results, list):
        raise RuntimeError(f"GraphQL response missing results list: {payload}")
    result_ids = [row.get("record_id") for row in results if isinstance(row, dict)]
    if "intro" not in result_ids:
        raise RuntimeError(f"GraphQL did not return expected record intro: {payload}")
    if "explain" in payload:
        raise RuntimeError(f"GraphQL query response should be lean without explain: {payload}")
    steps["graphql_query"] = True

    _, explain_payload = request_json(
        base_url,
        "POST",
        "/v1/graphql/bounded",
        {"query": graphql_query.replace("limit: 3)", "limit: 3, explain: true)")},
    )
    if not isinstance(explain_payload.get("results"), list) or not isinstance(
        explain_payload.get("explain"),
        dict,
    ):
        raise RuntimeError(f"GraphQL explain response missing results or explain: {explain_payload}")
    steps["graphql_explain"] = True

    invalid_status, invalid_payload = request_json(
        base_url,
        "POST",
        "/v1/graphql/bounded",
        {"query": 'mutation { docs(tenant_id: "tenant-a") { record_id } }'},
        expected_status=400,
    )
    invalid_error = invalid_payload.get("error")
    if invalid_payload.get("code") != "bad_request" or not (
        isinstance(invalid_error, str) and "invalid GraphQL adapter" in invalid_error
    ):
        raise RuntimeError(f"invalid GraphQL did not preserve bad-request envelope: {invalid_payload}")
    steps["invalid_graphql"] = True

    return {
        "ok": all(steps.values()),
        "mode": "graphql-http-conformance",
        "surface": "graphql",
        "steps": steps,
        "graphql_schema_tables": schema_tables,
        "graphql_schema_tokens": schema_tokens,
        "graphql_result_ids": result_ids,
        "graphql_explain": True,
        "invalid_graphql_status": invalid_status,
        "invalid_graphql_code": invalid_payload.get("code"),
        "invalid_graphql_error": invalid_error,
    }


def run_http_direct_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    scenarios: dict[str, dict[str, Any]] = {}
    with tempfile.TemporaryDirectory(prefix="tracedb-http-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        admin_dir = temp / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        env = os.environ.copy()
        env.update(
            {
                "TRACEDB_BIND": bind,
                "TRACEDB_DATA_DIR": str(data_dir),
                "TRACEDB_SERVICE_MODE": "engine",
                "CARGO_TERM_COLOR": "never",
                "CARGO_INCREMENTAL": "0",
            }
        )
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)

            def run_step(scenario_id: str, action: Any) -> None:
                try:
                    scenarios[scenario_id] = action()
                except Exception as error:  # noqa: BLE001 - report per-scenario failure details.
                    scenarios[scenario_id] = failed(scenario_id, error)

            schema = table_schema()
            intro = record("intro", "TraceDB direct HTTP conformance", "draft", [1.0, 0.0, 0.0])
            intro_changed = record(
                "intro",
                "TraceDB direct HTTP conformance changed",
                "draft",
                [1.0, 0.0, 0.0],
            )

            run_step(
                "schema_apply",
                lambda: passed(
                    "schema_apply",
                    "POST /v1/schema/apply",
                    {"epoch": request_json(base_url, "POST", "/v1/schema/apply", schema)[1]["epoch"]},
                ),
            )
            run_step(
                "put",
                lambda: passed(
                    "put",
                    "POST /v1/records/put",
                    {
                        "epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                        )[1]["epoch"]
                    },
                ),
            )
            run_step(
                "batch",
                lambda: passed(
                    "batch",
                    "POST /v1/records/put-batch",
                    {
                        "record_count": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put-batch",
                            {
                                "records": [
                                    record("sdk", "TraceDB SDK conformance", "published", [0.8, 0.2, 0.0]),
                                    record("ops", "TraceDB snapshot restore", "published", [0.0, 1.0, 0.0]),
                                ]
                            },
                            headers={"Idempotency-Key": "http-direct-batch"},
                        )[1]["record_count"]
                    },
                ),
            )
            run_step(
                "patch",
                lambda: passed(
                    "patch",
                    "POST /v1/records/patch",
                    {
                        "epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/patch",
                            {
                                "table": "docs",
                                "tenant_id": "tenant-a",
                                "id": "intro",
                                "fields": {"status": "reviewed"},
                            },
                            headers={"Idempotency-Key": "http-direct-patch"},
                        )[1]["epoch"]
                    },
                ),
            )
            run_step(
                "get",
                lambda: passed(
                    "get",
                    "POST /v1/records/get",
                    {
                        "status": request_json(
                            base_url,
                            "POST",
                            "/v1/records/get",
                            {"table": "docs", "tenant_id": "tenant-a", "id": "intro"},
                        )[1]["record"]["fields"]["status"]
                    },
                ),
            )
            run_step(
                "scan",
                lambda: passed(
                    "scan",
                    "POST /v1/records/scan",
                    {
                        "returned_count": request_json(
                            base_url,
                            "POST",
                            "/v1/records/scan",
                            {"table": "docs", "tenant_id": "tenant-a", "limit": 10},
                        )[1]["returned_count"]
                    },
                ),
            )
            run_step(
                "query",
                lambda: passed(
                    "query",
                    "POST /v1/query",
                    {
                        "result_count": len(
                            request_json(base_url, "POST", "/v1/query", query_body(explain=True))[1][
                                "results"
                            ]
                        )
                    },
                ),
            )
            run_step(
                "traceql_string_execution",
                lambda: traceql_string_execution_scenario(base_url),
            )
            run_step(
                "explain",
                lambda: passed(
                    "explain",
                    "POST /v1/explain",
                    {
                        "returned_count": request_json(
                            base_url,
                            "POST",
                            "/v1/explain",
                            query_body(explain=True),
                        )[1]["returned_count"]
                    },
                ),
            )
            run_step(
                "delete",
                lambda: passed(
                    "delete",
                    "POST /v1/records/delete",
                    {
                        "deleted": request_json(
                            base_url,
                            "POST",
                            "/v1/records/delete",
                            {
                                "table": "docs",
                                "tenant_id": "tenant-a",
                                "id": "ops",
                                "tombstone": "platform_conformance",
                            },
                            headers={"Idempotency-Key": "http-direct-delete"},
                        )[1]["deleted"],
                        "hidden": request_json(
                            base_url,
                            "POST",
                            "/v1/records/get",
                            {"table": "docs", "tenant_id": "tenant-a", "id": "ops"},
                        )[1]["record"]
                        is None,
                    },
                ),
            )
            run_step(
                "idempotency",
                lambda: passed(
                    "idempotency",
                    "Idempotency-Key replay and conflict",
                    {
                        "replay_epoch": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                        )[1]["epoch"],
                        "conflict_status": request_json(
                            base_url,
                            "POST",
                            "/v1/records/put",
                            intro_changed,
                            headers={"Idempotency-Key": "http-direct-put-intro"},
                            expected_status=409,
                        )[0],
                    },
                ),
            )
            run_step(
                "errors",
                lambda: error_envelope_scenario(base_url),
            )
            snapshot_dir = admin_dir / "snapshot"
            restore_dir = admin_dir / "restore"
            run_step(
                "snapshot_restore",
                lambda: passed(
                    "snapshot_restore",
                    "POST /v1/admin/snapshot and POST /v1/admin/restore",
                    {
                        "snapshot": request_json(
                            base_url,
                            "POST",
                            "/v1/admin/snapshot",
                            {"target": str(snapshot_dir)},
                            headers={"Idempotency-Key": "http-direct-snapshot"},
                        )[1]["snapshot"],
                        "restored": request_json(
                            base_url,
                            "POST",
                            "/v1/admin/restore",
                            {"source": str(snapshot_dir), "target": str(restore_dir)},
                            headers={"Idempotency-Key": "http-direct-restore"},
                        )[1]["restored"],
                    },
                ),
            )
        finally:
            stop_process(process)

    ordered = [
        scenarios.get(scenario_id, not_checked(scenario_id, "scenario did not run"))
        for scenario_id in contract_scenario_ids(manifest)
    ]
    return finalize_surface(
        "http_direct",
        "checked",
        ordered,
        evidence=["raw stdlib HTTP requests against tracedb-server"],
    )


def run_command(
    argv: list[str],
    cwd: Path,
    *,
    env_extra: dict[str, str] | None = None,
) -> dict[str, Any]:
    env = {**os.environ, "CARGO_TERM_COLOR": "never", "CARGO_INCREMENTAL": "0"}
    if env_extra:
        env.update(env_extra)
    started = time.monotonic()
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return {
        "argv": argv,
        "cwd": str(cwd),
        "duration_s": round(time.monotonic() - started, 3),
        "ok": process.returncode == 0,
        "returncode": process.returncode,
        "stdout": process.stdout,
        "stderr_tail": process.stderr[-12_000:],
    }


def map_rust_sdk_quickstart_summary(
    manifest: dict[str, Any],
    quickstart: dict[str, Any],
) -> dict[str, Any]:
    quickstart_steps = quickstart.get("steps", {})
    error_envelope = quickstart.get("error_envelope", {})
    records_put = quickstart.get("records_put")
    traceql_result_count = quickstart.get("traceql_result_count")

    def step_passed(name: str) -> bool:
        return quickstart.get("ok") is True and quickstart_steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "rust_sdk_quickstart steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("Rust SDK quickstart schema_apply did not pass")),
        "put": passed(
            "put",
            "rust_sdk_quickstart steps.put",
            {"records_put": records_put, "put_epoch": quickstart.get("put_epoch")},
        )
        if step_passed("put") and isinstance(records_put, int) and records_put >= 1
        else failed("put", RuntimeError("Rust SDK quickstart single-record put evidence missing")),
        "batch": passed(
            "batch",
            "rust_sdk_quickstart steps.batch_ingest and steps.row_batch_ingest",
            {
                "records_batched": quickstart.get("records_batched"),
                "records_row_batched": quickstart.get("records_row_batched"),
            },
        )
        if step_passed("batch_ingest") and step_passed("row_batch_ingest")
        else failed(
            "batch",
            RuntimeError("Rust SDK quickstart batch_ingest or row_batch_ingest did not pass"),
        ),
        "patch": passed("patch", "rust_sdk_quickstart steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("Rust SDK quickstart patch did not pass")),
        "get": passed("get", "rust_sdk_quickstart patched get and deleted get checks")
        if quickstart.get("patched_status") == "reviewed" and quickstart.get("deleted_hidden") is True
        else failed("get", RuntimeError("Rust SDK quickstart get evidence missing")),
        "scan": passed("scan", "rust_sdk_quickstart steps.scan")
        if step_passed("scan")
        else failed("scan", RuntimeError("Rust SDK quickstart scan did not pass")),
        "query": passed("query", "rust_sdk_quickstart steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("Rust SDK quickstart query did not pass")),
        "traceql_string_execution": passed(
            "traceql_string_execution",
            "rust_sdk_quickstart steps.traceql_string_execution",
            {
                "result_count": traceql_result_count,
                "explain": quickstart.get("traceql_explain"),
            },
        )
        if step_passed("traceql_string_execution")
        and isinstance(traceql_result_count, int)
        and traceql_result_count >= 1
        and quickstart.get("traceql_explain") is True
        else failed(
            "traceql_string_execution",
            RuntimeError("Rust SDK quickstart TraceQL string execution evidence missing"),
        ),
        "explain": passed("explain", "rust_sdk_quickstart steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("Rust SDK quickstart explain did not pass")),
        "delete": passed("delete", "rust_sdk_quickstart steps.delete and deleted_hidden")
        if step_passed("delete") and quickstart.get("deleted_hidden") is True
        else failed("delete", RuntimeError("Rust SDK quickstart delete did not pass")),
        "idempotency": passed("idempotency", "rust_sdk_quickstart idempotency_keys")
        if quickstart.get("idempotency_keys") is True and quickstart.get("idempotency_retries", 0) >= 1
        else failed("idempotency", RuntimeError("Rust SDK quickstart idempotency evidence missing")),
        "errors": passed("errors", "rust_sdk_quickstart error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("Rust SDK quickstart error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "rust_sdk_quickstart admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("Rust SDK quickstart snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="Rust SDK public surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "rust_sdk",
        "checked",
        scenarios,
        evidence=[RUST_SDK_CONFORMANCE_EVIDENCE],
    )


def map_python_sdk_smoke_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    error_envelope = smoke_summary.get("error_envelope", {})
    traceql_result_count = smoke_summary.get("traceql_result_count")

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "python sdk smoke steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("Python SDK smoke schema_apply did not pass")),
        "put": passed(
            "put",
            "python sdk smoke steps.put",
            {"records_put": smoke_summary.get("records_put"), "put_epoch": smoke_summary.get("put_epoch")},
        )
        if step_passed("put") and smoke_summary.get("records_put") == 1
        else failed("put", RuntimeError("Python SDK smoke single-record put evidence missing")),
        "batch": passed(
            "batch",
            "python sdk smoke steps.batch_ingest",
            {"records_inserted": smoke_summary.get("records_inserted")},
        )
        if step_passed("batch_ingest")
        else failed("batch", RuntimeError("Python SDK smoke batch_ingest did not pass")),
        "patch": passed("patch", "python sdk smoke steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("Python SDK smoke patch did not pass")),
        "get": passed("get", "python sdk smoke patched get and deleted get checks")
        if step_passed("get") and smoke_summary.get("patched_status") == "reviewed"
        else failed("get", RuntimeError("Python SDK smoke get evidence missing")),
        "scan": passed(
            "scan",
            "python sdk smoke steps.scan",
            {"records_scanned": smoke_summary.get("records_scanned")},
        )
        if step_passed("scan")
        else failed("scan", RuntimeError("Python SDK smoke scan did not pass")),
        "query": passed("query", "python sdk smoke steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("Python SDK smoke query did not pass")),
        "traceql_string_execution": passed(
            "traceql_string_execution",
            "python sdk smoke steps.traceql_string_execution",
            {
                "result_count": traceql_result_count,
                "explain": smoke_summary.get("traceql_explain"),
            },
        )
        if step_passed("traceql_string_execution")
        and isinstance(traceql_result_count, int)
        and traceql_result_count >= 1
        and smoke_summary.get("traceql_explain") is True
        else failed(
            "traceql_string_execution",
            RuntimeError("Python SDK smoke TraceQL string execution evidence missing"),
        ),
        "explain": passed("explain", "python sdk smoke steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("Python SDK smoke explain did not pass")),
        "delete": passed("delete", "python sdk smoke steps.delete and deleted_hidden")
        if step_passed("delete") and smoke_summary.get("deleted_hidden") is True
        else failed("delete", RuntimeError("Python SDK smoke delete did not pass")),
        "idempotency": passed(
            "idempotency",
            "python sdk smoke idempotency replay and conflict",
            {
                "replay_epoch": smoke_summary.get("idempotency_replay_epoch"),
                "conflict_status": smoke_summary.get("idempotency_conflict_status"),
            },
        )
        if step_passed("idempotency") and smoke_summary.get("idempotency_conflict_status") == 409
        else failed("idempotency", RuntimeError("Python SDK smoke idempotency evidence missing")),
        "errors": passed("errors", "python sdk smoke error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("Python SDK smoke error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "python sdk smoke admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("Python SDK smoke snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="Python SDK public surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "python_sdk",
        "checked",
        scenarios,
        evidence=[PYTHON_SDK_CONFORMANCE_EVIDENCE],
    )


def map_typescript_sdk_smoke_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    error_envelope = smoke_summary.get("error_envelope", {})
    traceql_result_count = smoke_summary.get("traceql_result_count")

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def error_envelope_passed() -> bool:
        return (
            step_passed("error_envelope")
            and isinstance(error_envelope, dict)
            and isinstance(error_envelope.get("status"), int)
            and error_envelope["status"] >= 400
            and isinstance(error_envelope.get("error"), str)
            and bool(error_envelope["error"])
        )

    scenario_map = {
        "schema_apply": passed("schema_apply", "typescript public sdk smoke steps.schema_apply")
        if step_passed("schema_apply")
        else failed("schema_apply", RuntimeError("TypeScript SDK smoke schema_apply did not pass")),
        "put": passed(
            "put",
            "typescript public sdk smoke steps.put",
            {"records_put": smoke_summary.get("records_put"), "put_epoch": smoke_summary.get("put_epoch")},
        )
        if step_passed("put") and smoke_summary.get("records_put") == 1
        else failed("put", RuntimeError("TypeScript SDK smoke single-record put evidence missing")),
        "batch": passed(
            "batch",
            "typescript public sdk smoke steps.batch_ingest",
            {"records_inserted": smoke_summary.get("records_inserted")},
        )
        if step_passed("batch_ingest")
        else failed("batch", RuntimeError("TypeScript SDK smoke batch_ingest did not pass")),
        "patch": passed("patch", "typescript public sdk smoke steps.patch")
        if step_passed("patch")
        else failed("patch", RuntimeError("TypeScript SDK smoke patch did not pass")),
        "get": passed("get", "typescript public sdk smoke patched get and deleted get checks")
        if step_passed("get") and smoke_summary.get("patched_status") == "reviewed"
        else failed("get", RuntimeError("TypeScript SDK smoke get evidence missing")),
        "scan": passed(
            "scan",
            "typescript public sdk smoke steps.scan",
            {"records_scanned": smoke_summary.get("records_scanned")},
        )
        if step_passed("scan")
        else failed("scan", RuntimeError("TypeScript SDK smoke scan did not pass")),
        "query": passed("query", "typescript public sdk smoke steps.query")
        if step_passed("query")
        else failed("query", RuntimeError("TypeScript SDK smoke query did not pass")),
        "traceql_string_execution": passed(
            "traceql_string_execution",
            "typescript public sdk smoke steps.traceql_string_execution",
            {
                "result_count": traceql_result_count,
                "explain": smoke_summary.get("traceql_explain"),
            },
        )
        if step_passed("traceql_string_execution")
        and isinstance(traceql_result_count, int)
        and traceql_result_count >= 1
        and smoke_summary.get("traceql_explain") is True
        else failed(
            "traceql_string_execution",
            RuntimeError("TypeScript SDK smoke TraceQL string execution evidence missing"),
        ),
        "explain": passed("explain", "typescript public sdk smoke steps.explain")
        if step_passed("explain")
        else failed("explain", RuntimeError("TypeScript SDK smoke explain did not pass")),
        "delete": passed("delete", "typescript public sdk smoke steps.delete and deleted_hidden")
        if step_passed("delete") and smoke_summary.get("deleted_hidden") is True
        else failed("delete", RuntimeError("TypeScript SDK smoke delete did not pass")),
        "idempotency": passed(
            "idempotency",
            "typescript public sdk smoke idempotency replay and conflict",
            {
                "replay_observed": smoke_summary.get("idempotency_replay_observed"),
                "conflict_status": smoke_summary.get("idempotency_conflict_status"),
            },
        )
        if step_passed("idempotency")
        and smoke_summary.get("idempotency_replay_observed") is True
        and smoke_summary.get("idempotency_conflict_status") == 409
        else failed("idempotency", RuntimeError("TypeScript SDK smoke idempotency evidence missing")),
        "errors": passed("errors", "typescript public sdk smoke error_envelope", dict(error_envelope))
        if error_envelope_passed()
        else failed("errors", RuntimeError("TypeScript SDK smoke error envelope evidence missing")),
        "snapshot_restore": passed("snapshot_restore", "typescript public sdk smoke admin snapshot/restore")
        if step_passed("snapshot") and step_passed("restore")
        else failed("snapshot_restore", RuntimeError("TypeScript SDK smoke snapshot/restore did not pass")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason="TypeScript public SDK surface does not expose native TraceQL execution yet",
    )
    return finalize_surface(
        "typescript_sdk",
        "checked",
        scenarios,
        evidence=["npm run public-http-smoke -- --summary-json"],
    )


def map_traceql_sqlish_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    result_ids = smoke_summary.get("sqlish_result_ids")
    invalid_error = smoke_summary.get("invalid_sqlish_error")

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def select_passed() -> bool:
        return step_passed("sqlish_select") and isinstance(result_ids, list) and "intro" in result_ids

    def explain_passed() -> bool:
        return step_passed("sqlish_explain") and smoke_summary.get("sqlish_explain") is True

    def invalid_sqlish_passed() -> bool:
        return (
            step_passed("invalid_sqlish")
            and smoke_summary.get("invalid_sqlish_status") == 400
            and smoke_summary.get("invalid_sqlish_code") == "bad_request"
            and isinstance(invalid_error, str)
            and "SQL-ish" in invalid_error
        )

    sqlish_details = {
        "result_ids": result_ids,
        "sql_compatibility": smoke_summary.get("sql_compatibility"),
        "postgres_compatibility": smoke_summary.get("postgres_compatibility"),
    }
    scenario_map = {
        "query": passed("query", TRACEQL_SQLISH_CONFORMANCE_EVIDENCE, sqlish_details)
        if select_passed()
        else failed("query", RuntimeError("SQL-ish SELECT did not return expected intro record")),
        "traceql_string_execution": passed(
            "traceql_string_execution",
            TRACEQL_SQLISH_CONFORMANCE_EVIDENCE,
            sqlish_details,
        )
        if select_passed()
        else failed(
            "traceql_string_execution",
            RuntimeError("bounded SQL-ish adapter did not execute through /v1/traceql"),
        ),
        "explain": passed(
            "explain",
            "EXPLAIN SELECT * FROM docs ... via POST /v1/traceql",
            {"explain": smoke_summary.get("sqlish_explain")},
        )
        if explain_passed()
        else failed("explain", RuntimeError("SQL-ish EXPLAIN evidence missing")),
        "errors": passed(
            "errors",
            "invalid SQL-ish bad-request envelope",
            {
                "status": smoke_summary.get("invalid_sqlish_status"),
                "code": smoke_summary.get("invalid_sqlish_code"),
                "error": invalid_error,
            },
        )
        if invalid_sqlish_passed()
        else failed("errors", RuntimeError("invalid SQL-ish error envelope evidence missing")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason=TRACEQL_SQLISH_NOT_CHECKED_REASON,
    )
    return finalize_surface(
        "traceql_sqlish",
        "checked",
        scenarios,
        evidence=[
            TRACEQL_SQLISH_CONFORMANCE_EVIDENCE,
            "scripts/platform_conformance.py --surface traceql_sqlish",
            "SQL compatibility not implemented; PostgreSQL compatibility not claimed",
        ],
    )


def map_graphql_http_summary(
    manifest: dict[str, Any],
    smoke_summary: dict[str, Any],
) -> dict[str, Any]:
    steps = smoke_summary.get("steps", {})
    schema_tables = smoke_summary.get("graphql_schema_tables")
    schema_tokens = smoke_summary.get("graphql_schema_tokens")
    result_ids = smoke_summary.get("graphql_result_ids")
    invalid_error = smoke_summary.get("invalid_graphql_error")

    def step_passed(name: str) -> bool:
        return smoke_summary.get("ok") is True and steps.get(name) is True

    def query_passed() -> bool:
        return step_passed("graphql_query") and isinstance(result_ids, list) and "intro" in result_ids

    def schema_passed() -> bool:
        return (
            step_passed("graphql_schema")
            and isinstance(schema_tables, list)
            and "docs" in schema_tables
            and isinstance(schema_tokens, list)
            and "type DocsRow" in schema_tokens
        )

    def explain_passed() -> bool:
        return step_passed("graphql_explain") and smoke_summary.get("graphql_explain") is True

    def invalid_graphql_passed() -> bool:
        return (
            step_passed("invalid_graphql")
            and smoke_summary.get("invalid_graphql_status") == 400
            and smoke_summary.get("invalid_graphql_code") == "bad_request"
            and isinstance(invalid_error, str)
            and "invalid GraphQL adapter" in invalid_error
        )

    scenario_map = {
        "schema_apply": passed(
            "schema_apply",
            GRAPHQL_SCHEMA_CONFORMANCE_EVIDENCE,
            {"tables": schema_tables, "tokens": schema_tokens},
        )
        if schema_passed()
        else failed("schema_apply", RuntimeError("GraphQL generated SDL did not reflect applied schema")),
        "query": passed(
            "query",
            GRAPHQL_HTTP_CONFORMANCE_EVIDENCE,
            {"result_ids": result_ids},
        )
        if query_passed()
        else failed("query", RuntimeError("GraphQL query did not return expected intro record")),
        "explain": passed(
            "explain",
            "POST /v1/graphql with explain: true",
            {"explain": smoke_summary.get("graphql_explain")},
        )
        if explain_passed()
        else failed("explain", RuntimeError("GraphQL explain evidence missing")),
        "errors": passed(
            "errors",
            "invalid GraphQL bad-request envelope",
            {
                "status": smoke_summary.get("invalid_graphql_status"),
                "code": smoke_summary.get("invalid_graphql_code"),
                "error": invalid_error,
            },
        )
        if invalid_graphql_passed()
        else failed("errors", RuntimeError("invalid GraphQL error envelope evidence missing")),
    }
    scenarios = ordered_surface_scenarios(
        manifest,
        scenario_map,
        default_reason=GRAPHQL_HTTP_NOT_CHECKED_REASON,
    )
    return finalize_surface(
        "graphql",
        "checked",
        scenarios,
        evidence=[
            GRAPHQL_SCHEMA_CONFORMANCE_EVIDENCE,
            GRAPHQL_HTTP_CONFORMANCE_EVIDENCE,
            "scripts/platform_conformance.py --surface graphql",
            "not GraphQL resolver runtime or full adapter parity",
        ],
    )


def standalone_sdk_repo(repo_root: Path, env_var: str, fallback_name: str) -> Path:
    override = os.environ.get(env_var, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (repo_root.parent / fallback_name).resolve()


def sdk_repo_not_found_report(
    manifest: dict[str, Any],
    surface_id: str,
    repo_path: Path,
    env_var: str,
) -> dict[str, Any]:
    return empty_surface_report(
        manifest,
        surface_id,
        "not_run",
        f"standalone SDK repo not found at {repo_path}; set {env_var} to override",
    )


def run_rust_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    sdk_repo = standalone_sdk_repo(repo_root, RUST_SDK_REPO_ENV, "tracedb-rust")
    if not sdk_repo.exists():
        return sdk_repo_not_found_report(manifest, "rust_sdk", sdk_repo, RUST_SDK_REPO_ENV)
    with tempfile.TemporaryDirectory(prefix="tracedb-rust-sdk-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        admin_dir = temp / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        env = os.environ.copy()
        env.update(
            {
                "TRACEDB_BIND": bind,
                "TRACEDB_DATA_DIR": str(data_dir),
                "TRACEDB_SERVICE_MODE": "engine",
                "CARGO_TERM_COLOR": "never",
                "CARGO_INCREMENTAL": "0",
            }
        )
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)
            command = run_command(
                [
                    "cargo",
                    "run",
                    "-q",
                    "--example",
                    "quickstart",
                    "--",
                    "--url",
                    base_url,
                    "--token",
                    "dev-token",
                    "--timeout-ms",
                    "5000",
                    "--safe-retries",
                    "1",
                    "--idempotency-retries",
                    "1",
                    "--admin-dir",
                    str(admin_dir),
                ],
                sdk_repo,
                env_extra={"TRACEDB_CORE_REPO": str(repo_root)},
            )
        finally:
            stop_process(process)
    if not command["ok"]:
        return finalize_surface(
            "rust_sdk",
            "failed",
            [
                scenario_result(
                    scenario_id,
                    "failed",
                    reason=(
                        "rust_sdk standalone quickstart failed: "
                        f"stdout={command['stdout'][-12_000:]} stderr={command['stderr_tail']}"
                    ),
                )
                for scenario_id in contract_scenario_ids(manifest)
            ],
            evidence=[json.dumps({"command": command["argv"], "returncode": command["returncode"]})],
        )
    quickstart_summary = json.loads(command["stdout"])
    surface = map_rust_sdk_quickstart_summary(manifest, quickstart_summary)
    surface["command"] = {
        "argv": command["argv"],
        "cwd": str(sdk_repo),
        "env": {"TRACEDB_CORE_REPO": str(repo_root)},
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def run_typescript_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    sdk_repo = standalone_sdk_repo(repo_root, TYPESCRIPT_SDK_REPO_ENV, "tracedb-js")
    if not sdk_repo.exists():
        return sdk_repo_not_found_report(manifest, "typescript_sdk", sdk_repo, TYPESCRIPT_SDK_REPO_ENV)
    with tempfile.TemporaryDirectory(prefix="tracedb-typescript-sdk-conformance-") as temp_dir:
        summary_path = Path(temp_dir) / "typescript-sdk-smoke.json"
        command = run_command(
            [
                "npm",
                "run",
                "public-http-smoke",
                "--",
                "--summary-json",
                str(summary_path),
            ],
            sdk_repo,
            env_extra={"TRACEDB_CORE_REPO": str(repo_root)},
        )
        if not command["ok"]:
            return finalize_surface(
                "typescript_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "typescript_sdk public HTTP smoke failed: "
                            f"stdout={command['stdout'][-12_000:]} stderr={command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[json.dumps({"command": command["argv"], "returncode": command["returncode"]})],
            )
        smoke_summary = json.loads(summary_path.read_text())
    surface = map_typescript_sdk_smoke_summary(manifest, smoke_summary)
    surface["command"] = {
        "argv": command["argv"],
        "cwd": str(sdk_repo),
        "env": {"TRACEDB_CORE_REPO": str(repo_root)},
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def install_python_sdk_package_for_conformance(source_dir: Path, temp_dir: Path) -> tuple[Path, dict[str, Any]]:
    target_dir = temp_dir / "python-sdk-site"
    command = run_command(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--target",
            str(target_dir),
            str(source_dir),
        ],
        temp_dir,
    )
    return target_dir, command


def run_python_sdk_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    sdk_repo = standalone_sdk_repo(repo_root, PYTHON_SDK_REPO_ENV, "tracedb-python")
    if not sdk_repo.exists():
        return sdk_repo_not_found_report(manifest, "python_sdk", sdk_repo, PYTHON_SDK_REPO_ENV)
    with tempfile.TemporaryDirectory(prefix="tracedb-python-sdk-conformance-") as temp_dir:
        temp_path = Path(temp_dir)
        summary_path = Path(temp_dir) / "python-sdk-smoke.json"
        target_dir, install_command = install_python_sdk_package_for_conformance(sdk_repo, temp_path)
        if not install_command["ok"]:
            return finalize_surface(
                "python_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "python_sdk package install failed before HTTP smoke: "
                            f"stdout={install_command['stdout'][-12_000:]} stderr={install_command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[
                    PYTHON_SDK_CONFORMANCE_EVIDENCE,
                    json.dumps({"command": install_command["argv"], "returncode": install_command["returncode"]}),
                ],
            )
        command = run_command(
            [
                sys.executable,
                str(sdk_repo / "http_smoke.py"),
                "--summary-json",
                str(summary_path),
            ],
            sdk_repo,
            env_extra={
                "PYTHONPATH": str(target_dir),
                "TRACEDB_PYTHON_IMPORT_MODE": "installed",
                "TRACEDB_CORE_REPO": str(repo_root),
            },
        )
        if not command["ok"]:
            return finalize_surface(
                "python_sdk",
                "failed",
                [
                    scenario_result(
                        scenario_id,
                        "failed",
                        reason=(
                            "python_sdk smoke failed: "
                            f"stdout={command['stdout'][-12_000:]} stderr={command['stderr_tail']}"
                        ),
                    )
                    for scenario_id in contract_scenario_ids(manifest)
                ],
                evidence=[
                    PYTHON_SDK_CONFORMANCE_EVIDENCE,
                    json.dumps({"command": install_command["argv"], "returncode": install_command["returncode"]}),
                    json.dumps({"command": command["argv"], "returncode": command["returncode"]}),
                ],
            )
        smoke_summary = json.loads(summary_path.read_text())
    surface = map_python_sdk_smoke_summary(manifest, smoke_summary)
    surface["install_command"] = {
        "argv": install_command["argv"],
        "duration_s": install_command["duration_s"],
        "returncode": install_command["returncode"],
    }
    surface["command"] = {
        "argv": command["argv"],
        "env": {
            "PYTHONPATH": str(target_dir),
            "TRACEDB_PYTHON_IMPORT_MODE": "installed",
            "TRACEDB_CORE_REPO": str(repo_root),
        },
        "duration_s": command["duration_s"],
        "returncode": command["returncode"],
    }
    return surface


def run_traceql_sqlish_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-traceql-sqlish-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        env = os.environ.copy()
        env.update(
            {
                "TRACEDB_BIND": bind,
                "TRACEDB_DATA_DIR": str(data_dir),
                "TRACEDB_SERVICE_MODE": "engine",
                "CARGO_TERM_COLOR": "never",
                "CARGO_INCREMENTAL": "0",
            }
        )
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)
            request_json(base_url, "POST", "/v1/schema/apply", table_schema())
            request_json(
                base_url,
                "POST",
                "/v1/records/put-batch",
                {
                    "records": [
                        record(
                            "intro",
                            "TraceDB SQL-ish conformance",
                            "reviewed",
                            [1.0, 0.0, 0.0],
                        ),
                        record(
                            "draft",
                            "TraceDB SQL-ish non-matching draft",
                            "draft",
                            [0.0, 1.0, 0.0],
                        ),
                    ]
                },
                headers={"Idempotency-Key": "traceql-sqlish-seed"},
            )
            smoke_summary = traceql_sqlish_conformance_summary(base_url)
        except Exception as error:  # noqa: BLE001 - conformance reports need the failure reason.
            return finalize_surface(
                "traceql_sqlish",
                "failed",
                [failed(scenario_id, error) for scenario_id in contract_scenario_ids(manifest)],
                evidence=[TRACEQL_SQLISH_CONFORMANCE_EVIDENCE],
            )
        finally:
            stop_process(process)

    return map_traceql_sqlish_summary(manifest, smoke_summary)


def run_traceql_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-traceql-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        admin_dir = temp / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        env = os.environ.copy()
        env.update(
            {
                "TRACEDB_BIND": bind,
                "TRACEDB_DATA_DIR": str(data_dir),
                "TRACEDB_SERVICE_MODE": "engine",
                "CARGO_TERM_COLOR": "never",
                "CARGO_INCREMENTAL": "0",
            }
        )
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)
            return traceql_native_surface_report(manifest, base_url, admin_dir)
        except Exception as error:  # noqa: BLE001 - conformance reports need the failure reason.
            return finalize_surface(
                "traceql",
                "failed",
                [failed(scenario_id, error) for scenario_id in contract_scenario_ids(manifest)],
                evidence=[TRACEQL_NATIVE_CONFORMANCE_EVIDENCE],
            )
        finally:
            stop_process(process)


def run_graphql_surface(manifest: dict[str, Any], repo_root: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="tracedb-graphql-conformance-") as temp_dir:
        temp = Path(temp_dir)
        data_dir = temp / "data"
        admin_dir = temp / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        bind = f"127.0.0.1:{free_port()}"
        base_url = f"http://{bind}"
        env = os.environ.copy()
        env.update(
            {
                "TRACEDB_BIND": bind,
                "TRACEDB_DATA_DIR": str(data_dir),
                "TRACEDB_SERVICE_MODE": "engine",
                "CARGO_TERM_COLOR": "never",
                "CARGO_INCREMENTAL": "0",
            }
        )
        process = subprocess.Popen(
            ["cargo", "run", "-q", "-p", "tracedb-server"],
            cwd=repo_root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_for_ready(base_url, process)
            return graphql_native_surface_report(manifest, base_url, admin_dir)
        except Exception as error:  # noqa: BLE001 - conformance reports need the failure reason.
            return finalize_surface(
                "graphql",
                "failed",
                [failed(scenario_id, error) for scenario_id in contract_scenario_ids(manifest)],
                evidence=[GRAPHQL_NATIVE_CONFORMANCE_EVIDENCE],
            )
        finally:
            stop_process(process)


def build_report(manifest: dict[str, Any], surfaces: list[dict[str, Any]]) -> dict[str, Any]:
    totals = {
        "surfaces": len(surfaces),
        "complete_surfaces": sum(1 for surface in surfaces if surface["complete"]),
        "passed_scenarios": sum(surface["passed"] for surface in surfaces),
        "required_scenarios": sum(surface["required"] for surface in surfaces),
        "not_checked_scenarios": sum(surface["not_checked"] for surface in surfaces),
        "failed_scenarios": sum(surface["failed"] for surface in surfaces),
    }
    return {
        "ok": all(surface["ok"] for surface in surfaces),
        "complete": all(surface["complete"] for surface in surfaces),
        "mode": "platform-conformance",
        "contract": manifest["contract"],
        "version": manifest.get("version"),
        "sql_compatibility": manifest.get("sql_compatibility"),
        "postgres_compatibility": manifest.get("postgres_compatibility"),
        "totals": totals,
        "surfaces": surfaces,
    }


def write_summary(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def run_selected_surfaces(
    manifest: dict[str, Any],
    repo_root: Path,
    selected_surfaces: list[str],
) -> list[dict[str, Any]]:
    unknown = sorted(set(selected_surfaces) - set(contract_surface_ids(manifest)))
    if unknown:
        raise ValueError(f"unknown platform conformance surface(s): {', '.join(unknown)}")
    surfaces = []
    for surface_id in selected_surfaces:
        if surface_id == "http_direct":
            surfaces.append(run_http_direct_surface(manifest, repo_root))
        elif surface_id == "rust_sdk":
            surfaces.append(run_rust_sdk_surface(manifest, repo_root))
        elif surface_id == "typescript_sdk":
            surfaces.append(run_typescript_sdk_surface(manifest, repo_root))
        elif surface_id == "python_sdk":
            surfaces.append(run_python_sdk_surface(manifest, repo_root))
        elif surface_id == "traceql":
            surfaces.append(run_traceql_surface(manifest, repo_root))
        elif surface_id == "traceql_sqlish":
            surfaces.append(run_traceql_sqlish_surface(manifest, repo_root))
        elif surface_id == "graphql":
            surfaces.append(run_graphql_surface(manifest, repo_root))
        else:
            surfaces.append(
                empty_surface_report(
                    manifest,
                    surface_id,
                    "not_run",
                    "surface is declared in Platform Contract v0 but not executable in this runner yet",
                )
            )
    return surfaces


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TraceDB Platform Contract v0 conformance lanes.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument(
        "--surface",
        action="append",
        dest="surfaces",
        help="Surface id to run. Defaults to http_direct.",
    )
    parser.add_argument("--summary-json", help="Optional path to write the JSON report.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    contract_path = Path(args.contract)
    if not contract_path.is_absolute():
        contract_path = repo_root / contract_path
    manifest = load_contract(contract_path)
    selected_surfaces = args.surfaces or DEFAULT_SURFACES
    report = build_report(manifest, run_selected_surfaces(manifest, repo_root, selected_surfaces))
    output = json.dumps(report, indent=2, sort_keys=True)
    print(output)
    if args.summary_json:
        write_summary(report, Path(args.summary_json))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
